"""
Post-simulation benefit-usage scrub.

The simulator generates claims in parallel workers and tracks cumulative
benefit usage per worker. Because members and their policies can be
partitioned to different workers (member_id is partition-owned, policy_id
is not), claims for a single member may land on different workers with
independent ``cumulative_usage`` views. The net effect is that annual
benefit limits can be breached by a small margin (typically a handful of
dollars across a handful of members).

Rather than re-architect partitioning, this module reconciles overspend
after the fact — exactly as a real insurer would at end-of-period — by
walking each overspent ``(member, year, benefit_category)`` bucket in
service-date order, capping benefits at the annual limit, and
re-deriving claim totals and the ``benefit_usage`` aggregate.
"""

from __future__ import annotations

import structlog
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = structlog.get_logger()


def scrub_benefit_usage(engine: Engine) -> dict[str, int]:
    """Cap over-limit benefits and regenerate the affected aggregates.

    Returns a dict with scrub counters:
        buckets_scrubbed: number of (member, year, category) buckets over cap
        lines_adjusted:  number of claim_line rows zeroed or partially capped
        claims_resummed: number of distinct claims whose totals were rewritten
    """
    result = {"buckets_scrubbed": 0, "lines_adjusted": 0, "claims_resummed": 0}

    with engine.begin() as conn:
        overcapped = conn.execute(
            text(
                """
                SELECT member_id,
                       benefit_year,
                       benefit_category_id,
                       MAX(annual_limit) AS annual_limit,
                       SUM(usage_amount) AS total_usage
                FROM claims.benefit_usage
                WHERE annual_limit IS NOT NULL
                GROUP BY member_id, benefit_year, benefit_category_id
                HAVING SUM(usage_amount) > MAX(annual_limit) + 0.01
                """
            )
        ).fetchall()

        if not overcapped:
            logger.info("benefit_usage_scrub_no_overspend")
            return result

        limits_id = conn.execute(
            text(
                "SELECT rejection_reason_id FROM reference.claim_rejection_reason "
                "WHERE reason_code = 'LIMITS_EXHAUSTED'"
            )
        ).scalar()

        claims_to_resum: set = set()

        for row in overcapped:
            member_id = row.member_id
            benefit_year = row.benefit_year
            category_id = row.benefit_category_id
            annual_limit = float(row.annual_limit)

            # The simulator keys benefit_usage.benefit_year on the payment
            # date, not the service date (see get_financial_year usage in
            # ClaimsProcess._record_benefit_usage), so we cannot derive the
            # scrub's claim_line set from service_date alone. Drive the set
            # from benefit_usage.claim_id for this bucket, order by the
            # bucket's usage_date (the FIFO used to accumulate usage), and
            # use benefit_usage.usage_date for the line-level ordering
            # fallback when multiple lines share a claim.
            lines = conn.execute(
                text(
                    """
                    SELECT cl.claim_line_id,
                           cl.claim_id,
                           cl.charge_amount,
                           cl.benefit_amount,
                           bu.usage_date,
                           cl.service_date
                    FROM claims.benefit_usage bu
                    JOIN claims.claim c ON c.claim_id = bu.claim_id
                    JOIN claims.claim_line cl ON cl.claim_id = c.claim_id
                    JOIN claims.extras_claim ec ON ec.claim_line_id = cl.claim_line_id
                    WHERE bu.member_id = :member_id
                      AND bu.benefit_year = :benefit_year
                      AND bu.benefit_category_id = :category_id
                      AND c.claim_status = 'Paid'
                      AND cl.line_status = 'Paid'
                    ORDER BY bu.usage_date ASC, cl.service_date ASC, cl.claim_line_id ASC
                    """
                ),
                {
                    "member_id": member_id,
                    "benefit_year": benefit_year,
                    "category_id": category_id,
                },
            ).fetchall()

            if not lines:
                continue

            remaining = annual_limit
            bucket_changed = False
            for line in lines:
                benefit = float(line.benefit_amount or 0)
                charge = float(line.charge_amount or 0)

                if benefit <= remaining + 0.01:
                    remaining -= benefit
                    continue

                if remaining > 0.01:
                    new_benefit = round(remaining, 2)
                    new_gap = round(charge - remaining, 2)
                    remaining = 0.0
                    new_reason = None
                else:
                    new_benefit = 0.0
                    new_gap = round(charge, 2)
                    new_reason = limits_id

                conn.execute(
                    text(
                        """
                        UPDATE claims.claim_line
                        SET benefit_amount = :benefit,
                            gap_amount = :gap,
                            rejection_reason_id = COALESCE(:reason_id, rejection_reason_id),
                            modified_at = NOW(),
                            modified_by = 'BENEFIT_SCRUB'
                        WHERE claim_line_id = :line_id
                        """
                    ),
                    {
                        "benefit": new_benefit,
                        "gap": new_gap,
                        "reason_id": new_reason,
                        "line_id": line.claim_line_id,
                    },
                )
                result["lines_adjusted"] += 1
                claims_to_resum.add(line.claim_id)
                bucket_changed = True

            if bucket_changed:
                result["buckets_scrubbed"] += 1

        for claim_id in claims_to_resum:
            conn.execute(
                text(
                    """
                    UPDATE claims.claim
                    SET total_benefit = sub.total_benefit,
                        total_gap = sub.total_gap,
                        modified_at = NOW(),
                        modified_by = 'BENEFIT_SCRUB'
                    FROM (
                        SELECT claim_id,
                               COALESCE(SUM(benefit_amount), 0) AS total_benefit,
                               COALESCE(SUM(gap_amount), 0) AS total_gap
                        FROM claims.claim_line
                        WHERE claim_id = :claim_id
                        GROUP BY claim_id
                    ) sub
                    WHERE claims.claim.claim_id = sub.claim_id
                    """
                ),
                {"claim_id": claim_id},
            )
            result["claims_resummed"] += 1

        # Re-derive benefit_usage aggregates for adjusted claims. Each
        # benefit_usage row corresponds to one (claim, member, category, year)
        # tuple; update usage_amount from the current per-claim benefit total
        # for that category. The original aggregation is 1 usage row per claim.
        if claims_to_resum:
            conn.execute(
                text(
                    """
                    UPDATE claims.benefit_usage bu
                    SET usage_amount = GREATEST(COALESCE(sub.paid_benefit, 0), 0),
                        remaining_limit = GREATEST(
                            COALESCE(bu.annual_limit, 0) - COALESCE(sub.paid_benefit, 0), 0
                        )
                    FROM (
                        SELECT cl.claim_id,
                               SUM(cl.benefit_amount) AS paid_benefit
                        FROM claims.claim_line cl
                        WHERE cl.claim_id = ANY(:claim_ids)
                          AND cl.line_status = 'Paid'
                        GROUP BY cl.claim_id
                    ) sub
                    WHERE bu.claim_id = sub.claim_id
                    """
                ),
                {"claim_ids": list(claims_to_resum)},
            )

    logger.info(
        "benefit_usage_scrub_completed",
        buckets=result["buckets_scrubbed"],
        lines=result["lines_adjusted"],
        claims=result["claims_resummed"],
    )
    return result
