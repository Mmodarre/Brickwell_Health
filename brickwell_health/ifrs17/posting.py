"""
IFRS 17 posting rule engine.

Loads the accountant-owned YAML mapping from movement buckets to
(debit_account_code, credit_account_code) pairs, validates that every
referenced account code exists in ``reference.gl_account``, and emits the
corresponding debit/credit :class:`IFRS17JournalLineCreate` rows for a given
(cohort, reporting_month) from a movement dict produced by the engine.

The YAML format is documented in ``data/reference/ifrs17_posting_rules.yaml``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Mapping

from brickwell_health.config.loader import load_yaml
from brickwell_health.domain.ifrs17 import IFRS17JournalLineCreate


ZERO = Decimal("0")


class PostingRuleError(ValueError):
    """Raised for any static problem with the posting-rules YAML."""


@dataclass(frozen=True)
class PostingRule:
    """One (bucket -> debit, credit) mapping loaded from YAML."""

    bucket: str
    debit_account_code: str
    credit_account_code: str
    description: str = ""


class PostingRuleEngine:
    """Builds IFRS 17 journal lines from movement buckets."""

    def __init__(self, rules: list[PostingRule]):
        if not rules:
            raise PostingRuleError("No posting rules provided")

        # Enforce one rule per bucket; duplicates would silently shadow.
        by_bucket: dict[str, PostingRule] = {}
        for r in rules:
            if r.bucket in by_bucket:
                raise PostingRuleError(
                    f"Duplicate posting rule for bucket '{r.bucket}'"
                )
            by_bucket[r.bucket] = r
        self._rules_by_bucket = by_bucket

    @classmethod
    def from_yaml(cls, path: Path | str) -> "PostingRuleEngine":
        """Load a rules file."""
        raw = load_yaml(Path(path))
        rules_cfg = raw.get("posting_rules") if isinstance(raw, dict) else None
        if not isinstance(rules_cfg, list) or not rules_cfg:
            raise PostingRuleError(
                f"YAML at {path} must contain a non-empty 'posting_rules' list"
            )

        rules: list[PostingRule] = []
        for i, entry in enumerate(rules_cfg):
            if not isinstance(entry, dict):
                raise PostingRuleError(
                    f"posting_rules[{i}] must be a mapping, got {type(entry).__name__}"
                )
            missing = [
                k for k in ("bucket", "debit_account_code", "credit_account_code")
                if k not in entry
            ]
            if missing:
                raise PostingRuleError(
                    f"posting_rules[{i}] missing required keys: {missing}"
                )
            rules.append(
                PostingRule(
                    bucket=str(entry["bucket"]),
                    debit_account_code=str(entry["debit_account_code"]),
                    credit_account_code=str(entry["credit_account_code"]),
                    description=str(entry.get("description", "")),
                )
            )
        return cls(rules)

    @property
    def buckets(self) -> list[str]:
        return list(self._rules_by_bucket.keys())

    def rule_for(self, bucket: str) -> PostingRule:
        try:
            return self._rules_by_bucket[bucket]
        except KeyError as e:
            raise PostingRuleError(
                f"No posting rule defined for bucket '{bucket}'"
            ) from e

    def validate_against_accounts(
        self, gl_account_by_code: Mapping[str, int]
    ) -> None:
        """Abort if any referenced account code is missing from the chart."""
        missing: list[tuple[str, str, str]] = []
        for rule in self._rules_by_bucket.values():
            if rule.debit_account_code not in gl_account_by_code:
                missing.append((rule.bucket, "debit", rule.debit_account_code))
            if rule.credit_account_code not in gl_account_by_code:
                missing.append((rule.bucket, "credit", rule.credit_account_code))
        if missing:
            detail = ", ".join(
                f"bucket={b} side={side} code={code}" for b, side, code in missing
            )
            raise PostingRuleError(
                f"Posting rules reference unknown gl_account codes: {detail}"
            )

    def build_lines(
        self,
        cohort_id: str,
        reporting_month: date,
        gl_period_id: int,
        gl_account_by_code: Mapping[str, int],
        movement: Mapping[str, object],
    ) -> list[IFRS17JournalLineCreate]:
        """
        Emit debit + credit journal lines for every bucket with a non-zero amount.

        ``movement`` is the per-month dict produced by ``IFRS17Engine._compute_month``
        (e.g. ``m['insurance_revenue']``, ``m['premiums_received']``, ...). Keys
        absent from the dict or with a zero / negative value are skipped.
        """
        lines: list[IFRS17JournalLineCreate] = []
        for bucket, rule in self._rules_by_bucket.items():
            raw_amount = movement.get(bucket)
            if raw_amount is None:
                continue
            # Normalise to Decimal; skip if non-positive (no posting for zero).
            amount = raw_amount if isinstance(raw_amount, Decimal) else Decimal(str(raw_amount))
            if amount <= ZERO:
                continue

            debit_account_id = gl_account_by_code[rule.debit_account_code]
            credit_account_id = gl_account_by_code[rule.credit_account_code]

            lines.append(
                IFRS17JournalLineCreate(
                    journal_line_id=uuid.uuid4(),
                    cohort_id=cohort_id,
                    reporting_month=reporting_month,
                    gl_period_id=gl_period_id,
                    gl_account_id=debit_account_id,
                    cost_centre_id=None,
                    movement_bucket=bucket,
                    debit_amount=amount,
                    credit_amount=ZERO,
                )
            )
            lines.append(
                IFRS17JournalLineCreate(
                    journal_line_id=uuid.uuid4(),
                    cohort_id=cohort_id,
                    reporting_month=reporting_month,
                    gl_period_id=gl_period_id,
                    gl_account_id=credit_account_id,
                    cost_centre_id=None,
                    movement_bucket=bucket,
                    debit_amount=ZERO,
                    credit_amount=amount,
                )
            )
        return lines
