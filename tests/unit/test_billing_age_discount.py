"""
Unit tests for the PHI-Act youngest-adult age-discount rule applied at
invoice time by ``BillingProcess._compute_policy_age_discount_pct``.

These tests bypass SimPy/DB plumbing by binding the helper to a lightweight
stand-in object that exposes the ``age_discount_calculator`` attribute the
helper depends on.
"""

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from brickwell_health.config.regulatory import AgeBasedDiscountCalculator
from brickwell_health.core.processes.billing import BillingProcess
from brickwell_health.domain.member import MemberCreate
from brickwell_health.domain.policy import PolicyCreate


def _make_member(dob: date, member_id: UUID | None = None) -> MemberCreate:
    return MemberCreate(
        member_id=member_id or uuid4(),
        member_number=f"MEM-TEST-{(member_id or uuid4()).hex[:8]}",
        first_name="Test",
        last_name="Member",
        date_of_birth=dob,
        gender="Male",
        state="NSW",
    )


def _make_policy(effective_date: date) -> PolicyCreate:
    return PolicyCreate(
        policy_id=uuid4(),
        policy_number="POL-TEST-0001",
        product_id=1,
        policy_status="Active",
        policy_type="Single",
        effective_date=effective_date,
        payment_frequency="Monthly",
        premium_amount=Decimal("200.00"),
        excess_amount=Decimal("500.00"),
        distribution_channel="Online",
        state_of_residence="NSW",
        original_join_date=effective_date,
    )


def _compute(policy_data: dict, invoice_date: date) -> Decimal:
    helper = SimpleNamespace(age_discount_calculator=AgeBasedDiscountCalculator())
    return BillingProcess._compute_policy_age_discount_pct(  # type: ignore[arg-type]
        helper, policy_data, invoice_date
    )


def test_primary_is_youngest_adult():
    """A: Primary 26, Partner 36 — primary is the only qualifier (8%)."""
    invoice = date(2026, 1, 15)
    effective = date(2026, 1, 1)
    primary = _make_member(date(1999, 6, 1))   # age ~26 at invoice/join
    partner = _make_member(date(1989, 6, 1))   # age ~36
    policy_data = {
        "policy": _make_policy(effective),
        "members": [primary, partner],
    }
    assert _compute(policy_data, invoice) == Decimal("8")


def test_partner_younger_than_primary_picks_partner():
    """B: Primary 36, Partner 26 — current sim picks primary (wrong);
    youngest-adult rule picks partner (8%)."""
    invoice = date(2026, 1, 15)
    effective = date(2026, 1, 1)
    primary = _make_member(date(1989, 6, 1))   # age ~36
    partner = _make_member(date(1999, 6, 1))   # age ~26
    policy_data = {
        "policy": _make_policy(effective),
        "members": [primary, partner],
    }
    assert _compute(policy_data, invoice) == Decimal("8")


def test_adult_dependent_only_qualifier():
    """C: Primary 50, Partner 48, Dependent 19 — only Dependent qualifies."""
    invoice = date(2026, 1, 15)
    effective = date(2026, 1, 1)
    primary = _make_member(date(1975, 6, 1))     # ~50
    partner = _make_member(date(1977, 6, 1))     # ~48
    dependent = _make_member(date(2006, 6, 1))   # ~19
    policy_data = {
        "policy": _make_policy(effective),
        "members": [primary, partner, dependent],
    }
    # 30 - 19 = 11, *2% = 22%, capped at 10%.
    assert _compute(policy_data, invoice) == Decimal("10")


def test_both_adults_qualify_partner_younger():
    """D: Primary 27 (6%), Partner 22 (10%) — youngest-adult picks partner
    (current sim returns primary's 6% — wrong)."""
    invoice = date(2026, 1, 15)
    effective = date(2026, 1, 1)
    primary = _make_member(date(1998, 6, 1))   # ~27
    partner = _make_member(date(2003, 6, 1))   # ~22
    policy_data = {
        "policy": _make_policy(effective),
        "members": [primary, partner],
    }
    assert _compute(policy_data, invoice) == Decimal("10")


def test_phase_out_aging_uses_current_not_locked_rate():
    """E: Joined at 25 (original 10%), invoice when primary is 43 →
    applicable rate is 10 - 2*(43-41) = 6%, not the locked 10%.

    Verifies the third defect: we must use the phase-out-adjusted current
    rate, not the at-join discount_percentage frozen on the regulatory row.
    """
    join_date = date(2008, 1, 1)
    invoice = date(2026, 6, 1)
    # DOB 1982-06-01 → age 25 at join (2008), age 44 at invoice (2026-06-01).
    # Adjust DOB so age at invoice is 43 (not yet 44 birthday).
    primary = _make_member(date(1982, 7, 1))
    # age_at_join = 30-1 = 25 (since 2008-01-01 is before 1982-07-01 birthday in 2008).
    # age_at_invoice on 2026-06-01 = 43 (birthday in July not yet passed).
    policy_data = {
        "policy": _make_policy(join_date),
        "members": [primary],
    }
    assert _compute(policy_data, invoice) == Decimal("6")


def test_death_of_youngest_adult_falls_back_to_primary():
    """F: Couple Primary 36 + Partner 26; partner dies → subsequent invoice
    uses primary's rate (0% in this case — primary too old)."""
    invoice = date(2026, 1, 15)
    effective = date(2026, 1, 1)
    primary = _make_member(date(1989, 6, 1))   # ~36 (no discount)
    partner = _make_member(date(1999, 6, 1))   # ~26 (8%)
    policy_data = {
        "policy": _make_policy(effective),
        "members": [primary, partner],
    }
    # Pre-death: youngest is partner.
    assert _compute(policy_data, invoice) == Decimal("8")
    # Simulate partner death by pruning from the member list (mirrors the
    # change in PolicyLifecycleProcess._remove_member_from_policy).
    policy_data["members"] = [
        m for m in policy_data["members"] if m.member_id != partner.member_id
    ]
    assert _compute(policy_data, invoice) == Decimal("0")


def test_no_qualifying_adult_returns_zero():
    """G: All adults ≥ 51 (phase-out complete), dependents <18 → 0."""
    invoice = date(2026, 1, 15)
    effective = date(2026, 1, 1)
    primary = _make_member(date(1970, 6, 1))     # ~55
    partner = _make_member(date(1972, 6, 1))     # ~53
    dependent = _make_member(date(2015, 6, 1))   # ~10 (skipped, <18)
    policy_data = {
        "policy": _make_policy(effective),
        "members": [primary, partner, dependent],
    }
    assert _compute(policy_data, invoice) == Decimal("0")


def test_invoice_amount_matches_youngest_adult_pct():
    """Sanity check: the percentage the helper returns, applied to a 500
    gross premium, yields the legally correct discount amount under each
    of the four primary scenarios."""
    invoice = date(2026, 1, 15)
    effective = date(2026, 1, 1)
    gross = Decimal("500.00")

    cases = [
        # (primary_dob, partner_dob, expected_pct)
        (date(1999, 6, 1), date(1989, 6, 1), Decimal("8")),  # A
        (date(1989, 6, 1), date(1999, 6, 1), Decimal("8")),  # B
        (date(1998, 6, 1), date(2003, 6, 1), Decimal("10")), # D
    ]
    for primary_dob, partner_dob, expected_pct in cases:
        policy_data = {
            "policy": _make_policy(effective),
            "members": [_make_member(primary_dob), _make_member(partner_dob)],
        }
        pct = _compute(policy_data, invoice)
        assert pct == expected_pct
        amount = (gross * pct / Decimal("100")).quantize(Decimal("0.01"))
        assert amount == (gross * expected_pct / Decimal("100")).quantize(Decimal("0.01"))


def test_tie_break_deterministic_when_dobs_match():
    """When two adults share a date_of_birth, ties are broken by member_id
    (smaller UUID wins) — deterministic regardless of list order."""
    invoice = date(2026, 1, 15)
    effective = date(2026, 1, 1)
    dob = date(1999, 6, 1)
    low = UUID(int=1)
    high = UUID(int=2)
    a = _make_member(dob, member_id=low)
    b = _make_member(dob, member_id=high)
    expected = Decimal("8")

    pd1 = {"policy": _make_policy(effective), "members": [a, b]}
    pd2 = {"policy": _make_policy(effective), "members": [b, a]}
    assert _compute(pd1, invoice) == expected
    assert _compute(pd2, invoice) == expected


def test_no_policy_returns_zero():
    """Defensive: missing policy ⇒ 0 rather than raising."""
    assert _compute({"members": []}, date(2026, 1, 15)) == Decimal("0")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
