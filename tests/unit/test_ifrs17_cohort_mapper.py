"""Unit tests for the IFRS 17 cohort mapper."""

from datetime import date

import pytest

from brickwell_health.ifrs17.cohort_mapper import (
    CohortMapper,
    afy_label_for,
    afy_window,
    all_afy_labels_in_range,
    cohort_id,
    portfolio_from_flags,
)


class TestAFYBoundaries:
    """AFY = Australian Financial Year, July 1 - June 30."""

    def test_end_of_afy25(self):
        assert afy_label_for(date(2025, 6, 30)) == "AFY25"

    def test_start_of_afy26(self):
        assert afy_label_for(date(2025, 7, 1)) == "AFY26"

    def test_calendar_end_of_year_is_afy_mid(self):
        assert afy_label_for(date(2025, 12, 31)) == "AFY26"

    def test_january_is_second_half_of_afy(self):
        assert afy_label_for(date(2026, 1, 1)) == "AFY26"

    def test_afy_window_end_of_afy25(self):
        start, end = afy_window(date(2025, 6, 30))
        assert start == date(2024, 7, 1)
        assert end == date(2025, 6, 30)

    def test_afy_window_start_of_afy26(self):
        start, end = afy_window(date(2025, 7, 1))
        assert start == date(2025, 7, 1)
        assert end == date(2026, 6, 30)


class TestPortfolioFlags:
    def test_hospital_and_extras_is_combined(self):
        assert portfolio_from_flags(True, True, False) == "COMBINED"

    def test_hospital_and_extras_with_ambulance_is_combined(self):
        assert portfolio_from_flags(True, True, True) == "COMBINED"

    def test_hospital_only(self):
        assert portfolio_from_flags(True, False, False) == "HOSPITAL_ONLY"

    def test_hospital_and_ambulance_is_hospital_only(self):
        # Per plan: ambulance flag alongside hospital does not create a new portfolio
        assert portfolio_from_flags(True, False, True) == "HOSPITAL_ONLY"

    def test_extras_only(self):
        assert portfolio_from_flags(False, True, False) == "EXTRAS_ONLY"

    def test_extras_and_ambulance_is_extras_only(self):
        assert portfolio_from_flags(False, True, True) == "EXTRAS_ONLY"

    def test_ambulance_only(self):
        assert portfolio_from_flags(False, False, True) == "AMBULANCE_ONLY"


class TestCohortIdComposition:
    def test_cohort_id_format(self):
        assert cohort_id("HOSPITAL_ONLY", "AFY26") == "HOSPITAL_ONLY-AFY26"

    def test_mapper_cohort_id(self, tmp_path):
        mapper = CohortMapper(product_to_portfolio={42: "EXTRAS_ONLY"})
        assert mapper.cohort_id_for(date(2025, 8, 1), 42) == "EXTRAS_ONLY-AFY26"

    def test_mapper_unknown_product_raises(self):
        mapper = CohortMapper(product_to_portfolio={1: "HOSPITAL_ONLY"})
        with pytest.raises(KeyError):
            mapper.portfolio_for(999)


class TestEnumerateCohorts:
    def test_enumerates_all_4_portfolios_per_afy(self):
        mapper = CohortMapper(product_to_portfolio={1: "HOSPITAL_ONLY"})
        rows = mapper.enumerate_cohorts(date(2024, 7, 1), date(2025, 6, 30))
        # 1 AFY × 4 canonical portfolios
        assert len(rows) == 4
        labels = {r[2] for r in rows}
        assert labels == {"AFY25"}
        portfolios = {r[1] for r in rows}
        assert portfolios == {"HOSPITAL_ONLY", "EXTRAS_ONLY", "COMBINED", "AMBULANCE_ONLY"}

    def test_partial_afy_at_tail(self):
        # 18-month window should still produce 2 AFYs (the tail AFY overlaps
        # partially).
        rows = all_afy_labels_in_range(date(2025, 1, 1), date(2026, 6, 30))
        labels = [r[0] for r in rows]
        assert labels == ["AFY25", "AFY26"]
