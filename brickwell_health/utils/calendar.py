"""
Australian business calendar utilities.

Provides functions for determining business days and public holidays
in Australia.
"""

from datetime import date, timedelta
from functools import lru_cache

import holidays


class AustralianCalendar:
    """
    Australian business calendar with public holidays.

    Caches holiday lookups for performance.

    Usage:
        calendar = AustralianCalendar(state="VIC")
        if calendar.is_business_day(date(2024, 1, 26)):
            # Process business day logic
            ...
    """

    def __init__(self, state: str = "VIC"):
        """
        Initialize the calendar.

        Args:
            state: Australian state code for state-specific holidays
        """
        self.state = state.upper()
        self._holiday_cache: dict[int, set[date]] = {}

    def _get_holidays_for_year(self, year: int) -> set[date]:
        """
        Get holidays for a specific year (cached).

        Args:
            year: Year to get holidays for

        Returns:
            Set of holiday dates
        """
        if year not in self._holiday_cache:
            # Get Australian federal holidays + state holidays
            au_holidays = holidays.Australia(
                years=year,
                prov=self.state,
            )
            self._holiday_cache[year] = set(au_holidays.keys())
        return self._holiday_cache[year]

    def is_holiday(self, d: date) -> bool:
        """
        Check if a date is a public holiday.

        Args:
            d: Date to check

        Returns:
            True if it's a public holiday
        """
        year_holidays = self._get_holidays_for_year(d.year)
        return d in year_holidays

    def is_weekend(self, d: date) -> bool:
        """
        Check if a date is a weekend.

        Args:
            d: Date to check

        Returns:
            True if it's Saturday or Sunday
        """
        return d.weekday() >= 5

    def is_business_day(self, d: date) -> bool:
        """
        Check if a date is a business day.

        A business day is not a weekend and not a public holiday.

        Args:
            d: Date to check

        Returns:
            True if it's a business day
        """
        return not self.is_weekend(d) and not self.is_holiday(d)

    def next_business_day(self, d: date) -> date:
        """
        Get the next business day on or after a date.

        Args:
            d: Starting date

        Returns:
            Next business day
        """
        while not self.is_business_day(d):
            d = d + timedelta(days=1)
        return d

    def previous_business_day(self, d: date) -> date:
        """
        Get the previous business day on or before a date.

        Args:
            d: Starting date

        Returns:
            Previous business day
        """
        while not self.is_business_day(d):
            d = d - timedelta(days=1)
        return d

    def add_business_days(self, d: date, days: int) -> date:
        """
        Add business days to a date.

        Args:
            d: Starting date
            days: Number of business days to add (can be negative)

        Returns:
            Resulting date
        """
        if days == 0:
            return d

        step = 1 if days > 0 else -1
        remaining = abs(days)

        while remaining > 0:
            d = d + timedelta(days=step)
            if self.is_business_day(d):
                remaining -= 1

        return d

    def business_days_between(self, start: date, end: date) -> int:
        """
        Count business days between two dates (exclusive of end).

        Args:
            start: Start date
            end: End date

        Returns:
            Number of business days
        """
        if start >= end:
            return 0

        count = 0
        current = start
        while current < end:
            if self.is_business_day(current):
                count += 1
            current = current + timedelta(days=1)

        return count

    def get_month_end_business_day(self, year: int, month: int) -> date:
        """
        Get the last business day of a month.

        Args:
            year: Year
            month: Month (1-12)

        Returns:
            Last business day of the month
        """
        # Get last day of month
        if month == 12:
            last_day = date(year, 12, 31)
        else:
            last_day = date(year, month + 1, 1) - timedelta(days=1)

        return self.previous_business_day(last_day)


# Module-level convenience functions
_default_calendar: AustralianCalendar | None = None


def _get_default_calendar() -> AustralianCalendar:
    """Get or create the default calendar."""
    global _default_calendar
    if _default_calendar is None:
        _default_calendar = AustralianCalendar()
    return _default_calendar


def is_business_day(d: date, state: str = "VIC") -> bool:
    """
    Check if a date is a business day.

    Args:
        d: Date to check
        state: Australian state code

    Returns:
        True if it's a business day
    """
    calendar = AustralianCalendar(state)
    return calendar.is_business_day(d)


def get_next_business_day(d: date, state: str = "VIC") -> date:
    """
    Get the next business day on or after a date.

    Args:
        d: Starting date
        state: Australian state code

    Returns:
        Next business day
    """
    calendar = AustralianCalendar(state)
    return calendar.next_business_day(d)


def get_business_days_between(start: date, end: date, state: str = "VIC") -> int:
    """
    Count business days between two dates.

    Args:
        start: Start date
        end: End date
        state: Australian state code

    Returns:
        Number of business days
    """
    calendar = AustralianCalendar(state)
    return calendar.business_days_between(start, end)
