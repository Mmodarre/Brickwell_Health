"""
Time conversion utilities for Brickwell Health Simulator.

Provides date manipulation functions used throughout the simulation.
"""

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta


def days_between(start: date, end: date) -> int:
    """
    Calculate the number of days between two dates.

    Args:
        start: Start date
        end: End date

    Returns:
        Number of days (positive if end > start)
    """
    return (end - start).days


def add_days(d: date, days: int) -> date:
    """
    Add days to a date.

    Args:
        d: Base date
        days: Number of days to add (can be negative)

    Returns:
        New date
    """
    return d + timedelta(days=days)


def add_months(d: date, months: int) -> date:
    """
    Add months to a date.

    Handles end-of-month edge cases (e.g., Jan 31 + 1 month = Feb 28).

    Args:
        d: Base date
        months: Number of months to add (can be negative)

    Returns:
        New date
    """
    return d + relativedelta(months=months)


def first_of_month(d: date) -> date:
    """
    Get the first day of the month.

    Args:
        d: Any date in the month

    Returns:
        First day of that month
    """
    return date(d.year, d.month, 1)


def last_of_month(d: date) -> date:
    """
    Get the last day of the month.

    Args:
        d: Any date in the month

    Returns:
        Last day of that month
    """
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def first_of_next_month(d: date) -> date:
    """
    Get the first day of the next month.

    Args:
        d: Any date

    Returns:
        First day of the following month
    """
    return add_months(first_of_month(d), 1)


def next_business_day(d: date, holidays: set[date] | None = None) -> date:
    """
    Get the next business day (excluding weekends and holidays).

    Args:
        d: Starting date
        holidays: Optional set of holiday dates

    Returns:
        Next business day on or after the given date
    """
    holidays = holidays or set()
    while d.weekday() >= 5 or d in holidays:  # Saturday = 5, Sunday = 6
        d = d + timedelta(days=1)
    return d


def get_age(date_of_birth: date, as_of_date: date) -> int:
    """
    Calculate age in complete years.

    Args:
        date_of_birth: Birth date
        as_of_date: Date to calculate age as of

    Returns:
        Age in complete years
    """
    age = as_of_date.year - date_of_birth.year
    
    # Adjust if birthday hasn't occurred yet this year
    if (as_of_date.month, as_of_date.day) < (date_of_birth.month, date_of_birth.day):
        age -= 1
    
    return max(0, age)


def get_financial_year(d: date) -> str:
    """
    Get the Australian financial year for a date.

    Financial year runs July 1 to June 30.

    Args:
        d: Date to get FY for

    Returns:
        Financial year string (e.g., "2024-2025")
    """
    if d.month >= 7:
        return f"{d.year}-{d.year + 1}"
    else:
        return f"{d.year - 1}-{d.year}"


def get_financial_year_start(fy: str) -> date:
    """
    Get the start date of a financial year.

    Args:
        fy: Financial year string (e.g., "2024-2025")

    Returns:
        July 1 of the start year
    """
    start_year = int(fy.split("-")[0])
    return date(start_year, 7, 1)


def get_financial_year_end(fy: str) -> date:
    """
    Get the end date of a financial year.

    Args:
        fy: Financial year string (e.g., "2024-2025")

    Returns:
        June 30 of the end year
    """
    end_year = int(fy.split("-")[1])
    return date(end_year, 6, 30)


def months_between(start: date, end: date) -> int:
    """
    Calculate complete months between two dates.

    Args:
        start: Start date
        end: End date

    Returns:
        Number of complete months
    """
    diff = relativedelta(end, start)
    return diff.years * 12 + diff.months


def date_range(start: date, end: date) -> list[date]:
    """
    Generate a list of dates from start to end (inclusive).

    Args:
        start: Start date
        end: End date

    Returns:
        List of dates
    """
    days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(days)]
