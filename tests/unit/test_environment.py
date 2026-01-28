"""
Unit tests for simulation environment.
"""

from datetime import date, datetime, timedelta

import numpy as np
import pytest

from brickwell_health.core.environment import SimulationEnvironment


class TestSimulationEnvironment:
    """Tests for SimulationEnvironment."""

    def test_initial_state(self, test_rng: np.random.Generator):
        """Environment should start at day 0."""
        env = SimulationEnvironment(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            rng=test_rng,
            worker_id=0,
        )

        assert env.now == 0
        assert env.current_date == date(2024, 1, 1)

    def test_duration_calculation(self, test_rng: np.random.Generator):
        """Duration should be correct number of days."""
        env = SimulationEnvironment(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            rng=test_rng,
            worker_id=0,
        )

        # 2024 is a leap year, so 366 days
        assert env.duration_days == 365

    def test_to_simpy_time_conversion(self, test_rng: np.random.Generator):
        """Should convert date to SimPy time correctly."""
        env = SimulationEnvironment(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            rng=test_rng,
            worker_id=0,
        )

        # 10 days after start
        target = date(2024, 1, 11)
        simpy_time = env.to_simpy_time(target)

        assert simpy_time == 10.0

    def test_to_date_conversion(self, test_rng: np.random.Generator):
        """Should convert SimPy time to date correctly."""
        env = SimulationEnvironment(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            rng=test_rng,
            worker_id=0,
        )

        result = env.to_date(30.0)

        assert result == date(2024, 1, 31)

    def test_financial_year_first_half(self, test_rng: np.random.Generator):
        """Financial year should be correct in Jan-Jun."""
        env = SimulationEnvironment(
            start_date=date(2024, 3, 15),
            end_date=date(2024, 12, 31),
            rng=test_rng,
            worker_id=0,
        )

        assert env.current_financial_year == "2023-2024"

    def test_financial_year_second_half(self, test_rng: np.random.Generator):
        """Financial year should be correct in Jul-Dec."""
        env = SimulationEnvironment(
            start_date=date(2024, 9, 15),
            end_date=date(2024, 12, 31),
            rng=test_rng,
            worker_id=0,
        )

        assert env.current_financial_year == "2024-2025"

    def test_progress_calculation(self, test_rng: np.random.Generator):
        """Progress should be calculated correctly."""
        env = SimulationEnvironment(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            rng=test_rng,
            worker_id=0,
        )

        # At start
        assert env.get_progress() == 0.0

        # Run simulation part way
        env.run(until=100)

        # Should be ~27% through
        progress = env.get_progress()
        assert 25 < progress < 30
