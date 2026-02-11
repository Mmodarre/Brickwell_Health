"""
Simulation environment wrapper for Brickwell Health Simulator.

Wraps SimPy environment with datetime conversion and simulation context.
"""

from datetime import date, datetime, timedelta
from typing import Generator, Any

import simpy
from numpy.random import Generator as RNG
from pydantic import ValidationError as PydanticValidationError

import structlog

logger = structlog.get_logger()


class SimulationEnvironment:
    """
    Wrapper around SimPy environment with datetime conversion.

    Time unit: days (float, supports fractional days)

    This class provides:
    - Conversion between SimPy time and datetime
    - Access to the random number generator
    - Process management
    - Simulation running

    Usage:
        rng = np.random.default_rng(42)
        sim_env = SimulationEnvironment(
            start_date=date(2020, 1, 1),
            end_date=date(2025, 12, 31),
            rng=rng,
            worker_id=0,
        )
        sim_env.process(my_process.run())
        sim_env.run()
    """

    def __init__(
        self,
        start_date: date,
        end_date: date,
        rng: RNG,
        worker_id: int = 0,
        elapsed_days_offset: int = 0,
    ):
        """
        Initialize the simulation environment.

        Args:
            start_date: Simulation start date (checkpoint date on resume)
            end_date: Simulation end date
            rng: NumPy random number generator
            worker_id: Worker process identifier (for logging/debugging)
            elapsed_days_offset: Days already elapsed in prior runs before this
                one. Used to correctly track warmup progress across incremental
                simulation runs. On fresh runs this is 0.
        """
        self.env = simpy.Environment()
        self.start_date = start_date
        self.end_date = end_date
        self.rng = rng
        self.worker_id = worker_id
        self.elapsed_days_offset = elapsed_days_offset

        # Calculate simulation duration in days
        self.duration_days = (end_date - start_date).days

        logger.info(
            "simulation_environment_created",
            worker_id=worker_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            duration_days=self.duration_days,
            elapsed_days_offset=elapsed_days_offset,
        )

    @property
    def now(self) -> float:
        """Current simulation time in days from start of this run."""
        return self.env.now

    @property
    def total_elapsed_days(self) -> float:
        """Total days elapsed since the very first simulation run started.

        Accounts for days completed in prior runs (elapsed_days_offset)
        plus days elapsed in the current run (env.now). Use this instead
        of ``now`` for warmup/threshold checks that must be correct across
        incremental simulation runs.
        """
        return self.elapsed_days_offset + self.env.now

    @property
    def current_date(self) -> date:
        """Current simulation date (truncated to day)."""
        return self.start_date + timedelta(days=int(self.env.now))

    @property
    def current_datetime(self) -> datetime:
        """Current simulation datetime (with fractional day component)."""
        days = int(self.env.now)
        fraction = self.env.now - days
        base_dt = datetime.combine(
            self.start_date + timedelta(days=days),
            datetime.min.time(),
        )
        return base_dt + timedelta(days=fraction)

    @property
    def current_year(self) -> int:
        """Current simulation year."""
        return self.current_date.year

    @property
    def current_financial_year(self) -> str:
        """
        Current financial year in format "YYYY-YYYY".

        Australian financial year runs July 1 to June 30.
        """
        current = self.current_date
        if current.month >= 7:
            return f"{current.year}-{current.year + 1}"
        else:
            return f"{current.year - 1}-{current.year}"

    def to_simpy_time(self, target_date: date) -> float:
        """
        Convert a date to SimPy time (days from start).

        Args:
            target_date: Date to convert

        Returns:
            Number of days from simulation start
        """
        return float((target_date - self.start_date).days)

    def to_date(self, simpy_time: float) -> date:
        """
        Convert SimPy time to date.

        Args:
            simpy_time: Time in days from start

        Returns:
            Corresponding date
        """
        return self.start_date + timedelta(days=int(simpy_time))

    def to_datetime(self, simpy_time: float) -> datetime:
        """
        Convert SimPy time to datetime.

        Args:
            simpy_time: Time in days from start (supports fractional)

        Returns:
            Corresponding datetime
        """
        days = int(simpy_time)
        fraction = simpy_time - days
        base_dt = datetime.combine(
            self.start_date + timedelta(days=days),
            datetime.min.time(),
        )
        return base_dt + timedelta(days=fraction)

    def timeout(self, days: float) -> simpy.Event:
        """
        Create a timeout event for the given number of days.

        Args:
            days: Number of days to wait

        Returns:
            SimPy timeout event
        """
        return self.env.timeout(days)

    def timeout_until(self, target_date: date) -> simpy.Event:
        """
        Create a timeout event until a specific date.

        Args:
            target_date: Date to wait until

        Returns:
            SimPy timeout event
        """
        target_time = self.to_simpy_time(target_date)
        delay = max(0, target_time - self.env.now)
        return self.env.timeout(delay)

    def process(self, generator: Generator) -> simpy.Process:
        """
        Start a SimPy process.

        Args:
            generator: Generator function for the process

        Returns:
            SimPy Process
        """
        return self.env.process(generator)

    def run(self, until: float | None = None) -> Any:
        """
        Run the simulation.

        Args:
            until: Stop time in days (default: full duration)

        Returns:
            Result of simulation run

        Note:
            SimPy has an incompatibility with Pydantic v2's ValidationError.
            SimPy tries to re-create exceptions using `type(exc)(*exc.args)`,
            but Pydantic's ValidationError requires specific keyword arguments.
            We catch TypeError from SimPy's exception handling and try to
            extract the original Pydantic error from the process event.
        """
        if until is None:
            until = self.duration_days
        try:
            return self.env.run(until=until)
        except TypeError as e:
            # Check if this is SimPy failing to re-raise a Pydantic ValidationError
            if "ValidationError.__new__()" in str(e):
                # Try to find the original exception in any pending events
                for event in list(self.env._queue):
                    if hasattr(event, '_value') and isinstance(event._value, PydanticValidationError):
                        # Re-raise with full traceback info
                        raise RuntimeError(
                            f"Pydantic validation error in simulation:\n{event._value}"
                        ) from event._value
                # If we can't find the original, just re-raise with the TypeError
                raise RuntimeError(
                    f"Pydantic validation error occurred in simulation (original error not recoverable): {e}"
                ) from e
            raise

    def run_until_date(self, target_date: date) -> Any:
        """
        Run simulation until a specific date.

        Args:
            target_date: Date to run until

        Returns:
            Result of simulation run
        """
        target_time = self.to_simpy_time(target_date)
        return self.env.run(until=target_time)

    def is_warmup_complete(self, warmup_days: int) -> bool:
        """
        Check if warmup period is complete.

        Uses total elapsed days (including prior runs) so that warmup
        progress is tracked correctly across incremental simulation runs.

        Args:
            warmup_days: Number of warmup days

        Returns:
            True if warmup is complete
        """
        return self.total_elapsed_days >= warmup_days

    def get_progress(self) -> float:
        """
        Get simulation progress as percentage.

        Returns:
            Progress percentage (0-100)
        """
        if self.duration_days == 0:
            return 100.0
        return min(100.0, (self.env.now / self.duration_days) * 100)

    def log_progress(self, interval_days: int = 30) -> None:
        """
        Log simulation progress.

        Should be called periodically from a monitoring process.

        Args:
            interval_days: Logging interval
        """
        logger.info(
            "simulation_progress",
            worker_id=self.worker_id,
            current_date=self.current_date.isoformat(),
            day=int(self.env.now),
            progress_pct=f"{self.get_progress():.1f}%",
        )
