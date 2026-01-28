"""
Base process class for SimPy processes.

Provides common functionality for all simulation processes.
"""

from abc import ABC, abstractmethod
from typing import Generator, Any

from numpy.random import Generator as RNG
from pydantic import ValidationError as PydanticValidationError
import structlog

from brickwell_health.config.models import SimulationConfig
from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.db.writer import BatchWriter
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.reference.loader import ReferenceDataLoader


logger = structlog.get_logger()


def wrap_generator_for_pydantic(gen: Generator, process_name: str) -> Generator:
    """
    Wrap a generator to catch Pydantic ValidationError and convert to RuntimeError.

    SimPy has an incompatibility with Pydantic v2's ValidationError where it tries
    to re-create exceptions using `type(exc)(*exc.args)`, but Pydantic's
    ValidationError requires specific keyword arguments.

    This wrapper catches ValidationError before SimPy can handle it and converts
    it to a RuntimeError with the full error details.

    Args:
        gen: The generator to wrap
        process_name: Name of the process for error messages

    Yields:
        Values from the wrapped generator
    """
    try:
        result = yield from gen
        return result
    except PydanticValidationError as e:
        # Convert to RuntimeError so SimPy can handle it
        raise RuntimeError(
            f"Pydantic validation error in {process_name}:\n{e}"
        ) from e


class BaseProcess(ABC):
    """
    Abstract base class for SimPy simulation processes.

    Provides:
    - Access to simulation environment
    - Access to generators and reference data
    - Batch writer for database writes
    - Common utility methods

    Subclasses must implement the run() method.
    """

    def __init__(
        self,
        sim_env: SimulationEnvironment,
        config: SimulationConfig,
        batch_writer: BatchWriter,
        id_generator: IDGenerator,
        reference: ReferenceDataLoader,
        worker_id: int = 0,
    ):
        """
        Initialize the process.

        Args:
            sim_env: SimPy simulation environment wrapper
            config: Simulation configuration
            batch_writer: Database batch writer
            id_generator: ID generator
            reference: Reference data loader
            worker_id: Worker process identifier
        """
        self.sim_env = sim_env
        self.env = sim_env.env  # SimPy environment for yield
        self.config = config
        self.batch_writer = batch_writer
        self.id_generator = id_generator
        self.reference = reference
        self.worker_id = worker_id
        self.rng = sim_env.rng

        # Statistics tracking
        self._stats: dict[str, int] = {}

    @abstractmethod
    def run(self) -> Generator:
        """
        Main process loop.

        Must be implemented by subclasses. Should yield events to SimPy.

        Example:
            def run(self) -> Generator:
                while True:
                    yield self.env.timeout(1)  # Wait 1 day
                    self.process_day()
        """
        pass

    def start(self) -> Any:
        """
        Start the process.

        Returns:
            SimPy Process

        Note:
            The generator is wrapped to catch Pydantic ValidationError and convert
            it to RuntimeError before SimPy can handle it (SimPy has an incompatibility
            with Pydantic v2's ValidationError constructor).
        """
        process_name = self.__class__.__name__
        wrapped_gen = wrap_generator_for_pydantic(self.run(), process_name)
        return self.sim_env.process(wrapped_gen)

    def increment_stat(self, name: str, value: int = 1) -> None:
        """
        Increment a statistics counter.

        Args:
            name: Statistic name
            value: Amount to increment
        """
        self._stats[name] = self._stats.get(name, 0) + value

    def get_stats(self) -> dict[str, int]:
        """
        Get all statistics.

        Returns:
            Dictionary of statistic names to values
        """
        return self._stats.copy()

    def log_progress(self, event: str, **kwargs: Any) -> None:
        """
        Log a progress event.

        Args:
            event: Event name
            **kwargs: Additional context
        """
        logger.debug(
            event,
            worker_id=self.worker_id,
            sim_day=int(self.sim_env.now),
            sim_date=self.sim_env.current_date.isoformat(),
            **kwargs,
        )

    def annual_rate_to_daily(self, annual_rate: float) -> float:
        """
        Convert annual rate to daily probability.

        Args:
            annual_rate: Annual rate (e.g., 0.08 for 8%)

        Returns:
            Daily probability
        """
        return annual_rate / 365.0

    def poisson_arrival(self, rate: float) -> bool:
        """
        Check if a Poisson arrival occurs.

        Args:
            rate: Daily rate (expected arrivals per day)

        Returns:
            True if arrival should occur
        """
        return self.rng.random() < (1 - (2.718281828 ** -rate))

    def exponential_time(self, mean: float) -> float:
        """
        Sample exponential inter-arrival time.

        Args:
            mean: Mean time between events

        Returns:
            Sampled time
        """
        return self.rng.exponential(mean)

    def uniform_time(self, low: float, high: float) -> float:
        """
        Sample uniform time.

        Args:
            low: Minimum time
            high: Maximum time

        Returns:
            Sampled time
        """
        return self.rng.uniform(low, high)
