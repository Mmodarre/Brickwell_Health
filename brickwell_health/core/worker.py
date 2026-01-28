"""
Worker process for Brickwell Health Simulator.

Entry point for parallel worker processes.
"""

import time
from pathlib import Path
from typing import Any, Generator

import numpy as np
import structlog

from brickwell_health.config.models import SimulationConfig
from brickwell_health.utils.logging import configure_logging
from brickwell_health.core.checkpoint import (
    CheckpointManager,
    create_checkpoint_state,
    restore_from_checkpoint,
)
from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.core.partition import PartitionManager
from brickwell_health.core.shared_state import SharedState
from brickwell_health.core.processes.acquisition import AcquisitionProcess
from brickwell_health.core.processes.policy_lifecycle import PolicyLifecycleProcess
from brickwell_health.core.processes.suspension import SuspensionProcess
from brickwell_health.core.processes.claims import ClaimsProcess
from brickwell_health.core.processes.billing import BillingProcess
from brickwell_health.db.connection import create_engine_for_worker
from brickwell_health.db.writer import BatchWriter
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.reference.loader import ReferenceDataLoader


logger = structlog.get_logger()


class SimulationWorker:
    """
    Worker process for running a partitioned simulation.

    Each worker:
    - Has its own RNG (seeded deterministically)
    - Owns a partition of entities (by UUID)
    - Runs all simulation processes
    - Writes to database using COPY
    """

    def __init__(
        self,
        config: SimulationConfig,
        worker_id: int,
        num_workers: int,
    ):
        """
        Initialize the worker.

        Args:
            config: Simulation configuration
            worker_id: This worker's ID (0 to num_workers-1)
            num_workers: Total number of workers
        """
        self.config = config
        self.worker_id = worker_id
        self.num_workers = num_workers

        # Initialize RNG with deterministic seed
        self.seed = config.seed + worker_id
        self.rng = np.random.default_rng(self.seed)

        # Partition manager
        self.partition = PartitionManager(worker_id, num_workers)

        # Reference data loader
        self.reference = ReferenceDataLoader(config.reference_data_path)

        # Database engine
        self.engine = create_engine_for_worker(config.database, worker_id)

        # Batch writer
        self.batch_writer = BatchWriter(self.engine, config.database.batch_size)

        # ID generator (worker_id ensures unique sequential numbers across workers)
        self.id_generator = IDGenerator(
            self.rng, 
            config.simulation.start_date.year,
            worker_id=worker_id,
        )

        # Simulation environment (created in run)
        self.sim_env: SimulationEnvironment | None = None

        # Shared state for cross-process communication
        self.shared_state: SharedState | None = None

        # Processes (created in run)
        self.acquisition: AcquisitionProcess | None = None
        self.lifecycle: PolicyLifecycleProcess | None = None
        self.suspension: SuspensionProcess | None = None
        self.claims: ClaimsProcess | None = None
        self.billing: BillingProcess | None = None

        # Checkpoint manager for crash recovery
        checkpoint_dir = Path(config.reference_data_path).parent / "checkpoints"
        self.checkpoint_manager = CheckpointManager(checkpoint_dir)
        self.checkpoint_interval_days = config.parallel.checkpoint_interval_minutes / (24 * 60)

        # Statistics
        self._start_time: float = 0
        self._stats: dict[str, Any] = {}

    def run(self) -> dict[str, Any]:
        """
        Run the simulation.

        Returns:
            Dictionary of statistics from the run
        """
        logger.info(
            "worker_starting",
            worker_id=self.worker_id,
            seed=self.seed,
            start_date=self.config.simulation.start_date.isoformat(),
            end_date=self.config.simulation.end_date.isoformat(),
        )

        self._start_time = time.time()

        # Check for existing checkpoint to restore
        checkpoint = self.checkpoint_manager.load(self.worker_id)
        if checkpoint:
            logger.info(
                "restoring_from_checkpoint",
                worker_id=self.worker_id,
                sim_time=checkpoint.get("sim_time"),
            )
            # Restore RNG state
            rng_state = checkpoint.get("rng_state")
            if rng_state:
                self.rng.bit_generator.state = rng_state

            # Restore ID counters
            counters = checkpoint.get("id_counters", {})
            if counters:
                self.id_generator.set_counters(**counters)

        # Create simulation environment
        self.sim_env = SimulationEnvironment(
            start_date=self.config.simulation.start_date,
            end_date=self.config.simulation.end_date,
            rng=self.rng,
            worker_id=self.worker_id,
        )

        # Initialize processes with shared state
        self._init_processes()

        # Start all processes
        self.acquisition.start()
        self.lifecycle.start()
        self.suspension.start()
        self.claims.start()
        self.billing.start()

        # Start checkpoint process
        self.sim_env.process(self._checkpoint_process())

        # Run simulation
        self.sim_env.run()

        # Flush remaining data
        self.batch_writer.flush_all()

        # Clean up checkpoint on successful completion
        self.checkpoint_manager.delete(self.worker_id)

        # Collect statistics
        elapsed = time.time() - self._start_time

        self._stats = {
            "worker_id": self.worker_id,
            "elapsed_seconds": elapsed,
            "simulation_days": self.sim_env.duration_days,
            "days_per_second": self.sim_env.duration_days / elapsed if elapsed > 0 else 0,
            "database_writes": self.batch_writer.get_all_counts(),
            "acquisition_stats": self.acquisition.get_stats() if self.acquisition else {},
            "lifecycle_stats": self.lifecycle.get_stats() if self.lifecycle else {},
            "suspension_stats": self.suspension.get_stats() if self.suspension else {},
            "claims_stats": self.claims.get_stats() if self.claims else {},
            "billing_stats": self.billing.get_stats() if self.billing else {},
            "shared_state": self.shared_state.get_stats() if self.shared_state else {},
        }

        logger.info(
            "worker_completed",
            worker_id=self.worker_id,
            elapsed_seconds=f"{elapsed:.1f}",
            days_per_second=f"{self._stats['days_per_second']:.1f}",
        )

        return self._stats

    def _checkpoint_process(self) -> Generator:
        """
        Periodic checkpoint save process.

        Saves simulation state at configured intervals for crash recovery.
        """
        while True:
            yield self.sim_env.env.timeout(self.checkpoint_interval_days)

            # Create checkpoint state
            state = create_checkpoint_state(
                sim_time=self.sim_env.now,
                id_counters=self.id_generator.get_counters(),
                rng_state=self.rng.bit_generator.state,
                active_policies=len(self.shared_state.active_policies) if self.shared_state else 0,
                active_members=len(self.shared_state.policy_members) if self.shared_state else 0,
            )

            # Save checkpoint
            self.checkpoint_manager.save(state, self.worker_id)

            logger.debug(
                "checkpoint_saved",
                worker_id=self.worker_id,
                sim_day=int(self.sim_env.now),
                active_policies=state["active_policies"],
                active_members=state["active_members"],
            )

    def _init_processes(self) -> None:
        """Initialize all simulation processes with shared state."""
        # Create shared state for cross-process communication
        self.shared_state = SharedState()

        common_args = {
            "sim_env": self.sim_env,
            "config": self.config,
            "batch_writer": self.batch_writer,
            "id_generator": self.id_generator,
            "reference": self.reference,
            "worker_id": self.worker_id,
        }

        # Acquisition populates shared state
        self.acquisition = AcquisitionProcess(
            **common_args,
            shared_state=self.shared_state,
        )

        # Suspension process for handling suspensions and reactivations
        self.suspension = SuspensionProcess(
            **common_args,
            active_policies=self.shared_state.active_policies,
        )

        # Lifecycle uses active_policies from shared state and delegates to suspension
        self.lifecycle = PolicyLifecycleProcess(
            **common_args,
            active_policies=self.shared_state.active_policies,
            suspension_process=self.suspension,
        )

        # Claims uses policy_members and waiting_periods from shared state
        # Also needs shared_state to check policy suspension status
        self.claims = ClaimsProcess(
            **common_args,
            policy_members=self.shared_state.policy_members,
            waiting_periods=self.shared_state.waiting_periods,
            shared_state=self.shared_state,
        )

        # Billing uses active_policies and pending_invoices from shared state
        self.billing = BillingProcess(
            **common_args,
            active_policies=self.shared_state.active_policies,
            pending_invoices=self.shared_state.pending_invoices,
            shared_state=self.shared_state,
        )


def run_worker(
    config: SimulationConfig,
    worker_id: int,
    num_workers: int,
    log_level: str = "INFO",
) -> dict[str, Any]:
    """
    Entry point for running a worker in a separate process.

    Args:
        config: Simulation configuration
        worker_id: Worker ID
        num_workers: Total workers
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Worker statistics
    """
    # Configure logging for this worker process
    # Each worker is a separate process and needs its own logging config
    configure_logging(level=log_level)

    worker = SimulationWorker(config, worker_id, num_workers)
    return worker.run()
