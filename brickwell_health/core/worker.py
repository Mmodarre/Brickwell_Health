"""
Worker process for Brickwell Health Simulator.

Entry point for parallel worker processes.
Supports both fresh runs and resume from checkpoint.
"""

import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Generator
from uuid import UUID

import numpy as np
import structlog

from brickwell_health.config.models import SimulationConfig
from brickwell_health.utils.logging import configure_logging
from brickwell_health.core.checkpoint import (
    CheckpointManager,
    create_checkpoint_state,
    restore_from_checkpoint,
)
from brickwell_health.core.checkpoint_v2 import (
    CheckpointManagerV2,
    CheckpointNotFoundError,
    CheckpointCorruptedError,
    get_checkpoint_dates,
    restore_shared_state_from_checkpoint,
    restore_crm_process_state,
    restore_billing_retry_state,
    restore_digital_process_state,
)
from brickwell_health.core.state_reconstruction import (
    reconstruct_shared_state_from_db,
    load_cumulative_usage,
    load_active_suspensions,
)
from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.core.partition import PartitionManager
from brickwell_health.core.shared_state import SharedState
from brickwell_health.core.processes.acquisition import AcquisitionProcess
from brickwell_health.core.processes.policy_lifecycle import PolicyLifecycleProcess
from brickwell_health.core.processes.member_lifecycle import MemberLifecycleProcess
from brickwell_health.core.processes.suspension import SuspensionProcess
from brickwell_health.core.processes.claims import ClaimsProcess
from brickwell_health.core.processes.billing import BillingProcess
from brickwell_health.core.processes.crm import CRMProcess
from brickwell_health.core.processes.communication import CommunicationProcess
from brickwell_health.core.processes.digital import DigitalBehaviorProcess
from brickwell_health.core.processes.survey import SurveyProcess
from brickwell_health.core.processes.nba import NBAActionProcess
from brickwell_health.db.connection import create_engine_for_worker
from brickwell_health.db.protocol import BatchWriterProtocol
from brickwell_health.db.writer import BatchWriter
from brickwell_health.domain.nba import (
    NBAActionWithRecommendation,
    BusinessIssue,
    ActionCategory,
    NBAChannel,
    RecommendationStatus,
)
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
    
    Supports two modes:
    - Fresh run: Start from scratch with config dates
    - Resume mode: Continue from checkpoint with state reconstruction
    """

    def __init__(
        self,
        config: SimulationConfig,
        worker_id: int,
        num_workers: int,
        resume_mode: bool = False,
    ):
        """
        Initialize the worker.

        Args:
            config: Simulation configuration
            worker_id: This worker's ID (0 to num_workers-1)
            num_workers: Total number of workers
            resume_mode: If True, resume from checkpoint instead of fresh start
        """
        self.config = config
        self.worker_id = worker_id
        self.num_workers = num_workers
        self.resume_mode = resume_mode

        # Initialize RNG with deterministic seed
        self.seed = config.seed + worker_id
        self.rng = np.random.default_rng(self.seed)

        # Partition manager
        self.partition = PartitionManager(worker_id, num_workers)

        # Database engine (needed by reference loader)
        self.engine = create_engine_for_worker(config.database, worker_id)

        # Reference data loader (reads from database, falls back to JSON)
        self.reference = ReferenceDataLoader(
            engine=self.engine,
            json_fallback_path=config.reference_data_path
        )

        # Batch writer (optionally wrapped with streaming)
        base_writer = BatchWriter(self.engine, config.database.batch_size)
        self.batch_writer: BatchWriterProtocol = self._maybe_wrap_with_streaming(base_writer)

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
        self.member_lifecycle: MemberLifecycleProcess | None = None
        self.suspension: SuspensionProcess | None = None
        self.claims: ClaimsProcess | None = None
        self.billing: BillingProcess | None = None
        self.crm: CRMProcess | None = None
        self.communication: CommunicationProcess | None = None
        self.digital: DigitalBehaviorProcess | None = None
        self.survey: SurveyProcess | None = None
        self.nba_process: NBAActionProcess | None = None

        # Checkpoint managers (v1 for legacy, v2 for incremental runs)
        checkpoint_dir = Path(config.reference_data_path).parent / "checkpoints"
        self.checkpoint_manager = CheckpointManager(checkpoint_dir)
        self.checkpoint_manager_v2 = CheckpointManagerV2(checkpoint_dir)
        self.checkpoint_interval_days = config.parallel.checkpoint_interval_minutes / (24 * 60)

        # Track original start date for resume
        self._original_start_date: date | None = None

        # Statistics
        self._start_time: float = 0
        self._stats: dict[str, Any] = {}

    def _maybe_wrap_with_streaming(self, base_writer: BatchWriter) -> BatchWriterProtocol:
        """
        Conditionally wrap BatchWriter with StreamingBatchWriter.

        Returns base_writer unchanged if streaming is disabled.
        """
        if not self.config.streaming.enabled:
            return base_writer

        from brickwell_health.streaming.factory import create_publisher
        from brickwell_health.streaming.topic_resolver import TopicResolver
        from brickwell_health.streaming.wrapper import StreamingBatchWriter

        publisher = create_publisher(self.config.streaming, self.worker_id)
        topic_resolver = TopicResolver(self.config.streaming)

        def get_sim_datetime() -> date:
            if self.sim_env:
                return self.sim_env.current_date
            return self.config.simulation.start_date

        wrapper = StreamingBatchWriter(
            inner=base_writer,
            publisher=publisher,
            topic_resolver=topic_resolver,
            tables=set(self.config.streaming.tables),
            worker_id=self.worker_id,
            fail_open=self.config.streaming.fail_open,
            get_sim_datetime=get_sim_datetime,
            flush_interval=self.config.streaming.flush_interval_seconds,
            batch_size=self.config.streaming.batch_size,
        )

        logger.info(
            "streaming_enabled",
            worker_id=self.worker_id,
            backend=self.config.streaming.backend,
            tables=self.config.streaming.tables,
        )

        return wrapper

    def _close_streaming(self) -> None:
        """Close streaming wrapper if active."""
        from brickwell_health.streaming.wrapper import StreamingBatchWriter

        if isinstance(self.batch_writer, StreamingBatchWriter):
            self.batch_writer.close()

    def _get_streaming_stats(self) -> dict[str, int]:
        """Get streaming statistics if streaming is active."""
        from brickwell_health.streaming.wrapper import StreamingBatchWriter

        if isinstance(self.batch_writer, StreamingBatchWriter):
            return self.batch_writer.get_streaming_stats()
        return {}

    def run(self) -> dict[str, Any]:
        """
        Run the simulation.

        Returns:
            Dictionary of statistics from the run

        Raises:
            CheckpointNotFoundError: If resume_mode is True but no checkpoint exists
            CheckpointCorruptedError: If checkpoint file is corrupted
        """
        if self.resume_mode:
            return self._run_resume_mode()
        else:
            return self._run_fresh_mode()

    def _run_fresh_mode(self) -> dict[str, Any]:
        """
        Run simulation from scratch (original behavior).
        
        Returns:
            Dictionary of statistics from the run
        """
        logger.info(
            "worker_starting",
            worker_id=self.worker_id,
            seed=self.seed,
            start_date=self.config.simulation.start_date.isoformat(),
            end_date=self.config.simulation.end_date.isoformat(),
            mode="fresh",
        )

        self._start_time = time.time()
        self._original_start_date = self.config.simulation.start_date

        # Check for existing checkpoint to restore (v1 crash recovery)
        checkpoint = self.checkpoint_manager.load(self.worker_id)
        if checkpoint:
            logger.info(
                "restoring_from_checkpoint_v1",
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
        self._start_all_processes()

        # Start checkpoint process (saves v2 checkpoints)
        self.sim_env.process(self._checkpoint_process_v2())

        # Run simulation
        self.sim_env.run()

        # Flush remaining data
        self.batch_writer.flush_all()

        # Close streaming wrapper (drains background thread)
        self._close_streaming()

        # Save final checkpoint for potential resume
        self._save_final_checkpoint()

        # Clean up v1 checkpoint on successful completion
        self.checkpoint_manager.delete(self.worker_id)

        return self._collect_stats()

    def _run_resume_mode(self) -> dict[str, Any]:
        """
        Resume simulation from checkpoint.
        
        Reconstructs state from database and checkpoint, then continues
        simulation from checkpoint date to config end_date.
        
        Returns:
            Dictionary of statistics from the run
            
        Raises:
            CheckpointNotFoundError: If no checkpoint exists
            CheckpointCorruptedError: If checkpoint is corrupted
        """
        logger.info(
            "worker_starting",
            worker_id=self.worker_id,
            seed=self.seed,
            end_date=self.config.simulation.end_date.isoformat(),
            mode="resume",
        )

        self._start_time = time.time()

        # Load checkpoint (strict - fail if not found)
        checkpoint = self.checkpoint_manager_v2.load_checkpoint_strict(self.worker_id)

        # Extract dates
        checkpoint_date, original_start_date = get_checkpoint_dates(checkpoint)
        self._original_start_date = original_start_date

        logger.info(
            "resuming_from_checkpoint",
            worker_id=self.worker_id,
            checkpoint_date=checkpoint_date.isoformat(),
            original_start_date=original_start_date.isoformat(),
            end_date=self.config.simulation.end_date.isoformat(),
        )

        # Restore RNG state
        rng_state = checkpoint.get("rng_state")
        if rng_state:
            self.rng.bit_generator.state = rng_state

        # Restore ID counters
        counters = checkpoint.get("id_counters", {})
        if counters:
            self.id_generator.set_counters(**counters)

        # Create simulation environment starting from CHECKPOINT date
        # This sidesteps SimPy's inability to set time directly.
        # elapsed_days_offset tracks how many days were completed in prior
        # runs so that warmup/threshold checks work across incremental runs.
        elapsed_days_offset = (checkpoint_date - original_start_date).days
        self.sim_env = SimulationEnvironment(
            start_date=checkpoint_date,  # Resume from checkpoint date
            end_date=self.config.simulation.end_date,
            rng=self.rng,
            worker_id=self.worker_id,
            elapsed_days_offset=elapsed_days_offset,
        )

        # Reconstruct SharedState from database (filtered by worker partition)
        self.shared_state = reconstruct_shared_state_from_db(
            self.engine, checkpoint_date, self.worker_id, self.config.parallel.num_workers
        )

        # Restore non-reconstructable state from checkpoint
        restore_shared_state_from_checkpoint(self.shared_state, checkpoint)

        # Initialize processes with restored shared state
        self._init_processes_for_resume()

        # Restore process-specific state
        self._restore_process_state(checkpoint)

        # Start all processes
        self._start_all_processes()

        # Start checkpoint process
        self.sim_env.process(self._checkpoint_process_v2())

        # Run simulation from checkpoint to end
        self.sim_env.run()

        # Flush remaining data
        self.batch_writer.flush_all()

        # Close streaming wrapper (drains background thread)
        self._close_streaming()

        # Save final checkpoint for potential future resume
        self._save_final_checkpoint()

        return self._collect_stats()

    def _start_all_processes(self) -> None:
        """Start all simulation processes."""
        self.acquisition.start()
        self.lifecycle.start()
        self.member_lifecycle.start()
        self.suspension.start()
        self.claims.start()
        self.billing.start()

        if self.crm:
            self.crm.start()
        if self.communication:
            self.communication.start()
        if self.digital:
            self.digital.start()
        if self.survey:
            self.survey.start()
        if self.nba_process:
            self.nba_process.start()

    def _restore_process_state(self, checkpoint: dict[str, Any]) -> None:
        """
        Restore process-specific state from checkpoint.
        
        Args:
            checkpoint: Loaded checkpoint dictionary
        """
        # Restore CRM process state
        if self.crm and checkpoint.get("crm_pending_cases"):
            restore_crm_process_state(self.crm, checkpoint)

        # Restore billing retry state
        if self.billing and checkpoint.get("billing_retry_state"):
            restore_billing_retry_state(
                self.shared_state.pending_invoices, checkpoint
            )

        # Restore digital process state
        if self.digital and checkpoint.get("digital_processed_triggers"):
            restore_digital_process_state(self.digital, checkpoint)

        # Restore cumulative usage for claims process (filtered by worker partition)
        if self.claims:
            with self.engine.connect() as conn:
                checkpoint_date = date.fromisoformat(checkpoint["checkpoint_date"])
                self.claims.cumulative_usage = load_cumulative_usage(
                    conn, checkpoint_date, self.worker_id, self.config.parallel.num_workers
                )

        # Restore active suspensions for suspension process (filtered by worker partition)
        if self.suspension:
            with self.engine.connect() as conn:
                checkpoint_date = date.fromisoformat(checkpoint["checkpoint_date"])
                active_suspensions = load_active_suspensions(
                    conn, checkpoint_date, self.worker_id, self.config.parallel.num_workers
                )
                self.suspension.active_suspensions = active_suspensions

        logger.info(
            "process_state_restored",
            worker_id=self.worker_id,
            crm_restored=self.crm is not None,
            billing_restored=self.billing is not None,
            digital_restored=self.digital is not None,
        )

    def _init_processes_for_resume(self) -> None:
        """
        Initialize processes for resume mode.
        
        Similar to _init_processes but uses already-populated shared_state.
        """
        common_args = {
            "sim_env": self.sim_env,
            "config": self.config,
            "batch_writer": self.batch_writer,
            "id_generator": self.id_generator,
            "reference": self.reference,
            "worker_id": self.worker_id,
        }

        # Acquisition (still needed to generate new policies)
        self.acquisition = AcquisitionProcess(
            **common_args,
            shared_state=self.shared_state,
        )

        # Suspension process
        # Pass shared_state and engine to enable in-memory state updates
        # when reactivating suspended policies during resume
        self.suspension = SuspensionProcess(
            **common_args,
            active_policies=self.shared_state.active_policies,
            shared_state=self.shared_state,
            engine=self.engine,
        )

        # Policy lifecycle
        self.lifecycle = PolicyLifecycleProcess(
            **common_args,
            active_policies=self.shared_state.active_policies,
            suspension_process=self.suspension,
            shared_state=self.shared_state,
        )

        # Member lifecycle
        self.member_lifecycle = MemberLifecycleProcess(
            **common_args,
            shared_state=self.shared_state,
        )

        # Claims process
        self.claims = ClaimsProcess(
            **common_args,
            policy_members=self.shared_state.policy_members,
            waiting_periods=self.shared_state.waiting_periods,
            shared_state=self.shared_state,
        )

        # Billing process
        self.billing = BillingProcess(
            **common_args,
            active_policies=self.shared_state.active_policies,
            pending_invoices=self.shared_state.pending_invoices,
            shared_state=self.shared_state,
        )

        # CRM process (conditionally enabled)
        crm_config = getattr(self.config, "crm", None)
        crm_enabled = crm_config.enabled if crm_config and hasattr(crm_config, "enabled") else True
        if crm_enabled:
            self.crm = CRMProcess(
                **common_args,
                shared_state=self.shared_state,
            )

        # Communication process
        comm_config = getattr(self.config, "communication", None)
        comm_enabled = comm_config.enabled if comm_config and hasattr(comm_config, "enabled") else True
        if comm_enabled:
            self.communication = CommunicationProcess(
                **common_args,
                shared_state=self.shared_state,
            )

        # Digital behavior process
        digital_config = getattr(self.config, "digital", None)
        digital_enabled = digital_config.enabled if digital_config and hasattr(digital_config, "enabled") else True
        if digital_enabled:
            self.digital = DigitalBehaviorProcess(
                **common_args,
                shared_state=self.shared_state,
            )

        # Survey process
        survey_config = getattr(self.config, "survey", None)
        survey_enabled = survey_config.enabled if survey_config and hasattr(survey_config, "enabled") else True
        if survey_enabled:
            self.survey = SurveyProcess(
                **common_args,
                shared_state=self.shared_state,
            )

        # Load NBA recommendations and create NBA process if NBA is enabled
        nba_config = getattr(self.config, "nba", None)
        nba_enabled = nba_config.enabled if nba_config and hasattr(nba_config, "enabled") else True
        if nba_enabled:
            self._load_nba_recommendations()
            self.nba_process = NBAActionProcess(
                **common_args,
                shared_state=self.shared_state,
            )

    def _save_final_checkpoint(self) -> None:
        """Save final checkpoint at end of simulation for potential future resume."""
        if not self.shared_state or not self.sim_env:
            return

        self.checkpoint_manager_v2.save_full_checkpoint(
            worker_id=self.worker_id,
            sim_time=self.sim_env.now,
            checkpoint_date=self.sim_env.current_date,
            original_start_date=self._original_start_date or self.config.simulation.start_date,
            id_counters=self.id_generator.get_counters(),
            rng_state=self.rng.bit_generator.state,
            shared_state=self.shared_state,
            crm_process=self.crm,
            billing_process=self.billing,
            digital_process=self.digital,
        )

        logger.info(
            "final_checkpoint_saved",
            worker_id=self.worker_id,
            checkpoint_date=self.sim_env.current_date.isoformat(),
        )

    def _collect_stats(self) -> dict[str, Any]:
        """Collect and return simulation statistics."""
        elapsed = time.time() - self._start_time

        self._stats = {
            "worker_id": self.worker_id,
            "elapsed_seconds": elapsed,
            "simulation_days": self.sim_env.duration_days if self.sim_env else 0,
            "days_per_second": self.sim_env.duration_days / elapsed if elapsed > 0 and self.sim_env else 0,
            "database_writes": self.batch_writer.get_all_counts(),
            "acquisition_stats": self.acquisition.get_stats() if self.acquisition else {},
            "lifecycle_stats": self.lifecycle.get_stats() if self.lifecycle else {},
            "member_lifecycle_stats": self.member_lifecycle.get_stats() if self.member_lifecycle else {},
            "suspension_stats": self.suspension.get_stats() if self.suspension else {},
            "claims_stats": self.claims.get_stats() if self.claims else {},
            "billing_stats": self.billing.get_stats() if self.billing else {},
            "crm_stats": self.crm.get_stats() if self.crm else {},
            "communication_stats": self.communication.get_stats() if self.communication else {},
            "digital_stats": self.digital.get_stats() if self.digital else {},
            "survey_stats": self.survey.get_stats() if self.survey else {},
            "nba_stats": self.nba_process.get_stats() if self.nba_process else {},
            "shared_state": self.shared_state.get_stats() if self.shared_state else {},
            "resume_mode": self.resume_mode,
            "streaming_stats": self._get_streaming_stats(),
        }

        logger.info(
            "worker_completed",
            worker_id=self.worker_id,
            elapsed_seconds=f"{elapsed:.1f}",
            days_per_second=f"{self._stats['days_per_second']:.1f}",
            mode="resume" if self.resume_mode else "fresh",
        )

        return self._stats

    def _checkpoint_process(self) -> Generator:
        """
        Periodic checkpoint save process (v1 - legacy).

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

    def _checkpoint_process_v2(self) -> Generator:
        """
        Periodic checkpoint save process (v2 - full state).

        Saves full simulation state for incremental runs.
        """
        while True:
            yield self.sim_env.env.timeout(self.checkpoint_interval_days)

            if not self.shared_state:
                continue

            self.checkpoint_manager_v2.save_full_checkpoint(
                worker_id=self.worker_id,
                sim_time=self.sim_env.now,
                checkpoint_date=self.sim_env.current_date,
                original_start_date=self._original_start_date or self.config.simulation.start_date,
                id_counters=self.id_generator.get_counters(),
                rng_state=self.rng.bit_generator.state,
                shared_state=self.shared_state,
                crm_process=self.crm,
                billing_process=self.billing,
                digital_process=self.digital,
            )

            logger.debug(
                "checkpoint_v2_saved",
                worker_id=self.worker_id,
                sim_day=int(self.sim_env.now),
                checkpoint_date=self.sim_env.current_date.isoformat(),
                active_policies=len(self.shared_state.active_policies),
            )

    def _init_processes(self) -> None:
        """Initialize all simulation processes with shared state."""
        # Create shared state for cross-process communication
        self.shared_state = SharedState()

        # Initialize fraud-prone providers if fraud is enabled
        self._init_fraud_providers()

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
            shared_state=self.shared_state,
        )

        # Member lifecycle handles demographic changes (address, phone, death, etc.)
        self.member_lifecycle = MemberLifecycleProcess(
            **common_args,
            shared_state=self.shared_state,
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

        # CRM process for interactions, cases, and complaints (conditionally enabled)
        crm_config = getattr(self.config, "crm", None)
        crm_enabled = crm_config.enabled if crm_config and hasattr(crm_config, "enabled") else True
        if crm_enabled:
            self.crm = CRMProcess(
                **common_args,
                shared_state=self.shared_state,
            )

        # Communication process for transactional/marketing communications (conditionally enabled)
        comm_config = getattr(self.config, "communication", None)
        comm_enabled = comm_config.enabled if comm_config and hasattr(comm_config, "enabled") else True
        if comm_enabled:
            self.communication = CommunicationProcess(
                **common_args,
                shared_state=self.shared_state,
            )

        # Digital behavior process for web sessions and events (conditionally enabled)
        digital_config = getattr(self.config, "digital", None)
        digital_enabled = digital_config.enabled if digital_config and hasattr(digital_config, "enabled") else True
        if digital_enabled:
            self.digital = DigitalBehaviorProcess(
                **common_args,
                shared_state=self.shared_state,
            )

        # Survey process for NPS/CSAT surveys (conditionally enabled)
        survey_config = getattr(self.config, "survey", None)
        survey_enabled = survey_config.enabled if survey_config and hasattr(survey_config, "enabled") else True
        if survey_enabled:
            self.survey = SurveyProcess(
                **common_args,
                shared_state=self.shared_state,
            )

        # Load NBA recommendations and create NBA process if NBA is enabled
        nba_config = getattr(self.config, "nba", None)
        nba_enabled = nba_config.enabled if nba_config and hasattr(nba_config, "enabled") else True
        if nba_enabled:
            self._load_nba_recommendations()
            self.nba_process = NBAActionProcess(
                **common_args,
                shared_state=self.shared_state,
            )

    def _init_fraud_providers(self) -> None:
        """
        Flag a subset of providers as fraud-prone.

        Called once during worker initialization. Uses the worker's RNG
        for deterministic selection.
        """
        fraud_config = getattr(self.config, "fraud", None)
        if not fraud_config or not fraud_config.enabled:
            return

        providers = self.reference.get_providers(active_only=True)
        if not providers:
            return

        fraud_rate = fraud_config.fraud_prone_provider_rate
        for provider in providers:
            provider_id = provider.get("provider_id")
            if provider_id and self.rng.random() < fraud_rate:
                self.shared_state.fraud_prone_providers[provider_id] = True

        logger.info(
            "fraud_providers_initialized",
            worker_id=self.worker_id,
            total_providers=len(providers),
            fraud_prone=len(self.shared_state.fraud_prone_providers),
        )

    def _load_nba_recommendations(self, as_of_date: date | None = None) -> None:
        """
        Load pending NBA recommendations from database into shared state.

        Queries the v_nba_pending_actions view for recommendations that:
        - Are valid for the current simulation date
        - Have status 'pending' or 'scheduled'
        - Belong to members in this worker's partition

        Args:
            as_of_date: Date to filter recommendations by (defaults to simulation start/current date)
        """
        if not self.shared_state or not self.sim_env:
            return

        # Use provided date or current simulation date
        filter_date = as_of_date or self.sim_env.current_date

        # Query pending recommendations for this worker's partition
        # Uses the v_nba_pending_actions view which joins catalog and recommendations
        query = """
            SELECT 
                r.recommendation_id,
                r.batch_id,
                r.batch_date,
                r.member_id,
                r.policy_id,
                r.action_id,
                r.propensity_score,
                r.urgency_score,
                r.business_value_score,
                r.priority_rank,
                r.final_score,
                r.trigger_reason,
                r.trigger_signals,
                r.model_version,
                r.valid_from,
                r.valid_until,
                r.status,
                c.action_code,
                c.action_name,
                c.business_issue,
                c.action_category,
                c.channel,
                c.description,
                c.eligibility_rules,
                c.suitability_rules,
                c.base_business_value,
                c.probability_multiplier,
                c.cooldown_days,
                c.max_attempts
            FROM nba_action_recommendation r
            JOIN nba_action_catalog c ON r.action_id = c.action_id
            WHERE r.status IN ('pending', 'scheduled')
              AND r.valid_from <= %s
              AND r.valid_until >= %s
              AND c.is_active = TRUE
              AND MOD(
                  ('x' || SUBSTRING(r.member_id::text, 1, 8))::bit(32)::int,
                  %s
              ) = %s
            ORDER BY r.final_score DESC, r.priority_rank ASC
        """

        recommendations = []

        try:
            with self.engine.connect() as conn:
                raw_conn = conn.connection.dbapi_connection
                with raw_conn.cursor() as cursor:
                    cursor.execute(
                        query,
                        (filter_date, filter_date, self.num_workers, self.worker_id),
                    )
                    rows = cursor.fetchall()

                    for row in rows:
                        # Convert row to NBAActionWithRecommendation
                        rec = NBAActionWithRecommendation(
                            # Recommendation fields
                            recommendation_id=row[0],
                            batch_id=row[1],
                            batch_date=row[2],
                            member_id=row[3],
                            policy_id=row[4],
                            action_id=row[5],
                            propensity_score=Decimal(str(row[6])) if row[6] else None,
                            urgency_score=Decimal(str(row[7])) if row[7] else None,
                            business_value_score=Decimal(str(row[8])) if row[8] else None,
                            priority_rank=row[9],
                            final_score=Decimal(str(row[10])) if row[10] else None,
                            trigger_reason=row[11],
                            trigger_signals=row[12],
                            model_version=row[13],
                            valid_from=row[14],
                            valid_until=row[15],
                            status=RecommendationStatus(row[16]),
                            # Catalog fields
                            action_code=row[17],
                            action_name=row[18],
                            business_issue=BusinessIssue(row[19]),
                            action_category=ActionCategory(row[20]),
                            channel=NBAChannel(row[21]),
                            description=row[22],
                            eligibility_rules=row[23],
                            suitability_rules=row[24],
                            base_business_value=Decimal(str(row[25])) if row[25] else None,
                            probability_multiplier=Decimal(str(row[26])) if row[26] else Decimal("1.0"),
                            cooldown_days=row[27] or 30,
                            max_attempts=row[28] or 3,
                        )
                        recommendations.append(rec)

            # Add to shared state queue
            self.shared_state.add_nba_recommendations(recommendations)

            logger.info(
                "nba_recommendations_loaded",
                worker_id=self.worker_id,
                count=len(recommendations),
                as_of_date=filter_date.isoformat(),
            )

        except Exception as e:
            # Log error but don't fail worker startup
            # NBA table might not exist in fresh databases
            logger.warning(
                "nba_recommendations_load_failed",
                worker_id=self.worker_id,
                error=str(e),
                as_of_date=filter_date.isoformat(),
            )


def run_worker(
    config: SimulationConfig,
    worker_id: int,
    num_workers: int,
    log_level: str = "INFO",
    resume_mode: bool = False,
) -> dict[str, Any]:
    """
    Entry point for running a worker in a separate process.

    Args:
        config: Simulation configuration
        worker_id: Worker ID
        num_workers: Total workers
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        resume_mode: If True, resume from checkpoint instead of fresh start

    Returns:
        Worker statistics
        
    Raises:
        CheckpointNotFoundError: If resume_mode is True but no checkpoint exists
        CheckpointCorruptedError: If checkpoint file is corrupted
    """
    # Configure logging for this worker process
    # Each worker is a separate process and needs its own logging config
    configure_logging(level=log_level)

    worker = SimulationWorker(config, worker_id, num_workers, resume_mode=resume_mode)
    return worker.run()
