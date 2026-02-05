"""
Parallel runner for Brickwell Health Simulator.

Orchestrates multiple worker processes for parallel simulation.
Supports both fresh runs and resume from checkpoint.
"""

import multiprocessing
import time
from pathlib import Path
from typing import Any

import structlog
from sqlalchemy import text

from brickwell_health.config.models import SimulationConfig
from brickwell_health.core.worker import run_worker
from brickwell_health.core.checkpoint_v2 import (
    CheckpointManagerV2,
    CheckpointNotFoundError,
)
from brickwell_health.db.connection import create_engine_from_config


logger = structlog.get_logger()


class ParallelRunner:
    """
    Orchestrates parallel simulation across multiple worker processes.

    Uses multiprocessing to run workers in parallel, each handling
    a partition of entities.

    Supports two modes:
    - Fresh run: Start simulation from scratch
    - Resume mode: Continue from checkpoint

    Usage:
        runner = ParallelRunner(config)
        results = runner.run()  # Fresh run
        results = runner.run(resume=True)  # Resume from checkpoint
    """

    def __init__(self, config: SimulationConfig, log_level: str = "INFO"):
        """
        Initialize the parallel runner.

        Args:
            config: Simulation configuration
            log_level: Log level for worker processes
        """
        self.config = config
        self.num_workers = config.parallel.num_workers
        self.log_level = log_level
        
        # Checkpoint manager for verifying checkpoints exist
        checkpoint_dir = Path(config.reference_data_path).parent / "checkpoints"
        self.checkpoint_manager = CheckpointManagerV2(checkpoint_dir)

    def run(self, resume: bool = False) -> dict[str, Any]:
        """
        Run the simulation with parallel workers.

        Args:
            resume: If True, resume from checkpoint instead of fresh start

        Returns:
            Aggregated results from all workers
            
        Raises:
            CheckpointNotFoundError: If resume=True but checkpoints missing
        """
        mode = "resume" if resume else "fresh"
        
        # Verify checkpoints exist for all workers if resuming
        if resume:
            self._verify_checkpoints_exist()
        
        logger.info(
            "parallel_run_starting",
            num_workers=self.num_workers,
            start_date=self.config.simulation.start_date.isoformat(),
            end_date=self.config.simulation.end_date.isoformat(),
            mode=mode,
        )

        start_time = time.time()

        # Create worker arguments (including resume_mode)
        worker_args = [
            (self.config, worker_id, self.num_workers, self.log_level, resume)
            for worker_id in range(self.num_workers)
        ]

        # Run workers in parallel
        with multiprocessing.Pool(self.num_workers) as pool:
            results = pool.starmap(run_worker, worker_args)

        # Aggregate results
        elapsed = time.time() - start_time
        aggregated = self._aggregate_results(results, elapsed)
        aggregated["mode"] = mode

        # Enrich survey contexts with historical data (post-simulation)
        self._enrich_survey_contexts()

        # Process pending surveys with LLM (if enabled)
        self._process_llm_surveys()

        logger.info(
            "parallel_run_completed",
            elapsed_seconds=f"{elapsed:.1f}",
            total_days=aggregated["total_simulation_days"],
            avg_days_per_second=f"{aggregated['avg_days_per_second']:.1f}",
            mode=mode,
        )

        return aggregated

    def run_sequential(self, resume: bool = False) -> dict[str, Any]:
        """
        Run simulation sequentially (for debugging).

        Args:
            resume: If True, resume from checkpoint instead of fresh start

        Returns:
            Aggregated results
            
        Raises:
            CheckpointNotFoundError: If resume=True but checkpoints missing
        """
        mode = "resume" if resume else "fresh"
        
        # Verify checkpoints exist for all workers if resuming
        if resume:
            self._verify_checkpoints_exist()
        
        logger.info(
            "sequential_run_starting",
            num_workers=self.num_workers,
            mode=mode,
        )

        start_time = time.time()

        results = []
        for worker_id in range(self.num_workers):
            result = run_worker(
                self.config, worker_id, self.num_workers, self.log_level, resume
            )
            results.append(result)

        elapsed = time.time() - start_time
        aggregated = self._aggregate_results(results, elapsed)
        aggregated["mode"] = mode

        # Enrich survey contexts with historical data (post-simulation)
        self._enrich_survey_contexts()

        # Process pending surveys with LLM (if enabled)
        self._process_llm_surveys()

        logger.info(
            "sequential_run_completed",
            elapsed_seconds=f"{elapsed:.1f}",
            mode=mode,
        )

        return aggregated

    def _verify_checkpoints_exist(self) -> None:
        """
        Verify that checkpoints exist for all workers.
        
        Raises:
            CheckpointNotFoundError: If any worker is missing a checkpoint
        """
        missing_workers = []
        for worker_id in range(self.num_workers):
            if not self.checkpoint_manager.has_checkpoint(worker_id):
                missing_workers.append(worker_id)
        
        if missing_workers:
            raise CheckpointNotFoundError(
                f"Cannot resume: missing checkpoints for workers {missing_workers}. "
                f"Run a fresh simulation first to create checkpoints."
            )
        
        logger.info(
            "checkpoints_verified",
            num_workers=self.num_workers,
            all_present=True,
        )

    def _aggregate_results(
        self,
        results: list[dict[str, Any]],
        total_elapsed: float,
    ) -> dict[str, Any]:
        """
        Aggregate results from all workers.

        Args:
            results: List of worker result dictionaries
            total_elapsed: Total elapsed time

        Returns:
            Aggregated statistics
        """
        # Sum database write counts
        total_writes: dict[str, int] = {}
        for result in results:
            for table, count in result.get("database_writes", {}).items():
                total_writes[table] = total_writes.get(table, 0) + count

        # Sum acquisition stats
        total_applications = sum(
            r.get("acquisition_stats", {}).get("applications_submitted", 0)
            for r in results
        )
        total_approved = sum(
            r.get("acquisition_stats", {}).get("applications_approved", 0)
            for r in results
        )
        total_members = sum(
            r.get("acquisition_stats", {}).get("members_created", 0)
            for r in results
        )
        total_policies = sum(
            r.get("acquisition_stats", {}).get("policies_created", 0)
            for r in results
        )

        # Calculate aggregate metrics
        total_days = results[0].get("simulation_days", 0) if results else 0
        avg_days_per_second = total_days / total_elapsed if total_elapsed > 0 else 0

        return {
            "num_workers": self.num_workers,
            "total_elapsed_seconds": total_elapsed,
            "total_simulation_days": total_days,
            "avg_days_per_second": avg_days_per_second,
            "database_writes": total_writes,
            "acquisition": {
                "applications_submitted": total_applications,
                "applications_approved": total_approved,
                "members_created": total_members,
                "policies_created": total_policies,
                "approval_rate": total_approved / total_applications if total_applications > 0 else 0,
            },
            "worker_results": results,
        }

    def _enrich_survey_contexts(self) -> None:
        """
        Enrich NPS survey LLM contexts with historical claim and interaction data.

        This runs after all workers complete to populate:
        - claim_history: Summary statistics (counts and amounts) for claims
          submitted before the survey was sent
        - interaction_history: Summary statistics (counts and durations) for interactions
          that occurred before the survey was sent
        - current_trigger: Full details for the triggering claim or interaction

        Only enriches surveys where will_respond = true to optimize performance.
        Uses date-filtered queries to ensure historical accuracy (e.g., a 2022 survey
        only sees 2022 claims, not all claims up to 2027).
        """
        logger.info("enriching_survey_contexts_starting")

        try:
            engine = create_engine_from_config(self.config.database)

            with engine.connect() as conn:
                # Execute batch enrichment query
                enrichment_query = text("""
                    WITH surveys AS (
                      SELECT pending_id, member_id, simulation_date, sent_datetime, llm_context, claim_id, interaction_id
                      FROM nps_survey_pending
                      WHERE will_respond = true AND processing_status = 'pending'
                    ),
                    claim_summaries AS (
                      SELECT 
                        s.pending_id,
                        jsonb_build_object(
                          'total_count', COUNT(*) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime)),
                          'approved_count', COUNT(*) FILTER (WHERE COALESCE(c.payment_date, c.assessment_date, c.lodgement_date) <= DATE(s.sent_datetime) AND c.claim_status IN ('Paid', 'Approved')),
                          'rejected_count', COUNT(*) FILTER (WHERE COALESCE(c.payment_date, c.assessment_date, c.lodgement_date) <= DATE(s.sent_datetime) AND c.claim_status = 'Rejected'),
                          'hospital_count', COUNT(*) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime) AND c.claim_type = 'Hospital'),
                          'extras_count', COUNT(*) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime) AND c.claim_type = 'Extras'),
                          'ambulance_count', COUNT(*) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime) AND c.claim_type = 'Ambulance'),
                          'total_charge_amount', COALESCE(SUM(c.total_charge) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime)), 0),
                          'total_benefit_amount', COALESCE(SUM(c.total_benefit) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime)), 0),
                          'average_charge_amount', COALESCE(AVG(c.total_charge) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime)), 0),
                          'average_benefit_amount', COALESCE(AVG(c.total_benefit) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime) AND c.total_benefit IS NOT NULL), 0),
                          'approved_charge_amount', COALESCE(SUM(c.total_charge) FILTER (WHERE COALESCE(c.payment_date, c.assessment_date, c.lodgement_date) <= DATE(s.sent_datetime) AND c.claim_status IN ('Paid', 'Approved')), 0),
                          'approved_benefit_amount', COALESCE(SUM(c.total_benefit) FILTER (WHERE COALESCE(c.payment_date, c.assessment_date, c.lodgement_date) <= DATE(s.sent_datetime) AND c.claim_status IN ('Paid', 'Approved')), 0),
                          'rejected_charge_amount', COALESCE(SUM(c.total_charge) FILTER (WHERE COALESCE(c.payment_date, c.assessment_date, c.lodgement_date) <= DATE(s.sent_datetime) AND c.claim_status = 'Rejected'), 0),
                          'hospital_charge_amount', COALESCE(SUM(c.total_charge) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime) AND c.claim_type = 'Hospital'), 0),
                          'extras_charge_amount', COALESCE(SUM(c.total_charge) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime) AND c.claim_type = 'Extras'), 0),
                          'ambulance_charge_amount', COALESCE(SUM(c.total_charge) FILTER (WHERE c.lodgement_date <= DATE(s.sent_datetime) AND c.claim_type = 'Ambulance'), 0)
                        ) AS claim_summary
                      FROM surveys s
                      LEFT JOIN claim c ON c.member_id = s.member_id
                      GROUP BY s.pending_id
                    ),
                    interaction_summaries AS (
                      SELECT 
                        s.pending_id,
                        jsonb_build_object(
                          'total_count', COUNT(*) FILTER (WHERE i.start_datetime <= s.sent_datetime),
                          'by_channel', jsonb_build_object(
                            'Phone', COUNT(*) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.channel = 'Phone'),
                            'Email', COUNT(*) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.channel = 'Email'),
                            'Chat', COUNT(*) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.channel = 'Chat'),
                            'Branch', COUNT(*) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.channel = 'Branch'),
                            'InApp', COUNT(*) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.channel = 'InApp')
                          ),
                          'by_direction', jsonb_build_object(
                            'Inbound', COUNT(*) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.direction = 'Inbound'),
                            'Outbound', COUNT(*) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.direction = 'Outbound')
                          ),
                          'first_contact_resolution_count', COUNT(*) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.first_contact_resolution = true),
                          'total_duration_minutes', COALESCE(SUM(i.duration_seconds) FILTER (WHERE i.start_datetime <= s.sent_datetime), 0) / 60.0,
                          'average_duration_minutes', COALESCE(AVG(i.duration_seconds) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.duration_seconds IS NOT NULL), 0) / 60.0,
                          'total_wait_minutes', COALESCE(SUM(i.wait_time_seconds) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.wait_time_seconds IS NOT NULL), 0) / 60.0,
                          'average_wait_minutes', COALESCE(AVG(i.wait_time_seconds) FILTER (WHERE i.start_datetime <= s.sent_datetime AND i.wait_time_seconds IS NOT NULL), 0) / 60.0
                        ) AS interaction_summary
                      FROM surveys s
                      LEFT JOIN interaction i ON i.member_id = s.member_id
                      GROUP BY s.pending_id
                    ),
                    trigger_claim_details AS (
                      SELECT 
                        s.pending_id,
                        jsonb_build_object(
                          'date', c.service_date::text,
                          'service_date', c.service_date::text,
                          'service_type', c.claim_type,
                          'clinical_category', NULL,
                          'charge_amount', c.total_charge,
                          'total_charge', c.total_charge,
                          'benefit_paid', c.total_benefit,
                          'gap_amount', c.total_gap,
                          'status', c.claim_status,
                          'processing_days', CASE 
                            WHEN c.payment_date IS NOT NULL THEN (c.payment_date - c.lodgement_date)
                            WHEN c.assessment_date IS NOT NULL THEN (c.assessment_date - c.lodgement_date)
                            ELSE NULL
                          END,
                          'rejection_reason', c.rejection_notes
                        ) AS trigger_claim
                      FROM surveys s
                      JOIN claim c ON c.claim_id = s.claim_id
                      WHERE s.claim_id IS NOT NULL
                    ),
                    trigger_interaction_details AS (
                      SELECT 
                        s.pending_id,
                        jsonb_build_object(
                          'interaction_date', i.start_datetime::date::text,
                          'type', NULL,
                          'channel', i.channel,
                          'duration_minutes', COALESCE(i.duration_seconds, 0) / 60.0,
                          'wait_time_minutes', CASE WHEN i.wait_time_seconds IS NOT NULL THEN i.wait_time_seconds / 60.0 ELSE NULL END,
                          'resolved', i.first_contact_resolution,
                          'related_to', i.trigger_event_type
                        ) AS trigger_interaction
                      FROM surveys s
                      JOIN interaction i ON i.interaction_id = s.interaction_id
                      WHERE s.interaction_id IS NOT NULL
                    ),
                    enriched_contexts AS (
                      SELECT 
                        s.pending_id,
                        jsonb_set(
                          jsonb_set(
                            jsonb_set(
                              s.llm_context,
                              '{claim_history}',
                              COALESCE(cs.claim_summary, '{}'::jsonb)
                            ),
                            '{interaction_history}',
                            COALESCE(int_sum.interaction_summary, '{}'::jsonb)
                          ),
                          '{current_trigger}',
                          COALESCE(tc.trigger_claim, ti.trigger_interaction, s.llm_context->'current_trigger')
                        ) AS enriched_context
                      FROM surveys s
                      LEFT JOIN claim_summaries cs ON cs.pending_id = s.pending_id
                      LEFT JOIN interaction_summaries int_sum ON int_sum.pending_id = s.pending_id
                      LEFT JOIN trigger_claim_details tc ON tc.pending_id = s.pending_id
                      LEFT JOIN trigger_interaction_details ti ON ti.pending_id = s.pending_id
                    ),
                    -- Assign processing_order per member for longitudinal processing
                    survey_ordering AS (
                      SELECT 
                        pending_id,
                        ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY sent_datetime) AS proc_order
                      FROM surveys
                    )
                    UPDATE nps_survey_pending s
                    SET llm_context = ec.enriched_context,
                        processing_order = so.proc_order
                    FROM enriched_contexts ec
                    JOIN survey_ordering so ON so.pending_id = ec.pending_id
                    WHERE s.pending_id = ec.pending_id
                """)

                result = conn.execute(enrichment_query)
                rows_updated = result.rowcount
                conn.commit()

                logger.info(
                    "enriching_survey_contexts_completed",
                    surveys_enriched=rows_updated,
                )

        except Exception as e:
            logger.error(
                "enriching_survey_contexts_failed",
                error=str(e),
                exc_info=True,
            )
            # Don't raise - enrichment failure shouldn't fail the simulation

    def _process_llm_surveys(self) -> None:
        """
        Process pending surveys with LLM.

        This runs after survey context enrichment to generate realistic
        survey responses using Databricks ai_query.

        Only runs if llm.enabled and llm.process_after_simulation are True.
        Failures are logged but don't fail the simulation.
        """
        if not self.config.llm.enabled:
            logger.debug("llm_processing_skipped", reason="llm.enabled=false")
            return

        if not self.config.llm.process_after_simulation:
            logger.info(
                "llm_processing_skipped",
                reason="llm.process_after_simulation=false",
            )
            return

        logger.info("llm_survey_processing_starting")

        try:
            from brickwell_health.core.llm_processor import LLMSurveyProcessor

            processor = LLMSurveyProcessor(self.config)
            stats = processor.process_all()

            logger.info(
                "llm_survey_processing_completed",
                nps_processed=stats["nps_processed"],
                nps_responded=stats["nps_responded"],
                csat_processed=stats["csat_processed"],
                csat_responded=stats["csat_responded"],
                llm_calls=stats["llm_calls"],
                errors=stats["errors"],
            )

        except Exception as e:
            logger.error(
                "llm_survey_processing_failed",
                error=str(e),
                exc_info=True,
            )
            # Don't raise - LLM failure shouldn't fail the simulation
