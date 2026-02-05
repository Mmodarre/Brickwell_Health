"""
Checkpoint manager v2 for Brickwell Health Simulator.

Provides full state serialization for incremental simulation runs.
Supports save/load of SharedState, process state, and simulation time.

Version 2.0 - Clean break from v1, no backward compatibility.
"""

import json
import os
import tempfile
from collections import deque
from datetime import date, datetime
from pathlib import Path
from typing import Any, TYPE_CHECKING
from uuid import UUID

import structlog

from brickwell_health.core.serializers import (
    CheckpointEncoder,
    deserialize_billing_retry_state,
    deserialize_crm_active_journeys,
    deserialize_crm_pending_cases,
    deserialize_crm_pending_complaints,
    deserialize_date,
    deserialize_digital_processed_triggers,
    deserialize_event_queue,
    deserialize_member_engagement_levels,
    deserialize_nba_active_effects,
    deserialize_nba_execution_history,
    deserialize_pending_campaign_responses,
    deserialize_pending_claims,
    serialize_billing_retry_state,
    serialize_crm_active_journeys,
    serialize_crm_pending_cases,
    serialize_crm_pending_complaints,
    serialize_digital_processed_triggers,
    serialize_event_queue,
    serialize_member_engagement_levels,
    serialize_nba_active_effects,
    serialize_nba_execution_history,
    serialize_pending_campaign_responses,
    serialize_pending_claims,
)

if TYPE_CHECKING:
    from brickwell_health.core.shared_state import SharedState
    from brickwell_health.core.processes.crm import CRMProcess
    from brickwell_health.core.processes.digital import DigitalBehaviorProcess
    from brickwell_health.core.processes.billing import BillingProcess


logger = structlog.get_logger()


CHECKPOINT_VERSION = "2.0"


class CheckpointError(Exception):
    """Raised when checkpoint operations fail."""

    pass


class CheckpointCorruptedError(CheckpointError):
    """Raised when checkpoint file is corrupted or invalid."""

    pass


class CheckpointNotFoundError(CheckpointError):
    """Raised when checkpoint file is not found for resume."""

    pass


class CheckpointManagerV2:
    """
    Manages simulation checkpoints for incremental runs.

    Version 2.0 features:
    - Full SharedState serialization (non-reconstructable fields)
    - Process-specific state serialization
    - Simulation time tracking for resume
    - Atomic writes with temp file + rename

    Checkpoint contains:
    - Simulation time and dates
    - RNG state
    - ID generator counters
    - SharedState (non-reconstructable fields only)
    - Process state (CRM, Billing, Digital)

    Usage:
        manager = CheckpointManagerV2(checkpoint_dir)
        
        # Save checkpoint
        manager.save_full_checkpoint(
            worker_id=0,
            sim_time=365.0,
            checkpoint_date=date(2024, 1, 1),
            original_start_date=date(2023, 1, 1),
            id_counters={...},
            rng_state={...},
            shared_state=shared_state,
            crm_process=crm,
            billing_process=billing,
            digital_process=digital,
        )
        
        # Load checkpoint
        checkpoint = manager.load_checkpoint(worker_id=0)
        if checkpoint is None:
            raise CheckpointNotFoundError("No checkpoint for resume")
    """

    CHECKPOINT_PREFIX = "checkpoint_v2_"
    CHECKPOINT_SUFFIX = ".json"

    def __init__(self, checkpoint_dir: Path | str):
        """
        Initialize checkpoint manager.

        Args:
            checkpoint_dir: Directory to store checkpoints
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def save_full_checkpoint(
        self,
        worker_id: int,
        sim_time: float,
        checkpoint_date: date,
        original_start_date: date,
        id_counters: dict[str, int],
        rng_state: Any,
        shared_state: "SharedState",
        crm_process: "CRMProcess | None" = None,
        billing_process: "BillingProcess | None" = None,
        digital_process: "DigitalBehaviorProcess | None" = None,
    ) -> Path:
        """
        Save a full checkpoint with all state needed for resume.

        Args:
            worker_id: Worker ID
            sim_time: Current simulation time (days from start)
            checkpoint_date: Current simulation date
            original_start_date: Original simulation start date
            id_counters: ID generator counter values
            rng_state: NumPy RNG state (via rng.bit_generator.state)
            shared_state: SharedState instance
            crm_process: CRMProcess instance (optional)
            billing_process: BillingProcess instance (optional)
            digital_process: DigitalBehaviorProcess instance (optional)

        Returns:
            Path to saved checkpoint
        """
        checkpoint = self._create_checkpoint_dict(
            worker_id=worker_id,
            sim_time=sim_time,
            checkpoint_date=checkpoint_date,
            original_start_date=original_start_date,
            id_counters=id_counters,
            rng_state=rng_state,
            shared_state=shared_state,
            crm_process=crm_process,
            billing_process=billing_process,
            digital_process=digital_process,
        )

        return self._write_checkpoint(checkpoint, worker_id)

    def _create_checkpoint_dict(
        self,
        worker_id: int,
        sim_time: float,
        checkpoint_date: date,
        original_start_date: date,
        id_counters: dict[str, int],
        rng_state: Any,
        shared_state: "SharedState",
        crm_process: "CRMProcess | None" = None,
        billing_process: "BillingProcess | None" = None,
        digital_process: "DigitalBehaviorProcess | None" = None,
    ) -> dict[str, Any]:
        """Create the checkpoint dictionary with all state."""
        checkpoint: dict[str, Any] = {
            # Metadata
            "version": CHECKPOINT_VERSION,
            "_checkpoint_time": datetime.now().isoformat(),
            "_worker_id": worker_id,
            # Simulation state
            "sim_time": sim_time,
            "checkpoint_date": checkpoint_date.isoformat(),
            "original_start_date": original_start_date.isoformat(),
            # Core state
            "id_counters": id_counters,
            "rng_state": rng_state,
            # SharedState - non-reconstructable fields
            "pending_claims": serialize_pending_claims(shared_state.pending_claims),
            "member_change_events": list(shared_state.member_change_events),
            "crm_event_queue": serialize_event_queue(shared_state.crm_event_queue),
            "communication_event_queue": serialize_event_queue(
                shared_state.communication_event_queue
            ),
            "pending_campaign_responses": serialize_pending_campaign_responses(
                shared_state.pending_campaign_responses
            ),
            "member_engagement_levels": serialize_member_engagement_levels(
                shared_state.member_engagement_levels
            ),
            # NBA Domain State
            "nba_execution_history": serialize_nba_execution_history(
                shared_state.nba_execution_history
            ),
            "nba_active_effects": serialize_nba_active_effects(
                shared_state.nba_active_effects
            ),
            # Statistics
            "stats": {
                "active_policies": len(shared_state.active_policies),
                "policy_members": len(shared_state.policy_members),
                "pending_claims_count": len(shared_state.pending_claims),
                "crm_queue_size": len(shared_state.crm_event_queue),
                "communication_queue_size": len(shared_state.communication_event_queue),
                "nba_queue_size": len(shared_state.nba_action_queue),
                "nba_execution_history_members": len(shared_state.nba_execution_history),
                "nba_active_effects_policies": len(shared_state.nba_active_effects),
            },
        }

        # CRM process state
        if crm_process is not None:
            checkpoint["crm_pending_cases"] = serialize_crm_pending_cases(
                getattr(crm_process, "pending_cases", {})
            )
            checkpoint["crm_pending_complaints"] = serialize_crm_pending_complaints(
                getattr(crm_process, "pending_complaints", {})
            )
            checkpoint["crm_active_journeys"] = serialize_crm_active_journeys(
                getattr(crm_process, "active_journeys", {})
            )
        else:
            checkpoint["crm_pending_cases"] = {}
            checkpoint["crm_pending_complaints"] = {}
            checkpoint["crm_active_journeys"] = {}

        # Billing process state (retry state for pending invoices)
        if billing_process is not None:
            # Get pending_invoices from billing process
            pending_invoices = getattr(billing_process, "pending_invoices", {})
            checkpoint["billing_retry_state"] = serialize_billing_retry_state(
                pending_invoices
            )
        else:
            checkpoint["billing_retry_state"] = {}

        # Digital process state
        if digital_process is not None:
            checkpoint["digital_processed_triggers"] = (
                serialize_digital_processed_triggers(
                    getattr(digital_process, "_processed_triggers", set())
                )
            )
        else:
            checkpoint["digital_processed_triggers"] = []

        return checkpoint

    def _write_checkpoint(self, checkpoint: dict[str, Any], worker_id: int) -> Path:
        """
        Write checkpoint atomically using temp file + rename.

        Args:
            checkpoint: Checkpoint dictionary
            worker_id: Worker ID

        Returns:
            Path to saved checkpoint
        """
        checkpoint_path = self._get_checkpoint_path(worker_id)

        # Write to temp file first
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=self.checkpoint_dir,
            prefix=f"tmp_{self.CHECKPOINT_PREFIX}",
            suffix=self.CHECKPOINT_SUFFIX,
            delete=False,
        ) as f:
            json.dump(checkpoint, f, cls=CheckpointEncoder, indent=2)
            temp_path = f.name

        # Atomic rename
        os.replace(temp_path, checkpoint_path)

        logger.info(
            "checkpoint_v2_saved",
            worker_id=worker_id,
            path=str(checkpoint_path),
            sim_time=checkpoint["sim_time"],
            checkpoint_date=checkpoint["checkpoint_date"],
            pending_claims=checkpoint["stats"]["pending_claims_count"],
        )

        return checkpoint_path

    def load_checkpoint(self, worker_id: int) -> dict[str, Any] | None:
        """
        Load a checkpoint if it exists.

        Args:
            worker_id: Worker ID

        Returns:
            Checkpoint dictionary or None if not found

        Raises:
            CheckpointCorruptedError: If checkpoint file is corrupted
        """
        checkpoint_path = self._get_checkpoint_path(worker_id)

        if not checkpoint_path.exists():
            return None

        try:
            with open(checkpoint_path) as f:
                checkpoint = json.load(f)

            # Validate version
            version = checkpoint.get("version")
            if version != CHECKPOINT_VERSION:
                raise CheckpointCorruptedError(
                    f"Checkpoint version mismatch: expected {CHECKPOINT_VERSION}, "
                    f"got {version}. Delete old checkpoints and restart."
                )

            logger.info(
                "checkpoint_v2_loaded",
                worker_id=worker_id,
                checkpoint_time=checkpoint.get("_checkpoint_time"),
                checkpoint_date=checkpoint.get("checkpoint_date"),
                sim_time=checkpoint.get("sim_time"),
            )

            return checkpoint

        except json.JSONDecodeError as e:
            raise CheckpointCorruptedError(
                f"Checkpoint file corrupted (invalid JSON): {e}"
            ) from e
        except KeyError as e:
            raise CheckpointCorruptedError(
                f"Checkpoint file corrupted (missing key): {e}"
            ) from e

    def load_checkpoint_strict(self, worker_id: int) -> dict[str, Any]:
        """
        Load a checkpoint, raising error if not found.

        Args:
            worker_id: Worker ID

        Returns:
            Checkpoint dictionary

        Raises:
            CheckpointNotFoundError: If no checkpoint exists
            CheckpointCorruptedError: If checkpoint is corrupted
        """
        checkpoint = self.load_checkpoint(worker_id)
        if checkpoint is None:
            raise CheckpointNotFoundError(
                f"No checkpoint found for worker {worker_id}. "
                "Cannot resume without existing checkpoint."
            )
        return checkpoint

    def delete(self, worker_id: int) -> bool:
        """
        Delete a checkpoint.

        Args:
            worker_id: Worker ID

        Returns:
            True if deleted, False if not found
        """
        checkpoint_path = self._get_checkpoint_path(worker_id)

        if checkpoint_path.exists():
            checkpoint_path.unlink()
            logger.debug("checkpoint_v2_deleted", worker_id=worker_id)
            return True

        return False

    def list_checkpoints(self) -> list[dict[str, Any]]:
        """
        List all available v2 checkpoints.

        Returns:
            List of checkpoint info dictionaries
        """
        checkpoints = []

        for path in self.checkpoint_dir.glob(
            f"{self.CHECKPOINT_PREFIX}*{self.CHECKPOINT_SUFFIX}"
        ):
            try:
                with open(path) as f:
                    state = json.load(f)

                checkpoints.append(
                    {
                        "path": path,
                        "worker_id": state.get("_worker_id"),
                        "checkpoint_time": state.get("_checkpoint_time"),
                        "checkpoint_date": state.get("checkpoint_date"),
                        "sim_time": state.get("sim_time"),
                        "version": state.get("version"),
                        "stats": state.get("stats", {}),
                    }
                )
            except (json.JSONDecodeError, IOError):
                continue

        return sorted(checkpoints, key=lambda x: x.get("checkpoint_time", ""))

    def has_checkpoint(self, worker_id: int) -> bool:
        """
        Check if a checkpoint exists for a worker.

        Args:
            worker_id: Worker ID

        Returns:
            True if checkpoint exists
        """
        return self._get_checkpoint_path(worker_id).exists()

    def _get_checkpoint_path(self, worker_id: int) -> Path:
        """Get checkpoint file path for a worker."""
        return (
            self.checkpoint_dir
            / f"{self.CHECKPOINT_PREFIX}{worker_id}{self.CHECKPOINT_SUFFIX}"
        )


def restore_shared_state_from_checkpoint(
    shared_state: "SharedState",
    checkpoint: dict[str, Any],
) -> None:
    """
    Restore non-reconstructable fields of SharedState from checkpoint.

    This should be called AFTER database reconstruction has populated
    the reconstructable fields (active_policies, policy_members, etc.).

    Args:
        shared_state: SharedState instance to restore into
        checkpoint: Loaded checkpoint dictionary
    """
    # Pending claims
    if "pending_claims" in checkpoint:
        shared_state.pending_claims = deserialize_pending_claims(
            checkpoint["pending_claims"]
        )
        
        # Reconstruct member_data for pending claims from serialized data
        # Don't rely on policy_members (which filters by active policies)
        # because claims may be pending for suspended/ended policies
        _reconstruct_pending_claims_member_data(shared_state)

    # Event queues
    if "member_change_events" in checkpoint:
        shared_state.member_change_events = checkpoint["member_change_events"]

    if "crm_event_queue" in checkpoint:
        shared_state.crm_event_queue = deserialize_event_queue(
            checkpoint["crm_event_queue"]
        )

    if "communication_event_queue" in checkpoint:
        shared_state.communication_event_queue = deserialize_event_queue(
            checkpoint["communication_event_queue"]
        )

    # Pending campaign responses
    if "pending_campaign_responses" in checkpoint:
        shared_state.pending_campaign_responses = deserialize_pending_campaign_responses(
            checkpoint["pending_campaign_responses"]
        )

    # Member engagement levels
    if "member_engagement_levels" in checkpoint:
        shared_state.member_engagement_levels = deserialize_member_engagement_levels(
            checkpoint["member_engagement_levels"]
        )

    # NBA Domain State
    if "nba_execution_history" in checkpoint:
        shared_state.nba_execution_history = deserialize_nba_execution_history(
            checkpoint["nba_execution_history"]
        )

    if "nba_active_effects" in checkpoint:
        shared_state.nba_active_effects = deserialize_nba_active_effects(
            checkpoint["nba_active_effects"]
        )

    logger.info(
        "shared_state_restored_from_checkpoint",
        pending_claims=len(shared_state.pending_claims),
        crm_queue=len(shared_state.crm_event_queue),
        communication_queue=len(shared_state.communication_event_queue),
        engagement_levels=len(shared_state.member_engagement_levels),
        nba_execution_history=len(shared_state.nba_execution_history),
        nba_active_effects=len(shared_state.nba_active_effects),
    )


def restore_crm_process_state(
    crm_process: "CRMProcess",
    checkpoint: dict[str, Any],
) -> None:
    """
    Restore CRM process state from checkpoint.

    Args:
        crm_process: CRMProcess instance
        checkpoint: Loaded checkpoint dictionary
    """
    if "crm_pending_cases" in checkpoint:
        crm_process.pending_cases = deserialize_crm_pending_cases(
            checkpoint["crm_pending_cases"]
        )

    if "crm_pending_complaints" in checkpoint:
        crm_process.pending_complaints = deserialize_crm_pending_complaints(
            checkpoint["crm_pending_complaints"]
        )

    if "crm_active_journeys" in checkpoint:
        crm_process.active_journeys = deserialize_crm_active_journeys(
            checkpoint["crm_active_journeys"]
        )

    logger.debug(
        "crm_process_state_restored",
        pending_cases=len(crm_process.pending_cases),
        pending_complaints=len(crm_process.pending_complaints),
        active_journeys=len(crm_process.active_journeys),
    )


def restore_billing_retry_state(
    pending_invoices: dict,
    checkpoint: dict[str, Any],
) -> None:
    """
    Restore billing retry state from checkpoint.

    Merges retry state into existing pending_invoices dict.

    Args:
        pending_invoices: Pending invoices dict (already populated from DB)
        checkpoint: Loaded checkpoint dictionary
    """
    if "billing_retry_state" not in checkpoint:
        return

    retry_state = deserialize_billing_retry_state(checkpoint["billing_retry_state"])

    # Merge retry state into existing invoices
    for invoice_id, state in retry_state.items():
        if invoice_id in pending_invoices:
            pending_invoices[invoice_id]["attempts"] = state["attempts"]
            pending_invoices[invoice_id]["next_attempt_date"] = state[
                "next_attempt_date"
            ]
            pending_invoices[invoice_id]["arrears_created"] = state["arrears_created"]

    logger.debug(
        "billing_retry_state_restored",
        invoices_updated=len(retry_state),
    )


def restore_digital_process_state(
    digital_process: "DigitalBehaviorProcess",
    checkpoint: dict[str, Any],
) -> None:
    """
    Restore digital process state from checkpoint.

    Args:
        digital_process: DigitalBehaviorProcess instance
        checkpoint: Loaded checkpoint dictionary
    """
    if "digital_processed_triggers" in checkpoint:
        digital_process._processed_triggers = deserialize_digital_processed_triggers(
            checkpoint["digital_processed_triggers"]
        )

    logger.debug(
        "digital_process_state_restored",
        processed_triggers=len(digital_process._processed_triggers),
    )


def get_checkpoint_dates(checkpoint: dict[str, Any]) -> tuple[date, date]:
    """
    Extract dates from checkpoint.

    Args:
        checkpoint: Loaded checkpoint dictionary

    Returns:
        Tuple of (checkpoint_date, original_start_date)
    """
    checkpoint_date = deserialize_date(checkpoint["checkpoint_date"])
    original_start_date = deserialize_date(checkpoint["original_start_date"])

    if checkpoint_date is None or original_start_date is None:
        raise CheckpointCorruptedError(
            "Checkpoint missing required date fields: checkpoint_date, original_start_date"
        )

    return checkpoint_date, original_start_date


def _reconstruct_pending_claims_member_data(shared_state: "SharedState") -> None:
    """
    Reconstruct member_data for pending claims from serialized data.
    
    Creates minimal member and policy objects with just the attributes
    needed for claim lifecycle processing (member_id, product_id).
    
    Does NOT rely on policy_members lookup because claims may be pending
    for suspended/ended policies where the service was rendered during
    active coverage.
    
    Args:
        shared_state: SharedState with pending_claims to reconstruct
    """
    def safe_uuid(val: str | UUID | None) -> UUID | None:
        """Safely convert to UUID, returning None for invalid values."""
        if val is None or val == "None" or val == "":
            return None
        if isinstance(val, UUID):
            return val
        try:
            return UUID(val)
        except (ValueError, TypeError):
            return None
    
    claims_to_remove = []
    
    for claim_id, claim_data in list(shared_state.pending_claims.items()):
        serialized_member_data = claim_data.get("member_data", {})
        
        # Extract IDs from serialized data
        member_id = safe_uuid(serialized_member_data.get("member_id"))
        policy_id = safe_uuid(serialized_member_data.get("policy_id"))
        pm_id = safe_uuid(serialized_member_data.get("policy_member_id"))
        
        # Validate we have the minimum required data
        if not member_id:
            logger.warning(
                "pending_claim_missing_member_id_removing",
                claim_id=str(claim_id),
            )
            claims_to_remove.append(claim_id)
            continue
        
        # First, try to use policy_members if member is still active
        # (this gives us the full data including product_id)
        if pm_id and pm_id in shared_state.policy_members:
            claim_data["member_data"] = shared_state.policy_members[pm_id]
            continue
        
        # Member not in active policy_members (policy suspended/ended)
        # Create minimal objects from serialized data
        
        # Create minimal member object with required attributes
        member_obj = type("MemberFromCheckpoint", (), {
            "member_id": member_id,
        })()
        
        # Create minimal policy object
        # product_id is needed for benefit limit lookup - try to get from claim data
        # If not available, the benefit usage tracking will skip limit lookup
        policy_obj = type("PolicyFromCheckpoint", (), {
            "policy_id": policy_id,
            "product_id": None,  # Will be looked up from claim_data["policy_id"] if needed
        })()
        
        # Reconstruct member_data with minimal objects
        claim_data["member_data"] = {
            "member": member_obj,
            "policy": policy_obj,
            "member_id": member_id,
            "policy_id": policy_id,
            "policy_member_id": pm_id,
            "age": serialized_member_data.get("age"),
            "gender": serialized_member_data.get("gender"),
            "hospital_coverage": True if serialized_member_data.get("has_hospital_coverage") else None,
            "extras_coverage": True if serialized_member_data.get("has_extras_coverage") else None,
            "ambulance_coverage": True if serialized_member_data.get("has_ambulance_coverage") else None,
        }
        
        logger.debug(
            "pending_claim_reconstructed_from_checkpoint",
            claim_id=str(claim_id),
            member_id=str(member_id),
            policy_id=str(policy_id) if policy_id else None,
        )
    
    # Remove invalid claims
    for claim_id in claims_to_remove:
        del shared_state.pending_claims[claim_id]
    
    if claims_to_remove:
        logger.info(
            "pending_claims_removed_invalid",
            count=len(claims_to_remove),
        )
