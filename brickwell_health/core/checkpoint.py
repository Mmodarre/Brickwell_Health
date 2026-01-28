"""
Checkpoint manager for Brickwell Health Simulator.

Provides save/load state functionality for crash recovery.
"""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog


logger = structlog.get_logger()


class CheckpointManager:
    """
    Manages simulation checkpoints for recovery.

    Checkpoints are saved atomically using temp file + rename
    to prevent corruption from crashes during write.

    Checkpoint contains:
    - Simulation time
    - RNG state
    - ID generator counters
    - Active entity counts

    Usage:
        checkpoint = CheckpointManager(checkpoint_dir)
        checkpoint.save(state_dict, worker_id)
        state = checkpoint.load(worker_id)
    """

    CHECKPOINT_PREFIX = "checkpoint_"
    CHECKPOINT_SUFFIX = ".json"

    def __init__(self, checkpoint_dir: Path | str):
        """
        Initialize checkpoint manager.

        Args:
            checkpoint_dir: Directory to store checkpoints
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        state: dict[str, Any],
        worker_id: int,
    ) -> Path:
        """
        Save a checkpoint atomically.

        Args:
            state: State dictionary to save
            worker_id: Worker ID

        Returns:
            Path to saved checkpoint
        """
        checkpoint_path = self._get_checkpoint_path(worker_id)

        # Add metadata
        state["_checkpoint_time"] = datetime.now().isoformat()
        state["_worker_id"] = worker_id

        # Write to temp file first
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=self.checkpoint_dir,
            prefix=f"tmp_{self.CHECKPOINT_PREFIX}",
            suffix=self.CHECKPOINT_SUFFIX,
            delete=False,
        ) as f:
            json.dump(state, f, indent=2, default=str)
            temp_path = f.name

        # Atomic rename
        os.replace(temp_path, checkpoint_path)

        logger.debug(
            "checkpoint_saved",
            worker_id=worker_id,
            path=str(checkpoint_path),
        )

        return checkpoint_path

    def load(
        self,
        worker_id: int,
    ) -> dict[str, Any] | None:
        """
        Load a checkpoint if it exists.

        Args:
            worker_id: Worker ID

        Returns:
            State dictionary or None if no checkpoint
        """
        checkpoint_path = self._get_checkpoint_path(worker_id)

        if not checkpoint_path.exists():
            return None

        try:
            with open(checkpoint_path) as f:
                state = json.load(f)

            logger.info(
                "checkpoint_loaded",
                worker_id=worker_id,
                checkpoint_time=state.get("_checkpoint_time"),
            )

            return state

        except (json.JSONDecodeError, IOError) as e:
            logger.warning(
                "checkpoint_load_failed",
                worker_id=worker_id,
                error=str(e),
            )
            return None

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
            return True

        return False

    def list_checkpoints(self) -> list[dict[str, Any]]:
        """
        List all available checkpoints.

        Returns:
            List of checkpoint info dictionaries
        """
        checkpoints = []

        for path in self.checkpoint_dir.glob(f"{self.CHECKPOINT_PREFIX}*{self.CHECKPOINT_SUFFIX}"):
            try:
                with open(path) as f:
                    state = json.load(f)

                checkpoints.append({
                    "path": path,
                    "worker_id": state.get("_worker_id"),
                    "checkpoint_time": state.get("_checkpoint_time"),
                    "sim_time": state.get("sim_time"),
                })
            except (json.JSONDecodeError, IOError):
                continue

        return sorted(checkpoints, key=lambda x: x.get("checkpoint_time", ""))

    def clean_old_checkpoints(
        self,
        keep_latest: int = 3,
    ) -> int:
        """
        Remove old checkpoints, keeping the most recent ones.

        Args:
            keep_latest: Number of checkpoints to keep per worker

        Returns:
            Number of checkpoints removed
        """
        # Group by worker
        by_worker: dict[int, list[Path]] = {}

        for path in self.checkpoint_dir.glob(f"{self.CHECKPOINT_PREFIX}*{self.CHECKPOINT_SUFFIX}"):
            try:
                worker_id = int(path.stem.split("_")[-1])
                if worker_id not in by_worker:
                    by_worker[worker_id] = []
                by_worker[worker_id].append(path)
            except ValueError:
                continue

        removed = 0

        for worker_id, paths in by_worker.items():
            # Sort by modification time
            paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)

            # Remove old ones
            for path in paths[keep_latest:]:
                path.unlink()
                removed += 1

        return removed

    def _get_checkpoint_path(self, worker_id: int) -> Path:
        """Get checkpoint file path for a worker."""
        return self.checkpoint_dir / f"{self.CHECKPOINT_PREFIX}{worker_id}{self.CHECKPOINT_SUFFIX}"


def create_checkpoint_state(
    sim_time: float,
    id_counters: dict[str, int],
    rng_state: Any,
    active_policies: int = 0,
    active_members: int = 0,
) -> dict[str, Any]:
    """
    Create a checkpoint state dictionary.

    Args:
        sim_time: Current simulation time (days)
        id_counters: ID generator counter values
        rng_state: NumPy RNG state (via rng.bit_generator.state)
        active_policies: Number of active policies
        active_members: Number of active members

    Returns:
        State dictionary for checkpointing
    """
    return {
        "sim_time": sim_time,
        "id_counters": id_counters,
        "rng_state": rng_state,
        "active_policies": active_policies,
        "active_members": active_members,
    }


def restore_from_checkpoint(
    checkpoint: dict[str, Any],
    sim_env,
    id_generator,
    rng,
) -> None:
    """
    Restore simulation state from a checkpoint.

    Args:
        checkpoint: Checkpoint state dictionary
        sim_env: Simulation environment to restore
        id_generator: ID generator to restore
        rng: NumPy RNG to restore
    """
    # Restore simulation time (if possible)
    # Note: SimPy doesn't support setting time directly,
    # so this would require rerunning to that point

    # Restore ID counters
    counters = checkpoint.get("id_counters", {})
    id_generator.set_counters(**counters)

    # Restore RNG state
    rng_state = checkpoint.get("rng_state")
    if rng_state:
        rng.bit_generator.state = rng_state
