"""
Integration tests for incremental simulation (checkpoint/resume).

These tests require a running PostgreSQL database and test the full
checkpoint-resume workflow.
"""

import os
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Skip all tests if database is not available
pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_DB_TESTS", "1") == "1",
    reason="Database tests disabled (set SKIP_DB_TESTS=0 to enable)"
)


class TestIncrementalSimulationFlow:
    """
    Integration tests for incremental simulation workflow.
    
    Tests the full flow:
    1. Run simulation for N days
    2. Save checkpoint
    3. Resume from checkpoint
    4. Continue for M more days
    5. Verify state consistency
    """

    @pytest.fixture
    def test_config(self, tmp_path):
        """Create a test configuration with minimal settings."""
        from brickwell_health.config.models import (
            SimulationConfig,
            SimulationTimeConfig,
            ScaleConfig,
            DatabaseConfig,
            ParallelConfig,
        )

        # Use temp directory for checkpoints
        checkpoint_dir = tmp_path / "checkpoints"
        checkpoint_dir.mkdir()

        # Create minimal reference data directory
        reference_dir = tmp_path / "reference"
        reference_dir.mkdir()

        config = SimulationConfig(
            simulation=SimulationTimeConfig(
                start_date=date(2023, 1, 1),
                end_date=date(2023, 1, 31),  # 1 month
                warmup_days=365,
            ),
            scale=ScaleConfig(
                target_member_count=1000,
                target_growth_rate=0.03,
                target_churn_rate=0.10,
            ),
            database=DatabaseConfig(
                host=os.environ.get("BRICKWELL_DB_HOST", "localhost"),
                port=int(os.environ.get("BRICKWELL_DB_PORT", "5432")),
                database=os.environ.get("BRICKWELL_DB_NAME", "brickwell_health_test"),
                username=os.environ.get("BRICKWELL_DB_USER", "brickwell"),
                password=os.environ.get("BRICKWELL_DB_PASSWORD", "brickwell"),
                batch_size=1000,
            ),
            parallel=ParallelConfig(
                num_workers=1,  # Single worker for simpler testing
                checkpoint_interval_minutes=1440,  # Daily
            ),
            reference_data_path=str(reference_dir),
            seed=42,
        )

        return config

    @pytest.fixture
    def checkpoint_manager(self, test_config):
        """Create a checkpoint manager for testing."""
        from brickwell_health.core.checkpoint_v2 import CheckpointManagerV2

        checkpoint_dir = Path(test_config.reference_data_path).parent / "checkpoints"
        return CheckpointManagerV2(checkpoint_dir)

    def test_checkpoint_structure(self, test_config, checkpoint_manager):
        """Test that checkpoint contains expected structure."""
        from brickwell_health.core.shared_state import SharedState
        from uuid import uuid4

        shared_state = SharedState()
        
        # Add test data
        member_id = uuid4()
        policy_id = uuid4()
        shared_state.member_engagement_levels[member_id] = "high"
        shared_state.active_policies[policy_id] = {"status": "Active"}

        # Save checkpoint
        checkpoint_manager.save_full_checkpoint(
            worker_id=0,
            sim_time=30.0,
            checkpoint_date=date(2023, 1, 31),
            original_start_date=date(2023, 1, 1),
            id_counters={"member": 100, "policy": 50},
            rng_state={"bit_generator": "test_state"},
            shared_state=shared_state,
        )

        # Load and verify structure
        checkpoint = checkpoint_manager.load_checkpoint(0)

        assert checkpoint is not None
        assert checkpoint["version"] == "2.0"
        assert checkpoint["sim_time"] == 30.0
        assert checkpoint["checkpoint_date"] == "2023-01-31"
        assert checkpoint["original_start_date"] == "2023-01-01"
        assert checkpoint["id_counters"]["member"] == 100
        assert str(member_id) in checkpoint["member_engagement_levels"]
        assert "stats" in checkpoint

    def test_shared_state_roundtrip(self, test_config, checkpoint_manager):
        """Test SharedState serialization and restoration round-trip."""
        from brickwell_health.core.shared_state import SharedState
        from brickwell_health.core.checkpoint_v2 import restore_shared_state_from_checkpoint
        from collections import deque
        from uuid import uuid4

        # Create original state
        original = SharedState()
        member_id = uuid4()
        original.member_engagement_levels[member_id] = "medium"
        original.crm_event_queue.append({"event_type": "claim_paid", "member_id": str(member_id)})
        original.member_change_events.append({"change_type": "ADDRESS_CHANGE"})

        # Save checkpoint
        checkpoint_manager.save_full_checkpoint(
            worker_id=0,
            sim_time=15.0,
            checkpoint_date=date(2023, 1, 16),
            original_start_date=date(2023, 1, 1),
            id_counters={},
            rng_state={},
            shared_state=original,
        )

        # Load checkpoint and restore to new state
        checkpoint = checkpoint_manager.load_checkpoint(0)
        restored = SharedState()
        restore_shared_state_from_checkpoint(restored, checkpoint)

        # Verify restoration
        assert member_id in restored.member_engagement_levels
        assert restored.member_engagement_levels[member_id] == "medium"
        assert len(restored.crm_event_queue) == 1
        assert len(restored.member_change_events) == 1

    def test_resume_mode_fails_without_checkpoint(self, test_config, checkpoint_manager):
        """Test that resume mode fails if no checkpoint exists."""
        from brickwell_health.core.checkpoint_v2 import CheckpointNotFoundError

        # Ensure no checkpoint exists
        checkpoint_manager.delete(0)

        # Attempting to load strict should fail
        with pytest.raises(CheckpointNotFoundError):
            checkpoint_manager.load_checkpoint_strict(0)

    def test_checkpoint_dates_extraction(self, test_config, checkpoint_manager):
        """Test extracting dates from checkpoint."""
        from brickwell_health.core.shared_state import SharedState
        from brickwell_health.core.checkpoint_v2 import get_checkpoint_dates

        shared_state = SharedState()

        # Save checkpoint
        checkpoint_manager.save_full_checkpoint(
            worker_id=0,
            sim_time=45.0,
            checkpoint_date=date(2023, 2, 15),
            original_start_date=date(2023, 1, 1),
            id_counters={},
            rng_state={},
            shared_state=shared_state,
        )

        # Load and extract dates
        checkpoint = checkpoint_manager.load_checkpoint(0)
        checkpoint_date, original_start_date = get_checkpoint_dates(checkpoint)

        assert checkpoint_date == date(2023, 2, 15)
        assert original_start_date == date(2023, 1, 1)

    def test_multiple_workers_checkpoints(self, test_config, checkpoint_manager):
        """Test checkpoints for multiple workers."""
        from brickwell_health.core.shared_state import SharedState

        # Save checkpoints for multiple workers
        for worker_id in range(3):
            shared_state = SharedState()
            checkpoint_manager.save_full_checkpoint(
                worker_id=worker_id,
                sim_time=30.0 + worker_id,
                checkpoint_date=date(2023, 1, 31),
                original_start_date=date(2023, 1, 1),
                id_counters={"member": 100 * (worker_id + 1)},
                rng_state={},
                shared_state=shared_state,
            )

        # Verify all checkpoints exist
        checkpoints = checkpoint_manager.list_checkpoints()
        assert len(checkpoints) == 3

        # Verify each has correct data
        for worker_id in range(3):
            checkpoint = checkpoint_manager.load_checkpoint(worker_id)
            assert checkpoint["id_counters"]["member"] == 100 * (worker_id + 1)

    def test_id_counter_continuity(self, test_config, checkpoint_manager):
        """Test that ID counters are preserved across checkpoint."""
        from brickwell_health.core.shared_state import SharedState
        from brickwell_health.generators.id_generator import IDGenerator
        import numpy as np

        # Create ID generator with some state
        rng = np.random.default_rng(42)
        id_gen = IDGenerator(rng, 2023, worker_id=0)

        # Generate some IDs to advance counters
        for _ in range(10):
            id_gen.generate_member_id()
            id_gen.generate_policy_number("Single")

        original_counters = id_gen.get_counters()

        # Save checkpoint
        shared_state = SharedState()
        checkpoint_manager.save_full_checkpoint(
            worker_id=0,
            sim_time=30.0,
            checkpoint_date=date(2023, 1, 31),
            original_start_date=date(2023, 1, 1),
            id_counters=original_counters,
            rng_state=rng.bit_generator.state,
            shared_state=shared_state,
        )

        # Load checkpoint
        checkpoint = checkpoint_manager.load_checkpoint(0)

        # Restore to new ID generator
        new_rng = np.random.default_rng(42)
        new_id_gen = IDGenerator(new_rng, 2023, worker_id=0)
        new_id_gen.set_counters(**checkpoint["id_counters"])

        # Verify counters match
        assert new_id_gen.get_counters() == original_counters


class TestStateReconstructionQueries:
    """
    Tests for database state reconstruction queries.
    
    These tests verify the SQL queries can execute correctly
    (actual data reconstruction tested with populated DB).
    """

    @pytest.fixture
    def db_engine(self, test_config):
        """Create database engine for testing."""
        from brickwell_health.db.connection import create_engine_from_config
        return create_engine_from_config(test_config.database)

    def test_load_active_policies_query_executes(self, db_engine):
        """Test that active policies query executes without error."""
        from brickwell_health.core.state_reconstruction import load_active_policies

        # This should execute without error even on empty DB
        with db_engine.connect() as conn:
            result = load_active_policies(conn, date(2023, 6, 15))
            assert isinstance(result, dict)

    def test_load_policy_members_query_executes(self, db_engine):
        """Test that policy members query executes without error."""
        from brickwell_health.core.state_reconstruction import load_policy_members

        with db_engine.connect() as conn:
            result = load_policy_members(conn, date(2023, 6, 15))
            assert isinstance(result, dict)

    def test_load_waiting_periods_query_executes(self, db_engine):
        """Test that waiting periods query executes without error."""
        from brickwell_health.core.state_reconstruction import load_waiting_periods

        with db_engine.connect() as conn:
            result = load_waiting_periods(conn, date(2023, 6, 15))
            assert isinstance(result, dict)

    def test_load_communication_preferences_query_executes(self, db_engine):
        """Test that communication preferences query executes without error."""
        from brickwell_health.core.state_reconstruction import load_communication_preferences

        with db_engine.connect() as conn:
            result = load_communication_preferences(conn)
            assert isinstance(result, dict)

    def test_load_pending_invoices_query_executes(self, db_engine):
        """Test that pending invoices query executes without error."""
        from brickwell_health.core.state_reconstruction import load_pending_invoices

        with db_engine.connect() as conn:
            result = load_pending_invoices(conn, date(2023, 6, 15))
            assert isinstance(result, dict)

    def test_load_cumulative_usage_query_executes(self, db_engine):
        """Test that cumulative usage query executes without error."""
        from brickwell_health.core.state_reconstruction import load_cumulative_usage

        with db_engine.connect() as conn:
            result = load_cumulative_usage(conn, date(2023, 6, 15))
            assert isinstance(result, dict)

    def test_reconstruct_shared_state_from_db_executes(self, db_engine):
        """Test that full reconstruction executes without error."""
        from brickwell_health.core.state_reconstruction import reconstruct_shared_state_from_db
        from brickwell_health.core.shared_state import SharedState

        result = reconstruct_shared_state_from_db(db_engine, date(2023, 6, 15))
        
        assert isinstance(result, SharedState)
        assert isinstance(result.active_policies, dict)
        assert isinstance(result.policy_members, dict)


class TestCLIResumeOptions:
    """Tests for CLI --resume and --extend-days options."""

    def test_extend_days_requires_resume(self, tmp_path):
        """Test that --extend-days without --resume fails."""
        from click.testing import CliRunner
        from brickwell_health.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["run", "--extend-days", "30"])

        assert result.exit_code != 0
        assert "requires --resume" in result.output.lower() or result.exit_code == 1

    def test_resume_without_checkpoint_fails(self, tmp_path):
        """Test that --resume without checkpoint fails gracefully."""
        from click.testing import CliRunner
        from brickwell_health.cli import main

        # Create minimal config
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text("""
simulation:
  start_date: 2023-01-01
  end_date: 2023-12-31
  warmup_days: 365
scale:
  target_member_count: 1000
database:
  host: localhost
  port: 5432
  database: brickwell_test
  username: test
  password: test
parallel:
  num_workers: 1
reference_data_path: /tmp/reference
seed: 42
""")

        runner = CliRunner()
        result = runner.invoke(main, ["-c", str(config_path), "run", "--resume"])

        # Should fail because no checkpoint exists
        assert result.exit_code != 0
