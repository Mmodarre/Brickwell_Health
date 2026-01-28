"""
Unit tests for partition management.
"""

from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.core.partition import (
    get_partition_id,
    is_owned_by_worker,
    PartitionManager,
)


class TestPartitionFunctions:
    """Tests for partition utility functions."""

    def test_get_partition_id_is_deterministic(self):
        """Same UUID should always map to same partition."""
        entity_id = uuid4()

        partition1 = get_partition_id(entity_id, 8)
        partition2 = get_partition_id(entity_id, 8)

        assert partition1 == partition2

    def test_get_partition_id_within_range(self):
        """Partition ID should be in [0, num_workers)."""
        num_workers = 8

        for _ in range(100):
            entity_id = uuid4()
            partition = get_partition_id(entity_id, num_workers)

            assert 0 <= partition < num_workers

    def test_is_owned_by_worker_correct(self):
        """is_owned_by_worker should match get_partition_id."""
        entity_id = uuid4()
        num_workers = 8

        partition = get_partition_id(entity_id, num_workers)

        # Only one worker should own it
        owners = [
            worker_id
            for worker_id in range(num_workers)
            if is_owned_by_worker(entity_id, worker_id, num_workers)
        ]

        assert len(owners) == 1
        assert owners[0] == partition


class TestPartitionManager:
    """Tests for PartitionManager class."""

    def test_owns_returns_correct_result(self):
        """PartitionManager.owns should be consistent."""
        manager = PartitionManager(worker_id=3, num_workers=8)

        # Generate some UUIDs and check ownership
        owned = []
        not_owned = []

        for _ in range(100):
            entity_id = uuid4()
            if manager.owns(entity_id):
                owned.append(entity_id)
            else:
                not_owned.append(entity_id)

        # Should have some of each (probabilistically)
        assert len(owned) > 0
        assert len(not_owned) > 0

        # All owned should have partition 3
        for entity_id in owned:
            assert manager.get_partition(entity_id) == 3

    def test_generate_owned_uuid_is_owned(self):
        """Generated UUIDs should be owned by this worker."""
        rng = np.random.default_rng(42)
        manager = PartitionManager(worker_id=5, num_workers=8)

        for _ in range(10):
            entity_id = manager.generate_owned_uuid(rng)
            assert manager.owns(entity_id)

    def test_filter_owned_filters_correctly(self):
        """filter_owned should return only owned UUIDs."""
        manager = PartitionManager(worker_id=2, num_workers=8)

        # Generate mixed list
        all_ids = [uuid4() for _ in range(100)]
        filtered = manager.filter_owned(all_ids)

        # All filtered should be owned
        for entity_id in filtered:
            assert manager.owns(entity_id)

        # Filtered should be subset
        assert set(filtered).issubset(set(all_ids))

    def test_partition_count_sums_correctly(self):
        """partition_count should sum to total."""
        manager = PartitionManager(worker_id=0, num_workers=8)

        ids = [uuid4() for _ in range(100)]
        counts = manager.partition_count(ids)

        assert sum(counts.values()) == 100
        assert all(0 <= count for count in counts.values())

    def test_distribution_roughly_even(self):
        """UUIDs should be roughly evenly distributed."""
        manager = PartitionManager(worker_id=0, num_workers=8)

        # Generate many UUIDs
        ids = [uuid4() for _ in range(10000)]
        counts = manager.partition_count(ids)

        # Each partition should have roughly 1250 (within 20%)
        expected = 10000 / 8
        for partition, count in counts.items():
            assert expected * 0.8 < count < expected * 1.2
