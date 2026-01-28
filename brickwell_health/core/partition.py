"""
Partition management for parallel execution.

Uses UUID-based partitioning to distribute entities across workers
without coordination.
"""

from uuid import UUID


def uuid_to_int(u: UUID) -> int:
    """
    Convert UUID to integer for partitioning.

    Args:
        u: UUID to convert

    Returns:
        Integer representation of UUID
    """
    return u.int


def get_partition_id(entity_id: UUID, num_workers: int) -> int:
    """
    Determine which worker owns this entity.

    Uses modulo of UUID integer value for deterministic partitioning.
    This ensures the same entity always maps to the same worker.

    Args:
        entity_id: UUID of the entity
        num_workers: Total number of workers

    Returns:
        Worker ID that owns this entity (0 to num_workers-1)
    """
    return uuid_to_int(entity_id) % num_workers


def is_owned_by_worker(entity_id: UUID, worker_id: int, num_workers: int) -> bool:
    """
    Check if entity is owned by a specific worker.

    Args:
        entity_id: UUID of the entity
        worker_id: Worker ID to check
        num_workers: Total number of workers

    Returns:
        True if the worker owns this entity
    """
    return get_partition_id(entity_id, num_workers) == worker_id


class PartitionManager:
    """
    Manages entity ownership for parallel execution.

    Each worker uses a PartitionManager to determine which entities
    it is responsible for generating and processing.

    The partitioning scheme is:
    - partition_id = uuid.int % num_workers
    - A worker owns all UUIDs where partition_id == worker_id
    - Child records (e.g., claims for a policy) are owned by the
      same worker that owns the parent

    Usage:
        partition = PartitionManager(worker_id=0, num_workers=8)
        
        # Check if this worker should process an entity
        if partition.owns(application.application_id):
            # Process the application
            ...
        
        # Generate a UUID owned by this worker
        member_id = partition.generate_owned_uuid(rng)
    """

    def __init__(self, worker_id: int, num_workers: int):
        """
        Initialize the partition manager.

        Args:
            worker_id: This worker's ID (0 to num_workers-1)
            num_workers: Total number of workers
        """
        self.worker_id = worker_id
        self.num_workers = num_workers

    def owns(self, entity_id: UUID) -> bool:
        """
        Check if this worker owns the entity.

        Args:
            entity_id: UUID of the entity

        Returns:
            True if this worker owns the entity
        """
        return is_owned_by_worker(entity_id, self.worker_id, self.num_workers)

    def get_partition(self, entity_id: UUID) -> int:
        """
        Get partition ID for an entity.

        Args:
            entity_id: UUID of the entity

        Returns:
            Partition/worker ID that owns this entity
        """
        return get_partition_id(entity_id, self.num_workers)

    def generate_owned_uuid(self, rng) -> UUID:
        """
        Generate a UUID that this worker owns.

        Uses rejection sampling to find a UUID that maps to this worker.
        On average, this requires num_workers attempts.

        Args:
            rng: NumPy random number generator

        Returns:
            UUID owned by this worker
        """
        while True:
            # Generate random bytes for UUID
            random_bytes = rng.bytes(16)
            # Set version 4 (random) UUID bits
            random_bytes = bytearray(random_bytes)
            random_bytes[6] = (random_bytes[6] & 0x0F) | 0x40  # Version 4
            random_bytes[8] = (random_bytes[8] & 0x3F) | 0x80  # Variant
            uuid = UUID(bytes=bytes(random_bytes))
            
            if self.owns(uuid):
                return uuid

    def filter_owned(self, entity_ids: list[UUID]) -> list[UUID]:
        """
        Filter a list of entity IDs to only those owned by this worker.

        Args:
            entity_ids: List of UUIDs to filter

        Returns:
            List of UUIDs owned by this worker
        """
        return [eid for eid in entity_ids if self.owns(eid)]

    def partition_count(self, entity_ids: list[UUID]) -> dict[int, int]:
        """
        Count entities by partition.

        Useful for debugging partition distribution.

        Args:
            entity_ids: List of UUIDs to count

        Returns:
            Dictionary mapping partition ID to count
        """
        counts: dict[int, int] = {i: 0 for i in range(self.num_workers)}
        for eid in entity_ids:
            partition = self.get_partition(eid)
            counts[partition] += 1
        return counts
