"""
Core simulation module for Brickwell Health Simulator.

Provides:
- SimPy simulation environment wrapper
- Partition management for parallel execution
- Process orchestration
"""

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.core.partition import PartitionManager, get_partition_id, is_owned_by_worker

__all__ = [
    "SimulationEnvironment",
    "PartitionManager",
    "get_partition_id",
    "is_owned_by_worker",
]
