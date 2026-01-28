"""
Database module for Brickwell Health Simulator.

Provides:
- PostgreSQL connection management
- COPY-based batch writer for high-throughput inserts
- Database initialization scripts
"""

from brickwell_health.db.connection import (
    create_engine_for_worker,
    get_connection_string,
)
from brickwell_health.db.writer import BatchWriter

__all__ = [
    "create_engine_for_worker",
    "get_connection_string",
    "BatchWriter",
]
