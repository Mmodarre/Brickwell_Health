"""
Database connection management for Brickwell Health Simulator.

Provides connection factory functions for worker processes.
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from brickwell_health.config.models import DatabaseConfig


def get_connection_string(config: DatabaseConfig) -> str:
    """
    Build PostgreSQL connection string.

    Args:
        config: Database configuration

    Returns:
        PostgreSQL connection string for psycopg3
    """
    return (
        f"postgresql+psycopg://{config.username}:{config.password}"
        f"@{config.host}:{config.port}/{config.database}"
    )


def create_engine_for_worker(config: DatabaseConfig, worker_id: int) -> Engine:
    """
    Create SQLAlchemy engine for a worker process.

    Each worker gets its own connection pool to avoid contention.
    Connections are labeled with worker ID for debugging.

    Args:
        config: Database configuration
        worker_id: Worker process identifier

    Returns:
        SQLAlchemy Engine instance
    """
    connection_string = get_connection_string(config)

    engine = create_engine(
        connection_string,
        pool_size=config.pool_size,
        pool_pre_ping=True,
        # Label connections for debugging + WAL optimization
        connect_args={
            "application_name": f"brickwell_worker_{worker_id}",
            "options": "-c synchronous_commit=off",  # WAL performance: async commit
        },
    )

    return engine


def create_engine_from_config(config: DatabaseConfig) -> Engine:
    """
    Create SQLAlchemy engine from configuration.

    For use in single-process contexts (e.g., database initialization).

    Args:
        config: Database configuration

    Returns:
        SQLAlchemy Engine instance
    """
    connection_string = get_connection_string(config)

    engine = create_engine(
        connection_string,
        pool_size=config.pool_size,
        pool_pre_ping=True,
    )

    return engine
