"""
Structured logging configuration for Brickwell Health Simulator.

Uses structlog for structured, contextual logging.
"""

import logging
import sys
from typing import Any

import structlog


def configure_logging(
    level: str = "INFO",
    json_output: bool = False,
    include_timestamp: bool = True,
) -> None:
    """
    Configure structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_output: If True, output logs as JSON
        include_timestamp: If True, include timestamp in logs
    """
    # Set up standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )

    # Configure processors
    processors: list[Any] = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if include_timestamp:
        processors.insert(0, structlog.processors.TimeStamper(fmt="iso"))

    if json_output:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """
    Get a structured logger.

    Args:
        name: Logger name (optional)

    Returns:
        Bound structlog logger
    """
    return structlog.get_logger(name)


class SimulationLogger:
    """
    Specialized logger for simulation events.

    Provides convenience methods for common simulation logging patterns.

    Usage:
        logger = SimulationLogger(worker_id=0)
        logger.member_created(member_id, member_number)
        logger.claim_submitted(claim_id, claim_type, amount)
    """

    def __init__(self, worker_id: int = 0):
        """
        Initialize the simulation logger.

        Args:
            worker_id: Worker process identifier
        """
        self.worker_id = worker_id
        self._logger = structlog.get_logger().bind(worker_id=worker_id)

    def bind(self, **kwargs: Any) -> "SimulationLogger":
        """Bind additional context to the logger."""
        self._logger = self._logger.bind(**kwargs)
        return self

    # Simulation lifecycle events
    def simulation_started(self, start_date: str, end_date: str, **kwargs: Any) -> None:
        """Log simulation start."""
        self._logger.info(
            "simulation_started",
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    def simulation_progress(
        self,
        current_date: str,
        progress_pct: float,
        **kwargs: Any,
    ) -> None:
        """Log simulation progress."""
        self._logger.info(
            "simulation_progress",
            current_date=current_date,
            progress_pct=f"{progress_pct:.1f}%",
            **kwargs,
        )

    def simulation_completed(self, elapsed_seconds: float, **kwargs: Any) -> None:
        """Log simulation completion."""
        self._logger.info(
            "simulation_completed",
            elapsed_seconds=elapsed_seconds,
            **kwargs,
        )

    # Entity creation events
    def member_created(self, member_id: str, member_number: str, **kwargs: Any) -> None:
        """Log member creation."""
        self._logger.debug(
            "member_created",
            member_id=member_id,
            member_number=member_number,
            **kwargs,
        )

    def policy_created(self, policy_id: str, policy_number: str, **kwargs: Any) -> None:
        """Log policy creation."""
        self._logger.debug(
            "policy_created",
            policy_id=policy_id,
            policy_number=policy_number,
            **kwargs,
        )

    def claim_submitted(
        self,
        claim_id: str,
        claim_type: str,
        amount: float,
        **kwargs: Any,
    ) -> None:
        """Log claim submission."""
        self._logger.debug(
            "claim_submitted",
            claim_id=claim_id,
            claim_type=claim_type,
            amount=amount,
            **kwargs,
        )

    # Batch events
    def batch_flushed(self, table: str, records: int, total: int, **kwargs: Any) -> None:
        """Log batch flush."""
        self._logger.debug(
            "batch_flushed",
            table=table,
            records=records,
            total=total,
            **kwargs,
        )

    # Error events
    def error(self, message: str, **kwargs: Any) -> None:
        """Log an error."""
        self._logger.error(message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log a warning."""
        self._logger.warning(message, **kwargs)
