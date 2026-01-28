"""
Configuration validation for Brickwell Health Simulator.

Provides additional validation beyond Pydantic model validation.
"""

from pathlib import Path

import structlog

from brickwell_health.config.models import SimulationConfig

logger = structlog.get_logger()


class ConfigurationError(Exception):
    """Raised when configuration is invalid."""

    pass


def validate_config(config: SimulationConfig) -> list[str]:
    """
    Validate simulation configuration.

    Performs validation checks beyond what Pydantic models provide,
    such as cross-field validation and resource availability checks.

    Args:
        config: SimulationConfig to validate

    Returns:
        List of warning messages (non-fatal issues)

    Raises:
        ConfigurationError: If configuration has fatal issues
    """
    warnings: list[str] = []
    errors: list[str] = []

    # Check simulation duration is reasonable
    duration_days = (config.simulation.end_date - config.simulation.start_date).days
    if duration_days < config.simulation.warmup_days:
        errors.append(
            f"Simulation duration ({duration_days} days) is less than "
            f"warmup period ({config.simulation.warmup_days} days)"
        )

    analysis_days = duration_days - config.simulation.warmup_days
    if analysis_days < 365:
        warnings.append(
            f"Analysis period after warmup is only {analysis_days} days. "
            "Consider extending end_date for more meaningful results."
        )

    # Check reference data path exists
    ref_path = Path(config.reference_data_path)
    if not ref_path.exists():
        errors.append(f"Reference data path does not exist: {ref_path}")
    elif not ref_path.is_dir():
        errors.append(f"Reference data path is not a directory: {ref_path}")
    else:
        # Check for required reference files
        required_files = [
            "product.json",
            "product_type.json",
            "product_tier.json",
            "state_territory.json",
            "benefit_category.json",
            "clinical_category.json",
        ]
        missing_files = [f for f in required_files if not (ref_path / f).exists()]
        if missing_files:
            warnings.append(
                f"Missing reference data files: {', '.join(missing_files)}. "
                "These may be required during simulation."
            )

    # Check worker count vs CPU
    import multiprocessing

    cpu_count = multiprocessing.cpu_count()
    if config.parallel.num_workers > cpu_count:
        warnings.append(
            f"num_workers ({config.parallel.num_workers}) exceeds CPU count ({cpu_count}). "
            "Performance may be degraded."
        )

    # Check acquisition rate is achievable
    warmup_days = config.simulation.warmup_days
    target_members = config.scale.target_member_count
    daily_rate_needed = target_members / warmup_days / config.acquisition.approval_rate

    if daily_rate_needed > 1000:
        warnings.append(
            f"Acquisition rate ({daily_rate_needed:.0f}/day) is very high. "
            "Consider increasing warmup_days or reducing target_member_count."
        )

    # Validate event rates don't exceed 100% annual
    total_event_rate = (
        config.events.upgrade_rate
        + config.events.downgrade_rate
        + config.events.cancellation_rate
        + config.events.suspension_rate
    )
    if total_event_rate > 0.5:
        warnings.append(
            f"Combined annual event rate ({total_event_rate:.0%}) is very high. "
            "This may result in unstable simulation behavior."
        )

    # Log warnings
    for warning in warnings:
        logger.warning("config_validation_warning", message=warning)

    # Raise if any errors
    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        raise ConfigurationError(error_msg)

    return warnings


def validate_database_connection(config: SimulationConfig) -> bool:
    """
    Test database connection using configuration.

    Args:
        config: SimulationConfig with database settings

    Returns:
        True if connection successful

    Raises:
        ConfigurationError: If connection fails
    """
    from sqlalchemy import create_engine, text

    try:
        engine = create_engine(config.database.connection_string)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        raise ConfigurationError(f"Database connection failed: {e}") from e
