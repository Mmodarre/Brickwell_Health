"""
Configuration module for Brickwell Health Simulator.

This module provides:
- Pydantic configuration models
- YAML configuration loading
- Regulatory constants (LHC, age-based discount)
- Configuration validation
"""

from brickwell_health.config.models import (
    SimulationConfig,
    SimulationTimeConfig,
    ScaleConfig,
    AcquisitionConfig,
    PolicyConfig,
    ClaimsConfig,
    EventRatesConfig,
    BillingConfig,
    DatabaseConfig,
    ParallelConfig,
)
from brickwell_health.config.loader import load_config
from brickwell_health.config.validation import validate_config

__all__ = [
    "SimulationConfig",
    "SimulationTimeConfig",
    "ScaleConfig",
    "AcquisitionConfig",
    "PolicyConfig",
    "ClaimsConfig",
    "EventRatesConfig",
    "BillingConfig",
    "DatabaseConfig",
    "ParallelConfig",
    "load_config",
    "validate_config",
]
