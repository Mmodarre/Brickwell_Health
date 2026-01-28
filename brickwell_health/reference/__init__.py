"""
Reference data module for Brickwell Health Simulator.

Provides:
- JSON reference data loading with caching
- Temporal lookup functions for effective-dated records
- Reference data models
"""

from brickwell_health.reference.loader import ReferenceDataLoader, get_effective_record

__all__ = [
    "ReferenceDataLoader",
    "get_effective_record",
]
