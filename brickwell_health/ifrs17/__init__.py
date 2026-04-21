"""
IFRS 17 / PAA LRC accounting engine.

Produces post-simulation LRC balance + roll-forward movements per cohort per
month, detects onerous contracts with loss components, and exports results as
CSV for downstream consumption.
"""

from brickwell_health.ifrs17.assumptions import IFRS17Assumptions, load_assumptions
from brickwell_health.ifrs17.cohort_mapper import CohortMapper

__all__ = [
    "IFRS17Assumptions",
    "load_assumptions",
    "CohortMapper",
    "IFRS17Engine",
]


def __getattr__(name: str):
    if name == "IFRS17Engine":
        from brickwell_health.ifrs17.engine import IFRS17Engine
        return IFRS17Engine
    raise AttributeError(f"module 'brickwell_health.ifrs17' has no attribute {name!r}")
