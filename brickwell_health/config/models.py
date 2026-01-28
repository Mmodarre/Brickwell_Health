"""
Pydantic configuration models for Brickwell Health Simulator.

These models define the structure and validation for simulation configuration.
"""

from datetime import date
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings


class SimulationTimeConfig(BaseModel):
    """Simulation time boundaries."""

    start_date: date = Field(..., description="Simulation start date")
    end_date: date = Field(..., description="Simulation end date")
    warmup_days: int = Field(
        default=730,
        ge=365,
        description=(
            "Warmup period in days. Minimum 365 days recommended "
            "since simulation starts with zero members and needs time "
            "to build population before claims can occur (waiting periods)."
        ),
    )

    @field_validator("end_date")
    @classmethod
    def end_after_start(cls, v: date, info) -> date:
        """Ensure end_date is after start_date."""
        if "start_date" in info.data and v <= info.data["start_date"]:
            raise ValueError("end_date must be after start_date")
        return v


class ScaleConfig(BaseModel):
    """Scale parameters for member population."""

    target_member_count: int = Field(
        ...,
        ge=1000,
        description=(
            "Target member count to reach by end of warmup period. "
            "Acquisition rate is calibrated to achieve this."
        ),
    )
    target_growth_rate: float = Field(
        default=0.03,
        ge=0,
        le=0.2,
        description="Annual growth rate after warmup (net of churn)",
    )
    target_churn_rate: float = Field(
        default=0.10,
        ge=0,
        le=0.3,
        description="Annual churn rate (cancellations/lapses)",
    )


class AcquisitionConfig(BaseModel):
    """Member acquisition parameters."""

    channels: dict[str, float] = Field(
        default={
            "Online": 0.45,
            "Phone": 0.25,
            "Broker": 0.20,
            "Corporate": 0.10,
        },
        description="Channel distribution (must sum to 1.0)",
    )
    approval_rate: float = Field(
        default=0.92,
        ge=0.5,
        le=1.0,
        description="Application approval rate",
    )
    decision_time_days: dict[str, tuple[float, float]] = Field(
        default={
            "Online": (0.1, 1.0),
            "Phone": (1.0, 3.0),
            "Broker": (2.0, 7.0),
            "Corporate": (5.0, 14.0),
        },
        description="Decision time range by channel (min, max days)",
    )

    @field_validator("channels")
    @classmethod
    def channels_sum_to_one(cls, v: dict[str, float]) -> dict[str, float]:
        """Ensure channel weights sum to approximately 1.0."""
        total = sum(v.values())
        if not 0.99 <= total <= 1.01:
            raise ValueError(f"Channel weights must sum to 1.0, got {total}")
        return v


class PolicyConfig(BaseModel):
    """Policy type distribution."""

    type_distribution: dict[str, float] = Field(
        default={
            "Single": 0.35,
            "Couple": 0.25,
            "Family": 0.30,
            "SingleParent": 0.10,
        },
        description="Policy type distribution (must sum to 1.0)",
    )
    tier_distribution: dict[str, float] = Field(
        default={
            "Gold": 0.20,
            "Silver": 0.35,
            "Bronze": 0.30,
            "Basic": 0.15,
        },
        description="Hospital tier distribution (must sum to 1.0)",
    )

    @field_validator("type_distribution", "tier_distribution")
    @classmethod
    def distribution_sum_to_one(cls, v: dict[str, float]) -> dict[str, float]:
        """Ensure distribution weights sum to approximately 1.0."""
        total = sum(v.values())
        if not 0.99 <= total <= 1.01:
            raise ValueError(f"Distribution weights must sum to 1.0, got {total}")
        return v


class HospitalSeverityConfig(BaseModel):
    """Hospital claim severity parameters (lognormal distribution)."""

    mu: float = Field(
        default=8.0,
        description="Lognormal mu parameter. mu=8.0, sigma=1.5 produces median ~$2,981",
    )
    sigma: float = Field(
        default=1.5,
        ge=0,
        description="Lognormal sigma parameter",
    )


class HighClaimTier(BaseModel):
    """High-value claim tier definition."""

    range: tuple[int, int] = Field(
        ...,
        description="(min, max) amount range for this tier",
    )
    weight: float = Field(
        ...,
        ge=0,
        le=1,
        description="Probability weight for this tier (within high claims)",
    )


class ServiceCostConfig(BaseModel):
    """Cost parameters for a service type (normal distribution)."""

    mean: float = Field(..., ge=0, description="Mean cost ($)")
    std: float = Field(..., ge=0, description="Standard deviation ($)")


class ExtrasServiceConfig(BaseModel):
    """Configuration for an extras service type."""

    frequency: float = Field(
        ...,
        ge=0,
        description="Annual Poisson lambda (expected claims per year)",
    )
    mean: float = Field(..., ge=0, description="Mean cost ($)")
    std: float = Field(..., ge=0, description="Standard deviation ($)")
    age_65_multiplier: float = Field(
        default=1.0,
        ge=1.0,
        description="Frequency multiplier for members 65+",
    )


class AmbulanceConfig(BaseModel):
    """Ambulance claim configuration."""

    frequency: float = Field(
        default=0.02,
        ge=0,
        description="Annual Poisson lambda",
    )
    mean: float = Field(default=950.0, ge=0, description="Mean cost ($)")
    std: float = Field(default=200.0, ge=0, description="Standard deviation ($)")


class ClaimApprovalConfig(BaseModel):
    """
    Claim approval/denial parameters based on APRA/PHIO research.

    Stochastic approval rates are applied AFTER deterministic checks pass.
    Deterministic checks include: limits_exhausted, waiting_period, membership_inactive.
    """

    hospital_approval_rate: float = Field(
        default=0.98,
        ge=0.5,
        le=1.0,
        description="Hospital claim approval rate (2% denial)",
    )
    extras_approval_rate: float = Field(
        default=0.92,
        ge=0.5,
        le=1.0,
        description="Extras claim approval rate (8% denial)",
    )
    ambulance_approval_rate: float = Field(
        default=0.95,
        ge=0.5,
        le=1.0,
        description="Ambulance claim approval rate (5% denial)",
    )
    stochastic_denial_weights: dict[str, float] = Field(
        default={
            "policy_exclusions": 0.50,
            "pre_existing": 0.24,
            "provider_issues": 0.16,
            "administrative": 0.10,
        },
        description=(
            "Weights for stochastic denial reasons (must sum to 1.0). "
            "pre_existing only applies to hospital claims."
        ),
    )

    @field_validator("stochastic_denial_weights")
    @classmethod
    def weights_sum_to_one(cls, v: dict[str, float]) -> dict[str, float]:
        """Ensure denial weights sum to approximately 1.0."""
        total = sum(v.values())
        if not 0.99 <= total <= 1.01:
            raise ValueError(f"Stochastic denial weights must sum to 1.0, got {total}")
        return v


class ClaimsConfig(BaseModel):
    """Claims generation parameters based on APRA 2024-2025 data."""

    # Legacy parameter for rejected claims
    uncovered_claim_attempt_rate: float = Field(
        default=0.05,
        ge=0,
        le=0.3,
        description=(
            "Probability that a member attempts to claim for a service type "
            "they don't have coverage for. These claims are rejected."
        ),
    )

    # Hospital frequency (Poisson lambda by age group)
    hospital_frequency: dict[str, float] = Field(
        default={
            "18-30": 0.3,
            "31-45": 0.5,
            "46-60": 1.2,
            "61-70": 2.0,
            "71+": 2.5,
        },
        description="Poisson lambda for hospital admissions by age group",
    )

    # Hospital severity (lognormal parameters)
    hospital_severity: HospitalSeverityConfig = Field(
        default_factory=HospitalSeverityConfig,
        description="Lognormal parameters for hospital claim amounts",
    )

    # High-claim distribution
    high_claim_probability: float = Field(
        default=0.088,
        ge=0,
        le=1.0,
        description="Probability that a hospital claim exceeds $10k (8.8% per APRA)",
    )
    high_claim_tiers: list[HighClaimTier] = Field(
        default=[
            HighClaimTier(range=(10000, 20000), weight=0.634),
            HighClaimTier(range=(20000, 30000), weight=0.231),
            HighClaimTier(range=(30000, 50000), weight=0.070),
            HighClaimTier(range=(50000, 100000), weight=0.031),
            HighClaimTier(range=(100000, 200000), weight=0.0024),
            HighClaimTier(range=(200000, 450000), weight=0.0002),
        ],
        description="Tiered distribution for high-value claims (>$10k)",
    )

    # Dental sub-category frequencies
    dental_frequency: dict[str, float] = Field(
        default={
            "preventative": 2.0,
            "general": 0.5,
            "major": 0.1,
        },
        description="Poisson lambda by dental sub-category",
    )

    # Dental costs by sub-category
    dental_costs: dict[str, ServiceCostConfig] = Field(
        default={
            "preventative": ServiceCostConfig(mean=175, std=35),
            "general": ServiceCostConfig(mean=280, std=90),
            "major": ServiceCostConfig(mean=1300, std=450),
        },
        description="Cost parameters by dental sub-category",
    )

    # Other extras services
    optical: ExtrasServiceConfig = Field(
        default_factory=lambda: ExtrasServiceConfig(frequency=0.8, mean=350, std=100),
        description="Optical claim parameters",
    )
    physiotherapy: ExtrasServiceConfig = Field(
        default_factory=lambda: ExtrasServiceConfig(
            frequency=1.5, mean=85, std=20, age_65_multiplier=1.5
        ),
        description="Physiotherapy claim parameters",
    )
    chiropractic: ExtrasServiceConfig = Field(
        default_factory=lambda: ExtrasServiceConfig(frequency=1.2, mean=70, std=15),
        description="Chiropractic claim parameters",
    )

    # Ambulance
    ambulance: AmbulanceConfig = Field(
        default_factory=AmbulanceConfig,
        description="Ambulance claim parameters",
    )

    # Approval/denial settings
    approval: ClaimApprovalConfig = Field(
        default_factory=ClaimApprovalConfig,
        description="Claim approval/denial parameters",
    )


class ChurnModelConfig(BaseModel):
    """
    Churn prediction model parameters.

    These parameters control the churn model behavior including
    life event probability, claims value thresholds, and dissatisfaction triggers.
    """

    life_event_annual_probability: float = Field(
        default=0.08,
        ge=0,
        le=0.3,
        description=(
            "Annual probability of a life event (job loss, divorce, etc.) "
            "triggering potential churn. 8% default based on research."
        ),
    )
    high_claims_threshold: float = Field(
        default=0.50,
        ge=0.1,
        le=1.0,
        description=(
            "Claims-to-premium ratio threshold for 'high value' perception. "
            "Members with claims >= 50% of premium paid are less likely to churn."
        ),
    )
    no_claims_months: int = Field(
        default=6,
        ge=1,
        le=24,
        description=(
            "Months without claims to trigger 'no recent claims' flag. "
            "Members with no claims in this period perceive low value."
        ),
    )
    dissatisfaction_denial_threshold: int = Field(
        default=1,
        ge=1,
        description=(
            "Number of claim denials to trigger dissatisfaction flag. "
            "Members with denied claims are more likely to churn."
        ),
    )


class EventRatesConfig(BaseModel):
    """
    Lifecycle event rates (annual percentages).

    These rates are applied daily: daily_prob = annual_rate / 365
    Events are mutually exclusive per day (only one event per policy per day).
    """

    upgrade_rate: float = Field(
        default=0.05,
        ge=0,
        le=0.2,
        description=(
            "Annual rate of upgrades (Bronze→Silver, Silver→Gold). "
            "5% = ~5,000 upgrades per year per 100k policies."
        ),
    )
    downgrade_rate: float = Field(
        default=0.03,
        ge=0,
        le=0.2,
        description=(
            "Annual rate of downgrades (Gold→Silver, Silver→Bronze). "
            "3% = ~3,000 downgrades per year per 100k policies."
        ),
    )
    cancellation_rate: float = Field(
        default=0.08,
        ge=0,
        le=0.3,
        description=(
            "Annual churn rate (policy cancellations). "
            "8% = industry average voluntary churn."
        ),
    )
    suspension_rate: float = Field(
        default=0.02,
        ge=0,
        le=0.1,
        description=(
            "Annual rate of suspensions (overseas travel, hardship). "
            "2% = ~2,000 suspensions per year per 100k policies."
        ),
    )


class BillingConfig(BaseModel):
    """Billing parameters."""

    final_payment_success_rate: float = Field(
        default=0.95,
        ge=0.5,
        le=1.0,
        description=(
            "Final payment success rate after all retry attempts. "
            "Per-attempt rate is calculated automatically based on max_debit_retries. "
            "E.g., 0.95 final rate with 2 retries (3 total attempts) = 63.2% per attempt."
        ),
    )
    max_debit_retries: int = Field(
        default=2,
        ge=0,
        le=5,
        description=(
            "Maximum number of retry attempts after initial failure. "
            "Total attempts = 1 (initial) + max_debit_retries."
        ),
    )
    retry_interval_days: int = Field(
        default=3,
        ge=1,
        le=14,
        description="Days between retry attempts",
    )
    days_to_arrears: int = Field(
        default=14,
        ge=7,
        le=60,
        description="Days overdue before marking as arrears",
    )
    days_to_suspension: int = Field(
        default=30,
        ge=14,
        le=60,
        description="Days overdue before suspending policy (claims blocked, can reinstate)",
    )
    days_to_lapse: int = Field(
        default=60,
        ge=30,
        le=120,
        description="Days overdue before lapsing policy (~2 months). Lapsed policies cannot be reinstated - new policy required.",
    )


class DatabaseConfig(BaseModel):
    """Database connection settings."""

    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    database: str = Field(default="brickwell_health", description="Database name")
    username: str = Field(default="brickwell", description="Database username")
    password: str = Field(default="", description="Database password")
    pool_size: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Connection pool size per worker",
    )
    batch_size: int = Field(
        default=10000,
        ge=1000,
        le=100000,
        description="Batch size for COPY operations",
    )

    @property
    def connection_string(self) -> str:
        """Build PostgreSQL connection string."""
        return (
            f"postgresql+psycopg://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class ParallelConfig(BaseModel):
    """Parallel execution settings."""

    num_workers: int = Field(
        default=4,
        ge=1,
        le=32,
        description="Number of parallel worker processes",
    )
    checkpoint_interval_minutes: int = Field(
        default=15,
        ge=1,
        description="Minutes between checkpoint saves",
    )


class SimulationConfig(BaseSettings):
    """
    Root simulation configuration.

    This is the main configuration class that contains all simulation settings.
    Values can be loaded from YAML files and overridden via environment variables.
    """

    simulation: SimulationTimeConfig
    scale: ScaleConfig
    acquisition: AcquisitionConfig = Field(default_factory=AcquisitionConfig)
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    claims: ClaimsConfig = Field(default_factory=ClaimsConfig)
    churn: ChurnModelConfig = Field(default_factory=ChurnModelConfig)
    events: EventRatesConfig = Field(default_factory=EventRatesConfig)
    billing: BillingConfig = Field(default_factory=BillingConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    parallel: ParallelConfig = Field(default_factory=ParallelConfig)

    reference_data_path: Path = Field(
        default=Path("data/reference"),
        description="Path to reference data JSON files",
    )
    seed: int = Field(
        default=42,
        description="Base random seed for reproducibility",
    )

    model_config = {
        "env_prefix": "BRICKWELL_",
        "env_nested_delimiter": "__",
    }
