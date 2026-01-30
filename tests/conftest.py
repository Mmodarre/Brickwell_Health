"""
Shared test fixtures for Brickwell Health Simulator tests.
"""

import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import numpy as np
import pytest

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
from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.reference.loader import ReferenceDataLoader


# =============================================================================
# RNG Fixtures
# =============================================================================


@pytest.fixture
def test_seed() -> int:
    """Fixed seed for reproducible tests."""
    return 42


@pytest.fixture
def test_rng(test_seed: int) -> np.random.Generator:
    """Deterministic random number generator."""
    return np.random.default_rng(test_seed)


# =============================================================================
# Configuration Fixtures
# =============================================================================


@pytest.fixture
def test_config() -> SimulationConfig:
    """Minimal test configuration."""
    return SimulationConfig(
        simulation=SimulationTimeConfig(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            warmup_days=365,
        ),
        scale=ScaleConfig(
            target_member_count=1000,
            target_growth_rate=0.03,
            target_churn_rate=0.10,
        ),
        acquisition=AcquisitionConfig(
            channels={"Online": 0.5, "Phone": 0.3, "Broker": 0.2},
            approval_rate=0.90,
            decision_time_days={"Online": (0.1, 1.0), "Phone": (1.0, 2.0), "Broker": (2.0, 5.0)},
        ),
        policy=PolicyConfig(
            type_distribution={"Single": 0.4, "Couple": 0.3, "Family": 0.3},
            tier_distribution={"Gold": 0.25, "Silver": 0.35, "Bronze": 0.40},
        ),
        claims=ClaimsConfig(),  # Use defaults with APRA-based claiming patterns
        events=EventRatesConfig(
            upgrade_rate=0.05,
            downgrade_rate=0.03,
            cancellation_rate=0.08,
            suspension_rate=0.02,
        ),
        billing=BillingConfig(
            final_payment_success_rate=0.95,
            days_to_arrears=14,
            days_to_suspension=60,
        ),
        database=DatabaseConfig(
            host="localhost",
            port=5432,
            database="brickwell_test",
            username="brickwell",
            password="test_password",
            pool_size=2,
            batch_size=1000,  # Minimum required batch_size
        ),
        parallel=ParallelConfig(
            num_workers=2,
            checkpoint_interval_minutes=60,
        ),
        reference_data_path=Path("tests/fixtures/reference_data"),
        seed=42,
    )


# =============================================================================
# Simulation Environment Fixtures
# =============================================================================


@pytest.fixture
def sim_env(test_rng: np.random.Generator) -> SimulationEnvironment:
    """Test simulation environment."""
    return SimulationEnvironment(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        rng=test_rng,
        worker_id=0,
    )


# =============================================================================
# Generator Fixtures
# =============================================================================


@pytest.fixture
def id_generator(test_rng: np.random.Generator) -> IDGenerator:
    """Test ID generator."""
    return IDGenerator(test_rng, prefix_year=2024)


# =============================================================================
# Reference Data Fixtures
# =============================================================================


@pytest.fixture
def test_reference_data_dir(tmp_path: Path) -> Path:
    """Create temporary reference data directory with minimal test data."""
    import json

    ref_dir = tmp_path / "reference"
    ref_dir.mkdir()

    # Create minimal reference data files
    _create_test_products(ref_dir)
    _create_test_states(ref_dir)
    _create_test_benefit_categories(ref_dir)
    _create_test_clinical_categories(ref_dir)
    _create_test_rebate_tiers(ref_dir)

    return ref_dir


def _create_test_products(ref_dir: Path) -> None:
    """Create test products file."""
    import json

    products = [
        {
            "product_id": 1,
            "product_code": "GOLD-HOSP-100",
            "product_name": "Gold Hospital 100",
            "product_type_id": 1,
            "product_tier_id": 1,
            "is_hospital": True,
            "is_extras": False,
            "default_excess": 500,
            "status": "Active",
            "available_policy_types": "Single,Couple,Family,SingleParent",
        },
        {
            "product_id": 2,
            "product_code": "SILVER-HOSP-250",
            "product_name": "Silver Hospital 250",
            "product_type_id": 1,
            "product_tier_id": 2,
            "is_hospital": True,
            "is_extras": False,
            "default_excess": 250,
            "status": "Active",
            "available_policy_types": "Single,Couple,Family,SingleParent",
        },
        {
            "product_id": 3,
            "product_code": "TOP-EXTRAS",
            "product_name": "Top Extras",
            "product_type_id": 2,
            "product_tier_id": None,
            "is_hospital": False,
            "is_extras": True,
            "default_excess": None,
            "status": "Active",
            "available_policy_types": "Single,Couple,Family,SingleParent",
        },
    ]

    with open(ref_dir / "product.json", "w") as f:
        json.dump(products, f)

    # Product types
    product_types = [
        {"product_type_id": 1, "type_code": "HOSP", "type_name": "Hospital"},
        {"product_type_id": 2, "type_code": "EXT", "type_name": "Extras"},
    ]
    with open(ref_dir / "product_type.json", "w") as f:
        json.dump(product_types, f)

    # Product tiers
    product_tiers = [
        {"product_tier_id": 1, "tier_code": "GOLD", "tier_name": "Gold", "tier_level": 1},
        {"product_tier_id": 2, "tier_code": "SILVER", "tier_name": "Silver", "tier_level": 2},
        {"product_tier_id": 3, "tier_code": "BRONZE", "tier_name": "Bronze", "tier_level": 3},
        {"product_tier_id": 4, "tier_code": "BASIC", "tier_name": "Basic", "tier_level": 4},
    ]
    with open(ref_dir / "product_tier.json", "w") as f:
        json.dump(product_tiers, f)


def _create_test_states(ref_dir: Path) -> None:
    """Create test states file."""
    import json

    states = [
        {"state_territory_id": 1, "state_code": "NSW", "state_name": "New South Wales"},
        {"state_territory_id": 2, "state_code": "VIC", "state_name": "Victoria"},
        {"state_territory_id": 3, "state_code": "QLD", "state_name": "Queensland"},
    ]

    with open(ref_dir / "state_territory.json", "w") as f:
        json.dump(states, f)


def _create_test_benefit_categories(ref_dir: Path) -> None:
    """Create test benefit categories file."""
    import json

    categories = [
        {"benefit_category_id": 1, "category_code": "DENTAL", "category_name": "Dental", "category_type": "Extras"},
        {"benefit_category_id": 2, "category_code": "OPTICAL", "category_name": "Optical", "category_type": "Extras"},
        {"benefit_category_id": 3, "category_code": "PHYSIO", "category_name": "Physiotherapy", "category_type": "Extras"},
    ]

    with open(ref_dir / "benefit_category.json", "w") as f:
        json.dump(categories, f)


def _create_test_clinical_categories(ref_dir: Path) -> None:
    """Create test clinical categories file."""
    import json

    categories = [
        {"clinical_category_id": 1, "category_code": "CARDIAC", "category_name": "Cardiac"},
        {"clinical_category_id": 2, "category_code": "JOINT", "category_name": "Joint Replacement"},
        {"clinical_category_id": 3, "category_code": "OBSTET", "category_name": "Obstetrics"},
    ]

    with open(ref_dir / "clinical_category.json", "w") as f:
        json.dump(categories, f)


def _create_test_rebate_tiers(ref_dir: Path) -> None:
    """Create test PHI rebate tiers file."""
    import json

    tiers = [
        {
            "rebate_tier_id": 1,
            "financial_year": "2024-2025",
            "tier_number": 0,
            "tier_name": "Base",
            "single_threshold_min": 0,
            "single_threshold_max": 97000,
            "family_threshold_min": 0,
            "family_threshold_max": 194000,
            "rebate_pct_under_65": "24.608",
            "rebate_pct_65_to_69": "28.710",
            "rebate_pct_70_plus": "32.812",
            "effective_date": "2024-07-01",
            "is_active": True,
        },
        {
            "rebate_tier_id": 2,
            "financial_year": "2024-2025",
            "tier_number": 1,
            "tier_name": "Tier 1",
            "single_threshold_min": 97000,
            "single_threshold_max": 113000,
            "family_threshold_min": 194000,
            "family_threshold_max": 226000,
            "rebate_pct_under_65": "16.405",
            "rebate_pct_65_to_69": "20.507",
            "rebate_pct_70_plus": "24.608",
            "effective_date": "2024-07-01",
            "is_active": True,
        },
    ]

    with open(ref_dir / "phi_rebate_tier.json", "w") as f:
        json.dump(tiers, f)


@pytest.fixture
def test_reference(test_reference_data_dir: Path) -> ReferenceDataLoader:
    """Test reference data loader."""
    return ReferenceDataLoader(test_reference_data_dir)


# =============================================================================
# Entity Fixtures
# =============================================================================


@pytest.fixture
def sample_member_data() -> dict:
    """Sample member data for testing."""
    return {
        "member_id": uuid4(),
        "member_number": "MEM-2024-000001",
        "title": "Mr",
        "first_name": "John",
        "middle_name": "Robert",
        "last_name": "Smith",
        "date_of_birth": date(1985, 6, 15),
        "gender": "Male",
        "state": "NSW",
        "postcode": "2000",
        "email": "john.smith@example.com",
    }


@pytest.fixture
def sample_policy_data() -> dict:
    """Sample policy data for testing."""
    return {
        "policy_id": uuid4(),
        "policy_number": "POL-2024-000001",
        "product_id": 1,
        "policy_status": "Active",
        "policy_type": "Single",
        "effective_date": date(2024, 1, 1),
        "premium_amount": Decimal("200.00"),
        "excess_amount": Decimal("500.00"),
        "state_of_residence": "NSW",
    }
