"""
Unit tests for fraud generation system.

Tests FraudType enum, FraudConfig, FraudGenerator methods,
and SharedState fraud tracking.
"""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import numpy as np
import pytest

from brickwell_health.config.models import (
    FraudConfig,
    FraudTypeConfig,
    DRGUpcodingConfig,
    ExtrasUpcodingConfig,
    ExactDuplicateConfig,
    NearDuplicateConfig,
    UnbundlingConfig,
    PhantomBillingConfig,
    ProviderOutlierConfig,
    TemporalAnomalyConfig,
    GeographicAnomalyConfig,
)
from brickwell_health.core.shared_state import SharedState
from brickwell_health.domain.claims import ClaimCreate
from brickwell_health.domain.enums import (
    ClaimChannel,
    ClaimStatus,
    ClaimType,
    FraudType,
)
from brickwell_health.generators.fraud_generator import FraudGenerator
from brickwell_health.generators.id_generator import IDGenerator


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def fraud_rng():
    """Deterministic RNG for fraud tests."""
    return np.random.default_rng(12345)


@pytest.fixture
def fraud_config():
    """Default fraud config (enabled)."""
    return FraudConfig(enabled=True)


@pytest.fixture
def fraud_config_disabled():
    """Disabled fraud config."""
    return FraudConfig(enabled=False)


@pytest.fixture
def mock_reference():
    """Minimal mock reference data loader."""
    ref = MagicMock()
    ref.get_providers_by_type_and_state.return_value = [
        {"provider_id": 100, "provider_name": "Test Provider VIC"},
        {"provider_id": 101, "provider_name": "Test Provider VIC 2"},
    ]
    return ref


@pytest.fixture
def mock_sim_env():
    """Mock simulation environment."""
    env = MagicMock()
    env.current_date = date(2024, 6, 15)
    env.current_datetime = datetime(2024, 6, 15, 10, 0, 0)
    return env


@pytest.fixture
def id_gen(fraud_rng):
    """Test ID generator."""
    return IDGenerator(fraud_rng, prefix_year=2024)


@pytest.fixture
def fraud_gen(fraud_rng, mock_reference, id_gen, mock_sim_env, fraud_config):
    """Configured FraudGenerator instance."""
    return FraudGenerator(
        rng=fraud_rng,
        reference=mock_reference,
        id_generator=id_gen,
        sim_env=mock_sim_env,
        fraud_config=fraud_config,
    )


@pytest.fixture
def shared_state():
    """Fresh SharedState for fraud tests."""
    return SharedState()


@pytest.fixture
def sample_extras_claim():
    """Sample extras claim for fraud modification tests."""
    return ClaimCreate(
        claim_id=uuid4(),
        claim_number="CLM-2024-00000001",
        policy_id=uuid4(),
        member_id=uuid4(),
        coverage_id=uuid4(),
        claim_type=ClaimType.EXTRAS,
        claim_status=ClaimStatus.SUBMITTED,
        service_date=date(2024, 6, 10),
        lodgement_date=date(2024, 6, 10),
        total_charge=Decimal("250.00"),
        total_benefit=Decimal("175.00"),
        total_gap=Decimal("75.00"),
        claim_channel=ClaimChannel.ONLINE,
    )


@pytest.fixture
def sample_hospital_claim():
    """Sample hospital claim for fraud modification tests."""
    return ClaimCreate(
        claim_id=uuid4(),
        claim_number="CLM-2024-00000002",
        policy_id=uuid4(),
        member_id=uuid4(),
        coverage_id=uuid4(),
        claim_type=ClaimType.HOSPITAL,
        claim_status=ClaimStatus.SUBMITTED,
        service_date=date(2024, 6, 12),
        lodgement_date=date(2024, 6, 12),
        total_charge=Decimal("5000.00"),
        total_benefit=Decimal("4500.00"),
        total_gap=Decimal("500.00"),
        provider_id=42,
        hospital_id=10,
        claim_channel=ClaimChannel.HOSPITAL,
    )


# =============================================================================
# FraudType Enum Tests
# =============================================================================


class TestFraudType:
    """Test FraudType enum."""

    def test_all_nine_fraud_types_exist(self):
        """All 9 fraud types are defined."""
        assert len(FraudType) == 9

    def test_fraud_type_values(self):
        """Enum values match expected strings."""
        assert FraudType.DRG_UPCODING.value == "DRGUpcoding"
        assert FraudType.EXTRAS_UPCODING.value == "ExtrasUpcoding"
        assert FraudType.EXACT_DUPLICATE.value == "ExactDuplicate"
        assert FraudType.NEAR_DUPLICATE.value == "NearDuplicate"
        assert FraudType.UNBUNDLING.value == "Unbundling"
        assert FraudType.PHANTOM_BILLING.value == "PhantomBilling"
        assert FraudType.PROVIDER_OUTLIER.value == "ProviderOutlier"
        assert FraudType.TEMPORAL_ANOMALY.value == "TemporalAnomaly"
        assert FraudType.GEOGRAPHIC_ANOMALY.value == "GeographicAnomaly"

    def test_fraud_type_is_str_enum(self):
        """FraudType inherits from str."""
        assert isinstance(FraudType.DRG_UPCODING, str)


# =============================================================================
# FraudConfig Tests
# =============================================================================


class TestFraudConfig:
    """Test FraudConfig defaults and validation."""

    def test_default_disabled(self):
        """FraudConfig is disabled by default."""
        config = FraudConfig()
        assert config.enabled is False

    def test_default_fraud_rate(self):
        """Default fraud rate is 6%."""
        config = FraudConfig()
        assert config.fraud_rate == 0.06

    def test_default_prone_member_rate(self):
        """Default fraud-prone member rate is 3%."""
        config = FraudConfig()
        assert config.fraud_prone_member_rate == 0.03

    def test_default_prone_provider_rate(self):
        """Default fraud-prone provider rate is 2%."""
        config = FraudConfig()
        assert config.fraud_prone_provider_rate == 0.02

    def test_default_claim_multiplier(self):
        """Default fraud-prone claim multiplier is 5x."""
        config = FraudConfig()
        assert config.fraud_prone_claim_multiplier == 5.0

    def test_all_fraud_types_enabled_by_default(self):
        """All fraud type sub-configs default to enabled."""
        config = FraudConfig()
        assert config.drg_upcoding.enabled is True
        assert config.extras_upcoding.enabled is True
        assert config.exact_duplicate.enabled is True
        assert config.near_duplicate.enabled is True
        assert config.unbundling.enabled is True
        assert config.phantom_billing.enabled is True
        assert config.provider_outlier.enabled is True
        assert config.temporal_anomaly.enabled is True
        assert config.geographic_anomaly.enabled is True

    def test_weights_sum_to_approximately_one(self):
        """Fraud type weights should sum to 1.0."""
        config = FraudConfig()
        total = (
            config.drg_upcoding.weight
            + config.extras_upcoding.weight
            + config.exact_duplicate.weight
            + config.near_duplicate.weight
            + config.unbundling.weight
            + config.phantom_billing.weight
            + config.provider_outlier.weight
            + config.temporal_anomaly.weight
            + config.geographic_anomaly.weight
        )
        assert abs(total - 1.0) < 0.01


# =============================================================================
# FraudGenerator Core Tests
# =============================================================================


class TestFraudGeneratorCore:
    """Test FraudGenerator core methods."""

    def test_should_apply_fraud_base_rate(self, fraud_gen, shared_state):
        """should_apply_fraud triggers at approximately the configured rate."""
        member_id = uuid4()
        n_trials = 10000
        n_fraud = sum(
            1 for _ in range(n_trials)
            if fraud_gen.should_apply_fraud(member_id, shared_state)
        )
        observed_rate = n_fraud / n_trials
        # 6% rate with tolerance
        assert 0.03 < observed_rate < 0.10

    def test_should_apply_fraud_prone_member_boost(self, fraud_gen, shared_state):
        """Fraud-prone members have 5x higher fraud rate."""
        member_id = uuid4()
        shared_state.fraud_prone_members[member_id] = True

        n_trials = 10000
        n_fraud = sum(
            1 for _ in range(n_trials)
            if fraud_gen.should_apply_fraud(member_id, shared_state)
        )
        observed_rate = n_fraud / n_trials
        # 6% * 5 = 30% rate with tolerance
        assert 0.20 < observed_rate < 0.40

    def test_select_fraud_type_hospital_allows_drg(self, fraud_gen):
        """Hospital claims can get DRG upcoding."""
        types_seen = set()
        for _ in range(500):
            ft = fraud_gen.select_fraud_type(ClaimType.HOSPITAL)
            types_seen.add(ft)
        assert FraudType.DRG_UPCODING in types_seen
        assert FraudType.EXTRAS_UPCODING not in types_seen

    def test_select_fraud_type_extras_allows_extras_upcoding(self, fraud_gen):
        """Extras claims can get extras upcoding."""
        types_seen = set()
        for _ in range(500):
            ft = fraud_gen.select_fraud_type(ClaimType.EXTRAS)
            types_seen.add(ft)
        assert FraudType.EXTRAS_UPCODING in types_seen
        assert FraudType.DRG_UPCODING not in types_seen

    def test_select_fraud_type_ambulance_universal_only(self, fraud_gen):
        """Ambulance claims only get universal fraud types."""
        hospital_only = {FraudType.DRG_UPCODING}
        extras_only = {FraudType.EXTRAS_UPCODING}
        unbundling = {FraudType.UNBUNDLING}
        restricted = hospital_only | extras_only | unbundling

        for _ in range(500):
            ft = fraud_gen.select_fraud_type(ClaimType.AMBULANCE)
            assert ft not in restricted

    def test_build_weights_with_disabled_type(self, fraud_rng, mock_reference, id_gen, mock_sim_env):
        """Disabled fraud types are excluded from selection."""
        config = FraudConfig(
            enabled=True,
            drg_upcoding=DRGUpcodingConfig(enabled=False),
        )
        gen = FraudGenerator(
            rng=fraud_rng,
            reference=mock_reference,
            id_generator=id_gen,
            sim_env=mock_sim_env,
            fraud_config=config,
        )
        assert FraudType.DRG_UPCODING not in gen.fraud_types


# =============================================================================
# DRG Upcoding Tests
# =============================================================================


class TestDRGUpcoding:
    """Test DRG upcoding fraud type."""

    def test_inflates_charge(self, fraud_gen, sample_hospital_claim):
        """DRG upcoding increases total_charge."""
        result = fraud_gen.apply_drg_upcoding(sample_hospital_claim)
        assert result["total_charge"] > sample_hospital_claim.total_charge

    def test_preserves_original_charge(self, fraud_gen, sample_hospital_claim):
        """fraud_original_charge stores the unmodified amount."""
        result = fraud_gen.apply_drg_upcoding(sample_hospital_claim)
        assert result["fraud_original_charge"] == sample_hospital_claim.total_charge

    def test_inflation_ratio_bounds(self, fraud_gen, sample_hospital_claim):
        """Multiplier is either CC (1.3) or MCC (1.7)."""
        result = fraud_gen.apply_drg_upcoding(sample_hospital_claim)
        ratio = float(result["fraud_inflation_ratio"])
        assert ratio in (1.3, 1.7)

    def test_sets_fraud_metadata(self, fraud_gen, sample_hospital_claim):
        """Result includes proper fraud metadata."""
        result = fraud_gen.apply_drg_upcoding(sample_hospital_claim)
        assert result["is_fraud"] is True
        assert result["fraud_type"] == FraudType.DRG_UPCODING
        assert result["fraud_inflation_amount"] > 0


# =============================================================================
# Extras Upcoding Tests
# =============================================================================


class TestExtrasUpcoding:
    """Test extras upcoding fraud type."""

    def test_inflates_charge(self, fraud_gen, sample_extras_claim):
        """Extras upcoding increases total_charge."""
        result = fraud_gen.apply_extras_upcoding(sample_extras_claim)
        assert result["total_charge"] > sample_extras_claim.total_charge

    def test_inflation_bounds(self, fraud_gen, sample_extras_claim):
        """Inflation ratio is clipped to [1.2, 2.5]."""
        for _ in range(100):
            result = fraud_gen.apply_extras_upcoding(sample_extras_claim)
            ratio = float(result["fraud_inflation_ratio"])
            assert 1.2 <= ratio <= 2.5

    def test_preserves_original(self, fraud_gen, sample_extras_claim):
        """Original charge is stored in fraud metadata."""
        result = fraud_gen.apply_extras_upcoding(sample_extras_claim)
        assert result["fraud_original_charge"] == sample_extras_claim.total_charge


# =============================================================================
# Exact Duplicate Tests
# =============================================================================


class TestExactDuplicate:
    """Test exact duplicate fraud type."""

    def test_matches_source_charge(self, fraud_gen):
        """Duplicate has same total_charge as source."""
        source = {
            "claim_id": uuid4(),
            "policy_id": uuid4(),
            "member_id": uuid4(),
            "coverage_id": uuid4(),
            "claim_type": "Extras",
            "service_date": date(2024, 6, 1),
            "total_charge": Decimal("300.00"),
            "provider_id": 42,
            "hospital_id": None,
            "claim_channel": "Online",
        }
        result = fraud_gen.generate_exact_duplicate(source, date(2024, 6, 15))
        assert result["total_charge"] == source["total_charge"]

    def test_references_source_claim(self, fraud_gen):
        """Duplicate stores source claim ID."""
        source_id = uuid4()
        source = {
            "claim_id": source_id,
            "policy_id": uuid4(),
            "member_id": uuid4(),
            "coverage_id": uuid4(),
            "claim_type": "Extras",
            "service_date": date(2024, 6, 1),
            "total_charge": Decimal("300.00"),
            "provider_id": 42,
            "hospital_id": None,
            "claim_channel": "Online",
        }
        result = fraud_gen.generate_exact_duplicate(source, date(2024, 6, 15))
        assert result["fraud_source_claim_id"] == source_id

    def test_lodgement_delay_bounds(self, fraud_gen):
        """Duplicate lodgement is 7-30 days after current date."""
        source = {
            "claim_id": uuid4(),
            "policy_id": uuid4(),
            "member_id": uuid4(),
            "coverage_id": uuid4(),
            "claim_type": "Extras",
            "service_date": date(2024, 6, 1),
            "total_charge": Decimal("300.00"),
            "provider_id": 42,
            "hospital_id": None,
            "claim_channel": "Online",
        }
        current = date(2024, 6, 15)
        for _ in range(50):
            result = fraud_gen.generate_exact_duplicate(source, current)
            delay = (result["lodgement_date"] - current).days
            assert 7 <= delay <= 30


# =============================================================================
# Near Duplicate Tests
# =============================================================================


class TestNearDuplicate:
    """Test near duplicate fraud type."""

    def test_amount_variation(self, fraud_gen):
        """Near duplicate charge varies within +/-5% of source."""
        source = {
            "claim_id": uuid4(),
            "policy_id": uuid4(),
            "member_id": uuid4(),
            "coverage_id": uuid4(),
            "claim_type": "Extras",
            "service_date": date(2024, 6, 1),
            "total_charge": Decimal("1000.00"),
            "provider_id": 42,
            "hospital_id": None,
            "claim_channel": "Online",
        }
        for _ in range(50):
            result = fraud_gen.generate_near_duplicate(source, date(2024, 7, 1))
            charge = float(result["total_charge"])
            assert 950.0 <= charge <= 1050.0

    def test_date_shift(self, fraud_gen):
        """Near duplicate service date shifts within +/-7 days."""
        source = {
            "claim_id": uuid4(),
            "policy_id": uuid4(),
            "member_id": uuid4(),
            "coverage_id": uuid4(),
            "claim_type": "Extras",
            "service_date": date(2024, 6, 15),
            "total_charge": Decimal("300.00"),
            "provider_id": 42,
            "hospital_id": None,
            "claim_channel": "Online",
        }
        for _ in range(50):
            result = fraud_gen.generate_near_duplicate(source, date(2024, 7, 1))
            shift = abs((result["service_date"] - source["service_date"]).days)
            assert shift <= 7


# =============================================================================
# Unbundling Tests
# =============================================================================


class TestUnbundling:
    """Test unbundling fraud type."""

    def test_fragment_count(self, fraud_gen):
        """Generates 2-3 fragments by default."""
        fragments = fraud_gen.generate_unbundled_claims(Decimal("1000.00"))
        assert 2 <= len(fragments) <= 3

    def test_fragments_sum_exceeds_original(self, fraud_gen):
        """Total of fragments exceeds original charge (inflation)."""
        original = Decimal("1000.00")
        fragments = fraud_gen.generate_unbundled_claims(original)
        total = sum(f["charge_amount"] for f in fragments)
        assert total > original

    def test_fragments_sum_consistently(self, fraud_gen):
        """Fragment charges sum is internally consistent (adjustment applied)."""
        original_charge = Decimal("500.00")
        fragments = fraud_gen.generate_unbundled_claims(original_charge)
        fragment_sum = sum(f["charge_amount"] for f in fragments)
        # All fragments share the same inflation metadata
        assert all(
            f["fraud_original_charge"] == original_charge for f in fragments
        )
        # Total is greater than original (inflated)
        assert fragment_sum > original_charge
        # All fragment charges are positive
        assert all(f["charge_amount"] > 0 for f in fragments)

    def test_all_fragments_marked_fraud(self, fraud_gen):
        """Every fragment is marked as UNBUNDLING fraud."""
        fragments = fraud_gen.generate_unbundled_claims(Decimal("800.00"))
        for f in fragments:
            assert f["is_fraud"] is True
            assert f["fraud_type"] == FraudType.UNBUNDLING


# =============================================================================
# Phantom Billing Tests
# =============================================================================


class TestPhantomBilling:
    """Test phantom billing fraud type."""

    def test_original_charge_zero(self, fraud_gen, sample_extras_claim, shared_state):
        """Phantom billing has original_charge=0 (no real service)."""
        result = fraud_gen.apply_phantom_billing(sample_extras_claim, shared_state)
        assert result["fraud_original_charge"] == Decimal("0")

    def test_inflation_is_full_charge(self, fraud_gen, sample_extras_claim, shared_state):
        """The entire charge is fraudulent inflation."""
        result = fraud_gen.apply_phantom_billing(sample_extras_claim, shared_state)
        assert result["fraud_inflation_amount"] == sample_extras_claim.total_charge

    def test_metadata(self, fraud_gen, sample_extras_claim, shared_state):
        """Correct fraud metadata."""
        result = fraud_gen.apply_phantom_billing(sample_extras_claim, shared_state)
        assert result["is_fraud"] is True
        assert result["fraud_type"] == FraudType.PHANTOM_BILLING


# =============================================================================
# Provider Outlier Tests
# =============================================================================


class TestProviderOutlier:
    """Test provider outlier fraud type."""

    def test_inflates_charge(self, fraud_gen, sample_extras_claim):
        """Provider outlier inflates total_charge."""
        result = fraud_gen.apply_provider_outlier(sample_extras_claim)
        assert result["total_charge"] > sample_extras_claim.total_charge

    def test_inflation_ratio_bounds(self, fraud_gen, sample_extras_claim):
        """Inflation ratio is between 1.3 and 1.7."""
        for _ in range(50):
            result = fraud_gen.apply_provider_outlier(sample_extras_claim)
            ratio = float(result["fraud_inflation_ratio"])
            assert 1.3 <= ratio <= 1.7


# =============================================================================
# Temporal Anomaly Tests
# =============================================================================


class TestTemporalAnomaly:
    """Test temporal anomaly fraud type."""

    def test_shifts_service_date(self, fraud_gen, sample_extras_claim):
        """Temporal anomaly changes the service_date."""
        result = fraud_gen.apply_temporal_anomaly(sample_extras_claim)
        assert result["service_date"] != sample_extras_claim.service_date

    def test_shifted_to_weekend_or_holiday(self, fraud_gen, sample_extras_claim):
        """Service date shifts to weekend or AU public holiday."""
        for _ in range(50):
            result = fraud_gen.apply_temporal_anomaly(sample_extras_claim)
            new_date = result["service_date"]
            # Weekend: Saturday=5, Sunday=6
            is_weekend = new_date.weekday() >= 5
            is_holiday = (new_date.month, new_date.day) in [
                (1, 1), (1, 26), (4, 25), (12, 25), (12, 26),
            ]
            assert is_weekend or is_holiday

    def test_no_charge_inflation(self, fraud_gen, sample_extras_claim):
        """Temporal anomaly doesn't change charges."""
        result = fraud_gen.apply_temporal_anomaly(sample_extras_claim)
        assert result["fraud_inflation_amount"] == Decimal("0")
        assert result["fraud_inflation_ratio"] == Decimal("1.000")


# =============================================================================
# Geographic Anomaly Tests
# =============================================================================


class TestGeographicAnomaly:
    """Test geographic anomaly fraud type."""

    def test_different_state_provider(self, fraud_gen, sample_extras_claim):
        """Provider is from a different state than the member."""
        result = fraud_gen.apply_geographic_anomaly(sample_extras_claim, "NSW")
        # The mock returns VIC providers, so provider_id should be set
        assert result.get("provider_id") is not None

    def test_no_charge_inflation(self, fraud_gen, sample_extras_claim):
        """Geographic anomaly doesn't change charges."""
        result = fraud_gen.apply_geographic_anomaly(sample_extras_claim, "NSW")
        assert result["fraud_inflation_amount"] == Decimal("0")


# =============================================================================
# SharedState Fraud Tracking Tests
# =============================================================================


class TestSharedStateFraud:
    """Test SharedState fraud tracking fields and methods."""

    def test_fraud_prone_member_tracking(self, shared_state):
        """Can flag and check fraud-prone members."""
        member_id = uuid4()
        assert shared_state.is_fraud_prone_member(member_id) is False

        shared_state.fraud_prone_members[member_id] = True
        assert shared_state.is_fraud_prone_member(member_id) is True

    def test_fraud_prone_provider_tracking(self, shared_state):
        """Can flag and check fraud-prone providers."""
        provider_id = 42
        assert shared_state.is_fraud_prone_provider(provider_id) is False

        shared_state.fraud_prone_providers[provider_id] = True
        assert shared_state.is_fraud_prone_provider(provider_id) is True

    def test_duplication_pool(self, shared_state):
        """Claims can be added to and retrieved from duplication pool."""
        member_id = uuid4()
        claim_snapshot = {
            "claim_id": uuid4(),
            "policy_id": uuid4(),
            "member_id": member_id,
            "claim_type": "Extras",
            "total_charge": Decimal("300.00"),
        }
        shared_state.add_claim_for_duplication(claim_snapshot)

        results = shared_state.get_duplicate_source_claims(member_id=member_id)
        assert len(results) == 1
        assert results[0]["member_id"] == member_id

    def test_duplication_pool_max_size(self, shared_state):
        """Duplication pool respects maxlen."""
        for i in range(600):
            shared_state.add_claim_for_duplication({"claim_id": uuid4(), "index": i})
        assert len(shared_state.recent_claims_for_duplication) == 500

    def test_fraud_rings_tracking(self, shared_state):
        """Fraud rings can be created and tracked."""
        ring_id = uuid4()
        member_ids = [uuid4(), uuid4(), uuid4()]
        shared_state.fraud_rings[ring_id] = member_ids
        assert len(shared_state.fraud_rings[ring_id]) == 3

    def test_get_stats_includes_fraud(self, shared_state):
        """get_stats includes fraud tracking counts."""
        member_id = uuid4()
        shared_state.fraud_prone_members[member_id] = True
        shared_state.fraud_prone_providers[42] = True
        shared_state.fraud_rings[uuid4()] = [member_id]

        stats = shared_state.get_stats()
        assert stats["fraud_prone_members"] == 1
        assert stats["fraud_prone_providers"] == 1
        assert stats["active_fraud_rings"] == 1
