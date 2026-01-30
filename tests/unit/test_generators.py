"""
Unit tests for data generators.
"""

from datetime import date

import numpy as np
import pytest

from brickwell_health.core.environment import SimulationEnvironment
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.generators.member_generator import MemberGenerator
from brickwell_health.domain.enums import Gender


class TestIDGenerator:
    """Tests for IDGenerator."""

    def test_generate_uuid_is_deterministic(self, test_rng: np.random.Generator):
        """Same seed should produce same UUID sequence."""
        gen1 = IDGenerator(np.random.default_rng(42), 2024)
        gen2 = IDGenerator(np.random.default_rng(42), 2024)

        uuid1 = gen1.generate_uuid()
        uuid2 = gen2.generate_uuid()

        assert uuid1 == uuid2

    def test_generate_member_number_format(self, id_generator: IDGenerator):
        """Member number should have correct format."""
        member_number = id_generator.generate_member_number()

        # Format: MEM-WN-YYYY-NNNNNN (e.g., MEM-W0-2024-000001)
        assert member_number.startswith("MEM-W")
        assert "-2024-" in member_number
        parts = member_number.split("-")
        assert len(parts) == 4  # MEM, WN, YYYY, NNNNNN

    def test_generate_policy_number_sequential(self, id_generator: IDGenerator):
        """Policy numbers should be sequential."""
        num1 = id_generator.generate_policy_number()
        num2 = id_generator.generate_policy_number()

        # Extract sequence numbers
        seq1 = int(num1.split("-")[-1])
        seq2 = int(num2.split("-")[-1])

        assert seq2 == seq1 + 1

    def test_generate_medicare_number_format(self, id_generator: IDGenerator):
        """Medicare number should have 11 digits."""
        medicare = id_generator.generate_medicare_number()

        assert len(medicare) == 11
        assert medicare.isdigit()

    def test_generate_bsb_format(self, id_generator: IDGenerator):
        """BSB should have format NNN-NNN."""
        bsb = id_generator.generate_bsb()

        assert len(bsb) == 7
        assert bsb[3] == "-"
        assert bsb[:3].isdigit()
        assert bsb[4:].isdigit()

    def test_set_counters_restores_state(self, id_generator: IDGenerator):
        """Setting counters should restore generator state."""
        # Generate some numbers
        id_generator.generate_member_number()
        id_generator.generate_member_number()
        id_generator.generate_policy_number()

        # Get current counters
        counters = id_generator.get_counters()
        assert counters["member"] == 2
        assert counters["policy"] == 1

        # Reset counters
        id_generator.set_counters(member=100, policy=200)

        # Next numbers should reflect new counters
        next_member = id_generator.generate_member_number()
        assert "000101" in next_member


class TestMemberGenerator:
    """Tests for MemberGenerator."""

    def test_generate_member_has_required_fields(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Generated member should have all required fields."""
        gen = MemberGenerator(test_rng, test_reference, id_generator, sim_env=sim_env)
        member = gen.generate(as_of_date=date(2024, 1, 1))

        assert member.member_id is not None
        assert member.member_number is not None
        assert member.first_name is not None
        assert member.last_name is not None
        assert member.date_of_birth is not None
        assert member.gender in [Gender.MALE, Gender.FEMALE]
        assert member.state in {"NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"}

    def test_generate_member_with_specific_age(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Member should be generated with specified age."""
        gen = MemberGenerator(test_rng, test_reference, id_generator, sim_env=sim_env)
        member = gen.generate(age=35, as_of_date=date(2024, 1, 1))

        # Calculate age
        age = 2024 - member.date_of_birth.year
        # Allow for birthday variation
        assert 34 <= age <= 36

    def test_generate_family_single_has_one_member(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Single policy type should generate one member."""
        gen = MemberGenerator(test_rng, test_reference, id_generator, sim_env=sim_env)
        members = gen.generate_family("Single", as_of_date=date(2024, 1, 1))

        assert len(members) == 1

    def test_generate_family_couple_has_two_members(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Couple policy type should generate two members."""
        gen = MemberGenerator(test_rng, test_reference, id_generator, sim_env=sim_env)
        members = gen.generate_family("Couple", as_of_date=date(2024, 1, 1))

        assert len(members) == 2

    def test_generate_family_has_at_least_primary_and_child(
        self,
        test_rng: np.random.Generator,
        test_reference,
        id_generator: IDGenerator,
        sim_env: SimulationEnvironment,
    ):
        """Family policy should have primary, partner, and at least one child."""
        gen = MemberGenerator(test_rng, test_reference, id_generator, sim_env=sim_env)
        members = gen.generate_family("Family", as_of_date=date(2024, 1, 1))

        # Family should have at least 3 members (primary, partner, child)
        assert len(members) >= 3
