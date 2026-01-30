"""
Coverage generator for Brickwell Health Simulator.

Generates coverage records for policies.
"""

from datetime import date
from decimal import Decimal
from typing import Any, TYPE_CHECKING
from uuid import UUID

from brickwell_health.domain.coverage import CoverageCreate
from brickwell_health.domain.enums import CoverageType, CoverageTier
from brickwell_health.domain.policy import PolicyCreate
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.generators.id_generator import IDGenerator

if TYPE_CHECKING:
    from brickwell_health.core.environment import SimulationEnvironment


class CoverageGenerator(BaseGenerator[CoverageCreate]):
    """
    Generates coverage records for policies.

    Creates Hospital, Extras, and Ambulance coverages based on product.
    """

    def __init__(
        self,
        rng,
        reference,
        id_generator: IDGenerator,
        sim_env: "SimulationEnvironment",
    ):
        """
        Initialize the coverage generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
            sim_env: Simulation environment for time access
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator

    def generate(
        self,
        policy: PolicyCreate,
        coverage_type: CoverageType,
        tier: CoverageTier | None = None,
        coverage_id: UUID | None = None,
        **kwargs: Any,
    ) -> CoverageCreate:
        """
        Generate a single coverage record.

        Args:
            policy: Policy to add coverage to
            coverage_type: Hospital/Extras/Ambulance
            tier: Optional tier for hospital coverage
            coverage_id: Optional pre-generated UUID

        Returns:
            CoverageCreate instance
        """
        if coverage_id is None:
            coverage_id = self.id_generator.generate_uuid()

        # Determine excess (hospital only)
        excess = None
        if coverage_type == CoverageType.HOSPITAL:
            excess = policy.excess_amount

        return CoverageCreate(
            coverage_id=coverage_id,
            policy_id=policy.policy_id,
            coverage_type=coverage_type,
            product_id=policy.product_id,
            effective_date=policy.effective_date,
            end_date=None,
            status="Active",
            tier=tier,
            excess_amount=excess,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

    def generate_coverages_for_policy(
        self,
        policy: PolicyCreate,
    ) -> list[CoverageCreate]:
        """
        Generate all coverages for a policy based on product type.

        Args:
            policy: Policy to create coverages for

        Returns:
            List of CoverageCreate instances
        """
        coverages = []

        # Get product details
        product = self.reference.get_product_by_id(policy.product_id)

        if product is None:
            # Default to combined hospital + extras
            product = {
                "is_hospital": True,
                "is_extras": True,
                "is_ambulance": False,
                "product_tier_id": 2,  # Silver default
            }

        # Determine tier
        tier_map = {1: CoverageTier.GOLD, 2: CoverageTier.SILVER, 3: CoverageTier.BRONZE, 4: CoverageTier.BASIC}
        tier_id = product.get("product_tier_id", 2)
        tier = tier_map.get(tier_id, CoverageTier.SILVER)

        # Generate hospital coverage if applicable
        if product.get("is_hospital"):
            hospital_coverage = self.generate(
                policy=policy,
                coverage_type=CoverageType.HOSPITAL,
                tier=tier,
            )
            coverages.append(hospital_coverage)

        # Generate extras coverage if applicable
        if product.get("is_extras"):
            extras_coverage = self.generate(
                policy=policy,
                coverage_type=CoverageType.EXTRAS,
                tier=None,
            )
            coverages.append(extras_coverage)

        # Generate ambulance coverage if applicable
        if product.get("is_ambulance"):
            ambulance_coverage = self.generate(
                policy=policy,
                coverage_type=CoverageType.AMBULANCE,
                tier=None,
            )
            coverages.append(ambulance_coverage)

        return coverages

    def end_coverage(
        self,
        coverage: CoverageCreate,
        end_date: date,
    ) -> CoverageCreate:
        """
        End a coverage.

        Args:
            coverage: Coverage to end
            end_date: End date

        Returns:
            Updated CoverageCreate
        """
        coverage.end_date = end_date
        coverage.status = "Ended"
        return coverage

    def upgrade_coverage(
        self,
        coverage: CoverageCreate,
        new_tier: CoverageTier,
        effective_date: date,
    ) -> CoverageCreate:
        """
        Create a new coverage record for an upgrade.

        Args:
            coverage: Current coverage
            new_tier: New tier
            effective_date: Upgrade effective date

        Returns:
            New CoverageCreate for upgraded tier
        """
        return CoverageCreate(
            coverage_id=self.id_generator.generate_uuid(),
            policy_id=coverage.policy_id,
            coverage_type=coverage.coverage_type,
            product_id=coverage.product_id,  # Would normally change
            effective_date=effective_date,
            end_date=None,
            status="Active",
            tier=new_tier,
            excess_amount=coverage.excess_amount,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )
