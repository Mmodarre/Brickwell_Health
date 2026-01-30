"""
Member generator for Brickwell Health Simulator.

Generates realistic Australian member data using Faker and ABS demographics.
"""

from datetime import date
from typing import Any, TYPE_CHECKING
from uuid import UUID

from faker import Faker

from brickwell_health.domain.enums import Gender, MaritalStatus
from brickwell_health.domain.member import MemberCreate
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.generators.id_generator import IDGenerator
from brickwell_health.statistics.abs_demographics import ABSDemographics

if TYPE_CHECKING:
    from brickwell_health.core.environment import SimulationEnvironment


class MemberGenerator(BaseGenerator[MemberCreate]):
    """
    Generates realistic member data.

    Uses:
    - Faker for names, addresses
    - ABS Census 2021 demographics for distributions
    - Reference data for valid states
    """

    def __init__(
        self,
        rng,
        reference,
        id_generator: IDGenerator,
        sim_env: "SimulationEnvironment",
    ):
        """
        Initialize the member generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator for member numbers
            sim_env: Simulation environment for time access
        """
        super().__init__(rng, reference, sim_env)
        self.id_generator = id_generator
        self.faker = Faker("en_AU")
        self.faker.seed_instance(int(rng.integers(0, 2**31)))
        self.demographics = ABSDemographics(rng)

    def generate(
        self,
        member_id: UUID | None = None,
        state: str | None = None,
        gender: str | None = None,
        age: int | None = None,
        date_of_birth: date | None = None,
        as_of_date: date | None = None,
        **kwargs: Any,
    ) -> MemberCreate:
        """
        Generate a member.

        Args:
            member_id: Optional pre-generated UUID
            state: Optional state code
            gender: Optional gender
            age: Optional age (alternative to date_of_birth)
            date_of_birth: Optional date of birth
            as_of_date: Reference date for age calculation

        Returns:
            MemberCreate instance
        """
        if member_id is None:
            member_id = self.id_generator.generate_uuid()

        if state is None:
            state = self.demographics.sample_state()

        if gender is None:
            gender = self.demographics.sample_gender()

        # Determine date of birth
        if date_of_birth is None:
            if age is None:
                age = self.demographics.sample_age(role="Primary")
            if as_of_date is None:
                as_of_date = self.get_current_date()
            # Calculate approximate DOB
            birth_year = as_of_date.year - age
            date_of_birth = date(
                birth_year,
                self.uniform_int(1, 13),
                self.uniform_int(1, 29),
            )

        # Generate name based on gender
        if gender == "Male":
            first_name = self.faker.first_name_male()
        else:
            first_name = self.faker.first_name_female()

        last_name = self.faker.last_name()
        title = self.demographics.sample_title(gender, age or 35)

        # Generate address
        postcode = self.demographics.sample_postcode(state)
        address = self._generate_address(state, postcode)

        # Generate Medicare number
        medicare_number = self.id_generator.generate_medicare_number()
        medicare_irn = str(self.uniform_int(1, 10))

        # Calculate LHC applicability
        calculated_age = self._calculate_age(date_of_birth, as_of_date or self.get_current_date())
        lhc_applicable = calculated_age >= 31

        # Sample marital status
        marital_status = self._sample_marital_status()

        return MemberCreate(
            member_id=member_id,
            member_number=self.id_generator.generate_member_number(),
            title=title,
            first_name=first_name,
            middle_name=self.faker.first_name() if self.bernoulli(0.3) else None,
            last_name=last_name,
            preferred_name=None,
            date_of_birth=date_of_birth,
            gender=Gender(gender),
            medicare_number=medicare_number,
            medicare_irn=medicare_irn,
            medicare_expiry_date=date(
                (as_of_date or self.get_current_date()).year + self.uniform_int(1, 5),
                self.uniform_int(1, 13),
                1,
            ),
            address_line_1=address["line_1"],
            address_line_2=address.get("line_2"),
            suburb=address["suburb"],
            state=state,
            postcode=postcode,
            country="AUS",
            email=self._generate_email(first_name, last_name),
            mobile_phone=self._generate_phone(),
            home_phone=self._generate_phone() if self.bernoulli(0.3) else None,
            australian_resident=True,
            tax_file_number_provided=self.bernoulli(0.7),
            lhc_applicable=lhc_applicable,
            marital_status=marital_status,
            created_at=self.get_current_datetime(),
            created_by="SIMULATION",
        )

    def generate_partner(
        self,
        primary: MemberCreate,
        as_of_date: date | None = None,
    ) -> MemberCreate:
        """
        Generate a partner member correlated to primary.

        Args:
            primary: Primary member
            as_of_date: Reference date

        Returns:
            Partner MemberCreate
        """
        primary_age = self._calculate_age(primary.date_of_birth, as_of_date or self.get_current_date())
        primary_gender = primary.gender.value

        partner_age = self.demographics.sample_partner_age(primary_age, primary_gender)

        # Partner has opposite gender typically
        if self.bernoulli(0.97):  # 3% same-sex couples
            partner_gender = "Female" if primary_gender == "Male" else "Male"
        else:
            partner_gender = primary_gender

        return self.generate(
            state=primary.state,
            gender=partner_gender,
            age=partner_age,
            as_of_date=as_of_date,
        )

    def generate_dependent(
        self,
        primary: MemberCreate,
        age: int | None = None,
        as_of_date: date | None = None,
    ) -> MemberCreate:
        """
        Generate a dependent member (child).

        Args:
            primary: Primary member
            age: Optional specific age
            as_of_date: Reference date

        Returns:
            Dependent MemberCreate
        """
        if age is None:
            age = self.demographics.sample_age(role="Dependent")

        # Children inherit last name typically
        return self.generate(
            state=primary.state,
            age=age,
            as_of_date=as_of_date,
        )

    def generate_family(
        self,
        policy_type: str,
        state: str | None = None,
        as_of_date: date | None = None,
    ) -> list[MemberCreate]:
        """
        Generate a family of members for a policy.

        Args:
            policy_type: Single/Couple/Family/SingleParent
            state: Optional state code
            as_of_date: Reference date

        Returns:
            List of MemberCreate instances (primary first)
        """
        members = []

        # Generate primary member
        primary = self.generate(state=state, as_of_date=as_of_date)
        members.append(primary)

        if policy_type == "Single":
            return members

        # Add partner for Couple and Family
        if policy_type in ["Couple", "Family"]:
            partner = self.generate_partner(primary, as_of_date)
            members.append(partner)

        # Add children for Family and SingleParent
        if policy_type in ["Family", "SingleParent"]:
            num_children = self.demographics.sample_num_children(policy_type)
            primary_age = self._calculate_age(primary.date_of_birth, as_of_date or self.get_current_date())
            child_ages = self.demographics.sample_child_ages(num_children, primary_age)

            for age in child_ages:
                dependent = self.generate_dependent(primary, age, as_of_date)
                members.append(dependent)

        return members

    def _generate_address(self, state: str, postcode: str) -> dict[str, str]:
        """Generate address components."""
        street_number = self.uniform_int(1, 999)
        street_name = self.faker.street_name()
        suburb = self.faker.city()

        address = {
            "line_1": f"{street_number} {street_name}",
            "suburb": suburb,
        }

        # Add unit number sometimes
        if self.bernoulli(0.2):
            unit = self.uniform_int(1, 50)
            address["line_2"] = address["line_1"]
            address["line_1"] = f"Unit {unit}"

        return address

    def _generate_email(self, first_name: str, last_name: str) -> str:
        """Generate email address."""
        domains = ["gmail.com", "outlook.com", "hotmail.com", "icloud.com", "yahoo.com.au"]
        domain = self.choice(domains)

        # Various email patterns
        patterns = [
            f"{first_name.lower()}.{last_name.lower()}",
            f"{first_name.lower()}{self.uniform_int(1, 99)}",
            f"{first_name[0].lower()}{last_name.lower()}",
            f"{first_name.lower()}_{last_name.lower()}",
        ]
        local = self.choice(patterns)

        return f"{local}@{domain}"

    def _generate_phone(self) -> str:
        """Generate Australian mobile number."""
        # Australian mobile numbers: 04XX XXX XXX
        prefix = self.choice(["04", "04", "04", "04"])
        rest = f"{self.uniform_int(10000000, 99999999)}"
        return f"{prefix}{rest[:2]} {rest[2:5]} {rest[5:]}"

    def _calculate_age(self, dob: date, as_of: date) -> int:
        """Calculate age in years."""
        age = as_of.year - dob.year
        if (as_of.month, as_of.day) < (dob.month, dob.day):
            age -= 1
        return max(0, age)

    def _sample_marital_status(self) -> MaritalStatus:
        """Sample marital status based on distribution."""
        # Default distribution (can be overridden via config)
        statuses = [
            MaritalStatus.SINGLE,
            MaritalStatus.MARRIED,
            MaritalStatus.DE_FACTO,
            MaritalStatus.DIVORCED,
            MaritalStatus.SEPARATED,
            MaritalStatus.WIDOWED,
        ]
        weights = [0.35, 0.40, 0.15, 0.07, 0.02, 0.01]
        idx = self.rng.choice(len(statuses), p=weights)
        return statuses[idx]

    # =========================================================================
    # Member Change Generation Methods
    # =========================================================================

    def generate_new_address(
        self,
        current_state: str,
        interstate_move_rate: float = 0.15,
    ) -> dict[str, str]:
        """
        Generate a new address for a member move.

        Args:
            current_state: Member's current state
            interstate_move_rate: Probability of moving to a different state

        Returns:
            Dictionary with address fields: line_1, line_2, suburb, state, postcode
        """
        # Determine if interstate move
        if self.bernoulli(interstate_move_rate):
            new_state = self.demographics.sample_state()
            # Ensure it's actually a different state
            while new_state == current_state:
                new_state = self.demographics.sample_state()
        else:
            new_state = current_state

        postcode = self.demographics.sample_postcode(new_state)
        address = self._generate_address(new_state, postcode)

        return {
            "address_line_1": address["line_1"],
            "address_line_2": address.get("line_2"),
            "suburb": address["suburb"],
            "state": new_state,
            "postcode": postcode,
        }

    def generate_new_phone(self) -> str:
        """
        Generate a new phone number.

        Returns:
            New Australian mobile number
        """
        return self._generate_phone()

    def generate_new_email(self, first_name: str, last_name: str) -> str:
        """
        Generate a new email address.

        Args:
            first_name: Member's first name
            last_name: Member's current last name

        Returns:
            New email address
        """
        return self._generate_email(first_name, last_name)

    def generate_married_name(
        self,
        current_last_name: str,
        partner_last_name: str | None = None,
    ) -> str:
        """
        Generate a new last name for marriage.

        Simulates the common patterns:
        - 60% take partner's name
        - 20% hyphenate
        - 20% keep original

        Args:
            current_last_name: Member's current last name
            partner_last_name: Partner's last name (if available)

        Returns:
            New last name
        """
        if partner_last_name is None:
            # Generate a random name if partner name not provided
            partner_last_name = self.faker.last_name()

        roll = self.rng.random()
        if roll < 0.6:
            # Take partner's name
            return partner_last_name
        elif roll < 0.8:
            # Hyphenate
            return f"{current_last_name}-{partner_last_name}"
        else:
            # Keep original
            return current_last_name

    def generate_divorce_name(
        self,
        current_last_name: str,
        maiden_name: str | None = None,
    ) -> str:
        """
        Generate a name change for divorce.

        Args:
            current_last_name: Member's current (married) last name
            maiden_name: Original name before marriage (if known)

        Returns:
            New last name (may revert to maiden name or keep current)
        """
        if maiden_name and self.bernoulli(0.6):
            # 60% revert to maiden name
            return maiden_name
        # 40% keep married name
        return current_last_name

    def generate_medicare_renewal(self, current_expiry: date) -> date:
        """
        Generate a renewed Medicare expiry date.

        Medicare cards are renewed for 5 years.

        Args:
            current_expiry: Current Medicare card expiry date

        Returns:
            New expiry date (5 years from current)
        """
        return date(current_expiry.year + 5, current_expiry.month, 1)

    def generate_new_marital_status(
        self,
        current_status: MaritalStatus,
    ) -> MaritalStatus:
        """
        Generate a new marital status based on valid transitions.

        Args:
            current_status: Member's current marital status

        Returns:
            New marital status
        """
        # Define valid transitions
        transitions: dict[MaritalStatus, list[tuple[MaritalStatus, float]]] = {
            MaritalStatus.SINGLE: [
                (MaritalStatus.MARRIED, 0.6),
                (MaritalStatus.DE_FACTO, 0.4),
            ],
            MaritalStatus.MARRIED: [
                (MaritalStatus.DIVORCED, 0.5),
                (MaritalStatus.SEPARATED, 0.3),
                (MaritalStatus.WIDOWED, 0.2),
            ],
            MaritalStatus.DE_FACTO: [
                (MaritalStatus.MARRIED, 0.5),
                (MaritalStatus.SINGLE, 0.3),
                (MaritalStatus.SEPARATED, 0.2),
            ],
            MaritalStatus.DIVORCED: [
                (MaritalStatus.MARRIED, 0.5),
                (MaritalStatus.DE_FACTO, 0.3),
                (MaritalStatus.SINGLE, 0.2),
            ],
            MaritalStatus.SEPARATED: [
                (MaritalStatus.DIVORCED, 0.6),
                (MaritalStatus.MARRIED, 0.2),  # Reconciliation
                (MaritalStatus.SINGLE, 0.2),
            ],
            MaritalStatus.WIDOWED: [
                (MaritalStatus.MARRIED, 0.4),
                (MaritalStatus.DE_FACTO, 0.3),
                (MaritalStatus.SINGLE, 0.3),
            ],
        }

        options = transitions.get(current_status, [(MaritalStatus.SINGLE, 1.0)])
        statuses = [opt[0] for opt in options]
        weights = [opt[1] for opt in options]

        idx = self.rng.choice(len(statuses), p=weights)
        return statuses[idx]

    def generate_preferred_name(self, first_name: str) -> str:
        """
        Generate a preferred name / nickname.

        Args:
            first_name: Member's legal first name

        Returns:
            Preferred name (nickname or shortened version)
        """
        # Common nickname patterns
        if len(first_name) > 4:
            # Shorten long names
            shortened = first_name[:3] + ("y" if self.bernoulli(0.5) else "ie")
            if self.bernoulli(0.7):
                return shortened.capitalize()

        # Or use a completely different nickname
        nicknames = ["Alex", "Sam", "Chris", "Pat", "Jo", "Max", "Charlie", "Jamie"]
        return self.choice(nicknames)
