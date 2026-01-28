"""
Application generator for Brickwell Health Simulator.

Generates new policy applications with applicant data.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from brickwell_health.domain.application import (
    ApplicationCreate,
    ApplicationMemberCreate,
    HealthDeclarationCreate,
)
from brickwell_health.domain.enums import (
    ApplicationStatus,
    ApplicationType,
    PolicyType,
    DistributionChannel,
    MemberRole,
    RelationshipType,
    Gender,
)
from brickwell_health.domain.member import MemberCreate
from brickwell_health.generators.base import BaseGenerator
from brickwell_health.generators.id_generator import IDGenerator


class ApplicationGenerator(BaseGenerator[ApplicationCreate]):
    """
    Generates new policy applications.

    Creates application records with associated application members.
    """

    # Standard health declaration questions for PHI applications
    HEALTH_QUESTIONS = [
        {
            "code": "PRE_EXIST",
            "text": "Do you have any pre-existing medical conditions?",
            "yes_rate": 0.25,  # 25% have pre-existing conditions
        },
        {
            "code": "TREATMENT",
            "text": "Are you currently receiving any medical treatment?",
            "yes_rate": 0.15,
        },
        {
            "code": "HOSPITAL_12M",
            "text": "Have you been admitted to hospital in the past 12 months?",
            "yes_rate": 0.10,
        },
        {
            "code": "SURGERY_PLAN",
            "text": "Do you have any planned surgeries or medical procedures?",
            "yes_rate": 0.05,
        },
    ]

    # Additional question for females of childbearing age (15-49)
    PREGNANCY_QUESTION = {
        "code": "PREGNANCY",
        "text": "Are you currently pregnant or planning to become pregnant in the next 12 months?",
        "yes_rate": 0.08,  # 8% of females in childbearing age
    }

    def __init__(self, rng, reference, id_generator: IDGenerator):
        """
        Initialize the application generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
            id_generator: ID generator
        """
        super().__init__(rng, reference)
        self.id_generator = id_generator

    def generate(
        self,
        members: list[MemberCreate],
        policy_type: PolicyType,
        product_id: int,
        channel: DistributionChannel,
        requested_start_date: date,
        submission_date: datetime | None = None,
        application_id: UUID | None = None,
        **kwargs: Any,
    ) -> tuple[ApplicationCreate, list[ApplicationMemberCreate]]:
        """
        Generate an application with applicant members.

        Args:
            members: List of members applying (primary first)
            policy_type: Type of policy being applied for
            product_id: Product being applied for
            channel: Distribution channel
            requested_start_date: Desired policy start date
            submission_date: Application submission timestamp
            application_id: Optional pre-generated UUID

        Returns:
            Tuple of (ApplicationCreate, list[ApplicationMemberCreate])
        """
        if application_id is None:
            application_id = self.id_generator.generate_uuid()

        if submission_date is None:
            submission_date = datetime.now()

        # Get primary member's state
        primary = members[0]
        state = primary.state

        # Determine excess
        excess_options = self.reference.get_excess_options()
        if excess_options:
            excess_amounts = [e.get("excess_amount", 500) for e in excess_options]
            excess = Decimal(str(self.choice(excess_amounts)))
        else:
            excess = Decimal("500")

        application = ApplicationCreate(
            application_id=application_id,
            application_number=self.id_generator.generate_application_number(),
            application_type=ApplicationType.NEW,
            application_status=ApplicationStatus.PENDING,
            product_id=product_id,
            requested_policy_type=policy_type,
            requested_excess=excess,
            requested_start_date=requested_start_date,
            channel=channel,
            previous_fund_code=None,
            transfer_certificate_received=False,
            submission_date=submission_date,
            decision_date=None,
            decision_by=None,
            decline_reason=None,
            state=state,
            created_at=datetime.now(),
            created_by="SIMULATION",
        )

        # Generate application members
        app_members = []
        for i, member in enumerate(members):
            if i == 0:
                role = MemberRole.PRIMARY
                relationship = RelationshipType.SELF
            elif i == 1 and policy_type in [PolicyType.COUPLE, PolicyType.FAMILY]:
                role = MemberRole.PARTNER
                relationship = RelationshipType.SPOUSE
            else:
                role = MemberRole.DEPENDENT
                relationship = RelationshipType.CHILD

            app_member = ApplicationMemberCreate(
                application_member_id=self.id_generator.generate_uuid(),
                application_id=application_id,
                member_role=role,
                title=member.title,
                first_name=member.first_name,
                middle_name=member.middle_name,
                last_name=member.last_name,
                date_of_birth=member.date_of_birth,
                gender=member.gender,
                relationship_to_primary=relationship,
                medicare_number=member.medicare_number,
                medicare_irn=member.medicare_irn,
                email=member.email,
                mobile_phone=member.mobile_phone,
                existing_member_id=None,
                created_at=datetime.now(),
                created_by="SIMULATION",
            )
            app_members.append(app_member)

        return application, app_members

    def generate_transfer_application(
        self,
        members: list[MemberCreate],
        policy_type: PolicyType,
        product_id: int,
        channel: DistributionChannel,
        requested_start_date: date,
        previous_fund_code: str,
        **kwargs: Any,
    ) -> tuple[ApplicationCreate, list[ApplicationMemberCreate]]:
        """
        Generate a transfer application (from another fund).

        Args:
            members: Members transferring
            policy_type: Policy type
            product_id: Product
            channel: Channel
            requested_start_date: Start date
            previous_fund_code: Fund code of previous insurer

        Returns:
            Tuple of (ApplicationCreate, list[ApplicationMemberCreate])
        """
        application, app_members = self.generate(
            members=members,
            policy_type=policy_type,
            product_id=product_id,
            channel=channel,
            requested_start_date=requested_start_date,
            **kwargs,
        )

        # Update for transfer
        application.application_type = ApplicationType.TRANSFER
        application.previous_fund_code = previous_fund_code
        application.transfer_certificate_received = self.bernoulli(0.9)

        return application, app_members

    def approve_application(
        self,
        application: ApplicationCreate,
        decision_date: datetime,
    ) -> ApplicationCreate:
        """
        Mark an application as approved.

        Args:
            application: Application to approve
            decision_date: Decision timestamp

        Returns:
            Updated ApplicationCreate
        """
        application.application_status = ApplicationStatus.APPROVED
        application.decision_date = decision_date
        application.decision_by = "SYSTEM"
        return application

    def decline_application(
        self,
        application: ApplicationCreate,
        decision_date: datetime,
        reason: str,
    ) -> ApplicationCreate:
        """
        Mark an application as declined.

        Args:
            application: Application to decline
            decision_date: Decision timestamp
            reason: Decline reason

        Returns:
            Updated ApplicationCreate
        """
        application.application_status = ApplicationStatus.DECLINED
        application.decision_date = decision_date
        application.decision_by = "SYSTEM"
        application.decline_reason = reason
        return application

    def generate_health_declarations(
        self,
        application_id: UUID,
        app_members: list[ApplicationMemberCreate],
        declaration_date: datetime,
    ) -> list[HealthDeclarationCreate]:
        """
        Generate health declarations for all application members.

        Args:
            application_id: Application ID
            app_members: List of application members
            declaration_date: Timestamp for declarations

        Returns:
            List of HealthDeclarationCreate records
        """
        declarations = []

        for app_member in app_members:
            member_declarations = self._generate_member_health_declarations(
                application_id=application_id,
                app_member=app_member,
                declaration_date=declaration_date,
            )
            declarations.extend(member_declarations)

        return declarations

    def _generate_member_health_declarations(
        self,
        application_id: UUID,
        app_member: ApplicationMemberCreate,
        declaration_date: datetime,
    ) -> list[HealthDeclarationCreate]:
        """
        Generate health declarations for a single application member.

        Args:
            application_id: Application ID
            app_member: Application member
            declaration_date: Timestamp for declarations

        Returns:
            List of HealthDeclarationCreate records for this member
        """
        declarations = []

        # Generate standard health questions
        for question in self.HEALTH_QUESTIONS:
            is_yes = self.bernoulli(question["yes_rate"])
            response = "Yes" if is_yes else "No"

            # Generate response details if answered "Yes"
            response_details = None
            if is_yes:
                response_details = self._generate_health_response_details(question["code"])

            declaration = HealthDeclarationCreate(
                health_declaration_id=self.id_generator.generate_uuid(),
                application_member_id=app_member.application_member_id,
                application_id=application_id,
                question_code=question["code"],
                question_text=question["text"],
                response=response,
                response_details=response_details,
                declaration_date=declaration_date,
                declaration_acknowledged=True,
                created_at=datetime.now(),
                created_by="SIMULATION",
            )
            declarations.append(declaration)

        # Add pregnancy question for females of childbearing age (15-49)
        if app_member.gender == Gender.FEMALE:
            age = self._calculate_age(app_member.date_of_birth, declaration_date.date())
            if 15 <= age <= 49:
                is_yes = self.bernoulli(self.PREGNANCY_QUESTION["yes_rate"])
                response = "Yes" if is_yes else "No"
                response_details = None
                if is_yes:
                    response_details = self._generate_pregnancy_details()

                declaration = HealthDeclarationCreate(
                    health_declaration_id=self.id_generator.generate_uuid(),
                    application_member_id=app_member.application_member_id,
                    application_id=application_id,
                    question_code=self.PREGNANCY_QUESTION["code"],
                    question_text=self.PREGNANCY_QUESTION["text"],
                    response=response,
                    response_details=response_details,
                    declaration_date=declaration_date,
                    declaration_acknowledged=True,
                    created_at=datetime.now(),
                    created_by="SIMULATION",
                )
                declarations.append(declaration)

        return declarations

    def _generate_health_response_details(self, question_code: str) -> str:
        """Generate sample response details based on question type."""
        details_map = {
            "PRE_EXIST": [
                "Managed type 2 diabetes",
                "Asthma, well controlled",
                "High blood pressure, on medication",
                "Arthritis, occasional treatment",
                "Heart condition, under specialist care",
                "Mental health condition, ongoing treatment",
            ],
            "TREATMENT": [
                "Regular GP visits for monitoring",
                "Physiotherapy for back pain",
                "Medication for chronic condition",
                "Psychiatric care",
                "Specialist follow-up appointments",
            ],
            "HOSPITAL_12M": [
                "Day surgery procedure",
                "Emergency room visit",
                "Planned surgical procedure",
                "Observation stay",
                "Childbirth",
            ],
            "SURGERY_PLAN": [
                "Knee replacement scheduled",
                "Cataract surgery planned",
                "Dental surgery required",
                "Orthopaedic procedure needed",
                "Exploratory procedure planned",
            ],
        }

        options = details_map.get(question_code, ["Details provided"])
        return str(self.choice(options))

    def _generate_pregnancy_details(self) -> str:
        """Generate pregnancy-related response details."""
        options = [
            "Currently pregnant, due in 6 months",
            "Planning pregnancy within 12 months",
            "Currently pregnant, first trimester",
            "Currently pregnant, second trimester",
            "Currently pregnant, third trimester",
        ]
        return str(self.choice(options))

    def _calculate_age(self, date_of_birth: date, as_of_date: date) -> int:
        """Calculate age at a given date."""
        age = as_of_date.year - date_of_birth.year
        if (as_of_date.month, as_of_date.day) < (date_of_birth.month, date_of_birth.day):
            age -= 1
        return age
