"""
ID generator for Brickwell Health Simulator.

Generates unique identifiers (UUIDs, member numbers, policy numbers, etc.)
in a deterministic, reproducible manner.
"""

from datetime import date
from uuid import UUID

from numpy.random import Generator as RNG


class IDGenerator:
    """
    Generates unique identifiers for simulation entities.

    All IDs are generated deterministically from the RNG for reproducibility.
    Worker ID is included in sequential numbers to ensure uniqueness across workers.

    Usage:
        id_gen = IDGenerator(rng, worker_id=0)
        member_id = id_gen.generate_uuid()
        member_number = id_gen.generate_member_number()
        policy_number = id_gen.generate_policy_number()
    """

    def __init__(self, rng: RNG, prefix_year: int = 2024, worker_id: int = 0):
        """
        Initialize the ID generator.

        Args:
            rng: NumPy random number generator
            prefix_year: Year to use in number prefixes
            worker_id: Worker ID for multi-worker uniqueness
        """
        self.rng = rng
        self.prefix_year = prefix_year
        self.worker_id = worker_id

        # Counters for sequential parts of numbers
        self._member_counter = 0
        self._policy_counter = 0
        self._application_counter = 0
        self._claim_counter = 0
        self._invoice_counter = 0
        self._payment_counter = 0
        self._refund_counter = 0
        self._mandate_counter = 0
        
        # NBA/NPS domain counters
        self._interaction_counter = 0
        self._case_counter = 0
        self._complaint_counter = 0
        self._communication_counter = 0
        self._campaign_counter = 0
        self._nps_survey_counter = 0
        self._csat_survey_counter = 0

    def generate_uuid(self) -> UUID:
        """
        Generate a random UUID.

        Uses the RNG for reproducibility.

        Returns:
            Random UUID
        """
        random_bytes = bytearray(self.rng.bytes(16))
        # Set version 4 (random) UUID bits
        random_bytes[6] = (random_bytes[6] & 0x0F) | 0x40
        random_bytes[8] = (random_bytes[8] & 0x3F) | 0x80
        return UUID(bytes=bytes(random_bytes))

    def generate_member_number(self) -> str:
        """
        Generate a unique member number.

        Format: MEM-WN-YYYY-NNNNNN (N = worker_id)

        Returns:
            Member number string
        """
        self._member_counter += 1
        return f"MEM-W{self.worker_id}-{self.prefix_year}-{self._member_counter:06d}"

    def generate_policy_number(self) -> str:
        """
        Generate a unique policy number.

        Format: POL-WN-YYYY-NNNNNN

        Returns:
            Policy number string
        """
        self._policy_counter += 1
        return f"POL-W{self.worker_id}-{self.prefix_year}-{self._policy_counter:06d}"

    def generate_application_number(self) -> str:
        """
        Generate a unique application number.

        Format: APP-WN-YYYY-NNNNNN

        Returns:
            Application number string
        """
        self._application_counter += 1
        return f"APP-W{self.worker_id}-{self.prefix_year}-{self._application_counter:06d}"

    def generate_claim_number(self) -> str:
        """
        Generate a unique claim number.

        Format: CLM-WN-YYYY-NNNNNNNN

        Returns:
            Claim number string
        """
        self._claim_counter += 1
        return f"CLM-W{self.worker_id}-{self.prefix_year}-{self._claim_counter:08d}"

    def generate_invoice_number(self) -> str:
        """
        Generate a unique invoice number.

        Format: INV-WN-YYYY-NNNNNN

        Returns:
            Invoice number string
        """
        self._invoice_counter += 1
        return f"INV-W{self.worker_id}-{self.prefix_year}-{self._invoice_counter:06d}"

    def generate_payment_number(self) -> str:
        """
        Generate a unique payment number.

        Format: PAY-WN-YYYY-NNNNNN

        Returns:
            Payment number string
        """
        self._payment_counter += 1
        return f"PAY-W{self.worker_id}-{self.prefix_year}-{self._payment_counter:06d}"

    def generate_refund_reference(self) -> str:
        """
        Generate a unique refund reference.

        Format: REF-WN-YYYY-NNNNNN

        Returns:
            Refund reference string
        """
        self._refund_counter += 1
        return f"REF-W{self.worker_id}-{self.prefix_year}-{self._refund_counter:06d}"

    def generate_mandate_reference(self) -> str:
        """
        Generate a unique direct debit mandate reference.

        Format: DDR-WN-YYYY-NNNNNN

        Returns:
            Mandate reference string
        """
        self._mandate_counter += 1
        return f"DDR-W{self.worker_id}-{self.prefix_year}-{self._mandate_counter:06d}"

    # =========================================================================
    # NBA/NPS Domain ID Methods
    # =========================================================================

    def generate_interaction_reference(self) -> str:
        """
        Generate a unique interaction reference.

        Format: INT-WN-YYYY-NNNNNN

        Returns:
            Interaction reference string
        """
        self._interaction_counter += 1
        return f"INT-W{self.worker_id}-{self.prefix_year}-{self._interaction_counter:06d}"

    def generate_case_number(self) -> str:
        """
        Generate a unique case number.

        Format: CASE-WN-YYYY-NNNNNN

        Returns:
            Case number string
        """
        self._case_counter += 1
        return f"CASE-W{self.worker_id}-{self.prefix_year}-{self._case_counter:06d}"

    def generate_complaint_number(self) -> str:
        """
        Generate a unique complaint number.

        Format: COMP-WN-YYYY-NNNNNN

        Returns:
            Complaint number string
        """
        self._complaint_counter += 1
        return f"COMP-W{self.worker_id}-{self.prefix_year}-{self._complaint_counter:06d}"

    def generate_communication_reference(self) -> str:
        """
        Generate a unique communication reference.

        Format: COMM-WN-YYYY-NNNNNN

        Returns:
            Communication reference string
        """
        self._communication_counter += 1
        return f"COMM-W{self.worker_id}-{self.prefix_year}-{self._communication_counter:06d}"

    def generate_campaign_code(self, campaign_type: str = "GEN") -> str:
        """
        Generate a unique campaign code.

        Format: {TYPE}-WN-YYYY-NNN

        Args:
            campaign_type: Campaign type (e.g., "RET", "UPS", "ENG")

        Returns:
            Campaign code string
        """
        self._campaign_counter += 1
        type_prefix = campaign_type[:3].upper()
        return f"{type_prefix}-W{self.worker_id}-{self.prefix_year}-{self._campaign_counter:03d}"

    def generate_nps_survey_reference(self) -> str:
        """
        Generate a unique NPS survey reference.

        Format: NPS-WN-YYYY-NNNNNN

        Returns:
            NPS survey reference string
        """
        self._nps_survey_counter += 1
        return f"NPS-W{self.worker_id}-{self.prefix_year}-{self._nps_survey_counter:06d}"

    def generate_csat_survey_reference(self) -> str:
        """
        Generate a unique CSAT survey reference.

        Format: CSAT-WN-YYYY-NNNNNN

        Returns:
            CSAT survey reference string
        """
        self._csat_survey_counter += 1
        return f"CSAT-W{self.worker_id}-{self.prefix_year}-{self._csat_survey_counter:06d}"

    def generate_medicare_number(self) -> str:
        """
        Generate a valid-format Medicare number.

        Format: 10 digits followed by IRN (1-9)

        Returns:
            Medicare number string
        """
        # 10-digit number
        digits = self.rng.integers(2000000000, 9999999999)
        # IRN (Individual Reference Number) 1-9
        irn = self.rng.integers(1, 10)
        return f"{digits}{irn}"

    def generate_bsb(self) -> str:
        """
        Generate a valid-format BSB number.

        Format: NNN-NNN

        Returns:
            BSB string
        """
        # First 3 digits indicate bank/state
        bank_codes = ["062", "063", "082", "083", "084", "033", "034", "013", "014"]
        bank = self.rng.choice(bank_codes)
        branch = self.rng.integers(100, 999)
        return f"{bank}-{branch}"

    def generate_masked_account_number(self) -> str:
        """
        Generate a masked bank account number.

        Format: ****NNNN

        Returns:
            Masked account number
        """
        last_four = self.rng.integers(1000, 9999)
        return f"****{last_four}"

    def set_year(self, year: int) -> None:
        """
        Set the year used in number prefixes.

        Args:
            year: Year for number prefixes
        """
        self.prefix_year = year

    def set_counters(
        self,
        member: int = 0,
        policy: int = 0,
        application: int = 0,
        claim: int = 0,
        invoice: int = 0,
        payment: int = 0,
        refund: int = 0,
        mandate: int = 0,
        interaction: int = 0,
        case: int = 0,
        complaint: int = 0,
        communication: int = 0,
        campaign: int = 0,
        nps_survey: int = 0,
        csat_survey: int = 0,
    ) -> None:
        """
        Set counter values (for checkpoint recovery).

        Args:
            member: Member counter value
            policy: Policy counter value
            application: Application counter value
            claim: Claim counter value
            invoice: Invoice counter value
            payment: Payment counter value
            refund: Refund counter value
            mandate: Mandate counter value
            interaction: Interaction counter value
            case: Case counter value
            complaint: Complaint counter value
            communication: Communication counter value
            campaign: Campaign counter value
            nps_survey: NPS survey counter value
            csat_survey: CSAT survey counter value
        """
        self._member_counter = member
        self._policy_counter = policy
        self._application_counter = application
        self._claim_counter = claim
        self._invoice_counter = invoice
        self._payment_counter = payment
        self._refund_counter = refund
        self._mandate_counter = mandate
        self._interaction_counter = interaction
        self._case_counter = case
        self._complaint_counter = complaint
        self._communication_counter = communication
        self._campaign_counter = campaign
        self._nps_survey_counter = nps_survey
        self._csat_survey_counter = csat_survey

    def get_counters(self) -> dict[str, int]:
        """
        Get current counter values (for checkpointing).

        Returns:
            Dictionary of counter names to values
        """
        return {
            "member": self._member_counter,
            "policy": self._policy_counter,
            "application": self._application_counter,
            "claim": self._claim_counter,
            "invoice": self._invoice_counter,
            "payment": self._payment_counter,
            "refund": self._refund_counter,
            "mandate": self._mandate_counter,
            "interaction": self._interaction_counter,
            "case": self._case_counter,
            "complaint": self._complaint_counter,
            "communication": self._communication_counter,
            "campaign": self._campaign_counter,
            "nps_survey": self._nps_survey_counter,
            "csat_survey": self._csat_survey_counter,
        }
