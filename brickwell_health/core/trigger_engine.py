"""
Event Trigger Engine for CRM Domain.

Determines which CRM events should be triggered based on claims/billing events.
Uses a probability matrix with both fixed and conditional probabilities.
"""

from typing import Any, Callable, Optional

from numpy.random import Generator as RNG


class EventTriggerEngine:
    """
    Engine for determining CRM event triggers based on source events.

    Uses a probability matrix where each source event type maps to target
    CRM actions with associated probabilities. Probabilities can be:
    - Fixed floats (e.g., 0.80 for 80% chance)
    - Conditional callables that take context and return a probability

    Usage:
        engine = EventTriggerEngine(rng, config)
        triggered = engine.get_triggered_events("claim_rejected", {"charge_amount": 1500})
        # Returns: ["interaction", "case", "complaint"] or subset based on dice rolls
    """

    def __init__(self, rng: RNG, config: dict | None = None):
        """
        Initialize the trigger engine.

        Args:
            rng: NumPy random number generator for reproducibility
            config: Optional configuration overrides for trigger probabilities
        """
        self.rng = rng
        self.config = config or {}
        self._load_trigger_matrix()

    def _load_trigger_matrix(self) -> None:
        """Load trigger probability matrix from config or defaults."""
        self.triggers: dict[str, dict[str, float | Callable]] = {
            # Claims triggers
            "claim_submitted": {
                "interaction": 0.40,
                "communication": 1.00,
            },
            "claim_rejected": {
                "interaction": 0.80,
                "case": self._case_on_high_value,
                "complaint": self._complaint_on_very_high_value,
                "communication": 1.00,
                "nps_survey": 0.50,
            },
            "claim_delayed": {
                "interaction": 0.30,
                "complaint": 0.10,
                "communication": 0.50,
            },
            "claim_paid": {
                "interaction": 0.10,
                "communication": 1.00,
                "nps_survey": 0.30,
            },
            # Billing triggers
            "payment_failed": {
                "interaction": 0.60,
                "case": self._case_on_third_failure,
                "communication": 1.00,
            },
            "arrears_created": {
                "interaction": 0.50,
                "case": 1.00,
                "communication": 1.00,
            },
            "policy_suspended": {
                "interaction": 0.70,
                "complaint": 0.20,
                "communication": 1.00,
            },
            # CRM triggers (for survey triggering)
            "interaction_completed": {
                "case": self._case_on_non_fcr,
                "nps_survey": 0.20,
                "csat_survey": 0.40,
            },
            "case_resolved": {
                "csat_survey": 0.60,
            },
            "complaint_resolved": {
                "nps_survey": 0.80,
                "csat_survey": 0.60,
            },
        }

        # Override with config values if provided
        if "event_triggers" in self.config:
            for event_type, probs in self.config["event_triggers"].items():
                if event_type in self.triggers:
                    for target, prob in probs.items():
                        # Only override non-callable values
                        if not callable(self.triggers[event_type].get(target)):
                            self.triggers[event_type][target] = prob

    def _case_on_high_value(self, context: dict) -> float:
        """
        Create case if charge > $500.

        Args:
            context: Event context with charge_amount

        Returns:
            Probability of creating a case (0.30 if high value, 0.0 otherwise)
        """
        charge = context.get("charge_amount", 0)
        threshold = self.config.get("case_threshold", 500)
        return 0.30 if charge > threshold else 0.0

    def _complaint_on_very_high_value(self, context: dict) -> float:
        """
        Create complaint if charge > $1000.

        Args:
            context: Event context with charge_amount

        Returns:
            Probability of creating a complaint
        """
        charge = context.get("charge_amount", 0)
        threshold = self.config.get("complaint_threshold", 1000)
        return 0.15 if charge > threshold else 0.05

    def _case_on_third_failure(self, context: dict) -> float:
        """
        Create case on 3rd failed payment attempt.

        Args:
            context: Event context with attempt_number

        Returns:
            1.0 if 3rd or later attempt, 0.0 otherwise
        """
        attempt = context.get("attempt_number", 1)
        return 1.00 if attempt >= 3 else 0.0

    def _case_on_non_fcr(self, context: dict) -> float:
        """
        Create case if interaction was not resolved on first contact.

        Args:
            context: Event context with first_contact_resolution

        Returns:
            0.30 if not FCR, 0.0 otherwise
        """
        fcr = context.get("first_contact_resolution", True)
        return 0.30 if not fcr else 0.0

    def get_triggered_events(
        self,
        event_type: str,
        context: dict | None = None,
    ) -> list[str]:
        """
        Determine which CRM events should be triggered.

        Args:
            event_type: Type of source event (claim_rejected, payment_failed, etc.)
            context: Event-specific context (charge_amount, attempt_number, etc.)

        Returns:
            List of event types to trigger (interaction, case, complaint, etc.)
        """
        context = context or {}
        triggered: list[str] = []

        if event_type not in self.triggers:
            return triggered

        for target_event, probability in self.triggers[event_type].items():
            # Handle conditional probabilities (callables)
            if callable(probability):
                probability = probability(context)

            # Skip if probability is 0 or negative
            if probability <= 0:
                continue

            # Roll the dice
            if self.rng.random() < probability:
                triggered.append(target_event)

        return triggered

    def get_interaction_type_for_trigger(self, event_type: str) -> str:
        """
        Get the appropriate interaction type code for a trigger event.

        Args:
            event_type: The source event type

        Returns:
            Interaction type code for reference data lookup
        """
        mapping = {
            "claim_submitted": "CLAIM_STATUS",
            "claim_rejected": "CLAIM_DISPUTE",
            "claim_delayed": "CLAIM_STATUS",
            "claim_paid": "CLAIM_STATUS",
            "payment_failed": "BILLING_INQUIRY",
            "arrears_created": "BILLING_DISPUTE",
            "policy_suspended": "PAYMENT_ARRANGEMENT",
        }
        return mapping.get(event_type, "GENERAL_INQUIRY")

    def get_case_type_for_trigger(self, event_type: str) -> str:
        """
        Get the appropriate case type code for a trigger event.

        Args:
            event_type: The source event type

        Returns:
            Case type code for reference data lookup
        """
        mapping = {
            "claim_rejected": "CLAIM_DISPUTE",
            "payment_failed": "PAYMENT_ISSUE",
            "arrears_created": "PAYMENT_ISSUE",
            "policy_suspended": "HARDSHIP",
            "interaction_completed": "GENERAL",
        }
        return mapping.get(event_type, "GENERAL")

    def get_complaint_category_for_trigger(self, event_type: str) -> str:
        """
        Get the appropriate complaint category for a trigger event.

        Args:
            event_type: The source event type

        Returns:
            Complaint category code for reference data lookup
        """
        mapping = {
            "claim_rejected": "CLAIM_DENIAL",
            "claim_delayed": "CLAIM_DELAY",
            "payment_failed": "BILLING_ERROR",
            "arrears_created": "ARREARS_DISPUTE",
            "policy_suspended": "PREMIUM_INCREASE",
        }
        return mapping.get(event_type, "OTHER")

    def should_trigger(
        self,
        event_type: str,
        target: str,
        context: dict | None = None,
    ) -> bool:
        """
        Check if a specific target should be triggered for an event.

        Useful for checking individual triggers without getting all triggered events.

        Args:
            event_type: Type of source event
            target: Target to check (interaction, case, complaint, etc.)
            context: Event-specific context

        Returns:
            True if target should be triggered, False otherwise
        """
        context = context or {}

        if event_type not in self.triggers:
            return False

        if target not in self.triggers[event_type]:
            return False

        probability = self.triggers[event_type][target]

        if callable(probability):
            probability = probability(context)

        if probability <= 0:
            return False

        return self.rng.random() < probability

    def predict_escalation(
        self,
        event_type: str,
        context: dict | None = None,
    ) -> dict:
        """
        Predict escalation probability and type based on claim characteristics.

        Uses a full factor model including:
        - charge_amount: Higher amounts increase escalation probability
        - denial_reason: Certain denial reasons trigger more complaints
        - member_tenure: Long-term members more likely to escalate
        - claim_history: Members with prior rejections more likely to escalate
        - digital_engagement: Low digital engagement correlates with phone calls

        This method makes a UNIFIED decision - the same dice roll that determines
        escalation also determines what CRM activity will be created.

        Args:
            event_type: Type of claim event (claim_paid, claim_rejected)
            context: Event context including:
                - charge_amount: Decimal amount
                - denial_reason: str (for rejected claims)
                - member_tenure_days: int
                - prior_claim_rejections: int
                - digital_engagement: str ("high", "medium", "low")

        Returns:
            {
                "will_escalate": bool,
                "escalation_type": "interaction" | "case" | "complaint" | None,
                "highest_level": "complaint" | "case" | "interaction" | None,
                "triggered_actions": list[str],  # All CRM actions triggered
                "factors": {
                    "amount_factor": float,
                    "reason_factor": float,
                    "tenure_factor": float,
                    "history_factor": float,
                    "engagement_factor": float,
                    "base_probability": float,
                    "final_probability": float,
                }
            }
        """
        context = context or {}

        # Get base probabilities from trigger matrix
        if event_type not in self.triggers:
            return {
                "will_escalate": False,
                "escalation_type": None,
                "highest_level": None,
                "triggered_actions": [],
                "factors": {},
            }

        # Calculate factor adjustments for rejected claims (full model)
        factors = self._calculate_escalation_factors(event_type, context)

        # Use the unified decision: call get_triggered_events which rolls dice
        # This ensures the same decision is used for both CRM creation and journey
        triggered = self.get_triggered_events(event_type, context)

        # Determine escalation hierarchy: complaint > case > interaction
        escalation_type = None
        highest_level = None

        if "complaint" in triggered:
            escalation_type = "complaint"
            highest_level = "complaint"
        elif "case" in triggered:
            escalation_type = "case"
            highest_level = "case"
        elif "interaction" in triggered:
            escalation_type = "interaction"
            highest_level = "interaction"

        will_escalate = escalation_type is not None

        return {
            "will_escalate": will_escalate,
            "escalation_type": escalation_type,
            "highest_level": highest_level,
            "triggered_actions": triggered,
            "factors": factors,
        }

    def _calculate_escalation_factors(
        self,
        event_type: str,
        context: dict,
    ) -> dict:
        """
        Calculate escalation factor adjustments based on claim characteristics.

        These factors provide transparency into why escalation was predicted.

        Args:
            event_type: Type of claim event
            context: Event context

        Returns:
            Dictionary of factor contributions
        """
        factors = {
            "amount_factor": 0.0,
            "reason_factor": 0.0,
            "tenure_factor": 0.0,
            "history_factor": 0.0,
            "engagement_factor": 0.0,
            "base_probability": 0.0,
            "final_probability": 0.0,
        }

        # Base probability from trigger matrix
        if event_type == "claim_rejected":
            factors["base_probability"] = 0.80  # Base interaction rate
        elif event_type == "claim_paid":
            factors["base_probability"] = 0.10  # Lower base for paid claims
        else:
            factors["base_probability"] = 0.30

        # Amount factor: higher amounts = higher escalation
        charge_amount = context.get("charge_amount", 0)
        if charge_amount > 1000:
            factors["amount_factor"] = 0.20
        elif charge_amount > 500:
            factors["amount_factor"] = 0.10
        elif charge_amount > 200:
            factors["amount_factor"] = 0.05

        # Denial reason factor (only for rejected claims)
        denial_reason = context.get("denial_reason", "")
        high_escalation_reasons = [
            "PolicyExclusions",
            "PreExisting",
            "LimitsExhausted",
        ]
        medium_escalation_reasons = [
            "ProviderIssues",
            "Administrative",
        ]

        if denial_reason in high_escalation_reasons:
            factors["reason_factor"] = 0.15
        elif denial_reason in medium_escalation_reasons:
            factors["reason_factor"] = 0.05

        # Tenure factor: long-term members more likely to complain
        tenure_days = context.get("member_tenure_days", 365)
        if tenure_days > 1825:  # 5+ years
            factors["tenure_factor"] = 0.10
        elif tenure_days > 730:  # 2+ years
            factors["tenure_factor"] = 0.05

        # Claim history factor: prior rejections increase escalation
        prior_rejections = context.get("prior_claim_rejections", 0)
        if prior_rejections >= 3:
            factors["history_factor"] = 0.15
        elif prior_rejections >= 1:
            factors["history_factor"] = 0.08

        # Digital engagement factor: low engagement = more phone calls
        engagement = context.get("digital_engagement", "medium")
        if engagement == "low":
            factors["engagement_factor"] = 0.10
        elif engagement == "high":
            factors["engagement_factor"] = -0.05  # High digital = less phone calls

        # Calculate final probability (capped at 0.95)
        final_prob = (
            factors["base_probability"]
            + factors["amount_factor"]
            + factors["reason_factor"]
            + factors["tenure_factor"]
            + factors["history_factor"]
            + factors["engagement_factor"]
        )
        factors["final_probability"] = min(0.95, max(0.0, final_prob))

        return factors
