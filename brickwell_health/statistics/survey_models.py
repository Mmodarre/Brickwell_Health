"""
Survey Statistical Models for Brickwell Health Simulator.

Provides response prediction and fallback score sampling for NPS/CSAT surveys.
"""

from typing import Any, Optional

import numpy as np
from numpy.random import Generator as RNG


class SurveyResponsePredictor:
    """
    Predicts whether a member will respond to a survey.

    Uses a logistic regression-style model with factors:
    - Tenure
    - Recent negative experience (claim rejection, complaint)
    - Digital engagement level
    - Survey fatigue
    - Member age
    """

    def __init__(self, rng: RNG, config: Optional[dict] = None):
        """
        Initialize the response predictor.

        Args:
            rng: NumPy random number generator
            config: Optional survey configuration
        """
        self.rng = rng
        self.config = config or {}

    def predict_nps_response(self, context: dict) -> tuple[bool, float]:
        """
        Predict if member will respond to NPS survey.

        Args:
            context: Survey context including member info, trigger, history

        Returns:
            Tuple of (will_respond, probability)
        """
        # Get base response rate by survey type
        survey_type = context.get("survey_type", "POST_CLAIM")

        nps_config = self.config.get("nps", {})
        triggers = nps_config.get("triggers", {})

        # Map survey type to trigger config key
        trigger_key = survey_type.lower().replace("post", "").replace("_", "")
        trigger_config = triggers.get(trigger_key, {})

        if isinstance(trigger_config, dict):
            base_rate = trigger_config.get("response_rate", 0.18)
        else:
            base_rate = nps_config.get("base_response_rate", 0.18)

        # Calculate log-odds for adjustments
        log_odds = np.log(base_rate / (1 - base_rate))

        # Factor 1: Tenure (longer tenure = more likely to respond)
        tenure_months = context.get("tenure_months", 12)
        if tenure_months > 24:
            log_odds += 0.2
        elif tenure_months < 6:
            log_odds -= 0.3

        # Factor 2: Recent negative experience (angry members want to vent)
        if context.get("recent_claim_rejected"):
            log_odds += 0.5
        if context.get("recent_complaint"):
            log_odds += 0.4

        # Factor 3: Digital engagement
        engagement_level = context.get("engagement_level", "medium")
        if engagement_level == "high":
            log_odds += 0.3
        elif engagement_level == "low":
            log_odds -= 0.2

        # Factor 4: Survey fatigue (too many surveys = less likely)
        surveys_last_6_months = context.get("surveys_received_6mo", 0)
        if surveys_last_6_months >= 3:
            log_odds -= 0.4
        elif surveys_last_6_months >= 2:
            log_odds -= 0.2

        # Factor 5: Age (older members more likely to respond)
        age = context.get("member_age", 40)
        if age > 55:
            log_odds += 0.15
        elif age < 30:
            log_odds -= 0.15

        # Convert back to probability
        probability = 1 / (1 + np.exp(-log_odds))

        # Make decision
        will_respond = self.rng.random() < probability

        return will_respond, float(probability)

    def predict_csat_response(self, context: dict) -> tuple[bool, float]:
        """
        Predict if member will respond to CSAT survey.

        Higher base rate than NPS due to shorter survey length.

        Args:
            context: Survey context

        Returns:
            Tuple of (will_respond, probability)
        """
        base_rate = self.config.get("csat", {}).get("base_response_rate", 0.35)

        log_odds = np.log(base_rate / (1 - base_rate))

        # FCR impacts willingness to respond
        if context.get("first_contact_resolution"):
            log_odds += 0.2  # Satisfied, quick to respond
        else:
            log_odds += 0.3  # Want to complain about unresolved issue

        # Recent timing (fresher = more likely)
        hours_since_interaction = context.get("hours_since_interaction", 24)
        if hours_since_interaction < 2:
            log_odds += 0.3
        elif hours_since_interaction > 48:
            log_odds -= 0.3

        # SLA breach makes members more likely to respond (negative feedback)
        if context.get("sla_breached"):
            log_odds += 0.4

        probability = 1 / (1 + np.exp(-log_odds))
        will_respond = self.rng.random() < probability

        return will_respond, float(probability)


class CRMStatisticalModels:
    """
    Statistical models for CRM data simulation.

    Provides fallback sampling for NPS/CSAT scores and response timing
    when LLM processing is not available or fails.
    """

    def __init__(self, rng: RNG, config: Optional[dict] = None):
        """
        Initialize the CRM statistical models.

        Args:
            rng: NumPy random number generator
            config: Optional configuration
        """
        self.rng = rng
        self.config = config or {}

    def sample_nps_score(self, context: Optional[dict] = None) -> int:
        """
        Sample NPS score based on context.

        Used as fallback if LLM processing fails.

        Args:
            context: Optional context with trigger info

        Returns:
            NPS score (0-10)
        """
        # Base distribution (Australian health insurance typical)
        # NPS industry average is around +5 to +15
        base_probs = [
            0.02,
            0.02,
            0.03,
            0.04,
            0.05,  # 0-4: Detractors
            0.05,
            0.04,  # 5-6: Detractors
            0.22,
            0.23,  # 7-8: Passives
            0.18,
            0.12,  # 9-10: Promoters
        ]

        if context:
            # Adjust based on trigger
            trigger_event = context.get("trigger_event", "")
            if trigger_event == "ClaimRejected":
                base_probs = self._shift_distribution(base_probs, -3)
            elif trigger_event == "ClaimPaid":
                processing_days = context.get("processing_days", 7)
                if processing_days <= 3:
                    base_probs = self._shift_distribution(base_probs, +2)
                elif processing_days <= 7:
                    base_probs = self._shift_distribution(base_probs, +1)

            # Complaint history shifts negative
            if context.get("recent_complaint"):
                base_probs = self._shift_distribution(base_probs, -2)

        return int(self.rng.choice(range(11), p=base_probs))

    def _shift_distribution(self, probs: list, shift: int) -> list:
        """
        Shift probability distribution by shift points.

        Args:
            probs: Original probability distribution
            shift: Number of points to shift (positive = higher scores)

        Returns:
            Shifted and normalized probability distribution
        """
        shifted = [0.0] * 11
        for i, p in enumerate(probs):
            new_idx = max(0, min(10, i + shift))
            shifted[new_idx] += p

        # Normalize
        total = sum(shifted)
        return [p / total for p in shifted]

    def get_nps_category(self, score: int) -> str:
        """
        Convert NPS score to category.

        Args:
            score: NPS score (0-10)

        Returns:
            Category string (Promoter, Passive, Detractor)
        """
        if score >= 9:
            return "Promoter"
        elif score >= 7:
            return "Passive"
        else:
            return "Detractor"

    def sample_csat_score(self, context: Optional[dict] = None) -> int:
        """
        Sample CSAT score (1-5).

        Args:
            context: Optional context with interaction info

        Returns:
            CSAT score (1-5)
        """
        # Default distribution (higher = more common)
        probs = [0.04, 0.08, 0.18, 0.40, 0.30]  # 1-5

        if context:
            if context.get("first_contact_resolution"):
                # FCR shifts positive
                probs = [0.02, 0.05, 0.15, 0.38, 0.40]
            elif context.get("sla_breached"):
                # SLA breach shifts negative
                probs = [0.10, 0.15, 0.30, 0.30, 0.15]

        return int(self.rng.choice(range(1, 6), p=probs))

    def get_csat_label(self, score: int) -> str:
        """
        Convert CSAT score to label.

        Args:
            score: CSAT score (1-5)

        Returns:
            Label string
        """
        labels = {
            1: "VeryDissatisfied",
            2: "Dissatisfied",
            3: "Neutral",
            4: "Satisfied",
            5: "VerySatisfied",
        }
        return labels.get(score, "Neutral")

    def sample_response_time_minutes(self, survey_type: str = "nps") -> int:
        """
        Sample response time using lognormal distribution.

        Args:
            survey_type: Type of survey ("nps" or "csat")

        Returns:
            Response time in minutes
        """
        if survey_type.lower() == "csat":
            # CSAT typically faster (shorter survey, sent right after interaction)
            # Median ~30 minutes, most within 24 hours
            mu = 3.4  # ln(30) ≈ 3.4
            sigma = 1.2
            min_time = 5
            max_time = 1440 * 3  # 3 days
        else:
            # NPS takes longer (more questions, less urgent)
            # Median ~4 hours, most within 72 hours
            mu = 5.5  # ln(240) ≈ 5.5
            sigma = 1.0
            min_time = 10
            max_time = 1440 * 7  # 7 days

        response_time = self.rng.lognormal(mu, sigma)
        return int(min(max(response_time, min_time), max_time))

    def sample_driver_scores(
        self, nps_score: int, context: Optional[dict] = None
    ) -> dict[str, int]:
        """
        Sample driver scores correlated with NPS score.

        Drivers should be within ±3 of NPS score typically.

        Args:
            nps_score: The overall NPS score
            context: Optional context for specific drivers

        Returns:
            Dictionary of driver scores
        """
        drivers = {}

        for driver in [
            "claims_processing",
            "customer_service",
            "value_for_money",
            "coverage_clarity",
            "digital_experience",
        ]:
            # Base score near NPS score with some variance
            base = nps_score + self.rng.normal(0, 1.5)

            # Context-specific adjustments
            if context:
                if driver == "claims_processing":
                    if context.get("trigger_event") == "ClaimRejected":
                        base -= 2
                    elif context.get("trigger_event") == "ClaimPaid":
                        base += 1
                elif driver == "customer_service":
                    if context.get("recent_complaint"):
                        base -= 1.5
                elif driver == "value_for_money":
                    if context.get("in_arrears"):
                        base -= 1

            # Clamp to 0-10
            drivers[driver] = int(max(0, min(10, round(base))))

        return drivers

    def sample_sentiment_score(self, nps_score: int) -> float:
        """
        Sample sentiment score correlated with NPS score.

        Args:
            nps_score: The NPS score

        Returns:
            Sentiment score (-1 to 1)
        """
        # Map NPS to sentiment range with some noise
        # Detractors: -1 to -0.3
        # Passives: -0.3 to 0.3
        # Promoters: 0.3 to 1.0

        if nps_score <= 6:  # Detractor
            base = -0.6 + (nps_score / 6) * 0.3
            noise = self.rng.normal(0, 0.15)
        elif nps_score <= 8:  # Passive
            base = -0.15 + ((nps_score - 7) / 2) * 0.3
            noise = self.rng.normal(0, 0.1)
        else:  # Promoter
            base = 0.5 + ((nps_score - 9) / 1) * 0.3
            noise = self.rng.normal(0, 0.1)

        return max(-1.0, min(1.0, base + noise))

    def get_sentiment_label(self, sentiment_score: float) -> str:
        """
        Convert sentiment score to label.

        Args:
            sentiment_score: Sentiment score (-1 to 1)

        Returns:
            Label string (Positive, Neutral, Negative)
        """
        if sentiment_score >= 0.25:
            return "Positive"
        elif sentiment_score <= -0.25:
            return "Negative"
        else:
            return "Neutral"
