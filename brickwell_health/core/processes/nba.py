"""
NBA Action Process for Brickwell Health Simulator.

Processes NBA (Next Best Action) recommendations from the queue,
applies contact policy rules, records executions, and creates
behavioral effects for consumption by other processes.
"""

from datetime import date, datetime, timedelta
from typing import Any, Generator, TYPE_CHECKING
from uuid import UUID

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.nba import (
    ActionCategory,
    NBAActionWithRecommendation,
    NBAChannel,
    NBAExecutionCreate,
    ImmediateResponse,
    ExecutionMethod,
    RecommendationStatus,
)

if TYPE_CHECKING:
    from brickwell_health.core.shared_state import SharedState


logger = structlog.get_logger()


class NBAActionProcess(BaseProcess):
    """
    NBA Action process for executing recommendations.

    This process:
    1. Consumes NBA recommendations from the SharedState queue (loaded at startup)
    2. Applies contact policy rules (cooldowns, fatigue limits)
    3. Executes actions based on category (Retention, Upsell, CrossSell, Service, Wellness)
    4. Creates behavioral effects for Retention/Upsell/CrossSell actions
    5. Emits events to CRM/Communication queues for interaction/message creation
    6. Records executions in the database
    7. Updates recommendation statuses
    8. Expires old recommendations and effects
    """

    def __init__(
        self,
        *args: Any,
        shared_state: "SharedState | None" = None,
        **kwargs: Any,
    ):
        """
        Initialize the NBA Action process.

        Args:
            shared_state: Shared state for cross-process communication
        """
        super().__init__(*args, **kwargs)
        self.shared_state = shared_state

        # Get NBA configuration
        self.nba_config = getattr(self.config, "nba", None)
        if self.nba_config is None:
            # Create minimal default config if not present
            from brickwell_health.config.models import NBAConfig
            self.nba_config = NBAConfig()

        # Initialize statistics
        self._stats = {
            "recommendations_processed": 0,
            "executions_recorded": 0,
            "recommendations_suppressed": 0,
            "recommendations_expired": 0,
            "effects_created": 0,
            "effects_expired": 0,
            "crm_events_emitted": 0,
            "communication_events_emitted": 0,
        }

    def run(self) -> Generator:
        """
        Main process loop - runs daily.

        Each day:
        1. Expire old recommendations past their valid_until date
        2. Process available recommendations
        3. Expire old behavioral effects
        """
        logger.info(
            "nba_process_started",
            worker_id=self.worker_id,
        )

        while True:
            current_date = self.sim_env.current_date

            # 1. Expire recommendations past their validity window
            self._expire_old_recommendations(current_date)

            # 2. Process daily recommendations
            self._process_daily_recommendations(current_date)

            # 3. Expire old behavioral effects
            self._expire_old_effects()

            # Wait for next day
            yield self.env.timeout(1.0)

            # Log progress monthly
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def _process_daily_recommendations(self, current_date: date) -> None:
        """
        Process NBA recommendations for the current day.

        Gets recommendations from queue, applies contact policy,
        and executes valid actions.
        """
        if not self.shared_state:
            return

        # Get today's recommendations (FIFO from queue)
        # Note: Queue was populated at startup with valid recommendations
        recommendations = self.shared_state.get_nba_recommendations()

        if not recommendations:
            return

        # Track actions per member today for max_actions_per_member_per_day limit
        member_actions_today: dict[UUID, int] = {}

        for rec in recommendations:
            # Skip if past validity
            if rec.valid_until < current_date:
                self._update_recommendation_status(
                    rec.recommendation_id,
                    RecommendationStatus.EXPIRED,
                )
                self._stats["recommendations_expired"] += 1
                continue

            # Skip if not yet valid
            if rec.valid_from > current_date:
                # Put back in queue for later processing
                self.shared_state.add_nba_recommendation(rec)
                continue

            # Check max actions per member per day
            member_id = rec.member_id
            actions_today = member_actions_today.get(member_id, 0)
            max_per_day = self.nba_config.max_actions_per_member_per_day
            if actions_today >= max_per_day:
                # Put back in queue for tomorrow
                self.shared_state.add_nba_recommendation(rec)
                continue

            # Check contact policy
            allowed, suppression_reason = self._check_contact_policy(rec)

            if not allowed:
                # Leave status as pending (may retry within validity window)
                # Put back in queue for possible retry
                self.shared_state.add_nba_recommendation(rec)
                self._stats["recommendations_suppressed"] += 1
                logger.debug(
                    "nba_action_suppressed",
                    recommendation_id=str(rec.recommendation_id),
                    member_id=str(rec.member_id),
                    reason=suppression_reason,
                )
                continue

            # Execute the action
            self._execute_action(rec, current_date)
            member_actions_today[member_id] = actions_today + 1
            self._stats["recommendations_processed"] += 1

    def _check_contact_policy(
        self, recommendation: NBAActionWithRecommendation
    ) -> tuple[bool, str | None]:
        """
        Check if action is allowed by contact policy.

        Applies:
        1. Same-action cooldown
        2. Same-category cooldown
        3. Max attempts for this action
        4. Daily channel limits
        5. Weekly total limit

        Returns:
            Tuple of (allowed, suppression_reason)
        """
        if not self.shared_state:
            return True, None

        member_id = recommendation.member_id
        action_id = recommendation.action_id
        action_category = recommendation.action_category.value
        channel = recommendation.channel.value
        current_datetime = self.sim_env.current_datetime
        contact_policy = self.nba_config.contact_policy

        # 1. Check cooldown (same action and same category)
        is_blocked, reason = self.shared_state.check_nba_cooldown(
            member_id=member_id,
            action_id=action_id,
            action_category=action_category,
            same_action_cooldown_days=recommendation.cooldown_days,
            same_category_cooldown_days=contact_policy.same_category_cooldown_days,
            current_datetime=current_datetime,
        )
        if is_blocked:
            return False, reason

        # 2. Check max attempts for this specific action
        executions = self.shared_state.get_recent_nba_executions(
            member_id, days=365, current_datetime=current_datetime
        )
        action_count = sum(
            1 for e in executions if e.get("action_id") == action_id
        )
        if action_count >= recommendation.max_attempts:
            return False, f"Max attempts reached ({recommendation.max_attempts})"

        # 3. Check daily channel limits
        today_start = datetime.combine(self.sim_env.current_date, datetime.min.time())
        today_executions = [
            e for e in executions
            if e.get("executed_at", datetime.min) >= today_start
        ]

        channel_counts: dict[str, int] = {}
        for e in today_executions:
            ch = e.get("channel", "")
            channel_counts[ch] = channel_counts.get(ch, 0) + 1

        # Check specific channel limit
        daily_limits = {
            "Email": contact_policy.max_email_per_day,
            "SMS": contact_policy.max_sms_per_day,
            "Phone": contact_policy.max_phone_per_day,
            "InApp": contact_policy.max_inapp_per_day,
        }
        if channel in daily_limits:
            if channel_counts.get(channel, 0) >= daily_limits[channel]:
                return False, f"Daily {channel} limit reached"

        # Check total daily limit
        total_today = sum(channel_counts.values())
        if total_today >= contact_policy.max_total_per_day:
            return False, "Daily total contact limit reached"

        # 4. Check weekly limits
        week_start = today_start - timedelta(days=today_start.weekday())
        week_executions = [
            e for e in executions
            if e.get("executed_at", datetime.min) >= week_start
        ]

        week_channel_counts: dict[str, int] = {}
        for e in week_executions:
            ch = e.get("channel", "")
            week_channel_counts[ch] = week_channel_counts.get(ch, 0) + 1

        weekly_limits = {
            "Email": contact_policy.max_email_per_week,
            "SMS": contact_policy.max_sms_per_week,
            "Phone": contact_policy.max_phone_per_week,
            "InApp": contact_policy.max_inapp_per_week,
        }
        if channel in weekly_limits:
            if week_channel_counts.get(channel, 0) >= weekly_limits[channel]:
                return False, f"Weekly {channel} limit reached"

        # Check total weekly limit
        total_week = sum(week_channel_counts.values())
        if total_week >= contact_policy.max_total_per_week:
            return False, "Weekly total contact limit reached"

        return True, None

    def _execute_action(
        self, recommendation: NBAActionWithRecommendation, current_date: date
    ) -> None:
        """
        Execute an NBA action based on its category.

        Routes to appropriate handler and records execution.
        """
        # Sample immediate response
        immediate_response = self._sample_immediate_response(recommendation)

        # Generate execution ID
        execution_id = self.id_generator.generate_uuid()

        # Handle based on action category
        category = recommendation.action_category

        if category == ActionCategory.RETENTION:
            self._handle_retention_action(recommendation, execution_id)
        elif category == ActionCategory.UPSELL:
            self._handle_upsell_action(recommendation, execution_id)
        elif category == ActionCategory.CROSS_SELL:
            self._handle_crosssell_action(recommendation, execution_id)
        elif category == ActionCategory.SERVICE:
            self._handle_service_action(recommendation)
        elif category == ActionCategory.WELLNESS:
            self._handle_wellness_action(recommendation)

        # Record execution
        self._record_execution(
            recommendation=recommendation,
            execution_id=execution_id,
            immediate_response=immediate_response,
        )

        # Update recommendation status
        self._update_recommendation_status(
            recommendation.recommendation_id,
            RecommendationStatus.EXECUTED,
        )

        # Track in SharedState for cooldown checks
        if self.shared_state:
            self.shared_state.add_nba_execution(
                member_id=recommendation.member_id,
                execution_data={
                    "execution_id": execution_id,
                    "action_id": recommendation.action_id,
                    "action_category": recommendation.action_category.value,
                    "channel": recommendation.channel.value,
                    "executed_at": self.sim_env.current_datetime,
                },
            )

        logger.debug(
            "nba_action_executed",
            recommendation_id=str(recommendation.recommendation_id),
            member_id=str(recommendation.member_id),
            action_code=recommendation.action_code,
            category=category.value,
            channel=recommendation.channel.value,
            immediate_response=immediate_response.value if immediate_response else None,
        )

    def _handle_retention_action(
        self,
        recommendation: NBAActionWithRecommendation,
        execution_id: UUID,
    ) -> None:
        """
        Handle retention action - reduce churn probability.

        Creates behavioral effect and emits to appropriate queue.
        """
        # Create behavioral effect (churn reduction)
        self._create_behavioral_effect(
            recommendation=recommendation,
            execution_id=execution_id,
            effect_type="churn_reduction",
        )

        # Emit to appropriate queue based on channel
        self._emit_to_queue(recommendation)

    def _handle_upsell_action(
        self,
        recommendation: NBAActionWithRecommendation,
        execution_id: UUID,
    ) -> None:
        """
        Handle upsell action - increase upgrade probability.

        Creates behavioral effect and emits to communication queue.
        """
        # Create behavioral effect (upgrade boost)
        self._create_behavioral_effect(
            recommendation=recommendation,
            execution_id=execution_id,
            effect_type="upgrade_boost",
        )

        # Emit to communication queue (upsell is typically not phone)
        self._emit_communication_event(recommendation)

    def _handle_crosssell_action(
        self,
        recommendation: NBAActionWithRecommendation,
        execution_id: UUID,
    ) -> None:
        """
        Handle cross-sell action - increase upgrade probability.

        Cross-sell works same as upsell (adding coverage = policy upgrade).
        """
        # Create behavioral effect (upgrade boost)
        self._create_behavioral_effect(
            recommendation=recommendation,
            execution_id=execution_id,
            effect_type="upgrade_boost",
        )

        # Emit to communication queue
        self._emit_communication_event(recommendation)

    def _handle_service_action(
        self, recommendation: NBAActionWithRecommendation
    ) -> None:
        """
        Handle service action - proactive outreach.

        No behavioral effect, just creates interaction.
        """
        # Emit to CRM queue (service actions create interactions)
        self._emit_crm_event(recommendation)

    def _handle_wellness_action(
        self, recommendation: NBAActionWithRecommendation
    ) -> None:
        """
        Handle wellness action - engagement and reminders.

        No behavioral effect, just sends communication.
        """
        # Emit to communication queue
        self._emit_communication_event(recommendation)

    def _create_behavioral_effect(
        self,
        recommendation: NBAActionWithRecommendation,
        execution_id: UUID,
        effect_type: str,
    ) -> None:
        """
        Create a behavioral effect in SharedState.

        Effects are consumed by other processes (PolicyLifecycle) to
        modify probabilities.
        """
        if not self.shared_state or not recommendation.policy_id:
            return

        # Get effect duration from config
        effect_duration = getattr(
            self.nba_config.response, "effect_duration_days", 30
        )

        # Calculate expiry
        expires_at = self.sim_env.current_datetime + timedelta(days=effect_duration)

        # Multiplier from the action catalog
        multiplier = float(recommendation.probability_multiplier)

        self.shared_state.add_nba_effect(
            policy_id=recommendation.policy_id,
            effect_data={
                "effect_type": effect_type,
                "value": multiplier,
                "expires_at": expires_at,
                "source_action_id": recommendation.action_id,
                "source_execution_id": execution_id,
                "action_code": recommendation.action_code,
                "created_at": self.sim_env.current_datetime,
            },
        )
        self._stats["effects_created"] += 1

    def _emit_to_queue(self, recommendation: NBAActionWithRecommendation) -> None:
        """
        Emit to appropriate queue based on channel.

        Phone channel -> CRM queue (creates interaction)
        Others -> Communication queue (sends message)
        """
        if recommendation.channel == NBAChannel.PHONE:
            self._emit_crm_event(recommendation)
        else:
            self._emit_communication_event(recommendation)

    def _emit_crm_event(self, recommendation: NBAActionWithRecommendation) -> None:
        """
        Emit event to CRM queue for Phone channel actions.

        CRMProcess will consume this and create an outbound interaction.
        """
        if not self.shared_state:
            return

        self.shared_state.add_crm_event({
            "member_id": recommendation.member_id,
            "policy_id": recommendation.policy_id,
            "event_type": "nba_outbound_call",
            "timestamp": self.sim_env.current_datetime,
            "details": {
                "action_code": recommendation.action_code,
                "action_name": recommendation.action_name,
                "recommendation_id": recommendation.recommendation_id,
                "action_category": recommendation.action_category.value,
            },
        })
        self._stats["crm_events_emitted"] += 1

    def _emit_communication_event(
        self, recommendation: NBAActionWithRecommendation
    ) -> None:
        """
        Emit event to Communication queue for Email/SMS/InApp actions.

        CommunicationProcess will consume this and send a message.
        """
        if not self.shared_state:
            return

        self.shared_state.add_communication_event({
            "member_id": recommendation.member_id,
            "policy_id": recommendation.policy_id,
            "event_type": "nba_communication",
            "timestamp": self.sim_env.current_datetime,
            "channel": recommendation.channel.value,
            "details": {
                "action_code": recommendation.action_code,
                "action_name": recommendation.action_name,
                "recommendation_id": recommendation.recommendation_id,
                "action_category": recommendation.action_category.value,
            },
        })
        self._stats["communication_events_emitted"] += 1

    def _sample_immediate_response(
        self, recommendation: NBAActionWithRecommendation
    ) -> ImmediateResponse:
        """
        Sample an immediate response based on channel effectiveness.

        Uses channel effectiveness from config to determine engagement.
        """
        channel = recommendation.channel.value
        effectiveness = self.nba_config.response.channel_effectiveness.get(
            channel, 0.15
        )

        # Sample engagement
        engaged = self.rng.random() < effectiveness

        if not engaged:
            # Map to appropriate "no engagement" response by channel
            if channel == "Phone":
                return ImmediateResponse.NO_ANSWER
            elif channel == "Email":
                return ImmediateResponse.IGNORED
            else:
                return ImmediateResponse.IGNORED

        # Engaged - determine response level
        if channel == "Phone":
            return ImmediateResponse.ANSWERED
        elif channel == "Email":
            # 60% opened only, 40% clicked
            if self.rng.random() < 0.4:
                return ImmediateResponse.CLICKED
            return ImmediateResponse.OPENED
        elif channel == "SMS":
            # SMS: delivered vs clicked
            if self.rng.random() < 0.3:
                return ImmediateResponse.CLICKED
            return ImmediateResponse.DELIVERED
        elif channel == "InApp":
            # InApp: high click rate if engaged
            if self.rng.random() < 0.6:
                return ImmediateResponse.CLICKED
            return ImmediateResponse.OPENED
        else:
            return ImmediateResponse.DELIVERED

    def _record_execution(
        self,
        recommendation: NBAActionWithRecommendation,
        execution_id: UUID,
        immediate_response: ImmediateResponse,
    ) -> None:
        """
        Record the execution in the database.
        """
        execution = NBAExecutionCreate(
            execution_id=execution_id,
            recommendation_id=recommendation.recommendation_id,
            action_id=recommendation.action_id,
            member_id=recommendation.member_id,
            policy_id=recommendation.policy_id,
            executed_at=self.sim_env.current_datetime,
            execution_channel=recommendation.channel,
            execution_method=ExecutionMethod.AUTOMATED,
            immediate_response=immediate_response,
            response_at=self.sim_env.current_datetime if immediate_response else None,
            worker_id=self.worker_id,
            simulation_date=self.sim_env.current_date,
        )

        self.batch_writer.add("nba.nba_action_execution", execution.model_dump_db())
        self._stats["executions_recorded"] += 1

    def _update_recommendation_status(
        self,
        recommendation_id: UUID,
        status: RecommendationStatus,
        suppression_reason: str | None = None,
    ) -> None:
        """
        Update the recommendation status in the database.
        """
        updates = {
            "status": status.value,
            "modified_at": self.sim_env.current_datetime,
        }
        if suppression_reason:
            updates["suppression_reason"] = suppression_reason

        self.batch_writer.update_record(
            table_name="nba_action_recommendation",
            key_field="recommendation_id",
            key_value=recommendation_id,
            updates=updates,
        )

    def _expire_old_recommendations(self, current_date: date) -> None:
        """
        Mark recommendations past their valid_until as expired.

        This handles recommendations that were never processed during
        their validity window.
        """
        if not self.shared_state:
            return

        # Check all recommendations in queue
        remaining: list[NBAActionWithRecommendation] = []
        recommendations = self.shared_state.peek_nba_recommendations()

        for rec in recommendations:
            if rec.valid_until < current_date:
                self._update_recommendation_status(
                    rec.recommendation_id,
                    RecommendationStatus.EXPIRED,
                )
                self._stats["recommendations_expired"] += 1
            else:
                remaining.append(rec)

        # Note: peek doesn't remove, and we process in _process_daily_recommendations
        # So we don't need to re-add here

    def _expire_old_effects(self) -> None:
        """
        Remove expired behavioral effects from SharedState.
        """
        if not self.shared_state:
            return

        removed = self.shared_state.expire_nba_effects(
            current_datetime=self.sim_env.current_datetime
        )
        self._stats["effects_expired"] += removed

    def _log_progress(self) -> None:
        """Log process progress."""
        logger.info(
            "nba_process_progress",
            worker_id=self.worker_id,
            sim_date=self.sim_env.current_date.isoformat(),
            **self._stats,
        )

    def get_stats(self) -> dict[str, int]:
        """Get process statistics."""
        return self._stats.copy()
