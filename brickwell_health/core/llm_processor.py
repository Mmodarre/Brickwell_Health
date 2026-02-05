"""
LLM Survey Processor for Brickwell Health Simulator.

Processes pending NPS and CSAT surveys using Databricks ai_query
to generate realistic survey responses based on member context.

This module is integrated into the simulation pipeline and runs
automatically after simulation completes (if enabled in config).

Architecture:
- PostgreSQL: Stores pending surveys and final results (via psycopg)
- Databricks SQL: Executes ai_query for LLM processing (via databricks-sql-connector)
"""

import json
from datetime import datetime
from typing import Any, Optional
from uuid import UUID, uuid4

import psycopg
import structlog
from psycopg.rows import dict_row

from databricks import sql as databricks_sql

from brickwell_health.config.models import SimulationConfig
from brickwell_health.domain.survey import (
    NPSSurveyLLMResponse,
    CSATSurveyLLMResponse,
)

logger = structlog.get_logger()


# =============================================================================
# DEFAULT PROMPT TEMPLATES
# =============================================================================

DEFAULT_NPS_PROMPT = """Generate a realistic NPS survey response as JSON for an Australian private health insurance member.

=== MEMBER CONTEXT ===
{member_context}

{prior_surveys_section}

=== ANALYSIS GUIDELINES ===
Analyze the context to determine sentiment. Key factors to consider:

1. CURRENT TRIGGER EVENT (highest impact)
   - Claim rejection → Strong negative (-3 to -5 NPS impact)
   - Claim paid quickly → Positive (+1 to +2)
   - High gap amount → Moderate negative
   - Fast processing → Slight positive

2. CLAIMS HISTORY PATTERNS
   - High rejection rate (rejected/total) → Compounding frustration
   - Multiple rejection reasons → Confusion about coverage
   - Benefit limits hit → Value concerns
   - Low average processing days → Positive efficiency signal

3. FINANCIAL STRESS SIGNALS
   - Failed payments → Financial pressure, may blame insurer
   - Currently in arrears → High stress
   - High LHC loading → "Penalty" perception, lower value
   - Recent premium increase > 5% → Cost concerns

4. SERVICE EXPERIENCE
   - Low FCR rate (< 50%) → Frustration with resolution
   - High average wait time (> 10 min) → Service complaints
   - Unresolved issues > 0 → Ongoing frustration

5. DIGITAL SIGNALS (intent indicators)
   - viewed_cancel_page = true → HIGH churn risk, likely Detractor
   - High engagement but poor claims → "Trying to leave" signal

6. COMPLAINT HISTORY
   - Any open complaints → Strong negative
   - PHIO escalations → Severe dissatisfaction

7. PRIOR SURVEYS (if provided)
   - Trajectory: improving, stable, or declining?
   - Prior Detractor still having issues → likely still Detractor
   - Prior Detractor with resolved issues → may improve

=== SCORING CALIBRATION ===
Australian PHI NPS benchmarks:
- Industry average: +5 to +15 (low compared to other industries)
- Top performers: +25 to +35
- After claim rejection: typically 0-4 (Detractor)
- After smooth claim payment: typically 6-8 (Passive to Promoter)

Driver scores should correlate with NPS (within ±3 points typically):
- Claims Processing: Based on processing days, rejection rate
- Customer Service: Based on FCR rate, wait times, unresolved issues
- Value for Money: Based on benefit ratio, LHC, premium increase
- Coverage Clarity: Based on rejection reasons, benefit limits hit
- Digital Experience: Based on online claims success, engagement level

=== OUTPUT FORMAT ===
Respond with ONLY valid JSON (no markdown, no explanation):
{{
    "nps_score": <0-10>,
    "driver_claims_processing": <0-10>,
    "driver_customer_service": <0-10>,
    "driver_value_for_money": <0-10>,
    "driver_coverage_clarity": <0-10>,
    "driver_digital_experience": <0-10>,
    "feedback_text": "<1-3 sentences in Australian English, reference specific issues from context>",
    "feedback_improvement": "<specific suggestion based on their experience, or null>",
    "sentiment_score": <-1.0 to 1.0>,
    "sentiment_label": "<Positive|Neutral|Negative>",
    "feedback_themes": ["<theme1>", "<theme2>"],
    "follow_up_consent": <true|false>
}}"""

DEFAULT_CSAT_PROMPT = """Generate a realistic CSAT survey response as JSON for an Australian private health insurance member.

=== INTERACTION CONTEXT ===
{interaction_context}

=== SCORING GUIDELINES ===
- CSAT scores 1-5 (Very Dissatisfied to Very Satisfied)
- Effort scores 1-5 (Very High Effort to Very Low Effort)
- First contact resolution strongly influences satisfaction
- Long wait times reduce scores significantly

=== OUTPUT FORMAT ===
Respond with ONLY valid JSON (no markdown, no explanation):
{{
    "csat_score": <1-5>,
    "effort_score": <1-5>,
    "recommend_agent": <true|false>,
    "feedback_text": "<1-2 sentences in Australian English>",
    "sentiment_label": "<Positive|Neutral|Negative>"
}}"""


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def clean_json_response(response_text: str) -> str:
    """Strip markdown code block wrapper if present."""
    cleaned = response_text.strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    return cleaned.strip()


def get_nps_category(score: int) -> str:
    """Get NPS category from score."""
    if score >= 9:
        return "Promoter"
    elif score >= 7:
        return "Passive"
    else:
        return "Detractor"


def get_csat_label(score: int) -> str:
    """Get CSAT label from score."""
    labels = {
        1: "VeryDissatisfied",
        2: "Dissatisfied",
        3: "Neutral",
        4: "Satisfied",
        5: "VerySatisfied",
    }
    return labels.get(score, "Neutral")


# =============================================================================
# LLM SURVEY PROCESSOR
# =============================================================================


class LLMSurveyProcessor:
    """
    Processes pending surveys using Databricks ai_query.

    Uses batch SQL queries with CTE pattern for efficiency:
    - Single network round-trip per batch
    - Databricks parallelizes LLM calls internally
    - Configurable batch size (default: 50)

    Supports longitudinal processing:
    - Surveys are processed in member/chronological order
    - Prior survey responses are included in context
    """

    def __init__(self, config: SimulationConfig, dry_run: bool = False):
        """
        Initialize the processor.

        Args:
            config: SimulationConfig with LLM and database settings
            dry_run: If True, don't write results to database
        """
        self.config = config
        self.llm_config = config.llm
        self.db_config = config.database
        self.batch_size = config.llm.batch_size
        self.dry_run = dry_run

        # Extract Databricks settings
        self.databricks_host = (
            config.llm.databricks.host.replace("https://", "").rstrip("/")
        )
        self.databricks_token = config.llm.databricks.token
        self.databricks_http_path = config.llm.databricks.http_path
        self.llm_model = config.llm.model

        # Get prompt templates from config or use defaults
        self.nps_prompt_template = config.llm.prompts.get("nps_survey", DEFAULT_NPS_PROMPT)
        self.csat_prompt_template = config.llm.prompts.get("csat_survey", DEFAULT_CSAT_PROMPT)

        # Statistics
        self._stats: dict[str, int] = {
            "nps_processed": 0,
            "nps_responded": 0,
            "csat_processed": 0,
            "csat_responded": 0,
            "llm_calls": 0,
            "errors": 0,
        }

    def _get_pg_connection(self) -> psycopg.Connection:
        """Create PostgreSQL connection."""
        return psycopg.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            dbname=self.db_config.database,
            user=self.db_config.username,
            password=self.db_config.password,
            row_factory=dict_row,
        )

    def _get_databricks_connection(self) -> Any:
        """Create Databricks SQL connection."""
        return databricks_sql.connect(
            server_hostname=self.databricks_host,
            http_path=self.databricks_http_path,
            access_token=self.databricks_token,
        )

    def process_all(self) -> dict[str, int]:
        """
        Process all pending surveys.

        NPS surveys are processed in member/chronological order to support
        longitudinal context (prior surveys influence later surveys).

        Returns:
            Statistics dictionary with counts
        """
        logger.info(
            "llm_survey_processing_starting",
            databricks_host=self.databricks_host,
            llm_model=self.llm_model,
            batch_size=self.batch_size,
            dry_run=self.dry_run,
        )

        # Process NPS surveys (with prior survey context)
        self._process_nps_surveys()

        # Process CSAT surveys (no prior context needed)
        self._process_csat_surveys()

        logger.info(
            "llm_survey_processing_completed",
            nps_processed=self._stats["nps_processed"],
            nps_responded=self._stats["nps_responded"],
            csat_processed=self._stats["csat_processed"],
            csat_responded=self._stats["csat_responded"],
            llm_calls=self._stats["llm_calls"],
            errors=self._stats["errors"],
        )

        return self._stats

    def _process_nps_surveys(self) -> None:
        """
        Process pending NPS surveys with prior survey context.

        Surveys are processed in member/chronological order so that
        prior survey responses can be included in the context for
        subsequent surveys.
        """
        logger.info("processing_nps_surveys")

        with self._get_pg_connection() as pg_conn:
            # Get pending NPS surveys ordered by member and processing_order
            with pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM nps_survey_pending
                    WHERE processing_status = 'pending'
                      AND will_respond = TRUE
                    ORDER BY member_id, processing_order NULLS LAST, sent_datetime ASC
                """)
                pending_surveys = cur.fetchall()

            if not pending_surveys:
                logger.info("no_pending_nps_surveys")
                return

            logger.info("found_pending_nps_surveys", count=len(pending_surveys))

            # Process in batches, but ensure we don't split a member across batches
            # when their surveys depend on each other
            current_batch: list[dict] = []
            current_member_id: Optional[UUID] = None

            for survey in pending_surveys:
                survey_member_id = survey["member_id"]

                # If we've accumulated a batch and this is a new member, process the batch
                if (
                    len(current_batch) >= self.batch_size
                    and survey_member_id != current_member_id
                ):
                    self._process_nps_batch(pg_conn, current_batch)
                    current_batch = []

                current_batch.append(survey)
                current_member_id = survey_member_id

            # Process remaining surveys
            if current_batch:
                self._process_nps_batch(pg_conn, current_batch)

    def _process_nps_batch(self, pg_conn: psycopg.Connection, batch: list[dict]) -> None:
        """
        Process a batch of NPS surveys via Databricks ai_query.

        For each survey, we:
        1. Query prior NPS surveys for the member
        2. Build prior_surveys_context
        3. Build prompt with full context
        4. Execute batch ai_query
        5. Save results and update prior context for next surveys
        """
        logger.info("processing_nps_batch", batch_size=len(batch))

        # Build prompts for each survey
        prompts_data: list[dict] = []
        for survey in batch:
            context = survey.get("llm_context", {})
            if isinstance(context, str):
                context = json.loads(context)

            # Get prior surveys context for this member
            prior_context = self._get_prior_surveys_context(
                pg_conn,
                survey["member_id"],
                survey["sent_datetime"],
            )

            # Build prompt
            member_context = json.dumps(context, indent=2, default=str)
            prior_section = self._build_prior_surveys_section(prior_context)

            prompt = self.nps_prompt_template.format(
                member_context=member_context,
                prior_surveys_section=prior_section,
            )

            prompts_data.append({
                "pending_id": str(survey["pending_id"]),
                "survey": survey,
                "prompt": prompt,
            })

        if self.dry_run:
            logger.info("dry_run_skip_nps_batch", count=len(prompts_data))
            return

        # Execute batch ai_query via Databricks
        try:
            results = self._execute_batch_ai_query(prompts_data)
            self._stats["llm_calls"] += 1

            # Process results
            for pending_id, response_text in results.items():
                survey_data = next(
                    (p["survey"] for p in prompts_data if p["pending_id"] == pending_id),
                    None,
                )
                if survey_data:
                    self._save_nps_result(pg_conn, survey_data, response_text)
                    self._stats["nps_processed"] += 1
                    self._stats["nps_responded"] += 1

        except Exception as e:
            logger.error("nps_batch_processing_failed", error=str(e), exc_info=True)
            self._stats["errors"] += len(batch)

    def _get_prior_surveys_context(
        self,
        pg_conn: psycopg.Connection,
        member_id: UUID,
        before_datetime: datetime,
    ) -> Optional[dict]:
        """
        Get prior NPS survey responses for longitudinal context.

        Args:
            pg_conn: PostgreSQL connection
            member_id: Member UUID
            before_datetime: Only include surveys sent before this datetime

        Returns:
            Prior surveys context dict or None if no prior surveys
        """
        max_prior = self.llm_config.max_prior_nps_surveys

        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    survey_id,
                    sent_date,
                    nps_score,
                    nps_category,
                    trigger_event,
                    feedback_text
                FROM nps_survey
                WHERE member_id = %s
                  AND sent_date < %s
                ORDER BY sent_date DESC
                LIMIT %s
                """,
                (str(member_id), before_datetime, max_prior),
            )
            prior_surveys = cur.fetchall()

        if not prior_surveys:
            return None

        # Calculate trajectory
        scores = [s["nps_score"] for s in prior_surveys if s["nps_score"] is not None]
        trajectory = "stable"
        if len(scores) >= 2:
            # Compare most recent to oldest
            if scores[0] > scores[-1] + 1:
                trajectory = "declining"
            elif scores[0] < scores[-1] - 1:
                trajectory = "improving"

        return {
            "prior_surveys": [
                {
                    "survey_date": s["sent_date"].isoformat() if s["sent_date"] else None,
                    "nps_score": s["nps_score"],
                    "nps_category": s["nps_category"],
                    "trigger_event": s["trigger_event"],
                    "feedback_summary": (
                        s["feedback_text"][:200] if s["feedback_text"] else None
                    ),
                }
                for s in prior_surveys
            ],
            "trajectory": trajectory,
            "average_prior_nps": sum(scores) / len(scores) if scores else None,
            "survey_count": len(prior_surveys),
        }

    def _build_prior_surveys_section(self, prior_context: Optional[dict]) -> str:
        """Build the prior surveys section for the prompt."""
        if not prior_context or not prior_context.get("prior_surveys"):
            return ""

        section = "=== PRIOR NPS SURVEYS ===\n"
        section += f"Survey History: {prior_context['survey_count']} prior surveys\n"
        section += f"NPS Trajectory: {prior_context['trajectory']}\n"

        avg_nps = prior_context.get("average_prior_nps")
        if avg_nps is not None:
            section += f"Average Prior NPS: {avg_nps:.1f}\n"

        section += "\n"

        for i, survey in enumerate(prior_context["prior_surveys"], 1):
            section += f"Survey {i} ({survey['survey_date']}):\n"
            section += f"  - NPS: {survey['nps_score']} ({survey['nps_category']})\n"
            section += f"  - Trigger: {survey['trigger_event']}\n"
            if survey.get("feedback_summary"):
                section += f'  - Feedback: "{survey["feedback_summary"]}"\n'
            section += "\n"

        return section

    def _process_csat_surveys(self) -> None:
        """Process pending CSAT surveys in batches."""
        logger.info("processing_csat_surveys")

        with self._get_pg_connection() as pg_conn:
            # Get pending CSAT surveys
            with pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM csat_survey_pending
                    WHERE processing_status = 'pending'
                      AND will_respond = TRUE
                    ORDER BY sent_datetime ASC
                """)
                pending_surveys = cur.fetchall()

            if not pending_surveys:
                logger.info("no_pending_csat_surveys")
                return

            logger.info("found_pending_csat_surveys", count=len(pending_surveys))

            # Process in batches
            for i in range(0, len(pending_surveys), self.batch_size):
                batch = pending_surveys[i : i + self.batch_size]
                self._process_csat_batch(pg_conn, batch)

    def _process_csat_batch(self, pg_conn: psycopg.Connection, batch: list[dict]) -> None:
        """Process a batch of CSAT surveys via Databricks ai_query."""
        logger.info("processing_csat_batch", batch_size=len(batch))

        # Build prompts for each survey
        prompts_data: list[dict] = []
        for survey in batch:
            context = survey.get("llm_context", {})
            if isinstance(context, str):
                context = json.loads(context)

            interaction_context = json.dumps(context, indent=2, default=str)
            prompt = self.csat_prompt_template.format(interaction_context=interaction_context)

            prompts_data.append({
                "pending_id": str(survey["pending_id"]),
                "survey": survey,
                "prompt": prompt,
            })

        if self.dry_run:
            logger.info("dry_run_skip_csat_batch", count=len(prompts_data))
            return

        # Execute batch ai_query via Databricks
        try:
            results = self._execute_batch_ai_query(prompts_data)
            self._stats["llm_calls"] += 1

            # Process results
            for pending_id, response_text in results.items():
                survey_data = next(
                    (p["survey"] for p in prompts_data if p["pending_id"] == pending_id),
                    None,
                )
                if survey_data:
                    self._save_csat_result(pg_conn, survey_data, response_text)
                    self._stats["csat_processed"] += 1
                    self._stats["csat_responded"] += 1

        except Exception as e:
            logger.error("csat_batch_processing_failed", error=str(e), exc_info=True)
            self._stats["errors"] += len(batch)

    def _execute_batch_ai_query(self, prompts_data: list[dict]) -> dict[str, str]:
        """
        Execute batch ai_query via Databricks SQL using CTE pattern.

        This sends multiple prompts in a SINGLE SQL query for efficiency.
        Databricks parallelizes the LLM calls internally.

        Args:
            prompts_data: List of dicts with 'pending_id' and 'prompt'

        Returns:
            Dict mapping pending_id to LLM response text
        """
        # Build VALUES clause
        values_rows = []
        for item in prompts_data:
            pending_id = item["pending_id"]
            prompt = item["prompt"].replace("'", "''")  # Escape single quotes
            values_rows.append(f"('{pending_id}', '{prompt}')")

        values_clause = ",\n            ".join(values_rows)

        # Build batch query using CTE pattern
        query = f"""
        WITH survey_inputs AS (
            SELECT * FROM VALUES
            {values_clause}
            AS t(pending_id, prompt)
        )
        SELECT 
            pending_id,
            ai_query(
                '{self.llm_model}',
                prompt
            ) AS response
        FROM survey_inputs
        """

        logger.debug("executing_batch_ai_query", prompt_count=len(prompts_data))

        # Execute via Databricks
        conn = self._get_databricks_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()

            # Build results dict
            return {pending_id: response for pending_id, response in rows}

        finally:
            conn.close()

    def _save_nps_result(
        self, pg_conn: psycopg.Connection, survey: dict, response_text: str
    ) -> None:
        """Parse LLM response and save NPS survey result."""
        try:
            cleaned = clean_json_response(response_text)
            response_json = json.loads(cleaned)
            validated = NPSSurveyLLMResponse(**response_json)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(
                "invalid_nps_response",
                pending_id=str(survey["pending_id"]),
                error=str(e),
            )
            self._mark_error(pg_conn, "nps_survey_pending", survey["pending_id"], str(e))
            self._stats["errors"] += 1
            return

        # Create final survey record
        survey_id = uuid4()
        nps_category = get_nps_category(validated.nps_score)

        # Insert into nps_survey
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO nps_survey (
                    survey_id, survey_reference, member_id, policy_id,
                    survey_type, trigger_event, trigger_entity_id,
                    claim_id, interaction_id, sent_date, completed_date,
                    nps_score, nps_category, feedback_text, feedback_improvement,
                    driver_claims_processing, driver_customer_service,
                    driver_value_for_money, driver_coverage_clarity,
                    driver_digital_experience, sentiment_score, sentiment_label,
                    feedback_themes, survey_channel, response_time_minutes,
                    follow_up_consent, pending_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """,
                (
                    str(survey_id),
                    survey["survey_reference"],
                    str(survey["member_id"]),
                    str(survey["policy_id"]),
                    survey["survey_type"],
                    survey.get("trigger_event"),
                    str(survey["trigger_entity_id"]) if survey.get("trigger_entity_id") else None,
                    str(survey["claim_id"]) if survey.get("claim_id") else None,
                    str(survey["interaction_id"]) if survey.get("interaction_id") else None,
                    survey["sent_datetime"],
                    survey.get("completed_datetime"),
                    validated.nps_score,
                    nps_category,
                    validated.feedback_text,
                    validated.feedback_improvement,
                    validated.driver_claims_processing,
                    validated.driver_customer_service,
                    validated.driver_value_for_money,
                    validated.driver_coverage_clarity,
                    validated.driver_digital_experience,
                    validated.sentiment_score,
                    validated.sentiment_label,
                    ",".join(validated.feedback_themes) if validated.feedback_themes else None,
                    "Email",
                    survey.get("response_time_minutes"),
                    validated.follow_up_consent,
                    str(survey["pending_id"]),
                ),
            )

            # Update pending status
            cur.execute(
                """
                UPDATE nps_survey_pending
                SET processing_status = 'completed',
                    processed_at = %s,
                    final_survey_id = %s
                WHERE pending_id = %s
                """,
                (datetime.now(), str(survey_id), str(survey["pending_id"])),
            )

            pg_conn.commit()

    def _save_csat_result(
        self, pg_conn: psycopg.Connection, survey: dict, response_text: str
    ) -> None:
        """Parse LLM response and save CSAT survey result."""
        try:
            cleaned = clean_json_response(response_text)
            response_json = json.loads(cleaned)
            validated = CSATSurveyLLMResponse(**response_json)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(
                "invalid_csat_response",
                pending_id=str(survey["pending_id"]),
                error=str(e),
            )
            self._mark_error(pg_conn, "csat_survey_pending", survey["pending_id"], str(e))
            self._stats["errors"] += 1
            return

        # Create final survey record
        survey_id = uuid4()
        csat_label = get_csat_label(validated.csat_score)

        # Insert into csat_survey
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO csat_survey (
                    survey_id, survey_reference, member_id, policy_id,
                    survey_type, interaction_id, case_id,
                    sent_date, completed_date, csat_score, csat_label,
                    effort_score, recommend_agent, feedback_text,
                    sentiment_label, survey_channel, response_time_minutes,
                    pending_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    str(survey_id),
                    survey["survey_reference"],
                    str(survey["member_id"]),
                    str(survey["policy_id"]),
                    survey["survey_type"],
                    str(survey["interaction_id"]) if survey.get("interaction_id") else None,
                    str(survey["case_id"]) if survey.get("case_id") else None,
                    survey["sent_datetime"],
                    survey.get("completed_datetime"),
                    validated.csat_score,
                    csat_label,
                    validated.effort_score,
                    validated.recommend_agent,
                    validated.feedback_text,
                    validated.sentiment_label,
                    "Email",
                    survey.get("response_time_minutes"),
                    str(survey["pending_id"]),
                ),
            )

            # Update pending status
            cur.execute(
                """
                UPDATE csat_survey_pending
                SET processing_status = 'completed',
                    processed_at = %s,
                    final_survey_id = %s
                WHERE pending_id = %s
                """,
                (datetime.now(), str(survey_id), str(survey["pending_id"])),
            )

            pg_conn.commit()

    def _mark_error(
        self, pg_conn: psycopg.Connection, table: str, pending_id: UUID, error: str
    ) -> None:
        """Mark a pending survey as errored."""
        with pg_conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {table}
                SET processing_status = 'error',
                    error_message = %s,
                    retry_count = retry_count + 1
                WHERE pending_id = %s
                """,
                (error[:500], str(pending_id)),
            )
            pg_conn.commit()
