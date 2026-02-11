"""
Member lifecycle process for Brickwell Health Simulator.

Handles member demographic changes over time:
- Address changes (moves)
- Phone/email updates
- Name changes (marriage/divorce)
- Marital status changes
- Medicare card renewals
- Death processing
"""

from datetime import date, timedelta
from typing import Any, Generator, TYPE_CHECKING
from uuid import UUID

import structlog

from brickwell_health.core.processes.base import BaseProcess
from brickwell_health.domain.enums import MaritalStatus, MemberChangeType, MemberRole
from brickwell_health.domain.member import MemberUpdate
from brickwell_health.generators.member_generator import MemberGenerator

if TYPE_CHECKING:
    from brickwell_health.core.shared_state import SharedState


logger = structlog.get_logger()


class MemberLifecycleProcess(BaseProcess):
    """
    SimPy process for member lifecycle events.

    Handles demographic changes for members on active policies:
    - Address changes (moves within Australia, interstate moves)
    - Phone number updates
    - Email address updates
    - Name changes (marriage, divorce)
    - Marital status changes
    - Medicare card renewals (deterministic, based on expiry)
    - Death processing (age-weighted probability)

    Changes are tracked in the member_update table for audit trail.
    Significant events (death, address change) are queued for other
    processes to react to.
    """

    # Default death rates by age group (annual)
    DEFAULT_DEATH_RATES = {
        "18-30": 0.0005,
        "31-45": 0.001,
        "46-60": 0.003,
        "61-70": 0.008,
        "71-80": 0.02,
        "81+": 0.05,
    }

    def __init__(
        self,
        *args: Any,
        shared_state: "SharedState | None" = None,
        **kwargs: Any,
    ):
        """
        Initialize the member lifecycle process.

        Args:
            shared_state: Shared state for cross-process communication
        """
        super().__init__(*args, **kwargs)

        self.shared_state = shared_state

        # Initialize member generator for change data
        self.member_gen = MemberGenerator(
            self.rng,
            self.reference,
            self.id_generator,
            sim_env=self.sim_env,
        )

        # Load configuration
        self.lifecycle_config = self.config.member_lifecycle

    def run(self) -> Generator:
        """
        Main member lifecycle process loop.

        Runs daily, processing potential changes for each member.
        """
        logger.info(
            "member_lifecycle_process_started",
            worker_id=self.worker_id,
        )

        while True:
            # Skip processing during warmup's first week
            if self.sim_env.total_elapsed_days < 7:
                yield self.env.timeout(1.0)
                continue

            current_date = self.sim_env.current_date

            # Process each type of member change
            self._process_address_changes(current_date)
            self._process_phone_changes(current_date)
            self._process_email_changes(current_date)
            self._process_name_changes(current_date)
            self._process_marital_status_changes(current_date)
            self._process_preferred_name_updates(current_date)
            self._process_medicare_renewals(current_date)
            self._process_deaths(current_date)

            # Wait until next day
            yield self.env.timeout(1.0)

            # Log progress monthly
            if int(self.sim_env.now) % 30 == 0:
                self._log_progress()

    def _get_config_rate(self, key: str, default: float) -> float:
        """Get a rate from config with fallback."""
        return getattr(self.lifecycle_config, key, default)

    def _get_death_rate(self, age: int) -> float:
        """Get annual death rate for a given age."""
        death_rates = self.lifecycle_config.death_rates

        if age <= 30:
            return death_rates.get("18-30", 0.0005)
        elif age <= 45:
            return death_rates.get("31-45", 0.001)
        elif age <= 60:
            return death_rates.get("46-60", 0.003)
        elif age <= 70:
            return death_rates.get("61-70", 0.008)
        elif age <= 80:
            return death_rates.get("71-80", 0.02)
        else:
            return death_rates.get("81+", 0.05)

    def _process_address_changes(self, current_date: date) -> None:
        """Process potential address changes for all members."""
        if not self.shared_state:
            return

        annual_rate = self._get_config_rate("address_change_rate", 0.12)
        daily_rate = self.annual_rate_to_daily(annual_rate)
        interstate_rate = self._get_config_rate("interstate_move_rate", 0.15)

        for pm_id, data in list(self.shared_state.policy_members.items()):
            if self.rng.random() >= daily_rate:
                continue

            member = data.get("member")
            if not member or getattr(member, "deceased_flag", False):
                continue

            policy = data.get("policy")
            if not policy:
                continue

            # Generate new address
            new_address = self.member_gen.generate_new_address(
                current_state=member.state,
                interstate_move_rate=interstate_rate,
            )

            # Track previous values
            previous_values = {
                "address_line_1": member.address_line_1,
                "address_line_2": member.address_line_2,
                "suburb": member.suburb,
                "state": member.state,
                "postcode": member.postcode,
            }

            # Create member update record
            self._create_member_update(
                member_id=member.member_id,
                change_type=MemberChangeType.ADDRESS_CHANGE,
                change_date=current_date,
                previous_values=previous_values,
                new_values=new_address,
                reason="Member relocation",
            )

            # Update member record in database
            self._update_member_in_db(member.member_id, new_address, current_date)

            # Update cached member data
            self.shared_state.update_member_data(member.member_id, new_address)

            # Queue event for policy/billing processes
            self.shared_state.add_member_change_event(
                member_id=member.member_id,
                policy_id=policy.policy_id,
                change_type="ADDRESS_CHANGE",
                change_data={
                    "previous_state": previous_values["state"],
                    "new_state": new_address["state"],
                    "member_role": data.get("member_role"),
                    "date": current_date.isoformat(),
                },
            )

            self.increment_stat("address_changes")
            logger.debug(
                "member_address_changed",
                member_id=str(member.member_id),
                from_state=previous_values["state"],
                to_state=new_address["state"],
            )

    def _process_phone_changes(self, current_date: date) -> None:
        """Process potential phone number changes."""
        if not self.shared_state:
            return

        annual_rate = self._get_config_rate("phone_change_rate", 0.08)
        daily_rate = self.annual_rate_to_daily(annual_rate)

        for pm_id, data in list(self.shared_state.policy_members.items()):
            if self.rng.random() >= daily_rate:
                continue

            member = data.get("member")
            if not member or getattr(member, "deceased_flag", False):
                continue

            # Generate new phone
            new_phone = self.member_gen.generate_new_phone()
            previous_phone = member.mobile_phone

            # Create update record
            self._create_member_update(
                member_id=member.member_id,
                change_type=MemberChangeType.PHONE_CHANGE,
                change_date=current_date,
                previous_values={"mobile_phone": previous_phone},
                new_values={"mobile_phone": new_phone},
                reason="Phone number update",
            )

            # Update database
            self._update_member_in_db(
                member.member_id,
                {"mobile_phone": new_phone},
                current_date,
            )

            # Update cache
            self.shared_state.update_member_data(
                member.member_id,
                {"mobile_phone": new_phone},
            )

            self.increment_stat("phone_changes")

    def _process_email_changes(self, current_date: date) -> None:
        """Process potential email address changes."""
        if not self.shared_state:
            return

        annual_rate = self._get_config_rate("email_change_rate", 0.05)
        daily_rate = self.annual_rate_to_daily(annual_rate)

        for pm_id, data in list(self.shared_state.policy_members.items()):
            if self.rng.random() >= daily_rate:
                continue

            member = data.get("member")
            if not member or getattr(member, "deceased_flag", False):
                continue

            # Generate new email
            new_email = self.member_gen.generate_new_email(
                member.first_name,
                member.last_name,
            )
            previous_email = member.email

            # Create update record
            self._create_member_update(
                member_id=member.member_id,
                change_type=MemberChangeType.EMAIL_CHANGE,
                change_date=current_date,
                previous_values={"email": previous_email},
                new_values={"email": new_email},
                reason="Email address update",
            )

            # Update database
            self._update_member_in_db(
                member.member_id,
                {"email": new_email},
                current_date,
            )

            # Update cache
            self.shared_state.update_member_data(
                member.member_id,
                {"email": new_email},
            )

            self.increment_stat("email_changes")

    def _process_name_changes(self, current_date: date) -> None:
        """Process potential name changes (marriage/divorce)."""
        if not self.shared_state:
            return

        annual_rate = self._get_config_rate("name_change_rate", 0.015)
        daily_rate = self.annual_rate_to_daily(annual_rate)
        marital_trigger_rate = self._get_config_rate("name_change_triggers_marital", 0.8)

        for pm_id, data in list(self.shared_state.policy_members.items()):
            if self.rng.random() >= daily_rate:
                continue

            member = data.get("member")
            if not member or getattr(member, "deceased_flag", False):
                continue

            current_status = getattr(member, "marital_status", MaritalStatus.SINGLE)
            previous_name = member.last_name

            # Determine if this is marriage or divorce
            if current_status in [MaritalStatus.SINGLE, MaritalStatus.DIVORCED, MaritalStatus.WIDOWED]:
                # Marriage - take new name
                new_name = self.member_gen.generate_married_name(previous_name)
                new_title = "Mrs" if member.gender.value == "Female" else member.title
            else:
                # Divorce - may revert name
                new_name = self.member_gen.generate_divorce_name(previous_name)
                new_title = "Ms" if member.gender.value == "Female" else member.title

            if new_name == previous_name:
                continue  # No actual change

            updates = {"last_name": new_name}
            if new_title != member.title:
                updates["title"] = new_title

            # Create update record
            self._create_member_update(
                member_id=member.member_id,
                change_type=MemberChangeType.NAME_CHANGE,
                change_date=current_date,
                previous_values={"last_name": previous_name, "title": member.title},
                new_values=updates,
                reason="Name change (marriage/divorce)",
            )

            # Update database
            self._update_member_in_db(member.member_id, updates, current_date)

            # Update cache
            self.shared_state.update_member_data(member.member_id, updates)

            self.increment_stat("name_changes")

            # Potentially trigger marital status change
            if self.rng.random() < marital_trigger_rate:
                self._change_marital_status(member, data, current_date)

    def _process_marital_status_changes(self, current_date: date) -> None:
        """Process standalone marital status changes."""
        if not self.shared_state:
            return

        annual_rate = self._get_config_rate("marital_status_change_rate", 0.02)
        daily_rate = self.annual_rate_to_daily(annual_rate)

        for pm_id, data in list(self.shared_state.policy_members.items()):
            if self.rng.random() >= daily_rate:
                continue

            member = data.get("member")
            if not member or getattr(member, "deceased_flag", False):
                continue

            self._change_marital_status(member, data, current_date)

    def _change_marital_status(self, member, member_data: dict, current_date: date) -> None:
        """Change a member's marital status."""
        current_status = getattr(member, "marital_status", MaritalStatus.SINGLE)
        new_status = self.member_gen.generate_new_marital_status(current_status)

        if new_status == current_status:
            return

        # Create update record
        self._create_member_update(
            member_id=member.member_id,
            change_type=MemberChangeType.MARITAL_STATUS_CHANGE,
            change_date=current_date,
            previous_values={"marital_status": current_status.value},
            new_values={"marital_status": new_status.value},
            reason="Marital status change",
        )

        # Update database
        self._update_member_in_db(
            member.member_id,
            {"marital_status": new_status.value},
            current_date,
        )

        # Update cache
        self.shared_state.update_member_data(
            member.member_id,
            {"marital_status": new_status},
        )

        self.increment_stat("marital_status_changes")
        logger.debug(
            "member_marital_status_changed",
            member_id=str(member.member_id),
            from_status=current_status.value,
            to_status=new_status.value,
        )

    def _process_preferred_name_updates(self, current_date: date) -> None:
        """Process preferred name updates."""
        if not self.shared_state:
            return

        annual_rate = self._get_config_rate("preferred_name_rate", 0.01)
        daily_rate = self.annual_rate_to_daily(annual_rate)

        for pm_id, data in list(self.shared_state.policy_members.items()):
            if self.rng.random() >= daily_rate:
                continue

            member = data.get("member")
            if not member or getattr(member, "deceased_flag", False):
                continue

            # Only update if no preferred name exists
            if member.preferred_name:
                continue

            new_preferred = self.member_gen.generate_preferred_name(member.first_name)

            # Create update record
            self._create_member_update(
                member_id=member.member_id,
                change_type=MemberChangeType.PREFERRED_NAME_UPDATE,
                change_date=current_date,
                previous_values={"preferred_name": None},
                new_values={"preferred_name": new_preferred},
                reason="Preferred name added",
            )

            # Update database
            self._update_member_in_db(
                member.member_id,
                {"preferred_name": new_preferred},
                current_date,
            )

            # Update cache
            self.shared_state.update_member_data(
                member.member_id,
                {"preferred_name": new_preferred},
            )

            self.increment_stat("preferred_name_updates")

    def _process_medicare_renewals(self, current_date: date) -> None:
        """Process Medicare card renewals (deterministic based on expiry)."""
        if not self.shared_state:
            return

        advance_days = self._get_config_rate("medicare_renewal_advance_days", 30)
        renewal_threshold = current_date + timedelta(days=int(advance_days))

        for pm_id, data in list(self.shared_state.policy_members.items()):
            member = data.get("member")
            if not member or getattr(member, "deceased_flag", False):
                continue

            expiry = member.medicare_expiry_date
            if not expiry or expiry > renewal_threshold:
                continue

            # Renew Medicare card
            new_expiry = self.member_gen.generate_medicare_renewal(expiry)

            # Create update record
            self._create_member_update(
                member_id=member.member_id,
                change_type=MemberChangeType.MEDICARE_RENEWAL,
                change_date=current_date,
                previous_values={"medicare_expiry_date": expiry.isoformat()},
                new_values={"medicare_expiry_date": new_expiry.isoformat()},
                reason="Medicare card renewal",
            )

            # Update database
            self._update_member_in_db(
                member.member_id,
                {"medicare_expiry_date": new_expiry},
                current_date,
            )

            # Update cache
            self.shared_state.update_member_data(
                member.member_id,
                {"medicare_expiry_date": new_expiry},
            )

            self.increment_stat("medicare_renewals")

    def _process_deaths(self, current_date: date) -> None:
        """Process member deaths with age-weighted probability."""
        if not self.shared_state:
            return

        for pm_id, data in list(self.shared_state.policy_members.items()):
            member = data.get("member")
            if not member:
                continue

            # Skip already deceased
            if getattr(member, "deceased_flag", False):
                continue

            age = data.get("age", 50)
            death_rate = self._get_death_rate(age)
            daily_rate = self.annual_rate_to_daily(death_rate)

            if self.rng.random() >= daily_rate:
                continue

            policy = data.get("policy")
            if not policy:
                continue

            member_role = data.get("member_role", MemberRole.PRIMARY.value)

            # Create member update record
            self._create_member_update(
                member_id=member.member_id,
                change_type=MemberChangeType.DEATH,
                change_date=current_date,
                previous_values={"deceased_flag": False},
                new_values={"deceased_flag": True, "deceased_date": current_date.isoformat()},
                reason="Member deceased",
            )

            # Update member record in database
            self.batch_writer.add_raw_sql(
                "member_death_update",
                f"""
                UPDATE member SET
                    deceased_flag = true,
                    deceased_date = '{current_date.isoformat()}',
                    modified_at = '{self.sim_env.current_datetime.isoformat()}',
                    modified_by = 'SIMULATION'
                WHERE member_id = '{member.member_id}'
                """,
            )

            # Queue event for PolicyLifecycleProcess
            self.shared_state.add_member_change_event(
                member_id=member.member_id,
                policy_id=policy.policy_id,
                change_type="DEATH",
                change_data={
                    "member_role": member_role,
                    "date": current_date.isoformat(),
                    "age": age,
                },
            )

            # Update cached member data (mark as deceased in cache via shared state)
            # Note: MemberCreate doesn't have deceased_flag, so we just mark via shared state
            self.shared_state.update_member_data(
                member.member_id,
                {"deceased_flag": True, "deceased_date": current_date},
            )

            self.increment_stat("deaths")
            logger.info(
                "member_deceased",
                member_id=str(member.member_id),
                policy_id=str(policy.policy_id),
                member_role=member_role,
                age=age,
            )

    def _create_member_update(
        self,
        member_id: UUID,
        change_type: MemberChangeType,
        change_date: date,
        previous_values: dict[str, Any],
        new_values: dict[str, Any],
        reason: str | None = None,
    ) -> None:
        """Create and write a member update record."""
        update = MemberUpdate(
            member_update_id=self.id_generator.generate_uuid(),
            member_id=member_id,
            change_type=change_type,
            change_date=change_date,
            previous_values=previous_values,
            new_values=new_values,
            reason=reason,
            triggered_by="SIMULATION",
            created_at=self.sim_env.current_datetime,
            created_by="SIMULATION",
        )
        self.batch_writer.add("member_lifecycle.member_update", update.model_dump_db())

    def _update_member_in_db(
        self,
        member_id: UUID,
        updates: dict[str, Any],
        current_date: date,
    ) -> None:
        """Update member record in database."""
        set_parts = []
        for field, value in updates.items():
            if value is None:
                set_parts.append(f"{field} = NULL")
            elif isinstance(value, str):
                # Escape single quotes
                escaped = value.replace("'", "''")
                set_parts.append(f"{field} = '{escaped}'")
            elif isinstance(value, date):
                set_parts.append(f"{field} = '{value.isoformat()}'")
            else:
                set_parts.append(f"{field} = '{value}'")

        set_clause = ", ".join(set_parts)

        sql = f"""
            UPDATE member SET
                {set_clause},
                modified_at = '{self.sim_env.current_datetime.isoformat()}',
                modified_by = 'SIMULATION'
            WHERE member_id = '{member_id}'
        """
        self.batch_writer.add_raw_sql("member_update", sql)

    def _log_progress(self) -> None:
        """Log member lifecycle process progress."""
        stats = self.get_stats()
        member_count = len(self.shared_state.policy_members) if self.shared_state else 0

        logger.info(
            "member_lifecycle_progress",
            worker_id=self.worker_id,
            sim_day=int(self.sim_env.now),
            sim_date=self.sim_env.current_date.isoformat(),
            tracked_members=member_count,
            address_changes=stats.get("address_changes", 0),
            phone_changes=stats.get("phone_changes", 0),
            email_changes=stats.get("email_changes", 0),
            name_changes=stats.get("name_changes", 0),
            marital_status_changes=stats.get("marital_status_changes", 0),
            preferred_name_updates=stats.get("preferred_name_updates", 0),
            medicare_renewals=stats.get("medicare_renewals", 0),
            deaths=stats.get("deaths", 0),
        )
