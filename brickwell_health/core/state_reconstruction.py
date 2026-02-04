"""
Database state reconstruction for checkpoint resume.

Provides SQL queries to reconstruct SharedState from the database
for incremental simulation runs.
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.engine import Engine

from brickwell_health.core.shared_state import SharedState
from brickwell_health.domain.enums import Gender, MaritalStatus, PolicyType
from brickwell_health.domain.member import MemberCreate


logger = structlog.get_logger()


def _extract_worker_from_number(number: str | None) -> int | None:
    """
    Extract worker ID from a policy/invoice number prefix.
    
    Format: PREFIX-WN-YYYY-NNNNNN where N is worker ID
    Example: POL-W0-2020-000001 -> worker 0
    
    Args:
        number: Policy or invoice number string
        
    Returns:
        Worker ID or None if couldn't parse
    """
    if not number or len(number) < 6:
        return None
    try:
        # Extract 'WN' part after first dash
        worker_part = number.split('-')[1]
        if worker_part.startswith('W'):
            return int(worker_part[1:])
    except (IndexError, ValueError):
        pass
    return None


def _is_owned_by_worker_number(policy_number: str | None, worker_id: int) -> bool:
    """
    Check if a policy/entity belongs to a worker based on its number prefix.
    
    This is used instead of UUID-based partitioning because the fresh simulation
    doesn't use partition-owned UUIDs, but does include worker ID in the number.
    
    Args:
        policy_number: Policy number string (e.g., POL-W0-2020-000001)
        worker_id: Worker ID to check ownership for
        
    Returns:
        True if the number indicates ownership by this worker
    """
    extracted = _extract_worker_from_number(policy_number)
    return extracted == worker_id


def reconstruct_shared_state_from_db(
    engine: Engine,
    checkpoint_date: date,
    worker_id: int,
    num_workers: int,
) -> SharedState:
    """
    Reconstruct SharedState from database for a specific worker.

    Creates a new SharedState instance and populates reconstructable
    fields by querying the database, filtering to only include entities
    owned by this worker's partition.

    Args:
        engine: SQLAlchemy engine
        checkpoint_date: Date to use for filtering active records
        worker_id: This worker's ID (0 to num_workers-1)
        num_workers: Total number of workers

    Returns:
        SharedState with reconstructable fields populated for this worker
    """
    shared_state = SharedState()

    with engine.connect() as conn:
        # Load reconstructable state (filtered by worker partition)
        shared_state.active_policies = load_active_policies(
            conn, checkpoint_date, worker_id, num_workers
        )
        shared_state.policy_members = load_policy_members(
            conn, checkpoint_date, worker_id, num_workers
        )
        shared_state.waiting_periods = load_waiting_periods(
            conn, checkpoint_date, worker_id, num_workers
        )
        shared_state.communication_preferences = load_communication_preferences(
            conn, worker_id, num_workers
        )
        shared_state.recent_interactions = load_recent_interactions(
            conn, checkpoint_date, worker_id, num_workers
        )
        # Note: pending_invoices base data loaded, retry state comes from checkpoint
        shared_state.pending_invoices = load_pending_invoices(
            conn, checkpoint_date, worker_id, num_workers
        )

    logger.info(
        "shared_state_reconstructed_from_db",
        worker_id=worker_id,
        active_policies=len(shared_state.active_policies),
        policy_members=len(shared_state.policy_members),
        waiting_periods=len(shared_state.waiting_periods),
        communication_preferences=len(shared_state.communication_preferences),
        recent_interactions=len(shared_state.recent_interactions),
        pending_invoices=len(shared_state.pending_invoices),
    )

    return shared_state


def load_active_policies(
    conn,
    checkpoint_date: date,
    worker_id: int,
    num_workers: int,
) -> dict[UUID, dict[str, Any]]:
    """
    Load active policies with members and coverages from database.

    Only loads policies owned by this worker's partition.

    Args:
        conn: Database connection
        checkpoint_date: Date for filtering
        worker_id: This worker's ID
        num_workers: Total number of workers

    Returns:
        Dict mapping policy_id to policy data dict (only for this worker's partition)
    """
    # Query active policies
    policy_query = text("""
        SELECT 
            p.policy_id,
            p.policy_number,
            p.policy_status,
            p.policy_type,
            p.effective_date,
            p.end_date,
            p.product_id,
            p.premium_amount,
            p.excess_amount,
            p.payment_frequency,
            p.state_of_residence,
            p.original_join_date,
            p.government_rebate_tier,
            p.distribution_channel
        FROM policy p
        WHERE p.policy_status = 'Active'
          AND p.effective_date <= :checkpoint_date
          AND (p.end_date IS NULL OR p.end_date > :checkpoint_date)
          AND NOT EXISTS (
              SELECT 1 FROM suspension s
              WHERE s.policy_id = p.policy_id
                AND s.status = 'Active'
                AND s.start_date <= :checkpoint_date
                AND (s.actual_end_date IS NULL OR s.actual_end_date > :checkpoint_date)
          )
    """)

    result = conn.execute(policy_query, {"checkpoint_date": checkpoint_date})
    policies: dict[UUID, dict[str, Any]] = {}

    for row in result:
        policy_id = UUID(str(row.policy_id))
        
        # Filter by worker based on policy_number prefix (not UUID partitioning)
        # This matches how the fresh simulation assigns workers to policies
        if not _is_owned_by_worker_number(row.policy_number, worker_id):
            continue
        
        # Create a policy object with attributes (compatible with billing process)
        # Convert policy_type string to enum for compatibility with all processes
        policy_type_enum = PolicyType(row.policy_type) if row.policy_type else None
        policy_obj = type("PolicyFromDB", (), {
            "policy_id": policy_id,
            "policy_number": row.policy_number,
            "policy_status": row.policy_status,
            "policy_type": policy_type_enum,
            "effective_date": row.effective_date,
            "end_date": row.end_date,
            "product_id": row.product_id,
            "premium_amount": row.premium_amount,
            "excess_amount": row.excess_amount,
            "payment_frequency": row.payment_frequency,
            "state_of_residence": row.state_of_residence,
            "original_join_date": row.original_join_date,
            "government_rebate_tier": row.government_rebate_tier,
            "distribution_channel": row.distribution_channel,
        })()
        
        policies[policy_id] = {
            "policy": policy_obj,  # Add policy object for billing process
            "policy_id": policy_id,
            "policy_number": row.policy_number,
            "status": row.policy_status,
            "policy_type": row.policy_type,
            "effective_date": row.effective_date,
            "end_date": row.end_date,
            "product_id": row.product_id,
            "premium_amount": row.premium_amount,
            "excess": row.excess_amount,
            "payment_frequency": row.payment_frequency,
            "state_of_residence": row.state_of_residence,
            "members": [],
            "coverages": [],
        }

    if not policies:
        return policies

    # Load members for these policies
    policy_ids = [str(pid) for pid in policies.keys()]
    member_query = text("""
        SELECT 
            pm.policy_member_id,
            pm.policy_id,
            pm.member_id,
            pm.member_role,
            pm.relationship_to_primary,
            m.member_number,
            m.title,
            m.first_name,
            m.middle_name,
            m.last_name,
            m.preferred_name,
            m.date_of_birth,
            m.gender,
            m.medicare_number,
            m.medicare_irn,
            m.medicare_expiry_date,
            m.address_line_1,
            m.address_line_2,
            m.suburb,
            m.state,
            m.postcode,
            m.country,
            m.email,
            m.mobile_phone,
            m.home_phone,
            m.australian_resident,
            m.tax_file_number_provided,
            m.lhc_applicable,
            m.marital_status
        FROM policy_member pm
        JOIN member m ON pm.member_id = m.member_id
        WHERE pm.policy_id = ANY(:policy_ids)
          AND pm.is_active = TRUE
          AND (pm.end_date IS NULL OR pm.end_date > :checkpoint_date)
          AND m.deceased_flag = FALSE
    """)

    result = conn.execute(
        member_query,
        {"policy_ids": policy_ids, "checkpoint_date": checkpoint_date},
    )

    for row in result:
        policy_id = UUID(str(row.policy_id))
        if policy_id in policies:
            # Convert to MemberCreate Pydantic model for consistency with fresh acquisition
            member = MemberCreate(
                member_id=UUID(str(row.member_id)),
                member_number=row.member_number,
                title=row.title,
                first_name=row.first_name,
                middle_name=row.middle_name,
                last_name=row.last_name,
                preferred_name=row.preferred_name,
                date_of_birth=row.date_of_birth,
                gender=Gender(row.gender) if isinstance(row.gender, str) else row.gender,
                medicare_number=row.medicare_number,
                medicare_irn=row.medicare_irn,
                medicare_expiry_date=row.medicare_expiry_date,
                address_line_1=row.address_line_1,
                address_line_2=row.address_line_2,
                suburb=row.suburb,
                state=row.state,
                postcode=row.postcode,
                country=row.country or "AUS",
                email=row.email,
                mobile_phone=row.mobile_phone,
                home_phone=row.home_phone,
                australian_resident=row.australian_resident if row.australian_resident is not None else True,
                tax_file_number_provided=row.tax_file_number_provided if row.tax_file_number_provided is not None else False,
                lhc_applicable=row.lhc_applicable if row.lhc_applicable is not None else False,
                marital_status=MaritalStatus(row.marital_status) if row.marital_status else MaritalStatus.SINGLE,
            )
            policies[policy_id]["members"].append(member)

    # Load coverages for these policies
    coverage_query = text("""
        SELECT 
            c.coverage_id,
            c.policy_id,
            c.coverage_type,
            c.tier,
            c.status,
            c.effective_date,
            c.end_date
        FROM coverage c
        WHERE c.policy_id = ANY(:policy_ids)
          AND c.status = 'Active'
          AND c.effective_date <= :checkpoint_date
          AND (c.end_date IS NULL OR c.end_date > :checkpoint_date)
    """)

    result = conn.execute(
        coverage_query,
        {"policy_ids": policy_ids, "checkpoint_date": checkpoint_date},
    )

    for row in result:
        policy_id = UUID(str(row.policy_id))
        if policy_id in policies:
            policies[policy_id]["coverages"].append(
                {
                    "coverage_id": UUID(str(row.coverage_id)),
                    "coverage_type": row.coverage_type,
                    "tier": row.tier,
                    "status": row.status,
                    "effective_date": row.effective_date,
                    "end_date": row.end_date,
                }
            )

    # Load LHC loading
    lhc_query = text("""
        SELECT policy_id, SUM(loading_percentage) as total_loading
        FROM lhc_loading
        WHERE policy_id = ANY(:policy_ids)
          AND is_loading_active = TRUE
        GROUP BY policy_id
    """)

    result = conn.execute(lhc_query, {"policy_ids": policy_ids})
    for row in result:
        policy_id = UUID(str(row.policy_id))
        if policy_id in policies:
            policies[policy_id]["lhc_loading"] = float(row.total_loading or 0)

    # Load age-based discount
    discount_query = text("""
        SELECT policy_id, SUM(current_discount_pct) as total_discount
        FROM age_based_discount
        WHERE policy_id = ANY(:policy_ids)
          AND is_active = TRUE
        GROUP BY policy_id
    """)

    result = conn.execute(discount_query, {"policy_ids": policy_ids})
    for row in result:
        policy_id = UUID(str(row.policy_id))
        if policy_id in policies:
            policies[policy_id]["age_discount"] = float(row.total_discount or 0)

    # Load PHI rebate
    rebate_query = text("""
        SELECT DISTINCT ON (policy_id)
            policy_id,
            rebate_percentage
        FROM phi_rebate_entitlement
        WHERE policy_id = ANY(:policy_ids)
          AND effective_date <= :checkpoint_date
          AND (end_date IS NULL OR end_date > :checkpoint_date)
        ORDER BY policy_id, effective_date DESC
    """)

    result = conn.execute(
        rebate_query,
        {"policy_ids": policy_ids, "checkpoint_date": checkpoint_date},
    )
    for row in result:
        policy_id = UUID(str(row.policy_id))
        if policy_id in policies:
            policies[policy_id]["rebate_pct"] = float(row.rebate_percentage or 0)

    # Load direct debit mandates for billing process
    mandate_query = text("""
        SELECT 
            d.direct_debit_id,
            d.policy_id,
            d.bank_account_id,
            d.debit_day,
            d.frequency,
            d.max_debit_amount,
            d.mandate_reference,
            d.authorization_date,
            d.status
        FROM direct_debit_mandate d
        WHERE d.policy_id = ANY(:policy_ids)
          AND d.status = 'Active'
    """)

    result = conn.execute(mandate_query, {"policy_ids": policy_ids})
    for row in result:
        policy_id = UUID(str(row.policy_id))
        if policy_id in policies:
            # Create mandate object compatible with billing process
            mandate_obj = type("MandateFromDB", (), {
                "direct_debit_id": UUID(str(row.direct_debit_id)),
                "policy_id": policy_id,
                "bank_account_id": UUID(str(row.bank_account_id)) if row.bank_account_id else None,
                "debit_day": row.debit_day,
                "frequency": row.frequency,
                "max_debit_amount": row.max_debit_amount,
                "mandate_reference": row.mandate_reference,
                "authorization_date": row.authorization_date,
                "status": row.status,
            })()
            policies[policy_id]["mandate"] = mandate_obj

    logger.debug("loaded_active_policies", count=len(policies))
    return policies


def load_policy_members(
    conn,
    checkpoint_date: date,
    worker_id: int,
    num_workers: int,
) -> dict[UUID, dict[str, Any]]:
    """
    Load policy members for claims processing.

    Only loads members for policies owned by this worker's partition.

    Args:
        conn: Database connection
        checkpoint_date: Date for filtering
        worker_id: This worker's ID
        num_workers: Total number of workers

    Returns:
        Dict mapping policy_member_id to member data dict (only for this worker's partition)
    """
    query = text("""
        SELECT 
            pm.policy_member_id,
            pm.policy_id,
            pm.member_id,
            pm.member_role,
            -- Member fields
            m.member_number,
            m.title,
            m.first_name,
            m.middle_name,
            m.last_name,
            m.preferred_name,
            m.date_of_birth,
            m.gender,
            m.medicare_number,
            m.medicare_irn,
            m.medicare_expiry_date,
            m.address_line_1,
            m.address_line_2,
            m.suburb,
            m.state,
            m.postcode,
            m.country,
            m.email,
            m.mobile_phone,
            m.home_phone,
            m.australian_resident,
            m.tax_file_number_provided,
            m.lhc_applicable,
            m.marital_status,
            -- Policy fields
            p.policy_number,
            p.policy_status,
            p.policy_type,
            p.product_id,
            p.effective_date as policy_effective_date,
            p.premium_amount,
            p.excess_amount,
            p.payment_frequency,
            p.state_of_residence,
            -- Hospital coverage
            hosp.coverage_id as hospital_coverage_id,
            hosp.tier as hospital_tier,
            hosp.excess_amount as hospital_excess_amount,
            -- Extras coverage
            ext.coverage_id as extras_coverage_id,
            ext.excess_amount as extras_excess_amount,
            -- Ambulance coverage
            amb.coverage_id as ambulance_coverage_id,
            amb.excess_amount as ambulance_excess_amount
        FROM policy_member pm
        JOIN member m ON pm.member_id = m.member_id
        JOIN policy p ON pm.policy_id = p.policy_id
        LEFT JOIN coverage hosp ON p.policy_id = hosp.policy_id 
            AND hosp.coverage_type = 'Hospital' 
            AND hosp.status = 'Active'
            AND (hosp.end_date IS NULL OR hosp.end_date > :checkpoint_date)
        LEFT JOIN coverage ext ON p.policy_id = ext.policy_id 
            AND ext.coverage_type = 'Extras' 
            AND ext.status = 'Active'
            AND (ext.end_date IS NULL OR ext.end_date > :checkpoint_date)
        LEFT JOIN coverage amb ON p.policy_id = amb.policy_id 
            AND amb.coverage_type = 'Ambulance' 
            AND amb.status = 'Active'
            AND (amb.end_date IS NULL OR amb.end_date > :checkpoint_date)
        WHERE pm.is_active = TRUE
          AND (pm.end_date IS NULL OR pm.end_date > :checkpoint_date)
          AND p.policy_status = 'Active'
          AND (p.end_date IS NULL OR p.end_date > :checkpoint_date)
          AND m.deceased_flag = FALSE
          AND NOT EXISTS (
              SELECT 1 FROM suspension s
              WHERE s.policy_id = p.policy_id
                AND s.status = 'Active'
                AND s.start_date <= :checkpoint_date
                AND (s.actual_end_date IS NULL OR s.actual_end_date > :checkpoint_date)
          )
    """)

    result = conn.execute(query, {"checkpoint_date": checkpoint_date})
    policy_members: dict[UUID, dict[str, Any]] = {}

    for row in result:
        policy_member_id = UUID(str(row.policy_member_id))
        policy_id = UUID(str(row.policy_id))
        member_id = UUID(str(row.member_id))

        # Filter by worker based on policy_number prefix
        if not _is_owned_by_worker_number(row.policy_number, worker_id):
            continue

        # Calculate age
        age = None
        if row.date_of_birth:
            age = (
                checkpoint_date.year
                - row.date_of_birth.year
                - (
                    (checkpoint_date.month, checkpoint_date.day)
                    < (row.date_of_birth.month, row.date_of_birth.day)
                )
            )

        # Convert marital_status string to enum
        marital_status_enum = MaritalStatus.SINGLE  # default
        if row.marital_status:
            try:
                # Handle both enum value strings and enum names
                marital_status_enum = MaritalStatus(row.marital_status)
            except ValueError:
                try:
                    marital_status_enum = MaritalStatus[row.marital_status.upper()]
                except (KeyError, AttributeError):
                    marital_status_enum = MaritalStatus.SINGLE

        # Convert gender string to enum
        gender_enum = Gender.UNKNOWN  # default
        if row.gender:
            try:
                gender_enum = Gender(row.gender)
            except ValueError:
                try:
                    gender_enum = Gender[row.gender.upper()]
                except (KeyError, AttributeError):
                    gender_enum = Gender.UNKNOWN

        # Create member object with attributes (compatible with all processes)
        member_obj = type("MemberFromDB", (), {
            "member_id": member_id,
            "member_number": row.member_number,
            "title": row.title,
            "first_name": row.first_name,
            "middle_name": row.middle_name,
            "last_name": row.last_name,
            "preferred_name": row.preferred_name,
            "date_of_birth": row.date_of_birth,
            "gender": gender_enum,
            "medicare_number": row.medicare_number,
            "medicare_irn": row.medicare_irn,
            "medicare_expiry_date": row.medicare_expiry_date,
            "address_line_1": row.address_line_1,
            "address_line_2": row.address_line_2,
            "suburb": row.suburb,
            "state": row.state,
            "postcode": row.postcode,
            "country": row.country,
            "email": row.email,
            "mobile_phone": row.mobile_phone,
            "home_phone": row.home_phone,
            "australian_resident": row.australian_resident,
            "tax_file_number_provided": row.tax_file_number_provided,
            "lhc_applicable": row.lhc_applicable,
            "marital_status": marital_status_enum,
        })()

        # Create policy object with attributes (compatible with all processes)
        # Convert policy_type string to enum for compatibility with all processes
        policy_type_enum = PolicyType(row.policy_type) if row.policy_type else None
        policy_obj = type("PolicyFromDB", (), {
            "policy_id": policy_id,
            "policy_number": row.policy_number,
            "policy_status": row.policy_status,
            "policy_type": policy_type_enum,
            "product_id": row.product_id,
            "effective_date": row.policy_effective_date,
            "premium_amount": row.premium_amount,
            "excess_amount": row.excess_amount,
            "payment_frequency": row.payment_frequency,
            "state_of_residence": row.state_of_residence,
        })()

        # Create coverage objects with attributes
        hospital_coverage_obj = None
        extras_coverage_obj = None
        ambulance_coverage_obj = None

        if row.hospital_coverage_id:
            hospital_coverage_obj = type("CoverageFromDB", (), {
                "coverage_id": UUID(str(row.hospital_coverage_id)),
                "tier": row.hospital_tier,
                "coverage_type": "Hospital",
                "excess_amount": row.hospital_excess_amount,
            })()

        if row.extras_coverage_id:
            extras_coverage_obj = type("CoverageFromDB", (), {
                "coverage_id": UUID(str(row.extras_coverage_id)),
                "coverage_type": "Extras",
                "excess_amount": row.extras_excess_amount,
            })()

        if row.ambulance_coverage_id:
            ambulance_coverage_obj = type("CoverageFromDB", (), {
                "coverage_id": UUID(str(row.ambulance_coverage_id)),
                "coverage_type": "Ambulance",
                "excess_amount": row.ambulance_excess_amount,
            })()

        policy_members[policy_member_id] = {
            "policy_member_id": policy_member_id,
            "policy_id": policy_id,
            "member_id": member_id,
            "member_role": row.member_role,
            "first_name": row.first_name,
            "last_name": row.last_name,
            "date_of_birth": row.date_of_birth,
            "gender": row.gender,
            "age": age,
            "email": row.email,
            "mobile_phone": row.mobile_phone,
            "state": row.state,
            "policy_number": row.policy_number,
            "policy_status": row.policy_status,
            "product_id": row.product_id,
            "policy_effective_date": row.policy_effective_date,
            # Coverage IDs for backward compatibility
            "hospital_coverage_id": UUID(str(row.hospital_coverage_id))
            if row.hospital_coverage_id
            else None,
            "hospital_tier": row.hospital_tier,
            "extras_coverage_id": UUID(str(row.extras_coverage_id))
            if row.extras_coverage_id
            else None,
            "ambulance_coverage_id": UUID(str(row.ambulance_coverage_id))
            if row.ambulance_coverage_id
            else None,
            # Objects for claims process compatibility
            "member": member_obj,
            "policy": policy_obj,
            "hospital_coverage": hospital_coverage_obj,
            "extras_coverage": extras_coverage_obj,
            "ambulance_coverage": ambulance_coverage_obj,
        }

    logger.debug("loaded_policy_members", count=len(policy_members))
    return policy_members


def load_waiting_periods(
    conn,
    checkpoint_date: date,
    worker_id: int,
    num_workers: int,
) -> dict[UUID, list[dict[str, Any]]]:
    """
    Load active waiting periods.

    Only loads waiting periods for policies owned by this worker's partition.

    Args:
        conn: Database connection
        checkpoint_date: Date for filtering
        worker_id: This worker's ID
        num_workers: Total number of workers

    Returns:
        Dict mapping policy_member_id to list of waiting period dicts (for this worker's partition)
    """
    query = text("""
        SELECT 
            wp.waiting_period_id,
            wp.policy_member_id,
            wp.coverage_id,
            wp.waiting_period_type,
            wp.start_date,
            wp.end_date,
            wp.status,
            wp.benefit_category_id,
            wp.clinical_category_id,
            wp.duration_months,
            c.coverage_type,
            pm.policy_id,
            p.policy_number
        FROM waiting_period wp
        JOIN coverage c ON wp.coverage_id = c.coverage_id
        JOIN policy_member pm ON wp.policy_member_id = pm.policy_member_id
        JOIN policy p ON pm.policy_id = p.policy_id
        WHERE wp.status = 'InProgress'
          AND wp.end_date >= :checkpoint_date
          AND pm.is_active = TRUE
          AND p.policy_status = 'Active'
          AND (p.end_date IS NULL OR p.end_date > :checkpoint_date)
        ORDER BY wp.policy_member_id, wp.start_date
    """)

    result = conn.execute(query, {"checkpoint_date": checkpoint_date})
    waiting_periods: dict[UUID, list[dict[str, Any]]] = {}

    for row in result:
        # Filter by worker based on policy_number prefix
        if not _is_owned_by_worker_number(row.policy_number, worker_id):
            continue
        
        policy_member_id = UUID(str(row.policy_member_id))

        if policy_member_id not in waiting_periods:
            waiting_periods[policy_member_id] = []

        waiting_periods[policy_member_id].append(
            {
                "waiting_period_id": UUID(str(row.waiting_period_id)),
                "coverage_id": UUID(str(row.coverage_id)),
                "coverage_type": row.coverage_type,
                "waiting_period_type": row.waiting_period_type,
                "start_date": row.start_date,
                "end_date": row.end_date,
                "status": row.status,
                "benefit_category_id": row.benefit_category_id,
                "clinical_category_id": row.clinical_category_id,
                "duration_months": row.duration_months,
            }
        )

    logger.debug(
        "loaded_waiting_periods",
        members_with_wp=len(waiting_periods),
        total_wp=sum(len(wps) for wps in waiting_periods.values()),
    )
    return waiting_periods


def load_communication_preferences(
    conn,
    worker_id: int,
    num_workers: int,
) -> dict[UUID, dict[str, bool]]:
    """
    Load communication preferences for active members.

    Only loads preferences for members with active policies owned by this worker.

    Args:
        conn: Database connection
        worker_id: This worker's ID
        num_workers: Total number of workers

    Returns:
        Dict mapping member_id to preferences dict (for this worker's partition)
    """
    query = text("""
        SELECT DISTINCT ON (cp.member_id, cp.preference_type, cp.channel)
            cp.member_id,
            cp.preference_type,
            cp.channel,
            cp.is_opted_in,
            pm.policy_id,
            p.policy_number
        FROM communication_preference cp
        JOIN member m ON cp.member_id = m.member_id
        JOIN policy_member pm ON cp.member_id = pm.member_id AND pm.is_active = TRUE
        JOIN policy p ON pm.policy_id = p.policy_id
        WHERE m.deceased_flag = FALSE
        ORDER BY cp.member_id, cp.preference_type, cp.channel, cp.created_at DESC
    """)

    result = conn.execute(query)
    preferences: dict[UUID, dict[str, bool]] = {}

    for row in result:
        # Filter by worker based on policy_number prefix
        if not _is_owned_by_worker_number(row.policy_number, worker_id):
            continue
            
        member_id = UUID(str(row.member_id))

        if member_id not in preferences:
            preferences[member_id] = {}

        # Key format matches SharedState: "transactional_email", "marketing_sms", etc.
        key = f"{row.preference_type.lower()}_{row.channel.lower()}"
        preferences[member_id][key] = row.is_opted_in

    logger.debug("loaded_communication_preferences", members=len(preferences))
    return preferences


def load_recent_interactions(
    conn,
    checkpoint_date: date,
    worker_id: int,
    num_workers: int,
    days_back: int = 30,
    max_per_member: int = 10,
) -> dict[UUID, list[dict[str, Any]]]:
    """
    Load recent interactions for survey suppression and context.

    Only loads interactions for policies owned by this worker.

    Args:
        conn: Database connection
        checkpoint_date: Date for filtering
        worker_id: This worker's ID
        num_workers: Total number of workers
        days_back: Number of days to look back
        max_per_member: Maximum interactions per member

    Returns:
        Dict mapping member_id to list of interaction dicts (for this worker's partition)
    """
    cutoff_date = checkpoint_date - timedelta(days=days_back)

    query = text("""
        SELECT 
            i.interaction_id,
            i.member_id,
            i.policy_id,
            p.policy_number,
            i.interaction_type_id,
            i.channel,
            i.direction,
            i.start_datetime,
            i.end_datetime,
            i.duration_seconds,
            i.subject,
            i.outcome_id,
            i.first_contact_resolution,
            i.satisfaction_score,
            i.trigger_event_type,
            i.case_id,
            i.claim_id,
            i.invoice_id,
            ROW_NUMBER() OVER (
                PARTITION BY i.member_id 
                ORDER BY i.start_datetime DESC
            ) as row_num
        FROM interaction i
        LEFT JOIN policy p ON i.policy_id = p.policy_id
        WHERE i.start_datetime >= :cutoff_date
          AND i.start_datetime <= :checkpoint_date
    """)

    result = conn.execute(
        query,
        {"cutoff_date": cutoff_date, "checkpoint_date": checkpoint_date},
    )

    interactions: dict[UUID, list[dict[str, Any]]] = {}

    for row in result:
        if row.row_num > max_per_member:
            continue

        # Filter by worker based on policy_number prefix
        if row.policy_number:
            if not _is_owned_by_worker_number(row.policy_number, worker_id):
                continue

        member_id = UUID(str(row.member_id))

        if member_id not in interactions:
            interactions[member_id] = []

        interactions[member_id].append(
            {
                "interaction_id": UUID(str(row.interaction_id)),
                "policy_id": UUID(str(row.policy_id)) if row.policy_id else None,
                "interaction_type_id": row.interaction_type_id,
                "channel": row.channel,
                "direction": row.direction,
                "timestamp": row.start_datetime,
                "start_datetime": row.start_datetime,
                "end_datetime": row.end_datetime,
                "duration_seconds": row.duration_seconds,
                "subject": row.subject,
                "outcome_id": row.outcome_id,
                "first_contact_resolution": row.first_contact_resolution,
                "satisfaction_score": row.satisfaction_score,
                "trigger_event_type": row.trigger_event_type,
                "case_id": UUID(str(row.case_id)) if row.case_id else None,
                "claim_id": UUID(str(row.claim_id)) if row.claim_id else None,
                "invoice_id": UUID(str(row.invoice_id)) if row.invoice_id else None,
            }
        )

    logger.debug(
        "loaded_recent_interactions",
        members=len(interactions),
        total_interactions=sum(len(ints) for ints in interactions.values()),
    )
    return interactions


def load_pending_invoices(
    conn,
    checkpoint_date: date,
    worker_id: int,
    num_workers: int,
) -> dict[UUID, dict[str, Any]]:
    """
    Load pending invoices (Issued or Overdue status).

    Only loads invoices for policies owned by this worker's partition.

    Creates a structure compatible with BillingProcess.pending_invoices:
    - "invoice": dict with invoice fields (not Pydantic model)
    - "policy_id": UUID
    - "due_date": date
    - "next_attempt_date": date
    - "attempts": int
    - "arrears_created": bool

    Args:
        conn: Database connection
        checkpoint_date: Date for filtering
        worker_id: This worker's ID
        num_workers: Total number of workers

    Returns:
        Dict mapping invoice_id to invoice data dict (for this worker's partition)
    """
    query = text("""
        SELECT 
            i.invoice_id,
            i.invoice_number,
            i.policy_id,
            p.policy_number,
            i.invoice_status,
            i.invoice_date,
            i.due_date,
            i.period_start,
            i.period_end,
            i.total_amount,
            i.balance_due,
            i.paid_amount,
            i.gross_premium,
            i.lhc_loading_amount,
            i.age_discount_amount,
            i.rebate_amount,
            i.other_adjustments,
            i.net_amount,
            i.gst_amount,
            i.retry_attempts,
            i.next_retry_date,
            i.arrears_created
        FROM invoice i
        JOIN policy p ON i.policy_id = p.policy_id
        WHERE i.invoice_status IN ('Issued', 'Overdue')
          AND i.balance_due > 0
          AND p.policy_status = 'Active'
          AND (p.end_date IS NULL OR p.end_date > :checkpoint_date)
    """)

    result = conn.execute(query, {"checkpoint_date": checkpoint_date})
    invoices: dict[UUID, dict[str, Any]] = {}

    for row in result:
        invoice_id = UUID(str(row.invoice_id))
        policy_id = UUID(str(row.policy_id))
        
        # Filter by worker based on policy_number prefix
        if not _is_owned_by_worker_number(row.policy_number, worker_id):
            continue

        # Create invoice dict structure compatible with BillingProcess
        # The billing process expects invoice_data["invoice"] to be an object
        # with attributes like invoice_id, invoice_status, due_date, etc.
        # We create a SimpleNamespace-like dict that can be accessed as attributes
        invoice_obj = type("InvoiceFromDB", (), {
            "invoice_id": invoice_id,
            "invoice_number": row.invoice_number,
            "policy_id": policy_id,
            "invoice_status": row.invoice_status,
            "invoice_date": row.invoice_date,
            "due_date": row.due_date,
            "period_start": row.period_start,
            "period_end": row.period_end,
            "total_amount": row.total_amount,
            "balance_due": row.balance_due,
            "paid_amount": row.paid_amount,
            "gross_premium": row.gross_premium,
            "lhc_loading_amount": row.lhc_loading_amount,
            "age_discount_amount": row.age_discount_amount,
            "rebate_amount": row.rebate_amount,
            "other_adjustments": row.other_adjustments,
            "net_amount": row.net_amount,
            "gst_amount": row.gst_amount,
        })()

        invoices[invoice_id] = {
            "invoice": invoice_obj,
            "policy_id": policy_id,
            "due_date": row.due_date,
            # Retry state from DB (if columns exist) or defaults
            "attempts": row.retry_attempts if row.retry_attempts is not None else 0,
            "next_attempt_date": row.next_retry_date or row.due_date,
            "arrears_created": row.arrears_created if row.arrears_created is not None else False,
        }

    logger.debug("loaded_pending_invoices", count=len(invoices))
    return invoices


def load_cumulative_usage(
    conn,
    checkpoint_date: date,
    worker_id: int,
    num_workers: int,
) -> dict[tuple[UUID, int, str], Decimal]:
    """
    Load cumulative benefit usage for current financial year.

    Only loads usage for members on policies owned by this worker.

    Args:
        conn: Database connection
        checkpoint_date: Date for determining financial year
        worker_id: This worker's ID
        num_workers: Total number of workers

    Returns:
        Dict mapping (member_id, benefit_category_id, benefit_year) to total used
    """
    # Determine current Australian financial year
    if checkpoint_date.month >= 7:
        fy_start_year = checkpoint_date.year
    else:
        fy_start_year = checkpoint_date.year - 1
    benefit_year = f"{fy_start_year}-{fy_start_year + 1}"

    query = text("""
        SELECT 
            bu.member_id,
            bu.benefit_category_id,
            bu.benefit_year,
            SUM(bu.usage_amount) as total_used,
            pm.policy_id,
            p.policy_number
        FROM benefit_usage bu
        JOIN policy_member pm ON bu.member_id = pm.member_id AND pm.is_active = TRUE
        JOIN policy p ON pm.policy_id = p.policy_id
        WHERE bu.benefit_year = :benefit_year
        GROUP BY bu.member_id, bu.benefit_category_id, bu.benefit_year, pm.policy_id, p.policy_number
    """)

    result = conn.execute(query, {"benefit_year": benefit_year})
    usage: dict[tuple[UUID, int, str], Decimal] = {}

    for row in result:
        # Filter by worker based on policy_number prefix
        if not _is_owned_by_worker_number(row.policy_number, worker_id):
            continue
            
        key = (
            UUID(str(row.member_id)),
            row.benefit_category_id,
            row.benefit_year,
        )
        usage[key] = Decimal(str(row.total_used))

    logger.debug(
        "loaded_cumulative_usage",
        worker_id=worker_id,
        benefit_year=benefit_year,
        entries=len(usage),
    )
    return usage


def load_active_suspensions(
    conn,
    checkpoint_date: date,
    worker_id: int,
    num_workers: int,
) -> dict[UUID, dict[str, Any]]:
    """
    Load active policy suspensions.

    Only loads suspensions for policies owned by this worker.

    Args:
        conn: Database connection
        checkpoint_date: Date for filtering
        worker_id: This worker's ID
        num_workers: Total number of workers

    Returns:
        Dict mapping policy_id to suspension data (for this worker's partition)
    """
    query = text("""
        SELECT 
            s.suspension_id,
            s.policy_id,
            p.policy_number,
            s.suspension_type,
            s.reason,
            s.start_date,
            s.expected_end_date,
            s.actual_end_date,
            s.status
        FROM suspension s
        JOIN policy p ON s.policy_id = p.policy_id
        WHERE s.status = 'Active'
          AND s.start_date <= :checkpoint_date
          AND (s.actual_end_date IS NULL OR s.actual_end_date > :checkpoint_date)
    """)

    result = conn.execute(query, {"checkpoint_date": checkpoint_date})
    suspensions: dict[UUID, dict[str, Any]] = {}

    for row in result:
        policy_id = UUID(str(row.policy_id))
        
        # Filter by worker based on policy_number prefix
        if not _is_owned_by_worker_number(row.policy_number, worker_id):
            continue
            
        suspensions[policy_id] = {
            "suspension_id": UUID(str(row.suspension_id)),
            "policy_id": policy_id,
            "suspension_type": row.suspension_type,
            "reason": row.reason,
            "start_date": row.start_date,
            "expected_end_date": row.expected_end_date,
            "actual_end_date": row.actual_end_date,
            "status": row.status,
        }

    logger.debug("loaded_active_suspensions", worker_id=worker_id, count=len(suspensions))
    return suspensions


# =============================================================================
# Single-Policy Loaders (for suspension reactivation)
# =============================================================================


def load_single_policy(
    conn,
    policy_id: UUID,
    checkpoint_date: date,
) -> dict[str, Any] | None:
    """
    Load a single policy with members and coverages from database.

    Used when reactivating a suspended policy to populate in-memory state.
    Does not filter by worker (assumes policy ownership already verified).

    Args:
        conn: Database connection
        policy_id: Policy UUID to load
        checkpoint_date: Date for filtering active records

    Returns:
        Policy data dict or None if not found/not active
    """
    # Query the specific policy
    policy_query = text("""
        SELECT 
            p.policy_id,
            p.policy_number,
            p.policy_status,
            p.policy_type,
            p.effective_date,
            p.end_date,
            p.product_id,
            p.premium_amount,
            p.excess_amount,
            p.payment_frequency,
            p.state_of_residence
        FROM policy p
        WHERE p.policy_id = :policy_id
          AND p.policy_status = 'Active'
          AND p.effective_date <= :checkpoint_date
          AND (p.end_date IS NULL OR p.end_date > :checkpoint_date)
          AND NOT EXISTS (
              SELECT 1 FROM suspension s
              WHERE s.policy_id = p.policy_id
                AND s.status = 'Active'
                AND s.start_date <= :checkpoint_date
                AND (s.actual_end_date IS NULL OR s.actual_end_date > :checkpoint_date)
          )
    """)

    result = conn.execute(
        policy_query,
        {"policy_id": str(policy_id), "checkpoint_date": checkpoint_date},
    )
    row = result.fetchone()

    if not row:
        logger.debug("load_single_policy_not_found", policy_id=str(policy_id))
        return None

    # Create a policy object with attributes (compatible with billing process)
    # Convert policy_type string to enum for compatibility with all processes
    policy_type_enum = PolicyType(row.policy_type) if row.policy_type else None
    policy_obj = type("PolicyFromDB", (), {
        "policy_id": policy_id,
        "policy_number": row.policy_number,
        "policy_status": row.policy_status,
        "policy_type": policy_type_enum,
        "effective_date": row.effective_date,
        "end_date": row.end_date,
        "product_id": row.product_id,
        "premium_amount": row.premium_amount,
        "excess_amount": row.excess_amount,
        "payment_frequency": row.payment_frequency,
        "state_of_residence": row.state_of_residence,
    })()

    policy_data: dict[str, Any] = {
        "policy": policy_obj,
        "policy_id": policy_id,
        "policy_number": row.policy_number,
        "status": row.policy_status,
        "effective_date": row.effective_date,
        "end_date": row.end_date,
        "product_id": row.product_id,
        "premium_amount": row.premium_amount,
        "excess": row.excess_amount,
        "payment_frequency": row.payment_frequency,
        "members": [],
        "coverages": [],
    }

    # Load members for this policy
    member_query = text("""
        SELECT 
            pm.policy_member_id,
            pm.policy_id,
            pm.member_id,
            pm.member_role,
            pm.relationship_to_primary,
            m.member_number,
            m.title,
            m.first_name,
            m.middle_name,
            m.last_name,
            m.preferred_name,
            m.date_of_birth,
            m.gender,
            m.medicare_number,
            m.medicare_irn,
            m.medicare_expiry_date,
            m.address_line_1,
            m.address_line_2,
            m.suburb,
            m.state,
            m.postcode,
            m.country,
            m.email,
            m.mobile_phone,
            m.home_phone,
            m.australian_resident,
            m.tax_file_number_provided,
            m.lhc_applicable,
            m.marital_status
        FROM policy_member pm
        JOIN member m ON pm.member_id = m.member_id
        WHERE pm.policy_id = :policy_id
          AND pm.is_active = TRUE
          AND (pm.end_date IS NULL OR pm.end_date > :checkpoint_date)
          AND m.deceased_flag = FALSE
    """)

    result = conn.execute(
        member_query,
        {"policy_id": str(policy_id), "checkpoint_date": checkpoint_date},
    )

    for member_row in result:
        # Convert to MemberCreate Pydantic model for consistency with fresh acquisition
        member = MemberCreate(
            member_id=UUID(str(member_row.member_id)),
            member_number=member_row.member_number,
            title=member_row.title,
            first_name=member_row.first_name,
            middle_name=member_row.middle_name,
            last_name=member_row.last_name,
            preferred_name=member_row.preferred_name,
            date_of_birth=member_row.date_of_birth,
            gender=Gender(member_row.gender) if isinstance(member_row.gender, str) else member_row.gender,
            medicare_number=member_row.medicare_number,
            medicare_irn=member_row.medicare_irn,
            medicare_expiry_date=member_row.medicare_expiry_date,
            address_line_1=member_row.address_line_1,
            address_line_2=member_row.address_line_2,
            suburb=member_row.suburb,
            state=member_row.state,
            postcode=member_row.postcode,
            country=member_row.country or "AUS",
            email=member_row.email,
            mobile_phone=member_row.mobile_phone,
            home_phone=member_row.home_phone,
            australian_resident=member_row.australian_resident if member_row.australian_resident is not None else True,
            tax_file_number_provided=member_row.tax_file_number_provided if member_row.tax_file_number_provided is not None else False,
            lhc_applicable=member_row.lhc_applicable if member_row.lhc_applicable is not None else False,
            marital_status=MaritalStatus(member_row.marital_status) if member_row.marital_status else MaritalStatus.SINGLE,
        )
        policy_data["members"].append(member)

    # Load coverages for this policy
    coverage_query = text("""
        SELECT 
            c.coverage_id,
            c.policy_id,
            c.coverage_type,
            c.tier,
            c.status,
            c.effective_date,
            c.end_date
        FROM coverage c
        WHERE c.policy_id = :policy_id
          AND c.status = 'Active'
          AND c.effective_date <= :checkpoint_date
          AND (c.end_date IS NULL OR c.end_date > :checkpoint_date)
    """)

    result = conn.execute(
        coverage_query,
        {"policy_id": str(policy_id), "checkpoint_date": checkpoint_date},
    )

    for cov_row in result:
        policy_data["coverages"].append(
            {
                "coverage_id": UUID(str(cov_row.coverage_id)),
                "coverage_type": cov_row.coverage_type,
                "tier": cov_row.tier,
                "status": cov_row.status,
                "effective_date": cov_row.effective_date,
                "end_date": cov_row.end_date,
            }
        )

    # Load LHC loading
    lhc_query = text("""
        SELECT SUM(loading_percentage) as total_loading
        FROM lhc_loading
        WHERE policy_id = :policy_id
          AND is_loading_active = TRUE
    """)

    result = conn.execute(lhc_query, {"policy_id": str(policy_id)})
    lhc_row = result.fetchone()
    if lhc_row and lhc_row.total_loading:
        policy_data["lhc_loading"] = float(lhc_row.total_loading)

    # Load age-based discount
    discount_query = text("""
        SELECT SUM(current_discount_pct) as total_discount
        FROM age_based_discount
        WHERE policy_id = :policy_id
          AND is_active = TRUE
    """)

    result = conn.execute(discount_query, {"policy_id": str(policy_id)})
    discount_row = result.fetchone()
    if discount_row and discount_row.total_discount:
        policy_data["age_discount"] = float(discount_row.total_discount)

    # Load PHI rebate
    rebate_query = text("""
        SELECT rebate_percentage
        FROM phi_rebate_entitlement
        WHERE policy_id = :policy_id
          AND effective_date <= :checkpoint_date
          AND (end_date IS NULL OR end_date > :checkpoint_date)
        ORDER BY effective_date DESC
        LIMIT 1
    """)

    result = conn.execute(
        rebate_query,
        {"policy_id": str(policy_id), "checkpoint_date": checkpoint_date},
    )
    rebate_row = result.fetchone()
    if rebate_row and rebate_row.rebate_percentage:
        policy_data["rebate_pct"] = float(rebate_row.rebate_percentage)

    logger.debug(
        "loaded_single_policy",
        policy_id=str(policy_id),
        members=len(policy_data["members"]),
        coverages=len(policy_data["coverages"]),
    )
    return policy_data


def load_single_policy_members(
    conn,
    policy_id: UUID,
    checkpoint_date: date,
) -> dict[UUID, dict[str, Any]]:
    """
    Load policy members for a single policy (for claims processing).

    Used when reactivating a suspended policy to populate in-memory state.
    Does not filter by worker (assumes policy ownership already verified).

    Args:
        conn: Database connection
        policy_id: Policy UUID to load members for
        checkpoint_date: Date for filtering

    Returns:
        Dict mapping policy_member_id to member data dict
    """
    query = text("""
        SELECT 
            pm.policy_member_id,
            pm.policy_id,
            pm.member_id,
            pm.member_role,
            -- Member fields
            m.member_number,
            m.title,
            m.first_name,
            m.middle_name,
            m.last_name,
            m.preferred_name,
            m.date_of_birth,
            m.gender,
            m.medicare_number,
            m.medicare_irn,
            m.medicare_expiry_date,
            m.address_line_1,
            m.address_line_2,
            m.suburb,
            m.state,
            m.postcode,
            m.country,
            m.email,
            m.mobile_phone,
            m.home_phone,
            m.australian_resident,
            m.tax_file_number_provided,
            m.lhc_applicable,
            m.marital_status,
            -- Policy fields
            p.policy_number,
            p.policy_status,
            p.policy_type,
            p.product_id,
            p.effective_date as policy_effective_date,
            p.premium_amount,
            p.excess_amount,
            p.payment_frequency,
            p.state_of_residence,
            -- Hospital coverage
            hosp.coverage_id as hospital_coverage_id,
            hosp.tier as hospital_tier,
            hosp.excess_amount as hospital_excess_amount,
            -- Extras coverage
            ext.coverage_id as extras_coverage_id,
            ext.excess_amount as extras_excess_amount,
            -- Ambulance coverage
            amb.coverage_id as ambulance_coverage_id,
            amb.excess_amount as ambulance_excess_amount
        FROM policy_member pm
        JOIN member m ON pm.member_id = m.member_id
        JOIN policy p ON pm.policy_id = p.policy_id
        LEFT JOIN coverage hosp ON p.policy_id = hosp.policy_id 
            AND hosp.coverage_type = 'Hospital' 
            AND hosp.status = 'Active'
            AND (hosp.end_date IS NULL OR hosp.end_date > :checkpoint_date)
        LEFT JOIN coverage ext ON p.policy_id = ext.policy_id 
            AND ext.coverage_type = 'Extras' 
            AND ext.status = 'Active'
            AND (ext.end_date IS NULL OR ext.end_date > :checkpoint_date)
        LEFT JOIN coverage amb ON p.policy_id = amb.policy_id 
            AND amb.coverage_type = 'Ambulance' 
            AND amb.status = 'Active'
            AND (amb.end_date IS NULL OR amb.end_date > :checkpoint_date)
        WHERE pm.policy_id = :policy_id
          AND pm.is_active = TRUE
          AND (pm.end_date IS NULL OR pm.end_date > :checkpoint_date)
          AND p.policy_status = 'Active'
          AND (p.end_date IS NULL OR p.end_date > :checkpoint_date)
          AND m.deceased_flag = FALSE
          AND NOT EXISTS (
              SELECT 1 FROM suspension s
              WHERE s.policy_id = p.policy_id
                AND s.status = 'Active'
                AND s.start_date <= :checkpoint_date
                AND (s.actual_end_date IS NULL OR s.actual_end_date > :checkpoint_date)
          )
    """)

    result = conn.execute(
        query,
        {"policy_id": str(policy_id), "checkpoint_date": checkpoint_date},
    )
    policy_members: dict[UUID, dict[str, Any]] = {}

    for row in result:
        policy_member_id = UUID(str(row.policy_member_id))
        pid = UUID(str(row.policy_id))
        member_id = UUID(str(row.member_id))

        # Calculate age
        age = None
        if row.date_of_birth:
            age = (
                checkpoint_date.year
                - row.date_of_birth.year
                - (
                    (checkpoint_date.month, checkpoint_date.day)
                    < (row.date_of_birth.month, row.date_of_birth.day)
                )
            )

        # Convert marital_status string to enum
        marital_status_enum = MaritalStatus.SINGLE
        if row.marital_status:
            try:
                marital_status_enum = MaritalStatus(row.marital_status)
            except ValueError:
                try:
                    marital_status_enum = MaritalStatus[row.marital_status.upper()]
                except (KeyError, AttributeError):
                    marital_status_enum = MaritalStatus.SINGLE

        # Convert gender string to enum
        gender_enum = Gender.UNKNOWN
        if row.gender:
            try:
                gender_enum = Gender(row.gender)
            except ValueError:
                try:
                    gender_enum = Gender[row.gender.upper()]
                except (KeyError, AttributeError):
                    gender_enum = Gender.UNKNOWN

        # Create member object with attributes
        member_obj = type("MemberFromDB", (), {
            "member_id": member_id,
            "member_number": row.member_number,
            "title": row.title,
            "first_name": row.first_name,
            "middle_name": row.middle_name,
            "last_name": row.last_name,
            "preferred_name": row.preferred_name,
            "date_of_birth": row.date_of_birth,
            "gender": gender_enum,
            "medicare_number": row.medicare_number,
            "medicare_irn": row.medicare_irn,
            "medicare_expiry_date": row.medicare_expiry_date,
            "address_line_1": row.address_line_1,
            "address_line_2": row.address_line_2,
            "suburb": row.suburb,
            "state": row.state,
            "postcode": row.postcode,
            "country": row.country,
            "email": row.email,
            "mobile_phone": row.mobile_phone,
            "home_phone": row.home_phone,
            "australian_resident": row.australian_resident,
            "tax_file_number_provided": row.tax_file_number_provided,
            "lhc_applicable": row.lhc_applicable,
            "marital_status": marital_status_enum,
        })()

        # Create policy object with attributes
        # Convert policy_type string to enum for compatibility with all processes
        policy_type_enum = PolicyType(row.policy_type) if row.policy_type else None
        policy_obj = type("PolicyFromDB", (), {
            "policy_id": pid,
            "policy_number": row.policy_number,
            "policy_status": row.policy_status,
            "policy_type": policy_type_enum,
            "product_id": row.product_id,
            "effective_date": row.policy_effective_date,
            "premium_amount": row.premium_amount,
            "excess_amount": row.excess_amount,
            "payment_frequency": row.payment_frequency,
            "state_of_residence": row.state_of_residence,
        })()

        # Create coverage objects
        hospital_coverage_obj = None
        extras_coverage_obj = None
        ambulance_coverage_obj = None

        if row.hospital_coverage_id:
            hospital_coverage_obj = type("CoverageFromDB", (), {
                "coverage_id": UUID(str(row.hospital_coverage_id)),
                "tier": row.hospital_tier,
                "coverage_type": "Hospital",
                "excess_amount": row.hospital_excess_amount,
            })()

        if row.extras_coverage_id:
            extras_coverage_obj = type("CoverageFromDB", (), {
                "coverage_id": UUID(str(row.extras_coverage_id)),
                "coverage_type": "Extras",
                "excess_amount": row.extras_excess_amount,
            })()

        if row.ambulance_coverage_id:
            ambulance_coverage_obj = type("CoverageFromDB", (), {
                "coverage_id": UUID(str(row.ambulance_coverage_id)),
                "coverage_type": "Ambulance",
                "excess_amount": row.ambulance_excess_amount,
            })()

        policy_members[policy_member_id] = {
            "policy_member_id": policy_member_id,
            "policy_id": pid,
            "member_id": member_id,
            "member_role": row.member_role,
            "first_name": row.first_name,
            "last_name": row.last_name,
            "date_of_birth": row.date_of_birth,
            "gender": row.gender,
            "age": age,
            "email": row.email,
            "mobile_phone": row.mobile_phone,
            "state": row.state,
            "policy_number": row.policy_number,
            "policy_status": row.policy_status,
            "product_id": row.product_id,
            "policy_effective_date": row.policy_effective_date,
            "hospital_coverage_id": UUID(str(row.hospital_coverage_id))
            if row.hospital_coverage_id
            else None,
            "hospital_tier": row.hospital_tier,
            "extras_coverage_id": UUID(str(row.extras_coverage_id))
            if row.extras_coverage_id
            else None,
            "ambulance_coverage_id": UUID(str(row.ambulance_coverage_id))
            if row.ambulance_coverage_id
            else None,
            "member": member_obj,
            "policy": policy_obj,
            "hospital_coverage": hospital_coverage_obj,
            "extras_coverage": extras_coverage_obj,
            "ambulance_coverage": ambulance_coverage_obj,
        }

    logger.debug(
        "loaded_single_policy_members",
        policy_id=str(policy_id),
        count=len(policy_members),
    )
    return policy_members


def load_single_policy_waiting_periods(
    conn,
    policy_id: UUID,
    checkpoint_date: date,
) -> dict[UUID, list[dict[str, Any]]]:
    """
    Load waiting periods for a single policy's members.

    Used when reactivating a suspended policy to populate in-memory state.
    Does not filter by worker (assumes policy ownership already verified).

    Args:
        conn: Database connection
        policy_id: Policy UUID to load waiting periods for
        checkpoint_date: Date for filtering

    Returns:
        Dict mapping policy_member_id to list of waiting period dicts
    """
    query = text("""
        SELECT 
            wp.waiting_period_id,
            wp.policy_member_id,
            wp.coverage_id,
            wp.waiting_period_type,
            wp.start_date,
            wp.end_date,
            wp.status,
            wp.benefit_category_id,
            wp.clinical_category_id,
            wp.duration_months,
            c.coverage_type
        FROM waiting_period wp
        JOIN coverage c ON wp.coverage_id = c.coverage_id
        JOIN policy_member pm ON wp.policy_member_id = pm.policy_member_id
        JOIN policy p ON pm.policy_id = p.policy_id
        WHERE pm.policy_id = :policy_id
          AND wp.status = 'InProgress'
          AND wp.end_date >= :checkpoint_date
          AND pm.is_active = TRUE
          AND p.policy_status = 'Active'
          AND (p.end_date IS NULL OR p.end_date > :checkpoint_date)
        ORDER BY wp.policy_member_id, wp.start_date
    """)

    result = conn.execute(
        query,
        {"policy_id": str(policy_id), "checkpoint_date": checkpoint_date},
    )
    waiting_periods: dict[UUID, list[dict[str, Any]]] = {}

    for row in result:
        policy_member_id = UUID(str(row.policy_member_id))

        if policy_member_id not in waiting_periods:
            waiting_periods[policy_member_id] = []

        waiting_periods[policy_member_id].append(
            {
                "waiting_period_id": UUID(str(row.waiting_period_id)),
                "coverage_id": UUID(str(row.coverage_id)),
                "coverage_type": row.coverage_type,
                "waiting_period_type": row.waiting_period_type,
                "start_date": row.start_date,
                "end_date": row.end_date,
                "status": row.status,
                "benefit_category_id": row.benefit_category_id,
                "clinical_category_id": row.clinical_category_id,
                "duration_months": row.duration_months,
            }
        )

    logger.debug(
        "loaded_single_policy_waiting_periods",
        policy_id=str(policy_id),
        members_with_wp=len(waiting_periods),
        total_wp=sum(len(wps) for wps in waiting_periods.values()),
    )
    return waiting_periods
