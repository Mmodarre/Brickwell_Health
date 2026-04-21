# Architecture Cheatsheet

## Orchestration hierarchy
```
ParallelRunner (brickwell_health/core/parallel_runner.py)
 └── SimulationWorker × N  (core/worker.py)  — one per UUID-hash partition
      ├── SimulationEnvironment (core/environment.py) — SimPy env + sim-time↔date conversion
      ├── SharedState (core/shared_state.py) — per-worker in-memory state (NOT shared across workers)
      ├── BatchWriter (db/writer.py) — COPY-based batched writes, respects TABLE_FLUSH_ORDER
      ├── IDGenerator (generators/id_generator.py) — deterministic UUIDs + sequence numbers, worker-scoped
      └── SimPy processes (core/processes/*.py)
```

## SimPy processes (all inherit `BaseProcess`)
| Process | File | Role |
|---|---|---|
| AcquisitionProcess | acquisition.py | New applications → policies/members; populates SharedState |
| PolicyLifecycleProcess | policy_lifecycle.py | Upgrades, downgrades, cancellations |
| MemberLifecycleProcess | member_lifecycle.py | Demographic changes (address, phone, death, Medicare renewal) |
| SuspensionProcess | suspension.py | Overseas / hardship suspensions + reactivation |
| ClaimsProcess | claims.py | Hospital / extras / ambulance claims with statistical models |
| BillingProcess | billing.py | Invoices, payments, arrears, lapses |
| CRMProcess | crm.py | Interactions, cases, complaints from trigger events |
| CommunicationProcess | communication.py | Transactional + marketing comms + responses |
| DigitalBehaviorProcess | digital.py | Web sessions & digital events |
| SurveyProcess | survey.py | Creates NPS / CSAT pending rows with `llm_context` |
| NBAProcess | nba.py | Next-Best-Action domain |

## Statistical models (`brickwell_health/statistics/`)
- `ChurnPredictionModel` — age-based log-odds + LHC/MLS/tenure adjustments
- `ClaimPropensityModel` — Poisson frequency + lognormal/normal severity
- `ProductSelectionModel` — policy type / state / age matching
- `ABSDemographicsModel` — ABS age/state/gender distributions
- `IncomeModel` — income for PHI rebate tier

## Claims validation (two layers)
1. **Deterministic rejection**: no active coverage, within waiting period, policy inactive/suspended, benefit limit exhausted.
2. **Stochastic approval rates**: hospital 98%, extras 92%, ambulance 95%.
Waiting periods stored in `SharedState.waiting_periods` keyed by `policy_member_id`.

## Regulatory rules (`brickwell_health/config/regulatory.py`)
- **LHC**: 2% loading per year over 30 at entry, capped 70%.
- **Age-based discount**: up to 10% for ages 18–29.
- **PHI rebate**: income-tested tiers with age brackets (0–8.47% → 24.608–32.812%).
- **MLS**: retention-factor input to churn model.
- **Waiting periods**: 2 mo (psych/rehab), 6 mo (pre-existing), 12 mo (pregnancy/IVF).

## NPS survey two-phase pipeline
1. **During sim**: `SurveyProcess` writes `nps_survey_pending` with `llm_context` JSON (demographics, claim history, interactions, trigger event).
2. **Post-sim**: `ParallelRunner._enrich_survey_contexts()` adds historical data via SQL.
3. **Deferred LLM**: downstream job (Databricks `ai_query()`; reference: `scripts/test_nps_ai_query.py`) generates NPS score, driver scores, sentiment, free-text.

## DB layer
- Schema split by domain: `schema_init.sql`, `schema_policy.sql`, `schema_billing.sql`, `schema_claims.sql`, `schema_crm.sql`, `schema_communication.sql`, `schema_digital.sql`, `schema_survey.sql`, `schema_member_lifecycle.sql`, `schema_nba.sql`, `schema_reference.sql` (+ `schema_reference_fk.sql`), `schema_regulatory.sql`, `schema_system.sql`.
- Initialized via `brickwell ... init-db` (delegates to `db/initialize.py`).
- `BatchWriter` flushes in dependency order — do not reorder `TABLE_FLUSH_ORDER` without understanding FKs.

## Streaming (optional)
- `brickwell_health/streaming/` — `factory.py`, `publisher.py`, `wrapper.py`, `topic_resolver.py`, `token_cache.py`, `implementations/`. Recent commits fixed topic resolution for schema-qualified / normalized table names.

## Checkpointing
- Two implementations live: `checkpoint.py` (v1) and `checkpoint_v2.py`. `state_reconstruction.py` rebuilds `SharedState` from DB on resume. Recent fix (commit `36964e2`) tracks warmup progress across incremental runs.
- Clean restart: `rm -rf data/checkpoints/`.
