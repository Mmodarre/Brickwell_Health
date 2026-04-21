# Brickwell Health — Project Overview

**Purpose**: Discrete-event simulator for Australian Private Health Insurance (PHI). Generates production-quality transactional data spanning acquisition, policy management, claims, billing, CRM, communications, digital behaviour, NPS surveys, and Australian regulatory compliance (LHC, MLS, PHI rebate). Output is written to PostgreSQL (and optionally streamed to Kafka/Event Hubs).

**Core engine**: SimPy discrete-event simulation wrapped in a multi-worker parallel architecture (`ParallelRunner` orchestrator -> N `SimulationWorker` processes, one per UUID hash partition).

**Python**: 3.12 (see `.python-version`), package `brickwell_health`, CLI entrypoint `brickwell` -> `brickwell_health.cli:main`.

## Top-level layout
- `brickwell_health/` — main package
  - `cli.py` — Click-based CLI
  - `config/` — Pydantic config models (`models.py`) + regulatory helpers (`regulatory.py`)
  - `core/` — simulation engine: `worker.py`, `parallel_runner.py`, `environment.py`, `shared_state.py`, `partition.py`, `checkpoint.py` / `checkpoint_v2.py`, `state_reconstruction.py`, `trigger_engine.py`, `llm_processor.py`, `serializers.py`
  - `core/processes/` — SimPy processes: `acquisition`, `policy_lifecycle`, `member_lifecycle`, `suspension`, `claims`, `billing`, `crm`, `communication`, `digital`, `survey`, `nba`, `base`
  - `db/` — `connection.py`, `writer.py` (high-speed COPY batch writer), `initialize.py`, `protocol.py`, `reference_db_loader.py`, plus `schema_*.sql` files
  - `domain/` — Pydantic entity models (enums, member, policy, claims, billing, crm, communication, digital, survey). Each entity has `*Create` input model + base storage model.
  - `generators/` — entity generators including `id_generator.py`
  - `statistics/` — churn, claim propensity, product selection, ABS demographics, income models
  - `streaming/` — Kafka/Event Hubs publisher stack (`factory.py`, `publisher.py`, `wrapper.py`, `topic_resolver.py`, `token_cache.py`, `implementations/`)
  - `reference/`, `utils/`
- `config/simulation.yaml` — main runtime config (prefix env vars with `BRICKWELL_` to override)
- `scripts/` — data-loading and testing utilities (`generate_reference_data.py`, `test_nps_ai_query.py`, WAL optimization scripts, etc.)
- `tests/` — `unit/`, `integration/`, shared `conftest.py`
- `data/reference/` — CSV reference data (products, benefits, providers, DRG, ABS demographics)
- `docs/` — design + runbook docs
- `docker-compose.yml` — PostgreSQL container (`brickwell_health_db`, user `brickwell`)
- `uv.lock` — committed lockfile (uv-managed)

## Key architectural invariants
- Deterministic entity partitioning by UUID hash; each worker only touches its partition.
- Per-worker RNG seeded with `config.seed + worker_id`; IDs scoped by `worker_id` for cross-worker uniqueness.
- `SharedState` is per-worker, NOT cross-worker. No inter-worker messaging during simulation.
- All DB writes go through `BatchWriter.add(table_name, record_dict)`; never INSERT directly. `TABLE_FLUSH_ORDER` enforces dependency order.
- Processes inherit `BaseProcess`, must `yield self.sim_env.env.timeout(days)`, and expose `get_stats()`.
- NPS surveys are two-phase: `nps_survey_pending` rows with `llm_context` JSON during sim, then post-sim enrichment + deferred LLM (`ai_query` on Databricks).
