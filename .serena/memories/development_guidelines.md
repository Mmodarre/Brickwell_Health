# Development Guidelines

## When adding or modifying a SimPy process
1. Inherit from `BaseProcess` (`brickwell_health/core/processes/base.py`).
2. Read/write shared data via `self.shared_state` only — never reach into another process's state directly.
3. Every loop iteration must `yield self.sim_env.env.timeout(days)`; otherwise the SimPy clock stalls.
4. Randomness: `self.rng` (NumPy `Generator`) exclusively. Do not call `random.random()` or re-seed.
5. All persistence: `self.batch_writer.add(table_name, record_dict)`. The writer handles FK ordering via `TABLE_FLUSH_ORDER`.
6. IDs: `self.id_generator` (`generators/id_generator.py`) — worker-scoped so two workers can't collide.
7. Expose a `get_stats()` returning a dict of counters; the orchestrator aggregates these.

## When adding or modifying a generator
- In `brickwell_health/generators/`.
- Inject `self.rng`, `self.id_generator`, `self.reference` — do not instantiate your own.
- Return the `*Create` Pydantic model, never an already-persisted row.

## Domain model changes
- Touching a `*Create` / base pair in `brickwell_health/domain/` usually also needs a matching column change in the relevant `schema_*.sql` file and a recheck of `TABLE_FLUSH_ORDER` in `db/writer.py`.
- Add new enums to `domain/enums.py`, not inline literals.

## Configuration changes
- New config fields go into the Pydantic model in `brickwell_health/config/models.py` with a default.
- Document the field in `config/simulation.yaml.example`. `config/simulation.yaml` is the live/tracked config.
- Environment-variable override uses `BRICKWELL_<FIELD>` prefix (handled by `pydantic-settings`).

## Determinism is a contract
- Same seed + same config must produce byte-identical output.
- Do not introduce: wall-clock time, iteration over unsorted sets/dicts whose order affects output, un-seeded `faker.Faker()`, or parallel non-deterministic shortcuts.
- If you add a new stochastic decision, thread it through `self.rng` and add a reproducibility check (run the sim twice).

## Statistical models
- Live in `brickwell_health/statistics/`. Parameters should be pulled from config, not hard-coded where possible.
- Validate shape of distributions (mean/variance sanity) against the Australian PHI reference data in `data/reference/` rather than assuming.

## Streaming
- Table names must be normalized before topic resolution (see recent fixes in `c7c4285` and `e7961a1`). Schema-qualified names (`schema.table`) resolve differently — use the resolver, don't build topic strings manually.

## Regulatory code
- `brickwell_health/config/regulatory.py` is the single source of truth for LHC, PHI rebate tiers, age-based discount, MLS, and waiting-period durations. Change once there; do not inline the constants in process code.

## Testing philosophy
- Unit tests in `tests/unit/` — exercise generators / statistical models / individual processes with an in-memory or mocked batch writer.
- Integration tests in `tests/integration/` — exercise the full worker / DB / streaming stack.
- `conftest.py` at `tests/conftest.py` holds shared fixtures.
- Strict mypy + ruff are part of acceptance — don't add `# type: ignore` without a specific reason documented inline.

## Out of scope by default (don't do unless asked)
- Refactors beyond what the task requires.
- New abstractions, feature flags, or compatibility shims.
- Commits and pushes — only on explicit user request.
- Destructive DB / branch operations — confirm first.
