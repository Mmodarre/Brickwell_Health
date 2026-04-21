# Code Style & Conventions

## Formatter / linter (authoritative)
- **ruff** — `target-version = "py312"`, `line-length = 100`
  - Lint selection: `E`, `F`, `I` (isort), `N` (pep8-naming), `W`, `UP` (pyupgrade)
  - Ignored: `E501` (line too long — ruff format handles width)
- **mypy** — strict config:
  - `python_version = "3.12"`
  - `warn_return_any = true`
  - `warn_unused_ignores = true`
  - `disallow_untyped_defs = true`  ← all functions must have type hints
  - `plugins = ["pydantic.mypy"]`

## Type hints
- Required on all function/method definitions (mypy enforced).
- Use modern 3.12 syntax (`list[int]`, `X | None`, `from __future__` not needed).
- Example from CLAUDE.md:
  ```python
  def process_claim(claim: ClaimCreate, rng: np.random.Generator) -> Claim: ...
  ```

## Naming (ruff N)
- Modules: `snake_case.py`
- Classes: `PascalCase`
- Functions / methods / variables: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Pydantic input models suffixed `*Create` (e.g. `MemberCreate`, `PolicyCreate`, `ClaimCreate`); storage/output models are the bare name (`Member`, `Policy`, `Claim`).
- Domain enums live in `brickwell_health/domain/enums.py`.

## Domain model pattern
- Every entity has a `*Create` (input) model and a base (output/storage) model, both Pydantic v2.
- Generators return `*Create` models ready for insertion via `BatchWriter.add(table_name, record_dict)`.

## Simulation process pattern
- Inherit `BaseProcess` (`core/processes/base.py`).
- Access shared per-worker state via `self.shared_state`; do not mutate other processes' state directly.
- Every loop must `yield self.sim_env.env.timeout(days)`.
- Use `self.rng` for all randomness (NumPy `np.random.Generator`).
- Use `self.id_generator` for UUIDs / sequential numbers (worker-scoped).
- Use `self.reference` for reference data (products, benefits, providers).
- Implement `get_stats()` returning process metrics.
- All writes via `self.batch_writer.add(...)` — never direct SQL.

## Determinism
- Every random decision routes through `self.rng`; each worker seeds with `config.seed + worker_id`.
- Same seed + same config must produce identical output; run twice to verify when touching stochastic code.

## Docstrings / comments
- Default: no comments. Add one only when the *why* is non-obvious (hidden constraint, workaround, regulatory rule).
- Regulatory calculations should reference the regulation (e.g. `config/regulatory.py` for LHC, rebate, MLS, age-based discount).
- Do not narrate what well-named code already says.

## Logging
- Use `structlog` (configured via CLI flag `--json-logs` for structured output).

## Config
- All configuration is Pydantic (`brickwell_health/config/models.py`), loaded from `config/simulation.yaml`. Env overrides use `BRICKWELL_` prefix.
