# Task Completion Checklist

Run these (in order) before considering a code change done. All must pass.

## 1. Format
```bash
uv run ruff format .
```

## 2. Lint
```bash
uv run ruff check .
```
If there are auto-fixable issues, run `uv run ruff check --fix .` and re-inspect.

## 3. Type check (strict — no untyped defs allowed)
```bash
uv run mypy brickwell_health
```

## 4. Tests
```bash
uv run pytest              # full suite
# or for a targeted change:
uv run pytest tests/unit/test_<module>.py -v
# parallel when full-suite:
uv run pytest -n auto
```

## 5. Determinism check (only if stochastic / simulation code touched)
Run the simulation twice with the same seed and confirm byte-identical results:
```bash
uv run brickwell -c config/simulation.yaml run --workers 4
uv run brickwell -c config/simulation.yaml run --workers 4
```

## 6. DB schema changes
If you touched `brickwell_health/db/schema_*.sql` or domain models that back a table, recreate the schema:
```bash
uv run brickwell -c config/simulation.yaml init-db --drop-existing
```

## 7. Do NOT commit unless explicitly asked
- Project instruction: create commits only when the user asks.
- Never `--no-verify`, never skip hooks.
- Never force-push, `git reset --hard`, or `git clean -f` without explicit user approval.

## Pre-flight sanity for UI-less backend work
- Backend only: the checks above (format + lint + mypy + pytest) are the acceptance bar — there is no UI to exercise.
- If the change crosses the streaming publisher boundary, run the relevant integration test under `tests/integration/` if one exists; otherwise note that manual verification against a running broker is required and say so explicitly.
