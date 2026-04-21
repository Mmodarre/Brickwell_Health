# Suggested Commands (macOS / Darwin, zsh)

All Python commands assume [uv](https://docs.astral.sh/uv/). The project venv is `.venv/`. Either prefix with `uv run` or activate via `source .venv/bin/activate`.

## Environment
```bash
uv sync                                # create venv + install runtime + dev deps (editable)
uv run brickwell --help                # verify install
```

## Database (PostgreSQL via docker-compose)
```bash
docker-compose up -d                   # start postgres container brickwell_health_db
docker-compose ps                      # health check
uv run brickwell -c config/simulation.yaml init-db                 # create schema
uv run brickwell -c config/simulation.yaml init-db --drop-existing # fresh start
docker exec -it brickwell_health_db psql -U brickwell -d brickwell_health
```

## Running simulations
```bash
uv run brickwell run                           # default config
uv run brickwell -c config/simulation.yaml run # custom config
uv run brickwell run --workers 8               # explicit worker count
uv run brickwell run --sequential              # single-process debug mode
uv run brickwell -v run                        # verbose logs
uv run brickwell --json-logs run               # structured logs
```

## Tests
```bash
uv run pytest                                            # all tests
uv run pytest -v                                         # verbose
uv run pytest -n auto                                    # parallel (pytest-xdist)
uv run pytest tests/unit/test_claims_generator.py        # specific file
uv run pytest --cov=brickwell_health --cov-report=html   # coverage
```

## Code quality (run before considering a task complete)
```bash
uv run ruff format .                  # format
uv run ruff check .                   # lint
uv run mypy brickwell_health          # type check (strict — disallow_untyped_defs)
```

## Checkpoint cleanup
```bash
rm -rf data/checkpoints/              # force clean restart if a worker crashed
```

## System / git (Darwin — BSD coreutils)
```bash
git status / git log / git diff       # standard
ls -la / ls -lh                       # BSD ls (no --color=auto by default; use -G for color)
find . -name "*.py" -type f           # BSD find (no -printf)
# For content search inside Claude Code, use the Grep tool (not shell rg/grep)
# For file search, use the Glob tool (not shell find)
```

Note: BSD `sed`/`find`/`date` differ from GNU variants. Prefer Claude's dedicated tools (Read/Edit/Glob/Grep) over shell `cat`/`find`/`grep`.
