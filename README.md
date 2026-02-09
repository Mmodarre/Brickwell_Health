# Brickwell Health Simulator

A discrete event simulation engine that generates production-quality Australian Private Health Insurance (PHI) transactional data at scale.

## Overview

Brickwell Health Simulator models the complete insurance lifecycle for an Australian health fund, producing realistic data across member acquisition, policy management, claims processing, billing, CRM interactions, digital behavior, NPS surveys, and Next Best Action (NBA) marketing. It uses [SimPy](https://simpy.readthedocs.io/) for discrete event simulation with a multi-worker parallel architecture.

**Generated data is suitable for:**
- Data warehouse and lakehouse development
- Analytics, reporting, and BI system validation
- Machine learning model training (churn, fraud detection, propensity)
- Performance and load testing of insurance administration systems

## Features

### Simulation Engine
- **Discrete Event Simulation** built on SimPy for accurate time-based event modeling
- **Multi-worker parallel architecture** with deterministic UUID-based entity partitioning
- **Seed-based reproducibility** for identical results across runs
- **Checkpoint and resume** with crash recovery and simulation extension

### Data Domains

| Domain | Process | What It Generates |
|--------|---------|-------------------|
| **Acquisition** | AcquisitionProcess | Applications, members, policies, coverages, waiting periods |
| **Policy Lifecycle** | PolicyLifecycleProcess | Upgrades, downgrades, cancellations with churn modeling |
| **Member Lifecycle** | MemberLifecycleProcess | Address changes, phone/email updates, death events, Medicare renewals |
| **Suspensions** | SuspensionProcess | Overseas travel and financial hardship suspensions |
| **Claims** | ClaimsProcess | Hospital, extras, and ambulance claims with APRA-validated distributions |
| **Billing** | BillingProcess | Invoices, payments, direct debits, arrears, and lapses |
| **CRM** | CRMProcess | Interactions, cases, and complaints triggered by lifecycle events |
| **Communications** | CommunicationProcess | Transactional and marketing communications with response tracking |
| **Digital Behavior** | DigitalBehaviorProcess | Web sessions and digital events |
| **Surveys** | SurveyProcess | NPS/CSAT surveys with LLM-enriched context for realistic responses |
| **Next Best Action** | NBAActionProcess | Retention, upsell, cross-sell recommendations with behavioral effects |

### Fraud Generation
- **9 distinct fraud types** based on published healthcare fraud research (NHCAA, APRA)
- Configurable fraud rates with labeled data for ML training
- Disabled by default; see [Fraud Generation](#fraud-generation) for details

### Regulatory Compliance
- **Lifetime Health Cover (LHC)**: 2% loading per year over age 30 at entry (max 70%)
- **Age-Based Discount**: Up to 10% for members aged 18-29
- **PHI Rebate**: Income-tested rebate tiers with age bracket adjustments
- **Medicare Levy Surcharge (MLS)**: Retention factor in churn model
- **Waiting Periods**: Standard 2/6/12-month and transfer-aware waiting periods

### Database
- **PostgreSQL COPY** batch writing (10-100x faster than INSERT)
- Dependency-ordered flushing (parents before children)
- Optional CDC replication slot for change data capture

## Quick Start

### Prerequisites
- Python 3.12+
- Docker (for PostgreSQL)

### 1. Clone and install

```bash
git clone https://github.com/brickwell-health/simulator.git
cd simulator

python -m venv .venv
source .venv/bin/activate

pip install -e .          # Core dependencies
pip install -e ".[dev]"   # Optional: adds pytest, ruff, mypy
```

### 2. Start PostgreSQL

```bash
docker compose up -d
```

### 3. Configure and initialize

```bash
cp config/simulation.yaml.example config/simulation.yaml   # Create config from example
brickwell -c config/simulation.yaml init-db                 # Initialize database schema
```

Edit `config/simulation.yaml` to adjust target member count, simulation dates, and other parameters.

### 4. Run the simulation

```bash
brickwell -c config/simulation.yaml run
```

That's it. The simulator will create members, policies, claims, billing, and all other domains based on your configuration. Output is written directly to PostgreSQL.

### 5. Query results

```bash
docker exec -it brickwell_health_db psql -U brickwell -d brickwell_health

-- Check what was generated
SELECT COUNT(*) FROM member;
SELECT COUNT(*) FROM policy;
SELECT COUNT(*) FROM claim;
```

## CLI Reference

```
brickwell [OPTIONS] COMMAND [ARGS]
```

**Global options:**

| Flag | Description |
|------|-------------|
| `-c, --config PATH` | Path to configuration YAML file |
| `-v, --verbose` | Enable debug-level logging |
| `--json-logs` | Output structured JSON logs |

**Commands:**

| Command | Description |
|---------|-------------|
| `run` | Run the simulation |
| `init-db` | Initialize database schema |
| `validate-config` | Validate configuration file |
| `status` | Check simulation and database status |
| `process-surveys` | Process pending surveys with LLM |

### `run`

```bash
brickwell run [OPTIONS]
```

| Flag | Description |
|------|-------------|
| `-w, --workers N` | Number of worker processes |
| `--sequential` | Run workers sequentially (debugging) |
| `--resume` | Resume from last checkpoint |
| `--extend-days N` | Extend simulation by N days (requires `--resume`) |
| `--end-date DATE` | Override end date (YYYY-MM-DD) |

### `init-db`

```bash
brickwell init-db [OPTIONS]
```

| Flag | Description |
|------|-------------|
| `--drop-existing` | Drop all tables before creating (interactive confirmation) |
| `--enable-cdc` | Create CDC replication slot (requires `wal_level=logical`) |

### `process-surveys`

```bash
brickwell process-surveys [OPTIONS]
```

| Flag | Description |
|------|-------------|
| `--batch-size N` | Surveys per LLM batch |
| `--dry-run` | Preview without making changes |

## Configuration

Configuration is managed via YAML with Pydantic validation. Copy the example to get started:

```bash
cp config/simulation.yaml.example config/simulation.yaml
```

### Key Sections

| Section | Description |
|---------|-------------|
| `simulation` | Time boundaries (`start_date`, `end_date`, `warmup_days`) |
| `scale` | Target population (`target_member_count`, `target_growth_rate`, `target_churn_rate`) |
| `acquisition` | Channel distribution, approval rates |
| `policy` | Policy type and tier distributions |
| `claims` | Claim frequencies, amounts, approval rates |
| `billing` | Payment processing, arrears thresholds, lapse timing |
| `events` | Lifecycle event rates (upgrade, downgrade, cancellation) |
| `member_lifecycle` | Demographic change rates (address, phone, death) |
| `crm` | CRM interaction and case generation |
| `communication` | Marketing campaigns and response tracking |
| `digital` | Web session and digital event generation |
| `survey` | NPS/CSAT survey triggers and response rates |
| `nba` | Next Best Action catalog and execution rules |
| `fraud` | Fraud injection rates and type weights (disabled by default) |
| `database` | PostgreSQL connection settings and batch size |
| `parallel` | Worker count and checkpoint interval |
| `llm` | Databricks AI configuration for survey processing |

### Minimal Example

```yaml
simulation:
  start_date: "2020-01-01"
  end_date: "2025-12-31"
  warmup_days: 730   # Build initial population before steady-state simulation begins

scale:
  target_member_count: 10000
  target_growth_rate: 0.03
  target_churn_rate: 0.10

database:
  host: localhost
  port: 5432
  database: brickwell_health
  username: brickwell
  password: brickwell_dev
  batch_size: 10000

parallel:
  num_workers: 4
  checkpoint_interval_minutes: 15

seed: 42
```

All other sections have sensible defaults and can be omitted.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLI (brickwell)                         │
├─────────────────────────────────────────────────────────────────┤
│                        ParallelRunner                           │
│  ┌─────────────┐  ┌─────────────┐       ┌─────────────┐        │
│  │  Worker 0   │  │  Worker 1   │  ...  │  Worker N   │        │
│  │             │  │             │       │             │        │
│  │ SimEnv      │  │ SimEnv      │       │ SimEnv      │        │
│  │ SharedState │  │ SharedState │       │ SharedState │        │
│  │ 11 Processes│  │ 11 Processes│       │ 11 Processes│        │
│  │ BatchWriter │  │ BatchWriter │       │ BatchWriter │        │
│  └─────────────┘  └─────────────┘       └─────────────┘        │
├─────────────────────────────────────────────────────────────────┤
│                      PostgreSQL Database                        │
└─────────────────────────────────────────────────────────────────┘
```

Each worker simulates a partition of entities independently. Workers don't communicate during simulation — they write to the database independently using high-performance COPY operations.

### Core Components

| Component | Description |
|-----------|-------------|
| **ParallelRunner** | Orchestrates worker processes and aggregates results |
| **SimulationWorker** | Initializes RNG, processes, and batch writer for a single partition |
| **SimulationEnvironment** | SimPy wrapper with datetime conversion and progress tracking |
| **SharedState** | In-memory state shared between processes within a single worker |
| **BatchWriter** | PostgreSQL COPY writer with in-memory buffering |
| **CheckpointManager** | Periodic state persistence for crash recovery and resume |

### Statistical Models

| Model | Purpose |
|-------|---------|
| **ChurnPredictionModel** | Age-based churn with LHC, MLS, tenure, and behavioral adjustments |
| **ClaimPropensityModel** | Poisson frequency + lognormal/tiered severity distributions |
| **ProductSelectionModel** | Product matching by policy type, state, and member characteristics |
| **ABSDemographicsModel** | Australian age, state, and gender distributions from ABS data |
| **IncomeModel** | Income estimation for PHI rebate tier calculations |

## Domain Model

```
MEMBER ─────────┬──────── POLICY_MEMBER ──────── POLICY
                │                                  │
                │                                  ├── COVERAGE
                │                                  ├── WAITING_PERIOD
                │                                  ├── SUSPENSION
                │                                  ├── INVOICE ──── PAYMENT
                │                                  ├── DIRECT_DEBIT_MANDATE
                │                                  └── NBA_RECOMMENDATION ──── NBA_EXECUTION
                │
                ├──────── CLAIM ─────┬──── CLAIM_LINE
                │                    ├──── HOSPITAL_ADMISSION
                │                    ├──── EXTRAS_CLAIM
                │                    └──── AMBULANCE_CLAIM
                │
                ├──────── CRM_INTERACTION
                ├──────── CRM_CASE ──── CRM_COMPLAINT
                ├──────── COMMUNICATION
                ├──────── WEB_SESSION ──── DIGITAL_EVENT
                └──────── NPS_SURVEY / CSAT_SURVEY
```

### Key Entities

| Entity | Description |
|--------|-------------|
| **Member** | Individual with demographics, contact info, Medicare details |
| **Policy** | Insurance policy with product, tier, premium, effective dates |
| **PolicyMember** | Links member to policy with role (Primary/Partner/Dependent) |
| **Coverage** | Specific coverage type on a policy (Hospital/Extras/Ambulance) |
| **Claim** | Claim header with totals, status, and optional fraud metadata |
| **Invoice / Payment** | Billing documents and payment transactions |
| **Interaction / Case** | CRM interactions and support cases |
| **Communication** | Outbound messages with channel and response tracking |
| **NPS/CSAT Survey** | Feedback surveys with LLM-generated context |
| **NBA Recommendation** | Next Best Action with execution and behavioral effects |

## Fraud Generation

The simulator includes a configurable fraud injection system that generates realistic fraudulent claims for ML model training and fraud detection testing. **Fraud is disabled by default.**

### Enabling Fraud

```yaml
fraud:
  enabled: true
  fraud_rate: 0.06                    # 6% of claims are fraudulent
  fraud_prone_member_rate: 0.03       # 3% of members are fraud-prone
  fraud_prone_provider_rate: 0.02     # 2% of providers are fraud-prone
  fraud_prone_claim_multiplier: 5.0   # Fraud-prone entities have 5x higher fraud rate
```

### 9 Fraud Types

| Type | Weight | Description |
|------|--------|-------------|
| **DRG Upcoding** | 25% | Shifts hospital claims to higher complexity DRG codes (1.3-1.7x) |
| **Extras Upcoding** | 15% | Inflates extras service charges (20-150% increase) |
| **Phantom Billing** | 10% | Bills for services never rendered, including fraud ring patterns |
| **Provider Outlier** | 20% | Abnormal claim frequency (2-3x) and amount inflation |
| **Unbundling** | 8% | Splits single services into 2-3 fragments with total inflation |
| **Exact Duplicate** | 6% | Identical claim submitted twice (7-30 day delay) |
| **Near Duplicate** | 6% | Slightly modified duplicate (+/-5% amount, shifted dates) |
| **Temporal Anomaly** | 5% | Claims on public holidays, impossible timelines |
| **Geographic Anomaly** | 5% | Service location inconsistent with member/provider geography |

All fraud claims are labeled with `is_fraudulent=true`, `fraud_type`, and detailed metadata for supervised learning.

See [docs/fraud_implementation.md](docs/fraud_implementation.md) for full configuration and methodology.

## Next Best Action (NBA)

The NBA system generates personalized marketing actions across the customer lifecycle, creating realistic recommendation-to-execution data pipelines.

### How It Works

1. **Catalog**: Predefined actions across 5 categories (Retention, Upsell, Cross-Sell, Service, Wellness)
2. **Recommendations**: Generated based on policy events (churn risk, anniversary, claim patterns)
3. **Execution**: Delivered via Email, SMS, Phone, In-App, Letter, or Web
4. **Behavioral Effects**: Retention and upsell actions create multipliers that influence churn and upgrade probabilities in the PolicyLifecycleProcess

### Configuration

```yaml
nba:
  enabled: true
  contact_policy:
    cooldown_days: 7
    max_actions_per_month: 4
```

### Generated Tables

| Table | Description |
|-------|-------------|
| `nba_action_catalog` | Master catalog of available actions |
| `nba_recommendation` | Recommendations with status tracking |
| `nba_execution` | Execution records with response and outcome |

## LLM Survey Processing

The simulator generates NPS and CSAT survey records with rich contextual data (member demographics, claim history, interaction history, trigger events). These can be enriched with LLM-generated responses using Databricks `ai_query`.

### Two-Phase Approach

1. **During simulation**: Creates pending survey records with `llm_context` JSON
2. **Post-simulation**: Run `brickwell process-surveys` to generate realistic scores, sentiment, and feedback text via Databricks `ai_query`

Set `process_after_simulation: true` to automatically trigger LLM processing when the simulation finishes, or leave it `false` and run `brickwell process-surveys` manually. Without LLM processing, pending surveys remain in the database with context but no generated responses.

### Configuration

```yaml
llm:
  model: "databricks-meta-llama-3-1-70b-instruct"
  batch_size: 50
  process_after_simulation: false   # Set true to auto-process after simulation
  databricks:
    host: "your-workspace.cloud.databricks.com"
    http_path: "/sql/1.0/warehouses/your-warehouse-id"
    token: ""  # Or set DATABRICKS_TOKEN env var
```

## Performance

### Benchmarks (M1 MacBook Pro, 10 cores)

| Scenario | Workers | Members | Duration | Time |
|----------|---------|---------|----------|------|
| Small | 4 | 10,000 | 5 years | ~30s |
| Medium | 8 | 100,000 | 5 years | ~5min |
| Large | 16 | 1,000,000 | 5 years | ~1hr |

### Tips

- Scale workers with available CPU cores (`--workers N`)
- Increase `database.batch_size` for fewer flushes (trades memory for speed)
- Use `--sequential` mode for debugging
- Delete checkpoints for a clean start: `rm -rf data/checkpoints/`

## Testing

```bash
pytest                                          # Run all tests
pytest --cov=brickwell_health --cov-report=html # With coverage
pytest tests/unit/test_claims_generator.py      # Specific module
pytest -n auto                                  # Parallel execution
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit changes (`git commit -m 'Add your feature'`)
4. Push to branch (`git push origin feature/your-feature`)
5. Open a Pull Request

**Code standards:** Python 3.12+ type hints, [Ruff](https://docs.astral.sh/ruff/) for formatting/linting, [MyPy](https://mypy-lang.org/) for type checking.

## License

MIT License - see [LICENSE](LICENSE) for details.
