# Brickwell Health Simulator

Australian Private Health Insurance (PHI) discrete event simulation for generating realistic transactional data.

## Overview

Brickwell Health Simulator is a comprehensive simulation engine that generates realistic private health insurance data for an Australian health fund. It uses discrete event simulation (SimPy) to model the complete policy lifecycle including:

- Member acquisition and application processing
- Policy management (upgrades, downgrades, suspensions, cancellations)
- Claims generation (hospital, extras, ambulance)
- Billing and payment processing
- Australian regulatory compliance (LHC, MLS, PHI rebates)

The simulator produces production-quality transactional data suitable for:
- Data warehouse development and testing
- Analytics and reporting system validation
- Machine learning model training
- Performance testing of insurance administration systems

## Features

### Simulation Engine
- **Discrete Event Simulation**: Built on SimPy for accurate time-based event modeling
- **Parallel Processing**: Multi-worker architecture with configurable parallelism
- **Deterministic Reproducibility**: Seed-based random number generation for reproducible results
- **Checkpoint Recovery**: Periodic checkpointing for crash recovery

### Data Generation
- **Realistic Demographics**: Age-appropriate member generation using ABS demographic distributions
- **Product Selection**: Intelligent product matching based on policy type, state, and member characteristics
- **APRA-Validated Claims**: Claim frequencies and amounts calibrated to APRA 2024-2025 statistics
- **Lognormal Hospital Claims**: 8.8% high-value claims (>$10k) with tiered distribution

### Regulatory Compliance
- **Lifetime Health Cover (LHC)**: Age-of-entry based loading calculations
- **Age-Based Discount**: Discounts for members joining aged 18-29
- **PHI Rebate**: Income-tested rebate tiers with age bracket adjustments
- **Medicare Levy Surcharge (MLS)**: Retention factor in churn model
- **Waiting Periods**: Standard (2/6/12 month) and transfer-aware waiting periods

### Churn Modeling
- **Age-Based Rates**: Research-validated churn rates (22% young adults → 3% elderly)
- **Retention Factors**: LHC lock-in, MLS avoidance, tenure-based retention
- **Behavioral Triggers**: Life events, claim denials, perceived value assessment
- **Cancellation Reasons**: Weighted sampling based on policy conditions

### Database Integration
- **PostgreSQL COPY**: High-performance batch writing (10-100x faster than INSERT)
- **Dependency-Ordered Flushing**: Automatic parent-before-child table writes
- **Buffer Management**: In-memory buffering with configurable batch sizes

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLI (brickwell)                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                            ParallelRunner                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  Worker 0   │  │  Worker 1   │  │  Worker 2   │  │  Worker N   │        │
│  │             │  │             │  │             │  │             │        │
│  │ SimEnv      │  │ SimEnv      │  │ SimEnv      │  │ SimEnv      │        │
│  │ Processes   │  │ Processes   │  │ Processes   │  │ Processes   │        │
│  │ BatchWriter │  │ BatchWriter │  │ BatchWriter │  │ BatchWriter │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
├─────────────────────────────────────────────────────────────────────────────┤
│                            PostgreSQL Database                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | Description |
|-----------|-------------|
| **SimulationEnvironment** | SimPy wrapper with datetime conversion and progress tracking |
| **SimulationWorker** | Orchestrates processes within a single worker |
| **SharedState** | In-memory state shared between processes (policies, members, invoices) |
| **BatchWriter** | High-performance PostgreSQL COPY writer with buffering |
| **CheckpointManager** | Periodic state persistence for crash recovery |

### Simulation Processes

| Process | Responsibility |
|---------|----------------|
| **AcquisitionProcess** | Generates applications, creates policies and members |
| **PolicyLifecycleProcess** | Handles upgrades, downgrades, cancellations |
| **SuspensionProcess** | Manages overseas travel and hardship suspensions |
| **ClaimsProcess** | Generates hospital, extras, and ambulance claims |
| **BillingProcess** | Creates invoices, processes payments, manages arrears |

### Statistical Models

| Model | Purpose |
|-------|---------|
| **ChurnPredictionModel** | Age-based churn with log-odds adjustments |
| **ClaimPropensityModel** | Poisson frequency, lognormal severity |
| **ProductSelectionModel** | Intelligent product matching |
| **ABSDemographicsModel** | Australian age/state/gender distributions |
| **IncomeModel** | Income estimation for rebate calculations |

## Installation

### Requirements
- Python 3.12+
- PostgreSQL 14+

### Install from Source

```bash
# Clone repository
git clone https://github.com/brickwell-health/simulator.git
cd simulator

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# Install package
pip install -e .

# For development
pip install -e ".[dev]"
```

### Dependencies

**Core:**
- `simpy>=4.1.1` - Discrete event simulation
- `sqlalchemy>=2.0.25` - Database abstraction
- `psycopg[binary]>=3.1.17` - PostgreSQL adapter
- `pydantic>=2.5.3` - Data validation
- `numpy>=1.26.3` - Numerical operations
- `faker>=22.0.0` - Realistic data generation

**Utilities:**
- `click>=8.1.7` - CLI framework
- `structlog>=24.1.0` - Structured logging
- `pyyaml>=6.0.1` - Configuration parsing

### Docker Setup (PostgreSQL)

The project includes a `docker-compose.yml` for quickly spinning up a PostgreSQL database.

#### Start the Database

```bash
# Start PostgreSQL in the background
docker-compose up -d

# Check container status
docker-compose ps

# View logs
docker-compose logs -f postgres
```

#### Connection Details

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `brickwell_health` |
| Username | `brickwell` |
| Password | `brickwell_dev` |

#### Database Management

```bash
# Connect to database via psql
docker exec -it brickwell_health_db psql -U brickwell -d brickwell_health

# Stop the database (preserves data)
docker-compose stop

# Start again
docker-compose start

# Stop and remove container (preserves data in volume)
docker-compose down

# Stop and remove container AND data volume (full reset)
docker-compose down -v

# Rebuild container (after postgres version changes)
docker-compose up -d --build
```

#### Initialize Schema

Once the database is running, initialize the schema:

```bash
# Initialize database tables
brickwell -c config/simulation.yaml init-db

# Or drop and recreate (for fresh start)
brickwell -c config/simulation.yaml init-db --drop-existing
```

#### Health Check

The container includes a health check that verifies PostgreSQL is ready:

```bash
# Check health status
docker inspect --format='{{.State.Health.Status}}' brickwell_health_db
# Should return: healthy
```

#### Data Persistence

Data is persisted in a Docker volume named `brickwell_pgdata`. This means:
- Data survives container restarts (`docker-compose stop/start`)
- Data survives container removal (`docker-compose down`)
- Data is only deleted with `docker-compose down -v`

To backup the volume:

```bash
# Backup
docker run --rm -v brickwell_pgdata:/data -v $(pwd):/backup alpine \
  tar czf /backup/brickwell_pgdata_backup.tar.gz -C /data .

# Restore
docker run --rm -v brickwell_pgdata:/data -v $(pwd):/backup alpine \
  tar xzf /backup/brickwell_pgdata_backup.tar.gz -C /data
```

## Configuration

Configuration is managed via YAML files with Pydantic validation.

### Example Configuration

```yaml
simulation:
  start_date: "2020-01-01"
  end_date: "2025-12-31"
  warmup_days: 730  # 2 years to build population

scale:
  target_member_count: 100000
  target_growth_rate: 0.03  # 3% annual growth
  target_churn_rate: 0.10   # 10% annual churn

acquisition:
  channels:
    Online: 0.45
    Phone: 0.25
    Broker: 0.20
    Corporate: 0.10
  approval_rate: 0.92

policy:
  type_distribution:
    Single: 0.35
    Couple: 0.25
    Family: 0.30
    SingleParent: 0.10
  tier_distribution:
    Gold: 0.20
    Silver: 0.35
    Bronze: 0.30
    Basic: 0.15

claims:
  hospital_frequency:
    "18-30": 0.3
    "31-45": 0.5
    "46-60": 1.2
    "61-70": 2.0
    "71+": 2.5
  high_claim_probability: 0.088
  approval:
    hospital_approval_rate: 0.98
    extras_approval_rate: 0.92
    ambulance_approval_rate: 0.95

billing:
  final_payment_success_rate: 0.95
  max_debit_retries: 2
  retry_interval_days: 3
  days_to_arrears: 14
  days_to_suspension: 30
  days_to_lapse: 60

database:
  host: localhost
  port: 5432
  database: brickwell_health
  username: brickwell
  password: ""
  batch_size: 10000

parallel:
  num_workers: 4
  checkpoint_interval_minutes: 15

seed: 42
reference_data_path: data/reference
```

### Configuration Sections

| Section | Description |
|---------|-------------|
| `simulation` | Time boundaries and warmup period |
| `scale` | Target population and growth rates |
| `acquisition` | Channel distribution and approval rates |
| `policy` | Policy type and tier distributions |
| `claims` | Claim frequencies, amounts, approval rates |
| `churn` | Churn model parameters |
| `events` | Lifecycle event rates (upgrade/downgrade/suspend) |
| `billing` | Payment processing and arrears thresholds |
| `database` | PostgreSQL connection settings |
| `parallel` | Worker count and checkpoint interval |

## Usage

### CLI Commands

```bash
# Run simulation with default config
brickwell run

# Run with custom config
brickwell -c config/production.yaml run

# Run with specified workers
brickwell run --workers 8

# Run sequentially (for debugging)
brickwell run --sequential

# Initialize database schema
brickwell init-db

# Drop and recreate tables
brickwell init-db --drop-existing

# Validate configuration
brickwell validate-config

# Check simulation status
brickwell status

# Enable verbose logging
brickwell -v run

# JSON log output
brickwell --json-logs run
```

### Programmatic Usage

```python
from brickwell_health.config import load_config
from brickwell_health.core.parallel_runner import ParallelRunner

# Load configuration
config = load_config("config/simulation.yaml")

# Create runner
runner = ParallelRunner(config)

# Run simulation
results = runner.run()

# Access results
print(f"Members created: {results['acquisition']['members_created']:,}")
print(f"Policies created: {results['acquisition']['policies_created']:,}")
print(f"Total claims: {sum(results['database_writes'].get(t, 0) for t in ['claim'])}")
```

## Domain Model

### Entity Relationship Overview

```
MEMBER ─────────┬──────── POLICY_MEMBER ──────── POLICY
                │                                  │
                │                                  ├── COVERAGE
                │                                  ├── WAITING_PERIOD
                │                                  ├── SUSPENSION
                │                                  ├── INVOICE ──── PAYMENT
                │                                  └── DIRECT_DEBIT_MANDATE
                │
                └──────── CLAIM ─────┬──── CLAIM_LINE
                                     ├──── HOSPITAL_ADMISSION ──── MEDICAL_SERVICE
                                     │                         └── PROSTHESIS_CLAIM
                                     ├──── EXTRAS_CLAIM
                                     └──── AMBULANCE_CLAIM
```

### Key Entities

| Entity | Description |
|--------|-------------|
| **Member** | Individual person with demographics, contact, Medicare info |
| **Policy** | Insurance policy with product, tier, premium, effective dates |
| **PolicyMember** | Link between member and policy with role (Primary/Partner/Dependent) |
| **Coverage** | Specific coverage on a policy (Hospital/Extras/Ambulance) |
| **WaitingPeriod** | Waiting period tracking for benefits |
| **Claim** | Claim header with totals and status |
| **HospitalAdmission** | Hospital-specific claim details (DRG, LOS, charges) |
| **ExtrasClaimDetails** | Extras-specific claim details (service type, provider) |
| **Invoice** | Billing invoice with amounts and status |
| **Payment** | Payment transaction record |
| **Suspension** | Policy suspension record (overseas/hardship) |

### Enumerations

| Enum | Values |
|------|--------|
| **PolicyStatus** | Active, Suspended, Cancelled, Lapsed |
| **PolicyType** | Single, Couple, Family, SingleParent |
| **CoverageTier** | Gold, Silver, Bronze, Basic |
| **ClaimType** | Hospital, Extras, Ambulance |
| **ClaimStatus** | Submitted, Assessed, Approved, Rejected, Paid |
| **DenialReason** | NoCoverage, LimitsExhausted, WaitingPeriod, PolicyExclusions, PreExisting |

## Data Generation Logic

### Member Acquisition

1. **Rate Calculation**:
   - Warmup: `target_members / warmup_days / approval_rate / avg_members_per_policy`
   - Steady: `(growth_rate + churn_rate) * policies / 365 / approval_rate`

2. **Application Flow**:
   ```
   Select Channel → Generate Members → Select Product → Create Application
        ↓                                                      ↓
   Decision Time (1-14 days)                           Health Declaration
        ↓
   Approve/Decline
        ↓ (if approved)
   Create Policy → Create Coverages → Create Waiting Periods → Setup Billing
   ```

### Claims Generation

**Hospital Claims (Poisson + Lognormal + High-Value Tiers)**:
```
Frequency by Age:
  18-30: λ=0.3/year
  31-45: λ=0.5/year
  46-60: λ=1.2/year
  61-70: λ=2.0/year
  71+:   λ=2.5/year

Amount Distribution:
  91.2%: Lognormal(μ=8.0, σ=1.5) → median ~$2,981
  8.8%:  High-value tiers:
         - $10k-$20k: 63.4%
         - $20k-$30k: 23.1%
         - $30k-$50k: 7.0%
         - $50k-$100k: 3.1%
         - $100k-$200k: 0.24%
         - $200k-$450k: 0.02%
```

**Extras Claims**:
```
Dental:
  - Preventative: λ=2.0, mean=$175, std=$35
  - General: λ=0.5, mean=$280, std=$90
  - Major: λ=0.1, mean=$1,300, std=$450

Other Services:
  - Optical: λ=0.8, mean=$350
  - Physiotherapy: λ=1.5, mean=$85 (×1.5 for 65+)
  - Chiropractic: λ=1.2, mean=$70
  - Ambulance: λ=0.02, mean=$950
```

### Churn Model

**Age-Based Annual Rates**:
```
18-24: 22%    45-49: 8%     70-74: 3%
25-29: 18%    50-54: 7%     75-79: 3%
30-34: 14%    55-59: 6%     80+:   4%
35-39: 11%    60-64: 5%
40-44: 9%     65-69: 4%
```

**Adjustments**:
- LHC Loading: ×0.80 (20% reduction)
- MLS Subject: ×0.85 (15% reduction)
- 10+ Years Tenure: ×0.80 (20% reduction)
- Q2 Premium Increase: ×1.15 (15% increase)
- No Recent Claims: +0.10 log-odds
- Dissatisfied: +0.15 log-odds
- Life Event: +0.25 log-odds
- High Claims Value: -0.10 log-odds

## Performance

### Benchmarks (M1 MacBook Pro, 10 cores)

| Scenario | Workers | Members | Duration | Time | Speed |
|----------|---------|---------|----------|------|-------|
| Small | 4 | 10,000 | 5 years | ~30s | ~60 days/sec |
| Medium | 8 | 100,000 | 5 years | ~5min | ~6 days/sec |
| Large | 16 | 1,000,000 | 5 years | ~1hr | ~0.5 days/sec |

### Optimization Tips

1. **Increase Workers**: Scale with available CPU cores
2. **Larger Batch Size**: Reduce flush frequency (but increase memory)
3. **SSD Storage**: Database writes benefit from fast I/O
4. **Disable Indexes**: Create indexes after bulk load
5. **Unlogged Tables**: For transient test data

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=brickwell_health --cov-report=html

# Run specific test module
pytest tests/test_claims.py

# Run in parallel
pytest -n auto

# Run with verbose output
pytest -v
```

## Project Structure

```
brickwell_health/
├── __init__.py              # Package metadata
├── cli.py                   # Command-line interface
├── config/
│   ├── loader.py            # YAML configuration loading
│   ├── models.py            # Pydantic configuration models
│   ├── regulatory.py        # Regulatory calculation helpers
│   └── validation.py        # Configuration validation
├── core/
│   ├── environment.py       # SimPy environment wrapper
│   ├── parallel_runner.py   # Multi-worker orchestration
│   ├── worker.py            # Single worker process
│   ├── shared_state.py      # Cross-process state container
│   ├── checkpoint.py        # Checkpoint management
│   ├── partition.py         # Entity partitioning
│   └── processes/
│       ├── base.py          # Base process class
│       ├── acquisition.py   # Member acquisition
│       ├── policy_lifecycle.py  # Upgrades/downgrades/cancellations
│       ├── suspension.py    # Policy suspensions
│       ├── claims.py        # Claims generation
│       └── billing.py       # Invoicing and payments
├── domain/
│   ├── enums.py             # Domain enumerations
│   ├── member.py            # Member models
│   ├── policy.py            # Policy models
│   ├── coverage.py          # Coverage models
│   ├── claims.py            # Claim models
│   ├── billing.py           # Billing models
│   └── application.py       # Application models
├── generators/
│   ├── base.py              # Base generator class
│   ├── member_generator.py  # Member data generation
│   ├── policy_generator.py  # Policy creation
│   ├── coverage_generator.py    # Coverage assignment
│   ├── claims_generator.py  # Claim data generation
│   ├── billing_generator.py # Invoice/payment generation
│   ├── waiting_period_generator.py  # Waiting period creation
│   ├── regulatory_generator.py  # LHC/rebate records
│   ├── application_generator.py  # Application creation
│   └── id_generator.py      # UUID and number generation
├── statistics/
│   ├── churn_model.py       # Churn prediction
│   ├── claim_propensity.py  # Claim frequency/severity
│   ├── product_selection.py # Product matching
│   ├── abs_demographics.py  # Australian demographics
│   ├── income_model.py      # Income estimation
│   └── distributions.py     # Statistical distributions
├── db/
│   ├── connection.py        # Database connection management
│   └── writer.py            # Batch writer with COPY
├── reference/
│   ├── loader.py            # Reference data loading
│   └── models.py            # Reference data models
└── utils/
    ├── calendar.py          # Business day calculations
    ├── logging.py           # Structured logging setup
    └── time_conversion.py   # Date/time utilities
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Style
- Python 3.12+ type hints
- Ruff for linting/formatting
- MyPy for type checking
- Docstrings for all public functions

## Acknowledgments

- APRA Private Health Insurance Statistics
- Australian Bureau of Statistics demographic data
- SimPy discrete event simulation library
