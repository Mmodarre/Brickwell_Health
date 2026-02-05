"""
Command-line interface for Brickwell Health Simulator.

Provides commands for running simulations, initializing database, etc.
"""

import sys
from pathlib import Path

import click
import structlog

from brickwell_health.config import load_config, validate_config
from brickwell_health.utils.logging import configure_logging


logger = structlog.get_logger()


@click.group()
@click.option(
    "--config", "-c",
    type=click.Path(exists=True),
    help="Path to configuration file",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.option(
    "--json-logs",
    is_flag=True,
    help="Output logs as JSON",
)
@click.pass_context
def main(ctx, config, verbose, json_logs):
    """Brickwell Health Private Health Insurance Simulator."""
    ctx.ensure_object(dict)

    # Configure logging
    log_level = "DEBUG" if verbose else "INFO"
    configure_logging(level=log_level, json_output=json_logs)

    # Store config path and log level in context
    ctx.obj["config_path"] = config
    ctx.obj["log_level"] = log_level


@main.command()
@click.option(
    "--workers", "-w",
    type=int,
    default=None,
    help="Number of worker processes (default: from config)",
)
@click.option(
    "--sequential",
    is_flag=True,
    help="Run workers sequentially (for debugging)",
)
@click.option(
    "--resume",
    is_flag=True,
    help="Resume from last checkpoint instead of fresh start",
)
@click.option(
    "--extend-days",
    type=int,
    default=None,
    help="Extend simulation by N days beyond checkpoint date (requires --resume)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Override end date (format: YYYY-MM-DD)",
)
@click.pass_context
def run(ctx, workers, sequential, resume, extend_days, end_date):
    """Run the simulation.
    
    Examples:
    
    \b
    # Fresh simulation
    brickwell run
    
    \b
    # Resume from checkpoint
    brickwell run --resume
    
    \b
    # Resume and extend by 30 days
    brickwell run --resume --extend-days 30
    
    \b
    # Resume to specific end date
    brickwell run --resume --end-date 2025-06-30
    """
    from datetime import timedelta
    from brickwell_health.core.parallel_runner import ParallelRunner
    from brickwell_health.core.checkpoint_v2 import (
        CheckpointManagerV2, 
        CheckpointNotFoundError,
        get_checkpoint_dates,
    )
    from pathlib import Path

    config_path = ctx.obj.get("config_path")

    try:
        config = load_config(config_path)

        # Override workers if specified
        if workers is not None:
            config.parallel.num_workers = workers

        # Handle --extend-days (requires --resume)
        if extend_days is not None and not resume:
            click.echo("Error: --extend-days requires --resume flag", err=True)
            sys.exit(1)

        # Handle end date override
        if end_date is not None:
            config.simulation.end_date = end_date.date()
        elif extend_days is not None and resume:
            # Get checkpoint date and calculate new end date
            checkpoint_dir = Path(config.reference_data_path).parent / "checkpoints"
            checkpoint_mgr = CheckpointManagerV2(checkpoint_dir)
            
            # Load checkpoint to get date (use worker 0 as reference)
            checkpoint = checkpoint_mgr.load_checkpoint(0)
            if checkpoint is None:
                raise CheckpointNotFoundError(
                    "No checkpoint found for worker 0. Cannot determine checkpoint date."
                )
            
            checkpoint_date, _ = get_checkpoint_dates(checkpoint)
            new_end_date = checkpoint_date + timedelta(days=extend_days)
            config.simulation.end_date = new_end_date
            
            click.echo(f"  Extending from checkpoint date {checkpoint_date} by {extend_days} days")

        # Validate configuration
        warnings = validate_config(config)
        for warning in warnings:
            click.echo(f"Warning: {warning}", err=True)

        mode = "resume" if resume else "fresh"
        click.echo(f"Starting simulation with {config.parallel.num_workers} workers...")
        click.echo(f"  Mode: {mode}")
        if not resume:
            click.echo(f"  Start date: {config.simulation.start_date}")
        click.echo(f"  End date: {config.simulation.end_date}")
        click.echo(f"  Target members: {config.scale.target_member_count:,}")

        log_level = ctx.obj.get("log_level", "INFO")
        runner = ParallelRunner(config, log_level=log_level)

        if sequential:
            results = runner.run_sequential(resume=resume)
        else:
            results = runner.run(resume=resume)

        # Print summary
        click.echo("\n=== Simulation Complete ===")
        click.echo(f"Mode: {results.get('mode', 'fresh')}")
        click.echo(f"Elapsed time: {results['total_elapsed_seconds']:.1f} seconds")
        click.echo(f"Simulation speed: {results['avg_days_per_second']:.1f} days/second")
        click.echo(f"\nAcquisition:")
        click.echo(f"  Applications: {results['acquisition']['applications_submitted']:,}")
        click.echo(f"  Approved: {results['acquisition']['applications_approved']:,}")
        click.echo(f"  Members created: {results['acquisition']['members_created']:,}")
        click.echo(f"  Policies created: {results['acquisition']['policies_created']:,}")

        click.echo(f"\nDatabase writes:")
        for table, count in sorted(results['database_writes'].items()):
            if count > 0:
                click.echo(f"  {table}: {count:,}")

    except CheckpointNotFoundError as e:
        click.echo(f"Checkpoint error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception("simulation_failed", error=str(e))
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.command("init-db")
@click.option(
    "--drop-existing",
    is_flag=True,
    help="Drop existing tables before creating",
)
@click.option(
    "--enable-cdc",
    is_flag=True,
    help="Create CDC replication slot for change data capture (requires wal_level=logical)",
)
@click.pass_context
def init_db(ctx, drop_existing, enable_cdc):
    """Initialize the database schema."""
    from brickwell_health.db.initialize import init_database
    
    config_path = ctx.obj.get("config_path")

    try:
        config = load_config(config_path)

        click.echo(f"Initializing database: {config.database.database}")
        click.echo(f"  Host: {config.database.host}:{config.database.port}")

        if drop_existing:
            if not click.confirm("This will drop ALL existing tables. Continue?"):
                click.echo("Aborted.")
                return

        init_database(config_path, drop_existing, enable_cdc)

        click.echo("Database initialized successfully.")
        if enable_cdc:
            click.echo("CDC replication slot created (cdc_slot).")

    except Exception as e:
        logger.exception("init_db_failed", error=str(e))
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.command("validate-config")
@click.pass_context
def validate_config_cmd(ctx):
    """Validate the configuration file."""
    from brickwell_health.config.validation import ConfigurationError

    config_path = ctx.obj.get("config_path")

    try:
        config = load_config(config_path)
        warnings = validate_config(config)

        click.echo("Configuration is valid.")

        if warnings:
            click.echo("\nWarnings:")
            for warning in warnings:
                click.echo(f"  - {warning}")

    except ConfigurationError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.command()
@click.pass_context
def status(ctx):
    """Check simulation and database status."""
    from brickwell_health.config.validation import validate_database_connection

    config_path = ctx.obj.get("config_path")

    try:
        config = load_config(config_path)

        click.echo("Configuration:")
        click.echo(f"  Config file: {config_path or 'default'}")
        click.echo(f"  Start date: {config.simulation.start_date}")
        click.echo(f"  End date: {config.simulation.end_date}")
        click.echo(f"  Target members: {config.scale.target_member_count:,}")
        click.echo(f"  Workers: {config.parallel.num_workers}")

        click.echo("\nDatabase:")
        click.echo(f"  Host: {config.database.host}:{config.database.port}")
        click.echo(f"  Database: {config.database.database}")

        try:
            validate_database_connection(config)
            click.echo("  Status: Connected")
        except Exception as e:
            click.echo(f"  Status: Not connected ({e})")

        click.echo("\nReference data:")
        ref_path = Path(config.reference_data_path)
        if ref_path.exists():
            files = list(ref_path.glob("*.json"))
            click.echo(f"  Path: {ref_path}")
            click.echo(f"  Files: {len(files)} JSON files")
        else:
            click.echo(f"  Path: {ref_path} (NOT FOUND)")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.command("process-surveys")
@click.option(
    "--batch-size",
    type=int,
    default=None,
    help="Number of surveys per batch (default: from config)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be processed without making changes",
)
@click.pass_context
def process_surveys(ctx, batch_size, dry_run):
    """Process pending surveys with LLM.
    
    This command processes pending NPS and CSAT surveys using Databricks ai_query
    to generate realistic survey responses based on member context.
    
    It can be used to:
    - Manually trigger LLM processing after simulation
    - Retry failed surveys
    - Process surveys when llm.process_after_simulation is false
    
    Examples:
    
    \b
    # Process all pending surveys
    brickwell process-surveys
    
    \b
    # Dry run to see what would be processed
    brickwell process-surveys --dry-run
    
    \b
    # Process with custom batch size
    brickwell process-surveys --batch-size 100
    """
    from brickwell_health.core.llm_processor import LLMSurveyProcessor

    config_path = ctx.obj.get("config_path")

    try:
        config = load_config(config_path)

        # Override batch size if specified
        if batch_size is not None:
            config.llm.batch_size = batch_size

        # Check if LLM is configured
        if not config.llm.databricks.is_configured():
            click.echo(
                "Error: Databricks credentials not configured. "
                "Set llm.databricks.host, llm.databricks.token, and llm.databricks.http_path "
                "in config or via environment variables.",
                err=True,
            )
            sys.exit(1)

        click.echo("Processing pending surveys...")
        click.echo(f"  Databricks host: {config.llm.databricks.host}")
        click.echo(f"  LLM model: {config.llm.model}")
        click.echo(f"  Batch size: {config.llm.batch_size}")
        if dry_run:
            click.echo("  Mode: DRY RUN (no changes will be made)")

        processor = LLMSurveyProcessor(config, dry_run=dry_run)
        stats = processor.process_all()

        # Print summary
        click.echo("\n=== Processing Complete ===")
        click.echo(f"NPS surveys processed: {stats['nps_processed']}")
        click.echo(f"NPS surveys responded: {stats['nps_responded']}")
        click.echo(f"CSAT surveys processed: {stats['csat_processed']}")
        click.echo(f"CSAT surveys responded: {stats['csat_responded']}")
        click.echo(f"LLM batch calls: {stats['llm_calls']}")
        click.echo(f"Errors: {stats['errors']}")

    except Exception as e:
        logger.exception("process_surveys_failed", error=str(e))
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
