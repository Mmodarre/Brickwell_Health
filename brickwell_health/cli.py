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
@click.pass_context
def run(ctx, workers, sequential):
    """Run the simulation."""
    from brickwell_health.core.parallel_runner import ParallelRunner

    config_path = ctx.obj.get("config_path")

    try:
        config = load_config(config_path)

        # Override workers if specified
        if workers is not None:
            config.parallel.num_workers = workers

        # Validate configuration
        warnings = validate_config(config)
        for warning in warnings:
            click.echo(f"Warning: {warning}", err=True)

        click.echo(f"Starting simulation with {config.parallel.num_workers} workers...")
        click.echo(f"  Start date: {config.simulation.start_date}")
        click.echo(f"  End date: {config.simulation.end_date}")
        click.echo(f"  Target members: {config.scale.target_member_count:,}")

        log_level = ctx.obj.get("log_level", "INFO")
        runner = ParallelRunner(config, log_level=log_level)

        if sequential:
            results = runner.run_sequential()
        else:
            results = runner.run()

        # Print summary
        click.echo("\n=== Simulation Complete ===")
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
    import importlib.util
    from pathlib import Path
    
    config_path = ctx.obj.get("config_path")

    try:
        config = load_config(config_path)

        click.echo(f"Initializing database: {config.database.database}")
        click.echo(f"  Host: {config.database.host}:{config.database.port}")

        if drop_existing:
            if not click.confirm("This will drop ALL existing tables. Continue?"):
                click.echo("Aborted.")
                return

        # Load init_db module from scripts directory
        scripts_path = Path(__file__).parent.parent / "scripts" / "init_db.py"
        spec = importlib.util.spec_from_file_location("init_db", scripts_path)
        init_db_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(init_db_module)
        
        init_db_module.init_database(config_path, drop_existing, enable_cdc)

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


if __name__ == "__main__":
    main()
