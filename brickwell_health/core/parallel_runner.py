"""
Parallel runner for Brickwell Health Simulator.

Orchestrates multiple worker processes for parallel simulation.
"""

import multiprocessing
import time
from typing import Any

import structlog

from brickwell_health.config.models import SimulationConfig
from brickwell_health.core.worker import run_worker


logger = structlog.get_logger()


class ParallelRunner:
    """
    Orchestrates parallel simulation across multiple worker processes.

    Uses multiprocessing to run workers in parallel, each handling
    a partition of entities.

    Usage:
        runner = ParallelRunner(config)
        results = runner.run()
    """

    def __init__(self, config: SimulationConfig, log_level: str = "INFO"):
        """
        Initialize the parallel runner.

        Args:
            config: Simulation configuration
            log_level: Log level for worker processes
        """
        self.config = config
        self.num_workers = config.parallel.num_workers
        self.log_level = log_level

    def run(self) -> dict[str, Any]:
        """
        Run the simulation with parallel workers.

        Returns:
            Aggregated results from all workers
        """
        logger.info(
            "parallel_run_starting",
            num_workers=self.num_workers,
            start_date=self.config.simulation.start_date.isoformat(),
            end_date=self.config.simulation.end_date.isoformat(),
        )

        start_time = time.time()

        # Create worker arguments
        worker_args = [
            (self.config, worker_id, self.num_workers, self.log_level)
            for worker_id in range(self.num_workers)
        ]

        # Run workers in parallel
        with multiprocessing.Pool(self.num_workers) as pool:
            results = pool.starmap(run_worker, worker_args)

        # Aggregate results
        elapsed = time.time() - start_time
        aggregated = self._aggregate_results(results, elapsed)

        logger.info(
            "parallel_run_completed",
            elapsed_seconds=f"{elapsed:.1f}",
            total_days=aggregated["total_simulation_days"],
            avg_days_per_second=f"{aggregated['avg_days_per_second']:.1f}",
        )

        return aggregated

    def run_sequential(self) -> dict[str, Any]:
        """
        Run simulation sequentially (for debugging).

        Returns:
            Aggregated results
        """
        logger.info(
            "sequential_run_starting",
            num_workers=self.num_workers,
        )

        start_time = time.time()

        results = []
        for worker_id in range(self.num_workers):
            result = run_worker(self.config, worker_id, self.num_workers, self.log_level)
            results.append(result)

        elapsed = time.time() - start_time
        aggregated = self._aggregate_results(results, elapsed)

        logger.info(
            "sequential_run_completed",
            elapsed_seconds=f"{elapsed:.1f}",
        )

        return aggregated

    def _aggregate_results(
        self,
        results: list[dict[str, Any]],
        total_elapsed: float,
    ) -> dict[str, Any]:
        """
        Aggregate results from all workers.

        Args:
            results: List of worker result dictionaries
            total_elapsed: Total elapsed time

        Returns:
            Aggregated statistics
        """
        # Sum database write counts
        total_writes: dict[str, int] = {}
        for result in results:
            for table, count in result.get("database_writes", {}).items():
                total_writes[table] = total_writes.get(table, 0) + count

        # Sum acquisition stats
        total_applications = sum(
            r.get("acquisition_stats", {}).get("applications_submitted", 0)
            for r in results
        )
        total_approved = sum(
            r.get("acquisition_stats", {}).get("applications_approved", 0)
            for r in results
        )
        total_members = sum(
            r.get("acquisition_stats", {}).get("members_created", 0)
            for r in results
        )
        total_policies = sum(
            r.get("acquisition_stats", {}).get("policies_created", 0)
            for r in results
        )

        # Calculate aggregate metrics
        total_days = results[0].get("simulation_days", 0) if results else 0
        avg_days_per_second = total_days / total_elapsed if total_elapsed > 0 else 0

        return {
            "num_workers": self.num_workers,
            "total_elapsed_seconds": total_elapsed,
            "total_simulation_days": total_days,
            "avg_days_per_second": avg_days_per_second,
            "database_writes": total_writes,
            "acquisition": {
                "applications_submitted": total_applications,
                "applications_approved": total_approved,
                "members_created": total_members,
                "policies_created": total_policies,
                "approval_rate": total_approved / total_applications if total_applications > 0 else 0,
            },
            "worker_results": results,
        }
