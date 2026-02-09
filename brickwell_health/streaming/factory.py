"""
Factory for creating publisher instances from configuration.
"""

from brickwell_health.config.models import StreamingConfig
from brickwell_health.streaming.publisher import EventPublisher


def create_publisher(config: StreamingConfig, worker_id: int) -> EventPublisher:
    """
    Create a publisher instance based on configuration.

    Args:
        config: Streaming configuration
        worker_id: Worker ID (used for file naming, etc.)

    Returns:
        An EventPublisher implementation
    """
    backend = config.backend.lower()

    if backend == "zerobus":
        from brickwell_health.streaming.implementations.zerobus import ZeroBusPublisher

        zb = config.zerobus
        return ZeroBusPublisher(
            workspace_id=zb.workspace_id,
            workspace_url=zb.workspace_url,
            region=zb.region,
            catalog=zb.catalog,
            schema_name=zb.schema_name,
            tables=config.tables,
            token=zb.token,
            client_id=zb.client_id,
            client_secret=zb.client_secret,
        )

    elif backend == "json_file":
        from brickwell_health.streaming.implementations.json_file import JsonFilePublisher

        return JsonFilePublisher(
            output_dir=config.json_file_output_dir,
            worker_id=worker_id,
        )

    elif backend == "log":
        from brickwell_health.streaming.implementations.log import LogPublisher

        return LogPublisher(level=config.log_level)

    else:
        # noop or unknown
        from brickwell_health.streaming.implementations.noop import NoopPublisher

        return NoopPublisher()
