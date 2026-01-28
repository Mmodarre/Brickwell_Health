"""
YAML configuration loader for Brickwell Health Simulator.

Loads configuration from YAML files with environment variable substitution.
"""

import os
import re
from pathlib import Path
from typing import Any

import yaml

from brickwell_health.config.models import SimulationConfig


def _substitute_env_vars(value: Any) -> Any:
    """
    Recursively substitute environment variables in configuration values.

    Supports ${VAR_NAME} and ${VAR_NAME:-default} syntax.
    """
    if isinstance(value, str):
        # Pattern matches ${VAR_NAME} or ${VAR_NAME:-default}
        pattern = r"\$\{([^}:]+)(?::-([^}]*))?\}"

        def replace(match: re.Match) -> str:
            var_name = match.group(1)
            default = match.group(2) if match.group(2) is not None else ""
            return os.environ.get(var_name, default)

        return re.sub(pattern, replace, value)
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_substitute_env_vars(item) for item in value]
    return value


def load_yaml(path: Path) -> dict[str, Any]:
    """
    Load a YAML file and substitute environment variables.

    Args:
        path: Path to the YAML file

    Returns:
        Dictionary with configuration values

    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        yaml.YAMLError: If the YAML is invalid
    """
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path) as f:
        raw_config = yaml.safe_load(f)

    if raw_config is None:
        return {}

    return _substitute_env_vars(raw_config)


def load_config(
    config_path: str | Path | None = None,
    override_values: dict[str, Any] | None = None,
) -> SimulationConfig:
    """
    Load simulation configuration from a YAML file.

    Args:
        config_path: Path to configuration YAML file. If None, looks for
                    config/simulation.yaml in the current directory.
        override_values: Dictionary of values to override after loading

    Returns:
        Validated SimulationConfig object

    Raises:
        FileNotFoundError: If configuration file not found
        ValidationError: If configuration is invalid
    """
    if config_path is None:
        # Look for default config locations
        default_paths = [
            Path("config/simulation.yaml"),
            Path("simulation.yaml"),
            Path.home() / ".brickwell" / "simulation.yaml",
        ]
        for path in default_paths:
            if path.exists():
                config_path = path
                break
        else:
            raise FileNotFoundError(
                "No configuration file found. Searched: "
                f"{', '.join(str(p) for p in default_paths)}"
            )

    config_path = Path(config_path)
    config_dict = load_yaml(config_path)

    # Apply overrides
    if override_values:
        config_dict = _deep_merge(config_dict, override_values)

    return SimulationConfig(**config_dict)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """
    Deep merge two dictionaries, with override taking precedence.

    Args:
        base: Base dictionary
        override: Dictionary with values to override

    Returns:
        Merged dictionary
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value

    return result
