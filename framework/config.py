from pathlib import Path
from typing import Any, Dict, Union

import yaml


def load_pipeline_config(path: Union[str, Path]) -> Dict[str, Any]:
    """Load and validate the YAML contract used by Airflow and the framework.

    Args:
        path: Filesystem path of the pipeline configuration.

    Returns:
        The parsed configuration mapping.

    Raises:
        ValueError: If required sections or the API provider are missing.
    """
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ValueError("Pipeline configuration must be a YAML mapping")

    for section in ("pipeline", "source", "bronze", "silver"):
        if section not in config:
            raise ValueError("Missing required configuration section: {}".format(section))

    for section in ("raw", "tech", "func"):
        if section not in config["silver"]:
            raise ValueError("Missing required silver section: {}".format(section))

    if config["source"].get("type") != "api":
        raise ValueError("source.type must be api")

    if not config["source"].get("provider"):
        raise ValueError("API sources must define a provider")

    return config