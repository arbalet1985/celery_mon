"""Configuration loader for Celery monitor.

Loads configuration from YAML file (priority) and environment variables.
"""

import os
from pathlib import Path
from typing import Any

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load configuration from YAML and merge with environment variables.

    Environment variables override YAML values.
    """
    config: dict[str, Any] = {
        "celery": {
            "app": "",
        },
        "zabbix": {
            "server": "127.0.0.1",
            "port": 10051,
            "hostname": "celery-host",
        },
        "interval": 60,
        "discovery_interval": 3600,  # seconds, 0 = disabled
        "queues": [],
        "tasks": [],
        "worker_offline_threshold": 180,  # seconds
    }

    if config_path and Path(config_path).exists() and HAS_YAML:
        with open(config_path, encoding="utf-8") as f:
            file_config = yaml.safe_load(f)
        if file_config:
            _deep_merge(config, file_config)

    # Environment overrides
    if os.environ.get("CELERY_APP"):
        config["celery"]["app"] = os.environ["CELERY_APP"]
    if os.environ.get("ZABBIX_SERVER"):
        config["zabbix"]["server"] = os.environ["ZABBIX_SERVER"]
    if os.environ.get("ZABBIX_PORT"):
        try:
            config["zabbix"]["port"] = int(os.environ["ZABBIX_PORT"])
        except ValueError:
            pass
    if os.environ.get("ZABBIX_HOSTNAME"):
        config["zabbix"]["hostname"] = os.environ["ZABBIX_HOSTNAME"]
    if os.environ.get("CELERY_MON_INTERVAL"):
        try:
            config["interval"] = int(os.environ["CELERY_MON_INTERVAL"])
        except ValueError:
            pass
    if os.environ.get("CELERY_MON_DISCOVERY_INTERVAL"):
        try:
            config["discovery_interval"] = int(os.environ["CELERY_MON_DISCOVERY_INTERVAL"])
        except ValueError:
            pass

    return config


def _deep_merge(base: dict, override: dict) -> None:
    """Merge override dict into base in-place."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
