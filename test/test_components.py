"""Unit tests for celery_monitor components (no Redis/Celery broker required)."""

import pytest


def test_sanitize_key_param():
    """Test Zabbix key parameter sanitization."""
    from celery_monitor.zabbix_exporter import _sanitize_key_param

    assert _sanitize_key_param("my_app.tasks.add") == "my_app_tasks_add"
    assert _sanitize_key_param("celery") == "celery"
    assert _sanitize_key_param("task[1]") == "task_1_"
    assert _sanitize_key_param("worker@host") == "worker@host"


def test_config_load():
    """Test config loading from YAML and defaults."""
    from celery_monitor.config import load_config

    config = load_config("test/config.yaml")
    assert config["celery"]["app"] == "test.celery_app:app"
    assert config["zabbix"]["hostname"] == "celery-test-host"
    assert config["interval"] == 10


def test_config_env_override(monkeypatch):
    """Test environment variable overrides."""
    from celery_monitor.config import load_config

    monkeypatch.setenv("ZABBIX_HOSTNAME", "env-host")
    monkeypatch.setenv("CELERY_APP", "myapp:app")
    config = load_config("test/config.yaml")
    assert config["zabbix"]["hostname"] == "env-host"
    assert config["celery"]["app"] == "myapp:app"


def test_get_celery_app():
    """Test loading Celery app from path."""
    from celery_monitor.metrics import get_celery_app

    app = get_celery_app("test.celery_app:app")
    assert app is not None
    assert app.main == "test_app"  # from Celery("test_app", ...)
