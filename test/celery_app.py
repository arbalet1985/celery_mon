"""Test Celery application for celery_monitor."""

import os
from celery import Celery

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/1")
REDIS_BACKEND = os.environ.get("REDIS_BACKEND", "redis://localhost:6379/2")

app = Celery(
    "test_app",
    broker=REDIS_URL,
    backend=REDIS_BACKEND,
)
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    imports=["test.tasks"],
)
