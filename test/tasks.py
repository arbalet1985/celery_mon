"""Test Celery tasks."""

from time import sleep

from .celery_app import app


@app.task(bind=True, max_retries=2)
def add(self, x: int, y: int) -> int:
    """Рабочая задача: сложение."""
    sleep(0.5)
    return x + y


@app.task(bind=True, max_retries=2)
def fail_task(self, message: str = "Intentional failure"):
    """Нерабочая задача: всегда падает с ошибкой."""
    raise RuntimeError(message)
