#!/usr/bin/env python3
"""Скрипт для запуска тестов celery_monitor.

Запуск (из корня celery_mon):
  1. Убедитесь что Redis запущен: redis-server
  2. В терминале 1: python -m celery -A test.celery_app worker -l info --pool=solo
  3. В терминале 2: celery -A test.celery_app worker -l info  (опционально)
  4. В терминале 3: python test/run_test.py
"""

import subprocess
import sys
import time
from pathlib import Path

# Добавляем корень проекта в path
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))


def run_tasks():
    """Запускает рабочие и падающие задачи."""
    from test.tasks import add, fail_task

    print("Запуск add(2, 3)...")
    r1 = add.delay(2, 3)
    print(f"  Result: {r1.get(timeout=5)}")

    time.sleep(1)

    print("Запуск add(10, 20)...")
    r2 = add.delay(10, 20)
    print(f"  Result: {r2.get(timeout=5)}")

    time.sleep(1)

    print("Запуск fail_task (упадёт)...")
    r3 = fail_task.delay("Test error")
    try:
        r3.get(timeout=5)
    except Exception as e:
        print(f"  Ожидаемая ошибка: {e}")

    print("Готово. Подождите пару секунд для событий...")


if __name__ == "__main__":
    run_tasks()
