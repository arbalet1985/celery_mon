# Тест celery_monitor

## Требования

- Python 3.10+
- Redis (localhost:6379)

## Задачи

- `add(x, y)` — рабочая задача, возвращает сумму
- `fail_task(message)` — нерабочая задача, всегда падает с RuntimeError

## Запуск Redis

**Вариант 1 — Docker:**
```bash
docker compose -f test/docker-compose.yml up -d
# или
.\test\start_redis.ps1
```

**Вариант 2 — локально:** установите Redis и запустите `redis-server`.

## Полный тест (PowerShell)

```powershell
.\test\run_full_test.ps1
```

## Ручной тест

1. **Терминал 1 — Celery worker:**
```bash
celery -A test.celery_app worker -l info
```

2. **Терминал 2 — celery_monitor (daemon, dry-run):**
```bash
python celery_monitor.py -A test.celery_app -c test/config.yaml --daemon --dry-run -v
```

3. **Терминал 3 — запуск задач:**
```bash
python test/run_test.py
```

Ожидаемый результат:
- `add(2,3)` — успешно, результат 5
- `add(10,20)` — успешно, результат 30
- `fail_task` — упадёт с RuntimeError

Через ~10 секунд в терминале 2 появятся метрики (events, inspect, queue_lengths).

## Быстрая проверка

```bash
# Discovery (queues без воркера)
python celery_monitor.py -A test.celery_app --discover queues -c test/config.yaml

# Discovery tasks/workers (нужен воркер)
python celery_monitor.py -A test.celery_app --discover tasks -c test/config.yaml
python celery_monitor.py -A test.celery_app --discover workers -c test/config.yaml

# One-shot dry-run
python celery_monitor.py -A test.celery_app --once --dry-run -c test/config.yaml
```

## Unit-тесты (без Redis)

```bash
pip install pytest
python -m pytest test/test_components.py -v
```
