# Celery-to-Zabbix Monitor

Скрипт собирает метрики Celery через Events API и Redis и отправляет их в Zabbix.

## Установка

```bash
pip install -r requirements.txt
```

## Режимы работы

### Daemon (постоянный процесс)

```bash
celery_monitor -A your_project.celery:app --config config.yaml --daemon
```

Или через переменные окружения:

```bash
CELERY_APP=your_project.celery:app ZABBIX_HOSTNAME=celery-host celery_monitor --daemon
```

### One-shot (cron)

```bash
celery_monitor -A your_project.celery:app --config config.yaml --once
```

### Low-Level Discovery

```bash
# Список задач
celery_monitor -A your_project.celery:app --discover tasks

# Список очередей
celery_monitor -A your_project.celery:app --discover queues

# Список воркеров
celery_monitor -A your_project.celery:app --discover workers
```

## Конфигурация

См. `config.example.yaml`. Параметры:

- `celery.app` — путь к Celery-приложению
- `zabbix.server`, `zabbix.port`, `zabbix.hostname` — параметры Zabbix
- `interval` — интервал отправки метрик в секундах (для daemon)
- `discovery_interval` — интервал discovery (tasks, queues, workers) в секундах; 0 — отключить. Daemon отправляет LLD через trapper
- `queues`, `tasks` — опциональные фильтры
- `zabbix.debug_failed_items` — при `true` при частичном фейле отправки пересылает каждый item отдельно и логирует, какие ключи не принял Zabbix

Переменные окружения: `CELERY_APP`, `ZABBIX_SERVER`, `ZABBIX_PORT`, `ZABBIX_HOSTNAME`, `CELERY_MON_INTERVAL`, `CELERY_MON_DISCOVERY_INTERVAL`.

## Метрики Zabbix

| Ключ | Описание |
|------|----------|
| `celery.task.started` | Количество начатых задач |
| `celery.task.succeeded` | Успешно завершённых |
| `celery.task.failed` | Завершившихся с ошибкой |
| `celery.task.retried` | Ретраев |
| `celery.task.failed[task_name]` | Ошибки по задаче |
| `celery.task.error[task_name]` | Текст последней ошибки при фейле задачи |
| `celery.task.succeeded[task_name]` | Успехи по задаче |
| `celery.task.runtime.avg[task_name]` | Среднее время выполнения |
| `celery.queue.length[queue_name]` | Длина очереди |
| `celery.queue.latency[queue_name]` | Средняя задержка в очереди |
| `celery.queue.throughput.in[queue_name]` | tasks/sec входящие |
| `celery.queue.throughput.out[queue_name]` | tasks/sec исходящие |
| `celery.worker.online[worker_name]` | Timestamp последнего heartbeat |
| `celery.workers.online` | Число онлайн-воркеров |
| `celery.tasks.active[worker_name]` | Активные задачи |
| `celery.tasks.prefetched[worker_name]` | Prefetched (reserved) |
| `celery.tasks.scheduled[worker_name]` | Запланированные |
| `celery.worker.concurrency[worker_name]` | Concurrency воркера |

Task-level метрики (started, succeeded, failed, runtime) доступны только в daemon-режиме (требуют Events).

## Шаблон Zabbix 6

Импортируйте `zabbix_template_celery_monitor.xml` в Zabbix: Data collection → Templates → Import.

### LLD (discovery)

**Вариант 1 (рекомендуется):** Daemon сам отправляет discovery через trapper. Задайте `discovery_interval` в конфиге (например, 3600 для 1 часа). UserParameter не нужен.

**Вариант 2:** Через Zabbix agent — добавьте в `zabbix_agentd.conf`:

```ini
UserParameter=celery.discover[*],/path/to/venv/bin/python /path/to/celery_monitor -A your_project.celery:app -c config.yaml --discover $1
```

Имя хоста в Zabbix должно совпадать с `zabbix.hostname` в конфиге celery_monitor.
