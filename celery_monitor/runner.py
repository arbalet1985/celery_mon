"""Runner for daemon, one-shot, and discover modes."""

import json
import logging
import sys
import threading
import time
from typing import Any

from .config import load_config
from .events_collector import EventsCollector
from .metrics import collect_inspect, collect_queue_lengths, get_celery_app, get_queue_list
from .zabbix_exporter import ZabbixExporter, _sanitize_key_param

logger = logging.getLogger(__name__)


def run_daemon(config: dict[str, Any], app, dry_run: bool = False) -> None:
    """Run event consumer in a thread and periodically send metrics to Zabbix."""
    interval = config.get("interval", 60)
    discovery_interval = config.get("discovery_interval", 3600)
    zabbix_cfg = config.get("zabbix", {})
    queues = config.get("queues") or []
    tasks = config.get("tasks") or []

    exporter = None
    if not dry_run:
        exporter = ZabbixExporter(
            hostname=zabbix_cfg.get("hostname", "celery-host"),
            server=zabbix_cfg.get("server", "127.0.0.1"),
            port=int(zabbix_cfg.get("port", 10051)),
        )

    collector = EventsCollector(app, task_filter=tasks or None, queue_filter=queues or None)
    collector_done = threading.Event()
    last_discovery_time = 0.0

    def events_thread() -> None:
        try:
            collector.run(limit=None, timeout=None)
        except Exception as e:
            logger.error("Events receiver stopped: %s", e)
        finally:
            collector_done.set()

    thread = threading.Thread(target=events_thread, daemon=True)
    thread.start()
    logger.info("Events collector started, interval=%ds, discovery_interval=%ds", interval, discovery_interval)

    if discovery_interval > 0:
        _run_discovery(config, app, exporter, dry_run)
        last_discovery_time = time.time()

    try:
        while not collector_done.is_set():
            time.sleep(interval)
            if collector_done.is_set():
                break

            now = time.time()
            if discovery_interval > 0 and (now - last_discovery_time) >= discovery_interval:
                _run_discovery(config, app, exporter, dry_run)
                last_discovery_time = now

            events_data = collector.get_and_reset()
            inspect_data = {}
            queue_lengths = {}

            try:
                inspect_data = collect_inspect(app)
            except Exception as e:
                logger.warning("Inspect failed: %s", e)

            try:
                qlist = queues or get_queue_list(app)
                queue_lengths = collect_queue_lengths(app, qlist)
            except Exception as e:
                logger.warning("Queue lengths failed: %s", e)

            if dry_run:
                print("=== DRY RUN (metrics) ===")
                print("events:", json.dumps(events_data, indent=2, default=str))
                print("inspect:", json.dumps(inspect_data, indent=2, default=str))
                print("queue_lengths:", json.dumps(queue_lengths, indent=2))
            else:
                try:
                    exporter.send(
                        events_data=events_data,
                        inspect_data=inspect_data,
                        queue_lengths=queue_lengths,
                        interval_sec=float(interval),
                    )
                except Exception as e:
                    logger.error("Zabbix send failed: %s", e)

    except KeyboardInterrupt:
        logger.info("Stopping daemon")
    collector_done.set()


def run_once(config: dict[str, Any], app, dry_run: bool = False) -> None:
    """One-shot: collect inspect + queue lengths, send to Zabbix, exit."""
    zabbix_cfg = config.get("zabbix", {})
    queues = config.get("queues") or []

    inspect_data = {}
    queue_lengths = {}

    try:
        inspect_data = collect_inspect(app)
    except Exception as e:
        logger.warning("Inspect failed: %s", e)

    try:
        qlist = queues or get_queue_list(app)
        queue_lengths = collect_queue_lengths(app, qlist)
    except Exception as e:
        logger.warning("Queue lengths failed: %s", e)

    if dry_run:
        print("=== DRY RUN (metrics, no Zabbix send) ===")
        print("inspect:", json.dumps(inspect_data, indent=2, default=str))
        print("queue_lengths:", json.dumps(queue_lengths, indent=2))
        sys.exit(0)

    exporter = ZabbixExporter(
        hostname=zabbix_cfg.get("hostname", "celery-host"),
        server=zabbix_cfg.get("server", "127.0.0.1"),
        port=int(zabbix_cfg.get("port", 10051)),
    )
    try:
        ok = exporter.send(
            events_data=None,
            inspect_data=inspect_data,
            queue_lengths=queue_lengths,
            interval_sec=60.0,
        )
        sys.exit(0 if ok else 1)
    except Exception as e:
        logger.error("Zabbix send failed: %s", e)
        sys.exit(1)


def _get_discovery_data(config: dict[str, Any], app, target: str) -> list[dict[str, str]]:
    """Return LLD data for tasks, queues, or workers."""
    data: list[dict[str, str]] = []

    if target == "tasks":
        try:
            inspect_data = collect_inspect(app)
            reg = inspect_data.get("registered", [])
            for name in reg:
                data.append({"{#TASK_NAME}": _sanitize_key_param(name)})
        except Exception as e:
            logger.error("Discover tasks failed: %s", e)

    elif target == "queues":
        try:
            queues = config.get("queues") or get_queue_list(app)
            for q in queues:
                data.append({"{#QUEUE_NAME}": _sanitize_key_param(q)})
        except Exception as e:
            logger.error("Discover queues failed: %s", e)

    elif target == "workers":
        try:
            inspect_data = collect_inspect(app)
            for worker in inspect_data.get("stats", {}).keys():
                data.append({"{#WORKER_NAME}": _sanitize_key_param(worker)})
        except Exception as e:
            logger.error("Discover workers failed: %s", e)

    else:
        logger.error("Unknown discover target: %s", target)
    return data


def _run_discovery(config: dict[str, Any], app, exporter: ZabbixExporter | None, dry_run: bool) -> None:
    """Run discovery for tasks, queues, workers and send to Zabbix or print."""
    for target in ("tasks", "queues", "workers"):
        data = _get_discovery_data(config, app, target)
        if dry_run:
            print(f"=== DRY RUN (discovery {target}) ===")
            print(json.dumps({"data": data}))
        elif exporter:
            exporter.send_discovery(target, data)


def run_discover(config: dict[str, Any], app, target: str) -> None:
    """Output LLD JSON for tasks, queues, or workers (CLI mode)."""
    data = _get_discovery_data(config, app, target)
    print(json.dumps({"data": data}))
