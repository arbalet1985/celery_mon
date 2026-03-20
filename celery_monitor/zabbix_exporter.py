"""Zabbix metrics exporter via Sender protocol."""

import json
import logging
from typing import Any, Dict, List, Optional

try:
    from zabbix_utils import ItemValue, Sender
    HAS_ZABBIX = True
except ImportError:
    HAS_ZABBIX = False
    ItemValue = None  # type: ignore[misc, assignment]

logger = logging.getLogger(__name__)


def _sanitize_key_param(value: str) -> str:
    """Replace chars that break Zabbix item keys. Exported for LLD consistency."""
    return value.replace(".", "_").replace(" ", "_").replace("[", "_").replace("]", "_")


class ZabbixExporter:
    """Sends Celery metrics to Zabbix via Sender protocol."""

    def __init__(
        self,
        hostname,
        server="127.0.0.1",
        port=10051,
        chunk_size=250,
        retries=2,
        debug_failed_items=False,
    ):
        if not HAS_ZABBIX:
            raise ImportError("zabbix_utils is required. Install with: pip install zabbix_utils")
        self.hostname = hostname
        self.sender = Sender(server=server, port=port, chunk_size=chunk_size)
        self.retries = retries
        self.debug_failed_items = debug_failed_items

    def _items_from_events(
        self,
        events_data,
        interval_sec,
    ):
        """Build ItemValues from events collector data."""
        items = []  # type: List

        items.append(ItemValue(self.hostname, "celery.task.started", events_data.get("task_started", 0)))
        items.append(ItemValue(self.hostname, "celery.task.succeeded", events_data.get("task_succeeded", 0)))
        items.append(ItemValue(self.hostname, "celery.task.failed", events_data.get("task_failed", 0)))
        items.append(ItemValue(self.hostname, "celery.task.retried", events_data.get("task_retried", 0)))

        for task_name, count in events_data.get("task_failed_by_name", {}).items():
            key = "celery.task.failed[{}]".format(_sanitize_key_param(task_name))
            items.append(ItemValue(self.hostname, key, count))

        for task_name, error_text in events_data.get("task_failed_error_by_name", {}).items():
            if error_text:
                key = "celery.task.error[{}]".format(_sanitize_key_param(task_name))
                items.append(ItemValue(self.hostname, key, error_text))

        for task_name, count in events_data.get("task_succeeded_by_name", {}).items():
            key = f"celery.task.succeeded[{_sanitize_key_param(task_name)}]"
            items.append(ItemValue(self.hostname, key, count))

        runtime_sum = events_data.get("runtime_sum_by_name", {})
        runtime_count = events_data.get("runtime_count_by_name", {})
        for task_name in set(runtime_sum) | set(runtime_count):
            s = runtime_sum.get(task_name, 0)
            c = runtime_count.get(task_name, 0)
            avg = round(s / c, 4) if c else 0
            key = f"celery.task.runtime.avg[{_sanitize_key_param(task_name)}]"
            items.append(ItemValue(self.hostname, key, avg))

        for queue, latencies in events_data.get("queue_latencies", {}).items():
            if latencies:
                avg_lat = round(sum(latencies) / len(latencies), 4)
            else:
                avg_lat = 0
            key = f"celery.queue.latency[{_sanitize_key_param(queue)}]"
            items.append(ItemValue(self.hostname, key, avg_lat))

        for queue, count in events_data.get("queue_throughput_in", {}).items():
            rate = round(count / interval_sec, 4) if interval_sec > 0 else 0
            key = f"celery.queue.throughput.in[{_sanitize_key_param(queue)}]"
            items.append(ItemValue(self.hostname, key, rate))

        for queue, count in events_data.get("queue_throughput_out", {}).items():
            rate = round(count / interval_sec, 4) if interval_sec > 0 else 0
            key = f"celery.queue.throughput.out[{_sanitize_key_param(queue)}]"
            items.append(ItemValue(self.hostname, key, rate))

        for worker, ts in events_data.get("worker_last_seen", {}).items():
            key = f"celery.worker.online[{_sanitize_key_param(worker)}]"
            items.append(ItemValue(self.hostname, key, int(ts)))

        return items

    def _items_from_inspect(self, inspect_data):
        """Build ItemValues from inspect data."""
        items = []

        for worker, count in inspect_data.get("active", {}).items():
            key = f"celery.tasks.active[{_sanitize_key_param(worker)}]"
            items.append(ItemValue(self.hostname, key, count))

        for worker, count in inspect_data.get("reserved", {}).items():
            key = f"celery.tasks.prefetched[{_sanitize_key_param(worker)}]"
            items.append(ItemValue(self.hostname, key, count))

        for worker, count in inspect_data.get("scheduled", {}).items():
            key = f"celery.tasks.scheduled[{_sanitize_key_param(worker)}]"
            items.append(ItemValue(self.hostname, key, count))

        online_count = 0
        for worker, stats in inspect_data.get("stats", {}).items():
            conc = stats.get("concurrency", 0)
            key = f"celery.worker.concurrency[{_sanitize_key_param(worker)}]"
            items.append(ItemValue(self.hostname, key, conc))
            online_count += 1

        items.append(ItemValue(self.hostname, "celery.workers.online", online_count))

        return items

    def _items_from_queue_lengths(self, lengths):
        """Build ItemValues from queue lengths."""
        items = []
        for queue, length in lengths.items():
            key = f"celery.queue.length[{_sanitize_key_param(queue)}]"
            items.append(ItemValue(self.hostname, key, length))
        return items

    def send_discovery(self, target, lld_data):
        """Send LLD JSON to Zabbix discovery rule via trapper.

        target: "tasks", "queues", or "workers"
        lld_data: list of dicts like [{"{#TASK_NAME}": "..."}, ...]
        """
        key = f"celery.discover[{target}]"
        value = json.dumps({"data": lld_data})
        items = [ItemValue(self.hostname, key, value)]
        for attempt in range(self.retries + 1):
            try:
                resp = self.sender.send(items)
                failed = getattr(resp, "failed", None)
                if failed is None and hasattr(resp, "get"):
                    failed = resp.get("failed", 1)
                if failed is None:
                    failed = 1
                if failed == 0:
                    logger.debug("Sent discovery %s with %d entries", target, len(lld_data))
                    return True
                logger.warning("Zabbix discovery send failed: %s", resp)
            except Exception as e:
                logger.warning("Zabbix discovery send attempt %d failed: %s", attempt + 1, e)
        return False

    def _log_failed_items(self, items):
        """Send each item individually to identify which keys fail."""
        failed_keys = []
        for item in items:
            try:
                resp = self.sender.send([item])
                failed = getattr(resp, "failed", 1)
                if failed > 0:
                    failed_keys.append((item.key, "Zabbix rejected (processed=%s failed=%s)" % (
                        getattr(resp, "processed", "?"),
                        getattr(resp, "failed", "?"),
                    )))
            except Exception as e:
                failed_keys.append((item.key, str(e)))
        if failed_keys:
            logger.warning("Zabbix failed items (debug mode):")
            for key, err in failed_keys:
                logger.warning("  - %s: %s", key, err)

    def send(
        self,
        events_data=None,
        inspect_data=None,
        queue_lengths=None,
        interval_sec=60.0,
    ):
        """Build and send all metrics to Zabbix."""
        items = []

        if events_data:
            items.extend(self._items_from_events(events_data, interval_sec))
        if inspect_data:
            items.extend(self._items_from_inspect(inspect_data))
        if queue_lengths:
            items.extend(self._items_from_queue_lengths(queue_lengths))

        if not items:
            logger.debug("No items to send")
            return True

        for attempt in range(self.retries + 1):
            try:
                resp = self.sender.send(items)
                failed = getattr(resp, "failed", None)
                if failed is None and hasattr(resp, "get"):
                    failed = resp.get("failed", 1)
                if failed is None:
                    failed = 1
                if failed == 0:
                    logger.debug("Sent %d items to Zabbix", len(items))
                    return True
                processed = getattr(resp, "processed", "?")
                total = getattr(resp, "total", len(items))
                logger.warning(
                    "Zabbix send partial/failed: processed=%s failed=%s total=%s",
                    processed, failed, total
                )
                if self.debug_failed_items and failed > 0:
                    self._log_failed_items(items)
            except Exception as e:
                logger.warning("Zabbix send attempt %d failed: %s", attempt + 1, e)
        return False
