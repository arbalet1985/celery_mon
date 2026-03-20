"""Celery Events collector for task and worker metrics.

Aggregates task-started, task-succeeded, task-failed, task-retried,
task-received, worker-heartbeat events into in-memory counters.
"""

import logging
import threading
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

if TYPE_CHECKING:
    from celery import Celery

logger = logging.getLogger(__name__)


class EventsCollector:
    """Collects metrics from Celery events and provides aggregated counters."""

    def __init__(
        self,
        app,  # Celery
        task_filter=None,  # type: Optional[List[str]]
        queue_filter=None,  # type: Optional[List[str]]
    ):
        self.app = app
        self.task_filter = set(task_filter or [])  # type: Set[str]
        self.queue_filter = set(queue_filter or [])  # type: Set[str]

        self._lock = threading.Lock()
        self._task_started = 0
        self._task_succeeded = 0
        self._task_failed = 0
        self._task_retried = 0

        # Per-task counters
        self._task_started_by_name = defaultdict(int)  # type: Dict[str, int]
        self._task_succeeded_by_name = defaultdict(int)  # type: Dict[str, int]
        self._task_failed_by_name = defaultdict(int)  # type: Dict[str, int]
        self._task_retried_by_name = defaultdict(int)  # type: Dict[str, int]
        # Last error message per task (for failed tasks)
        self._task_failed_error_by_name = {}  # type: Dict[str, str]

        # Runtime for avg: sum and count per task
        self._runtime_sum_by_name = defaultdict(float)  # type: Dict[str, float]
        self._runtime_count_by_name = defaultdict(int)  # type: Dict[str, int]

        # task_id -> (queue, received_ts) for latency
        self._pending_tasks = {}  # type: Dict[str, Tuple[str, float]]
        self._queue_latencies = defaultdict(list)  # type: Dict[str, List[float]]
        self._queue_throughput_in = defaultdict(int)  # type: Dict[str, int]
        self._queue_throughput_out = defaultdict(int)  # type: Dict[str, int]

        # worker -> last heartbeat timestamp
        self._worker_last_seen = {}  # type: Dict[str, float]
        # worker -> celery version (from heartbeat)
        self._worker_version = {}  # type: Dict[str, str]
        # worker -> total tasks processed (from heartbeat)
        self._worker_processed = {}  # type: Dict[str, int]

        self._state = self.app.events.State()

    def _default_queue(self):
        return getattr(self.app.conf, "task_default_queue", "celery")

    def _resolve_queue(self, queue, event=None):
        """Return queue name; use default instead of 'unknown' so Zabbix accepts metrics."""
        if queue and queue != "unknown":
            return queue
        if event and event.get("queue"):
            return event["queue"]
        return self._default_queue()

    def _should_track_task(self, task_name, queue):
        if self.task_filter and task_name not in self.task_filter:
            return False
        if self.queue_filter and queue and queue not in self.queue_filter:
            return False
        return True

    def _get_handlers(self):
        def handle_task_received(event):
            state = self._state
            state.event(event)
            task = state.tasks.get(event.get("uuid"))
            if not task:
                return
            task_name = getattr(task, "name", "unknown")
            queue = self._resolve_queue(getattr(task, "queue", None), event)
            if not self._should_track_task(task_name, queue):
                return
            with self._lock:
                ts = event.get("timestamp") or 0
                self._pending_tasks[event["uuid"]] = (queue, ts)
                self._queue_throughput_in[queue] += 1

        def handle_task_started(event):
            state = self._state
            state.event(event)
            task = state.tasks.get(event.get("uuid"))
            if not task:
                return
            task_name = getattr(task, "name", "unknown")
            queue = self._resolve_queue(getattr(task, "queue", None))
            if not self._should_track_task(task_name, queue):
                return
            with self._lock:
                self._task_started += 1
                self._task_started_by_name[task_name] += 1
                self._queue_throughput_out[queue] += 1

                # Latency: started_ts - received_ts
                pending = self._pending_tasks.pop(event["uuid"], None)
                if pending:
                    q, recv_ts = pending
                    started_ts = event.get("timestamp") or 0
                    if recv_ts > 0:
                        self._queue_latencies[q].append(started_ts - recv_ts)

        def handle_task_succeeded(event):
            state = self._state
            state.event(event)
            task = state.tasks.get(event.get("uuid"))
            if not task:
                return
            task_name = getattr(task, "name", "unknown")
            queue = self._resolve_queue(getattr(task, "queue", None))
            if not self._should_track_task(task_name, queue):
                return
            runtime = event.get("runtime")
            with self._lock:
                self._task_succeeded += 1
                self._task_succeeded_by_name[task_name] += 1
                if runtime is not None:
                    self._runtime_sum_by_name[task_name] += float(runtime)
                    self._runtime_count_by_name[task_name] += 1

        def handle_task_failed(event):
            state = self._state
            state.event(event)
            task = state.tasks.get(event.get("uuid"))
            task_name = getattr(task, "name", "unknown") if task else "unknown"
            queue = self._resolve_queue(getattr(task, "queue", None) if task else None)
            if not self._should_track_task(task_name, queue):
                return
            # Get error message from event or task state
            error_text = ""
            exc = event.get("exception")
            if exc is not None:
                error_text = str(exc) if not isinstance(exc, str) else exc
            if not error_text and event.get("traceback"):
                tb = str(event["traceback"])
                lines = [L.strip() for L in tb.split("\n") if L.strip()]
                error_text = lines[-1] if lines else ""
            if not error_text and task and getattr(task, "exception", None):
                error_text = str(task.exception)
            # Truncate for Zabbix text item (often 64KB limit, we use 2000 for readability)
            if len(error_text) > 2000:
                error_text = error_text[:1997] + "..."
            with self._lock:
                self._task_failed += 1
                self._task_failed_by_name[task_name] += 1
                if error_text:
                    self._task_failed_error_by_name[task_name] = error_text

        def handle_task_retried(event):
            state = self._state
            state.event(event)
            task = state.tasks.get(event.get("uuid"))
            task_name = getattr(task, "name", "unknown") if task else "unknown"
            queue = self._resolve_queue(getattr(task, "queue", None) if task else None)
            if not self._should_track_task(task_name, queue):
                return
            with self._lock:
                self._task_retried += 1
                self._task_retried_by_name[task_name] += 1

        def handle_worker_heartbeat(event):
            state = self._state
            state.event(event)
            hostname = event.get("hostname", "")
            ts = event.get("timestamp") or 0
            sw_ver = event.get("sw_ver")
            processed = event.get("processed")
            with self._lock:
                self._worker_last_seen[hostname] = ts
                if sw_ver is not None:
                    self._worker_version[hostname] = str(sw_ver)
                if processed is not None:
                    self._worker_processed[hostname] = int(processed)

        def handle_worker_online(event):
            # Same as heartbeat for version/processed
            hostname = event.get("hostname", "")
            sw_ver = event.get("sw_ver")
            if hostname and sw_ver is not None:
                with self._lock:
                    self._worker_version[hostname] = str(sw_ver)

        return {
            "task-received": handle_task_received,
            "task-started": handle_task_started,
            "task-succeeded": handle_task_succeeded,
            "task-failed": handle_task_failed,
            "task-retried": handle_task_retried,
            "worker-heartbeat": handle_worker_heartbeat,
            "worker-online": handle_worker_online,
            "*": self._state.event,
        }

    def run(self, limit=None, timeout=None):
        """Blocking: capture events. Used in daemon mode in a thread."""
        handlers = self._get_handlers()
        try:
            with self.app.connection() as conn:
                recv = self.app.events.Receiver(conn, handlers=handlers)
                recv.capture(limit=limit, timeout=timeout, wakeup=True)
        except Exception as e:
            logger.error("Events receiver error: %s", e, exc_info=True)

    def get_and_reset(self):
        """Return current counters and reset them for next interval."""
        with self._lock:
            data = {
                "task_started": self._task_started,
                "task_succeeded": self._task_succeeded,
                "task_failed": self._task_failed,
                "task_retried": self._task_retried,
                "task_started_by_name": dict(self._task_started_by_name),
                "task_succeeded_by_name": dict(self._task_succeeded_by_name),
                "task_failed_by_name": dict(self._task_failed_by_name),
                "task_failed_error_by_name": dict(self._task_failed_error_by_name),
                "task_retried_by_name": dict(self._task_retried_by_name),
                "runtime_sum_by_name": dict(self._runtime_sum_by_name),
                "runtime_count_by_name": dict(self._runtime_count_by_name),
                "queue_latencies": {k: list(v) for k, v in self._queue_latencies.items()},
                "queue_throughput_in": dict(self._queue_throughput_in),
                "queue_throughput_out": dict(self._queue_throughput_out),
                "worker_last_seen": dict(self._worker_last_seen),
                "worker_version": dict(self._worker_version),
                "worker_processed": dict(self._worker_processed),
            }

            # Reset counters
            self._task_started = 0
            self._task_succeeded = 0
            self._task_failed = 0
            self._task_retried = 0
            self._task_started_by_name.clear()
            self._task_succeeded_by_name.clear()
            self._task_failed_by_name.clear()
            self._task_failed_error_by_name.clear()
            self._task_retried_by_name.clear()
            self._runtime_sum_by_name.clear()
            self._runtime_count_by_name.clear()
            self._queue_latencies.clear()
            self._queue_throughput_in.clear()
            self._queue_throughput_out.clear()
            # Keep worker_last_seen; don't reset

        return data

    def get_snapshot(self):
        """Non-destructive snapshot of current counters (for inspection)."""
        with self._lock:
            return {
                "task_started": self._task_started,
                "task_succeeded": self._task_succeeded,
                "task_failed": self._task_failed,
                "task_retried": self._task_retried,
                "task_started_by_name": dict(self._task_started_by_name),
                "task_succeeded_by_name": dict(self._task_succeeded_by_name),
                "task_failed_by_name": dict(self._task_failed_by_name),
                "task_failed_error_by_name": dict(self._task_failed_error_by_name),
                "task_retried_by_name": dict(self._task_retried_by_name),
                "runtime_sum_by_name": dict(self._runtime_sum_by_name),
                "runtime_count_by_name": dict(self._runtime_count_by_name),
                "queue_latencies": {k: list(v) for k, v in self._queue_latencies.items()},
                "queue_throughput_in": dict(self._queue_throughput_in),
                "queue_throughput_out": dict(self._queue_throughput_out),
                "worker_last_seen": dict(self._worker_last_seen),
                "worker_version": dict(self._worker_version),
                "worker_processed": dict(self._worker_processed),
            }
