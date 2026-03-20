"""Collects metrics from Celery inspect API and Redis broker."""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from celery import Celery

logger = logging.getLogger(__name__)

# Redis priority queue suffix for Celery (0, 3, 6, 9)
REDIS_PRIORITY_SUFFIXES = ("", "\x06\x163", "\x06\x166", "\x06\x169")


def get_celery_app(app_path):
    """Load Celery app from string like 'project.celery:app'."""
    if ":" in app_path:
        module_path, attr = app_path.rsplit(":", 1)
    else:
        module_path = app_path
        attr = "app"
    import importlib
    mod = importlib.import_module(module_path)
    return getattr(mod, attr)


def collect_inspect(app, timeout=5.0):
    """Collect worker stats via inspect API."""
    result = {
        "active": {},
        "reserved": {},
        "scheduled": {},
        "stats": {},
        "registered": [],
    }
    inspect = app.control.inspect(timeout=timeout)
    try:
        active = inspect.active()
        if active:
            for worker, tasks in active.items():
                result["active"][worker] = len(tasks)
    except Exception as e:
        logger.warning("inspect.active failed: %s", e)

    try:
        reserved = inspect.reserved()
        if reserved:
            for worker, tasks in reserved.items():
                result["reserved"][worker] = len(tasks)
    except Exception as e:
        logger.warning("inspect.reserved failed: %s", e)

    try:
        scheduled = inspect.scheduled()
        if scheduled:
            for worker, tasks in scheduled.items():
                result["scheduled"][worker] = len(tasks)
    except Exception as e:
        logger.warning("inspect.scheduled failed: %s", e)

    try:
        stats = inspect.stats()
        if stats:
            for worker, s in stats.items():
                total_by_name = s.get("total") or {}
                total_count = sum(total_by_name.values()) if isinstance(total_by_name, dict) else 0
                rusage = s.get("rusage") or {}
                result["stats"][worker] = {
                    "concurrency": s.get("pool", {}).get("max-concurrency", 0),
                    "uptime": s.get("uptime"),
                    "pid": s.get("pid"),
                    "prefetch_count": s.get("prefetch_count"),
                    "total": total_count,
                    "maxrss": rusage.get("maxrss"),
                }
    except Exception as e:
        logger.warning("inspect.stats failed: %s", e)

    try:
        registered = inspect.registered()
        if registered:
            for worker, tasks in registered.items():
                result["registered"] = list(tasks) if isinstance(tasks, (list, tuple)) else []
                break  # tasks are usually same across workers
    except Exception as e:
        logger.warning("inspect.registered failed: %s", e)

    return result


def get_queue_list(app):
    """Get list of queue names from app config or default."""
    queues = []
    task_queues = getattr(app.conf, "task_queues", None)
    if task_queues:
        for q in task_queues:
            if hasattr(q, "name"):
                queues.append(q.name)
            elif isinstance(q, (list, tuple)) and len(q) >= 1:
                queues.append(str(q[0]))
            else:
                queues.append(str(q))
    if not queues:
        default = getattr(app.conf, "task_default_queue", "celery")
        queues = [default]
    return queues


def collect_queue_lengths(app, queues=None):
    """Collect queue lengths from Redis via broker connection."""
    lengths = {}
    qlist = queues or get_queue_list(app)
    try:
        with app.connection_or_acquire() as conn:
            default_channel = conn.default_channel
            client = getattr(default_channel, "client", None) or getattr(
                default_channel, "_connection", None
            )
            if client is None:
                return {q: 0 for q in qlist}

            for q in qlist:
                total = 0
                for suffix in REDIS_PRIORITY_SUFFIXES:
                    key = q + suffix
                    try:
                        total += client.llen(key)
                    except Exception as e:
                        logger.debug("llen %r failed: %s", key, e)
                lengths[q] = total
    except Exception as e:
        logger.warning("collect_queue_lengths failed: %s", e)
        for q in qlist:
            lengths[q] = 0

    return lengths


def discover_queues_via_redis(app):
    """Discover queue names from Redis keys (with caution - may include non-Celery keys)."""
    seen = set()
    try:
        with app.connection_or_acquire() as conn:
            default_channel = conn.default_channel
            client = getattr(default_channel, "_connection", None) or getattr(
                default_channel, "connection", None
            )
            if client is None:
                return list(get_queue_list(app))

            # SCAN for list keys that look like Celery queues
            cursor = 0
            while True:
                cursor, keys = client.scan(cursor, count=100)
                for k in keys:
                    key = k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k)
                    # Skip internal keys
                    if key.startswith("celery-task-meta-") or key.startswith("unacked"):
                        continue
                    if key.startswith("_kombu."):
                        continue
                    # Base queue name without priority suffix
                    base = key
                    for s in REDIS_PRIORITY_SUFFIXES:
                        if s and key.endswith(s):
                            base = key[: -len(s)]
                            break
                    if base and base not in seen:
                        seen.add(base)
                if cursor == 0:
                    break
    except Exception as e:
        logger.warning("discover_queues_via_redis failed: %s", e)
        return list(get_queue_list(app))

    return list(seen) if seen else list(get_queue_list(app))
