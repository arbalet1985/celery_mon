"""Celery-to-Zabbix monitor - CLI entry point."""

import argparse
import logging
import sys

from .config import load_config
from .metrics import get_celery_app
from .runner import run_daemon, run_discover, run_once


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect Celery metrics and send to Zabbix",
    )
    parser.add_argument(
        "-A",
        "--app",
        dest="celery_app",
        default=None,
        help="Celery app path (e.g. project.celery:app)",
    )
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run as daemon: collect events and send metrics periodically",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="One-shot: collect inspect + queue lengths, send to Zabbix, exit",
    )
    parser.add_argument(
        "--discover",
        choices=["tasks", "queues", "workers"],
        metavar="TARGET",
        help="Output LLD JSON for Zabbix discovery",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print metrics to stdout instead of sending to Zabbix",
    )
    args = parser.parse_args()

    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s %(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO,
    )
    logger = logging.getLogger(__name__)

    config = load_config(args.config)
    app_path = args.celery_app or config.get("celery", {}).get("app", "")
    if not app_path:
        logger.error("Celery app not specified. Use -A or set CELERY_APP or celery.app in config.")
        sys.exit(1)

    try:
        app = get_celery_app(app_path)
    except Exception as e:
        logger.error("Failed to load Celery app %r: %s", app_path, e, exc_info=args.verbose)
        sys.exit(1)

    if args.discover:
        run_discover(config, app, args.discover)
    elif args.once:
        run_once(config, app, dry_run=args.dry_run)
    elif args.daemon:
        run_daemon(config, app, dry_run=args.dry_run)
    else:
        parser.print_help()
        logger.error("Specify one of: --daemon, --once, --discover TARGET")
        sys.exit(1)


if __name__ == "__main__":
    main()
