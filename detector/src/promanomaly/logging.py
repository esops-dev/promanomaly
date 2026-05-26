"""structlog configuration. JSON output on stderr by default; falls back
to a console renderer when stderr is attached to a TTY (interactive
development).

Stdlib loggers (uvicorn, httpx, apscheduler, ...) are routed through the
same structlog pipeline so a single `journalctl`/`docker logs` follows
the whole process in one format. The pipeline is idempotent: callers may
invoke :func:`configure_logging` multiple times (e.g. CLI + serve()).
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logging(level: str = "INFO") -> None:
    """Configure stdlib + structlog so every logger emits structured JSON.

    Called from main() and from the CLI; safe to call repeatedly.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    interactive = sys.stderr.isatty()
    renderer: structlog.types.Processor = (
        structlog.dev.ConsoleRenderer(colors=True)
        if interactive
        else structlog.processors.JSONRenderer()
    )

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # ProcessorFormatter routes stdlib log records through structlog's
    # processor chain, so uvicorn / httpx / apscheduler emit JSON too.
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            timestamper,
        ],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    # Replace any pre-existing handlers (e.g. uvicorn's defaults) so we
    # don't double-log every line through both paths.
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(log_level)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> Any:
    """Return a bound structlog logger. Convenience wrapper."""
    return structlog.get_logger(name) if name else structlog.get_logger()
