

from __future__ import annotations

"""
Minimal, production-friendly logging setup.

Goals
-----
- One-line setup: `from dame_epc.logging_setup import setup_logging, get_logger; setup_logging()`
- JSON logs to stdout by default (great for Cloud Run, Pipelines, local tailing).
- Optional Google Cloud Logging handler if available and enabled.
- Easy contextual logs via `LoggerAdapter` so you can bind fields like
  `kind`, `month`, `step`, `rows`, `gcs_uri`, `table`, `job_id`.

Usage
-----
    from dame_epc.logging_setup import setup_logging, get_logger
    setup_logging()  # idempotent
    log = get_logger(__name__, component="ingest")
    log.info("starting month", extra={"kind": "domestic", "month": "2024-01"})
    try:
        ...
    except Exception:
        log.exception("ingest failed", extra={"kind": "domestic", "month": "2024-01"})

Environment knobs
-----------------
- LOG_LEVEL: DEBUG|INFO|WARNING|ERROR|CRITICAL (default: INFO)
- ENABLE_GCLOUD_LOGGING: "1"/"true" to enable Cloud Logging handler if library present
"""

import json
import logging
import os
import socket
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

# Optional Google Cloud Logging
try:
    from google.cloud import logging as gcloud_logging  # type: ignore
    from google.cloud.logging.handlers import CloudLoggingHandler  # type: ignore

    _GCLOUD_LOGGING_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    _GCLOUD_LOGGING_AVAILABLE = False


# ---------------------------
# JSON formatter
# ---------------------------


class JsonFormatter(logging.Formatter):
    """Structured JSON formatter with ISO8601 timestamps and extra field support."""

    def format(self, record: logging.LogRecord) -> str:
        # Base envelope
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pid": record.process,
            "host": socket.gethostname(),
        }

        # Extras: anything attached via LoggerAdapter or `extra=...`
        for k, v in record.__dict__.items():
            if k in (
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
            ):
                continue
            # avoid overwriting base fields unless intentional
            if k not in payload:
                payload[k] = v

        # Exception info
        if record.exc_info:
            payload["exc_type"] = getattr(record.exc_info[0], "__name__", str(record.exc_info[0]))
            payload["exc"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


# ---------------------------
# Setup & helpers
# ---------------------------


_CONFIGURED = False


def _level_from_env(default: str = "INFO") -> int:
    lvl = os.environ.get("LOG_LEVEL", default).upper()
    return {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }.get(lvl, logging.INFO)


def setup_logging(force: bool = False) -> None:
    """
    Configure root logging once, idempotently.

    - StreamHandler to stdout with JSON formatting.
    - If ENABLE_GCLOUD_LOGGING is set and the library is available,
      attach a CloudLoggingHandler as well.

    Args:
        force: if True, remove existing handlers and reconfigure.
    """
    global _CONFIGURED
    if _CONFIGURED and not force:
        return

    root = logging.getLogger()
    level = _level_from_env("INFO")

    if force:
        for h in list(root.handlers):
            root.removeHandler(h)

    if not root.handlers:
        root.setLevel(level)

        # JSON to stdout (works everywhere)
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(JsonFormatter())
        root.addHandler(sh)

        # Optional: Cloud Logging handler (in addition to stdout)
        if os.environ.get("ENABLE_GCLOUD_LOGGING", "").lower() in {"1", "true", "yes"} and _GCLOUD_LOGGING_AVAILABLE:
            try:
                client = gcloud_logging.Client()  # ADC will be used if available
                clh = CloudLoggingHandler(client)
                clh.setLevel(level)
                # No formatter: Cloud handler preserves structured fields in record.__dict__
                root.addHandler(clh)
            except Exception:
                # If Cloud Logging fails to init, keep stdout handler only.
                root.warning("Cloud Logging handler not initialized; continuing with stdout JSON.")

    _CONFIGURED = True


class ContextAdapter(logging.LoggerAdapter):
    """
    LoggerAdapter that merges context `extra` dicts (adapter.extra < call.extra).
    """

    def process(self, msg: Any, kwargs: Mapping[str, Any]) -> tuple[Any, Mapping[str, Any]]:
        call_extra = dict(kwargs.get("extra") or {})
        merged = dict(self.extra or {})
        merged.update(call_extra)
        kwargs["extra"] = merged
        return msg, kwargs


def get_logger(name: str = "dame_epc", **context: Any) -> ContextAdapter:
    """
    Return a context-aware logger. Call `setup_logging()` once at program start.

    Example:
        log = get_logger(__name__, component="ingest", kind="domestic")
        log.info("downloaded", extra={"month": "2024-01", "rows": 1234})
    """
    logger = logging.getLogger(name)
    return ContextAdapter(logger, context)