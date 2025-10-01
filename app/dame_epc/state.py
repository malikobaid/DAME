

from __future__ import annotations

"""
State/checkpoint utilities for idempotent monthly ingestion.

We persist one JSON file per step in GCS at:
  state/{kind}/{YYYYMM}/{step}.json

API:
- is_done(bucket, kind, month, step, project_id=None) -> bool
- get_status(bucket, kind, month, step, project_id=None) -> dict | None
- mark_done(bucket, kind, month, step, meta=None, project_id=None) -> dict
- clear_checkpoint(bucket, kind, month, step, project_id=None) -> None

Design notes:
- `month` must be 'YYYY-MM'.
- Files are uploaded with content_type='application/json' and no-store cache.
- Overwrites are allowed (last write wins).
"""

import datetime as _dt
import json
import re
from typing import Any, Dict, Optional

from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import storage

__all__ = [
    "is_done",
    "get_status",
    "mark_done",
    "clear_checkpoint",
    "checkpoint_path",
]

_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


def _mm_compact(month: str) -> str:
    """Return compact YYYYMM after validating 'YYYY-MM'."""
    if not _MONTH_RE.match(month):
        raise ValueError(f"month must be 'YYYY-MM', got {month!r}")
    return month.replace("-", "")


def checkpoint_path(kind: str, month: str, step: str) -> str:
    """Return the GCS object key for a checkpoint."""
    return f"state/{kind}/{_mm_compact(month)}/{step}.json"


def _blob(client: storage.Client, bucket: str, key: str) -> storage.Blob:
    bkt = client.bucket(bucket)
    return bkt.blob(key)


def is_done(
    bucket: str,
    kind: str,
    month: str,
    step: str,
    project_id: Optional[str] = None,
) -> bool:
    """
    Check if the checkpoint file exists.

    Returns:
        True if the checkpoint object exists; False otherwise.
    """
    client = storage.Client(project=project_id)
    key = checkpoint_path(kind, month, step)
    blob = _blob(client, bucket, key)
    try:
        return blob.exists(client)
    except GoogleAPICallError:
        # Conservative: if we can't check, report False so the step can retry.
        return False


def get_status(
    bucket: str,
    kind: str,
    month: str,
    step: str,
    project_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Read the checkpoint JSON document, if present.

    Returns:
        The parsed dict or None if not found.
    """
    client = storage.Client(project=project_id)
    key = checkpoint_path(kind, month, step)
    blob = _blob(client, bucket, key)
    if not blob.exists(client):
        return None
    data = blob.download_as_bytes()
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        # Return a minimal structure if the payload is not valid JSON.
        return {"raw": data.decode("utf-8", "ignore")}


def mark_done(
    bucket: str,
    kind: str,
    month: str,
    step: str,
    meta: Optional[Dict[str, Any]] = None,
    project_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Write/overwrite the checkpoint JSON document for a completed step.

    Args:
        bucket: GCS bucket name.
        kind: 'domestic' | 'non-domestic'.
        month: 'YYYY-MM'.
        step: logical step name, e.g. 'certs' or 'recs'.
        meta: optional metadata (e.g., rows, gcs_uri, table, job_id).
        project_id: optional GCP project override.

    Returns:
        The document that was written.
    """
    client = storage.Client(project=project_id)
    key = checkpoint_path(kind, month, step)
    blob = _blob(client, bucket, key)

    doc: Dict[str, Any] = {
        "kind": kind,
        "month": month,
        "step": step,
        "status": "done",
        "ts": _dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "meta": meta or {},
        "version": 1,
    }

    payload = json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    # Set metadata for better cache behavior/inspection.
    blob.cache_control = "no-store, max-age=0"
    blob.content_type = "application/json"
    blob.upload_from_string(payload, content_type="application/json")
    return doc


def clear_checkpoint(
    bucket: str,
    kind: str,
    month: str,
    step: str,
    project_id: Optional[str] = None,
) -> None:
    """
    Delete a checkpoint (useful for reprocessing a month/step).

    This is safe to call if the object does not exist.
    """
    client = storage.Client(project=project_id)
    key = checkpoint_path(kind, month, step)
    blob = _blob(client, bucket, key)
    try:
        blob.delete()  # type: ignore[no-untyped-call]
    except NotFound:
        return