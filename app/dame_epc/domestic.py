

from __future__ import annotations

"""
Domestic EPC monthly ingestion:
- Pull JSON certificates via EPC API (search-after pagination)
- Normalize to a minimal, stable envelope
- Write NDJSON (gz) to GCS at a deterministic key
- Load into BigQuery raw table with partitioning/clustering on first create

Public API
----------
run_month(month: str, settings) -> dict
    Execute the full unit of work for the given YYYY-MM.
    Returns a dict with {kind, month, rows, gcs_uri, table, status}.

CLI
---
python -m dame_epc.domestic --month 2024-01
"""

import datetime as _dt
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

from google.cloud import bigquery

from .epc_api import fetch_certificates_json
from .io_utils import ensure_dataset, gcs_key, load_bq_raw, write_ndjson_gcs
from .schema import DOMESTIC_CLUSTERING, DOMESTIC_RAW_TABLE
from .settings import Settings, settings as _settings

KIND = "domestic"


# ---------------------------
# Helpers
# ---------------------------


def _parse_month(month: str) -> Tuple[_dt.date, _dt.date, str]:
    """Return (start_date, end_date, yyyymm) for 'YYYY-MM'."""
    y, m = map(int, month.split("-"))
    start = _dt.date(y, m, 1)
    end = (_dt.date(y + 1, 1, 1) if m == 12 else _dt.date(y, m + 1, 1)) - _dt.timedelta(days=1)
    return start, end, f"{y:04d}{m:02d}"


def _normalize(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize raw EPC record to envelope; skip if no LMK."""
    def g(d: Dict[str, Any], *keys: str) -> Any:
        for k in keys:
            if k in d:
                return d[k]
        return None

    lmk = g(rec, "lmk_key", "lmk-key", "LMK_KEY")
    if not lmk:
        return None
    lmk = str(lmk)

    uprn = g(rec, "uprn", "UPRN")
    uprn = str(uprn) if uprn not in (None, "") else None

    postcode = g(rec, "postcode", "POSTCODE")
    postcode = str(postcode) if postcode not in (None, "") else None

    lodg = g(rec, "lodgement_date", "lodgement-date", "LODgement-Date")
    if isinstance(lodg, str):
        try:
            _dt.date.fromisoformat(lodg)
        except Exception:
            lodg = None
    else:
        lodg = None

    return {
        "lmk_key": lmk,
        "lodgement_date": lodg,
        "postcode": postcode,
        "uprn": uprn,
        "payload": rec,
    }


# ---------------------------
# Public entrypoint
# ---------------------------


def run_month(month: str, settings: Settings = _settings) -> Dict[str, Any]:
    """
    Ingest one calendar month of domestic EPC certificates.

    Args:
        month: 'YYYY-MM'
        settings: loaded Settings (defaults to global settings)

    Returns:
        Dict with summary: {kind, month, rows, gcs_uri, table, status}
    """
    # Ensure dataset exists
    ensure_dataset(settings.project_id, settings.dataset_raw, settings.bq_location)

    # Fetch & normalize
    raw_iter = fetch_certificates_json(
        kind=KIND,
        month=month,
        page_size=settings.page_size,
        timeout=settings.request_timeout_seconds,
        auth=settings.epc_auth,
        retry_max=settings.retry_max,
        retry_backoff=settings.retry_backoff,
    )
    rows: List[Dict[str, Any]] = []
    for rec in raw_iter:
        norm = _normalize(rec)
        if norm:
            rows.append(norm)

    if not rows:
        return {"kind": KIND, "month": month, "rows": 0, "status": "no-data"}

    # Write to GCS
    key = gcs_key(KIND, month, "certs")
    uri = write_ndjson_gcs(settings.project_id, settings.bucket, key, rows)

    # Load to BigQuery
    table_id = load_bq_raw(
        project=settings.project_id,
        dataset=settings.dataset_raw,
        table=DOMESTIC_RAW_TABLE,
        region=settings.bq_location,
        gs_uri=uri,
        is_new_table=False,  # create options applied automatically if table is missing
        clustering=DOMESTIC_CLUSTERING,
    )

    return {
        "kind": KIND,
        "month": month,
        "rows": len(rows),
        "gcs_uri": uri,
        "table": table_id,
        "status": "loaded",
    }


# ---------------------------
# CLI
# ---------------------------

if __name__ == "__main__":
    import argparse
    import os
    from .settings import load_settings

    parser = argparse.ArgumentParser(description="Ingest domestic EPC certificates for a month.")
    parser.add_argument("--month", required=True, help="YYYY-MM (e.g., 2024-01)")
    parser.add_argument("--env-file", default=None, help="Optional path to .env (overrides ENV_FILE)")
    args = parser.parse_args()

    if args.env_file:
        os.environ["ENV_FILE"] = args.env_file
        s = load_settings()
    else:
        s = _settings

    result = run_month(args.month, s)
    print(json.dumps(result, indent=2))