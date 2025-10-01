

from __future__ import annotations

"""
Recommendations ingestion for EPC data.

Two supported modes:

1) Incremental (per-month, per-LMK):
   - Collect LMK keys from that month's certificates (from raw tables) OR accept a provided list
   - Call the EPC recommendations endpoint per LMK
   - Normalize, write NDJSON to GCS, and load into `{kind}_recommendations_raw_json`

2) Backfill by year (ZIP with recommendations.csv):
   - Accept a local ZIP path, a HTTP(S) URL, or a GCS URI for the yearly recommendations ZIP
   - Extract the recommendations CSV, normalize to our JSON envelope, write to GCS and load

Landing schema (same for both):
  - lmk_key STRING REQUIRED
  - lodgement_date DATE NULLABLE   (if unavailable in source, left NULL)
  - payload JSON NULLABLE

This module has no EPC fetching for certificates (see domestic.py / nondomestic.py).
"""

import csv
import datetime as _dt
import io
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from .epc_api import fetch_recommendations_by_lmk
from .io_utils import ensure_dataset, gcs_key, load_bq_raw, write_ndjson_gcs
from .schema import (
    DOMESTIC_RAW_TABLE,
    NON_DOMESTIC_RAW_TABLE,
    DOMESTIC_RECS_RAW_TABLE,
    NON_DOMESTIC_RECS_RAW_TABLE,
)
from .settings import Settings, settings as _settings

K_DOM = "domestic"
K_NDOM = "non-domestic"
_VALID_KINDS = {K_DOM, K_NDOM}


def _yyyymm(month: str) -> str:
    return month.replace("-", "")


def _gcs_key_year(kind: str, year: int) -> str:
    """GCS key for a yearly recommendations dump."""
    return f"epc/json/{kind}/{year}/recs/part-0001.json.gz"


def _parse_month_bounds(month: str) -> Tuple[_dt.date, _dt.date]:
    y, m = map(int, month.split("-"))
    start = _dt.date(y, m, 1)
    end = (_dt.date(y + 1, 1, 1) if m == 12 else _dt.date(y, m + 1, 1)) - _dt.timedelta(days=1)
    return start, end


def _normalize_rec(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a recommendation dict to our envelope.
    We only require LMK; lodgement_date is often absent for recs (left NULL).
    """
    def g(d: Dict[str, Any], *ks: str) -> Any:
        for k in ks:
            if k in d:
                return d[k]
        return None

    lmk = g(rec, "lmk_key", "lmk-key", "LMK_KEY")
    if not lmk:
        return None

    lodg = g(rec, "lodgement_date", "lodgement-date", "LODgement-Date")
    if isinstance(lodg, str):
        try:
            _dt.date.fromisoformat(lodg)
        except Exception:
            lodg = None
    else:
        lodg = None

    return {"lmk_key": str(lmk), "lodgement_date": lodg, "postcode": None, "uprn": None, "payload": rec}


def _resolve_tables(kind: str) -> Tuple[str, str]:
    """Return (cert_raw_table, recs_raw_table)."""
    if kind == K_DOM:
        return DOMESTIC_RAW_TABLE, DOMESTIC_RECS_RAW_TABLE
    if kind == K_NDOM:
        return NON_DOMESTIC_RAW_TABLE, NON_DOMESTIC_RECS_RAW_TABLE
    raise ValueError(f"Unknown kind {kind!r}; expected one of {_VALID_KINDS}")


def _distinct_lmks_for_month(client: bigquery.Client, project: str, dataset: str, kind: str, month: str, location: str) -> List[str]:
    """Query BQ for distinct LMK keys for certificates lodged in the given month."""
    cert_table, _ = _resolve_tables(kind)
    table_id = f"`{project}.{dataset}.{cert_table}`"
    start, end = _parse_month_bounds(month)
    sql = f"""
    SELECT DISTINCT lmk_key
    FROM {table_id}
    WHERE lodgement_date BETWEEN @start AND @end
      AND lmk_key IS NOT NULL
    """
    job = client.query(
        sql,
        location=location,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start.isoformat()),
                bigquery.ScalarQueryParameter("end", "DATE", end.isoformat()),
            ]
        ),
    )
    rows = job.result()
    return [r["lmk_key"] for r in rows]


def _fetch_recs_for_lmks(kind: str, lmks: List[str], settings: Settings) -> List[Dict[str, Any]]:
    """Call per-LMK recommendations; flatten and normalize."""
    out: List[Dict[str, Any]] = []
    for lmk in lmks:
        try:
            recs = fetch_recommendations_by_lmk(
                kind=kind,
                lmk_key=lmk,
                timeout=settings.request_timeout_seconds,
                auth=settings.epc_auth,
                retry_max=settings.retry_max,
                retry_backoff=settings.retry_backoff,
            )
        except RuntimeError:
            # Log if you like; for now, skip this LMK on hard errors
            recs = []
        for r in recs:
            n = _normalize_rec(r)
            if n:
                out.append(n)
    return out


# ---------------------------
# Public entrypoints
# ---------------------------


def run_month_incremental(
    kind: str,
    month: str,
    settings: Settings = _settings,
    lmk_keys: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Incremental recommendations ingestion for a single month.

    Strategy:
      - Get LMK keys for that month's certificates (or use provided lmk_keys)
      - Fetch recommendations per LMK via EPC API
      - Write to GCS at epc/json/{kind}/{YYYYMM}/recs/part-0001.json.gz
      - Load to {kind}_recommendations_raw_json

    Returns: dict summary
    """
    if kind not in _VALID_KINDS:
        raise ValueError(f"kind must be one of {_VALID_KINDS}, got {kind!r}")

    ensure_dataset(settings.project_id, settings.dataset_raw, settings.bq_location)

    # Resolve LMKs if not provided
    if lmk_keys is None:
        bq = bigquery.Client(project=settings.project_id)
        lmk_keys = _distinct_lmks_for_month(bq, settings.project_id, settings.dataset_raw, kind, month, settings.bq_location)

    if not lmk_keys:
        return {"kind": kind, "month": month, "rows": 0, "status": "no-lmks"}

    # Fetch recommendations
    rows = _fetch_recs_for_lmks(kind, lmk_keys, settings)
    if not rows:
        return {"kind": kind, "month": month, "rows": 0, "status": "no-recs"}

    # Write & load
    key = gcs_key(kind, month, "recs")
    uri = write_ndjson_gcs(settings.project_id, settings.bucket, key, rows)

    _, recs_table = _resolve_tables(kind)
    table_id = load_bq_raw(
        project=settings.project_id,
        dataset=settings.dataset_raw,
        table=recs_table,
        region=settings.bq_location,
        gs_uri=uri,
        is_new_table=False,
        clustering=["lmk_key"],
    )

    return {
        "mode": "incremental",
        "kind": kind,
        "month": month,
        "rows": len(rows),
        "gcs_uri": uri,
        "table": table_id,
        "status": "loaded",
    }


@dataclass
class ZipSource:
    """Define where to get the yearly ZIP from."""
    local_path: Optional[str] = None
    http_url: Optional[str] = None
    gcs_uri: Optional[str] = None  # gs://bucket/key.zip


def _download_zip_to_temp(src: ZipSource, project: Optional[str] = None) -> Optional[str]:
    """
    Download/copy the ZIP to a local temp file and return its path.
    Supports local path, HTTP(S) URL, or GCS URI.
    """
    if src.local_path:
        if not os.path.exists(src.local_path):
            raise FileNotFoundError(src.local_path)
        return src.local_path

    if src.http_url:
        resp = requests.get(src.http_url, stream=True, timeout=120)
        resp.raise_for_status()
        fd, tmp_path = tempfile.mkstemp(suffix=".zip")
        with os.fdopen(fd, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return tmp_path

    if src.gcs_uri:
        m = re.match(r"^gs://([^/]+)/(.+)$", src.gcs_uri)
        if not m:
            raise ValueError(f"Invalid GCS URI: {src.gcs_uri}")
        bucket, key = m.group(1), m.group(2)
        client = storage.Client(project=project)
        blob = client.bucket(bucket).blob(key)
        if not blob.exists(client):
            raise FileNotFoundError(f"GCS object not found: {src.gcs_uri}")
        fd, tmp_path = tempfile.mkstemp(suffix=".zip")
        with os.fdopen(fd, "wb") as f:
            blob.download_to_file(f)
        return tmp_path

    # Nothing provided
    return None


def _iter_recs_from_zip(zip_path: str) -> Iterator[Dict[str, Any]]:
    """
    Yield recommendation rows (dicts) from a yearly ZIP that contains a CSV file.
    We pick the first file that looks like a recommendations CSV.
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        # find a member with recommendations in the name or .csv
        name = None
        for n in zf.namelist():
            lower = n.lower()
            if lower.endswith(".csv") and ("recom" in lower or "recommendation" in lower):
                name = n
                break
        if name is None:
            # fallback: any csv
            for n in zf.namelist():
                if n.lower().endswith(".csv"):
                    name = n
                    break
        if name is None:
            raise RuntimeError("No CSV file found in ZIP")

        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline="")
            reader = csv.DictReader(text)
            for row in reader:
                yield row


def run_year(
    kind: str,
    year: int,
    settings: Settings = _settings,
    source: Optional[ZipSource] = None,
) -> Dict[str, Any]:
    """
    Backfill recommendations using a yearly ZIP.

    You must provide a ZipSource pointing to a local file, HTTP URL, or GCS URI
    for the year's recommendations ZIP.

    Returns: dict summary
    """
    if kind not in _VALID_KINDS:
        raise ValueError(f"kind must be one of {_VALID_KINDS}, got {kind!r}")

    ensure_dataset(settings.project_id, settings.dataset_raw, settings.bq_location)

    if source is None:
        # No source provided; nothing to do (explicit and safe)
        return {"mode": "backfill", "kind": kind, "year": year, "rows": 0, "status": "no-source"}

    zip_path = _download_zip_to_temp(source, project=settings.project_id)
    if not zip_path:
        return {"mode": "backfill", "kind": kind, "year": year, "rows": 0, "status": "no-source"}

    # Parse CSV rows and normalize
    rows: List[Dict[str, Any]] = []
    for rec in _iter_recs_from_zip(zip_path):
        n = _normalize_rec(rec)
        if n:
            rows.append(n)

    if not rows:
        return {"mode": "backfill", "kind": kind, "year": year, "rows": 0, "status": "no-recs"}

    # Write & load
    key = _gcs_key_year(kind, year)
    uri = write_ndjson_gcs(settings.project_id, settings.bucket, key, rows)

    _, recs_table = _resolve_tables(kind)
    table_id = load_bq_raw(
        project=settings.project_id,
        dataset=settings.dataset_raw,
        table=recs_table,
        region=settings.bq_location,
        gs_uri=uri,
        is_new_table=False,
        clustering=["lmk_key"],
    )

    return {
        "mode": "backfill",
        "kind": kind,
        "year": year,
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
    import json

    parser = argparse.ArgumentParser(description="Ingest EPC recommendations (incremental or backfill).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_inc = sub.add_parser("incremental", help="Fetch per-LMK recommendations for a month.")
    p_inc.add_argument("--kind", required=True, choices=[K_DOM, K_NDOM])
    p_inc.add_argument("--month", required=True, help="YYYY-MM")
    p_inc.add_argument("--lmk", action="append", help="Optional LMK key (repeatable). If omitted, query BQ for that month.")

    p_back = sub.add_parser("backfill", help="Load yearly recommendations from a ZIP.")
    p_back.add_argument("--kind", required=True, choices=[K_DOM, K_NDOM])
    p_back.add_argument("--year", required=True, type=int)
    p_back.add_argument("--local-zip", default=None, help="Path to local ZIP file.")
    p_back.add_argument("--http-url", default=None, help="HTTP(S) URL to ZIP.")
    p_back.add_argument("--gcs-uri", default=None, help="gs://bucket/key.zip")

    args = parser.parse_args()

    if args.cmd == "incremental":
        lmks = args.lmk if args.lmk else None
        res = run_month_incremental(kind=args.kind, month=args.month, lmk_keys=lmks)
        print(json.dumps(res, indent=2))
    else:
        src = ZipSource(local_path=args.local_zip, http_url=args.http_url, gcs_uri=args.gcs_uri)
        res = run_year(kind=args.kind, year=args.year, source=src)
        print(json.dumps(res, indent=2))