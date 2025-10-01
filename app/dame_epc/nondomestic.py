import datetime as _dt
import gzip
import io
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from .schema import (
    NON_DOMESTIC_CLUSTERING,
    NON_DOMESTIC_RAW_TABLE,
    PARTITION_FIELD,
    get_raw_schema,
)
from .settings import Settings, settings as _settings

KIND = "non-domestic"


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


def _fetch_certificates(
    month: str,
    page_size: int,
    timeout: int,
    auth: Tuple[str, str],
) -> Iterable[Dict[str, Any]]:
    """
    Generator yielding normalized non-domestic certificate rows for the month.
    Uses EPC /api/v1/non-domestic/search with search-after pagination.
    """
    start, end, _ = _parse_month(month)
    url = f"https://epc.opendatacommunities.org/api/v1/{KIND}/search"
    headers = {"Accept": "application/json"}
    basic = requests.auth.HTTPBasicAuth(*auth)

    search_after: Optional[str] = None
    while True:
        params = {
            "from-year": start.year,
            "from-month": start.month,
            "to-year": end.year,
            "to-month": end.month,
            "size": page_size,
        }
        if search_after:
            params["search-after"] = search_after

        r = requests.get(url, params=params, headers=headers, auth=basic, timeout=timeout)
        if r.status_code == 401:
            raise RuntimeError("EPC 401 Unauthorized: check EPC_EMAIL/EPC_API_KEY")
        r.raise_for_status()

        data = r.json()
        rows = data.get("rows", data if isinstance(data, list) else [])
        if not rows:
            break

        for rec in rows:
            norm = _normalize(rec)
            if norm:
                yield norm

        search_after = r.headers.get("X-Next-Search-After")
        if not search_after:
            break


def _gcs_key(month: str) -> str:
    """Deterministic GCS key for the monthly non-domestic dump."""
    _, _, yyyymm = _parse_month(month)
    return f"epc/json/{KIND}/{yyyymm}/certs/part-0001.json.gz"


def _write_ndjson_gcs(project: str, bucket: str, key: str, objs: Iterable[Dict[str, Any]]) -> str:
    """Write gzipped NDJSON to GCS, return gs:// URI."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for o in objs:
            gz.write((json.dumps(o, ensure_ascii=False) + "\n").encode("utf-8"))
    client = storage.Client(project=project)
    blob = client.bucket(bucket).blob(key)
    blob.upload_from_string(buf.getvalue(), content_type="application/gzip")
    return f"gs://{bucket}/{key}"


def _load_bq_raw(
    project: str,
    dataset: str,
    table: str,
    region: str,
    gs_uri: str,
) -> str:
    """Load the NDJSON file into the raw table, creating with partitioning/clustering if needed."""
    bq = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"

    # exists?
    try:
        bq.get_table(table_id)
        exists = True
    except NotFound:
        exists = False

    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=get_raw_schema(),
        ignore_unknown_values=True,
    )
    if not exists:
        cfg.time_partitioning = bigquery.TimePartitioning(field=PARTITION_FIELD)
        cfg.clustering_fields = NON_DOMESTIC_CLUSTERING

    job = bq.load_table_from_uri(gs_uri, table_id, job_config=cfg, location=region)
    job.result()
    return table_id


# ---------------------------
# Public entrypoint
# ---------------------------


def run_month(month: str, settings: Settings = _settings) -> Dict[str, Any]:
    """
    Ingest one calendar month of non-domestic EPC certificates.

    Args:
        month: 'YYYY-MM'
        settings: loaded Settings (defaults to global settings)

    Returns:
        Dict with summary: {kind, month, rows, gcs_uri, table, status}
    """
    # Fetch & normalize
    rows = list(
        _fetch_certificates(
            month=month,
            page_size=settings.page_size,
            timeout=settings.request_timeout_seconds,
            auth=settings.epc_auth,
        )
    )
    if not rows:
        return {"kind": KIND, "month": month, "rows": 0, "status": "no-data"}

    # Write to GCS
    key = _gcs_key(month)
    uri = _write_ndjson_gcs(settings.project_id, settings.bucket, key, rows)

    # Load to BigQuery
    table_id = _load_bq_raw(
        project=settings.project_id,
        dataset=settings.dataset_raw,
        table=NON_DOMESTIC_RAW_TABLE,
        region=settings.bq_location,
        gs_uri=uri,
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
    from .settings import load_settings

    parser = argparse.ArgumentParser(description="Ingest non-domestic EPC certificates for a month.")
    parser.add_argument("--month", required=True, help="YYYY-MM (e.g., 2024-01)")
    parser.add_argument("--env-file", default=None, help="Optional path to .env (overrides ENV_FILE)")
    args = parser.parse_args()

    if args.env_file:
        # re-load settings with alternate .env
        import os

        os.environ["ENV_FILE"] = args.env_file
        _s = load_settings()
    else:
        _s = _settings

    result = run_month(args.month, _s)
    print(json.dumps(result, indent=2))