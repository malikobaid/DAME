from __future__ import annotations

"""
GCS and BigQuery helpers used by the DAME EPC loaders.

This module contains no EPC-specific logic. It focuses on:
- Deterministic GCS object keys for monthly data
- Writing gzipped NDJSON to GCS
- Ensuring BigQuery datasets exist in the correct region
- Loading NDJSON into raw tables with a small, stable schema

Functions
---------
gcs_key(kind, month, what) -> str
    Return a deterministic GCS key like:
    'epc/json/{kind}/{YYYYMM}/{what}/part-0001.json.gz'

write_ndjson_gcs(project, bucket, key, objs) -> str
    Write gzipped NDJSON to GCS and return 'gs://bucket/key'.

ensure_dataset(project, dataset, region) -> None
    Create the dataset if it does not exist (in the chosen region).

load_bq_raw(project, dataset, table, region, gs_uri, is_new_table=False, clustering=None) -> str
    Load NDJSON from GCS into a BigQuery table with the minimal landing schema.
    If the table doesn't exist (or is_new_table=True), set partitioning and clustering.
    Returns the fully-qualified table id.
"""

import gzip
import io
from typing import Iterable, List, Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from .schema import PARTITION_FIELD, get_raw_schema

# ---------------------------
# GCS helpers
# ---------------------------


def _yyyymm(month: str) -> str:
    """Convert 'YYYY-MM' to 'YYYYMM' without validation."""
    return month.replace("-", "")


def gcs_key(kind: str, month: str, what: str) -> str:
    """
    Return a deterministic GCS key for a kind/month artifact.

    Args:
        kind: 'domestic' | 'non-domestic'
        month: 'YYYY-MM'
        what: logical subfolder, typically 'certs' or 'recs'

    Example:
        gcs_key("domestic", "2024-01", "certs")
        -> 'epc/json/domestic/202401/certs/part-0001.json.gz'
    """
    return f"epc/json/{kind}/{_yyyymm(month)}/{what}/part-0001.json.gz"


def write_ndjson_gcs(project: str, bucket: str, key: str, objs: Iterable[dict]) -> str:
    """
    Write gzipped NDJSON to GCS and return the gs:// URI.

    Notes:
        - One JSON object per line, UTF-8 encoded.
        - Uses in-memory buffer; suitable for typical monthly batches.
    """
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for o in objs:
            gz.write((bigquery._helpers._json.dumps(o, ensure_ascii=False) + "\n").encode("utf-8"))  # type: ignore[attr-defined]

    client = storage.Client(project=project)
    blob = client.bucket(bucket).blob(key)
    blob.upload_from_string(buf.getvalue(), content_type="application/gzip")
    return f"gs://{bucket}/{key}"


# ---------------------------
# BigQuery helpers
# ---------------------------


def ensure_dataset(project: str, dataset: str, region: str) -> None:
    """
    Ensure a dataset exists in the specified region (idempotent).
    """
    client = bigquery.Client(project=project)
    try:
        client.get_dataset(dataset)
        return
    except NotFound:
        pass

    ds = bigquery.Dataset(f"{project}.{dataset}")
    ds.location = region
    client.create_dataset(ds, exists_ok=True)


def _table_exists(client: bigquery.Client, table_id: str) -> bool:
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def load_bq_raw(
    project: str,
    dataset: str,
    table: str,
    region: str,
    gs_uri: str,
    is_new_table: bool = False,
    clustering: Optional[List[str]] = None,
) -> str:
    """
    Load NDJSON from GCS into a BigQuery raw table with the minimal landing schema.

    Args:
        project: GCP project id.
        dataset: BigQuery dataset id.
        table: Target table id (without project/dataset).
        region: BigQuery job location (e.g., 'europe-west2').
        gs_uri: gs:// URI of the gzipped NDJSON file.
        is_new_table: If True, force table-creation options (partitioning/clustering).
                      If False, options are applied only when the table doesn't exist.
        clustering: Optional clustering fields to set on first create.

    Returns:
        Fully-qualified table id as a string.
    """
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"

    exists = _table_exists(client, table_id)
    create_opts = is_new_table or not exists

    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=get_raw_schema(),
        ignore_unknown_values=True,
    )
    if create_opts:
        cfg.time_partitioning = bigquery.TimePartitioning(field=PARTITION_FIELD)
        if clustering:
            cfg.clustering_fields = clustering

    job = client.load_table_from_uri(gs_uri, table_id, job_config=cfg, location=region)
    job.result()
    return table_id


__all__ = [
    "gcs_key",
    "write_ndjson_gcs",
    "ensure_dataset",
    "load_bq_raw",
]
