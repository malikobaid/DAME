from __future__ import annotations

"""
Centralized schema and view SQL templates for the DAME EPC pipeline.

This module is deliberately IO-free. It only defines:
- Raw table names and clustering/partitioning conventions
- A helper to build the raw BigQuery schema
- Rendering functions that return CREATE OR REPLACE VIEW SQL strings

Other modules (e.g., scripts/apply_views.py, io_utils.py) should import from here.
"""

from typing import List

from google.cloud import bigquery

# ---------------------------
# Raw table canonical names
# ---------------------------
DOMESTIC_RAW_TABLE = "domestic_raw_json"
NON_DOMESTIC_RAW_TABLE = "non_domestic_raw_json"

DOMESTIC_RECS_RAW_TABLE = "domestic_recommendations_raw_json"
NON_DOMESTIC_RECS_RAW_TABLE = "non_domestic_recommendations_raw_json"

# ---------------------------
# Partitioning / clustering
# ---------------------------
PARTITION_FIELD = "lodgement_date"
DOMESTIC_CLUSTERING = ["lmk_key", "postcode", "uprn"]
NON_DOMESTIC_CLUSTERING = ["lmk_key"]  # keep simple; many cols are sparse

# ---------------------------
# Raw schema
# ---------------------------


def get_raw_schema() -> List[bigquery.SchemaField]:
    """
    Minimal, stable landing schema used for all raw JSON tables.

    - lmk_key: REQUIRED STRING (join key)
    - lodgement_date: DATE (partitioning field; may be NULL)
    - postcode: STRING (nullable)
    - uprn: STRING (nullable)
    - payload: JSON (entire source record)

    Returns a list of google.cloud.bigquery.SchemaField objects.
    """
    return [
        bigquery.SchemaField("lmk_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("lodgement_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("postcode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("uprn", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("payload", "JSON", mode="NULLABLE"),
    ]


# ---------------------------
# View SQL templates
# ---------------------------

def domestic_curated_view_sql(project: str, dataset: str) -> str:
    """
    Curated domestic certificates view over the raw JSON table.

    Selects common fields and uses SAFE_CAST where appropriate.
    """
    fq_source = f"`{project}.{dataset}.{DOMESTIC_RAW_TABLE}`"
    fq_view = f"`{project}.{dataset}.enr_domestic_certificates_v`"
    return f"""
CREATE OR REPLACE VIEW {fq_view} AS
SELECT
  lmk_key,
  lodgement_date,
  postcode,
  uprn,
  SAFE_CAST(JSON_VALUE(payload, '$."current-energy-efficiency"') AS INT64)    AS current_energy_efficiency,
  SAFE_CAST(JSON_VALUE(payload, '$."potential-energy-efficiency"') AS INT64)  AS potential_energy_efficiency,
  JSON_VALUE(payload, '$.address1')                                           AS address1,
  JSON_VALUE(payload, '$.address2')                                           AS address2,
  JSON_VALUE(payload, '$."property-type"')                                    AS property_type,
  JSON_VALUE(payload, '$."built-form"')                                       AS built_form,
  JSON_VALUE(payload, '$."main-heating-controls"')                            AS main_heating_controls,
  JSON_VALUE(payload, '$."main-fuel"')                                        AS main_fuel,
  JSON_VALUE(payload, '$."main-heating-description"')                         AS main_heating_description,
  JSON_VALUE(payload, '$."hot-water-description"')                            AS hot_water_description,
  SAFE_CAST(JSON_VALUE(payload, '$."co2-emissions-current"') AS FLOAT64)      AS co2_emissions_current
FROM {fq_source};
""".strip()


def non_domestic_curated_view_sql(project: str, dataset: str) -> str:
    """
    Curated non-domestic certificates view over the raw JSON table.
    """
    fq_source = f"`{project}.{dataset}.{NON_DOMESTIC_RAW_TABLE}`"
    fq_view = f"`{project}.{dataset}.enr_non_domestic_certificates_v`"
    return f"""
CREATE OR REPLACE VIEW {fq_view} AS
SELECT
  lmk_key,
  lodgement_date,
  postcode,
  uprn,
  JSON_VALUE(payload, '$."building-category"')                                 AS building_category,
  JSON_VALUE(payload, '$."lodgement-type"')                                    AS lodgement_type,
  SAFE_CAST(JSON_VALUE(payload, '$."asset-rating"') AS FLOAT64)                AS asset_rating,
  SAFE_CAST(JSON_VALUE(payload, '$."co2-emissions"') AS FLOAT64)               AS co2_emissions
FROM {fq_source};
""".strip()


def domestic_latest_by_lmk_view_sql(project: str, dataset: str) -> str:
    """
    Latest domestic certificate per LMK key.
    """
    fq_source = f"`{project}.{dataset}.enr_domestic_certificates_v`"
    fq_view = f"`{project}.{dataset}.enr_domestic_latest_by_lmk`"
    return f"""
CREATE OR REPLACE VIEW {fq_view} AS
SELECT *
FROM {fq_source}
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY lmk_key
  ORDER BY lodgement_date DESC
) = 1;
""".strip()


def non_domestic_latest_by_lmk_view_sql(project: str, dataset: str) -> str:
    """
    Latest non-domestic certificate per LMK key.
    """
    fq_source = f"`{project}.{dataset}.enr_non_domestic_certificates_v`"
    fq_view = f"`{project}.{dataset}.enr_non_domestic_latest_by_lmk`"
    return f"""
CREATE OR REPLACE VIEW {fq_view} AS
SELECT *
FROM {fq_source}
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY lmk_key
  ORDER BY lodgement_date DESC
) = 1;
""".strip()


def domestic_recommendations_view_sql(project: str, dataset: str) -> str:
    """
    Lightweight recommendations view (domestic).
    Keeps the full JSON recommendation and exposes a few helper columns as STRING.
    """
    fq_source = f"`{project}.{dataset}.{DOMESTIC_RECS_RAW_TABLE}`"
    fq_view = f"`{project}.{dataset}.enr_domestic_recommendations_v`"
    return f"""
CREATE OR REPLACE VIEW {fq_view} AS
SELECT
  lmk_key,
  lodgement_date,
  JSON_VALUE(payload, '$."improvement-description"') AS improvement_description,
  JSON_VALUE(payload, '$."indicative-cost"')         AS indicative_cost,
  JSON_VALUE(payload, '$."typical-saving"')          AS typical_saving,
  payload
FROM {fq_source};
""".strip()


def non_domestic_recommendations_view_sql(project: str, dataset: str) -> str:
    """
    Lightweight recommendations view (non-domestic).
    """
    fq_source = f"`{project}.{dataset}.{NON_DOMESTIC_RECS_RAW_TABLE}`"
    fq_view = f"`{project}.{dataset}.enr_non_domestic_recommendations_v`"
    return f"""
CREATE OR REPLACE VIEW {fq_view} AS
SELECT
  lmk_key,
  lodgement_date,
  JSON_VALUE(payload, '$."improvement-description"') AS improvement_description,
  JSON_VALUE(payload, '$."indicative-cost"')         AS indicative_cost,
  JSON_VALUE(payload, '$."typical-saving"')          AS typical_saving,
  payload
FROM {fq_source};
""".strip()


def domestic_cert_with_recs_view_sql(project: str, dataset: str) -> str:
    """
    Denormalized domestic view: each certificate with an ARRAY of recommendation payloads.
    Uses a correlated subquery to avoid GROUP BY on all columns.
    """
    fq_cert = f"`{project}.{dataset}.enr_domestic_certificates_v`"
    fq_recs = f"`{project}.{dataset}.{DOMESTIC_RECS_RAW_TABLE}`"
    fq_view = f"`{project}.{dataset}.enr_domestic_cert_with_recs_v`"
    return f"""
CREATE OR REPLACE VIEW {fq_view} AS
SELECT
  c.*,
  (SELECT ARRAY_AGG(r.payload) FROM {fq_recs} AS r WHERE r.lmk_key = c.lmk_key) AS recommendations
FROM {fq_cert} AS c;
""".strip()


def non_domestic_cert_with_recs_view_sql(project: str, dataset: str) -> str:
    """
    Denormalized non-domestic view: each certificate with an ARRAY of recommendation payloads.
    """
    fq_cert = f"`{project}.{dataset}.enr_non_domestic_certificates_v`"
    fq_recs = f"`{project}.{dataset}.{NON_DOMESTIC_RECS_RAW_TABLE}`"
    fq_view = f"`{project}.{dataset}.enr_non_domestic_cert_with_recs_v`"
    return f"""
CREATE OR REPLACE VIEW {fq_view} AS
SELECT
  c.*,
  (SELECT ARRAY_AGG(r.payload) FROM {fq_recs} AS r WHERE r.lmk_key = c.lmk_key) AS recommendations
FROM {fq_cert} AS c;
""".strip()


__all__ = [
    # raw tables / config
    "DOMESTIC_RAW_TABLE",
    "NON_DOMESTIC_RAW_TABLE",
    "DOMESTIC_RECS_RAW_TABLE",
    "NON_DOMESTIC_RECS_RAW_TABLE",
    "PARTITION_FIELD",
    "DOMESTIC_CLUSTERING",
    "NON_DOMESTIC_CLUSTERING",
    "get_raw_schema",
    # views
    "domestic_curated_view_sql",
    "non_domestic_curated_view_sql",
    "domestic_latest_by_lmk_view_sql",
    "non_domestic_latest_by_lmk_view_sql",
    "domestic_recommendations_view_sql",
    "non_domestic_recommendations_view_sql",
    "domestic_cert_with_recs_view_sql",
    "non_domestic_cert_with_recs_view_sql",
]
