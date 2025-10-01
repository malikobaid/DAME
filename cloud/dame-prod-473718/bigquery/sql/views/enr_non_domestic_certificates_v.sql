

-- DAME: Curated non‑domestic certificates view
-- Source: {{PROJECT}}.{{DATASET}}.non_domestic_raw_json (partitioned on lodgement_date)
-- Idempotent: CREATE OR REPLACE VIEW

CREATE OR REPLACE VIEW `{{PROJECT}}.{{DATASET}}.enr_non_domestic_certificates_v` AS
SELECT
  lmk_key,
  lodgement_date,
  postcode,
  uprn,
  JSON_VALUE(payload, '$."building-category"')                                 AS building_category,
  JSON_VALUE(payload, '$."lodgement-type"')                                    AS lodgement_type,
  SAFE_CAST(JSON_VALUE(payload, '$."asset-rating"') AS FLOAT64)                AS asset_rating,
  SAFE_CAST(JSON_VALUE(payload, '$."co2-emissions"') AS FLOAT64)               AS co2_emissions
FROM `{{PROJECT}}.{{DATASET}}.non_domestic_raw_json`;