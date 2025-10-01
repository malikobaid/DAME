

-- DAME: Combined certificates (domestic + non‑domestic) with recommendations
-- Sources:
--   - {{PROJECT}}.{{DATASET}}.enr_domestic_certificates_v
--   - {{PROJECT}}.{{DATASET}}.enr_non_domestic_certificates_v
--   - {{PROJECT}}.{{DATASET}}.domestic_recommendations_raw_json
--   - {{PROJECT}}.{{DATASET}}.non_domestic_recommendations_raw_json
-- Idempotent: CREATE OR REPLACE VIEW
-- Notes:
--   - Produces a unified schema with a `kind` discriminator.
--   - Recommendations are returned as ARRAY<JSON> named `recommendations`.
--   - Domestic‑specific cols are NULL for non‑domestic rows and vice‑versa.

CREATE OR REPLACE VIEW `{{PROJECT}}.{{DATASET}}.enr_combined_certs_with_recs_v` AS
-- Domestic branch
SELECT
  'domestic' AS kind,
  c.lmk_key,
  c.lodgement_date,
  c.postcode,
  c.uprn,
  -- Domestic-only fields
  c.current_energy_efficiency,
  c.potential_energy_efficiency,
  c.address1,
  c.address2,
  c.property_type,
  c.built_form,
  c.main_heating_controls,
  c.main_fuel,
  c.main_heating_description,
  c.hot_water_description,
  c.co2_emissions_current,
  -- Non-domestic placeholders
  CAST(NULL AS STRING)  AS building_category,
  CAST(NULL AS STRING)  AS lodgement_type,
  CAST(NULL AS FLOAT64) AS asset_rating,
  CAST(NULL AS FLOAT64) AS co2_emissions,
  -- Recommendations as ARRAY<JSON>
  (
    SELECT ARRAY_AGG(r.payload)
    FROM `{{PROJECT}}.{{DATASET}}.domestic_recommendations_raw_json` AS r
    WHERE r.lmk_key = c.lmk_key
  ) AS recommendations
FROM `{{PROJECT}}.{{DATASET}}.enr_domestic_certificates_v` AS c

UNION ALL

-- Non-domestic branch
SELECT
  'non-domestic' AS kind,
  c.lmk_key,
  c.lodgement_date,
  c.postcode,
  c.uprn,
  -- Domestic placeholders
  CAST(NULL AS INT64)   AS current_energy_efficiency,
  CAST(NULL AS INT64)   AS potential_energy_efficiency,
  CAST(NULL AS STRING)  AS address1,
  CAST(NULL AS STRING)  AS address2,
  CAST(NULL AS STRING)  AS property_type,
  CAST(NULL AS STRING)  AS built_form,
  CAST(NULL AS STRING)  AS main_heating_controls,
  CAST(NULL AS STRING)  AS main_fuel,
  CAST(NULL AS STRING)  AS main_heating_description,
  CAST(NULL AS STRING)  AS hot_water_description,
  CAST(NULL AS FLOAT64) AS co2_emissions_current,
  -- Non-domestic-only fields
  c.building_category,
  c.lodgement_type,
  c.asset_rating,
  c.co2_emissions,
  -- Recommendations as ARRAY<JSON>
  (
    SELECT ARRAY_AGG(r.payload)
    FROM `{{PROJECT}}.{{DATASET}}.non_domestic_recommendations_raw_json` AS r
    WHERE r.lmk_key = c.lmk_key
  ) AS recommendations
FROM `{{PROJECT}}.{{DATASET}}.enr_non_domestic_certificates_v` AS c;