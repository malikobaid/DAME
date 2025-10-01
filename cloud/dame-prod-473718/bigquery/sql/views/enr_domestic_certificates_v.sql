-- DAME: Curated domestic certificates view
-- Source: {{PROJECT}}.{{DATASET}}.domestic_raw_json (partitioned on lodgement_date)
-- Idempotent: CREATE OR REPLACE VIEW

CREATE OR REPLACE VIEW `{{PROJECT}}.{{DATASET}}.enr_domestic_certificates_v` AS
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
FROM `{{PROJECT}}.{{DATASET}}.domestic_raw_json`;
