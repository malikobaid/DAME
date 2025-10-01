-- DAME: Non‑domestic recommendations view
-- Source: {{PROJECT}}.{{DATASET}}.non_domestic_recommendations_raw_json
-- Idempotent: CREATE OR REPLACE VIEW

CREATE OR REPLACE VIEW `{{PROJECT}}.{{DATASET}}.enr_non_domestic_recommendations_v` AS
SELECT
  lmk_key,
  lodgement_date,
  JSON_VALUE(payload, '$."improvement-description"') AS improvement_description,
  JSON_VALUE(payload, '$."indicative-cost"')         AS indicative_cost,
  JSON_VALUE(payload, '$."typical-saving"')          AS typical_saving,
  payload
FROM `{{PROJECT}}.{{DATASET}}.non_domestic_recommendations_raw_json`;
