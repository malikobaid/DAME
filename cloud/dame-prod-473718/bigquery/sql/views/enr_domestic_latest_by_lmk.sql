

-- DAME: Latest domestic certificate per LMK key
-- Source: {{PROJECT}}.{{DATASET}}.enr_domestic_certificates_v
-- Idempotent: CREATE OR REPLACE VIEW

CREATE OR REPLACE VIEW `{{PROJECT}}.{{DATASET}}.enr_domestic_latest_by_lmk` AS
SELECT
  *
FROM `{{PROJECT}}.{{DATASET}}.enr_domestic_certificates_v`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY lmk_key
  -- Ensure non-NULL dates are preferred, then newest first
  ORDER BY (lodgement_date IS NULL), lodgement_date DESC, lmk_key
) = 1;