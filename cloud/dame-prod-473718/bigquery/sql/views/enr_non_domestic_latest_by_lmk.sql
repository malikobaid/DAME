

-- DAME: Latest non‑domestic certificate per LMK key
-- Source: {{PROJECT}}.{{DATASET}}.enr_non_domestic_certificates_v
-- Idempotent: CREATE OR REPLACE VIEW

CREATE OR REPLACE VIEW `{{PROJECT}}.{{DATASET}}.enr_non_domestic_latest_by_lmk` AS
SELECT
  *
FROM `{{PROJECT}}.{{DATASET}}.enr_non_domestic_certificates_v`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY lmk_key
  -- Prefer non-NULL dates, then newest first (tiebreaker on lmk_key for determinism)
  ORDER BY (lodgement_date IS NULL), lodgement_date DESC, lmk_key
) = 1;