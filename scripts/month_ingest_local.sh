#!/usr/bin/env bash
# DAME — Local monthly ingest helper
#
# Purpose:
#   Thin wrapper around the Python orchestrator to run monthly EPC ingestion
#   on your machine with a single command. It reads configuration from a .env file
#   (via ENV_FILE) and optionally applies the BigQuery views afterwards.
#
# Usage examples:
#   ./scripts/month_ingest_local.sh --env .env \
#     --start 2024-01 --end 2024-03 --kinds domestic --with-recs --apply-views
#
#   # Dry-run plan only (no network, no writes)
#   ./scripts/month_ingest_local.sh --env .env --start 2024-01 --end 2024-01 --dry-run
#
#   # Apply views only (no ingestion)
#   ./scripts/month_ingest_local.sh --env .env --only-views
#
# Notes:
#   - Ensure your Google ADC is set up locally (gcloud auth application-default login).
#   - Make the script executable once: chmod +x scripts/month_ingest_local.sh

set -Eeuo pipefail

# ------------------------------------------------------------
# Paths & defaults
# ------------------------------------------------------------
SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="${SCRIPT_DIR%/scripts}"

ENV_FILE="${ENV_FILE:-.env}"          # can be overridden via --env
START=""
END=""
KINDS="domestic,non-domestic"
WITH_RECS=0
DRY_RUN=0
APPLY_VIEWS=0
ONLY_VIEWS=0
VIEWS_DIR="${REPO_ROOT}/cloud/dame-prod-473718/bigquery/sql/views"

PYTHON_BIN="${PYTHON:-python3}"

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[33m%s\033[0m\n' "$*"; }

usage() {
  cat <<'EOF'
DAME — Local monthly ingest helper

Flags:
  --env PATH             Path to .env file (default: ./.env or ENV_FILE env)
  --start YYYY-MM        Inclusive start month (default: from .env START_MONTH)
  --end YYYY-MM          Inclusive end month (default: from .env END_MONTH)
  --kinds LIST           Comma-separated: domestic,non-domestic (default: both)
  --with-recs            Also fetch per-LMK recommendations (default: off)
  --dry-run              Show plan only; do not call EPC/GCS/BQ
  --apply-views          After ingest, apply SQL views under the views dir
  --only-views           Apply views only; skip ingestion
  --views-dir PATH       Path to views SQL directory
  --python PATH          Python executable to use (default: python3)
  -h, --help             Show this help

Examples:
  ./scripts/month_ingest_local.sh --env .env --start 2024-01 --end 2024-02 --kinds domestic --with-recs --apply-views
  ./scripts/month_ingest_local.sh --env .env --only-views
EOF
}

# ------------------------------------------------------------
# Parse args
# ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)         ENV_FILE="$2"; shift 2 ;;
    --start)       START="$2"; shift 2 ;;
    --end)         END="$2"; shift 2 ;;
    --kinds)       KINDS="$2"; shift 2 ;;
    --with-recs)   WITH_RECS=1; shift 1 ;;
    --dry-run)     DRY_RUN=1; shift 1 ;;
    --apply-views) APPLY_VIEWS=1; shift 1 ;;
    --only-views)  ONLY_VIEWS=1; shift 1 ;;
    --views-dir)   VIEWS_DIR="$2"; shift 2 ;;
    --python)      PYTHON_BIN="$2"; shift 2 ;;
    -h|--help)     usage; exit 0 ;;
    *)
      red "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

# ------------------------------------------------------------
# Validations
# ------------------------------------------------------------
if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  red "Python not found at '${PYTHON_BIN}'. Set --python PATH or export PYTHON."
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  yellow "ENV file not found at '${ENV_FILE}'. The orchestrator will fall back to process env."
fi

if [[ ! -d "${VIEWS_DIR}" ]]; then
  yellow "Views directory not found at '${VIEWS_DIR}'. You can set --views-dir PATH."
fi

export ENV_FILE  # consumed by dame_epc.settings

# ------------------------------------------------------------
# Build orchestrator args
# ------------------------------------------------------------
ORCH_ARGS=()
[[ -n "${START}" ]] && ORCH_ARGS+=(--start "${START}")
[[ -n "${END}"   ]] && ORCH_ARGS+=(--end "${END}")
[[ -n "${KINDS}" ]] && ORCH_ARGS+=(--kinds "${KINDS}")
[[ "${WITH_RECS}" -eq 1 ]] && ORCH_ARGS+=(--with-recs)
[[ "${DRY_RUN}"  -eq 1 ]] && ORCH_ARGS+=(--dry-run)

# ------------------------------------------------------------
# Run
# ------------------------------------------------------------
if [[ "${ONLY_VIEWS}" -eq 1 ]]; then
  green "Applying views only from: ${VIEWS_DIR}"
  "${PYTHON_BIN}" -m scripts.apply_views --views-dir "${VIEWS_DIR}" --env-file "${ENV_FILE}"
  green "Views applied."
  exit 0
fi

green "Running orchestrator with args: ${ORCH_ARGS[*]}"
"${PYTHON_BIN}" -m dame_epc.main "${ORCH_ARGS[@]}"

if [[ "${APPLY_VIEWS}" -eq 1 ]]; then
  green "Applying views from: ${VIEWS_DIR}"
  "${PYTHON_BIN}" -m scripts.apply_views --views-dir "${VIEWS_DIR}" --env-file "${ENV_FILE}"
  green "Views applied."
fi

green "Done."
