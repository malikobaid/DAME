

# DAME — EPC → BigQuery Ingestion & Enrichment

Industrial‑grade, minimal‑ops pipeline to ingest **UK EPC** (Energy Performance Certificate) data from the _Open Data Communities_ API, land it as **JSON** in **GCS**, and load into **BigQuery** with curated **views** ready for analytics and GenAI/RAG.

---

## Why this exists

- **JSON in, JSON out**: Avoids brittle CSV parsing and schema drift.
- **Idempotent**: Checkpoints in GCS prevent double work.
- **Partitioned & clustered**: Cheap scans and fast filters in BigQuery.
- **Separation of concerns**: HTTP client, loaders, orchestration, SQL views are cleanly split.
- **One‑command local run** or **BigQuery Studio Pipeline** (scheduled 08:00 Europe/London).

---

## High‑level architecture

```
EPC API (JSON)
    │
    ▼
[domestic.py | nondomestic.py]  ── normalize → {lmk_key, lodgement_date, postcode, uprn, payload}
    │
    ├─► GCS: epc/json/{kind}/{YYYYMM}/certs/part-0001.json.gz
    │
    └─► BigQuery: {kind}_raw_json (partition by lodgement_date; cluster on lmk_key, etc.)
                         │
                         ├─► Views: enr_*_certificates_v, enr_*_latest_by_lmk
                         └─► (optional) Recommendations → *_recommendations_raw_json → views
```

---

## Repo layout (key paths)

```
app/dame_epc/
  ├─ settings.py                 # .env + env loader
  ├─ schema.py                   # table names, landing schema, view SQL helpers
  ├─ epc_api.py                  # HTTP-only EPC client (search-after, retries)
  ├─ io_utils.py                 # GCS & BigQuery helpers
  ├─ logging_setup.py            # JSON logging
  ├─ state.py                    # GCS checkpoints (is_done/mark_done/clear)
  ├─ domestic.py                 # monthly domestic ingest (JSON → GCS → BQ)
  ├─ nondomestic.py              # monthly non-domestic ingest
  ├─ bulk_recommendations.py     # per‑LMK or yearly ZIP recommendations
  └─ main.py                     # orchestrator (loop months/kinds, checkpoints)

cloud/dame-prod-473718/
  └─ bigquery/
      ├─ pipeline/pipeline.md    # BQ Studio Pipeline playbook (Prod)
      └─ sql/views/              # curated view DDLs (templated with {{PROJECT}}, {{DATASET}})

scripts/
  ├─ apply_views.py              # create/replace all views from SQL files
  └─ month_ingest_local.sh       # local helper to run orchestrator + apply views

README.md
```

---

## Prerequisites

- **Google Cloud project** (e.g., `<your-gcp-project-id>`) with:
  - BigQuery enabled (location: `<your-gcp-region>`),
  - GCS bucket (e.g., `<your-gcs-bucket>` in `<your-gcp-region>`).
- **EPC API credentials** from Open Data Communities: email + API key.
- **gcloud** CLI (logged in) and **Application Default Credentials (ADC)** configured.

```bash
gcloud auth login
gcloud config set project <your-gcp-project-id>
gcloud auth application-default login
```

> For production runners, prefer **Secret Manager** to hold `EPC_EMAIL` and `EPC_API_KEY`.

---

## Setup (local)

```bash
# 1) Create & activate a virtualenv
python3 -m venv .venv
source .venv/bin/activate

# 2) Install deps
pip install -U pip
pip install -r requirements.txt   # or your chosen lock file

# 3) Create .env (see example below)
cp .env.example .env  # then edit values

# 4) Ensure ADC is configured
gcloud auth application-default login
```

### `.env` example

```dotenv
# GCP Configuration
PROJECT_ID=<your-gcp-project-id>
REGION=<your-gcp-region>
BUCKET=<your-gcs-bucket>

# BigQuery Configuration
BQ_DATASET=<your-bq-dataset>

# EPC API Credentials (REQUIRED)
EPC_EMAIL=your-email@example.com
EPC_API_KEY=your-epc-api-key-here

# Data Processing Configuration
START_YEAR=2010
END_YEAR=2025
PAGE_SIZE=5000
FORMAT=csv
RUN_MODE=incremental
MAX_CONCURRENCY=2

# Request Configuration
REQUEST_TIMEOUT_SECONDS=60
RETRY_MAX=5
RETRY_BACKOFF=2.0
```

> Raw and enriched datasets can be the same (`dame_epc`). Views are created in `DATASET_ENR`.

---

## Quickstart — one command

```bash
chmod +x scripts/month_ingest_local.sh

# Run a month (or range), include recommendations, then apply views
./scripts/month_ingest_local.sh \
  --env .env \
  --start 2024-01 --end 2024-01 \
  --kinds domestic,non-domestic \
  --with-recs \
  --apply-views
```

This will:
1. Ingest certs month‑by‑month (`domestic.py` and `nondomestic.py`).
2. Optionally fetch **recommendations** per LMK for the same month.
3. Write **NDJSON.gz** to GCS under `epc/json/{kind}/{YYYYMM}/…`.
4. Load into partitioned/clustered BigQuery raw tables.
5. **Apply curated views** in `cloud/.../sql/views/`.

Checkpoints are written to:
```
gs://<BUCKET>/state/{kind}/{YYYYMM}/{certs|recs}.json
```

---

## Orchestrator (Python)

You can also call the orchestrator directly:

```bash
python -m dame_epc.main \
  --start 2024-01 --end 2024-03 \
  --kinds domestic,non-domestic \
  --with-recs \
  --env-file .env
```

Flags:
- `--dry-run` prints the plan only.
- `--reset certs|recs` clears checkpoints for the window before running.

---

## BigQuery views

View SQL lives under `cloud/dame-prod-473718/bigquery/sql/views/` and uses placeholders:

- `enr_domestic_certificates_v.sql`
- `enr_domestic_latest_by_lmk.sql`
- `enr_non_domestic_certificates_v.sql`
- `enr_non_domestic_latest_by_lmk.sql`
- `enr_domestic_recommendations_v.sql`
- `enr_non_domestic_recommendations_v.sql`
- `enr_combined_certs_with_recs_v.sql`

Apply them with:

```bash
python -m scripts.apply_views --env-file .env
# or only specific ones
python -m scripts.apply_views --only domestic
```

---

## BigQuery Studio Pipeline (production)

See: `cloud/dame-prod-473718/bigquery/pipeline/pipeline.md` for a ready‑to‑wire daily pipeline at **08:00 Europe/London** using:
- Task A: Notebook Ingest **Domestic**
- Task B: Notebook Ingest **Non‑Domestic**
- Task C: **SQL** apply views
- Task D (optional): **Recommendations**

---

## Data model (landing schema)

Landing schema for raw tables (both kinds):

| Column          | Type   | Mode     | Notes                                  |
|-----------------|--------|----------|----------------------------------------|
| lmk_key         | STRING | REQUIRED | Certificate LMK (primary key surrogate)|
| lodgement_date  | DATE   | NULLABLE | Partition field (if present)           |
| postcode        | STRING | NULLABLE |                                        |
| uprn            | STRING | NULLABLE | Treat as string to avoid int pitfalls  |
| payload         | JSON   | NULLABLE | Full source record                     |

Tables:
- `domestic_raw_json` (cluster: `lmk_key, postcode, uprn`)
- `non_domestic_raw_json` (cluster: `lmk_key`)
- `domestic_recommendations_raw_json` (cluster: `lmk_key`)
- `non_domestic_recommendations_raw_json` (cluster: `lmk_key`)

---

## Troubleshooting

- **EPC 401 Unauthorized**  
  Check `EPC_EMAIL` / `EPC_API_KEY` and that the runner has access to those secrets.

- **ADC: “default credentials not found”**  
  Run `gcloud auth application-default login`. In CI, mount a service account key or use Workload Identity.

- **BigQuery: “Incompatible partitioning/clustering”**  
  First creation of a table sets these options. If you need to change them, **drop the table once** or load into a new name.

- **Too many CSV errors**  
  Not applicable: we ingest **JSON** and use `NEWLINE_DELIMITED_JSON`.

- **No data for a month**  
  That’s valid; checkpoints mark `no-data` so it won’t re-run.

- **Recommendations missing**  
  API may not have recs for every LMK; that’s expected. We still load certs.

---

## Security & cost notes

- Store credentials in **Secret Manager** for production. Avoid committing secrets.
- BigQuery **partition on `lodgement_date`** + **clustering** reduces query cost.
- Consider GCS lifecycle rules (e.g., Nearline after 90 days) for NDJSON.

---

## License & ownership

© DAME Project. Internal use; licensing TBD.

---