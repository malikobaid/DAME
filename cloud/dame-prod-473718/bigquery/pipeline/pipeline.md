

# DAME — BigQuery Pipeline (Prod)  
Project: `dame-prod-473718` • Region: `europe-west2` • Dataset: `dame_epc` • Bucket: `gs://dame-epc-london`

This document is the **single source of truth** for wiring the DAME EPC ingestion in **BigQuery Studio Pipelines**.  
It assumes the code + SQL in this repo and schedules a daily run at **08:00 Europe/London**.

---

## 0) Repo layout (relevant paths)

```
cloud/dame-prod-473718/bigquery/notebooks/ingest_epc.ipynb
cloud/dame-prod-473718/bigquery/sql/views/*.sql
app/dame_epc/...
scripts/apply_views.py                  # optional (alternative to SQL tasks)
```

---

## 1) Prerequisites

1. **BigQuery dataset**: `dame_epc` in `europe-west2`.
2. **GCS bucket**: `dame-epc-london` in `europe-west2`.
3. **EPC API credentials** in execution environment  
   - For Prod, prefer **Secret Manager**:
     - `projects/dame-prod-473718/secrets/EPC_EMAIL`
     - `projects/dame-prod-473718/secrets/EPC_API_KEY`
   - Or set as **environment variables** on the pipeline runtime (dev-only).
4. **GitHub Repository connected** in *BigQuery Studio → Repositories*  
   - Connect this repo/branch so the pipeline can run `ingest_epc.ipynb` and apply SQL.

---

## 2) Service Account & IAM

Create a runner service account, e.g. `bq-pipeline-runner@dame-prod-473718.iam.gserviceaccount.com`.

Grant:
- **Project roles** (or least-privilege at resource level):
  - `roles/bigquery.jobUser` (project)
  - `roles/bigquery.dataEditor` (on dataset `dame_epc`)
  - `roles/storage.objectAdmin` (on bucket `dame-epc-london`)
  - `roles/secretmanager.secretAccessor` (if using Secret Manager)
  - `roles/logging.logWriter` (implicit on many runners, ensures logs)

> If you will let the notebook install packages, also grant **Artifact Registry Reader** where appropriate.

---

## 3) Parameters (Pipeline-level)

Define these **Pipeline parameters** so we can reuse the same pipeline for dev/changes:

| Name            | Example             | Purpose                                     |
|-----------------|---------------------|---------------------------------------------|
| `PROJECT`       | `dame-prod-473718`  | GCP project id                              |
| `REGION`        | `europe-west2`      | BQ job location                             |
| `DATASET`       | `dame_epc`          | BigQuery dataset (raw + views)              |
| `BUCKET`        | `dame-epc-london`   | Landing bucket                              |
| `START_MONTH`   | `2024-01`           | Inclusive start YYYY-MM                      |
| `END_MONTH`     | `2024-01`           | Inclusive end YYYY-MM                        |
| `WITH_RECS`     | `false`             | Whether to fetch recommendations             |

> For a **rolling monthly** run, set `START_MONTH`/`END_MONTH` to the **previous month**; the notebook includes a small helper to compute that if left blank (see Section 6).

---

## 4) Tasks & Graph

We implement three tasks. `A → B → C` (sequential). Recommendations can be **D** and depend on A and B if enabled.

```mermaid
flowchart LR
  A[Notebook: Ingest Domestic] --> B[Notebook: Ingest Non‑Domestic]
  B --> C[SQL: Apply Views]
  C -->|optional| D[Notebook: Recommendations (both kinds)]
```

### Task A — Notebook: Ingest Domestic
- Notebook: `cloud/dame-prod-473718/bigquery/notebooks/ingest_epc.ipynb`
- Parameters passed into the notebook (see Section 6):  
  ```
  kind="domestic"
  start_month=${START_MONTH}
  end_month=${END_MONTH}
  project_id=${PROJECT}
  region=${REGION}
  bucket=${BUCKET}
  dataset=${DATASET}
  with_recs=${WITH_RECS}
  ```
- Timeout: **2h**
- Retries: **3**
- Service account: **pipeline runner** (above)

### Task B — Notebook: Ingest Non‑Domestic
- Same notebook + params, but `kind="non-domestic"`

### Task C — SQL: Apply Views
- For each file in `cloud/dame-prod-473718/bigquery/sql/views/*.sql`:
  - Replace `{{PROJECT}}` and `{{DATASET}}` with pipeline params
  - Execute with `--location=europe-west2`
- Example SQL (idempotent):
  ```sql
  CREATE OR REPLACE VIEW `{{PROJECT}}.{{DATASET}}.enr_domestic_certificates_v` AS
  SELECT * FROM `{{PROJECT}}.{{DATASET}}.domestic_raw_json` LIMIT 0; -- placeholder, actual file used
  ```
- Prefer a **SQL task per file** to see per-view status in logs.

### Task D — Notebook: Recommendations (optional)
- Only if `${WITH_RECS} == true`
- The same notebook can call `bulk_recommendations.run_month_incremental` for each kind/month after certs.

---

## 5) Schedule & Triggers

- **Schedule**: Daily at **08:00 Europe/London** (cron in UTC: `0 7 * * *`)
- **Catch-up**: disabled (avoid reruns if the console was down)
- **Max concurrent runs**: 1
- **On failure**: alert via Cloud Monitoring (Policy: “DAME Pipeline Failures” on `bigquery.googleapis.com/queries/failed_count` filtered by job labels)

---

## 6) Notebook parameterization (how the pipeline passes values)

`ingest_epc.ipynb` should read pipeline-supplied parameters. Use a top cell like this:

```python
# Parameters (BigQuery Studio injects these when configured on the task)
PROJECT_ID = globals().get("project_id") or "dame-prod-473718"
REGION     = globals().get("region") or "europe-west2"
BUCKET     = globals().get("bucket") or "dame-epc-london"
DATASET    = globals().get("dataset") or "dame_epc"
KIND       = globals().get("kind") or "domestic"  # or "non-domestic"
WITH_RECS  = str(globals().get("with_recs") or "false").lower() in {"1","true","yes"}

START_MONTH = globals().get("start_month") or ""
END_MONTH   = globals().get("end_month") or ""

# If months are blank, default to previous calendar month
import datetime as _dt
def prev_month():
    today = _dt.date.today().replace(day=1)
    last = today - _dt.timedelta(days=1)
    return f"{last.year:04d}-{last.month:02d}"

if not START_MONTH or not END_MONTH:
    START_MONTH = END_MONTH = prev_month()
```

> The remaining cells can import `dame_epc.settings` and override its values or just **call the module functions directly** with these local variables (recommended).

Example call cell inside the notebook:
```python
from dame_epc.domestic import run_month as run_dom
from dame_epc.nondomestic import run_month as run_nd
from dame_epc.bulk_recommendations import run_month_incremental as run_recs

if KIND == "domestic":
    print(run_dom(START_MONTH))
elif KIND == "non-domestic":
    print(run_nd(START_MONTH))

if WITH_RECS:
    print(run_recs(KIND, START_MONTH))
```

---

## 7) Apply Views (SQL files)

Keep the canonical view DDLs in version control under:
```
cloud/dame-prod-473718/bigquery/sql/views/
```
Each file uses placeholders:
```
CREATE OR REPLACE VIEW `{{PROJECT}}.{{DATASET}}.enr_domestic_certificates_v` AS
-- ...
```
The pipeline’s **Task C** replaces placeholders and runs them with location `europe-west2`.

---

## 8) Labels & lineage

Add these **job labels** in each task for observability:
- `system=dame`
- `stage=ingest`
- `kind=domestic|non_domestic|views|recs`
- `month=${START_MONTH}` (for A/B/D)

This lets you filter Jobs/bytes in INFORMATION_SCHEMA and in Monitoring.

---

## 9) Cost guardrails

- **Partition on `lodgement_date`**, **cluster** by `lmk_key` (+ `postcode,uprn` for domestic).
- Query **views** instead of raw (`SELECT columns` not `SELECT *`).
- Keep raw NDJSON gz in GCS; set object lifecycle (e.g., move to Nearline after 90 days).

---

## 10) Troubleshooting

- **401 Unauthorized (EPC)**: validate EPC_EMAIL/EPC_API_KEY on the runner SA env / Secret Manager.
- **BQ “Incompatible table partitioning/clustering”**: you attempted to recreate a table with different options.  
  Fix: **drop the table once** or load into a new table name; first creation sets partitioning & clustering.
- **CSV parsing errors**: not applicable in this flow (we land **JSON**).
- **No rows**: some months legitimately have no certs → checkpoints still mark “no-data”.

---

## 11) Runbook (manual)

- **Backfill a month**: set `START_MONTH=END_MONTH=YYYY-MM` and run pipeline once.
- **Reset month**: delete the checkpoint objects under `gs://dame-epc-london/state/{kind}/{YYYYMM}/` then re-run.
- **Recommendations only**: set `WITH_RECS=true` and run Task D (or run the CLI locally).

---

## 12) Security

- Secrets in **Secret Manager**, **not** in git or notebooks.
- SA has least privilege; bind dataset-level roles, not project-wide where possible.
- Enable **Cloud Logging**; optionally enable **Data Access logs** for BigQuery datasets.

---

**Owner:** DAME Data Eng • contact: `#dame-data` • last updated: _commit history_