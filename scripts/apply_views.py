from __future__ import annotations

"""
Apply (create or replace) all BigQuery views found under a views directory.

- Replaces placeholders {{PROJECT}} and {{DATASET}} in each SQL file.
- Executes each file as a separate BigQuery job (idempotent).
- Prints a JSON summary with per-file status.

Defaults are taken from dame_epc.settings (.env + env), but can be overridden
via CLI flags.

Examples
--------
# Use settings from .env and apply all views under the default path
python -m scripts.apply_views

# Apply views from a custom directory to a different dataset (dry run)
python -m scripts.apply_views --views-dir cloud/.../sql/views --dataset dame_epc_dev --dry-run

# Only apply files whose names contain "domestic"
python -m scripts.apply_views --only domestic
"""

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional

from google.cloud import bigquery

from dame_epc.io_utils import ensure_dataset
from dame_epc.logging_setup import get_logger, setup_logging
from dame_epc.settings import Settings, load_settings, settings as _settings


@dataclass
class ViewResult:
    file: str
    view_name: Optional[str]
    status: str           # "applied" | "skipped" | "dry-run" | "error"
    error: Optional[str] = None
    bytes_processed: Optional[int] = None
    job_id: Optional[str] = None


def _discover_sql_files(views_dir: Path, only: Optional[str]) -> List[Path]:
    if not views_dir.exists():
        raise FileNotFoundError(f"Views directory not found: {views_dir}")
    files = sorted(p for p in views_dir.glob("*.sql"))
    if only:
        files = [p for p in files if only.lower() in p.name.lower()]
    return files


def _render_sql(sql_text: str, project: str, dataset: str) -> str:
    return sql_text.replace("{{PROJECT}}", project).replace("{{DATASET}}", dataset)


def _extract_view_name(sql_text: str) -> Optional[str]:
    """
    Best-effort parse to get the view FQN between 'CREATE OR REPLACE VIEW' and 'AS'.
    """
    low = sql_text.lower()
    marker = "create or replace view"
    if marker not in low:
        return None
    try:
        after = sql_text[low.index(marker) + len(marker):]
        # Split on AS (case-insensitive)
        idx_as = after.lower().index(" as")
        name = after[:idx_as].strip()
        # Remove trailing semicolons/backticks whitespace
        return " ".join(name.split())
    except Exception:
        return None


def _apply_sql_file(
    client: bigquery.Client,
    sql_path: Path,
    project: str,
    dataset: str,
    region: str,
    dry_run: bool,
) -> ViewResult:
    text = sql_path.read_text(encoding="utf-8")
    sql = _render_sql(text, project, dataset)
    view_name = _extract_view_name(sql)

    if dry_run:
        return ViewResult(file=str(sql_path), view_name=view_name, status="dry-run")

    cfg = bigquery.QueryJobConfig(location=region)
    # Labels make it easy to filter in INFORMATION_SCHEMA and Monitoring
    cfg.labels = {
        "system": "dame",
        "stage": "views",
        "file": sql_path.stem[:58].replace("-", "_"),  # label key/value length limits
    }

    job = client.query(sql, job_config=cfg)
    result = job.result()  # wait
    return ViewResult(
        file=str(sql_path),
        view_name=view_name,
        status="applied",
        bytes_processed=getattr(result, "total_bytes_processed", None),
        job_id=job.job_id,
    )


def run(views_dir: Path, s: Settings, only: Optional[str], dry_run: bool) -> List[ViewResult]:
    setup_logging()
    log = get_logger(__name__, component="apply_views", project=s.project_id, dataset=s.dataset_enr)

    ensure_dataset(s.project_id, s.dataset_raw, s.bq_location)  # ensure dataset exists (views can be in same ds)

    client = bigquery.Client(project=s.project_id)

    files = _discover_sql_files(views_dir, only)
    if not files:
        log.warning("no .sql files found", extra={"dir": str(views_dir), "only": only or ""})
        return []

    log.info("applying views", extra={"count": len(files), "dir": str(views_dir), "dataset": s.dataset_enr})

    results: List[ViewResult] = []
    for f in files:
        try:
            res = _apply_sql_file(client, f, s.project_id, s.dataset_enr, s.bq_location, dry_run)
            results.append(res)
            log.info("view processed", extra={"file": f.name, "status": res.status})
        except Exception as e:
            log.exception("failed to apply view", extra={"file": f.name})
            results.append(ViewResult(file=str(f), view_name=None, status="error", error=str(e)))

    # Print JSON for CI/pipeline logs
    print(json.dumps([asdict(r) for r in results], indent=2))
    return results


def _parse_args() -> tuple[Path, Settings, Optional[str], bool]:
    import argparse

    parser = argparse.ArgumentParser(description="Apply BigQuery views from SQL files.")
    parser.add_argument("--views-dir", default="cloud/dame-prod-473718/bigquery/sql/views", help="Directory with .sql files.")
    parser.add_argument("--env-file", default=None, help="Optional path to .env (overrides ENV_FILE).")
    parser.add_argument("--project", default=None, help="Override project id.")
    parser.add_argument("--dataset", default=None, help="Override dataset id (views created here).")
    parser.add_argument("--region", default=None, help="Override BigQuery location.")
    parser.add_argument("--only", default=None, help="Apply only files whose names contain this substring (case-insensitive).")
    parser.add_argument("--dry-run", action="store_true", help="Do not execute queries; print plan only.")

    args = parser.parse_args()

    # Load settings (optionally from a custom env file)
    if args.env_file:
        os.environ["ENV_FILE"] = args.env_file
        s = load_settings()
    else:
        s = _settings

    # CLI overrides
    if args.project:
        s.project_id = args.project  # type: ignore[attr-defined]
    if args.dataset:
        s.dataset_enr = args.dataset  # type: ignore[attr-defined]
    if args.region:
        s.region = args.region  # type: ignore[attr-defined]

    return Path(args.views_dir), s, args.only, bool(args.dry_run)


if __name__ == "__main__":
    views_dir, s, only, dry_run = _parse_args()
    run(views_dir, s, only, dry_run)