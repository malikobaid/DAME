

from __future__ import annotations

"""
Main orchestration for DAME EPC ingestion.

Responsibilities
---------------
- Read settings (from .env + env) and compute the month window
- For each kind in {domestic, non-domestic}:
    * Skip work if a checkpoint exists (idempotent)
    * Run the monthly certificate ingestion
    * Optionally run monthly recommendations ingestion
    * Write checkpoints with small JSON docs to GCS

Usage
-----
# Use settings from .env (default) and run both kinds for the configured window:
python -m dame_epc.main

# Override window and include only domestic with recommendations:
python -m dame_epc.main --start 2024-01 --end 2024-03 --kinds domestic --with-recs

# Dry run (show plan, no network calls):
python -m dame_epc.main --dry-run

# Reset a step's checkpoint (e.g., re-run recs for a month):
python -m dame_epc.main --kinds domestic --start 2024-01 --end 2024-01 --reset recs

Notes
-----
- Checkpoints live in GCS under: state/{kind}/{YYYYMM}/{step}.json
  where step is "certs" or "recs".
- Steps are sequential per month: certs -> recs (if enabled).
"""

import json
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Literal, Optional, Sequence, Tuple

from google.api_core.exceptions import GoogleAPICallError

from . import domestic as mod_domestic
from . import nondomestic as mod_nondomestic
from . import bulk_recommendations as mod_recs
from .io_utils import ensure_dataset
from .logging_setup import get_logger, setup_logging
from .settings import Settings, load_settings, settings
from .state import clear_checkpoint, is_done, mark_done

Kind = Literal["domestic", "non-domestic"]

STEPS: Tuple[str, str] = ("certs", "recs")


@dataclass
class RunOptions:
    kinds: List[Kind]
    start_month: str
    end_month: str
    with_recs: bool
    dry_run: bool
    reset_step: Optional[str] = None  # "certs" | "recs"


def _month_range(start: str, end: str) -> List[str]:
    # Local utility to avoid importing internal helpers
    ys, ms = map(int, start.split("-"))
    ye, me = map(int, end.split("-"))
    months = []
    y, m = ys, ms
    while True:
        months.append(f"{y:04d}-{m:02d}")
        if y == ye and m == me:
            break
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return months


def _ensure_env(s: Settings) -> None:
    # Ensure the raw dataset exists once (idempotent)
    ensure_dataset(s.project_id, s.dataset_raw, s.bq_location)


def _process_certs(kind: Kind, month: str, s: Settings, log) -> Dict[str, any]:
    # Skip if checkpoint exists
    if is_done(s.bucket, kind, month, "certs", project_id=s.project_id):
        log.info("skip: certs checkpoint exists", extra={"kind": kind, "month": month, "step": "certs"})
        return {"kind": kind, "month": month, "status": "skipped"}

    log.info("start: certificates", extra={"kind": kind, "month": month, "step": "certs"})
    if kind == "domestic":
        res = mod_domestic.run_month(month, s)
    else:
        res = mod_nondomestic.run_month(month, s)
    # Mark checkpoint even for no-data to avoid re-pulling empty months
    mark_done(
        s.bucket,
        kind,
        month,
        "certs",
        meta=res,
        project_id=s.project_id,
    )
    log.info("done: certificates", extra={"kind": kind, "month": month, "step": "certs", "rows": res.get("rows", 0)})
    return res


def _process_recs(kind: Kind, month: str, s: Settings, log) -> Dict[str, any]:
    if is_done(s.bucket, kind, month, "recs", project_id=s.project_id):
        log.info("skip: recs checkpoint exists", extra={"kind": kind, "month": month, "step": "recs"})
        return {"kind": kind, "month": month, "status": "skipped"}

    log.info("start: recommendations", extra={"kind": kind, "month": month, "step": "recs"})
    res = mod_recs.run_month_incremental(kind=kind, month=month, settings=s)
    mark_done(
        s.bucket,
        kind,
        month,
        "recs",
        meta=res,
        project_id=s.project_id,
    )
    log.info("done: recommendations", extra={"kind": kind, "month": month, "step": "recs", "rows": res.get("rows", 0)})
    return res


def _reset_step_if_requested(kind: Kind, month: str, step: str, s: Settings, log) -> None:
    try:
        clear_checkpoint(s.bucket, kind, month, step, project_id=s.project_id)
        log.info("checkpoint cleared", extra={"kind": kind, "month": month, "step": step})
    except GoogleAPICallError:
        # Non-fatal; proceed and let the next attempt re-create it
        log.warning("failed to clear checkpoint", extra={"kind": kind, "month": month, "step": step})


def run(opts: RunOptions, s: Settings) -> List[Dict[str, any]]:
    setup_logging()
    log = get_logger(__name__, component="orchestrator", project=s.project_id, dataset=s.dataset_raw)

    _ensure_env(s)

    months = _month_range(opts.start_month, opts.end_month)
    log.info(
        "plan",
        extra={
            "kinds": ",".join(opts.kinds),
            "start": opts.start_month,
            "end": opts.end_month,
            "months": len(months),
            "with_recs": opts.with_recs,
            "dry_run": opts.dry_run,
        },
    )

    results: List[Dict[str, any]] = []

    for month in months:
        for kind in opts.kinds:
            log.info("month/kind begin", extra={"kind": kind, "month": month})
            if opts.reset_step in {"certs", "recs"}:
                _reset_step_if_requested(kind, month, opts.reset_step, s, log)

            if opts.dry_run:
                log.info(
                    "dry-run: would run steps",
                    extra={"kind": kind, "month": month, "steps": "certs -> recs" if opts.with_recs else "certs"},
                )
                results.append({"kind": kind, "month": month, "status": "dry-run"})
                continue

            # Certificates
            try:
                res_c = _process_certs(kind, month, s, log)
                results.append(res_c)
            except Exception as e:
                log.exception("certificates step failed", extra={"kind": kind, "month": month})
                results.append({"kind": kind, "month": month, "step": "certs", "status": "error", "error": str(e)})
                # Continue to next kind/month; do not attempt recs if certs failed
                continue

            # Recommendations (optional)
            if opts.with_recs:
                try:
                    res_r = _process_recs(kind, month, s, log)
                    results.append(res_r)
                except Exception as e:
                    log.exception("recommendations step failed", extra={"kind": kind, "month": month})
                    results.append({"kind": kind, "month": month, "step": "recs", "status": "error", "error": str(e)})

            log.info("month/kind end", extra={"kind": kind, "month": month})

    # Pretty print a compact summary to stdout (useful in notebooks/pipelines)
    print(json.dumps(results, indent=2))
    return results


def _parse_args(argv: Optional[Sequence[str]] = None) -> Tuple[RunOptions, Settings]:
    import argparse

    parser = argparse.ArgumentParser(description="DAME EPC monthly orchestrator.")
    parser.add_argument("--env-file", default=None, help="Optional path to .env (overrides ENV_FILE)")
    parser.add_argument(
        "--kinds",
        default="domestic,non-domestic",
        help="Comma-separated kinds: domestic,non-domestic (default: both)",
    )
    parser.add_argument("--start", dest="start_month", default=None, help="Start month YYYY-MM (default: settings)")
    parser.add_argument("--end", dest="end_month", default=None, help="End month YYYY-MM (default: settings)")
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--with-recs", action="store_true", help="Also fetch per-LMK recommendations (default off)")
    g.add_argument("--no-recs", action="store_true", help="Skip recommendations even if configured (default)")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show plan but do not call EPC/GCS/BQ; no checkpoints written."
    )
    parser.add_argument(
        "--reset",
        dest="reset_step",
        choices=("certs", "recs"),
        help="If provided, clears the chosen step checkpoint for the selected months/kinds before running.",
    )

    args = parser.parse_args(argv)

    # Load settings, possibly from a custom env file
    if args.env_file:
        os.environ["ENV_FILE"] = args.env_file
        s = load_settings()
    else:
        s = settings

    kinds = [k.strip() for k in args.kinds.split(",") if k.strip()]
    kinds = [k for k in kinds if k in ("domestic", "non-domestic")]
    if not kinds:
        kinds = ["domestic", "non-domestic"]

    start_month = args.start_month or s.start_month
    end_month = args.end_month or s.end_month

    with_recs = True if args.with_recs else False
    if args.no_recs:
        with_recs = False

    opts = RunOptions(
        kinds=kinds,
        start_month=start_month,
        end_month=end_month,
        with_recs=with_recs,
        dry_run=bool(args.dry_run),
        reset_step=args.reset_step,
    )
    return opts, s


if __name__ == "__main__":
    opts, s = _parse_args()
    run(opts, s)