

from __future__ import annotations

"""
Typed settings loaded from a single `.env` file (developer-friendly) with safe
overrides from real environment variables (for CI/Cloud). Uses pydantic-settings.

Precedence (left < right):
  defaults  <  .env (if present)  <  real environment variables  <  runtime kwargs

Usage:
    from dame_epc.settings import settings
    print(settings.project_id)
    for m in settings.month_range():
        ...

Environment:
- Set ENV_FILE to point at an alternate .env path (defaults to ".env").
- Required: PROJECT_ID, BUCKET, EPC_EMAIL, EPC_API_KEY, START_MONTH, END_MONTH
"""

import os
import re
import datetime as _dt
from pathlib import Path
from typing import Iterable, List, Tuple

from pydantic import Field, PositiveInt, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic.functional_validators import field_validator, model_validator


_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _parse_month(s: str) -> _dt.date:
    """Parse 'YYYY-MM' to first-of-month date."""
    if not _MONTH_RE.match(s or ""):
        raise ValueError(f"Month must be 'YYYY-MM' with 01-12: got {s!r}")
    y, m = map(int, s.split("-"))
    return _dt.date(y, m, 1)


def _months_between(start: str, end: str) -> List[str]:
    """Inclusive list of YYYY-MM from start to end (validated)."""
    a = _parse_month(start)
    b = _parse_month(end)
    if (b.year, b.month) < (a.year, a.month):
        raise ValueError(f"END_MONTH {end!r} is before START_MONTH {start!r}")
    out: List[str] = []
    d = a
    while True:
        out.append(f"{d.year:04d}-{d.month:02d}")
        if d.year == b.year and d.month == b.month:
            break
        # advance one month
        if d.month == 12:
            d = _dt.date(d.year + 1, 1, 1)
        else:
            d = _dt.date(d.year, d.month + 1, 1)
    return out


class Settings(BaseSettings):
    # GCP / BigQuery / GCS
    project_id: str = Field(..., alias="PROJECT_ID")
    region: str = Field("europe-west2", alias="REGION")
    bucket: str = Field(..., alias="BUCKET")
    dataset_raw: str = Field("dame_epc", alias="DATASET_RAW")
    dataset_enr: str = Field("dame_epc", alias="DATASET_ENR")

    # EPC API
    epc_email: str = Field(..., alias="EPC_EMAIL")
    epc_api_key: str = Field(..., alias="EPC_API_KEY")

    # Ingestion window
    start_month: str = Field(..., alias="START_MONTH", description="YYYY-MM")
    end_month: str = Field(..., alias="END_MONTH", description="YYYY-MM")

    # Network / pagination
    page_size: PositiveInt = Field(5000, alias="PAGE_SIZE")
    concurrency: PositiveInt = Field(2, alias="CONCURRENCY")
    request_timeout_seconds: PositiveInt = Field(60, alias="REQUEST_TIMEOUT_SECONDS")
    retry_max: PositiveInt = Field(5, alias="RETRY_MAX")
    retry_backoff: float = Field(2.0, alias="RETRY_BACKOFF")

    # Optional: explicit Google ADC path (for local dev)
    google_application_credentials: str | None = Field(
        default=None, alias="GOOGLE_APPLICATION_CREDENTIALS"
    )

    # Config: do not hard-bind env_file here; we pass it at construction time.
    model_config = SettingsConfigDict(
        extra="ignore",
        case_sensitive=False,
        env_prefix="",
        # env_file=None is intentional; see load_settings() below
    )

    # --- Validators ---

    @field_validator("start_month", "end_month")
    @classmethod
    def _validate_month(cls, v: str) -> str:
        _parse_month(v)  # will raise with a helpful message
        return v

    @field_validator("region")
    @classmethod
    def _validate_region(cls, v: str) -> str:
        # Keep permissive; restrict only if you need to.
        if not v:
            raise ValueError("region must be set")
        return v

    @model_validator(mode="after")
    def _check_window(self) -> "Settings":
        # Ensures end >= start
        _months_between(self.start_month, self.end_month)
        return self

    # --- Convenience ---

    @property
    def epc_auth(self) -> Tuple[str, str]:
        """Tuple for requests' HTTPBasicAuth."""
        return (self.epc_email, self.epc_api_key)

    def month_range(self) -> List[str]:
        """Inclusive YYYY-MM list between START_MONTH and END_MONTH."""
        return _months_between(self.start_month, self.end_month)

    def iter_months(self) -> Iterable[str]:
        """Generator over the month range (inclusive)."""
        for m in self.month_range():
            yield m

    @property
    def bq_location(self) -> str:
        """Alias for BigQuery job location (same as region)."""
        return self.region


def load_settings() -> Settings:
    """
    Create a Settings instance, honoring ENV_FILE if set.

    ENV_FILE allows swapping the .env path without code changes:
      ENV_FILE=.env.test python -m dame_epc.main ...
    """
    env_file = os.environ.get("ENV_FILE", ".env")
    env_path = Path(env_file)
    try:
        s = Settings(_env_file=env_path if env_path.exists() else None)
    except ValidationError as e:
        missing = []
        # Extract a friendlier message for missing required fields
        for err in e.errors():
            if err.get("type") == "missing":
                loc = err.get("loc", ["?"])[0]
                missing.append(str(loc))
        if missing:
            raise RuntimeError(
                "Missing required settings: "
                + ", ".join(sorted(set(missing)))
                + f". Looked in ENV_FILE={env_file!r} and process environment."
            ) from e
        raise
    # Optionally set ADC env for downstream libs if provided in .env
    if s.google_application_credentials:
        os.environ.setdefault(
            "GOOGLE_APPLICATION_CREDENTIALS", s.google_application_credentials
        )
    return s


# Singleton used across modules
settings: Settings = load_settings()

__all__ = [
    "Settings",
    "settings",
    "load_settings",
    "_months_between",
]