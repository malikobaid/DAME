

from __future__ import annotations

"""
HTTP-only client for EPC Open Data Communities API.

This module deliberately contains no cloud (GCS/BQ) logic.
It exposes small, testable functions that return raw JSON dicts from the API.
Normalization and persistence happen in other modules.

Endpoints used
--------------
- Certificates search (paged with "search-after"):
    GET https://epc.opendatacommunities.org/api/v1/{kind}/search
    Params:
      from-year, from-month, to-year, to-month, size, (optional) search-after
    Headers:
      Accept: application/json
    Auth:
      HTTP Basic (email, api_key)

- Recommendations (per certificate / LMK key):
    GET https://epc.opendatacommunities.org/api/v1/{kind}/recommendations/{lmk_key}

Notes
-----
- `kind` must be "domestic" or "non-domestic".
- Pagination uses response header "X-Next-Search-After" to request the next page.
- For robustness, we accept both {"rows":[...]} and raw list responses.
- Network errors and non-2xx (except 404 on recommendations) raise RuntimeError with context.
"""

import datetime as _dt
from typing import Dict, Iterable, Iterator, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://epc.opendatacommunities.org/api/v1"
VALID_KINDS = {"domestic", "non-domestic"}


# ---------------------------
# Helpers
# ---------------------------


def _month_bounds(month: str) -> tuple[_dt.date, _dt.date]:
    """Return (start_date, end_date) for a month given as 'YYYY-MM'."""
    y, m = map(int, month.split("-"))
    start = _dt.date(y, m, 1)
    end = (_dt.date(y + 1, 1, 1) if m == 12 else _dt.date(y, m + 1, 1)) - _dt.timedelta(days=1)
    return start, end


def _build_session(retry_max: int, backoff: float) -> requests.Session:
    """Retrying session for resilient API calls."""
    s = requests.Session()
    retry = Retry(
        total=retry_max,
        read=retry_max,
        connect=retry_max,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _check_kind(kind: str) -> str:
    if kind not in VALID_KINDS:
        raise ValueError(f"kind must be one of {sorted(VALID_KINDS)}, got {kind!r}")
    return kind


# ---------------------------
# Public API
# ---------------------------


def fetch_certificates_json(
    kind: str,
    month: str,
    page_size: int,
    timeout: int,
    auth: Tuple[str, str],
    retry_max: int = 5,
    retry_backoff: float = 2.0,
) -> Iterator[Dict]:
    """
    Iterate raw certificate JSON objects for a month.

    Args:
        kind: "domestic" | "non-domestic"
        month: "YYYY-MM"
        page_size: page size (e.g., 5000)
        timeout: per-request timeout in seconds
        auth: (email, api_key) tuple for HTTP Basic
        retry_max: total retry attempts for transient errors
        retry_backoff: exponential backoff base (seconds)

    Yields:
        dict: raw certificate objects as returned by the EPC API.

    Raises:
        RuntimeError for HTTP/network errors (other than an empty page).
    """
    _check_kind(kind)
    start, end = _month_bounds(month)
    url = f"{BASE_URL}/{kind}/search"
    headers = {"Accept": "application/json"}
    basic = requests.auth.HTTPBasicAuth(*auth)
    session = _build_session(retry_max, retry_backoff)

    search_after = None
    while True:
        params = {
            "from-year": start.year,
            "from-month": start.month,
            "to-year": end.year,
            "to-month": end.month,
            "size": page_size,
        }
        if search_after:
            params["search-after"] = search_after

        try:
            r = session.get(url, params=params, headers=headers, auth=basic, timeout=timeout)
        except requests.RequestException as e:
            raise RuntimeError(f"EPC {kind} search request failed: {e}") from e

        # Unauthorized gets a special message
        if r.status_code == 401:
            raise RuntimeError("EPC 401 Unauthorized: check EPC_EMAIL/EPC_API_KEY")

        if r.status_code // 100 != 2:
            raise RuntimeError(
                f"EPC {kind} search HTTP {r.status_code}: {r.text[:200]} (params={params})"
            )

        try:
            data = r.json()
        except ValueError as e:
            raise RuntimeError(f"EPC {kind} search returned non-JSON payload") from e

        rows = data.get("rows", data if isinstance(data, list) else [])
        if not rows:
            break

        for rec in rows:
            yield rec

        # Advance pagination
        search_after = r.headers.get("X-Next-Search-After")
        if not search_after:
            break


def fetch_recommendations_by_lmk(
    kind: str,
    lmk_key: str,
    timeout: int,
    auth: Tuple[str, str],
    retry_max: int = 5,
    retry_backoff: float = 2.0,
) -> List[Dict]:
    """
    Fetch recommendation rows for a given LMK key.

    Args:
        kind: "domestic" | "non-domestic"
        lmk_key: certificate LMK key
        timeout: per-request timeout in seconds
        auth: (email, api_key) tuple
        retry_max: total retry attempts for transient errors
        retry_backoff: exponential backoff factor in seconds

    Returns:
        list of recommendation dicts (may be empty).

    Behavior:
        - 404 is treated as "no recommendations" and returns [].
        - Other non-2xx codes raise RuntimeError with context.
    """
    _check_kind(kind)
    url = f"{BASE_URL}/{kind}/recommendations/{lmk_key}"
    headers = {"Accept": "application/json"}
    basic = requests.auth.HTTPBasicAuth(*auth)
    session = _build_session(retry_max, retry_backoff)

    try:
        r = session.get(url, headers=headers, auth=basic, timeout=timeout)
    except requests.RequestException as e:
        raise RuntimeError(f"EPC {kind} recommendations request failed: {e}") from e

    if r.status_code == 404:
        return []
    if r.status_code == 401:
        raise RuntimeError("EPC 401 Unauthorized: check EPC_EMAIL/EPC_API_KEY")
    if r.status_code // 100 != 2:
        raise RuntimeError(
            f"EPC {kind} recommendations HTTP {r.status_code}: {r.text[:200]} (lmk_key={lmk_key})"
        )

    try:
        data = r.json()
    except ValueError as e:
        raise RuntimeError("EPC recommendations returned non-JSON payload") from e

    # API may return {"rows":[..]} or list
    rows = data.get("rows", data if isinstance(data, list) else [])
    return rows


__all__ = [
    "fetch_certificates_json",
    "fetch_recommendations_by_lmk",
]