from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


JQUANTS_BASE = "https://api.jquants.com"
JQUANTS_TOPIX_ENDPOINT = "/v2/indices/bars/daily/topix"


def fetch_topix_jquants(
    *,
    api_key: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    timeout: int = 30,
    pause_seconds: float = 0.2,
) -> pd.DataFrame:
    """
    Fetch official TOPIX daily OHLC data from J-Quants.

    Parameters
    ----------
    api_key
        J-Quants API key.
    start
        Inclusive start date, YYYY-MM-DD.
    end
        Inclusive end date, YYYY-MM-DD.
    timeout
        HTTP timeout in seconds.
    pause_seconds
        Pause between paginated requests.

    Returns
    -------
    pd.DataFrame
        Canonical raw TOPIX daily data with columns:

        tradingDate
        open
        high
        low
        close
    """
    if not api_key:
        raise ValueError("J-Quants API key is required")

    url = JQUANTS_BASE + JQUANTS_TOPIX_ENDPOINT

    headers = {
        "x-api-key": api_key,
    }

    params = {}

    if start:
        params["from"] = start

    if end:
        params["to"] = end

    rows: list[dict] = []

    while True:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=timeout,
        )

        if response.status_code == 401:
            raise RuntimeError(
                "J-Quants unauthorized: check API key"
            )

        if response.status_code == 403:
            raise RuntimeError(
                "J-Quants forbidden: API key or subscription "
                "does not permit TOPIX access"
            )

        if response.status_code >= 400:
            raise RuntimeError(
                f"J-Quants HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )

        payload = response.json() or {}

        page_rows = _extract_topix_rows(payload)
        rows.extend(page_rows)

        pagination_key = payload.get("pagination_key")

        if not pagination_key:
            break

        params["pagination_key"] = pagination_key

        if pause_seconds:
            time.sleep(pause_seconds)

    if not rows:
        raise RuntimeError(
            "J-Quants returned no TOPIX observations "
            "for the requested date range"
        )

    raw = pd.DataFrame(rows)

    return normalize_topix(raw)


def _extract_topix_rows(payload: dict) -> list[dict]:
    """
    Tolerate small J-Quants response-schema differences.
    """
    if not isinstance(payload, dict):
        return []

    for key in (
        "daily_topix",
        "topix",
        "data",
    ):
        value = payload.get(key)

        if isinstance(value, list):
            return value

    return []


def normalize_topix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize J-Quants TOPIX data to Paper 2 canonical raw format.
    """
    out = df.copy()

    out.columns = [
        str(col).strip().lower()
        for col in out.columns
    ]

    rename = {
        "date": "tradingDate",

        "o": "open",
        "open": "open",
        "openprice": "open",

        "h": "high",
        "high": "high",
        "highprice": "high",

        "l": "low",
        "low": "low",
        "lowprice": "low",

        "c": "close",
        "close": "close",
        "closeprice": "close",
    }

    out = out.rename(columns=rename)

    required = {
        "tradingDate",
        "close",
    }

    missing = required - set(out.columns)

    if missing:
        raise ValueError(
            "TOPIX response missing required columns: "
            + ", ".join(sorted(missing))
        )

    # J-Quants daily index dates are trading dates already.
    # Do not perform unnecessary UTC/JST conversion.
    out["tradingDate"] = pd.to_datetime(
        out["tradingDate"],
        errors="raise",
    ).dt.normalize()

    for col in [
        "open",
        "high",
        "low",
        "close",
    ]:
        if col not in out.columns:
            out[col] = pd.NA

        out[col] = pd.to_numeric(
            out[col],
            errors="coerce",
        )

    out = (
        out[
            [
                "tradingDate",
                "open",
                "high",
                "low",
                "close",
            ]
        ]
        .sort_values("tradingDate")
        .drop_duplicates(
            subset=["tradingDate"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    if out["tradingDate"].duplicated().any():
        raise AssertionError(
            "Duplicate TOPIX trading dates remain "
            "after normalization"
        )

    if out["close"].isna().any():
        bad = int(out["close"].isna().sum())

        raise ValueError(
            f"TOPIX contains {bad} missing close values"
        )

    return out


def build_topix_returns(
    raw_topix: pd.DataFrame,
) -> pd.DataFrame:
    """
    Construct canonical Stage 7C TOPIX series.

    Both simple and log returns are retained. Stage 7D will select
    the return convention matching the Paper 1 event-study code.
    """
    import numpy as np

    df = raw_topix.copy()

    df = df.sort_values(
        "tradingDate"
    ).reset_index(drop=True)

    df["topixReturnSimple"] = (
        df["close"].pct_change()
    )

    df["topixReturnLog"] = (
        np.log(df["close"])
        .diff()
    )

    df = df.rename(
        columns={
            "open": "topixOpen",
            "high": "topixHigh",
            "low": "topixLow",
            "close": "topixClose",
        }
    )

    return df[
        [
            "tradingDate",
            "topixOpen",
            "topixHigh",
            "topixLow",
            "topixClose",
            "topixReturnSimple",
            "topixReturnLog",
        ]
    ]


def write_topix_raw(
    df: pd.DataFrame,
    path: str | Path,
) -> Path:
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    out = df.copy()

    out["tradingDate"] = (
        pd.to_datetime(out["tradingDate"])
        .dt.strftime("%Y-%m-%d")
    )

    out.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
    )

    return path


def write_topix_returns(
    df: pd.DataFrame,
    path: str | Path,
) -> Path:
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    out = df.copy()

    out["tradingDate"] = (
        pd.to_datetime(out["tradingDate"])
        .dt.strftime("%Y-%m-%d")
    )

    out.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
    )

    return path