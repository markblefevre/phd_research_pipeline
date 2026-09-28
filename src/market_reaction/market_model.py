from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class StockReturnSeries:
    """
    Compact per-security representation used by Stage 7D.

    session_index:
        Position on the canonical TOPIX trading calendar.
    returns:
        Corporate-action-adjusted simple stock return on that session.
    """
    session_index: np.ndarray
    returns: np.ndarray


def _normalize_sec_code(s: pd.Series) -> pd.Series:
    """
    Normalize J-Quants / EDINET security codes to five-character strings.

    Examples:
        94320 -> "94320"
        1301  -> "01301" only if source actually supplies four digits.

    J-Quants normally supplies five-digit Code values for domestic equities.
    """
    out = s.astype("string").str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    return out


def load_topix_returns(topix_csv: Path | str) -> pd.DataFrame:
    """
    Load the frozen Stage 7C TOPIX output.

    Required columns:
        tradingDate
        topixReturnSimple

    Returns:
        DataFrame with:
            tradingDate
            topixReturnSimple
            sessionIndex

    The first TOPIX observation has a missing return by construction.
    """
    topix_csv = Path(topix_csv)

    df = pd.read_csv(topix_csv, low_memory=False)

    required = {"tradingDate", "topixReturnSimple"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required TOPIX columns in {topix_csv}: {sorted(missing)}"
        )

    df = df[["tradingDate", "topixReturnSimple"]].copy()
    df["tradingDate"] = pd.to_datetime(
        df["tradingDate"], errors="raise"
    ).dt.normalize()
    df["topixReturnSimple"] = pd.to_numeric(
        df["topixReturnSimple"], errors="coerce"
    )

    df = (
        df.sort_values("tradingDate")
        .drop_duplicates("tradingDate", keep="last")
        .reset_index(drop=True)
    )

    if not df["tradingDate"].is_monotonic_increasing:
        raise ValueError("TOPIX trading dates are not strictly ordered")

    df["sessionIndex"] = np.arange(len(df), dtype=np.int32)

    return df


def build_stock_return_index(
    price_files: Iterable[Path | str],
    topix_df: pd.DataFrame,
) -> tuple[dict[str, StockReturnSeries], dict]:
    """
    Build the Stage 7D stock-return index efficiently.

    Frozen Paper 2 return rule
    --------------------------
    1. Use J-Quants raw close C.
    2. Use current-row AdjFactor to remove corporate-action discontinuities.
    3. Compute simple returns:
           C_t / (C_(t-1) * AdjFactor_t) - 1
    4. Compute a return ONLY when t-1 and t are consecutive sessions on the
       GLOBAL TSE trading calendar.
    5. Map valid returns onto the Stage 7C TOPIX calendar.

    Important:
    The global TSE calendar is constructed from the union of dates observed
    in the raw J-Quants equity files. This calendar is used only to determine
    whether two stock observations are truly consecutive TSE sessions.

    The returned per-security arrays are indexed by TOPIX session position,
    which lets Stage 7D estimate each event without repeated DataFrame
    filtering or merging.
    """
    files = [Path(p) for p in price_files]
    if not files:
        raise ValueError("No J-Quants stock-price files supplied")

    frames: list[pd.DataFrame] = []

    for path in files:
        d = pd.read_csv(
            path,
            usecols=["Date", "Code", "C", "AdjFactor"],
            dtype={"Code": "string"},
            low_memory=False,
        )
        d["Date"] = pd.to_datetime(d["Date"], errors="coerce").dt.normalize()
        d["Code"] = _normalize_sec_code(d["Code"])
        d["C"] = pd.to_numeric(d["C"], errors="coerce")
        d["AdjFactor"] = pd.to_numeric(d["AdjFactor"], errors="coerce")
        frames.append(d)

    prices = pd.concat(frames, ignore_index=True)

    # Keep rows whose date/code are usable. Missing closes are retained here
    # because they are meaningful when detecting gaps in valid observations.
    prices = prices.dropna(subset=["Date", "Code"])

    # J-Quants should have one row per Code/Date. Fail loudly if not.
    dupes = int(prices.duplicated(["Code", "Date"]).sum())
    if dupes:
        raise ValueError(
            f"Raw stock archive contains {dupes:,} duplicate Code/Date row(s)"
        )

    # Global TSE calendar: union of ALL observed J-Quants equity dates.
    tse_dates = (
        prices["Date"]
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    tse_date_to_pos = pd.Series(
        np.arange(len(tse_dates), dtype=np.int32),
        index=pd.DatetimeIndex(tse_dates),
    )

    prices["tseSessionIndex"] = prices["Date"].map(tse_date_to_pos)

    prices = prices.sort_values(["Code", "Date"], kind="mergesort")

    # Previous VALID close/date for each security.
    valid = prices["C"].notna() & np.isfinite(prices["C"]) & (prices["C"] > 0)

    v = prices.loc[
        valid,
        ["Code", "Date", "C", "AdjFactor", "tseSessionIndex"],
    ].copy()

    g = v.groupby("Code", sort=False, observed=True)
    v["prevDate"] = g["Date"].shift(1)
    v["prevClose"] = g["C"].shift(1)
    v["prevTseSessionIndex"] = g["tseSessionIndex"].shift(1)

    # Only true one-session returns survive.
    consecutive = (
        v["prevTseSessionIndex"].notna()
        & (v["tseSessionIndex"] == v["prevTseSessionIndex"] + 1)
    )

    factor = v["AdjFactor"].fillna(1.0)

    # A non-positive adjustment factor is invalid.
    valid_factor = np.isfinite(factor) & (factor > 0)

    v["stockReturn"] = np.nan
    calc = consecutive & valid_factor

    v.loc[calc, "stockReturn"] = (
        v.loc[calc, "C"]
        / (v.loc[calc, "prevClose"] * factor.loc[calc])
        - 1.0
    )

    # Map stock-return dates onto the canonical TOPIX calendar.
    topix_date_to_pos = pd.Series(
        topix_df["sessionIndex"].to_numpy(dtype=np.int32),
        index=pd.DatetimeIndex(topix_df["tradingDate"]),
    )
    v["topixSessionIndex"] = v["Date"].map(topix_date_to_pos)

    usable = (
        v["stockReturn"].notna()
        & np.isfinite(v["stockReturn"])
        & v["topixSessionIndex"].notna()
    )

    r = v.loc[
        usable,
        ["Code", "topixSessionIndex", "stockReturn"],
    ].copy()
    r["topixSessionIndex"] = r["topixSessionIndex"].astype(np.int32)

    # Build compact arrays once. Stage 7D then never filters the 9.7M-row
    # DataFrame inside the event loop.
    stock_index: dict[str, StockReturnSeries] = {}

    for code, grp in r.groupby("Code", sort=False, observed=True):
        grp = grp.sort_values("topixSessionIndex")
        stock_index[str(code)] = StockReturnSeries(
            session_index=grp["topixSessionIndex"].to_numpy(
                dtype=np.int32, copy=True
            ),
            returns=grp["stockReturn"].to_numpy(
                dtype=np.float64, copy=True
            ),
        )

    qc = {
        "priceFiles": len(files),
        "rawRows": int(len(prices)),
        "globalTseSessions": int(len(tse_dates)),
        "validCloseRows": int(len(v)),
        "consecutiveSessionReturns": int(calc.sum()),
        "usableReturnsOnTopixCalendar": int(len(r)),
        "securitiesIndexed": int(len(stock_index)),
        "firstTseDate": str(tse_dates.iloc[0].date()),
        "lastTseDate": str(tse_dates.iloc[-1].date()),
    }

    return stock_index, qc


def _ols_market_model(
    y: np.ndarray,
    x: np.ndarray,
) -> tuple[float, float, float]:
    """
    Fast OLS with intercept:
        y = alpha + beta*x + eps

    Returns:
        alpha, beta, r_squared
    """
    n = len(y)
    if n < 2:
        return np.nan, np.nan, np.nan

    mean_y = float(y.mean())
    mean_x = float(x.mean())

    yd = y - mean_y
    xd = x - mean_x

    sxx = float(np.dot(xd, xd))
    if not np.isfinite(sxx) or sxx <= 0.0:
        return np.nan, np.nan, np.nan

    beta = float(np.dot(xd, yd) / sxx)
    alpha = float(mean_y - beta * mean_x)

    resid = y - (alpha + beta * x)
    sse = float(np.dot(resid, resid))
    sst = float(np.dot(yd, yd))

    # Constant-return security: R² is undefined rather than forcing zero.
    r_squared = np.nan if sst <= 0.0 else float(1.0 - sse / sst)

    return alpha, beta, r_squared


def estimate_event_market_models(
    events: pd.DataFrame,
    topix_df: pd.DataFrame,
    stock_index: dict[str, StockReturnSeries],
    *,
    estimation_start: int = -120,
    estimation_end: int = -20,
    min_observations: int = 60,
) -> pd.DataFrame:
    """
    Estimate one event-specific market model for every Stage 7B event.

    Required event columns:
        edinetCode
        curr_docID
        secCode
        eventTradingDate

    Window definition:
        t = 0 is the Stage 7B canonical eventTradingDate.
        The estimation window is defined on the TOPIX/global trading calendar,
        NOT on each security's own sequence of observations.

    The inclusive [-120,-20] window therefore contains 101 possible market
    sessions. Missing/suspended stock observations simply reduce the number
    of matched observations; they do not compress event time.
    """
    required = {
        "edinetCode",
        "curr_docID",
        "secCode",
        "eventTradingDate",
    }
    missing = required - set(events.columns)
    if missing:
        raise ValueError(
            f"Missing required Stage 7 event columns: {sorted(missing)}"
        )

    if estimation_start >= estimation_end:
        raise ValueError("estimation_start must be less than estimation_end")

    if min_observations < 2:
        raise ValueError("min_observations must be at least 2")

    ev = events.copy()
    ev["secCode"] = _normalize_sec_code(ev["secCode"])
    ev["eventTradingDate"] = pd.to_datetime(
        ev["eventTradingDate"], errors="raise"
    ).dt.normalize()

    # Canonical TOPIX session lookup and market-return array.
    topix_dates = pd.DatetimeIndex(topix_df["tradingDate"])
    date_to_session = {
        d: int(i)
        for i, d in enumerate(topix_dates)
    }
    market_returns = topix_df["topixReturnSimple"].to_numpy(
        dtype=np.float64
    )

    results: list[dict] = []

    # Group events by security so dictionary lookup occurs once per security.
    for sec_code, grp in ev.groupby("secCode", sort=False, observed=True):
        series = stock_index.get(str(sec_code))

        for row in grp.itertuples(index=False):
            event_date = row.eventTradingDate
            event_idx = date_to_session.get(event_date)

            base = {
                "edinetCode": row.edinetCode,
                "curr_docID": row.curr_docID,
                "secCode": str(sec_code),
                "eventTradingDate": event_date,
            }

            if event_idx is None:
                results.append({
                    **base,
                    "estimationStartDate": pd.NaT,
                    "estimationEndDate": pd.NaT,
                    "estimationObs": 0,
                    "alpha": np.nan,
                    "beta": np.nan,
                    "rSquared": np.nan,
                    "marketModelStatus": "event_date_not_in_topix_calendar",
                })
                continue

            start_idx = event_idx + estimation_start
            end_idx = event_idx + estimation_end

            if start_idx < 0 or end_idx >= len(topix_df):
                results.append({
                    **base,
                    "estimationStartDate": pd.NaT,
                    "estimationEndDate": pd.NaT,
                    "estimationObs": 0,
                    "alpha": np.nan,
                    "beta": np.nan,
                    "rSquared": np.nan,
                    "marketModelStatus": "insufficient_market_history",
                })
                continue

            start_date = topix_dates[start_idx]
            end_date = topix_dates[end_idx]

            if series is None:
                results.append({
                    **base,
                    "estimationStartDate": start_date,
                    "estimationEndDate": end_date,
                    "estimationObs": 0,
                    "alpha": np.nan,
                    "beta": np.nan,
                    "rSquared": np.nan,
                    "marketModelStatus": "security_not_in_stock_return_index",
                })
                continue

            # session_index is sorted. Slice the exact fixed market-calendar
            # interval with two binary searches.
            lo = np.searchsorted(
                series.session_index, start_idx, side="left"
            )
            hi = np.searchsorted(
                series.session_index, end_idx, side="right"
            )

            stock_sessions = series.session_index[lo:hi]
            y = series.returns[lo:hi]

            # Direct array lookup replaces a DataFrame merge.
            x = market_returns[stock_sessions]

            paired = np.isfinite(y) & np.isfinite(x)
            if not paired.all():
                y = y[paired]
                x = x[paired]

            n = int(len(y))

            if n < min_observations:
                results.append({
                    **base,
                    "estimationStartDate": start_date,
                    "estimationEndDate": end_date,
                    "estimationObs": n,
                    "alpha": np.nan,
                    "beta": np.nan,
                    "rSquared": np.nan,
                    "marketModelStatus": "insufficient_paired_observations",
                })
                continue

            alpha, beta, r_squared = _ols_market_model(y, x)

            status = (
                "estimated"
                if np.isfinite(alpha) and np.isfinite(beta)
                else "estimation_failed"
            )

            results.append({
                **base,
                "estimationStartDate": start_date,
                "estimationEndDate": end_date,
                "estimationObs": n,
                "alpha": alpha,
                "beta": beta,
                "rSquared": r_squared,
                "marketModelStatus": status,
            })

    out = pd.DataFrame(results)

    # Preserve stable event ordering for reproducibility.
    if not out.empty:
        out = out.reset_index(drop=True)

    return out


def build_market_model_summary(
    estimates: pd.DataFrame,
    stock_qc: dict,
    *,
    estimation_start: int,
    estimation_end: int,
    min_observations: int,
) -> dict:
    """
    Compact JSON-serializable Stage 7D QC summary.
    """
    estimated = estimates[
        estimates["marketModelStatus"] == "estimated"
    ]

    status_counts = (
        estimates["marketModelStatus"]
        .value_counts(dropna=False)
        .to_dict()
    )

    def describe_numeric(col: str) -> dict:
        s = pd.to_numeric(estimated[col], errors="coerce").dropna()
        if s.empty:
            return {}
        d = s.describe(
            percentiles=[0.01, 0.05, 0.50, 0.95, 0.99]
        ).to_dict()
        return {
            str(k): float(v)
            for k, v in d.items()
        }

    return {
        "stage": "7D_event_specific_market_model",
        "model": "R_i,t = alpha_i + beta_i * R_m,t + epsilon_i,t",
        "stockReturn": "corporate-action-adjusted simple return",
        "marketReturn": "TOPIX simple return",
        "estimationStart": int(estimation_start),
        "estimationEnd": int(estimation_end),
        "minPairedObservations": int(min_observations),
        "eventsInput": int(len(estimates)),
        "eventsEstimated": int(len(estimated)),
        "eventsNotEstimated": int(len(estimates) - len(estimated)),
        "statusCounts": {
            str(k): int(v)
            for k, v in status_counts.items()
        },
        "estimationObs": describe_numeric("estimationObs"),
        "alpha": describe_numeric("alpha"),
        "beta": describe_numeric("beta"),
        "rSquared": describe_numeric("rSquared"),
        "stockReturnIndex": stock_qc,
    }
