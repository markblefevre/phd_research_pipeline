#!/usr/bin/env python3
"""
run_market_model_stage.py

Pipeline stage:
- Read TOPIX market prices and stock prices
- Compute daily returns
- Estimate per-ticker market model alpha/beta
- Write alphas_betas.csv

This is callable from run_pipeline.py (no argparse).
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional, Tuple, Dict

import numpy as np
import pandas as pd
import json
import time

from src.event_study.estimate_market_model import estimate_market_model_event_fast


# --------- helpers (same as your script) ---------

def _pick_first_col(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(
        f"None of the expected columns found. Looked for {candidates} in {list(df.columns)}"
    )


# basis has no impact as TOPIX has same values for close, adj_price
def _load_market(market_csv: Path | str, basis: str) -> pd.DataFrame:
    """
    Load TOPIX market file and compute MarketReturn.
    basis: 'price' uses 'close'; 'total' uses 'adj_close'.

    Returns columns: Date, MarketReturn
    """
    market_csv = Path(market_csv)
    m = pd.read_csv(market_csv, parse_dates=["date"], dtype={"symbol": "string"}, low_memory=False)

    # Prefer JST trading_date if present (cleanest joins). Fallback to 'date'
    if "trading_date" in m.columns:
        m["Date"] = pd.to_datetime(m["trading_date"]).dt.date
    else:
        m["Date"] = pd.to_datetime(m["date"]).dt.date

    price_col = "close" if basis == "price" else "adj_close"
    if price_col not in m.columns:
        price_col = _pick_first_col(m, ["adj_close", "close", "Adj Close", "Close"])

    m = m.sort_values("Date")
    m["MarketReturn"] = pd.to_numeric(m[price_col], errors="coerce").pct_change()
    m = m[["Date", "MarketReturn"]].dropna()
    return m


def _load_stocks(stocks_csv: Path | str) -> pd.DataFrame:
    """
    Load stock long file and compute per-symbol daily returns from adj_close if available,
    else from close.

    Returns columns: Date, Ticker, Return
    """
    stocks_csv = Path(stocks_csv)
    s = pd.read_csv(stocks_csv, parse_dates=["date"], low_memory=False)

    sym_col = None
    for c in ["symbol", "ticker", "Symbol", "Ticker"]:
        if c in s.columns:
            sym_col = c
            break
    if sym_col is None:
        raise ValueError(
            "Could not find a symbol/ticker column in prices_long.csv. "
            "Expected one of: symbol,ticker,Symbol,Ticker"
        )

    px_col = None
    for c in ["adj_close", "Adj Close", "adjusted_close", "close", "Close"]:
        if c in s.columns:
            px_col = c
            break
    if px_col is None:
        raise ValueError("Could not find a price column (adj_close/close) in prices_long.csv")

    if "trading_date" in s.columns:
        s["Date"] = pd.to_datetime(s["trading_date"]).dt.date
    else:
        s["Date"] = pd.to_datetime(s["date"]).dt.date

    s = s.rename(columns={sym_col: "Ticker"})
    s = s.sort_values(["Ticker", "Date"])

    s[px_col] = pd.to_numeric(s[px_col], errors="coerce")
    s["Return"] = s.groupby("Ticker", observed=True)[px_col].pct_change()

    out = s[["Date", "Ticker", "Return"]].dropna()
    out = out[np.isfinite(out["Return"])]
    return out


# --------- stage function ---------

def run_market_model(
    *,
    panel_csv: Path | str,   # NEW
    market_csv: Path | str,
    stocks_csv: Path | str,
    out_csv: Path | str,
    ensure_overlap_calendar: bool = True,
):

    t0 = time.perf_counter()

    market_csv = Path(market_csv)
    stocks_csv = Path(stocks_csv)
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # ---- Load data ----

    market_df = _load_market(market_csv, basis="price")
    stock_df = _load_stocks(stocks_csv)

    market_rows_loaded = len(market_df)
    stock_rows_loaded = len(stock_df)
    ticker_count_loaded = stock_df["Ticker"].nunique()

    # ---- Calendar intersection ----

    if ensure_overlap_calendar:
        common_dates = sorted(set(market_df["Date"]).intersection(set(stock_df["Date"])))

        if not common_dates:
            raise ValueError(
                "No overlapping dates between market and stock returns."
            )

        common_dates_count = len(common_dates)

        market_df = market_df[market_df["Date"].isin(common_dates)]
        stock_df = stock_df[stock_df["Date"].isin(common_dates)]

    else:
        common_dates_count = None

    market_rows_after_overlap = len(market_df)
    stock_rows_after_overlap = len(stock_df)

    # ---- Merge diagnostics ----

    merged = stock_df.merge(market_df, on="Date", how="inner")
    merged_rows = len(merged)

    obs_per_ticker = merged.groupby("Ticker").size()

    obs_min = int(obs_per_ticker.min())
    obs_med = float(obs_per_ticker.median())
    obs_max = int(obs_per_ticker.max())

    # ---- Estimation ----

    panel = pd.read_csv(panel_csv)
    
    res = estimate_market_model_event_fast(panel, stock_df, market_df)

    res.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # ---- Event-based QC ----
    
    panel_df = panel.copy()
    res_df = res.copy()
    
    # Normalize types for joins (VERY IMPORTANT)
    panel_df["filing_date"] = pd.to_datetime(panel_df["filing_date"]).dt.date
    res_df["filing_date"] = pd.to_datetime(res_df["filing_date"]).dt.date
    
    # --- Core counts ---
    panel_tickers = panel_df["symbol"].nunique()
    output_tickers = res_df["Ticker"].nunique()
    
    events_total = len(panel_df)
    events_estimated = len(res_df)
    events_dropped = events_total - events_estimated
    
    # --- Event-level matching ---
    panel_keys = set(zip(panel_df["symbol"], panel_df["filing_date"]))
    res_keys = set(zip(res_df["Ticker"], res_df["filing_date"]))
    
    dropped_keys = panel_keys - res_keys
    kept_keys = panel_keys & res_keys
    
    dropped_df = pd.DataFrame(list(dropped_keys), columns=["Ticker", "filing_date"])
    kept_df = pd.DataFrame(list(kept_keys), columns=["Ticker", "filing_date"])

    # Convert date columns to string (JSON safe)
    if "filing_date" in dropped_df:
        dropped_df["filing_date"] = dropped_df["filing_date"].astype(str)
    
    if "filing_date" in kept_df:
        kept_df["filing_date"] = kept_df["filing_date"].astype(str)
    
    # --- Per-ticker coverage ---
    events_per_ticker = panel_df.groupby("symbol").size()
    estimated_per_ticker = res_df.groupby("Ticker").size()
    
    # Align index for safe comparison
    coverage_df = (
        events_per_ticker.rename("events_total")
        .to_frame()
        .join(estimated_per_ticker.rename("events_estimated"), how="left")
        .fillna(0)
    )
    
    coverage_df["events_estimated"] = coverage_df["events_estimated"].astype(int)
    coverage_df["events_dropped"] = coverage_df["events_total"] - coverage_df["events_estimated"]
    
    # --- Coverage summary stats ---
    coverage_min = int(coverage_df["events_estimated"].min())
    coverage_med = float(coverage_df["events_estimated"].median())
    coverage_max = int(coverage_df["events_estimated"].max())
    
    # --- Alpha/Beta sanity ---
    alpha_stats = res_df["alpha"].describe().to_dict() if "alpha" in res_df else {}
    beta_stats = res_df["beta"].describe().to_dict() if "beta" in res_df else {}
    
    # --- Build QC dict ---
    qc = {
        # --- Inputs / runtime ---
        "market_csv": str(market_csv),
        "stocks_csv": str(stocks_csv),
        "panel_csv": str(panel_csv),
        "output_csv": str(out_csv),
        "elapsed_seconds": round(time.perf_counter() - t0, 4),
    
        # --- Raw data coverage ---
        "market_rows_loaded": int(market_rows_loaded),
        "stock_rows_loaded": int(stock_rows_loaded),
        "ticker_count_loaded": int(ticker_count_loaded),
    
        "common_dates_count": int(common_dates_count) if common_dates_count is not None else None,
        "market_rows_after_overlap": int(market_rows_after_overlap),
        "stock_rows_after_overlap": int(stock_rows_after_overlap),
    
        "merged_rows_used": int(merged_rows),
    
        "obs_per_ticker_min": int(obs_min),
        "obs_per_ticker_median": float(obs_med),
        "obs_per_ticker_max": int(obs_max),
    
        # --- EVENT STUDY CORE METRICS ---
        "panel_tickers": int(panel_tickers),
        "output_tickers": int(output_tickers),
        "tickers_missing_from_output": int(panel_tickers - output_tickers),
    
        "events_total": int(events_total),
        "events_estimated": int(events_estimated),
        "events_dropped": int(events_dropped),
    
        "events_per_ticker_min": int(events_per_ticker.min()),
        "events_per_ticker_median": float(events_per_ticker.median()),
        "events_per_ticker_max": int(events_per_ticker.max()),
    
        "events_estimated_per_ticker_min": coverage_min,
        "events_estimated_per_ticker_median": coverage_med,
        "events_estimated_per_ticker_max": coverage_max,
    
        # --- Model sanity ---
        "alpha_stats": alpha_stats,
        "beta_stats": beta_stats,
    
        # --- Diagnostics (samples only to keep JSON small) ---
        "dropped_events_sample": dropped_df.head(25).to_dict(orient="records"),
        "kept_events_sample": kept_df.head(25).to_dict(orient="records"),
    
        # --- Optional: coverage sample ---
        "coverage_sample": coverage_df.head(25).reset_index().to_dict(orient="records"),
    }
    
    # ---- Write QC ----
    qc_path = out_csv.with_suffix(".qc.json")
    qc_path.write_text(json.dumps(qc, indent=2), encoding="utf-8")
    
    # ---- Optional: write dropped events to CSV (HIGHLY recommended) ----
    dropped_path = out_csv.with_name(out_csv.stem + "_dropped_events.csv")
    dropped_df.to_csv(dropped_path, index=False, encoding="utf-8-sig")

    return res
