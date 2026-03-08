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

from src.event_study.estimate_market_model import estimate_market_model, estimate_market_model_fast


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
    market_csv: Path | str,
    stocks_csv: Path | str,
    out_csv: Path | str,
    ensure_overlap_calendar: bool = True,
) -> pd.DataFrame:

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

    alphas, betas = estimate_market_model_fast(stock_df, market_df)

    res = (
        pd.DataFrame(
            {
                "Ticker": list(alphas.keys()),
                "alpha": [alphas[k] for k in alphas.keys()],
                "beta": [betas[k] for k in alphas.keys()],
            }
        )
        .sort_values("Ticker")
        .reset_index(drop=True)
    )

    res.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # ---- QC sidecar ----

    qc = {
        "market_csv": str(market_csv),
        "stocks_csv": str(stocks_csv),
        "output_csv": str(out_csv),
        "elapsed_seconds": round(time.perf_counter() - t0, 4),

        "market_rows_loaded": market_rows_loaded,
        "stock_rows_loaded": stock_rows_loaded,

        "ticker_count_loaded": int(ticker_count_loaded),

        "common_dates_count": common_dates_count,
        "market_rows_after_overlap": market_rows_after_overlap,
        "stock_rows_after_overlap": stock_rows_after_overlap,

        "merged_rows_used": merged_rows,

        "obs_per_ticker_min": obs_min,
        "obs_per_ticker_median": obs_med,
        "obs_per_ticker_max": obs_max,

        "tickers_output": int(res["Ticker"].nunique()),
        "tickers_dropped": int(ticker_count_loaded - res["Ticker"].nunique()),
    }

    qc_path = out_csv.with_suffix(".qc.json")
    qc_path.write_text(json.dumps(qc, indent=2), encoding="utf-8")

    return res
