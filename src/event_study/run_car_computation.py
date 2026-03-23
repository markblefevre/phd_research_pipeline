#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
from src.utils.project_paths import get_project_root
from src.event_study.calculate_car_per_event import calculate_car_per_event
from datetime import datetime


def run_car_computation(
    windows=None,
    paper="paper1",
    run_id=None,
    sentiment_csv=None,
    alphas_betas_csv=None,
    prices_csv=None,
    market_csv=None,
):

    data_root = get_project_root()
    print(f"[INFO] Data root (inputs): {data_root}")

    # === Inputs ===
    sentiment_csv = Path(sentiment_csv) if sentiment_csv else data_root / "Code" / "out" / "mdna_summary_nikkei225_with_lmmd.csv"
    alphas_betas_csv = Path(alphas_betas_csv) if alphas_betas_csv else data_root / "Code" / "out" / "alphas_betas.csv"
    prices_csv = Path(prices_csv) if prices_csv else data_root / "nikkei" / "out" / "prices_long.csv"
    market_csv = Path(market_csv) if market_csv else data_root / "nikkei" / "out" / "TOPIX_prices.csv"

    root = Path(__file__).resolve().parents[2]
    if run_id is None:
        run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    out_dir = root / "outputs" / paper / run_id / "event_study"
    out_dir.mkdir(parents=True, exist_ok=True)

    if windows is None:
        windows = [(0, 0), (0, 1), (-1, 1)]

    print(f"[INFO] Windows: {windows}")
    print(f"[INFO] Output dir: {out_dir}")

    # === Load data ===
    sentiment_df = pd.read_csv(sentiment_csv)
    alphas_betas = pd.read_csv(alphas_betas_csv, dtype={"Ticker": str})
    prices = pd.read_csv(prices_csv, parse_dates=["date"])
    market = pd.read_csv(market_csv, parse_dates=["date"])

    # --- Market returns ---
    market = market.rename(columns={"date": "Date"})
    if "MarketReturn" not in market.columns:
        market = market.rename(columns={"adj_close": "MarketAdjClose"})
        market["MarketReturn"] = market["MarketAdjClose"].pct_change(fill_method=None)
    market_df = market[["Date", "MarketReturn"]].dropna()

    # --- Stock returns ---
    prices = prices.rename(columns={"date": "Date", "symbol": "Ticker"})
    if "Return" not in prices.columns:
        prices["Return"] = prices.groupby("Ticker")["adj_close"].pct_change(fill_method=None)
    stock_df = prices[["Date", "Ticker", "Return"]].dropna()

    # === Build event panel ===
    event_df = sentiment_df[["symbol", "filing_date"]].copy()
    event_df = event_df.rename(columns={
        "symbol": "Ticker",
        "filing_date": "EventDate",
    })
    
    event_df["EventDate"] = pd.to_datetime(event_df["EventDate"], errors="coerce")
    event_df = event_df.dropna(subset=["EventDate"])

    # === CRITICAL: merge event-specific alpha/beta ===
    alphas_betas["filing_date"] = pd.to_datetime(alphas_betas["filing_date"])

    event_df = event_df.merge(
        alphas_betas,
        left_on=["Ticker", "EventDate"],
        right_on=["Ticker", "filing_date"],
        how="inner",
    )

    # sanity check
    if event_df.empty:
        raise ValueError("No events matched with alpha/beta — check merge keys")

    print(f"[INFO] Events after merge: {len(event_df)}")

    # === Run event study ===
    for w in windows:
        print(f"\n[INFO] Computing CARs for window {w}")

        car_df = calculate_car_per_event(
            stock_df=stock_df,
            market_df=market_df,
            event_df=event_df,   # ← contains alpha/beta now
            event_window=w,
        )

        out_csv = out_dir / f"car_results_{w[0]}_{w[1]}.csv"
        car_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

        print(f"[INFO] Saved: {out_csv}")

