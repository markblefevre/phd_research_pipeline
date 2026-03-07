#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_event_study_all.py

Pipeline (no OpenAI calls):
1) Load precomputed MD&A sentiment panel:
   Code/out/mdna_summary_nikkei225_with_lmmd.csv  (GPT sentiment already computed)
2) Load daily stock prices (Nikkei 225) and TOPIX
3) Load pre-estimated market-model alpha/beta per ticker
4) Compute CAR for each (Ticker, FilingDate) over chosen event windows
5) Run clustered regression (cluster by Ticker): CAR ~ Sentiment
6) Save outputs per window under Code/out/event_study/

Notes:
- Changing event windows does NOT require rerunning GPT / API.
- Only rerun GPT if you regenerate mdna_summary_nikkei225_filtered.csv.
"""

import pandas as pd
import statsmodels.api as sm
from pathlib import Path
from project_paths import get_project_root
from calculate_car_per_event import calculate_car_per_event


def run_event_study_all(windows=None, sentiment_col="document_score"):
    root = get_project_root()

    # === Inputs ===
    sentiment_csv = root / "Code" / "out" / "mdna_summary_nikkei225_with_lmmd.csv"
    alphas_betas_csv = root / "Code" / "out" / "alphas_betas.csv"
    prices_csv = root / "nikkei" / "out" / "prices_long.csv"
    market_csv = root / "nikkei" / "out" / "TOPIX_prices.csv"

    out_dir = root / "Code" / "out" / "event_study"
    out_dir.mkdir(parents=True, exist_ok=True)

    if windows is None:
        windows = [(0, 0), (0, 1), (-1, 1), (-2,2), (-3, 3)]

    print(f"[INFO] Project root: {root}")
    print(f"[INFO] Sentiment source: {sentiment_csv} (col={sentiment_col})")
    print(f"[INFO] Windows: {windows}")
    print(f"[INFO] Output dir: {out_dir}")

    # === Load data ===
    sentiment_df = pd.read_csv(sentiment_csv, dtype=str)
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

    # --- Alpha/Beta dicts ---
    alphas = dict(zip(alphas_betas["Ticker"], alphas_betas["alpha"]))
    betas  = dict(zip(alphas_betas["Ticker"], alphas_betas["beta"]))

    # --- Events ---
    if sentiment_col not in sentiment_df.columns:
        raise ValueError(f"Sentiment column '{sentiment_col}' not found in {sentiment_csv}")

    event_dates = sentiment_df[["symbol", "filing_date", sentiment_col]].copy()
    event_dates = event_dates.rename(columns={
        "symbol": "Ticker",
        "filing_date": "EventDate",
        sentiment_col: "Sentiment",
    })
    event_dates["EventDate"] = pd.to_datetime(event_dates["EventDate"], errors="coerce")
    event_dates["Sentiment"] = pd.to_numeric(event_dates["Sentiment"], errors="coerce")
    event_dates = event_dates.dropna(subset=["EventDate", "Sentiment"])

    # === Run windows ===
    reg_rows = []

    for w in windows:
        print(f"\n[INFO] Computing CARs for window {w} ...")

        car_df = calculate_car_per_event(
            stock_df=stock_df,
            market_df=market_df,
            alphas=alphas,
            betas=betas,
            event_dates=event_dates,
            event_window=w,
        )

        # Save per-window CAR file
        w_tag = f"m{abs(w[0])}" if w[0] < 0 else f"p{w[0]}"
        w_tag = f"{w_tag}_p{w[1]}"
        out_csv = out_dir / f"car_results_all_{w[0]}_{w[1]}_{sentiment_col}.csv"
        car_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        print(f"[INFO] Saved: {out_csv}")

        # Regression (clustered by ticker)
        if car_df.empty:
            print("[WARN] No CARs computed for this window.")
            continue

        # Ensure numeric
        car_df["Sentiment"] = pd.to_numeric(car_df["Sentiment"], errors="coerce")
        car_df["CAR"] = pd.to_numeric(car_df["CAR"], errors="coerce")
        car_df = car_df.dropna(subset=["Sentiment", "CAR"])
        
        # -----------------------------
        # Standardize sentiment (z-score)
        # -----------------------------
        s = car_df["Sentiment"]
        mean_s = s.mean()
        std_s = s.std(ddof=0)
        
        if std_s == 0:
            print("[WARN] Sentiment std is zero. Skipping standardization.")
            car_df["Sentiment_z"] = s
        else:
            car_df["Sentiment_z"] = (s - mean_s) / std_s
        
        # Regression using standardized sentiment
        X = sm.add_constant(car_df["Sentiment_z"])
        y = car_df["CAR"]
        
        model = sm.OLS(y, X).fit(
            cov_type="cluster",
            cov_kwds={"groups": car_df["Ticker"]}
        )
        
        beta = float(model.params["Sentiment_z"])
        se   = float(model.bse["Sentiment_z"])
        pval = float(model.pvalues["Sentiment_z"])
        nobs = int(model.nobs)
        r2   = float(model.rsquared)

        reg_rows.append({
            "sentiment_col": sentiment_col,
            "window_start": w[0],
            "window_end": w[1],
            "nobs": nobs,
            "beta": beta,
            "se_cluster": se,
            "p_value": pval,
            "r2": r2,
        })

        print(f"[RESULT] window={w}  beta={beta:.6f}  se={se:.6f}  p={pval:.4g}  R2={r2:.4f}  N={nobs}")

    # Save regression summary table
    reg_df = pd.DataFrame(reg_rows).sort_values(["window_start", "window_end"])
    reg_out = out_dir / f"regression_summary_{sentiment_col}.csv"
    reg_df.to_csv(reg_out, index=False, encoding="utf-8-sig")
    print(f"\n[INFO] Saved regression summary: {reg_out}")

    return reg_df


if __name__ == "__main__":
    sentiment_cols = ["document_score", "lmmd_net", "neg_rate", "pos_rate"]
    for col in sentiment_cols:
        print(f"\n=== Running event study for sentiment_col={col} ===")
        run_event_study_all(sentiment_col=col)

