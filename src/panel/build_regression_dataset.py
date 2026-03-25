#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.edinet.industry import attach_ticker_industry

def build_regression_dataset(
    *,
    car_csv: Path,
    panel_csv: Path,
    out_csv: Path,
) -> pd.DataFrame:

    car_df = pd.read_csv(car_csv, parse_dates=["EventDate"])
    panel = pd.read_csv(panel_csv)

    repo_root = Path(__file__).resolve().parents[2]
    panel = attach_ticker_industry(
        panel,
        repo_root,
        ticker_col="symbol",
        label="both"
    )

    if "industry" not in panel.columns:
        raise ValueError("Panel missing 'industry' column")
    
    panel = panel.rename(columns={
        "symbol": "Ticker",
        "filing_date": "EventDate",
    })
    panel["EventDate"] = pd.to_datetime(panel["EventDate"], errors="coerce")

    panel = panel[
        [
            "Ticker",
            "EventDate",
            "document_score",
            "lmmd_net",
            "neg_rate",
            "pos_rate",
            "industry",
            "ln_market_cap",
            "ln_volatility",
        ]
    ]

    required_cols = ["ln_market_cap", "ln_volatility"]
    missing = [c for c in required_cols if c not in panel.columns]
    if missing:
        raise ValueError(f"Missing control columns in panel: {missing}")
    
    df = car_df.merge(panel, on=["Ticker", "EventDate"], how="inner")

    # --- z-score ---
    def z(s):
        s = pd.to_numeric(s, errors="coerce")
        return (s - s.mean()) / s.std()

    df["gpt_z"] = z(df["document_score"])
    df["lmmd_z"] = z(df["lmmd_net"])
    df["neg_z"] = z(df["neg_rate"])
    df["pos_z"] = z(df["pos_rate"])

    df["disagreement"] = df["gpt_z"] - df["lmmd_z"]

    df.to_csv(out_csv, index=False)
    print(f"[INFO] Saved: {out_csv}")
    
    return df