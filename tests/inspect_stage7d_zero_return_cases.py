#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def norm_code(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip()
    return out.str.replace(r"\.0$", "", regex=True)


def main() -> int:
    root = repo_root()

    audit_path = (
        root
        / "data/interim/paper2/market_reaction/diagnostics/"
          "stage7d_market_model_failure_audit.csv"
    )
    prices_dir = root / "data/raw/paper2/prices"

    audit = pd.read_csv(
        audit_path,
        dtype={
            "edinetCode": "string",
            "curr_docID": "string",
            "secCode": "string",
        },
        parse_dates=[
            "eventTradingDate",
            "estimationStartDate",
            "estimationEndDate",
            "firstPriceDate",
            "lastPriceDate",
        ],
        low_memory=False,
    )

    zero = audit.loc[
        audit["usablePairedReturnsInWindow"].eq(0)
    ].copy()

    print("=== Stage 7D zero-usable-return inspection ===")
    print(f"Events:      {len(zero):,}")
    print(f"Securities:  {zero['secCode'].nunique():,}")
    print()

    cols = [
        "secCode",
        "edinetCode",
        "curr_docID",
        "eventTradingDate",
        "marketModelStatus",
        "productionEstimationObs",
        "estimationStartDate",
        "estimationEndDate",
        "rawSecurityRows",
        "validCloseRows",
        "rawRowsInWindow",
        "validCloseRowsInWindow",
        "consecutiveReturnsTotal",
        "usableTopixReturnsTotal",
        "explanation",
    ]
    print(zero[cols].sort_values(
        ["secCode", "eventTradingDate"]
    ).to_string(index=False))
    print()

    codes = set(zero["secCode"].dropna().astype(str))

    frames = []
    for path in sorted(
        prices_dir.glob("equities_bars_daily_*.csv.gz")
    ):
        d = pd.read_csv(
            path,
            usecols=["Date", "Code", "C", "AdjFactor"],
            dtype={"Code": "string"},
            low_memory=False,
        )
        d["Code"] = norm_code(d["Code"])
        d = d.loc[d["Code"].isin(codes)].copy()
        if d.empty:
            continue

        d["Date"] = pd.to_datetime(
            d["Date"], errors="coerce"
        )
        d["C"] = pd.to_numeric(d["C"], errors="coerce")
        d["AdjFactor"] = pd.to_numeric(
            d["AdjFactor"], errors="coerce"
        )
        frames.append(d)

    if not frames:
        print("No raw rows found for zero-return securities.")
        return 0

    prices = pd.concat(frames, ignore_index=True)
    prices = prices.sort_values(["Code", "Date"])

    print("=== Security-level raw-price diagnostics ===")
    for code in sorted(codes):
        p = prices.loc[prices["Code"].eq(code)].copy()

        valid = (
            p["C"].notna()
            & np.isfinite(p["C"])
            & (p["C"] > 0)
        )

        print()
        print(f"--- {code} ---")
        print(f"raw rows:          {len(p):,}")
        print(f"valid closes:      {int(valid.sum()):,}")
        print(f"missing closes:    {int(p['C'].isna().sum()):,}")
        print(f"zero closes:       {int(p['C'].eq(0).sum()):,}")
        print(f"negative closes:   {int((p['C'] < 0).sum()):,}")
        print(f"first raw date:    {p['Date'].min().date()}")
        print(f"last raw date:     {p['Date'].max().date()}")

        if valid.any():
            pv = p.loc[valid]
            print(f"first valid close: {pv['Date'].min().date()}")
            print(f"last valid close:  {pv['Date'].max().date()}")

        print("\nfirst 10 raw rows:")
        print(
            p[["Date", "C", "AdjFactor"]]
            .head(10)
            .to_string(index=False)
        )

        print("\nlast 10 raw rows:")
        print(
            p[["Date", "C", "AdjFactor"]]
            .tail(10)
            .to_string(index=False)
        )

        valid_sample = p.loc[
            valid, ["Date", "C", "AdjFactor"]
        ]
        if not valid_sample.empty:
            print("\nvalid-close rows:")
            if len(valid_sample) <= 25:
                print(valid_sample.to_string(index=False))
            else:
                print(valid_sample.head(10).to_string(index=False))
                print("...")
                print(valid_sample.tail(10).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
