#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
join_n225_edinet.py

Join Nikkei 225 data with EDINET data (English or Japanese).

Performs a left join from Nikkei → EDINET using:
  - 'Securities Identification Code' if EDINET is English
  - '証券コード' if EDINET is Japanese
Strips trailing zeros from EDINET codes and saves a merged CSV.

Usage
-----
python join_n225_edinet.py --lang eng --output join_n225_edinet.csv
"""

import argparse
import pandas as pd
from pathlib import Path
from .read_n225_csv import read_n225_csv
from src.edinet.read_edinet_codelist_csv import (
    read_eng_csv_sjis,
    read_jpn_csv_sjis,
    filter_eng_listedcompanies,
    filter_jpn_listedcompanies,
)
from src.utils.project_paths import get_project_root


# =========================
# Core join function
# =========================
def join_n225_edinet(n225_df: pd.DataFrame, edinet_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join Nikkei 225 data with EDINET data (English or Japanese)."""
    edinet_df = edinet_df.copy()

    # Detect EDINET language
    if "Securities Identification Code" in edinet_df.columns:
        key = "Securities Identification Code"
    elif "証券コード" in edinet_df.columns:
        key = "証券コード"
    else:
        raise KeyError("EDINET data missing expected ID column")

    # Normalize EDINET codes
    edinet_df[key] = (
        edinet_df[key]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"0$", "", regex=True)
        .str.zfill(4)
    )

    # Normalize Nikkei codes
    n225_df = n225_df.copy()
    n225_df["code"] = n225_df["code"].astype(str).str.zfill(4)

    # Left join
    merged = n225_df.merge(edinet_df, how="left", left_on="code", right_on=key)
    return merged


# =========================
# Argument parsing
# =========================
def parse_args():
    project_root = get_project_root()

    ap = argparse.ArgumentParser(
        description="Join Nikkei 225 and EDINET master lists (English or Japanese)."
    )
    ap.add_argument(
        "--lang",
        choices=["eng", "jpn"],
        default="eng",
        help="Select EDINET language: eng or jpn (default: eng)",
    )
    ap.add_argument(
        "--nikkei",
        type=Path,
        default=project_root / "nikkei" / "nikkei225_all.csv",
        help="Path to Nikkei 225 CSV file",
    )
    ap.add_argument(
        "--edinet-dir",
        type=Path,
        default=project_root / "EDINET Information" / "EDINET Code",
        help="Path to EDINET Information directory",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=project_root / "Code" / "out" / "join_n225_edinet.csv",
        help="Output CSV file path",
    )
    return ap.parse_args()


# =========================
# Main
# =========================
def main():
    args = parse_args()
    print(f"[INFO] Using project root: {get_project_root()}")

    # --- Load Nikkei 225 ---
    n225_df = read_n225_csv(args.nikkei)
    print(f"[INFO] Loaded Nikkei 225 file: {args.nikkei} ({len(n225_df)} rows)")

    # --- Load EDINET master list ---
    if args.lang == "eng":
        _, edinet_df = read_eng_csv_sjis(args.edinet_dir, Path("EdinetcodeDlInfoENG.csv"))
        edinet_df = filter_eng_listedcompanies(edinet_df)
        print("[INFO] Loaded English EDINET list.")
    else:
        _, edinet_df = read_jpn_csv_sjis(args.edinet_dir, Path("EdinetcodeDlInfoJPN.csv"))
        edinet_df = filter_jpn_listedcompanies(edinet_df)
        print("[INFO] Loaded Japanese EDINET list.")

    # --- Join ---
    merged_df = join_n225_edinet(n225_df, edinet_df)
    print(f"[INFO] Merged DataFrame has {len(merged_df)} rows")

    # --- Save ---
    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[INFO] Saved merged file to: {args.output}")

    # --- Preview ---
    print("\nSample rows:")
    print(merged_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
