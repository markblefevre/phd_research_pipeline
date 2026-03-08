#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from datetime import datetime
import json

import pandas as pd


def _write_qc_json(path: Path, qc: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    qc = dict(qc)
    qc["written_at"] = datetime.now().isoformat(timespec="seconds")
    path.write_text(json.dumps(qc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


# derived from old merge_gpt_lmmd_panel.py
def build_panel(
    *,
    gpt_csv: Path | str,
    lmmd_csv: Path | str,
    out_csv: Path | str,
    how: str = "inner",
    dropna_cols: Optional[Iterable[str]] = ("document_score", "lmmd_net", "pos_rate", "neg_rate"),
    qc_json: Path | str | None = None,
) -> pd.DataFrame:
    """
    Merge GPT sentiment panel with LMMD scores into a single regression-ready panel.

    Goal: match legacy merge_gpt_lmmd_panel.py output structure (column names & drops)
    to avoid breaking downstream code. We can widen later.

    Writes:
      - out_csv
      - qc_json sidecar (default: out_csv + ".qc.json")
    """
    gpt_csv = Path(gpt_csv)
    lmmd_csv = Path(lmmd_csv)
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if qc_json is None:
        qc_json = out_csv.parent / f"{out_csv.name}.qc.json"
    qc_json = Path(qc_json)

    # ---- Load ----
    gpt_df = pd.read_csv(gpt_csv, dtype={"symbol": "string"})
    lmmd_df = pd.read_csv(lmmd_csv, dtype={"symbol": "string"})

    # ---- Harmonize legacy date column names (legacy LMMD uses filing_date_parsed) ----
    if "filing_date" not in lmmd_df.columns and "filing_date_parsed" in lmmd_df.columns:
        lmmd_df = lmmd_df.rename(columns={"filing_date_parsed": "filing_date"})
    # (optional) support GPT legacy name too
    if "filing_date" not in gpt_df.columns and "filing_date_parsed" in gpt_df.columns:
        gpt_df = gpt_df.rename(columns={"filing_date_parsed": "filing_date"})

    # ---- Validate keys exist ----
    if "filing_date" not in gpt_df.columns:
        raise ValueError(f"GPT panel missing 'filing_date' column: {gpt_csv}")
    if "filing_date" not in lmmd_df.columns:
        raise ValueError(f"LMMD panel missing 'filing_date' column: {lmmd_csv}")
    if "symbol" not in gpt_df.columns:
        raise ValueError(f"GPT panel missing 'symbol' column: {gpt_csv}")
    if "symbol" not in lmmd_df.columns:
        raise ValueError(f"LMMD panel missing 'symbol' column: {lmmd_csv}")

    # ---- Parse dates ----
    gpt_df["filing_date"] = pd.to_datetime(gpt_df["filing_date"], errors="coerce")
    lmmd_df["filing_date"] = pd.to_datetime(lmmd_df["filing_date"], errors="coerce")

    # Drop rows where keys are missing
    gpt_df = gpt_df.dropna(subset=["symbol", "filing_date"])
    lmmd_df = lmmd_df.dropna(subset=["symbol", "filing_date"])

    # Ensure symbol is consistent string dtype
    gpt_df["symbol"] = gpt_df["symbol"].astype("string")
    lmmd_df["symbol"] = lmmd_df["symbol"].astype("string")

    # ---- QC pre-merge stats ----
    gpt_keys = gpt_df[["symbol", "filing_date"]].copy()
    lmmd_keys = lmmd_df[["symbol", "filing_date"]].copy()
    qc: Dict[str, Any] = {
        "inputs": {
            "gpt_csv": str(gpt_csv),
            "lmmd_csv": str(lmmd_csv),
        },
        "rows": {
            "gpt": int(len(gpt_df)),
            "lmmd": int(len(lmmd_df)),
        },
        "keys": {
            "gpt_unique_keys": int(gpt_keys.drop_duplicates().shape[0]),
            "lmmd_unique_keys": int(lmmd_keys.drop_duplicates().shape[0]),
            "gpt_duplicate_keys": int(len(gpt_df) - gpt_keys.drop_duplicates().shape[0]),
            "lmmd_duplicate_keys": int(len(lmmd_df) - lmmd_keys.drop_duplicates().shape[0]),
        },
    }

    # ---- Merge ----
    # IMPORTANT: do NOT pass suffixes so pandas uses legacy defaults: _x / _y
    merged = pd.merge(
        gpt_df,
        lmmd_df,
        on=["symbol", "filing_date"],
        how=how,
        # no suffixes -> legacy-style filename_x/filename_y etc.
    )

    qc["rows"]["merged_pre_dropna"] = int(len(merged))

    # ---- Drop missing sentiment columns (matches old behavior) ----
    if dropna_cols:
        missing = [c for c in dropna_cols if c not in merged.columns]
        if missing:
            raise ValueError(f"Merged panel missing expected columns {missing}. Columns={list(merged.columns)}")
        merged = merged.dropna(subset=list(dropna_cols))

    qc["rows"]["merged_post_dropna"] = int(len(merged))

    # ---- Match legacy column drops (old script intentionally pruned these) ----
    merged = merged.drop(
        columns=[
            "filename_x",
            "filename_y",
            "edinet_code_x",
            "edinet_code_y",
            "filing_date_parsed",
            "status",
            "token_count",
            "pos_count",
            "neg_count",
        ],
        errors="ignore",
    )

    qc["outputs"] = {
        "out_csv": str(out_csv),
        "qc_json": str(qc_json),
        "columns": int(len(merged.columns)),
    }

    # Some useful missingness QC (post-dropna, so should be 0 for these)
    if dropna_cols:
        qc["missing_after_dropna"] = {c: int(merged[c].isna().sum()) for c in dropna_cols}

    # ---- Write ----
    merged.to_csv(out_csv, index=False, encoding="utf-8-sig")
    _write_qc_json(qc_json, qc)

    return merged
