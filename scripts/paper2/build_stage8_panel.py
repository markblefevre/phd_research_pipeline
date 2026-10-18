#!/usr/bin/env python3
"""Build a non-destructive Paper 2 Stage 8 filing-level sentiment/market-reaction panel.

Inputs: Stage 7 final_event_study_table.csv and Stage 6F-B component_sentiment.csv.
Outputs: a new CSV plus a JSON audit in a NEW output directory.

This script never modifies inputs or overwrites existing output files.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

import numpy as np
import pandas as pd

KEYS = ["prev_docID", "curr_docID"]
WINDOWS = ("0_0", "0_1", "m1_1")
SENTIMENT_COLUMNS = [
    "model", "promptSha256",
    "persistentChars", "persistentTextSha256", "persistentStatus",
    "persistentLlmPositive", "persistentLlmNegative", "persistentLlmNet",
    "persistentLlmPositiveEqualWeight", "persistentLlmNegativeEqualWeight",
    "persistentLlmNetEqualWeight", "persistentLlmMedianUnitNet",
    "persistentLlmNumUnits", "persistentLlmNarrativeChars",
    "persistentLlmForwardShare", "persistentLlmRiskShare", "persistentSentiment",
    "novelChars", "novelTextSha256", "novelStatus",
    "novelLlmPositive", "novelLlmNegative", "novelLlmNet",
    "novelLlmPositiveEqualWeight", "novelLlmNegativeEqualWeight",
    "novelLlmNetEqualWeight", "novelLlmMedianUnitNet",
    "novelLlmNumUnits", "novelLlmNarrativeChars",
    "novelLlmForwardShare", "novelLlmRiskShare", "novelSentiment",
    "novelMinusPersistentSentiment",
]


def require_columns(frame: pd.DataFrame, columns: list[str], name: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{name} missing columns: {missing}")


def check_keys(frame: pd.DataFrame, name: str) -> None:
    if frame[KEYS].isna().any().any():
        raise ValueError(f"{name} contains null pair identifiers")
    duplicates = frame.duplicated(KEYS, keep=False)
    if duplicates.any():
        examples = frame.loc[duplicates, KEYS].head(5).to_dict("records")
        raise ValueError(f"{name} has {int(duplicates.sum())} duplicate-key rows; examples={examples}")


def get_paths(root: Path, explicit_sentiment: str | None) -> tuple[Path, Path]:
    stage7 = root / "data/interim/paper2/market_reaction/final_event_study_table.csv"
    if explicit_sentiment:
        stage6 = Path(explicit_sentiment).expanduser().resolve()
    else:
        runs = root / "data/interim/paper2/sentiment/persistent_novel/llm_luna_stage6fb/runs"
        candidates = sorted(runs.glob("*/component_sentiment.csv"))
        if len(candidates) != 1:
            raise ValueError(
                f"Found {len(candidates)} Stage 6F-B run CSVs. "
                "Pass --sentiment-csv explicitly to select the frozen production run."
            )
        stage6 = candidates[0]
    for p in (stage7, stage6):
        if not p.is_file():
            raise FileNotFoundError(p)
    return stage7, stage6


def safe_bool(series: pd.Series, name: str) -> pd.Series:
    """Convert bool or string-encoded booleans; reject unexpected values."""
    normalized = series.astype("string").str.strip().str.lower()
    mapping = {"true": True, "false": False, "1": True, "0": False}
    bad = normalized.notna() & ~normalized.isin(mapping)
    if bad.any():
        raise ValueError(f"Unexpected {name} values: {normalized[bad].unique().tolist()[:10]}")
    return normalized.map(mapping).astype("boolean")


def build(args: argparse.Namespace) -> tuple[Path, Path, dict]:
    root = Path(args.root).expanduser().resolve()
    stage7_path, stage6_path = get_paths(root, args.sentiment_csv)
    outdir = (Path(args.output_dir).expanduser().resolve() if args.output_dir
              else root / "data/interim/paper2/analysis/stage8_panel")
    output_csv = outdir / "stage8_filing_panel.csv"
    output_json = outdir / "stage8_panel_audit.json"
    if output_csv.exists() or output_json.exists():
        raise FileExistsError(f"Output exists. Refusing to overwrite: {output_csv} or {output_json}. "
                              "Choose a new --output-dir.")

    stage7 = pd.read_csv(stage7_path, low_memory=False, dtype={k: "string" for k in KEYS})
    stage6 = pd.read_csv(stage6_path, low_memory=False, dtype={k: "string" for k in KEYS})
    require_columns(stage7, KEYS + ["eventEligible", "marketModelStatus"] +
                    [f"car_{w}" for w in WINDOWS] +
                    [f"carValid_{w}" for w in WINDOWS] +
                    [f"eventStudySample_{w}" for w in WINDOWS], "Stage 7")
    require_columns(stage6, KEYS + SENTIMENT_COLUMNS, "Stage 6F-B")
    check_keys(stage7, "Stage 7")
    check_keys(stage6, "Stage 6F-B")

    merged = stage7.merge(stage6[KEYS + SENTIMENT_COLUMNS], on=KEYS,
                          how="left", validate="one_to_one", indicator=True)
    counts = merged["_merge"].value_counts().to_dict()
    if counts.get("left_only", 0) != 0 or len(merged) != len(stage7):
        raise ValueError(f"Unmatched Stage 7 keys or row count changed: {counts}")
    reverse = stage6[KEYS].merge(stage7[KEYS], on=KEYS, how="left", indicator=True)
    if (reverse["_merge"] == "left_only").any():
        raise ValueError(f"Stage 6F-B contains {int((reverse['_merge'] == 'left_only').sum())} unmatched pairs")
    merged = merged.drop(columns=["_merge"])

    for col in ["persistentSentiment", "novelSentiment", "novelMinusPersistentSentiment",
                "persistentChars", "novelChars"]:
        merged[col] = pd.to_numeric(merged[col], errors="raise")
    if merged["persistentSentiment"].isna().any():
        raise ValueError("Persistent sentiment has missing values")
    expected_delta = merged["novelSentiment"] - merged["persistentSentiment"]
    delta_mismatch = (expected_delta.notna() & merged["novelMinusPersistentSentiment"].notna() &
                      ~np.isclose(expected_delta, merged["novelMinusPersistentSentiment"], atol=1e-8, rtol=1e-8))
    if delta_mismatch.any():
        raise ValueError(f"Sentiment delta mismatch: {int(delta_mismatch.sum())} rows")
    if (expected_delta.isna() != merged["novelMinusPersistentSentiment"].isna()).any():
        raise ValueError("Sentiment delta null pattern does not match inputs")

    chars = merged["persistentChars"] + merged["novelChars"]
    if (chars <= 0).any():
        raise ValueError("Found zero/negative combined component character counts")
    merged["novelShareComponentChars"] = merged["novelChars"] / chars
    merged["persistentShareComponentChars"] = merged["persistentChars"] / chars
    # Note: these are Stage 6E component character shares, not necessarily
    # the same definition as other novelty measures already in Stage 7.

    missing_novel = merged["novelSentiment"].isna()
    bad_missing = missing_novel & (merged["novelChars"] != 0)
    if bad_missing.any():
        raise ValueError(f"Missing novel sentiment for nonempty components: {int(bad_missing.sum())}")
    bad_present = ~missing_novel & (merged["novelChars"] == 0)
    if bad_present.any():
        raise ValueError(f"Novel sentiment present for empty components: {int(bad_present.sum())}")

    audit = {
        "createdUtc": datetime.now(timezone.utc).isoformat(),
        "stage7Input": str(stage7_path),
        "stage6fbInput": str(stage6_path),
        "stage7Rows": len(stage7),
        "stage6fbRows": len(stage6),
        "panelRows": len(merged),
        "matchedPairs": counts.get("both", 0),
        "unmatchedStage7Pairs": counts.get("left_only", 0),
        "unmatchedStage6fbPairs": 0,
        "missingNovelSentiment": int(missing_novel.sum()),
        "emptyNovelComponents": int((merged["novelChars"] == 0).sum()),
        "distinctModels": sorted(merged["model"].dropna().unique().tolist()),
        "distinctPromptSha256": sorted(merged["promptSha256"].dropna().unique().tolist()),
        "carWindows": {},
        "notes": [
            "No input files were modified.",
            "CAR values in car_* columns are in decimal-return units; car_pp_* are percentage points.",
            "Missing novel sentiment for empty novel components is preserved as missing, not zero.",
            "novelShareComponentChars is a component-character ratio, not a market-model measure.",
            "This is a measurement panel, not a completed regression or a certification of the historical venue audit.",
        ],
    }
    for w in WINDOWS:
        col = f"car_{w}"
        merged[col] = pd.to_numeric(merged[col], errors="raise")
        merged[f"car_pp_{w}"] = 100 * merged[col]
        valid = safe_bool(merged[f"carValid_{w}"], f"carValid_{w}")
        sample = safe_bool(merged[f"eventStudySample_{w}"], f"eventStudySample_{w}")
        if (valid.fillna(False) != sample.fillna(False)).any():
            raise ValueError(f"Window {w}: carValid and eventStudySample differ")
        if (sample.fillna(False) & merged[col].isna()).any():
            raise ValueError(f"Window {w}: sample marked valid but CAR missing")
        if ((~sample.fillna(False)) & merged[col].notna()).any():
            raise ValueError(f"Window {w}: CAR populated outside final sample")
        audit["carWindows"][w] = {
            "finalSampleRows": int(sample.fillna(False).sum()),
            "nonmissingCar": int(merged[col].notna().sum()),
            "completeCaseWithBothSentiments": int((sample.fillna(False) &
                merged["persistentSentiment"].notna() & merged["novelSentiment"].notna()).sum()),
        }

    if args.expected_pairs is not None and len(merged) != args.expected_pairs:
        raise ValueError(f"Expected {args.expected_pairs} pairs, got {len(merged)}")
    if args.expected_empty_novel is not None and int(missing_novel.sum()) != args.expected_empty_novel:
        raise ValueError(f"Expected {args.expected_empty_novel} empty novel components, got {int(missing_novel.sum())}")

    outdir.mkdir(parents=True, exist_ok=True)
    # 'x' is exclusive creation; never replace existing outputs, even if another process races us.
    with output_csv.open("x", encoding="utf-8", newline="") as handle:
        merged.to_csv(handle, index=False)
    with output_json.open("x", encoding="utf-8") as handle:
        json.dump(audit, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return output_csv, output_json, audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="~/paper2_local", help="Paper 2 local root")
    parser.add_argument("--sentiment-csv", help="Explicit frozen Stage 6F-B CSV (required if multiple runs exist)")
    parser.add_argument("--output-dir", help="New output directory; refuses to overwrite existing files")
    parser.add_argument("--expected-pairs", type=int, default=33046)
    parser.add_argument("--expected-empty-novel", type=int, default=9)
    args = parser.parse_args()
    try:
        csv_path, json_path, audit = build(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Wrote: {csv_path}\nWrote: {json_path}")
    print(json.dumps({k: audit[k] for k in ["panelRows", "matchedPairs", "missingNovelSentiment", "carWindows"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
