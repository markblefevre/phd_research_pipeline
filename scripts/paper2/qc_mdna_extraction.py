#!/usr/bin/env python3
"""
QC the completed Paper 2 MD&A extraction corpus.

Purpose
-------
This script is intentionally read-only. It does not modify the filing manifest,
the extraction manifest, or any extracted text files.

The goal is to answer a few concrete questions before moving to Stage 3:

1. Did Stage 2 cover the entire Stage 1 filing universe?
2. How many extractions succeeded or failed?
3. Were successful extractions mostly obtained through the standardized XBRL tag?
4. Are there suspiciously short or long extracted texts that deserve inspection?
5. Are there any "successful" manifest rows whose output files are missing or empty?
6. Are failures concentrated in a small number of issuers rather than scattered
   randomly across the Japanese-firm sample?
7. Are there any duplicate document IDs or other structural inconsistencies?

Outputs are written under:

    data/interim/paper2/mdna/qc/

The resulting files provide a reproducible audit trail for the canonical MD&A
corpus before downstream text statistics, longitudinal matching, sentiment,
and novelty calculations.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_FILINGS_CSV = Path("data/interim/paper2/edinet/filings.csv")
DEFAULT_EXTRACTION_MANIFEST = Path(
    "data/interim/paper2/mdna/extraction_manifest.csv"
)
DEFAULT_OUTPUT_DIR = Path("data/interim/paper2/mdna/qc")

# These are review thresholds, not exclusion rules. A short MD&A can be fully
# legitimate (for example, a newly formed company with little operating history).
DEFAULT_SHORT_THRESHOLD = 500
DEFAULT_LONG_THRESHOLD = 50_000


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a CSV atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _write_json(payload: dict[str, Any], path: Path) -> None:
    """Write JSON atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _length_summary(ok_df: pd.DataFrame) -> dict[str, Any]:
    """
    Summarize successful extraction lengths.

    Percentiles help identify the corpus tails. They are descriptive and should
    not be treated as automatic trimming thresholds.
    """
    chars = pd.to_numeric(ok_df["textChars"], errors="coerce").dropna()

    if chars.empty:
        return {
            "count": 0,
            "min": None,
            "p01": None,
            "p05": None,
            "median": None,
            "mean": None,
            "p95": None,
            "p99": None,
            "max": None,
        }

    return {
        "count": int(chars.shape[0]),
        "min": int(chars.min()),
        "p01": float(chars.quantile(0.01)),
        "p05": float(chars.quantile(0.05)),
        "median": float(chars.median()),
        "mean": float(chars.mean()),
        "p95": float(chars.quantile(0.95)),
        "p99": float(chars.quantile(0.99)),
        "max": int(chars.max()),
    }


def run_qc(
    *,
    filings_csv: Path,
    extraction_manifest: Path,
    output_dir: Path,
    short_threshold: int = DEFAULT_SHORT_THRESHOLD,
    long_threshold: int = DEFAULT_LONG_THRESHOLD,
) -> dict[str, Any]:
    """
    Run structural and distributional QC for Stage 2.

    This function does not infer that unusual observations are wrong. It
    identifies observations that deserve inspection and reconciles the
    extraction corpus against the Stage 1 filing universe.
    """
    filings_csv = Path(filings_csv)
    extraction_manifest = Path(extraction_manifest)
    output_dir = Path(output_dir)

    if not filings_csv.exists():
        raise FileNotFoundError(f"Missing Stage 1 filings manifest: {filings_csv}")
    if not extraction_manifest.exists():
        raise FileNotFoundError(
            f"Missing Stage 2 extraction manifest: {extraction_manifest}"
        )

    filings = pd.read_csv(filings_csv, dtype=str, keep_default_na=True)
    extraction = pd.read_csv(
        extraction_manifest, dtype=str, keep_default_na=True
    )

    required_filings = {"docID", "edinetCode"}
    missing = required_filings - set(filings.columns)
    if missing:
        raise ValueError(
            f"{filings_csv} missing required columns: {sorted(missing)}"
        )

    required_extraction = {
        "docID",
        "edinetCode",
        "status",
        "method",
        "textChars",
        "outputPath",
    }
    missing = required_extraction - set(extraction.columns)
    if missing:
        raise ValueError(
            f"{extraction_manifest} missing required columns: {sorted(missing)}"
        )

    filings = filings.copy()
    extraction = extraction.copy()

    for df in (filings, extraction):
        df["docID"] = df["docID"].fillna("").astype(str).str.strip()
        df["edinetCode"] = (
            df["edinetCode"].fillna("").astype(str).str.strip()
        )

    extraction["textChars_num"] = pd.to_numeric(
        extraction["textChars"], errors="coerce"
    )

    # ------------------------------------------------------------------
    # 1. Stage 1 / Stage 2 reconciliation
    # ------------------------------------------------------------------
    # We want a one-to-one audit trail from filings.csv into the extraction
    # manifest. Missing rows matter more than individual parser failures because
    # they could indicate that a filing was silently skipped.
    filings_docids = set(filings.loc[filings["docID"].ne(""), "docID"])
    extraction_docids = set(
        extraction.loc[extraction["docID"].ne(""), "docID"]
    )

    missing_from_extraction_ids = sorted(filings_docids - extraction_docids)
    extra_in_extraction_ids = sorted(extraction_docids - filings_docids)

    missing_from_extraction = filings[
        filings["docID"].isin(missing_from_extraction_ids)
    ].copy()
    extra_in_extraction = extraction[
        extraction["docID"].isin(extra_in_extraction_ids)
    ].copy()

    duplicate_filings = filings[
        filings.duplicated(subset=["docID"], keep=False)
    ].sort_values(["docID", "edinetCode"], kind="stable")
    duplicate_extraction = extraction[
        extraction.duplicated(subset=["docID"], keep=False)
    ].sort_values(["docID", "edinetCode"], kind="stable")

    # ------------------------------------------------------------------
    # 2. Status and extraction-method distributions
    # ------------------------------------------------------------------
    status_counts = (
        extraction["status"]
        .fillna("<MISSING>")
        .value_counts(dropna=False)
        .rename_axis("status")
        .reset_index(name="count")
    )
    status_counts["rate"] = status_counts["count"] / len(extraction)

    method_counts = (
        extraction["method"]
        .fillna("<MISSING>")
        .value_counts(dropna=False)
        .rename_axis("method")
        .reset_index(name="count")
    )
    method_counts["rate"] = method_counts["count"] / len(extraction)

    ok_df = extraction.loc[extraction["status"].eq("ok")].copy()
    failures = extraction.loc[~extraction["status"].eq("ok")].copy()
    fallback_cases = extraction.loc[
        extraction["method"].eq("anchor_fallback")
    ].copy()

    # Concentrated failures in a small number of issuers are qualitatively
    # different from random failures spread across many Japanese firms.
    failure_by_edinet = (
        failures.groupby("edinetCode", dropna=False)
        .size()
        .rename("failures")
        .reset_index()
        .sort_values(["failures", "edinetCode"], ascending=[False, True])
    )

    if "filerName" in extraction.columns:
        failure_names = (
            failures[["edinetCode", "filerName"]]
            .drop_duplicates(subset=["edinetCode"])
        )
        failure_by_edinet = failure_by_edinet.merge(
            failure_names, on="edinetCode", how="left"
        )

    # ------------------------------------------------------------------
    # 3. Text-length tail checks
    # ------------------------------------------------------------------
    # These exception lists are for review only. Unusually short or long text
    # is not automatically invalid.
    short_texts = ok_df.loc[
        ok_df["textChars_num"].notna()
        & (ok_df["textChars_num"] < short_threshold)
    ].copy()
    long_texts = ok_df.loc[
        ok_df["textChars_num"].notna()
        & (ok_df["textChars_num"] > long_threshold)
    ].copy()

    short_texts = short_texts.sort_values(
        ["textChars_num", "docID"], kind="stable"
    )
    long_texts = long_texts.sort_values(
        ["textChars_num", "docID"],
        ascending=[False, True],
        kind="stable",
    )

    # ------------------------------------------------------------------
    # 4. Output-file integrity
    # ------------------------------------------------------------------
    # A manifest row marked "ok" should have a corresponding non-empty .txt
    # file. This catches accidental deletion, interrupted writes, or path drift.
    output_issues: list[dict[str, Any]] = []

    for _, row in ok_df.iterrows():
        output_path_raw = row.get("outputPath")
        output_path = None
        if pd.notna(output_path_raw) and str(output_path_raw).strip():
            output_path = Path(str(output_path_raw))

        issue = None
        actual_bytes = None

        if output_path is None:
            issue = "missing_output_path"
        else:
            try:
                actual_bytes = output_path.stat().st_size
                if actual_bytes <= 0:
                    issue = "empty_output_file"
            except FileNotFoundError:
                issue = "missing_output_file"

        if issue is not None:
            output_issues.append(
                {
                    "docID": row.get("docID"),
                    "edinetCode": row.get("edinetCode"),
                    "filerName": row.get("filerName"),
                    "status": row.get("status"),
                    "method": row.get("method"),
                    "textChars": row.get("textChars"),
                    "outputPath": row.get("outputPath"),
                    "issue": issue,
                    "actualBytes": actual_bytes,
                }
            )

    output_issues_df = pd.DataFrame(
        output_issues,
        columns=[
            "docID",
            "edinetCode",
            "filerName",
            "status",
            "method",
            "textChars",
            "outputPath",
            "issue",
            "actualBytes",
        ],
    )

    successful = int(ok_df.shape[0])
    failed = int(failures.shape[0])
    total = int(extraction.shape[0])
    primary_tag_successes = int(ok_df["method"].eq("tag").sum())
    fallback_successes = int(
        ok_df["method"].eq("anchor_fallback").sum()
    )

    summary = {
        "stage1": {
            "filings_rows": int(len(filings)),
            "unique_doc_ids": int(
                filings.loc[filings["docID"].ne(""), "docID"].nunique()
            ),
            "duplicate_doc_id_rows": int(len(duplicate_filings)),
        },
        "stage2": {
            "manifest_rows": total,
            "unique_doc_ids": int(
                extraction.loc[
                    extraction["docID"].ne(""), "docID"
                ].nunique()
            ),
            "duplicate_doc_id_rows": int(len(duplicate_extraction)),
            "successful": successful,
            "failed": failed,
            "success_rate": successful / total if total else None,
            "primary_tag_successes": primary_tag_successes,
            "anchor_fallback_successes": fallback_successes,
            "primary_tag_share_of_successes": (
                primary_tag_successes / successful if successful else None
            ),
        },
        "reconciliation": {
            "missing_from_extraction": len(missing_from_extraction_ids),
            "extra_in_extraction": len(extra_in_extraction_ids),
            "output_file_issues_for_ok_rows": int(len(output_issues_df)),
        },
        "text_length": _length_summary(ok_df),
        "review_thresholds": {
            "short_text_chars_below": int(short_threshold),
            "long_text_chars_above": int(long_threshold),
            "short_text_count": int(len(short_texts)),
            "long_text_count": int(len(long_texts)),
        },
        "failures_by_edinet_code": {
            str(row["edinetCode"]): int(row["failures"])
            for _, row in failure_by_edinet.iterrows()
        },
    }

    output_dir.mkdir(parents=True, exist_ok=True)

    _write_json(summary, output_dir / "mdna_qc_summary.json")
    _write_csv(status_counts, output_dir / "status_counts.csv")
    _write_csv(method_counts, output_dir / "method_counts.csv")
    _write_csv(failures, output_dir / "failures.csv")
    _write_csv(
        failure_by_edinet,
        output_dir / "failures_by_edinet_code.csv",
    )
    _write_csv(fallback_cases, output_dir / "fallback_cases.csv")
    _write_csv(short_texts, output_dir / "short_texts.csv")
    _write_csv(long_texts, output_dir / "long_texts.csv")
    _write_csv(
        missing_from_extraction,
        output_dir / "missing_from_extraction.csv",
    )
    _write_csv(
        extra_in_extraction,
        output_dir / "extra_in_extraction.csv",
    )
    _write_csv(
        duplicate_filings,
        output_dir / "duplicate_docids_filings.csv",
    )
    _write_csv(
        duplicate_extraction,
        output_dir / "duplicate_docids_extraction.csv",
    )
    _write_csv(
        output_issues_df,
        output_dir / "output_file_issues.csv",
    )

    return summary


def _print_summary(summary: dict[str, Any]) -> None:
    stage1 = summary["stage1"]
    stage2 = summary["stage2"]
    reconciliation = summary["reconciliation"]
    lengths = summary["text_length"]
    thresholds = summary["review_thresholds"]

    print("MD&A extraction QC")
    print("==================")
    print(f"Stage 1 filings rows:            {stage1['filings_rows']:,}")
    print(f"Stage 2 manifest rows:           {stage2['manifest_rows']:,}")
    print(f"Successful extractions:          {stage2['successful']:,}")
    print(f"Failed extractions:              {stage2['failed']:,}")
    if stage2["success_rate"] is not None:
        print(f"Success rate:                    {stage2['success_rate']:.4%}")
    print(f"Primary-tag successes:           {stage2['primary_tag_successes']:,}")
    print(f"Anchor-fallback successes:       {stage2['anchor_fallback_successes']:,}")
    print(f"Missing Stage 2 manifest rows:   {reconciliation['missing_from_extraction']:,}")
    print(f"Extra Stage 2 manifest rows:     {reconciliation['extra_in_extraction']:,}")
    print(f"Output-file issues on ok rows:   {reconciliation['output_file_issues_for_ok_rows']:,}")
    print(f"Duplicate Stage 1 docID rows:    {stage1['duplicate_doc_id_rows']:,}")
    print(f"Duplicate Stage 2 docID rows:    {stage2['duplicate_doc_id_rows']:,}")
    print()

    print("Text-length distribution for successful extractions")
    print("---------------------------------------------------")
    print(f"min:      {lengths['min']}")
    print(f"p01:      {lengths['p01']}")
    print(f"p05:      {lengths['p05']}")
    print(f"median:   {lengths['median']}")
    print(f"mean:     {lengths['mean']}")
    print(f"p95:      {lengths['p95']}")
    print(f"p99:      {lengths['p99']}")
    print(f"max:      {lengths['max']}")
    print()

    print(
        f"Short texts (< {thresholds['short_text_chars_below']:,} chars): "
        f"{thresholds['short_text_count']:,}"
    )
    print(
        f"Long texts (> {thresholds['long_text_chars_above']:,} chars): "
        f"{thresholds['long_text_count']:,}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="QC the completed Paper 2 MD&A extraction corpus."
    )
    parser.add_argument(
        "--filings",
        type=Path,
        default=DEFAULT_FILINGS_CSV,
        help=f"Stage 1 filing manifest (default: {DEFAULT_FILINGS_CSV})",
    )
    parser.add_argument(
        "--extraction-manifest",
        type=Path,
        default=DEFAULT_EXTRACTION_MANIFEST,
        help=(
            "Stage 2 extraction manifest "
            f"(default: {DEFAULT_EXTRACTION_MANIFEST})"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"QC output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--short-threshold",
        type=int,
        default=DEFAULT_SHORT_THRESHOLD,
        help=(
            "Flag successful texts shorter than this many characters "
            f"(default: {DEFAULT_SHORT_THRESHOLD})"
        ),
    )
    parser.add_argument(
        "--long-threshold",
        type=int,
        default=DEFAULT_LONG_THRESHOLD,
        help=(
            "Flag successful texts longer than this many characters "
            f"(default: {DEFAULT_LONG_THRESHOLD})"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.short_threshold < 0:
        raise SystemExit("--short-threshold must be >= 0")
    if args.long_threshold < 1:
        raise SystemExit("--long-threshold must be >= 1")
    if args.short_threshold >= args.long_threshold:
        raise SystemExit(
            "--short-threshold must be less than --long-threshold"
        )

    summary = run_qc(
        filings_csv=args.filings,
        extraction_manifest=args.extraction_manifest,
        output_dir=args.output_dir,
        short_threshold=args.short_threshold,
        long_threshold=args.long_threshold,
    )

    _print_summary(summary)
    print()
    print(f"Wrote QC outputs to: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
