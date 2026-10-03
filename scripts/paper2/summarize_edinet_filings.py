#!/usr/bin/env python3
"""Summarize the Paper 2 EDINET filing manifest.

This is a read-only reporting/QC utility for ``filings.csv``.  It is safe to
run while the EDINET downloader is active because the downloader atomically
replaces the manifest after a completed write; this script only reads the
current snapshot.

Default inputs/outputs are relative to the repository root::

    data/interim/paper2/edinet/filings.csv
    data/interim/paper2/edinet/summary/

Outputs
-------
filing_summary.json
    High-level counts, date coverage, missingness, duplicate counts, and flag
    distributions.
filings_by_submission_year.csv
    Filing and firm counts by submission year.
filings_by_fiscal_year.csv
    Filing and firm counts by fiscal year (year of periodEnd).
firm_year_counts.csv
    Number of filings for each EDINET-code/fiscal-year pair.
duplicate_firm_years.csv
    Firm-years represented by more than one filing, for investigation.
missing_fields.csv
    Missing-value counts and rates for key manifest fields.
field_distributions.csv
    Counts for selected EDINET categorical/status fields and format flags.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_FILINGS_CSV = Path("data/interim/paper2/edinet/filings.csv")
DEFAULT_OUTPUT_DIR = Path("data/interim/paper2/edinet/summary")

KEY_FIELDS = [
    "docID",
    "edinetCode",
    "secCode",
    "filerName",
    "periodStart",
    "periodEnd",
    "submitDateTime",
    "docTypeCode",
    "ordinanceCode",
    "formCode",
]

DISTRIBUTION_FIELDS = [
    "docTypeCode",
    "ordinanceCode",
    "formCode",
    "withdrawalStatus",
    "docInfoEditStatus",
    "disclosureStatus",
    "xbrlFlag",
    "pdfFlag",
    "csvFlag",
    "englishDocFlag",
    "legalStatus",
]


def _scalar(value: Any) -> Any:
    """Convert pandas/numpy scalar values into JSON-serializable Python values."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _safe_year(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.year.astype("Int64")


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(path)


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def summarize_filings(
    filings_csv: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Read an EDINET filing manifest and write summary/QC artifacts.

    The input manifest is never modified.
    """
    filings_csv = Path(filings_csv)
    output_dir = Path(output_dir)

    if not filings_csv.exists():
        raise FileNotFoundError(f"Filings CSV not found: {filings_csv}")

    # The downloader writes filings.csv atomically, so this reads one complete
    # manifest snapshot even if the downloader continues after we start.
    df = pd.read_csv(filings_csv, dtype=str, keep_default_na=True)
    if df.empty:
        raise ValueError(f"Filings CSV is empty: {filings_csv}")

    missing_required = [c for c in ("docID", "edinetCode", "periodEnd", "submitDateTime") if c not in df]
    if missing_required:
        raise ValueError(f"Required columns missing from {filings_csv}: {missing_required}")

    work = df.copy()
    work["submission_year"] = _safe_year(work["submitDateTime"])
    work["fiscal_year"] = _safe_year(work["periodEnd"])

    # ---- High-level summary -------------------------------------------------
    submit_dt = pd.to_datetime(work["submitDateTime"], errors="coerce")
    period_end_dt = pd.to_datetime(work["periodEnd"], errors="coerce")

    duplicate_docid_rows = int(work.duplicated(subset=["docID"], keep=False).sum())
    duplicate_docids = int(work.loc[work.duplicated("docID", keep=False), "docID"].nunique())

    fy_valid = work.dropna(subset=["edinetCode", "fiscal_year"])
    firm_year_sizes = (
        fy_valid.groupby(["edinetCode", "fiscal_year"], dropna=False)
        .size()
        .rename("filings")
        .reset_index()
        .sort_values(["fiscal_year", "edinetCode"], kind="stable")
    )
    duplicate_firm_years = firm_year_sizes.loc[firm_year_sizes["filings"] > 1].copy()

    summary: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_csv": str(filings_csv),
        "total_filings": int(len(work)),
        "unique_doc_ids": int(work["docID"].nunique(dropna=True)),
        "duplicate_doc_ids": duplicate_docids,
        "rows_in_duplicate_doc_ids": duplicate_docid_rows,
        "unique_edinet_codes": int(work["edinetCode"].nunique(dropna=True)),
        "unique_security_codes": int(work["secCode"].nunique(dropna=True)) if "secCode" in work else None,
        "unique_firm_years": int(len(firm_year_sizes)),
        "duplicate_firm_years": int(len(duplicate_firm_years)),
        "first_submission_datetime": submit_dt.min().isoformat() if submit_dt.notna().any() else None,
        "last_submission_datetime": submit_dt.max().isoformat() if submit_dt.notna().any() else None,
        "first_fiscal_period_end": period_end_dt.min().date().isoformat() if period_end_dt.notna().any() else None,
        "last_fiscal_period_end": period_end_dt.max().date().isoformat() if period_end_dt.notna().any() else None,
        "submission_year_min": int(work["submission_year"].min()) if work["submission_year"].notna().any() else None,
        "submission_year_max": int(work["submission_year"].max()) if work["submission_year"].notna().any() else None,
        "fiscal_year_min": int(work["fiscal_year"].min()) if work["fiscal_year"].notna().any() else None,
        "fiscal_year_max": int(work["fiscal_year"].max()) if work["fiscal_year"].notna().any() else None,
    }

    # ---- Missing fields -----------------------------------------------------
    missing_rows: list[dict[str, Any]] = []
    for col in KEY_FIELDS:
        if col not in work:
            continue
        count = int(work[col].isna().sum() + work[col].fillna("").str.strip().eq("").sum())
        # The two conditions above are disjoint because fillna("") turns NA into
        # empty strings; correct double counting explicitly.
        na_count = int(work[col].isna().sum())
        blank_non_na = int(work[col].notna().sum() - work.loc[work[col].notna(), col].str.strip().ne("").sum())
        count = na_count + blank_non_na
        missing_rows.append(
            {
                "field": col,
                "missing_count": count,
                "missing_rate": count / len(work),
            }
        )
    missing_df = pd.DataFrame(missing_rows)

    summary["missing_key_fields"] = {
        row["field"]: {
            "count": int(row["missing_count"]),
            "rate": float(row["missing_rate"]),
        }
        for row in missing_rows
    }

    # ---- By submission year -------------------------------------------------
    submission_year = (
        work.dropna(subset=["submission_year"])
        .groupby("submission_year")
        .agg(
            filings=("docID", "size"),
            unique_doc_ids=("docID", "nunique"),
            firms=("edinetCode", "nunique"),
            security_codes=("secCode", "nunique") if "secCode" in work else ("edinetCode", "nunique"),
        )
        .reset_index()
        .sort_values("submission_year")
    )

    # ---- By fiscal year -----------------------------------------------------
    fiscal_year = (
        work.dropna(subset=["fiscal_year"])
        .groupby("fiscal_year")
        .agg(
            filings=("docID", "size"),
            unique_doc_ids=("docID", "nunique"),
            firms=("edinetCode", "nunique"),
            security_codes=("secCode", "nunique") if "secCode" in work else ("edinetCode", "nunique"),
        )
        .reset_index()
        .sort_values("fiscal_year")
    )

    # ---- Categorical/status distributions ----------------------------------
    distribution_frames: list[pd.DataFrame] = []
    for col in DISTRIBUTION_FIELDS:
        if col not in work:
            continue
        counts = (
            work[col]
            .fillna("<MISSING>")
            .astype(str)
            .value_counts(dropna=False)
            .rename_axis("value")
            .reset_index(name="count")
        )
        counts.insert(0, "field", col)
        counts["rate"] = counts["count"] / len(work)
        distribution_frames.append(counts)

    distributions = (
        pd.concat(distribution_frames, ignore_index=True)
        if distribution_frames
        else pd.DataFrame(columns=["field", "value", "count", "rate"])
    )

    # ---- Write outputs ------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(summary, output_dir / "filing_summary.json")
    _write_csv(submission_year, output_dir / "filings_by_submission_year.csv")
    _write_csv(fiscal_year, output_dir / "filings_by_fiscal_year.csv")
    _write_csv(firm_year_sizes, output_dir / "firm_year_counts.csv")
    _write_csv(duplicate_firm_years, output_dir / "duplicate_firm_years.csv")
    _write_csv(missing_df, output_dir / "missing_fields.csv")
    _write_csv(distributions, output_dir / "field_distributions.csv")

    return summary


def _print_summary(summary: dict[str, Any]) -> None:
    print("EDINET filing summary")
    print("---------------------")
    print(f"Total filings:          {summary['total_filings']:,}")
    print(f"Unique docIDs:          {summary['unique_doc_ids']:,}")
    print(f"Unique EDINET codes:    {summary['unique_edinet_codes']:,}")
    if summary.get("unique_security_codes") is not None:
        print(f"Unique security codes:  {summary['unique_security_codes']:,}")
    print(f"Unique firm-years:      {summary['unique_firm_years']:,}")
    print(f"Duplicate docIDs:       {summary['duplicate_doc_ids']:,}")
    print(f"Duplicate firm-years:   {summary['duplicate_firm_years']:,}")
    print(f"Submission coverage:    {summary['first_submission_datetime']} -> {summary['last_submission_datetime']}")
    print(f"Fiscal period coverage: {summary['first_fiscal_period_end']} -> {summary['last_fiscal_period_end']}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--filings-csv",
        type=Path,
        default=DEFAULT_FILINGS_CSV,
        help=f"Input filing manifest (default: {DEFAULT_FILINGS_CSV})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for summary/QC outputs (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    summary = summarize_filings(args.filings_csv, args.output_dir)
    _print_summary(summary)
    print(f"\nWrote summary outputs to: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
