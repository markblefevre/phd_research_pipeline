"""Core logic for Stage 3: longitudinal matching.

Stage 3 is intentionally mechanical. It combines the canonical Stage 1 filing
manifest with successful Stage 2 MD&A extractions, orders filings within issuer,
and emits a reproducible manifest of adjacent reporting-period pairs for Stage 4
textual novelty measurement. It also applies an explicit historical domestic-company
eligibility rule at the Stage 3 -> Stage 4 boundary.

Important design principle
--------------------------
Stage 3 does NOT define fiscal years using ``periodEnd.year``. A company may
legitimately change its fiscal year-end and therefore have two reporting periods
ending in the same calendar year. Those are not duplicate firm-years.

Instead, Stage 3 works directly with reporting-period start/end dates and flags
whether adjacent periods are contiguous and approximately annual.

No textual similarity or novelty calculation belongs in this module.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


DOMESTIC_ASR_FORM_CODES = {"030000", "030200", "040000"}


REQUIRED_FILING_COLUMNS = {
    "docID",
    "edinetCode",
    "periodStart",
    "periodEnd",
    "submitDateTime",
    "formCode",
}

REQUIRED_EXTRACTION_COLUMNS = {
    "docID",
    "status",
    "outputPath",
}


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


def _require_columns(
    df: pd.DataFrame,
    required: set[str],
    *,
    label: str,
) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    for column in ("periodStart", "periodEnd", "submitDateTime"):
        if column in work.columns:
            work[column] = pd.to_datetime(work[column], errors="coerce")

    if "periodStart" in work.columns and "periodEnd" in work.columns:
        work["period_days"] = (
            work["periodEnd"] - work["periodStart"]
        ).dt.days + 1

    return work


def _successful_extractions(
    extraction_manifest: pd.DataFrame,
) -> pd.DataFrame:
    """Return successful Stage 2 extraction rows.

    Stage 2 is expected to contain one row per docID. Duplicate docIDs are a
    structural error and Stage 3 should fail rather than guess.
    """
    duplicated = extraction_manifest.duplicated("docID", keep=False)
    if duplicated.any():
        duplicate_ids = (
            extraction_manifest.loc[duplicated, "docID"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        raise ValueError(
            "Extraction manifest contains duplicate docIDs; "
            "cannot construct longitudinal panel safely. "
            f"Examples: {duplicate_ids[:10]}"
        )

    return extraction_manifest.loc[
        extraction_manifest["status"].astype(str).str.lower().eq("ok")
    ].copy()


def _build_panel(
    filings: pd.DataFrame,
    extraction_manifest: pd.DataFrame,
) -> pd.DataFrame:
    """Join successful MD&A extractions to Stage 1 filing metadata."""
    _require_columns(
        filings,
        REQUIRED_FILING_COLUMNS,
        label="filings.csv",
    )
    _require_columns(
        extraction_manifest,
        REQUIRED_EXTRACTION_COLUMNS,
        label="extraction_manifest.csv",
    )

    if filings["docID"].duplicated().any():
        duplicate_ids = (
            filings.loc[filings["docID"].duplicated(keep=False), "docID"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        raise ValueError(
            "filings.csv contains duplicate docIDs; "
            "cannot construct longitudinal panel safely. "
            f"Examples: {duplicate_ids[:10]}"
        )

    filings_work = _parse_dates(filings)
    ok = _successful_extractions(extraction_manifest)

    extraction_fields = [
        column
        for column in (
            "docID",
            "outputPath",
            "method",
            "textChars",
            "matchedLocalName",
            "fallbackScore",
            "xbrlMember",
        )
        if column in ok.columns
    ]

    panel = filings_work.merge(
        ok[extraction_fields],
        on="docID",
        how="inner",
        validate="one_to_one",
    )

    panel["is_domestic_asr"] = panel["formCode"].isin(
        DOMESTIC_ASR_FORM_CODES
    )
    panel["exclusion_reason"] = panel["is_domestic_asr"].map(
        {True: "", False: "foreign_company_form"}
    )

    panel = panel.sort_values(
        ["edinetCode", "periodEnd", "periodStart", "submitDateTime", "docID"],
        kind="stable",
    ).reset_index(drop=True)

    return panel


def _identify_duplicate_reporting_periods(
    panel: pd.DataFrame,
) -> pd.DataFrame:
    """Identify true duplicate reporting periods.

    Two filings are considered a duplicate reporting period when they share the
    same issuer, periodStart, and periodEnd. These are distinct from legitimate
    fiscal-year-end changes, where two periods can end in the same calendar year.
    """
    required = ["edinetCode", "periodStart", "periodEnd"]
    valid = panel.dropna(subset=required).copy()

    mask = valid.duplicated(
        subset=["edinetCode", "periodStart", "periodEnd"],
        keep=False,
    )

    return valid.loc[mask].sort_values(
        ["edinetCode", "periodStart", "periodEnd", "submitDateTime", "docID"],
        kind="stable",
    )


def _construct_adjacent_pairs(
    panel: pd.DataFrame,
    *,
    min_standard_period_days: int,
    max_standard_period_days: int,
) -> pd.DataFrame:
    """Construct adjacent reporting-period pairs within each issuer.

    Pair classification
    -------------------
    standard_annual
        Periods are contiguous and both prior/current periods are approximately
        annual according to the configured duration bounds.

    transition_period
        Periods are contiguous, but one or both reporting periods are outside
        the normal annual-duration bounds. These commonly arise from fiscal
        year-end changes and should be retained as valid longitudinal links.

    noncontiguous
        The current reporting period does not start on the day immediately
        following the prior period end. These require review because there may
        be a missing filing, overlapping periods, or another structural issue.
    """
    pair_rows: list[dict[str, Any]] = []

    usable = panel.dropna(
        subset=["edinetCode", "periodStart", "periodEnd"]
    ).copy()

    for edinet_code, group in usable.groupby(
        "edinetCode",
        sort=True,
        dropna=False,
    ):
        group = group.sort_values(
            ["periodEnd", "periodStart", "submitDateTime", "docID"],
            kind="stable",
        )

        rows = list(group.to_dict("records"))

        for previous, current in zip(rows, rows[1:]):
            prev_start = previous["periodStart"]
            prev_end = previous["periodEnd"]
            curr_start = current["periodStart"]
            curr_end = current["periodEnd"]

            prev_period_days = int((prev_end - prev_start).days + 1)
            curr_period_days = int((curr_end - curr_start).days + 1)

            period_end_gap_days = int((curr_end - prev_end).days)
            start_after_prev_end_days = int((curr_start - prev_end).days)

            periods_contiguous = start_after_prev_end_days == 1

            prev_standard = (
                min_standard_period_days
                <= prev_period_days
                <= max_standard_period_days
            )
            curr_standard = (
                min_standard_period_days
                <= curr_period_days
                <= max_standard_period_days
            )

            standard_annual_pair = (
                periods_contiguous
                and prev_standard
                and curr_standard
            )

            if standard_annual_pair:
                pair_status = "standard_annual"
            elif periods_contiguous:
                pair_status = "transition_period"
            else:
                pair_status = "noncontiguous"

            prev_form_code = previous.get("formCode")
            curr_form_code = current.get("formCode")
            pair_is_domestic = (
                prev_form_code in DOMESTIC_ASR_FORM_CODES
                and curr_form_code in DOMESTIC_ASR_FORM_CODES
            )
            is_research_eligible = (
                standard_annual_pair and pair_is_domestic
            )

            if not pair_is_domestic:
                exclusion_reason = "foreign_company_form"
            elif not standard_annual_pair:
                exclusion_reason = pair_status
            else:
                exclusion_reason = ""

            pair_rows.append(
                {
                    "edinetCode": edinet_code,
                    "filerName": current.get("filerName"),
                    "secCode": current.get("secCode"),
                    "prev_docID": previous["docID"],
                    "curr_docID": current["docID"],
                    "prev_formCode": prev_form_code,
                    "curr_formCode": curr_form_code,
                    "is_domestic_asr": pair_is_domestic,
                    "is_research_eligible": is_research_eligible,
                    "exclusion_reason": exclusion_reason,
                    "prev_periodStart": prev_start,
                    "prev_periodEnd": prev_end,
                    "curr_periodStart": curr_start,
                    "curr_periodEnd": curr_end,
                    "prev_period_days": prev_period_days,
                    "curr_period_days": curr_period_days,
                    "period_end_gap_days": period_end_gap_days,
                    "start_after_prev_end_days": start_after_prev_end_days,
                    "periods_contiguous": periods_contiguous,
                    "prev_standard_period": prev_standard,
                    "curr_standard_period": curr_standard,
                    "standard_annual_pair": standard_annual_pair,
                    "prev_submitDateTime": previous.get("submitDateTime"),
                    "curr_submitDateTime": current.get("submitDateTime"),
                    "prev_outputPath": previous.get("outputPath"),
                    "curr_outputPath": current.get("outputPath"),
                    "prev_textChars": previous.get("textChars"),
                    "curr_textChars": current.get("textChars"),
                    "prev_method": previous.get("method"),
                    "curr_method": current.get("method"),
                    "pair_status": pair_status,
                }
            )

    pairs = pd.DataFrame(pair_rows)

    if pairs.empty:
        return pd.DataFrame(
            columns=[
                "edinetCode",
                "filerName",
                "secCode",
                "prev_docID",
                "curr_docID",
                "prev_formCode",
                "curr_formCode",
                "is_domestic_asr",
                "is_research_eligible",
                "exclusion_reason",
                "prev_periodStart",
                "prev_periodEnd",
                "curr_periodStart",
                "curr_periodEnd",
                "prev_period_days",
                "curr_period_days",
                "period_end_gap_days",
                "start_after_prev_end_days",
                "periods_contiguous",
                "prev_standard_period",
                "curr_standard_period",
                "standard_annual_pair",
                "prev_submitDateTime",
                "curr_submitDateTime",
                "prev_outputPath",
                "curr_outputPath",
                "prev_textChars",
                "curr_textChars",
                "prev_method",
                "curr_method",
                "pair_status",
            ]
        )

    return pairs.sort_values(
        ["curr_periodEnd", "edinetCode", "curr_docID"],
        kind="stable",
    ).reset_index(drop=True)


def run_longitudinal_match(
    *,
    filings_csv: Path,
    extraction_manifest: Path,
    output_dir: Path,
    min_period_end_gap_days: int = 300,
    max_period_end_gap_days: int = 430,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run Stage 3 longitudinal matching.

    Notes
    -----
    The adapter currently supplies ``min_period_end_gap_days`` and
    ``max_period_end_gap_days``. In Stage 3 these are interpreted as acceptable
    reporting-period *durations* for a standard annual period.

    This preserves the existing TOML names for now while changing the underlying
    matching logic from calendar-year labels to actual reporting periods.
    """
    log = logger or logging.getLogger(__name__)

    filings_csv = Path(filings_csv)
    extraction_manifest = Path(extraction_manifest)
    output_dir = Path(output_dir)

    min_standard_period_days = int(min_period_end_gap_days)
    max_standard_period_days = int(max_period_end_gap_days)

    if min_standard_period_days < 1:
        raise ValueError("min_period_end_gap_days must be >= 1")
    if max_standard_period_days < min_standard_period_days:
        raise ValueError(
            "max_period_end_gap_days must be >= min_period_end_gap_days"
        )

    if not filings_csv.exists():
        raise FileNotFoundError(f"Filings CSV not found: {filings_csv}")
    if not extraction_manifest.exists():
        raise FileNotFoundError(
            f"Extraction manifest not found: {extraction_manifest}"
        )

    log.info("Loading Stage 1 filings: %s", filings_csv)
    filings = pd.read_csv(
        filings_csv,
        dtype=str,
        keep_default_na=True,
        encoding="utf-8-sig",
    )

    log.info("Loading Stage 2 extraction manifest: %s", extraction_manifest)
    extractions = pd.read_csv(
        extraction_manifest,
        dtype=str,
        keep_default_na=True,
        encoding="utf-8-sig",
    )

    panel = _build_panel(filings, extractions)

    duplicate_reporting_periods = _identify_duplicate_reporting_periods(panel)

    pairs = _construct_adjacent_pairs(
        panel,
        min_standard_period_days=min_standard_period_days,
        max_standard_period_days=max_standard_period_days,
    )

    if pairs.empty:
        standard_annual_pairs = pairs.copy()
        transition_period_pairs = pairs.copy()
        noncontiguous_pairs = pairs.copy()
    else:
        standard_annual_pairs = pairs.loc[
            pairs["pair_status"].eq("standard_annual")
        ].copy()

        transition_period_pairs = pairs.loc[
            pairs["pair_status"].eq("transition_period")
        ].copy()

        noncontiguous_pairs = pairs.loc[
            pairs["pair_status"].eq("noncontiguous")
        ].copy()

    research_eligible_pairs = pairs.loc[
        pairs["is_research_eligible"].astype(bool)
    ].copy()

    output_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        panel,
        output_dir / "longitudinal_panel.csv",
    )
    _write_csv(
        duplicate_reporting_periods,
        output_dir / "duplicate_reporting_periods.csv",
    )
    _write_csv(
        pairs,
        output_dir / "adjacent_period_pairs.csv",
    )
    _write_csv(
        standard_annual_pairs,
        output_dir / "standard_annual_pairs.csv",
    )
    _write_csv(
        transition_period_pairs,
        output_dir / "transition_period_pairs.csv",
    )
    _write_csv(
        noncontiguous_pairs,
        output_dir / "noncontiguous_pairs.csv",
    )
    _write_csv(
        research_eligible_pairs,
        output_dir / "research_eligible_pairs.csv",
    )

    summary: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "filings_csv": str(filings_csv),
        "extraction_manifest": str(extraction_manifest),
        "output_dir": str(output_dir),
        "min_standard_period_days": min_standard_period_days,
        "max_standard_period_days": max_standard_period_days,
        "stage1_filing_rows": int(len(filings)),
        "stage2_manifest_rows": int(len(extractions)),
        "successful_stage2_extractions": int(
            extractions["status"]
            .astype(str)
            .str.lower()
            .eq("ok")
            .sum()
        ),
        "matched_panel_rows": int(len(panel)),
        "matched_unique_edinet_codes": int(
            panel["edinetCode"].nunique(dropna=True)
        ),
        "duplicate_reporting_period_rows": int(
            len(duplicate_reporting_periods)
        ),
        "duplicate_reporting_period_groups": int(
            duplicate_reporting_periods[
                ["edinetCode", "periodStart", "periodEnd"]
            ]
            .drop_duplicates()
            .shape[0]
            if not duplicate_reporting_periods.empty
            else 0
        ),
        "adjacent_period_pairs": int(len(pairs)),
        "standard_annual_pairs": int(len(standard_annual_pairs)),
        "transition_period_pairs": int(len(transition_period_pairs)),
        "noncontiguous_pairs": int(len(noncontiguous_pairs)),
        "domestic_matched_panel_rows": int(
            panel["is_domestic_asr"].astype(bool).sum()
        ),
        "foreign_matched_panel_rows": int(
            (~panel["is_domestic_asr"].astype(bool)).sum()
        ),
        "domestic_standard_annual_pairs": int(
            standard_annual_pairs["is_domestic_asr"].astype(bool).sum()
            if not standard_annual_pairs.empty
            else 0
        ),
        "foreign_standard_annual_pairs": int(
            (~standard_annual_pairs["is_domestic_asr"].astype(bool)).sum()
            if not standard_annual_pairs.empty
            else 0
        ),
        "research_eligible_pairs": int(len(research_eligible_pairs)),
    }

    _write_json(summary, output_dir / "summary.json")

    log.info("Stage 3 summary: %s", summary)
    log.info(
        "Stage 3 outputs: panel=%s standard=%s transition=%s "
        "noncontiguous=%s research_eligible=%s",
        output_dir / "longitudinal_panel.csv",
        output_dir / "standard_annual_pairs.csv",
        output_dir / "transition_period_pairs.csv",
        output_dir / "noncontiguous_pairs.csv",
        output_dir / "research_eligible_pairs.csv",
    )

    return summary
