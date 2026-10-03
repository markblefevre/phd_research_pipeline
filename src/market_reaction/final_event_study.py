from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd


def _normalize_sec_code(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip()
    return out.str.replace(r"\.0$", "", regex=True)


def _offset_label(offset: int) -> str:
    return f"m{abs(offset)}" if offset < 0 else str(offset)


def _window_label(start: int, end: int) -> str:
    return f"{_offset_label(start)}_{_offset_label(end)}"


def _validate_windows(windows: Sequence[Sequence[int]]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for raw in windows:
        if len(raw) != 2:
            raise ValueError(f"Invalid CAR window {raw!r}; expected [start, end]")
        start, end = int(raw[0]), int(raw[1])
        if start > end:
            raise ValueError(f"Invalid CAR window [{start}, {end}]; start > end")
        out.append((start, end))
    if not out:
        raise ValueError("At least one CAR window is required")
    return out


def build_final_event_study_table(
    analysis_panel: pd.DataFrame,
    eligibility: pd.DataFrame,
    abnormal_returns: pd.DataFrame,
    *,
    windows: Sequence[Sequence[int]],
) -> pd.DataFrame:
    """
    Build the canonical Stage 7F market-reaction table.

    The Stage 6A analysis panel is authoritative for the full research-pair
    universe. Stage 7A eligibility and Stage 7E market-reaction results are
    left-joined so no research pair is silently lost.

    Final sample flags are window-specific:
        eventStudySample_<window> = Stage 7A eligible AND valid CAR(window)

    This stage does not impose a single common CAR sample across windows.
    """
    windows_t = _validate_windows(windows)
    key = ["edinetCode", "secCode", "curr_docID"]

    for name, df in [
        ("analysis_panel", analysis_panel),
        ("eligibility", eligibility),
        ("abnormal_returns", abnormal_returns),
    ]:
        missing = set(key) - set(df.columns)
        if missing:
            raise ValueError(
                f"{name} missing required event key column(s): {sorted(missing)}"
            )

    base = analysis_panel.copy()
    elig = eligibility.copy()
    ar = abnormal_returns.copy()

    for df in (base, elig, ar):
        df["secCode"] = _normalize_sec_code(df["secCode"])

    for name, df in [
        ("analysis_panel", base),
        ("eligibility", elig),
        ("abnormal_returns", ar),
    ]:
        dupes = int(df.duplicated(key).sum())
        if dupes:
            raise ValueError(
                f"{name} contains {dupes:,} duplicate Stage 7 event key(s)"
            )

    # Stage 7A audit fields only. Coverage diagnostics that already exist in the
    # eligibility master are retained when present, but overlapping Stage 6A
    # columns are not duplicated.
    preferred_elig_cols = [
        "eventEligible",
        "finalEligibilityStatus",
        "finalExclusionReason",
        "exclusionSubClass",
        "regionalExchange",
        "coverageStatus",
        "firstPriceDate",
        "lastPriceDate",
        "verifiedMarketEntryDate",
        "verifiedTSEDelistingDate",
        "verificationStatus",
        "verificationSourceName",
        "verificationSourceURL",
        "auditNotes",
    ]
    elig_cols = key + [
        c for c in preferred_elig_cols
        if c in elig.columns and c not in base.columns
    ]

    out = base.merge(
        elig[elig_cols],
        on=key,
        how="left",
        validate="one_to_one",
        indicator="_elig_merge",
    )

    if (out["_elig_merge"] != "both").any():
        n = int((out["_elig_merge"] != "both").sum())
        raise ValueError(
            f"{n:,} Stage 6A research pair(s) have no matching Stage 7A eligibility row"
        )
    out = out.drop(columns="_elig_merge")

    # Stage 7E contains only Stage 7B-eligible events. Keep its complete
    # market-reaction payload so the canonical final table remains auditable.
    ar_payload = [
        c for c in ar.columns
        if c not in key and c not in out.columns
    ]

    out = out.merge(
        ar[key + ar_payload],
        on=key,
        how="left",
        validate="one_to_one",
        indicator="_ar_merge",
    )

    # An eligible Stage 7A event should have progressed into the Stage 7E input
    # universe. Excluded Stage 7A rows are expected not to match.
    eligible = out["eventEligible"].fillna(False).astype(bool)
    missing_market_reaction = eligible & out["_ar_merge"].ne("both")
    if missing_market_reaction.any():
        n = int(missing_market_reaction.sum())
        raise ValueError(
            f"{n:,} Stage 7A-eligible event(s) have no Stage 7E row"
        )

    out["marketReactionConstructed"] = out["_ar_merge"].eq("both")
    out = out.drop(columns="_ar_merge")

    # Window-specific final event-study sample flags.
    for start, end in windows_t:
        label = _window_label(start, end)
        valid_col = f"carValid_{label}"
        status_col = f"carStatus_{label}"
        sample_col = f"eventStudySample_{label}"

        if valid_col not in out.columns:
            raise ValueError(
                f"Stage 7E output missing configured window column: {valid_col}"
            )
        if status_col not in out.columns:
            raise ValueError(
                f"Stage 7E output missing configured window column: {status_col}"
            )

        car_valid = out[valid_col].astype("boolean").fillna(False).astype(bool)
        out[sample_col] = eligible & car_valid

        # Give Stage 7A exclusions an explicit final status instead of NaN.
        excluded_mask = ~eligible
        out.loc[excluded_mask, status_col] = "stage7a_ineligible"

    return out.reset_index(drop=True)


def build_final_event_study_summary(
    final_table: pd.DataFrame,
    *,
    windows: Sequence[Sequence[int]],
) -> dict:
    """
    Build the Stage 7F attrition/QC summary.
    """
    windows_t = _validate_windows(windows)

    n_total = int(len(final_table))
    eligible = final_table["eventEligible"].fillna(False).astype(bool)
    n_eligible = int(eligible.sum())

    exclusion_counts = {}
    if "finalExclusionReason" in final_table.columns:
        exclusion_counts = {
            str(k): int(v)
            for k, v in final_table.loc[
                ~eligible, "finalExclusionReason"
            ].value_counts(dropna=False).to_dict().items()
        }

    mm_counts = {}
    if "marketModelStatus" in final_table.columns:
        mm_counts = {
            str(k): int(v)
            for k, v in final_table.loc[
                eligible, "marketModelStatus"
            ].value_counts(dropna=False).to_dict().items()
        }

    estimated = (
        eligible
        & final_table.get(
            "marketModelStatus",
            pd.Series(index=final_table.index, dtype="string"),
        ).eq("estimated")
    )
    n_estimated = int(estimated.sum())

    summary: dict = {
        "stage": "7F_final_market_reaction_table_and_QC",
        "researchPairs": n_total,
        "stage7AEligible": n_eligible,
        "stage7AExcluded": int(n_total - n_eligible),
        "stage7AEligibilityRate": (
            float(n_eligible / n_total) if n_total else np.nan
        ),
        "stage7AExclusionReasonCounts": exclusion_counts,
        "stage7DMarketModelsEstimated": n_estimated,
        "stage7DMarketModelFailureCount": int(n_eligible - n_estimated),
        "stage7DMarketModelStatusCounts": mm_counts,
        "windows": {},
    }

    for start, end in windows_t:
        label = _window_label(start, end)
        sample_col = f"eventStudySample_{label}"
        status_col = f"carStatus_{label}"
        car_col = f"car_{label}"

        sample = final_table[sample_col].fillna(False).astype(bool)
        n_final = int(sample.sum())

        status_counts = {
            str(k): int(v)
            for k, v in final_table[status_col]
            .value_counts(dropna=False)
            .to_dict()
            .items()
        }

        cars = pd.to_numeric(
            final_table.loc[sample, car_col], errors="coerce"
        ).dropna()

        item = {
            "start": int(start),
            "end": int(end),
            "finalSample": n_final,
            "droppedFromResearchPairs": int(n_total - n_final),
            "retentionVsResearchPairs": (
                float(n_final / n_total) if n_total else np.nan
            ),
            "retentionVsStage7AEligible": (
                float(n_final / n_eligible) if n_eligible else np.nan
            ),
            "retentionVsStage7DEstimated": (
                float(n_final / n_estimated) if n_estimated else np.nan
            ),
            "statusCounts": status_counts,
        }

        if not cars.empty:
            desc = cars.describe(
                percentiles=[0.01, 0.05, 0.50, 0.95, 0.99]
            ).to_dict()
            item["carDistribution"] = {
                str(k): float(v) for k, v in desc.items()
            }

        summary["windows"][label] = item

    return summary
