#!/usr/bin/env python
"""
Build the canonical Stage 7A event-eligibility table.

Inputs
------
data/interim/paper2/market_reaction/diagnostics/price_coverage_diagnostic.csv
data/interim/paper2/market_reaction/diagnostics/insufficient_history_classification.csv
data/interim/paper2/market_reaction/diagnostics/missing_security_exchange_events.csv

For transition convenience, the script also accepts these files directly under
data/interim/paper2/market_reaction/.
data/raw/paper2/prices/equities_bars_daily_*.csv*

Outputs
-------
data/interim/paper2/market_reaction/stage7_event_eligibility.csv
data/interim/paper2/market_reaction/stage7_exclusion_audit.csv
data/interim/paper2/market_reaction/stage7_eligibility_summary.json

Design
------
The 33,046-row event-eligibility table is canonical.  The exclusion-audit
CSV is a filtered convenience view.  Earlier diagnostic CSVs remain
provenance/QC artifacts but are not the Stage 7A handoff.
"""

from __future__ import annotations

import json
from pathlib import Path
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------
# Curated external verification used only for auditability.
# Classification itself is driven by the J-Quants price-history evidence.
# ---------------------------------------------------------------------

# TSE-entry dates for the 15 events with <60 post-entry estimation observations.
# The verified entry date equals the first J-Quants price date in every case.
MARKET_ENTRY_VERIFICATION = {
    "65350": dict(date="2016-10-27", entryType="IPO",
                  sourceName="JPX new listing record",
                  sourceURL="https://www.jpx.co.jp/news/1031/20161026-01.html"),
    "92740": dict(date="2018-06-26", entryType="IPO",
                  sourceName="JPX new listing record",
                  sourceURL="https://www.jpx.co.jp/english/news/1031/20180625-01.html"),
    "39870": dict(date="2018-06-22", entryType="regional_to_TSE",
                  sourceName="JPX monthly equity statistics",
                  sourceURL="https://www.jpx.co.jp/markets/statistics-equities/monthly/nlsgeu00000386y6-att/1806.pdf"),
    "81040": dict(date="2018-03-20", entryType="regional_to_TSE",
                  sourceName="Historical listing record",
                  sourceURL="https://ca.image.jp/matsui/?seldate=0&serviceDatefrom=&serviceDateto=&sort=10&type=6&word1=&word2="),
    "14310": dict(date="2019-06-18", entryType="regional_to_TSE",
                  sourceName="Issuer IR listing history",
                  sourceURL="https://www.libwork.co.jp/ir-questions/"),
    "79590": dict(date="2019-10-07", entryType="regional_to_TSE",
                  sourceName="JPX issuer disclosure",
                  sourceURL="https://www2.jpx.co.jp/disc/79590/140120191007405208.pdf"),
    "81080": dict(date="2020-03-23", entryType="regional_to_TSE",
                  sourceName="JPX issuer disclosure",
                  sourceURL="https://www2.jpx.co.jp/disc/81080/140120200229472248.pdf"),
    "38020": dict(date="2020-04-28", entryType="regional_to_TSE",
                  sourceName="JPX issuer disclosure",
                  sourceURL="https://www2.jpx.co.jp/disc/38020/140120200428400879.pdf"),
    "34220": dict(date="2021-03-12", entryType="regional_to_TSE",
                  sourceName="Historical listing record",
                  sourceURL="https://ca.image.jp/matsui/?seldate=0&serviceDatefrom=&serviceDateto=&sort=10&type=6&word1=&word2="),
    "42470": dict(date="2022-03-10", entryType="regional_to_TSE",
                  sourceName="Historical listing record",
                  sourceURL="https://ca.image.jp/matsui/?seldate=0&serviceDatefrom=&serviceDateto=&sort=10&type=6&word1=&word2="),
    "14380": dict(date="2022-09-28", entryType="regional_to_TSE",
                  sourceName="Historical listing record",
                  sourceURL="https://ca.image.jp/matsui/?page=13&seldate=0&serviceDatefrom=&serviceDateto=&sort=1&type=6&word1=&word2="),
    "44470": dict(date="2022-10-06", entryType="regional_to_TSE",
                  sourceName="Historical listing record",
                  sourceURL="https://ca.image.jp/matsui/?page=8&seldate=0&serviceDatefrom=&serviceDateto=&sort=6&type=6&word1=&word2="),
    "89960": dict(date="2022-12-23", entryType="regional_to_TSE",
                  sourceName="Historical listing record",
                  sourceURL="https://ca.image.jp/matsui/?seldate=0&serviceDatefrom=&serviceDateto=&sort=10&type=6&word1=&word2="),
    "67970": dict(date="2024-03-13", entryType="regional_to_TSE",
                  sourceName="JPX new-listing archive",
                  sourceURL="https://www.jpx.co.jp/listing/stocks/new/00-archives-02.html"),
    "53560": dict(date="2024-03-18", entryType="regional_to_TSE",
                  sourceName="Issuer/TDnet listing disclosure mirror",
                  sourceURL="https://irbank.net/5356/140120240315554833"),
}

# Official TSE exits underlying the 26 "no price on or after event" observations.
TSE_EXIT_VERIFICATION = {
    "42170": dict(date="2020-06-19", exitClass="corporate_action_exit", regionalExchange="",
                  sourceName="JPX decision on delisting: Hitachi Chemical",
                  sourceURL="https://www.jpx.co.jp/english/news/1021/20200605-11.html"),
    "56900": dict(date="2021-09-29", exitClass="corporate_reorganization", regionalExchange="",
                  sourceName="JPX delisted companies archive 2021",
                  sourceURL="https://www.jpx.co.jp/english/listing/stocks/delisted/archives-05.html"),
    "40180": dict(date="2021-09-12", exitClass="transfer_to_regional_exchange", regionalExchange="FUKUOKA",
                  sourceName="JPX decision on delisting: Geolocation Technology",
                  sourceURL="https://www.jpx.co.jp/english/news/1021/20210811_01.html"),
    "42500": dict(date="2021-10-31", exitClass="transfer_to_regional_exchange", regionalExchange="FUKUOKA",
                  sourceName="JPX decision on delisting: Frontier",
                  sourceURL="https://www.jpx.co.jp/english/news/1021/20210928_01.html"),
    "98100": dict(date="2023-06-21", exitClass="corporate_action_exit", regionalExchange="",
                  sourceName="JPX decision on delisting: Nippon Steel Trading",
                  sourceURL="https://www.jpx.co.jp/english/news/1023/20230602-11.html"),
    "50750": dict(date="2022-12-25", exitClass="transfer_to_regional_exchange", regionalExchange="NAGOYA",
                  sourceName="JPX decision on delisting: UPCON",
                  sourceURL="https://www.jpx.co.jp/english/news/1021/20221124_19.html"),
    "97830": dict(date="2024-05-17", exitClass="corporate_action_exit", regionalExchange="",
                  sourceName="JPX decision on delisting: Benesse Holdings",
                  sourceURL="https://www.jpx.co.jp/english/news/1023/20240429-11.html"),
    "93880": dict(date="2025-03-20", exitClass="transfer_to_regional_exchange", regionalExchange="FUKUOKA",
                  sourceName="JPX decision on delisting: PAPANETS",
                  sourceURL="https://www.jpx.co.jp/english/news/1021/20250217_18.html"),
    "44350": dict(date="2025-06-11", exitClass="corporate_action_exit", regionalExchange="",
                  sourceName="JPX decision on delisting: kaonavi",
                  sourceURL="https://www.jpx.co.jp/english/news/1023/20250522-11.html"),
    "43040": dict(date="2025-06-20", exitClass="corporate_action_exit", regionalExchange="",
                  sourceName="JPX decision on delisting: Estore",
                  sourceURL="https://www.jpx.co.jp/english/news/1023/20250530-13.html"),
    "90700": dict(date="2025-06-19", exitClass="corporate_action_exit", regionalExchange="",
                  sourceName="JPX decision on delisting: Tonami Holdings",
                  sourceURL="https://www.jpx.co.jp/english/news/1023/20250530-12.html"),
    "52410": dict(date="2024-12-22", exitClass="transfer_to_regional_exchange", regionalExchange="NAGOYA",
                  sourceName="JPX decision on delisting: Nihon Office Automation Research",
                  sourceURL="https://www.jpx.co.jp/english/news/1021/20241120_18.html"),
    "77900": dict(date="2025-02-02", exitClass="transfer_to_regional_exchange", regionalExchange="NAGOYA",
                  sourceName="JPX decision on delisting: BARCOS",
                  sourceURL="https://www.jpx.co.jp/english/news/1021/20241227_19.html"),
    "53860": dict(date="2026-03-17", exitClass="transfer_to_regional_exchange", regionalExchange="NAGOYA",
                  sourceName="JPX decision on delisting: TSURUYA",
                  sourceURL="https://www.jpx.co.jp/english/news/1021/20260216-11.html"),
    "65650": dict(date="2026-05-01", exitClass="transfer_to_regional_exchange", regionalExchange="NAGOYA",
                  sourceName="JPX decision on delisting: AB Hotel",
                  sourceURL="https://www.jpx.co.jp/news/1021/20260330-11.html"),
    "71180": dict(date="2024-10-20", exitClass="transfer_to_regional_exchange", regionalExchange="SAPPORO",
                  sourceName="JPX decision on delisting: Shinwa-holdings",
                  sourceURL="https://www.jpx.co.jp/english/news/1021/20240912_18.html"),
    "25400": dict(date="2026-06-18", exitClass="corporate_action_exit", regionalExchange="",
                  sourceName="JPX decision on delisting: Yomeishu Seizo",
                  sourceURL="https://www.jpx.co.jp/english/news/1023/20260601-11.html"),
}

# One historical regional-exchange venue that the EDINET XBRL extractor
# did not resolve in the relevant filings.
REGIONAL_EXCHANGE_MANUAL_OVERRIDE = {
    "81710": "NAGOYA",
}


def norm_code(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True).str.zfill(5)


def read_price_boundaries(price_dir: Path, target_codes: set[str]) -> pd.DataFrame:
    """Return first/last J-Quants price dates for target security codes."""
    parts = []

    files = sorted(price_dir.glob("equities_bars_daily_*.csv*"))
    if not files:
        raise FileNotFoundError(f"No J-Quants price files found under {price_dir}")

    for path in files:
        header = pd.read_csv(path, nrows=0, compression="infer")
        lower = {c.lower(): c for c in header.columns}
        code_col = lower.get("code") or lower.get("symbol")
        date_col = lower.get("date") or lower.get("trading_date")
        if not code_col or not date_col:
            continue

        df = pd.read_csv(
            path,
            usecols=[code_col, date_col],
            dtype={code_col: "string"},
            compression="infer",
            low_memory=False,
        )
        df[code_col] = norm_code(df[code_col])
        df = df[df[code_col].isin(target_codes)]
        if df.empty:
            continue

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])
        parts.append(
            df.rename(columns={code_col: "secCode", date_col: "Date"})[
                ["secCode", "Date"]
            ]
        )

    if not parts:
        return pd.DataFrame(columns=["secCode", "firstPriceDate", "lastPriceDate"])

    prices = pd.concat(parts, ignore_index=True).drop_duplicates(["secCode", "Date"])
    return (
        prices.groupby("secCode")["Date"]
        .agg(firstPriceDate="min", lastPriceDate="max")
        .reset_index()
    )


def resolve_regional_exchange(missing: pd.DataFrame) -> dict[str, str]:
    """
    Resolve a security-level regional exchange using all successful event-level
    EDINET XBRL classifications. UNKNOWN observations inherit the known venue
    for the same security.
    """
    m = missing.copy()
    m["secCode"] = norm_code(m["secCode"])

    valid_regions = {"NAGOYA", "FUKUOKA", "SAPPORO"}
    resolved: dict[str, str] = {}

    for code, grp in m.groupby("secCode"):
        vals = {
            str(v).strip().upper()
            for v in grp["exchangeClassification"].dropna()
            if str(v).strip().upper() in valid_regions
        }

        if code in REGIONAL_EXCHANGE_MANUAL_OVERRIDE:
            vals.add(REGIONAL_EXCHANGE_MANUAL_OVERRIDE[code])

        if len(vals) != 1:
            raise ValueError(
                f"Could not uniquely resolve regional exchange for {code}: {sorted(vals)}"
            )
        resolved[code] = next(iter(vals))

    return resolved


def build_stage7_event_eligibility(*, root: Path, paper: str = "paper2") -> dict:
    mr = root / f"data/interim/{paper}/market_reaction"
    price_dir = root / f"data/raw/{paper}/prices"

    diagnostics = mr / "diagnostics"

    def find_input(name: str) -> Path:
        """Prefer the final diagnostics/ layout, but accept today's legacy location."""
        candidates = [diagnostics / name, mr / name]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            f"Could not find {name}; checked: "
            + ", ".join(str(x) for x in candidates)
        )

    coverage_path = find_input("price_coverage_diagnostic.csv")
    insufficient_path = find_input("insufficient_history_classification.csv")
    missing_exchange_path = find_input("missing_security_exchange_events.csv")

    coverage = pd.read_csv(coverage_path, dtype={"secCode": "string"}, low_memory=False)
    insuff = pd.read_csv(insufficient_path, dtype={"secCode": "string"}, low_memory=False)
    missing = pd.read_csv(missing_exchange_path, dtype={"secCode": "string"}, low_memory=False)

    coverage["secCode"] = norm_code(coverage["secCode"])
    insuff["secCode"] = norm_code(insuff["secCode"])
    missing["secCode"] = norm_code(missing["secCode"])

    key = ["edinetCode", "secCode", "curr_docID"]

    if coverage.duplicated(key).any():
        raise ValueError("Duplicate event keys in price_coverage_diagnostic.csv")
    if insuff.duplicated(key).any():
        raise ValueError("Duplicate event keys in insufficient_history_classification.csv")
    if missing.duplicated(key).any():
        raise ValueError("Duplicate event keys in missing_security_exchange_events.csv")

    # Canonical master begins with every Stage 7 event.
    out = coverage.copy()
    out["eventEligible"] = False
    out["finalEligibilityStatus"] = pd.NA
    out["finalExclusionReason"] = pd.NA
    out["exclusionSubClass"] = pd.NA
    out["regionalExchange"] = pd.NA
    out["firstPriceDate"] = pd.NaT
    out["lastPriceDate"] = pd.NaT
    out["verifiedMarketEntryDate"] = pd.NaT
    out["verifiedTSEDelistingDate"] = pd.NaT
    out["verificationStatus"] = pd.NA
    out["verificationSourceName"] = pd.NA
    out["verificationSourceURL"] = pd.NA
    out["auditNotes"] = pd.NA

    # --------------------------------------------------------------
    # Eligible events
    # --------------------------------------------------------------
    eligible_statuses = {"full", "partial_but_usable"}
    mask = out["coverageStatus"].isin(eligible_statuses)
    out.loc[mask, "eventEligible"] = True
    out.loc[mask, "finalEligibilityStatus"] = "eligible_market_reaction"

    # --------------------------------------------------------------
    # 814 securities absent from J-Quants/TSE price universe
    # --------------------------------------------------------------
    regional_map = resolve_regional_exchange(missing)
    mask = out["coverageStatus"].eq("missing_security")
    out.loc[mask, "finalEligibilityStatus"] = "excluded"
    out.loc[mask, "finalExclusionReason"] = "non_TSE_regional_exchange"
    out.loc[mask, "exclusionSubClass"] = "regional_exchange_only"
    out.loc[mask, "regionalExchange"] = out.loc[mask, "secCode"].map(regional_map)
    out.loc[mask, "verificationStatus"] = "EDINET_XBRL_security_level_resolved"
    out.loc[mask, "auditNotes"] = (
        "Security absent from J-Quants TSE stock-price universe; "
        "historical EDINET filings identify a regional-exchange listing."
    )

    if out.loc[mask, "regionalExchange"].isna().any():
        bad = out.loc[mask & out["regionalExchange"].isna(), key]
        raise ValueError(f"Unresolved regional-exchange rows:\n{bad}")

    # --------------------------------------------------------------
    # 72 initially labelled insufficient estimation history
    # --------------------------------------------------------------
    insuff_cols = key + [
        "firstPriceDate",
        "lastPriceDate",
        "firstPriceOnOrAfterEvent",
        "recalculatedEstimationObs",
        "stage7Classification",
    ]
    ins = insuff[insuff_cols].copy()
    ins = ins.rename(
        columns={
            "firstPriceDate": "ins_firstPriceDate",
            "lastPriceDate": "ins_lastPriceDate",
            "recalculatedEstimationObs": "ins_estObs",
            "stage7Classification": "ins_class",
        }
    )
    out = out.merge(ins, on=key, how="left", validate="one_to_one")

    mask72 = out["coverageStatus"].eq("insufficient_estimation_history")
    if out.loc[mask72, "ins_class"].isna().any():
        raise ValueError("Some insufficient-history events lack classification.")

    # 57: filing precedes the first TSE/J-Quants price by >30 days.
    m57 = mask72 & out["ins_class"].eq("price_history_begins_long_after_event")
    out.loc[m57, "finalEligibilityStatus"] = "excluded"
    out.loc[m57, "finalExclusionReason"] = "not_in_TSE_price_universe_at_event"
    out.loc[m57, "exclusionSubClass"] = "TSE_price_history_begins_after_event"
    out.loc[m57, "firstPriceDate"] = pd.to_datetime(out.loc[m57, "ins_firstPriceDate"])
    out.loc[m57, "lastPriceDate"] = pd.to_datetime(out.loc[m57, "ins_lastPriceDate"])
    out.loc[m57, "verificationStatus"] = "JQuants_price_boundary_evidence"
    out.loc[m57, "auditNotes"] = (
        "Filing predates the security's first J-Quants/TSE price by >30 days; "
        "older subscription history cannot create pre-entry observations."
    )

    # 15: filing follows TSE entry but there are <60 usable observations
    # in the frozen [-120,-20] market-model window.
    m15 = mask72 & out["ins_class"].eq("security_history_itself_too_short")
    out.loc[m15, "finalEligibilityStatus"] = "excluded"
    out.loc[m15, "finalExclusionReason"] = "insufficient_post_listing_estimation_history"
    out.loc[m15, "exclusionSubClass"] = "fewer_than_60_observations_in_m120_m20"
    out.loc[m15, "firstPriceDate"] = pd.to_datetime(out.loc[m15, "ins_firstPriceDate"])
    out.loc[m15, "lastPriceDate"] = pd.to_datetime(out.loc[m15, "ins_lastPriceDate"])

    for idx in out.index[m15]:
        code = out.at[idx, "secCode"]
        v = MARKET_ENTRY_VERIFICATION.get(code)
        if v is None:
            raise ValueError(f"Missing market-entry verification for {code}")
        out.at[idx, "verifiedMarketEntryDate"] = pd.Timestamp(v["date"])
        out.at[idx, "verificationStatus"] = "verified_TSE_entry_equals_first_JQuants_price"
        out.at[idx, "verificationSourceName"] = v["sourceName"]
        out.at[idx, "verificationSourceURL"] = v["sourceURL"]
        out.at[idx, "auditNotes"] = (
            f"{v['entryType']}; first J-Quants price equals verified TSE entry date; "
            f"only {int(out.at[idx, 'ins_estObs'])} usable estimation observations."
        )

        if pd.Timestamp(out.at[idx, "ins_firstPriceDate"]) != pd.Timestamp(v["date"]):
            raise ValueError(
                f"{code}: first price {out.at[idx, 'ins_firstPriceDate']} "
                f"!= verified TSE entry {v['date']}"
            )

    unknown_insuff = mask72 & ~out["ins_class"].isin(
        {"price_history_begins_long_after_event", "security_history_itself_too_short"}
    )
    if unknown_insuff.any():
        raise ValueError(
            "Unexpected insufficient-history classifications:\n"
            + str(out.loc[unknown_insuff, key + ["ins_class"]])
        )

    # --------------------------------------------------------------
    # 26 no price on/after event: verified TSE exits before filing.
    # Re-read raw prices to preserve first/last price boundaries in the
    # canonical audit output.
    # --------------------------------------------------------------
    m26 = out["coverageStatus"].eq("no_price_on_or_after_event")
    target26 = set(out.loc[m26, "secCode"])
    boundaries = read_price_boundaries(price_dir, target26)
    out = out.merge(
        boundaries.rename(
            columns={
                "firstPriceDate": "raw_firstPriceDate",
                "lastPriceDate": "raw_lastPriceDate",
            }
        ),
        on="secCode",
        how="left",
        validate="many_to_one",
    )

    for idx in out.index[m26]:
        code = out.at[idx, "secCode"]
        v = TSE_EXIT_VERIFICATION.get(code)
        if v is None:
            raise ValueError(f"Missing TSE-exit verification for {code}")

        out.at[idx, "finalEligibilityStatus"] = "excluded"
        out.at[idx, "finalExclusionReason"] = "TSE_delisted_before_event"
        out.at[idx, "exclusionSubClass"] = v["exitClass"]
        out.at[idx, "regionalExchange"] = v["regionalExchange"] or pd.NA
        out.at[idx, "firstPriceDate"] = out.at[idx, "raw_firstPriceDate"]
        out.at[idx, "lastPriceDate"] = out.at[idx, "raw_lastPriceDate"]
        out.at[idx, "verifiedTSEDelistingDate"] = pd.Timestamp(v["date"])
        out.at[idx, "verificationStatus"] = "verified_TSE_exit_before_event"
        out.at[idx, "verificationSourceName"] = v["sourceName"]
        out.at[idx, "verificationSourceURL"] = v["sourceURL"]
        out.at[idx, "auditNotes"] = (
            "Security's J-Quants/TSE price history ends before the filing; "
            f"official TSE exit verified ({v['exitClass']})."
        )

        last_px = pd.Timestamp(out.at[idx, "raw_lastPriceDate"])
        submit = pd.Timestamp(out.at[idx, "submitDateTime"]).normalize()
        delist = pd.Timestamp(v["date"])

        if pd.isna(last_px):
            raise ValueError(f"{code}: no last price found")
        if not (last_px < submit):
            raise ValueError(f"{code}: last price is not before filing")
        if not (delist <= submit):
            raise ValueError(f"{code}: verified delisting is not before/on filing")

    # --------------------------------------------------------------
    # Clean helper columns and validate the complete 33,046-event universe.
    # --------------------------------------------------------------
    helper_cols = [
        "ins_firstPriceDate",
        "ins_lastPriceDate",
        "firstPriceOnOrAfterEvent",
        "ins_estObs",
        "ins_class",
        "raw_firstPriceDate",
        "raw_lastPriceDate",
    ]
    out = out.drop(columns=[c for c in helper_cols if c in out.columns])

    unresolved = out["finalEligibilityStatus"].isna()
    if unresolved.any():
        raise ValueError(
            "Unresolved Stage 7 events:\n"
            + str(out.loc[unresolved, key + ["coverageStatus"]].head(50))
        )

    # Strong reproducibility assertions based on the validated exploratory run.
    expected_total = 33_046
    expected_eligible = 32_134
    expected_excluded = 912
    expected_reasons = {
        "non_TSE_regional_exchange": 814,
        "not_in_TSE_price_universe_at_event": 57,
        "insufficient_post_listing_estimation_history": 15,
        "TSE_delisted_before_event": 26,
    }

    if len(out) != expected_total:
        raise AssertionError(f"Expected {expected_total:,} events, found {len(out):,}")

    eligible_n = int(out["eventEligible"].sum())
    excluded_n = int((~out["eventEligible"]).sum())

    if eligible_n != expected_eligible:
        raise AssertionError(f"Expected {expected_eligible:,} eligible, found {eligible_n:,}")
    if excluded_n != expected_excluded:
        raise AssertionError(f"Expected {expected_excluded:,} excluded, found {excluded_n:,}")

    reason_counts = (
        out.loc[~out["eventEligible"], "finalExclusionReason"]
        .value_counts()
        .to_dict()
    )
    if reason_counts != expected_reasons:
        raise AssertionError(
            f"Unexpected exclusion counts.\nExpected: {expected_reasons}\nFound: {reason_counts}"
        )

    # Canonical column ordering: identifiers/status first, then inherited diagnostics.
    front = [
        "edinetCode",
        "secCode",
        "curr_docID",
        "submitDateTime",
        "eventCalendarDate",
        "eventTradingDate",
        "eventEligible",
        "finalEligibilityStatus",
        "finalExclusionReason",
        "exclusionSubClass",
        "coverageStatus",
        "estimationObsAvailable",
        "full120DayLookback",
        "meets60ObsMinimum",
        "hasPriceCode",
        "regionalExchange",
        "firstPriceDate",
        "lastPriceDate",
        "verifiedMarketEntryDate",
        "verifiedTSEDelistingDate",
        "verificationStatus",
        "verificationSourceName",
        "verificationSourceURL",
        "auditNotes",
    ]
    front = [c for c in front if c in out.columns]
    rest = [c for c in out.columns if c not in front]
    out = out[front + rest]

    master_path = mr / "stage7_event_eligibility.csv"
    exclusion_path = mr / "stage7_exclusion_audit.csv"
    summary_path = mr / "stage7_eligibility_summary.json"

    out.to_csv(master_path, index=False)
    exclusions = out.loc[~out["eventEligible"]].copy()
    exclusions.to_csv(exclusion_path, index=False)

    summary = {
        "inputEvents": int(len(out)),
        "eligibleEvents": eligible_n,
        "excludedEvents": excluded_n,
        "eligiblePercent": round(100.0 * eligible_n / len(out), 4),
        "coverageStatusCounts": {
            str(k): int(v)
            for k, v in out["coverageStatus"].value_counts(dropna=False).items()
        },
        "finalExclusionReasonCounts": {
            str(k): int(v)
            for k, v in exclusions["finalExclusionReason"].value_counts().items()
        },
        "exclusionSubClassCounts": {
            str(k): int(v)
            for k, v in exclusions["exclusionSubClass"].value_counts().items()
        },
        "marketModelEstimationWindow": [-120, -20],
        "minimumEstimationObservations": 60,
    }
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("=" * 72)
    print("STAGE 7A — EVENT ELIGIBILITY")
    print("=" * 72)
    print(f"Input events:    {len(out):,}")
    print(f"Eligible:        {eligible_n:,} ({100 * eligible_n / len(out):.2f}%)")
    print(f"Excluded:          {excluded_n:,}")
    print()
    print("Final exclusion reasons:")
    print(exclusions["finalExclusionReason"].value_counts().to_string())
    print()
    print("Exclusion subclasses:")
    print(exclusions["exclusionSubClass"].value_counts().to_string())
    print()
    print(f"Wrote: {master_path}")
    print(f"Wrote: {exclusion_path}")
    print(f"Wrote: {summary_path}")

    return {
        "master_path": str(master_path),
        "exclusion_path": str(exclusion_path),
        "summary_path": str(summary_path),
        "input_events": int(len(out)),
        "eligible_events": eligible_n,
        "excluded_events": excluded_n,
        "exclusion_reason_counts": reason_counts,
    }


