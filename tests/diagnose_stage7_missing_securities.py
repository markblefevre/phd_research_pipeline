from pathlib import Path
import pandas as pd

ROOT = Path(r"Z:\Documents\Education\2021 EDHEC Exec PhD\5 Pipeline")

COVERAGE_CSV = (
    ROOT
    / "data/interim/paper2/market_reaction/price_coverage_diagnostic.csv"
)

PAIRS_CSV = (
    ROOT
    / "data/interim/paper2/longitudinal/research_eligible_pairs.csv"
)

OUT_DIR = ROOT / "data/interim/paper2/market_reaction"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------
# Load coverage diagnostic
# ------------------------------------------------------------

coverage = pd.read_csv(
    COVERAGE_CSV,
    dtype={
        "secCode": "string",
        "edinetCode": "string",
        "curr_docID": "string",
    },
    low_memory=False,
)

pairs = pd.read_csv(
    PAIRS_CSV,
    dtype={
        "secCode": "string",
        "edinetCode": "string",
        "curr_docID": "string",
    },
    low_memory=False,
)


# ------------------------------------------------------------
# Bring in filerName and useful filing metadata
# ------------------------------------------------------------

keep_cols = [
    "edinetCode",
    "secCode",
    "curr_docID",
    "filerName",
    "curr_submitDateTime",
    "curr_periodEnd",
]

keep_cols = [c for c in keep_cols if c in pairs.columns]

meta = pairs[keep_cols].drop_duplicates(
    subset=["edinetCode", "secCode", "curr_docID"]
)

df = coverage.merge(
    meta,
    on=["edinetCode", "secCode", "curr_docID"],
    how="left",
    suffixes=("", "_pair"),
)


# ------------------------------------------------------------
# Missing securities
# ------------------------------------------------------------

missing = df[df["coverageStatus"] == "missing_security"].copy()

if not missing.empty:
    missing["submitDateTime"] = pd.to_datetime(
        missing["submitDateTime"],
        errors="coerce",
    )

    by_code = (
        missing
        .groupby(["secCode", "edinetCode", "filerName"], dropna=False)
        .agg(
            eventCount=("curr_docID", "count"),
            firstEventDate=("submitDateTime", "min"),
            lastEventDate=("submitDateTime", "max"),
        )
        .reset_index()
        .sort_values(
            ["eventCount", "secCode"],
            ascending=[False, True],
        )
    )

    by_code.to_csv(
        OUT_DIR / "missing_security_by_code.csv",
        index=False,
        encoding="utf-8-sig",
    )

    missing.to_csv(
        OUT_DIR / "missing_security_events.csv",
        index=False,
        encoding="utf-8-sig",
    )

else:
    by_code = pd.DataFrame()


# ------------------------------------------------------------
# No price on or after event
# ------------------------------------------------------------

no_after = df[
    df["coverageStatus"] == "no_price_on_or_after_event"
].copy()

no_after.to_csv(
    OUT_DIR / "no_price_after_event.csv",
    index=False,
    encoding="utf-8-sig",
)


# ------------------------------------------------------------
# Insufficient estimation history
# ------------------------------------------------------------

insufficient = df[
    df["coverageStatus"] == "insufficient_estimation_history"
].copy()

insufficient.to_csv(
    OUT_DIR / "insufficient_estimation_history.csv",
    index=False,
    encoding="utf-8-sig",
)


# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------

print("\n==============================")
print("Stage 7A.1 Missing-Security Diagnostic")
print("==============================")

print(f"Missing-security events:      {len(missing):,}")
print(
    f"Unique missing secCodes:      "
    f"{missing['secCode'].nunique():,}"
    if not missing.empty
    else "Unique missing secCodes:      0"
)

print(
    f"Unique missing EDINET codes:  "
    f"{missing['edinetCode'].nunique():,}"
    if not missing.empty
    else "Unique missing EDINET codes:  0"
)

print(f"No-price-after events:        {len(no_after):,}")
print(f"Insufficient-history events:  {len(insufficient):,}")

if not by_code.empty:
    print("\nTop missing securities by event count:")
    print(
        by_code.head(30).to_string(index=False)
    )

print("\nOutputs:")
print(OUT_DIR / "missing_security_by_code.csv")
print(OUT_DIR / "missing_security_events.csv")
print(OUT_DIR / "no_price_after_event.csv")
print(OUT_DIR / "insufficient_estimation_history.csv")