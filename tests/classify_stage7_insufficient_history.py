from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(r"Z:\Documents\Education\2021 EDHEC Exec PhD\5 Pipeline")

PRICE_DIR = ROOT / "data/raw/paper2/prices"
COVERAGE_CSV = (
    ROOT
    / "data/interim/paper2/market_reaction/price_coverage_diagnostic.csv"
)

OUT_CSV = (
    ROOT
    / "data/interim/paper2/market_reaction/"
      "insufficient_history_classification.csv"
)

# ------------------------------------------------------------
# Load the 72 cases
# ------------------------------------------------------------

cov = pd.read_csv(
    COVERAGE_CSV,
    dtype={"secCode": "string"},
    low_memory=False,
)

x = cov.loc[
    cov["coverageStatus"].eq("insufficient_estimation_history")
].copy()

x["secCode"] = x["secCode"].str.strip().str.zfill(5)
x["submitDateTime"] = pd.to_datetime(x["submitDateTime"])
x["eventDate"] = x["submitDateTime"].dt.normalize()

target_codes = set(x["secCode"].dropna())

print(f"Target events:     {len(x):,}")
print(f"Target securities: {len(target_codes):,}")

# ------------------------------------------------------------
# Read only Date + Code for relevant securities
# ------------------------------------------------------------

parts = []
archive_start = None
archive_end = None

files = sorted(PRICE_DIR.glob("equities_bars_daily_*.csv*"))

print(f"Price files:       {len(files):,}")

for path in files:

    header = pd.read_csv(
        path,
        nrows=0,
        compression="infer",
    )

    lower = {c.lower(): c for c in header.columns}

    code_col = (
        lower.get("code")
        or lower.get("symbol")
    )

    date_col = (
        lower.get("date")
        or lower.get("trading_date")
    )

    if code_col is None or date_col is None:
        print(f"Skipping unrecognized file: {path.name}")
        continue

    df = pd.read_csv(
        path,
        usecols=[code_col, date_col],
        dtype={code_col: "string"},
        compression="infer",
        low_memory=False,
    )

    df[code_col] = (
        df[code_col]
        .str.strip()
        .str.zfill(5)
    )

    df[date_col] = pd.to_datetime(
        df[date_col],
        errors="coerce",
    )

    # Track actual archive boundaries using all rows.
    file_min = df[date_col].min()
    file_max = df[date_col].max()

    if pd.notna(file_min):
        archive_start = (
            file_min
            if archive_start is None
            else min(archive_start, file_min)
        )

    if pd.notna(file_max):
        archive_end = (
            file_max
            if archive_end is None
            else max(archive_end, file_max)
        )

    df = df.loc[
        df[code_col].isin(target_codes),
        [code_col, date_col],
    ]

    if not df.empty:
        df = df.rename(
            columns={
                code_col: "secCode",
                date_col: "Date",
            }
        )
        parts.append(df)

prices = pd.concat(parts, ignore_index=True)

prices = (
    prices
    .dropna(subset=["Date"])
    .drop_duplicates(["secCode", "Date"])
    .sort_values(["secCode", "Date"])
)

print()
print(f"Archive start: {archive_start.date()}")
print(f"Archive end:   {archive_end.date()}")

# ------------------------------------------------------------
# Make date arrays by security
# ------------------------------------------------------------

dates_by_code = {
    code: grp["Date"].sort_values().to_numpy(dtype="datetime64[ns]")
    for code, grp in prices.groupby("secCode")
}

# Give a few days of tolerance around the archive boundary.
archive_boundary_cutoff = archive_start + pd.Timedelta(days=7)

# ------------------------------------------------------------
# Classify each event
# ------------------------------------------------------------

rows = []

for _, r in x.iterrows():

    code = r["secCode"]
    event_date = r["eventDate"]

    dates = dates_by_code.get(code)

    rec = r.to_dict()

    if dates is None or len(dates) == 0:
        rec.update(
            firstPriceDate=pd.NaT,
            lastPriceDate=pd.NaT,
            firstPriceGapDays=np.nan,
            recalculatedEstimationObs=0,
            stage7Classification="price_series_missing_needs_review",
            olderHistoryCouldHelp=False,
        )
        rows.append(rec)
        continue

    event64 = np.datetime64(event_date)

    first_date = pd.Timestamp(dates[0])
    last_date = pd.Timestamp(dates[-1])

    # First trading observation on or after filing calendar date.
    t0 = int(np.searchsorted(dates, event64, side="left"))

    if t0 < len(dates):
        first_on_after = pd.Timestamp(dates[t0])
        event_gap_days = (first_on_after - event_date).days
    else:
        first_on_after = pd.NaT
        event_gap_days = np.nan

    first_price_gap = (first_date - event_date).days

    # Paper 1 estimation window:
    #
    #       [-120, -20]
    #
    # relative to t0.
    est_start = max(0, t0 - 120)
    est_end = max(0, t0 - 20)

    estimation_dates = dates[est_start:est_end]
    est_obs = len(estimation_dates)

    if t0 > 0:
        last_pre_event = pd.Timestamp(dates[t0 - 1])
    else:
        last_pre_event = pd.NaT

    # --------------------------------------------------------
    # Classification
    # --------------------------------------------------------

    #
    # Price series begins materially AFTER the filing.
    #
    # Older history will not fix this.
    #
    if first_date > event_date + pd.Timedelta(days=30):

        classification = "price_history_begins_long_after_event"
        older_help = False

    #
    # Price series begins shortly AFTER filing.
    # Could be IPO, transfer, suspension, code transition, etc.
    #
    elif first_date > event_date:

        classification = "price_history_begins_near_after_event_review"
        older_help = False

    #
    # The security's first available history starts right at the
    # beginning of OUR downloaded J-Quants archive.
    #
    # This is the key category where a deeper subscription could
    # potentially recover the missing pre-event observations.
    #
    elif (
        first_date <= archive_boundary_cutoff
        and est_obs < 60
    ):

        classification = "archive_boundary_older_history_may_help"
        older_help = True

    #
    # Security starts trading well AFTER our archive begins and
    # does not have 60 estimation observations.
    #
    # That looks like genuine listing / market-entry history,
    # not a subscription-depth problem.
    #
    elif (
        first_date > archive_boundary_cutoff
        and est_obs < 60
    ):

        classification = "security_history_itself_too_short"
        older_help = False

    else:

        classification = "other_needs_review"
        older_help = False

    rec.update(
        firstPriceDate=first_date,
        lastPriceDate=last_date,
        firstPriceOnOrAfterEvent=first_on_after,
        lastPriceBeforeEvent=last_pre_event,
        firstPriceGapDays=first_price_gap,
        eventPriceGapDays=event_gap_days,
        recalculatedEstimationObs=est_obs,
        stage7Classification=classification,
        olderHistoryCouldHelp=older_help,
    )

    rows.append(rec)

out = pd.DataFrame(rows)

# ------------------------------------------------------------
# Results
# ------------------------------------------------------------

print()
print("=" * 72)
print("STAGE 7 — INSUFFICIENT HISTORY CLASSIFICATION")
print("=" * 72)

print()
print(out["stage7Classification"].value_counts(dropna=False))

print()
print("Older J-Quants history potentially useful:")
print(
    f"{out['olderHistoryCouldHelp'].sum():,} / {len(out):,}"
)

print()
print("By classification and filing year:")

out["filingYear"] = out["submitDateTime"].dt.year

print(
    pd.crosstab(
        out["filingYear"],
        out["stage7Classification"],
    ).to_string()
)

print()
print("=" * 72)
print("CASES THAT OLDER HISTORY COULD POTENTIALLY RESCUE")
print("=" * 72)

cols = [
    "edinetCode",
    "secCode",
    "curr_docID",
    "submitDateTime",
    "firstPriceDate",
    "firstPriceOnOrAfterEvent",
    "lastPriceBeforeEvent",
    "recalculatedEstimationObs",
]

rescue = out.loc[
    out["olderHistoryCouldHelp"],
    cols,
].sort_values("submitDateTime")

if rescue.empty:
    print("None")
else:
    print(rescue.to_string(index=False))

print()
print("=" * 72)
print("LONG-AFTER-EVENT PRICE STARTS")
print("=" * 72)

z = out.loc[
    out["stage7Classification"].eq(
        "price_history_begins_long_after_event"
    ),
    [
        "edinetCode",
        "secCode",
        "submitDateTime",
        "firstPriceDate",
        "firstPriceGapDays",
    ],
].sort_values("submitDateTime")

print(z.to_string(index=False))

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(OUT_CSV, index=False)

print()
print(f"Wrote: {OUT_CSV}")