from pathlib import Path
import pandas as pd

ROOT = Path(r"Z:\Documents\Education\2021 EDHEC Exec PhD\5 Pipeline")

PRICE_DIR = ROOT / "data/raw/paper2/prices"
EVENTS_CSV = ROOT / "data/interim/paper2/longitudinal/research_eligible_pairs.csv"

OUT_DIR = ROOT / "data/interim/paper2/market_reaction"
OUT_DIR.mkdir(parents=True, exist_ok=True)

EST_START = -120
EST_END = -20
MIN_EST_OBS = 60


def normalize_code(s):
    """
    Normalize EDINET secCode / J-Quants Code.

    EDINET secCode is commonly 5 digits, e.g. 72030.
    J-Quants may also use 5-digit issue codes.
    """
    s = s.astype("string").str.strip()

    # Remove Excel/CSV-style .0 if present
    s = s.str.replace(r"\.0$", "", regex=True)

    # Preserve leading zero if present
    return s


# ------------------------------------------------------------
# Load Paper 2 event universe
# ------------------------------------------------------------

events = pd.read_csv(
    EVENTS_CSV,
    dtype={"secCode": "string", "edinetCode": "string"},
    low_memory=False,
)

required = {
    "edinetCode",
    "secCode",
    "curr_docID",
    "curr_submitDateTime",
}

missing_cols = required - set(events.columns)
if missing_cols:
    raise ValueError(f"Missing event columns: {sorted(missing_cols)}")

events = events[list(required)].copy()

events["secCode"] = normalize_code(events["secCode"])
events["submitDateTime"] = pd.to_datetime(
    events["curr_submitDateTime"],
    errors="coerce",
)

events = events.dropna(subset=["secCode", "submitDateTime"])

print(f"Research events loaded: {len(events):,}")
print(f"Unique security codes:  {events['secCode'].nunique():,}")


# ------------------------------------------------------------
# Read only Date + Code from J-Quants monthly gzip files
# ------------------------------------------------------------

files = sorted(PRICE_DIR.glob("equities_bars_daily_*.csv.gz"))

if not files:
    raise FileNotFoundError(f"No price files found in {PRICE_DIR}")

print(f"Monthly J-Quants files: {len(files)}")

price_parts = []

date_col = None
code_col = None

for i, path in enumerate(files, 1):

    # Inspect header first so the script tolerates slightly different
    # capitalization/naming.
    header = pd.read_csv(path, compression="gzip", nrows=0)

    columns = list(header.columns)
    lower = {c.lower(): c for c in columns}

    if date_col is None:
        for candidate in ("date", "tradingdate", "trading_date"):
            if candidate in lower:
                date_col = lower[candidate]
                break

    if code_col is None:
        for candidate in ("code", "symbol", "issuecode"):
            if candidate in lower:
                code_col = lower[candidate]
                break

    if date_col is None or code_col is None:
        raise ValueError(
            f"Could not identify date/code columns in {path.name}\n"
            f"Columns: {columns}"
        )

    df = pd.read_csv(
        path,
        compression="gzip",
        usecols=[date_col, code_col],
        dtype={code_col: "string"},
        low_memory=False,
    )

    df = df.rename(
        columns={
            date_col: "Date",
            code_col: "Code",
        }
    )

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Code"] = normalize_code(df["Code"])

    df = df.dropna(subset=["Date", "Code"])

    price_parts.append(df)

    if i % 12 == 0 or i == len(files):
        print(f"  read {i:3d}/{len(files)} files")


prices = pd.concat(price_parts, ignore_index=True)

prices = (
    prices
    .drop_duplicates(["Code", "Date"])
    .sort_values(["Code", "Date"])
    .reset_index(drop=True)
)

print(f"\nPrice observations:     {len(prices):,}")
print(f"Price security codes:   {prices['Code'].nunique():,}")
print(f"First price date:       {prices['Date'].min().date()}")
print(f"Last price date:        {prices['Date'].max().date()}")


# ------------------------------------------------------------
# Build per-security trading-date arrays
# ------------------------------------------------------------

dates_by_code = {
    code: grp["Date"].to_numpy()
    for code, grp in prices.groupby("Code", sort=False)
}


# ------------------------------------------------------------
# Event coverage diagnostic
#
# For now, align t=0 to first available stock trading date
# on or after the filing calendar date.
#
# Stage 7B will refine this using submit TIME / TSE close.
# ------------------------------------------------------------

rows = []

for n, row in enumerate(events.itertuples(index=False), 1):

    code = row.secCode
    filing_ts = row.submitDateTime
    filing_date = pd.Timestamp(filing_ts).normalize()

    trading_dates = dates_by_code.get(code)

    result = {
        "edinetCode": row.edinetCode,
        "secCode": code,
        "curr_docID": row.curr_docID,
        "submitDateTime": filing_ts,
        "eventCalendarDate": filing_date,
        "eventTradingDate": pd.NaT,
        "estimationObsAvailable": 0,
        "full120DayLookback": False,
        "meets60ObsMinimum": False,
        "hasPriceCode": trading_dates is not None,
        "coverageStatus": None,
    }

    if trading_dates is None or len(trading_dates) == 0:
        result["coverageStatus"] = "missing_security"
        rows.append(result)
        continue

    # First trading date >= filing date
    idx = trading_dates.searchsorted(
        filing_date.to_datetime64(),
        side="left",
    )

    if idx >= len(trading_dates):
        result["coverageStatus"] = "no_price_on_or_after_event"
        rows.append(result)
        continue

    event_date = pd.Timestamp(trading_dates[idx])
    result["eventTradingDate"] = event_date

    # Paper 1 estimation window = [-120, -20]
    start_idx = idx + EST_START
    end_idx = idx + EST_END

    # Number actually available if history truncates at beginning
    usable_start = max(0, start_idx)
    usable_end = min(len(trading_dates) - 1, end_idx)

    if usable_end >= usable_start:
        obs = usable_end - usable_start + 1
    else:
        obs = 0

    result["estimationObsAvailable"] = obs
    result["full120DayLookback"] = start_idx >= 0
    result["meets60ObsMinimum"] = obs >= MIN_EST_OBS

    if start_idx >= 0 and obs >= MIN_EST_OBS:
        result["coverageStatus"] = "full"
    elif obs >= MIN_EST_OBS:
        result["coverageStatus"] = "partial_but_usable"
    else:
        result["coverageStatus"] = "insufficient_estimation_history"

    rows.append(result)

    if n % 5000 == 0:
        print(f"  checked {n:,}/{len(events):,} events")


coverage = pd.DataFrame(rows)

out_csv = OUT_DIR / "price_coverage_diagnostic.csv"
coverage.to_csv(out_csv, index=False, encoding="utf-8-sig")


# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------

print("\n==============================")
print("Stage 7 Price Coverage")
print("==============================")

print(f"Events checked:              {len(coverage):,}")
print(f"Full coverage:               {(coverage.coverageStatus == 'full').sum():,}")
print(
    f"Partial but >=60 obs:        "
    f"{(coverage.coverageStatus == 'partial_but_usable').sum():,}"
)
print(
    f"Insufficient history:        "
    f"{(coverage.coverageStatus == 'insufficient_estimation_history').sum():,}"
)
print(
    f"Missing security:            "
    f"{(coverage.coverageStatus == 'missing_security').sum():,}"
)
print(
    f"No price after event:        "
    f"{(coverage.coverageStatus == 'no_price_on_or_after_event').sum():,}"
)

usable = coverage["meets60ObsMinimum"].sum()

print(
    f"\nUsable >=60 observations:    "
    f"{usable:,} / {len(coverage):,} "
    f"({100 * usable / len(coverage):.2f}%)"
)

print("\nCoverage by filing year:")

coverage["year"] = pd.to_datetime(
    coverage["submitDateTime"]
).dt.year

summary = pd.crosstab(
    coverage["year"],
    coverage["coverageStatus"],
)

print(summary.to_string())

print(f"\nDetailed output:\n{out_csv}")