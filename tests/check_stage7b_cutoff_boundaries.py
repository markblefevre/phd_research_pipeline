from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

INPUT = (
    ROOT
    / "data/interim/paper2/market_reaction/stage7_event_dates.csv"
)

CHANGE_DATE = pd.Timestamp("2024-11-05")


def main():
    df = pd.read_csv(INPUT, low_memory=False)

    df["submitDateTime"] = pd.to_datetime(df["submitDateTime"])
    df["eventTradingDate"] = pd.to_datetime(df["eventTradingDate"])
    df["submissionDate"] = pd.to_datetime(df["submissionDate"])

    # Distance from the applicable market close in minutes.
    old_mask = df["submissionDate"] < CHANGE_DATE

    df["closeTimestamp"] = df["submissionDate"]

    df.loc[old_mask, "closeTimestamp"] += pd.Timedelta(hours=15)
    df.loc[~old_mask, "closeTimestamp"] += pd.Timedelta(
        hours=15,
        minutes=30,
    )

    df["minutesFromClose"] = (
        df["submitDateTime"] - df["closeTimestamp"]
    ).dt.total_seconds() / 60.0

    # ---------------------------------------------------------
    # 1. Look at observations nearest the cutoff
    # ---------------------------------------------------------

    near = (
        df.assign(absMinutesFromClose=df["minutesFromClose"].abs())
        .sort_values("absMinutesFromClose")
        .head(40)
    )

    id_cols = [
        c for c in [
            "edinetCode",
            "curr_docID",
            "docID",
            "secCode",
        ]
        if c in df.columns
    ]

    cols = id_cols + [
        "submitDateTime",
        "marketCloseTime",
        "minutesFromClose",
        "eventTradingDate",
        "eventDateRule",
    ]

    print("\n=== 40 observations closest to market close ===\n")
    print(
        near[cols].to_string(
            index=False,
            justify="left",
        )
    )

    # ---------------------------------------------------------
    # 2. Explicit pre-Nov-2024 boundary checks
    # ---------------------------------------------------------

    old = df[df["submissionDate"] < CHANGE_DATE].copy()

    print("\n=== OLD REGIME: nearest observations to 15:00 ===\n")
    print(
        old.assign(absMinutesFromClose=old["minutesFromClose"].abs())
        .sort_values("absMinutesFromClose")
        .head(20)[cols]
        .to_string(index=False)
    )

    # ---------------------------------------------------------
    # 3. Explicit post-Nov-2024 boundary checks
    # ---------------------------------------------------------

    new = df[df["submissionDate"] >= CHANGE_DATE].copy()

    print("\n=== NEW REGIME: nearest observations to 15:30 ===\n")
    print(
        new.assign(absMinutesFromClose=new["minutesFromClose"].abs())
        .sort_values("absMinutesFromClose")
        .head(20)[cols]
        .to_string(index=False)
    )

    # ---------------------------------------------------------
    # 4. Mechanical rule validation
    # ---------------------------------------------------------

    expected_same_day = df["minutesFromClose"] <= 0

    actual_same_day = df["eventDateRule"].eq(
        "same_day_at_or_before_close"
    )

    mismatch = df[expected_same_day != actual_same_day]

    print("\n=== RULE VALIDATION ===\n")
    print(f"Total events:                  {len(df):,}")
    print(
        f"Expected same-day <= close:    "
        f"{expected_same_day.sum():,}"
    )
    print(
        f"Actual same-day assignments:   "
        f"{actual_same_day.sum():,}"
    )
    print(f"Rule mismatches:               {len(mismatch):,}")

    if len(mismatch):
        print("\nMISMATCHES:\n")
        print(
            mismatch[cols]
            .sort_values("submitDateTime")
            .to_string(index=False)
        )
        raise AssertionError(
            "Stage 7B close-time rule mismatch detected."
        )

    # ---------------------------------------------------------
    # 5. Exact boundary timestamps, if any
    # ---------------------------------------------------------

    exact = df[df["minutesFromClose"] == 0]

    print("\n=== EXACTLY AT MARKET CLOSE ===\n")
    print(f"Exact-close filings: {len(exact):,}")

    if len(exact):
        print(
            exact[cols]
            .sort_values("submitDateTime")
            .to_string(index=False)
        )

        bad_exact = exact[
            ~exact["eventDateRule"].eq(
                "same_day_at_or_before_close"
            )
        ]

        if len(bad_exact):
            raise AssertionError(
                "A filing exactly at market close was not assigned same-day."
            )

    print("\nPASS: Stage 7B market-close boundary logic is internally consistent.")


if __name__ == "__main__":
    main()