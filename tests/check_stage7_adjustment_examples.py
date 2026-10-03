from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PRICE_DIR = ROOT / "data/raw/paper2/prices"

EXAMPLES = [
    ("13010", "2016-09-28"),
    ("17260", "2016-09-28"),
    ("19040", "2016-09-28"),
    ("19310", "2016-09-28"),
    ("30920", "2016-09-28"),
]

WINDOW = 5


def load_all_prices():
    frames = []

    files = sorted(
        PRICE_DIR.glob("equities_bars_daily_*.csv.gz")
    )

    for file in files:
        df = pd.read_csv(
            file,
            usecols=["Date", "Code", "C", "AdjFactor"],
            dtype={"Code": "string"},
            low_memory=False,
        )

        df["Date"] = pd.to_datetime(
            df["Date"],
            errors="coerce",
        )

        df["C"] = pd.to_numeric(
            df["C"],
            errors="coerce",
        )

        df["AdjFactor"] = pd.to_numeric(
            df["AdjFactor"],
            errors="coerce",
        )

        frames.append(df)

    return pd.concat(
        frames,
        ignore_index=True,
    )


def main():
    prices = load_all_prices()

    prices = (
        prices
        .dropna(subset=["Date", "Code"])
        .sort_values(["Code", "Date"])
        .reset_index(drop=True)
    )

    for code, event_date_str in EXAMPLES:

        event_date = pd.Timestamp(event_date_str)

        stock = (
            prices[
                prices["Code"] == code
            ]
            .sort_values("Date")
            .reset_index(drop=True)
        )

        if stock.empty:
            print(f"\n{code}: no data found")
            continue

        matches = stock.index[
            stock["Date"] == event_date
        ].tolist()

        if not matches:
            print(
                f"\n{code}: no row found "
                f"for {event_date_str}"
            )
            continue

        idx = matches[0]

        start = max(
            0,
            idx - WINDOW,
        )

        end = min(
            len(stock),
            idx + WINDOW + 1,
        )

        x = stock.iloc[start:end].copy()

        x["rawReturn"] = (
            x["C"]
            .pct_change()
        )

        print("\n" + "=" * 80)
        print(
            f"Code {code} | "
            f"adjustment event {event_date_str}"
        )
        print("=" * 80)

        print(
            x[
                [
                    "Date",
                    "Code",
                    "C",
                    "AdjFactor",
                    "rawReturn",
                ]
            ]
            .to_string(
                index=False,
                formatters={
                    "rawReturn":
                        lambda v:
                        "" if pd.isna(v)
                        else f"{v: .6f}",
                },
            )
        )


if __name__ == "__main__":
    main()