from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PRICE_DIR = ROOT / "data/raw/paper2/prices"

OUT = (
    ROOT
    / "data/interim/paper2/market_reaction"
    / "adjusted_return_qc.csv"
)


def load_prices():
    frames = []

    files = sorted(
        PRICE_DIR.glob("equities_bars_daily_*.csv.gz")
    )

    if not files:
        raise FileNotFoundError(
            f"No price files found under {PRICE_DIR}"
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

    prices = pd.concat(
        frames,
        ignore_index=True,
    )

    prices = (
        prices
        .dropna(subset=["Date", "Code"])
        .sort_values(["Code", "Date"])
        .reset_index(drop=True)
    )

    return prices, len(files)


def build_tse_calendar(prices):
    return (
        pd.Index(
            prices["Date"]
            .dropna()
            .drop_duplicates()
            .sort_values()
        )
    )


def build_return_pairs(prices, tse_calendar):
    calendar_pos = {
        date: i
        for i, date in enumerate(tse_calendar)
    }

    rows = []

    for code, stock in prices.groupby(
        "Code",
        sort=False,
        observed=True,
    ):
        stock = (
            stock
            .sort_values("Date")
            .reset_index(drop=True)
        )

        prev_valid_date = None
        prev_valid_close = None

        for row in stock.itertuples(index=False):

            date = row.Date
            close = row.C
            factor = row.AdjFactor

            if pd.isna(close):
                continue

            close = float(close)

            if (
                prev_valid_date is not None
                and prev_valid_close is not None
            ):

                prev_pos = calendar_pos.get(prev_valid_date)
                curr_pos = calendar_pos.get(date)

                consecutive = (
                    prev_pos is not None
                    and curr_pos is not None
                    and curr_pos == prev_pos + 1
                )

                if consecutive:

                    factor_t = (
                        float(factor)
                        if pd.notna(factor)
                        else 1.0
                    )

                    raw_return = (
                        close / prev_valid_close - 1.0
                    )

                    adjusted_return = (
                        close
                        / (
                            prev_valid_close
                            * factor_t
                        )
                        - 1.0
                    )

                    rows.append(
                        {
                            "Code": code,
                            "prevDate": prev_valid_date,
                            "Date": date,
                            "prevClose": prev_valid_close,
                            "close": close,
                            "AdjFactor": factor_t,
                            "rawReturn": raw_return,
                            "adjustedReturn":
                                adjusted_return,
                            "hadAdjustment":
                                not np.isclose(
                                    factor_t,
                                    1.0,
                                ),
                            "consecutiveSession":
                                True,
                        }
                    )

            prev_valid_date = date
            prev_valid_close = close

    return pd.DataFrame(rows)


def main():

    prices, n_files = load_prices()

    tse_calendar = build_tse_calendar(prices)

    returns = build_return_pairs(
        prices,
        tse_calendar,
    )

    adjusted = returns[
        returns["hadAdjustment"]
    ].copy()

    normal = returns[
        ~returns["hadAdjustment"]
    ].copy()

    print(
        "\n=== Stage 7 adjusted-return QC "
        "(consecutive sessions only) ===\n"
    )

    print(
        f"Price files scanned:              "
        f"{n_files:,}"
    )

    print(
        f"TSE trading sessions:             "
        f"{len(tse_calendar):,}"
    )

    print(
        f"Valid consecutive-session returns:"
        f" {len(returns):,}"
    )

    print(
        f"Returns crossing an adjustment:   "
        f" {len(adjusted):,}"
    )

    print(
        f"Unique adjusted securities:       "
        f" {adjusted['Code'].nunique():,}"
    )

    print(
        "\n=== Adjustment-return comparison ===\n"
    )

    comparison = pd.DataFrame(
        {
            "absRawReturn":
                adjusted["rawReturn"].abs(),
            "absAdjustedReturn":
                adjusted["adjustedReturn"].abs(),
        }
    )

    print(
        comparison.describe(
            percentiles=[
                0.50,
                0.90,
                0.95,
                0.99,
            ]
        ).to_string()
    )

    print(
        "\nMechanical jumps removed:\n"
    )

    for threshold in [
        0.25,
        0.50,
        1.00,
        5.00,
    ]:

        raw_n = (
            adjusted["rawReturn"]
            .abs()
            .gt(threshold)
            .sum()
        )

        adj_n = (
            adjusted["adjustedReturn"]
            .abs()
            .gt(threshold)
            .sum()
        )

        print(
            f"|return| > {threshold:>4.2f}:  "
            f"raw={raw_n:>6,}   "
            f"adjusted={adj_n:>6,}"
        )

    max_normal_diff = (
        normal["rawReturn"]
        - normal["adjustedReturn"]
    ).abs().max()

    print(
        "\nNo-adjustment consistency check:"
    )

    print(
        "max |rawReturn - adjustedReturn| = "
        f"{max_normal_diff:.12g}"
    )

    print(
        "\n=== Largest absolute adjusted returns ===\n"
    )

    worst = (
        adjusted
        .assign(
            absAdjustedReturn=
                adjusted[
                    "adjustedReturn"
                ].abs()
        )
        .sort_values(
            "absAdjustedReturn",
            ascending=False,
        )
        .head(30)
    )

    print(
        worst[
            [
                "Code",
                "prevDate",
                "Date",
                "prevClose",
                "close",
                "AdjFactor",
                "rawReturn",
                "adjustedReturn",
            ]
        ]
        .to_string(index=False)
    )

    OUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    adjusted.to_csv(
        OUT,
        index=False,
    )

    print(
        f"\nFull adjustment-crossing QC written to:\n"
        f"{OUT}"
    )


if __name__ == "__main__":
    main()