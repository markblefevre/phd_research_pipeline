from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PRICE_DIR = ROOT / "data/raw/paper2/prices"


def main():
    files = sorted(PRICE_DIR.glob("equities_bars_daily_*.csv.gz"))

    if not files:
        raise FileNotFoundError(f"No price files found under {PRICE_DIR}")

    hits = []

    for file in files:
        df = pd.read_csv(
            file,
            usecols=["Date", "Code", "C", "AdjFactor"],
            low_memory=False,
        )

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df["AdjFactor"] = pd.to_numeric(df["AdjFactor"], errors="coerce")

        x = df[
            df["AdjFactor"].notna()
            & (df["AdjFactor"] != 1.0)
        ].copy()

        if not x.empty:
            x["sourceFile"] = file.name
            hits.append(x)

    if not hits:
        print("No rows found with AdjFactor != 1.0")
        return

    out = pd.concat(hits, ignore_index=True)

    print("\n=== Adjustment-factor QC ===\n")
    print(f"Files scanned:                  {len(files):,}")
    print(f"Rows with AdjFactor != 1.0:     {len(out):,}")
    print(f"Unique securities affected:     {out['Code'].nunique():,}")
    print(f"Unique dates affected:          {out['Date'].nunique():,}")

    print("\nAdjustment-factor distribution:\n")
    print(
        out["AdjFactor"]
        .value_counts(dropna=False)
        .sort_index()
        .to_string()
    )

    print("\nDate range:")
    print(out["Date"].min(), "through", out["Date"].max())

    print("\nFirst 50 adjustment rows:\n")
    print(
        out[
            ["Date", "Code", "C", "AdjFactor", "sourceFile"]
        ]
        .sort_values(["Date", "Code"])
        .head(50)
        .to_string(index=False)
    )

    print("\nLargest / smallest adjustment factors:\n")
    print(
        out[
            ["Date", "Code", "C", "AdjFactor"]
        ]
        .sort_values("AdjFactor")
        .head(20)
        .to_string(index=False)
    )

    print()
    print(
        out[
            ["Date", "Code", "C", "AdjFactor"]
        ]
        .sort_values("AdjFactor", ascending=False)
        .head(20)
        .to_string(index=False)
    )

    out_path = (
        ROOT
        / "data/interim/paper2/market_reaction/"
        / "adjustment_factor_events.csv"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"\nFull output written to:\n{out_path}")


if __name__ == "__main__":
    main()