from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


DOC_KEY = ["edinetCode", "docID"]


def _save(fig, stem: Path, formats: list[str]) -> None:
    stem.parent.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        fig.savefig(stem.with_suffix(f".{fmt}"), bbox_inches="tight", dpi=200)
    plt.close(fig)


def build_lmmd_diagnostics(
    *,
    sentiment_csv: str | Path,
    filings_csv: str | Path,
    output_dir: str | Path,
    formats: list[str] | None = None,
    min_year_observations: int = 25,
) -> pd.DataFrame:
    """Create LMMD distribution/time-series diagnostics without altering sentiment scores."""
    sentiment_csv = Path(sentiment_csv).expanduser().resolve()
    filings_csv = Path(filings_csv).expanduser().resolve()
    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    formats = formats or ["png", "pdf"]

    s = pd.read_csv(
        sentiment_csv,
        dtype={"edinetCode": "string", "docID": "string"},
        low_memory=False,
    )
    required_s = {*DOC_KEY, "lmmdPositiveRate", "lmmdNegativeRate", "lmmdNet"}
    missing = required_s - set(s.columns)
    if missing:
        raise ValueError(f"{sentiment_csv} missing columns: {sorted(missing)}")
    if s.duplicated(DOC_KEY).any():
        raise ValueError("LMMD sentiment contains duplicate document keys")

    f = pd.read_csv(
        filings_csv,
        dtype={"edinetCode": "string", "docID": "string"},
        low_memory=False,
    )
    required_f = {*DOC_KEY, "periodEnd"}
    missing = required_f - set(f.columns)
    if missing:
        raise ValueError(f"{filings_csv} missing columns: {sorted(missing)}")

    f = f[DOC_KEY + ["periodEnd"]].drop_duplicates(DOC_KEY)
    if f.duplicated(DOC_KEY).any():
        raise ValueError("Filing metadata contains duplicate document keys after projection")

    df = s.merge(f, on=DOC_KEY, how="left", validate="one_to_one")
    missing_period = int(df["periodEnd"].isna().sum())
    if missing_period:
        raise ValueError(f"{missing_period:,} LMMD documents lack periodEnd in filing metadata")

    df["periodEnd"] = pd.to_datetime(df["periodEnd"], errors="coerce")
    if df["periodEnd"].isna().any():
        raise ValueError("Could not parse one or more periodEnd values")
    df["fiscalYear"] = df["periodEnd"].dt.year.astype(int)

    annual = (
        df.groupby("fiscalYear", as_index=False)
        .agg(
            n=("lmmdNet", "size"),
            lmmdMean=("lmmdNet", "mean"),
            lmmdMedian=("lmmdNet", "median"),
            lmmdStd=("lmmdNet", "std"),
            positiveRateMean=("lmmdPositiveRate", "mean"),
            negativeRateMean=("lmmdNegativeRate", "mean"),
        )
        .sort_values("fiscalYear")
    )
    annual.to_csv(output_dir / "annual_lmmd_summary.csv", index=False)

    plot_annual = annual.loc[annual["n"] >= int(min_year_observations)].copy()
    if plot_annual.empty:
        raise ValueError("No fiscal year meets min_year_observations")

    # 1. Overall distribution
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(df["lmmdNet"], bins=80)
    ax.axvline(0.0, linewidth=1)
    ax.set_xlabel("LMMD net sentiment")
    ax.set_ylabel("Documents")
    ax.set_title("LMMD net sentiment distribution")
    _save(fig, output_dir / "lmmd_distribution", formats)

    # 2. Annual mean and median
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(plot_annual["fiscalYear"], plot_annual["lmmdMean"], marker="o", label="Mean")
    ax.plot(plot_annual["fiscalYear"], plot_annual["lmmdMedian"], marker="o", label="Median")
    ax.axhline(0.0, linewidth=1)
    ax.set_xlabel("Fiscal year")
    ax.set_ylabel("LMMD net sentiment")
    ax.set_title("LMMD net sentiment by fiscal year")
    ax.legend()
    _save(fig, output_dir / "lmmd_by_fiscal_year", formats)

    # 3. Annual distributions
    years = plot_annual["fiscalYear"].tolist()
    values = [df.loc[df["fiscalYear"].eq(y), "lmmdNet"].dropna().to_numpy() for y in years]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.boxplot(values, labels=years, showfliers=False)
    ax.axhline(0.0, linewidth=1)
    ax.set_xlabel("Fiscal year")
    ax.set_ylabel("LMMD net sentiment")
    ax.set_title("LMMD net sentiment distribution by fiscal year")
    ax.tick_params(axis="x", rotation=45)
    _save(fig, output_dir / "lmmd_by_year_boxplot", formats)

    return annual
