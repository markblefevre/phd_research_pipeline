from __future__ import annotations

from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PAIR_KEYS = ["edinetCode", "prev_docID", "curr_docID"]


def _save(fig: plt.Figure, stem: Path, formats: Iterable[str]) -> None:
    stem.parent.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        fig.savefig(stem.with_suffix(f".{fmt}"), bbox_inches="tight", dpi=300)
    plt.close(fig)


def _read_variant(novelty_root: Path, variant: str) -> pd.DataFrame:
    path = novelty_root / variant / "pair_novelty.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing Stage 5 novelty output: {path}")
    df = pd.read_csv(path, dtype={"edinetCode": str, "prev_docID": str, "curr_docID": str})
    required = set(PAIR_KEYS + ["novelty"])
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {path}: {sorted(missing)}")
    return df[PAIR_KEYS + ["novelty"]].rename(columns={"novelty": variant})


def _load_inputs(
    *, novelty_root: Path, pairs_csv: Path, diagnostics_csv: Path,
    baseline_variant: str, raw_variant: str,
) -> pd.DataFrame:
    base = _read_variant(novelty_root, baseline_variant)
    raw = _read_variant(novelty_root, raw_variant)
    pairs = pd.read_csv(
        pairs_csv,
        dtype={"edinetCode": str, "prev_docID": str, "curr_docID": str},
    )
    diagnostics = pd.read_csv(
        diagnostics_csv,
        dtype={"edinetCode": str, "prev_docID": str, "curr_docID": str},
    )

    needed_pairs = set(PAIR_KEYS + ["curr_periodEnd"])
    missing = needed_pairs - set(pairs.columns)
    if missing:
        raise ValueError(f"Missing columns in {pairs_csv}: {sorted(missing)}")
    if "absLogLengthChange" not in diagnostics.columns:
        raise ValueError(f"Missing absLogLengthChange in {diagnostics_csv}")

    out = base.merge(raw, on=PAIR_KEYS, validate="one_to_one")
    out = out.merge(
        pairs[PAIR_KEYS + ["curr_periodEnd"]], on=PAIR_KEYS, validate="one_to_one"
    )
    out = out.merge(
        diagnostics[PAIR_KEYS + ["absLogLengthChange", "logLengthChange"]],
        on=PAIR_KEYS, validate="one_to_one",
    )
    out["curr_periodEnd"] = pd.to_datetime(out["curr_periodEnd"], errors="raise")
    out["fiscalYear"] = out["curr_periodEnd"].dt.year.astype(int)
    return out


def run_novelty_plots(
    *,
    novelty_root: Path,
    pairs_csv: Path,
    output_dir: Path,
    baseline_variant: str = "sudachi_c_num",
    raw_variant: str = "sudachi_c_raw",
    diagnostics_csv: Path | None = None,
    formats: Iterable[str] = ("png", "pdf"),
    min_year_observations: int = 25,
) -> dict[str, Path]:
    """Create reproducible Stage 5 descriptive/QC figures and annual summary."""
    novelty_root = Path(novelty_root)
    pairs_csv = Path(pairs_csv)
    output_dir = Path(output_dir)
    diagnostics_csv = diagnostics_csv or novelty_root / "pair_diagnostics.csv"

    paper_dir = output_dir / "paper"
    diagnostic_dir = output_dir / "diagnostics"
    paper_dir.mkdir(parents=True, exist_ok=True)
    diagnostic_dir.mkdir(parents=True, exist_ok=True)

    df = _load_inputs(
        novelty_root=novelty_root,
        pairs_csv=pairs_csv,
        diagnostics_csv=Path(diagnostics_csv),
        baseline_variant=baseline_variant,
        raw_variant=raw_variant,
    )

    annual = (
        df.groupby("fiscalYear")
        .agg(
            n=(baseline_variant, "size"),
            cNumMean=(baseline_variant, "mean"),
            cNumMedian=(baseline_variant, "median"),
            cRawMean=(raw_variant, "mean"),
            cRawMedian=(raw_variant, "median"),
            absLogLengthChangeMean=("absLogLengthChange", "mean"),
            absLogLengthChangeMedian=("absLogLengthChange", "median"),
        )
        .reset_index()
    )
    annual_csv = output_dir / "annual_novelty_summary.csv"
    annual.to_csv(annual_csv, index=False)

    # 1. Distribution: baseline vs raw.
    fig, ax = plt.subplots(figsize=(8, 5))
    bins = np.linspace(0, max(df[baseline_variant].max(), df[raw_variant].max()), 60)
    ax.hist(df[raw_variant], bins=bins, alpha=0.5, density=True, label="C raw")
    ax.hist(df[baseline_variant], bins=bins, alpha=0.5, density=True, label="C number-normalized")
    ax.set_xlabel("Textual novelty (1 - cosine similarity)")
    ax.set_ylabel("Density")
    ax.set_title("Distribution of textual novelty")
    ax.legend()
    _save(fig, paper_dir / "novelty_distribution", formats)

    # 2. Raw vs normalized scatter.
    corr = df[[baseline_variant, raw_variant]].corr().iloc[0, 1]
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(df[baseline_variant], df[raw_variant], s=5, alpha=0.15)
    hi = max(df[baseline_variant].max(), df[raw_variant].max())
    ax.plot([0, hi], [0, hi], linestyle="--", linewidth=1)
    ax.set_xlabel("C number-normalized novelty")
    ax.set_ylabel("C raw novelty")
    ax.set_title(f"Raw vs number-normalized novelty (r = {corr:.3f})")
    _save(fig, diagnostic_dir / "novelty_raw_vs_normalized_scatter", formats)

    # Publication-year series: retain all years meeting an explicit minimum n.
    annual_plot = annual.loc[annual["n"] >= min_year_observations].copy()

    # 3. Baseline mean and median by fiscal year.
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(annual_plot["fiscalYear"], annual_plot["cNumMean"], marker="o", label="Mean")
    ax.plot(annual_plot["fiscalYear"], annual_plot["cNumMedian"], marker="o", label="Median")
    ax.set_xlabel("Fiscal year end")
    ax.set_ylabel("C number-normalized novelty")
    ax.set_title("Textual novelty by fiscal year")
    ax.legend()
    _save(fig, paper_dir / "novelty_by_fiscal_year", formats)

    # 4. Raw and normalized annual means.
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(annual_plot["fiscalYear"], annual_plot["cNumMean"], marker="o", label="C number-normalized")
    ax.plot(annual_plot["fiscalYear"], annual_plot["cRawMean"], marker="o", label="C raw")
    ax.set_xlabel("Fiscal year end")
    ax.set_ylabel("Mean textual novelty")
    ax.set_title("Raw and number-normalized novelty by fiscal year")
    ax.legend()
    _save(fig, diagnostic_dir / "novelty_raw_vs_normalized_by_year", formats)

    # 5. Novelty vs absolute log length change. Hexbin avoids overplotting 33k pairs.
    fig, ax = plt.subplots(figsize=(7, 5))
    hb = ax.hexbin(df["absLogLengthChange"], df[baseline_variant], gridsize=50, mincnt=1)
    fig.colorbar(hb, ax=ax, label="Pair count")
    ax.set_xlabel("Absolute log change in MD&A length")
    ax.set_ylabel("C number-normalized novelty")
    ax.set_title("Textual novelty and document-length change")
    _save(fig, diagnostic_dir / "novelty_vs_length_change", formats)

    # 6. Distribution by fiscal year. Suppress sparse boundary years by n threshold.
    years = annual_plot["fiscalYear"].tolist()
    data = [df.loc[df["fiscalYear"] == year, baseline_variant].to_numpy() for year in years]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.boxplot(data, labels=[str(y) for y in years], showfliers=False)
    ax.set_xlabel("Fiscal year end")
    ax.set_ylabel("C number-normalized novelty")
    ax.set_title("Distribution of textual novelty by fiscal year")
    _save(fig, diagnostic_dir / "novelty_by_year_boxplot", formats)

    return {
        "annual_summary": annual_csv,
        "paper_dir": paper_dir,
        "diagnostic_dir": diagnostic_dir,
    }
