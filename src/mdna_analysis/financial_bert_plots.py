"""Reproducible publication and diagnostic plots for Financial BERT sentiment."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

DOC_KEY = ["edinetCode", "docID"]


def _load(bert_csv: Path, filings_csv: Path, lmmd_csv: Path | None = None):
    bert = pd.read_csv(
        bert_csv, dtype={"edinetCode": "string", "docID": "string"}, low_memory=False
    )
    filings = pd.read_csv(
        filings_csv, dtype={"edinetCode": "string", "docID": "string"}, low_memory=False
    )
    required = {
        *DOC_KEY, "bertNet", "bertPositiveSentenceRate", "bertNegativeSentenceRate",
        "bertProbabilityNet",
    }
    missing = required - set(bert.columns)
    if missing:
        raise ValueError(f"BERT CSV missing columns: {sorted(missing)}")
    if bert.duplicated(DOC_KEY).any() or filings.duplicated("docID").any():
        raise ValueError("Duplicate keys detected in BERT or filings input")
    df = bert.merge(filings[["docID", "periodEnd"]], on="docID", how="left", validate="one_to_one")
    df["periodEnd"] = pd.to_datetime(df["periodEnd"], errors="coerce")
    if df["periodEnd"].isna().any():
        raise ValueError("Missing/invalid periodEnd after BERT/filings join")
    df["fiscalYear"] = df["periodEnd"].dt.year.astype(int)

    lmmd = None
    if lmmd_csv is not None:
        lmmd = pd.read_csv(
            lmmd_csv, dtype={"edinetCode": "string", "docID": "string"}, low_memory=False
        )
        if "lmmdNet" not in lmmd.columns:
            raise ValueError("LMMD CSV missing lmmdNet")
        if lmmd.duplicated(DOC_KEY).any():
            raise ValueError("LMMD CSV contains duplicate document keys")
    return df, lmmd


def _save(fig, base: Path, formats: tuple[str, ...], dpi: int) -> None:
    base.parent.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        fig.savefig(base.with_suffix(f".{fmt}"), bbox_inches="tight", dpi=dpi)


def generate_financial_bert_plots(
    *,
    bert_csv: Path,
    filings_csv: Path,
    output_dir: Path,
    lmmd_csv: Path | None = None,
    formats: tuple[str, ...] = ("pdf", "png"),
    dpi: int = 180,
    min_year_observations: int = 25,
) -> pd.DataFrame:
    import matplotlib.pyplot as plt

    bert_csv = Path(bert_csv).expanduser().resolve()
    filings_csv = Path(filings_csv).expanduser().resolve()
    output_dir = Path(output_dir).expanduser().resolve()
    lmmd_csv = Path(lmmd_csv).expanduser().resolve() if lmmd_csv is not None else None
    paper_dir = output_dir / "paper"
    diag_dir = output_dir / "diagnostics"
    paper_dir.mkdir(parents=True, exist_ok=True)
    diag_dir.mkdir(parents=True, exist_ok=True)

    df, lmmd = _load(bert_csv, filings_csv, lmmd_csv)
    annual = df.groupby("fiscalYear").agg(
        n=("bertNet", "size"),
        bertMean=("bertNet", "mean"),
        bertMedian=("bertNet", "median"),
        bertStd=("bertNet", "std"),
        positiveRateMean=("bertPositiveSentenceRate", "mean"),
        negativeRateMean=("bertNegativeSentenceRate", "mean"),
        probabilityNetMean=("bertProbabilityNet", "mean"),
    ).reset_index()
    annual.to_csv(output_dir / "annual_financial_bert_summary.csv", index=False)
    show = annual.loc[annual["n"] >= int(min_year_observations)].copy()

    # 1. Paper: overall hard-label BERT net distribution.
    fig, ax = plt.subplots(figsize=(7.2, 4.5))
    ax.hist(df["bertNet"].dropna(), bins=70)
    ax.axvline(0.0, linewidth=1)
    ax.set_xlabel("Financial BERT sentiment (positive sentence share − negative sentence share)")
    ax.set_ylabel("Documents")
    ax.set_title("Distribution of Japanese Financial BERT sentiment")
    _save(fig, paper_dir / "bert_distribution", formats, dpi)
    plt.close(fig)

    # 2. Paper: annual mean / median.
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    ax.plot(show["fiscalYear"], show["bertMean"], marker="o", label="Mean")
    ax.plot(show["fiscalYear"], show["bertMedian"], marker="o", label="Median")
    ax.axhline(0.0, linewidth=1)
    ax.set_xlabel("Fiscal year (period end)")
    ax.set_ylabel("Financial BERT sentiment")
    ax.set_title("Financial BERT sentiment by fiscal year")
    ax.legend()
    _save(fig, paper_dir / "bert_by_fiscal_year", formats, dpi)
    plt.close(fig)

    # 3. Diagnostic: positive and negative sentence rates separately.
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    ax.plot(show["fiscalYear"], show["positiveRateMean"], marker="o", label="Positive sentence share")
    ax.plot(show["fiscalYear"], show["negativeRateMean"], marker="o", label="Negative sentence share")
    ax.set_xlabel("Fiscal year (period end)")
    ax.set_ylabel("Mean classified sentence share")
    ax.set_title("Financial BERT positive and negative incidence by year")
    ax.legend()
    _save(fig, diag_dir / "bert_positive_negative_rates_by_year", formats, dpi)
    plt.close(fig)

    # 4. Diagnostic: year-by-year distributions to make the 2018 discontinuity visible.
    years = show["fiscalYear"].astype(int).tolist()
    groups = [df.loc[df["fiscalYear"].eq(y), "bertNet"].dropna().values for y in years]
    if years:
        fig, ax = plt.subplots(figsize=(8.5, 4.8))
        ax.boxplot(groups, labels=years, showfliers=False)
        ax.axhline(0.0, linewidth=1)
        ax.set_xlabel("Fiscal year (period end)")
        ax.set_ylabel("Financial BERT sentiment")
        ax.set_title("Financial BERT sentiment distributions by year")
        _save(fig, diag_dir / "bert_by_year_boxplot", formats, dpi)
        plt.close(fig)

    # 5. Diagnostic: BERT vs LMMD document-level comparison.
    if lmmd is not None:
        both = df[DOC_KEY + ["bertNet"]].merge(
            lmmd[DOC_KEY + ["lmmdNet"]], on=DOC_KEY, how="inner", validate="one_to_one"
        ).dropna(subset=["bertNet", "lmmdNet"])
        if not both.empty:
            pearson = both[["bertNet", "lmmdNet"]].corr(method="pearson").iloc[0, 1]
            spearman = both[["bertNet", "lmmdNet"]].corr(method="spearman").iloc[0, 1]
            fig, ax = plt.subplots(figsize=(6.2, 5.0))
            hb = ax.hexbin(both["lmmdNet"], both["bertNet"], gridsize=55, mincnt=1)
            fig.colorbar(hb, ax=ax, label="Documents")
            ax.axhline(0.0, linewidth=0.8)
            ax.axvline(0.0, linewidth=0.8)
            ax.set_xlabel("LMMD net sentiment")
            ax.set_ylabel("Financial BERT sentiment")
            ax.set_title(f"BERT vs LMMD (Pearson={pearson:.3f}; Spearman={spearman:.3f})")
            _save(fig, diag_dir / "bert_vs_lmmd", formats, dpi)
            plt.close(fig)

    return annual


__all__ = ["generate_financial_bert_plots"]
