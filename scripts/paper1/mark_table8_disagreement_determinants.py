#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import warnings

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

from src.utils.project_paths import get_project_root
from src.edinet.industry import attach_ticker_industry


INPUT_CSV = Path("data/curated/paper1/panel/mdna_summary_nikkei225_with_lmmd.csv")

OUT_DIR = Path("outputs/tables")
OUT_CSV = OUT_DIR / "table8_disagreement_determinants.csv"
OUT_TEX = OUT_DIR / "table8_disagreement_determinants.tex"
OUT_TXT = OUT_DIR / "table8_disagreement_determinants.txt"
OUT_DIAG_TXT = OUT_DIR / "table8_disagreement_determinants_diagnostics.txt"
OUT_INDUSTRY_CSV = OUT_DIR / "table8_industry_disagreement_means.csv"


def stars(p: float) -> str:
    if pd.isna(p):
        return ""
    if p < 0.01:
        return "***"
    if p < 0.05:
        return "**"
    if p < 0.10:
        return "*"
    return ""


def fmt_coef_se(res, var: str) -> tuple[str, str]:
    coef = res.params.get(var, np.nan)
    se = res.bse.get(var, np.nan)
    pval = res.pvalues.get(var, np.nan)

    if pd.isna(coef):
        return "", ""

    return f"{coef:.3f}{stars(pval)}", f"({se:.3f})"


def prepare_sample(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Prepare regression sample and return:
      - cleaned dataframe
      - sample stats dict for diagnostics
    """
    out = df.copy()

    stats: dict[str, int | float] = {}
    stats["input_rows"] = len(out)

    # Industry merge is done before this function is called.
    stats["rows_missing_industry_pre_filter"] = int(out["industry"].isna().sum())

    out["filing_date"] = pd.to_datetime(out["filing_date"], errors="coerce")
    out["year"] = out["filing_date"].dt.year

    out["token_count"] = pd.to_numeric(out["token_count"], errors="coerce")
    out["sent_disagreement_abs_z"] = pd.to_numeric(
        out["sent_disagreement_abs_z"], errors="coerce"
    )
    out["gpt_valid"] = pd.to_numeric(out["gpt_valid"], errors="coerce")

    stats["rows_after_gpt_valid_filter"] = int((out["gpt_valid"] == 1).sum())
    out = out.loc[out["gpt_valid"] == 1].copy()

    # token_count must be positive for log
    out = out.loc[out["token_count"].notna() & (out["token_count"] > 0)].copy()
    out["log_token_count"] = np.log(out["token_count"])

    # final regression variables
    required = [
        "sent_disagreement_abs_z",
        "log_token_count",
        "year",
        "industry",
        "symbol",
    ]
    out = out.dropna(subset=required).copy()

    stats["final_rows"] = len(out)
    stats["n_firms"] = int(out["symbol"].nunique())
    stats["n_years"] = int(out["year"].nunique())
    stats["n_industries"] = int(out["industry"].nunique())

    return out, stats


def run_models(df: pd.DataFrame) -> list:
    formulas = [
        "sent_disagreement_abs_z ~ log_token_count",
        "sent_disagreement_abs_z ~ log_token_count + C(year)",
        "sent_disagreement_abs_z ~ log_token_count + C(industry)",
        "sent_disagreement_abs_z ~ log_token_count + C(year) + C(industry)",
    ]

    results = []
    for formula in formulas:
        res = smf.ols(formula, data=df).fit(
            cov_type="cluster",
            cov_kwds={"groups": df["symbol"]},
        )
        results.append(res)

    return results


def build_table(results: list) -> pd.DataFrame:
    rows = [
        "log(Token count)",
        "",
        "Year FE",
        "Industry FE",
        "N",
        "R-squared",
    ]

    table = pd.DataFrame(index=rows)

    for i, res in enumerate(results, start=1):
        coef, se = fmt_coef_se(res, "log_token_count")

        table[f"({i})"] = [
            coef,
            se,
            "Yes" if "C(year)" in res.model.formula else "No",
            "Yes" if "C(industry)" in res.model.formula else "No",
            f"{int(res.nobs):,}",
            f"{res.rsquared:.3f}",
        ]

    return table


def write_latex(table: pd.DataFrame, out_tex: Path) -> None:
    latex = table.to_latex(
        escape=False,
        column_format="lcccc",
        caption="Determinants of disagreement between GPT-based and LMMD-based sentiment.",
        label="tab:disagreement_determinants",
    )
    out_tex.write_text(latex, encoding="utf-8")


def make_industry_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("industry")
        .agg(
            n_obs=("industry", "size"),
            n_firms=("symbol", "nunique"),
            mean_abs_disagreement_z=("sent_disagreement_abs_z", "mean"),
        )
        .sort_values("mean_abs_disagreement_z", ascending=False)
        .reset_index()
    )
    return out


def write_model_summaries(results: list, out_txt: Path) -> None:
    with out_txt.open("w", encoding="utf-8") as f:
        for i, res in enumerate(results, start=1):
            f.write(f"=== Model ({i}) ===\n")
            f.write(res.summary().as_text())
            f.write("\n\n")


def write_diagnostics(
    df: pd.DataFrame,
    sample_stats: dict,
    industry_summary: pd.DataFrame,
    out_txt: Path,
) -> None:
    token_desc = df["token_count"].describe()
    firms_per_industry = df.groupby("industry")["symbol"].nunique().sort_values()
    small_industries = firms_per_industry.loc[firms_per_industry < 3]

    with out_txt.open("w", encoding="utf-8") as f:
        f.write("TABLE 8 DIAGNOSTICS\n")
        f.write("===================\n\n")

        f.write("Sample construction\n")
        f.write("-------------------\n")
        f.write(f"Input rows: {sample_stats['input_rows']:,}\n")
        f.write(
            f"Rows missing industry before filters: "
            f"{sample_stats['rows_missing_industry_pre_filter']:,}\n"
        )
        f.write(
            f"Rows with gpt_valid == 1 before final drops: "
            f"{sample_stats['rows_after_gpt_valid_filter']:,}\n"
        )
        f.write(f"Final regression rows: {sample_stats['final_rows']:,}\n")
        f.write(f"Unique firms: {sample_stats['n_firms']:,}\n")
        f.write(f"Unique years: {sample_stats['n_years']:,}\n")
        f.write(f"Unique industries: {sample_stats['n_industries']:,}\n\n")

        f.write("Token count summary\n")
        f.write("-------------------\n")
        for k, v in token_desc.items():
            f.write(f"{k}: {v:.3f}\n")
        f.write("\n")

        f.write("Firm clusters per industry\n")
        f.write("--------------------------\n")
        f.write(f"Min firms/industry: {int(firms_per_industry.min())}\n")
        f.write(f"Median firms/industry: {float(firms_per_industry.median()):.1f}\n")
        f.write(f"Max firms/industry: {int(firms_per_industry.max())}\n\n")

        if not small_industries.empty:
            f.write("Industries with fewer than 3 firms\n")
            f.write("----------------------------------\n")
            for industry, n_firms in small_industries.items():
                f.write(f"{industry}: {int(n_firms)} firms\n")
            f.write("\n")

        f.write("Top 10 industries by mean disagreement\n")
        f.write("--------------------------------------\n")
        top10 = industry_summary.head(10)
        for _, row in top10.iterrows():
            f.write(
                f"{row['industry']}: "
                f"mean={row['mean_abs_disagreement_z']:.3f}, "
                f"n_obs={int(row['n_obs'])}, "
                f"n_firms={int(row['n_firms'])}\n"
            )
        f.write("\n")

        f.write("Bottom 10 industries by mean disagreement\n")
        f.write("-----------------------------------------\n")
        bot10 = industry_summary.tail(10).sort_values("mean_abs_disagreement_z")
        for _, row in bot10.iterrows():
            f.write(
                f"{row['industry']}: "
                f"mean={row['mean_abs_disagreement_z']:.3f}, "
                f"n_obs={int(row['n_obs'])}, "
                f"n_firms={int(row['n_firms'])}\n"
            )
        f.write("\n")


def main() -> None:

    repo_root = Path(__file__).resolve().parents[2]
    in_csv = repo_root / INPUT_CSV
    out_dir = repo_root / OUT_DIR
    out_csv = repo_root / OUT_CSV
    out_tex = repo_root / OUT_TEX
    out_txt = repo_root / OUT_TXT
    out_diag_txt = repo_root / OUT_DIAG_TXT
    out_industry_csv = repo_root / OUT_INDUSTRY_CSV

    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_csv)

    repo_root = get_project_root()
    # Attach industry inside the script so it remains self-contained.
    df = attach_ticker_industry(
        df,
        repo_root=repo_root,
        ticker_col="symbol",
        label="en",
        create_canonical=True,
        canonical_col="industry",
    )

    df, sample_stats = prepare_sample(df)

    if df.empty:
        raise ValueError("Regression sample is empty after filters.")

    industry_summary = make_industry_summary(df)

    # Keep warnings visible, but do not fail the script.
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always")
        results = run_models(df)

    table = build_table(results)

    table.to_csv(out_csv, index=True)
    write_latex(table, out_tex)
    write_model_summaries(results, out_txt)
    write_diagnostics(df, sample_stats, industry_summary, out_diag_txt)
    industry_summary.to_csv(out_industry_csv, index=False)

    print(f"Wrote CSV: {out_csv}")
    print(f"Wrote TeX: {out_tex}")
    print(f"Wrote TXT: {out_txt}")
    print(f"Wrote diagnostics: {out_diag_txt}")
    print(f"Wrote industry summary CSV: {out_industry_csv}")

    if caught_warnings:
        print("\nWarnings captured during model estimation:")
        for w in caught_warnings:
            print(f"- {w.category.__name__}: {w.message}")


if __name__ == "__main__":
    main()