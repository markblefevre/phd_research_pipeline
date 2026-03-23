#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import pandas as pd


INPUT_CSV = Path("data/curated/paper1/panel/mdna_summary_nikkei225_with_lmmd.csv")
OUT_DIR = Path("outputs/tables")
OUT_CSV = OUT_DIR / "table7_disagreement_summary.csv"
OUT_TEX = OUT_DIR / "table7_disagreement_summary.tex"


def summarize_panel(df: pd.DataFrame) -> dict[str, float]:
    out = {}

    out["N"] = len(df)

    # Raw absolute disagreement
    out["Mean abs. disagreement (raw)"] = df["sent_disagreement_abs"].mean()
    out["Median abs. disagreement (raw)"] = df["sent_disagreement_abs"].median()
    out["P75 abs. disagreement (raw)"] = df["sent_disagreement_abs"].quantile(0.75)
    out["P90 abs. disagreement (raw)"] = df["sent_disagreement_abs"].quantile(0.90)

    # Standardized absolute disagreement
    out["Mean abs. disagreement (z)"] = df["sent_disagreement_abs_z"].mean()
    out["Median abs. disagreement (z)"] = df["sent_disagreement_abs_z"].median()
    out["P75 abs. disagreement (z)"] = df["sent_disagreement_abs_z"].quantile(0.75)
    out["P90 abs. disagreement (z)"] = df["sent_disagreement_abs_z"].quantile(0.90)

    # Conflict / classification rates
    out["Polarity conflict rate"] = df["polarity_conflict"].mean()
    out["Thresholded polarity conflict rate"] = df["polarity_conflict_thresh"].mean()
    out["Same-sign nonzero rate"] = df["same_sign_nonzero"].mean()
    out["Either-neutral threshold rate"] = df["either_neutral_thresh"].mean()

    return out


def format_table(tbl: pd.DataFrame) -> pd.DataFrame:
    formatted = tbl.copy().astype(object)

    if "N" in formatted.index:
        for col in formatted.columns:
            formatted.loc["N", col] = f"{int(round(float(tbl.loc['N', col]))):,}"

    for row in formatted.index:
        if row != "N":
            for col in formatted.columns:
                val = float(tbl.loc[row, col])
                formatted.loc[row, col] = f"{val:.3f}"

    return formatted


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV)

    full_sample = df.copy()
    gpt_valid_sample = df.loc[df["gpt_valid"] == 1].copy()

    summary = pd.DataFrame({
        "Full sample": pd.Series(summarize_panel(full_sample)),
        "GPT-valid sample": pd.Series(summarize_panel(gpt_valid_sample)),
    })

    summary.to_csv(OUT_CSV, index=True)

    formatted = format_table(summary)

    latex = formatted.to_latex(
        escape=False,
        column_format="lcc",
        caption=(
            "Distribution of disagreement between GPT-based and LMMD-based sentiment measures."
        ),
        label="tab:disagreement_summary",
    )

    OUT_TEX.write_text(latex, encoding="utf-8")

    print(f"Wrote CSV: {OUT_CSV}")
    print(f"Wrote TeX: {OUT_TEX}")


if __name__ == "__main__":
    main()