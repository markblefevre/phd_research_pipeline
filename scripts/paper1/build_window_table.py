from pathlib import Path
import pandas as pd

# This file lives at: root/scripts/paper1/build_window_table.py
ROOT = Path(__file__).resolve().parents[2]

INPUT_CSV = ROOT / "outputs" / "paper1" / "1" / "event_study" / "regression_summary.csv"
OUTPUT_DIR = ROOT / "outputs" / "paper1" / "1" / "tables"

WINDOWS = [(-1, 1), (0, 0), (0, 1), (-2, 2), (-3, 3)]
FE_ROWS = [
    ("NoYear+NoInd", ""),
    ("NoYear+Ind", "+IndFE"),
    ("Year+NoInd", "+YearFE"),
    ("Year+Ind", "+YearFE+IndFE"),
]


def stars(p):
    if pd.isna(p):
        return ""
    if p < 0.01:
        return "***"
    if p < 0.05:
        return "**"
    if p < 0.10:
        return "*"
    return ""


def fmt_bps(x):
    if pd.isna(x):
        return ""
    return f"{x * 10000:.2f}"


def fmt_r2(x):
    if pd.isna(x):
        return ""
    return f"{x:.3f}"


def coef_cell(coef, se, p):
    return f"{fmt_bps(coef)}{stars(p)} ({fmt_bps(se)})"


def window_label(w):
    a, b = w
    return f"$[{a},{b}]$"


def get_row(df, spec, window):
    ws, we = window
    sub = df[
        (df["spec"] == spec)
        & (df["window_start"] == ws)
        & (df["window_end"] == we)
    ]
    if len(sub) != 1:
        raise ValueError(f"Expected exactly one row for spec={spec}, window={window}, found {len(sub)}")
    return sub.iloc[0]


def build_single_factor_table(df, *, base_spec, coef_col, se_col, p_col, caption, label, out_name, measure_name):
    lines = []
    lines.append(r"\begin{table}[p]")
    lines.append(r"\centering")
    lines.append(rf"\caption{{{caption}}}")
    lines.append(rf"\label{{{label}}}")
    lines.append(r"\scriptsize")
    lines.append(r"\setlength{\tabcolsep}{4pt}")
    lines.append(r"\renewcommand{\arraystretch}{1.1}")
    lines.append(r"\begin{tabular}{l l r r l}")
    lines.append(r"\toprule")
    lines.append(rf"Window & FE & N & $R^{{2}}$ & {measure_name} \\")
    lines.append(r"\midrule")

    for i, w in enumerate(WINDOWS):
        lines.append(rf"\multicolumn{{5}}{{l}}{{\textit{{Panel: {window_label(w)}}}}} \\")
        lines.append(r"\addlinespace[1pt]")

        for fe_label, suffix in FE_ROWS:
            spec = f"{base_spec}{suffix}"
            row = get_row(df, spec, w)
            coef_txt = coef_cell(row[coef_col], row[se_col], row[p_col])
            lines.append(
                f"{window_label(w)} & {fe_label} & {int(row['nobs'])} & {fmt_r2(row['r2'])} & {coef_txt} \\\\"
            )

        if i < len(WINDOWS) - 1:
            lines.append(r"\addlinespace[2pt]")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(
        r"\parbox{0.95\linewidth}{\footnotesize Notes: Each row reports a cross-sectional regression of CAR (in bps) on standardized sentiment measures for the stated event window and fixed-effects structure. Coefficients are scaled by 10,000. Standard errors clustered by firm in parentheses. *, **, *** denote significance at 10\%, 5\%, and 1\%.}"
    )
    lines.append(r"\end{table}")
    lines.append("")

    out_path = OUTPUT_DIR / out_name
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote: {out_path}")


def build_joint_table(df):
    caption = "Appendix: Joint specification regressions (GPT and LMMD)"
    label = "tab:app_joint_spec"
    out_name = "appendix_table_joint_spec.tex"

    lines = []
    lines.append(r"\begin{table}[p]")
    lines.append(r"\centering")
    lines.append(rf"\caption{{{caption}}}")
    lines.append(rf"\label{{{label}}}")
    lines.append(r"\scriptsize")
    lines.append(r"\setlength{\tabcolsep}{4pt}")
    lines.append(r"\renewcommand{\arraystretch}{1.1}")
    lines.append(r"\begin{tabular}{l l r r l l}")
    lines.append(r"\toprule")
    lines.append(r"Window & FE & N & $R^{2}$ & GPT & LMMD \\")
    lines.append(r"\midrule")

    for i, w in enumerate(WINDOWS):
        lines.append(rf"\multicolumn{{6}}{{l}}{{\textit{{Panel: {window_label(w)}}}}} \\")
        lines.append(r"\addlinespace[1pt]")

        for fe_label, suffix in FE_ROWS:
            spec = f"GPT+LMMD{suffix}"
            row = get_row(df, spec, w)
            gpt_txt = coef_cell(row["beta_gpt_z"], row["se_gpt_z"], row["p_gpt_z"])
            lmmd_txt = coef_cell(row["beta_lmmd_z"], row["se_lmmd_z"], row["p_lmmd_z"])
            lines.append(
                f"{window_label(w)} & {fe_label} & {int(row['nobs'])} & {fmt_r2(row['r2'])} & {gpt_txt} & {lmmd_txt} \\\\"
            )

        if i < len(WINDOWS) - 1:
            lines.append(r"\addlinespace[2pt]")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(
        r"\parbox{0.95\linewidth}{\footnotesize Notes: Each row reports a cross-sectional regression of CAR (in bps) on standardized sentiment measures for the stated event window and fixed-effects structure. Coefficients are scaled by 10,000. Standard errors clustered by firm in parentheses. *, **, *** denote significance at 10\%, 5\%, and 1\%.}"
    )
    lines.append(r"\end{table}")
    lines.append("")

    out_path = OUTPUT_DIR / out_name
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote: {out_path}")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(INPUT_CSV)

    build_single_factor_table(
        df,
        base_spec="GPT",
        coef_col="beta_gpt_z",
        se_col="se_gpt_z",
        p_col="p_gpt_z",
        caption="Appendix: GPT sentiment and CARs (specification grid)",
        label="tab:app_gpt_only",
        out_name="appendix_table_gpt_only.tex",
        measure_name="GPT",
    )

    build_single_factor_table(
        df,
        base_spec="LMMD",
        coef_col="beta_lmmd_z",
        se_col="se_lmmd_z",
        p_col="p_lmmd_z",
        caption="Appendix: LMMD sentiment and CARs (specification grid)",
        label="tab:app_lmmd_only",
        out_name="appendix_table_lmmd_only.tex",
        measure_name="LMMD",
    )

    build_joint_table(df)

    print(f"Read:  {INPUT_CSV}")
    print(f"Wrote tables to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()