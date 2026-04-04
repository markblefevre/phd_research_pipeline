from pathlib import Path
import pandas as pd
import math

# This file lives at: root/scripts/paper1/build_window_table.py
ROOT = Path(__file__).resolve().parents[2]

INPUT_CSV = ROOT / "outputs" / "paper1" / "1" / "event_study" / "regression_summary.csv"
OUTPUT_TEX = ROOT / "outputs" / "paper1" / "1" / "tables" / "table_lmmd_decomp.tex"

TARGET_SPEC = "LMMD_PosNeg+YearFE+IndFE"
WINDOWS = [(0, 0), (0, 1), (-1, 1), (-2, 2), (-3, 3)]

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


def cell(coef, se, p):
    return (
        rf"\begin{{tabular}}[c]{{@{{}}c@{{}}}}"
        rf"{fmt_bps(coef)}{stars(p)}\\({fmt_bps(se)})"
        rf"\end{{tabular}}"
    )

df = pd.read_csv(INPUT_CSV)

sub = df.loc[df["spec"].eq(TARGET_SPEC)].copy()

# Keep only requested windows
sub = sub[sub.apply(lambda r: (int(r["window_start"]), int(r["window_end"])) in WINDOWS, axis=1)].copy()

# Build lookup
lookup = {
    (int(r["window_start"]), int(r["window_end"])): r
    for _, r in sub.iterrows()
}

missing = [w for w in WINDOWS if w not in lookup]
if missing:
    raise ValueError(f"Missing windows for spec {TARGET_SPEC}: {missing}")

neg_cells = []
pos_cells = []
n_vals = []

for w in WINDOWS:
    r = lookup[w]
    neg_cells.append(cell(r["beta_neg_z"], r["se_neg_z"], r["p_neg_z"]))
    pos_cells.append(cell(r["beta_pos_z"], r["se_pos_z"], r["p_pos_z"]))
    n_vals.append(str(int(r["nobs"])))

# Optional sanity check: all N should match
if len(set(n_vals)) != 1:
    raise ValueError(f"Inconsistent nobs across windows: {n_vals}")

header_cols = " & ".join([rf"$[{a},{b}]$" for a, b in WINDOWS])
neg_row = " & ".join(neg_cells)
pos_row = " & ".join(pos_cells)
n_row = " & ".join(n_vals)

tex = rf"""
\begin{{table}}[htbp]
\centering
\caption{{Appendix: Decomposition of Japanese LMMD sentiment into negative and positive components}}
\label{{tab:car_sentiment_lmmd_decomp}}
\small
\begin{{tabular}}{{lccccc}}
\toprule
 & {header_cols} \\
\midrule
\multicolumn{{6}}{{l}}{{Panel D: Japanese LMMD components (negative and positive)}} \\
Negative (z) & {neg_row} \\
Positive (z) & {pos_row} \\
\addlinespace
\midrule
Year FE & Yes & Yes & Yes & Yes & Yes \\
Industry FE & Yes & Yes & Yes & Yes & Yes \\
\midrule
N & {n_row} \\
\bottomrule
\end{{tabular}}
\vspace{{2mm}}
\parbox{{0.95\linewidth}}{{\footnotesize Notes: Coefficients are reported in basis points (bps) of CAR with firm-clustered standard errors in parentheses. Sentiment measures are standardized within the regression sample. *, **, and *** denote significance at the 10\%, 5\%, and 1\% levels.}}
\end{{table}}
""".strip() + "\n"

OUTPUT_TEX.write_text(tex, encoding="utf-8")
print(f"Wrote {OUTPUT_TEX.resolve()}")