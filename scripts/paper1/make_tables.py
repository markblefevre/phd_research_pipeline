from pathlib import Path
import pandas as pd

# ================= CONFIG =================
RUN_ID = "1"
WINDOW_MAIN = (0, 1)

ROOT = Path(__file__).resolve().parents[2]
IN_PATH = ROOT / f"outputs/paper1/{RUN_ID}/event_study/regression_summary.csv"
OUT_DIR = ROOT / f"outputs/paper1/{RUN_ID}/tables"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ================= LOAD =================
df_all = pd.read_csv(IN_PATH)

# ================= HELPERS =================
def stars(p):
    if pd.isna(p): return ""
    if p < 0.01: return "***"
    elif p < 0.05: return "**"
    elif p < 0.1: return "*"
    return ""

def fmt_coef(beta, p):
    if pd.isna(beta): return ""
    return f"{beta:.4f}{stars(p)}"

def fmt_se(se):
    if pd.isna(se): return ""
    return f"({se:.4f})"

def build_table(df, spec_order, variables, filename):
    df = df[df["spec"].isin(spec_order)].copy()

    missing = [s for s in spec_order if s not in df["spec"].unique()]
    if missing:
        raise ValueError(f"Missing specs: {missing}")

    df["spec"] = pd.Categorical(df["spec"], categories=spec_order, ordered=True)
    df = df.sort_values("spec")

    table = pd.DataFrame()

    for i, (_, row) in enumerate(df.iterrows()):
        col = f"({i+1})"
        col_data = []

        for _, b, s, p in variables:
            col_data.append(fmt_coef(row.get(b), row.get(p)))
            col_data.append(fmt_se(row.get(s)))

        col_data += [
            "Yes" if row["year_fe"] else "No",
            "Yes" if row["industry_fe"] else "No",
            "Yes" if "Controls" in row["spec"] else "No",
            f"{int(row['nobs'])}",
            f"{row['r2']:.3f}",
        ]

        table[col] = col_data

    # index
    idx = []
    for name, *_ in variables:
        idx += [name, " "]
    idx += ["Year FE", "Industry FE", "Controls", "N", "R²"]

    table.index = idx

    # save CSV
    csv_path = OUT_DIR / f"{filename}.csv"
    table.to_csv(csv_path)

    # save TEX
    tex = table.to_latex(
        escape=False,
        na_rep="",
        column_format="l" + "c"*table.shape[1],
    )

    tex = (
        "\\begin{table}[htbp]\n\\centering\n"
        f"\\caption{{{filename.replace('_',' ').title()}}}\n"
        f"\\label{{tab:{filename}}}\n"
        + tex +
        "\\begin{flushleft}\n"
        "\\footnotesize Notes: Standard errors in parentheses. "
        "*** p<0.01, ** p<0.05, * p<0.1. "
        "Standard errors clustered at firm level.\n"
        "\\end{flushleft}\n\\end{table}"
    )

    tex_path = OUT_DIR / f"{filename}.tex"
    with open(tex_path, "w") as f:
        f.write(tex)

    print(f"[OK] {filename}")

# ================= FILTER MAIN WINDOW =================
df_main = df_all[
    (df_all["window_start"] == WINDOW_MAIN[0]) &
    (df_all["window_end"] == WINDOW_MAIN[1])
]

# ================= VARIABLES =================
vars_main = [
    ("GPT (z)", "beta_gpt_z", "se_gpt_z", "p_gpt_z"),
    ("LMMD (z)", "beta_lmmd_z", "se_lmmd_z", "p_lmmd_z"),
]

# =====================================================
# TABLE 1: MAIN RESULT
# =====================================================
spec_main = [
    "GPT",
    "LMMD+YearFE+IndFE",
    "GPT+LMMD",
    "GPT+LMMD+YearFE+IndFE",
]

build_table(df_main, spec_main, vars_main, "table_main")

# =====================================================
# TABLE 2: GPT vs LMMD (CLEAN COMPARISON)
# =====================================================
spec_compare = [
    "GPT+Controls+YearFE+IndFE",
    "LMMD+Controls+YearFE+IndFE",
]

build_table(df_main, spec_compare, vars_main, "table_compare")

# =====================================================
# TABLE 3: WINDOWS (LMMD only)
# =====================================================
windows = [(0,0),(0,1),(-1,1),(-2,2),(-3,3)]

rows = []

for w0, w1 in windows:
    d = df_all[
        (df_all["window_start"] == w0) &
        (df_all["window_end"] == w1) &
        (df_all["spec"] == "LMMD+Controls+YearFE+IndFE")
    ]

    if len(d) == 0:
        continue

    r = d.iloc[0]

    rows.append({
        "Window": f"({w0},{w1})",
        "LMMD": fmt_coef(r["beta_lmmd_z"], r["p_lmmd_z"]),
        "SE": fmt_se(r["se_lmmd_z"]),
        "R²": f"{r['r2']:.3f}",
    })

df_win = pd.DataFrame(rows).set_index("Window")

df_win.to_csv(OUT_DIR / "table_windows.csv")

df_win_tex = df_win.to_latex(escape=False)

with open(OUT_DIR / "table_windows.tex", "w") as f:
    f.write(df_win_tex)

print("[OK] table_windows")

# =====================================================
# TABLE 4: DISAGREEMENT
# =====================================================
vars_dis = [
    ("Disagreement", "beta_disagreement", "se_disagreement", "p_disagreement"),
    ("LMMD (z)", "beta_lmmd_z", "se_lmmd_z", "p_lmmd_z"),
]

spec_dis = [
    "DISAGREE+Controls+YearFE+IndFE",
    "LMMD+DISAGREE+Controls+YearFE+IndFE",
]

build_table(df_main, spec_dis, vars_dis, "table_disagreement")

print("\nALL TABLES GENERATED.")