from pathlib import Path
import pandas as pd

# ================= CONFIG =================
RUN_ID = "1"
WINDOW_MAIN = (-1, 1)

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

def describe_columns(spec_order):
    desc = []
    for i, s in enumerate(spec_order, 1):
        if "GPT+LMMD" in s:
            desc.append(f"({i}) GPT and LMMD jointly")
        elif "GPT" in s and "LMMD" not in s:
            desc.append(f"({i}) GPT sentiment only")
        elif "LMMD" in s and "GPT" not in s:
            desc.append(f"({i}) LMMD sentiment only")
        else:
            desc.append(f"({i}) {s}")
    return ", ".join(desc)

def get_any(row, candidates):
    """Return the first available value from row for a column name or list of names."""
    if isinstance(candidates, str):
        candidates = [candidates]
    for c in candidates:
        if c in row.index:
            return row.get(c)
    return pd.NA

def missing_variable_columns(df, variables):
    """Identify variables for which none of the candidate beta/se/p columns exist."""
    missing = []
    for name, b, s, p in variables:
        for label, candidates in [("beta", b), ("se", s), ("p", p)]:
            if isinstance(candidates, str):
                candidates = [candidates]
            if not any(c in df.columns for c in candidates):
                missing.append((name, label, candidates))
    return missing

def build_table(df, spec_order, variables, filename, caption, show_control_coeffs=False):
    df = df[df["spec"].isin(spec_order)].copy()

    missing = [s for s in spec_order if s not in df["spec"].unique()]
    if missing:
        raise ValueError(f"Missing specs: {missing}")

    df["spec"] = pd.Categorical(df["spec"], categories=spec_order, ordered=True)
    df = df.sort_values("spec")

    # Add actual control coefficients only for tables that explicitly request them.
    # Candidate column names make the script robust to slightly different naming
    # conventions in regression_summary.csv. If your summary uses different names,
    # add them to the lists below.
    if show_control_coeffs:
        control_variables = [
            (
                "Log Market Cap",
                [
                    "beta_ln_market_cap",
                    "beta_log_mktcap_z", "beta_log_market_cap_z", "beta_log_mkt_cap_z",
                    "beta_mktcap_z", "beta_size_z", "beta_log_size_z",
                ],
                [
                    "se_ln_market_cap",
                    "se_log_mktcap_z", "se_log_market_cap_z", "se_log_mkt_cap_z",
                    "se_mktcap_z", "se_size_z", "se_log_size_z",
                ],
                [
                    "p_ln_market_cap",
                    "p_log_mktcap_z", "p_log_market_cap_z", "p_log_mkt_cap_z",
                    "p_mktcap_z", "p_size_z", "p_log_size_z",
                ],
            ),
            (
                "Return Volatility",
                [
                    "beta_ln_volatility",
                    "beta_return_volatility_z", "beta_pre_event_volatility_z",
                    "beta_log_volatility_z", "beta_volatility_z", "beta_ret_vol_z",
                    "beta_log_ret_vol_z",
                ],
                [
                    "se_ln_volatility",
                    "se_return_volatility_z", "se_pre_event_volatility_z",
                    "se_log_volatility_z", "se_volatility_z", "se_ret_vol_z",
                    "se_log_ret_vol_z",
                ],
                [
                    "p_ln_volatility",
                    "p_return_volatility_z", "p_pre_event_volatility_z",
                    "p_log_volatility_z", "p_volatility_z", "p_ret_vol_z",
                    "p_log_ret_vol_z",
                ],
            ),
        ]
        variables = variables + control_variables

        missing_cols = missing_variable_columns(df, control_variables)
        if missing_cols:
            msg = "\n".join(
                f"  - {name} {label}: tried {candidates}"
                for name, label, candidates in missing_cols
            )
            raise ValueError(
                "Could not find one or more control coefficient columns in regression_summary.csv.\n"
                "Update the candidate column names in control_variables. Missing:\n" + msg
            )

    table = pd.DataFrame()

    for i, (_, row) in enumerate(df.iterrows()):
        col = f"({i+1})"
        col_data = []

        for _, b, s, p in variables:
            col_data.append(fmt_coef(get_any(row, b), get_any(row, p)))
            col_data.append(fmt_se(get_any(row, s)))

        col_data += [
            "Yes" if row["year_fe"] else "No",
            "Yes" if row["industry_fe"] else "No",
        ]

        if not show_control_coeffs:
            col_data.append("Yes" if "Controls" in row["spec"] else "No")

        col_data += [
            f"{int(row['nobs'])}",
            f"{row['r2']:.3f}",
        ]

        table[col] = col_data

    # index
    idx = []
    for name, *_ in variables:
        idx += [name, " "]
    idx += ["Year FE", "Industry FE"]
    if not show_control_coeffs:
        idx += ["Controls"]
    idx += ["N", "R²"]

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

    lines = tex.splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("Year FE"):
            lines.insert(i, r"\midrule")
            break
    tex = "\n".join(lines)

    col_desc = describe_columns(spec_order)
    
    # --- Detect if controls are used ---
    has_controls = any("Controls" in s for s in spec_order)
    
    # --- Build notes dynamically ---
    notes = (
        "\\footnotesize Notes: Standard errors in parentheses. "
        "The dependent variable is CAR multiplied by 100, so coefficients are in percentage points. "
        "*** $p<0.01$, ** $p<0.05$, * $p<0.1$. "
        "Standard errors clustered at firm level. "
    )
    
    if has_controls:
        notes += "All specifications include controls for log market capitalization and return volatility. "
    else:
        notes += "No additional control variables are included. "
    
    notes += f"Columns {col_desc}, respectively.\n"

    tex = (
        "\\begin{table}[htbp]\n\\centering\n"
        f"\\caption{{{caption}}}\n"
        f"\\label{{tab:{filename}}}\n"
        + tex +
        "\\begin{flushleft}\n"
        + notes +
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
# TABLE 5: MAIN RESULT
# =====================================================
spec_main = [
    "GPT+YearFE+IndFE",
    "LMMD+YearFE+IndFE",
    "GPT+LMMD+YearFE+IndFE",
]

build_table(df_main, spec_main, vars_main, "car_sentiment_main",
            "Market Reaction to Disclosure Sentiment, CAR[-1,1] in Percentage Points")

# =====================================================
# TABLE 6: MAIN RESULT WITH CONTROLS
# ==================================================== = [
spec_controls = [
    "GPT+Controls+YearFE+IndFE",
    "LMMD+Controls+YearFE+IndFE",
    "GPT+LMMD+Controls+YearFE+IndFE",
]

build_table(
    df_main,
    spec_controls,
    vars_main,
    "car_sentiment_controls",
    "Market Reaction to Disclosure Sentiment with Controls, CAR[-1,1] in Percentage Points",
    show_control_coeffs=True,
)

# =====================================================
# TABLE 2: GPT vs LMMD (CLEAN COMPARISON)
# =====================================================
spec_compare = [
    "GPT+Controls+YearFE+IndFE",
    "LMMD+Controls+YearFE+IndFE",
]

build_table(df_main, spec_compare, vars_main, "table_compare",
            "Table Compare")

# =====================================================
# TABLE 3: WINDOWS (GPT vs LMMD vs JOINT)
# =====================================================
windows = [(0,0),(0,1),(-1,1),(-2,2),(-3,3)]

rows = []

for w0, w1 in windows:
    row = {"Window": f"({w0},{w1})"}

    df_w = df_all[
        (df_all["window_start"] == w0) &
        (df_all["window_end"] == w1)
    ]

    # --- GPT ---
    d_gpt = df_w[df_w["spec"] == "GPT+YearFE+IndFE"]
    if not d_gpt.empty:
        r = d_gpt.iloc[0]
        row["GPT"] = f"{fmt_coef(r['beta_gpt_z'], r['p_gpt_z'])} {fmt_se(r['se_gpt_z'])}"
    else:
        row["GPT"] = ""

    # --- LMMD ---
    d_lmmd = df_w[df_w["spec"] == "LMMD+YearFE+IndFE"]
    if not d_lmmd.empty:
        r = d_lmmd.iloc[0]
        row["LMMD"] = f"{fmt_coef(r['beta_lmmd_z'], r['p_lmmd_z'])} {fmt_se(r['se_lmmd_z'])}"
    else:
        row["LMMD"] = ""

    # --- JOINT ---
    d_joint = df_w[df_w["spec"] == "GPT+LMMD+YearFE+IndFE"]
    if not d_joint.empty:
        r = d_joint.iloc[0]
        row["Joint"] = (
            f"GPT: {fmt_coef(r['beta_gpt_z'], r['p_gpt_z'])} {fmt_se(r['se_gpt_z'])}; "
            f"LMMD: {fmt_coef(r['beta_lmmd_z'], r['p_lmmd_z'])} {fmt_se(r['se_lmmd_z'])}"
        )
    else:
        row["Joint"] = ""

    rows.append(row)

df_win = pd.DataFrame(rows)

# --- SAVE CSV ---
df_win.to_csv(OUT_DIR / "car_sentiment_windows.csv", index=False)

# --- LATEX ---
tex = df_win.to_latex(
    index=False,
    escape=False,
    column_format="lccc",
)

tex = (
    "\\begin{table}[htbp]\n\\centering\n"
    "\\caption{Sentiment Effects Across Event Windows, CAR in Percentage Points}\n"
    "\\label{tab:car_sentiment_windows}\n"
    + tex +
    "\\begin{flushleft}\n"
    "\\footnotesize Notes: Each cell reports coefficient(s) and standard error(s) "
    "from regressions including year and industry fixed effects. "
    "The dependent variable is CAR multiplied by 100, so coefficients are in percentage points. "
    "*** $p<0.01$, ** $p<0.05$, * $p<0.1$.\n"
    "\\end{flushleft}\n\\end{table}"
)

with open(OUT_DIR / "car_sentiment_windows.tex", "w") as f:
    f.write(tex)

print("[OK] car_sentiment_windows")
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

build_table(df_main, spec_dis, vars_dis, "table_disagreement",
            "Table Disagreement")

print("\nALL TABLES GENERATED.")