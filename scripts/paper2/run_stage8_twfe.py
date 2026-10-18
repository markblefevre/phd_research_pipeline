#!/usr/bin/env python3
"""Paper 2 Stage 8: year FE vs firm+year FE with firm-clustered SE.

M1: year FE, persistent + novel sentiment.
M2: firm and year FE, persistent + novel sentiment.
M3: firm and year FE, plus noveltyCNum and logLengthChange.
M4: M3 with dependent CAR winsorized at 1st/99th percentiles.

Same complete-case sample within each CAR window. Read-only inputs; refuses
an existing output directory. No additional LLM calls or event-study reruns.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components
from scipy.stats import t as student_t

WINDOWS = {"0_1": "car_pp_0_1", "0_0": "car_pp_0_0", "m1_1": "car_pp_m1_1"}
SENTIMENT = ["persistentSentiment", "novelSentiment"]
CONTROLS = ["noveltyCNum", "logLengthChange"]
MODEL_SPECS = (
    ("M1_yearFE", False, False, False),
    ("M2_firmYearFE", True, False, False),
    ("M3_firmYearFE_controls", True, True, False),
    ("M4_firmYearFE_controls_winsorCAR", True, True, True),
)


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_panel(path: Path) -> pd.DataFrame:
    cols = ["prev_docID", "curr_docID", "edinetCode", "fiscalYear", *SENTIMENT,
            *CONTROLS, "novelMinusPersistentSentiment", *WINDOWS.values(),
            *(f"eventStudySample_{w}" for w in WINDOWS)]
    df = pd.read_csv(path, usecols=lambda c: c in cols, low_memory=False,
                     dtype={"prev_docID": "string", "curr_docID": "string", "edinetCode": "string"})
    missing = sorted(set(cols) - set(df.columns))
    if missing:
        raise ValueError(f"Panel missing required columns: {missing}")
    if df[["prev_docID", "curr_docID", "edinetCode"]].isna().any().any():
        raise ValueError("Missing filing pair or firm identifiers")
    if df.duplicated(["prev_docID", "curr_docID"]).any():
        raise ValueError("Duplicate filing pair identifiers")
    for col in [*SENTIMENT, *CONTROLS, "novelMinusPersistentSentiment", *WINDOWS.values(), "fiscalYear"]:
        df[col] = pd.to_numeric(df[col], errors="raise").replace([np.inf, -np.inf], np.nan)
    if df["fiscalYear"].dropna().mod(1).ne(0).any():
        raise ValueError("Non-integer fiscal year")
    for col in SENTIMENT:
        bad = df[col].notna() & ~df[col].between(-1.000001, 1.000001)
        if bad.any():
            raise ValueError(f"{col}: {int(bad.sum())} values outside [-1,1]")
    expected = df["novelSentiment"] - df["persistentSentiment"]
    delta = df["novelMinusPersistentSentiment"]
    if not expected.isna().equals(delta.isna()) or (expected.notna() & ~np.isclose(expected, delta, atol=1e-8, rtol=1e-8)).any():
        raise ValueError("novelMinusPersistentSentiment mismatch")
    for w, col in WINDOWS.items():
        raw = df[f"eventStudySample_{w}"].astype("string").str.lower().str.strip()
        if not raw.dropna().isin(["true", "false", "1", "0"]).all():
            raise ValueError(f"Unrecognized eventStudySample_{w} values")
        valid = raw.isin(["true", "1"])
        if (valid & df[col].isna()).any() or ((~valid) & df[col].notna()).any():
            raise ValueError(f"{w}: event sample / CAR availability mismatch")
        df[f"_valid_{w}"] = valid
    return df


def sample_for_window(frame: pd.DataFrame, window: str) -> tuple[pd.DataFrame, dict]:
    columns = ["edinetCode", "fiscalYear", WINDOWS[window], *SENTIMENT, *CONTROLS]
    eligible = frame.loc[frame[f"_valid_{window}"], columns]
    sample = eligible.dropna().copy()
    if len(sample) < 100 or sample["edinetCode"].nunique() < 3:
        raise ValueError(f"{window}: too few complete observations")
    sample["fiscalYear"] = sample["fiscalYear"].astype(int).astype(str)
    sample["edinetCode"] = sample["edinetCode"].astype(str)
    firm_counts = sample.groupby("edinetCode").size()
    return sample, {
        "window": window,
        "stage7ValidCAR": int(len(eligible)),
        "completeCaseRows": int(len(sample)),
        "droppedMissingSentimentOrControlsOrYear": int(len(eligible) - len(sample)),
        "firms": int(sample["edinetCode"].nunique()),
        "years": sorted(sample["fiscalYear"].unique().tolist()),
        "singleObservationFirms": int((firm_counts == 1).sum()),
        "observationsInSingleObservationFirms": int(firm_counts[firm_counts == 1].sum()),
    }


def absorb(matrix: np.ndarray, groups: list[np.ndarray], tol: float = 1e-11,
           max_iter: int = 10000) -> tuple[np.ndarray, int, float]:
    """Alternating within projections, correct for unbalanced firm-year panels."""
    z = np.array(matrix, dtype=float, copy=True)
    group_info = [(g, np.bincount(g).astype(float)) for g in groups]
    if len(groups) == 1:
        g, counts = group_info[0]
        means = np.vstack([np.bincount(g, weights=z[:, j], minlength=len(counts)) / counts
                           for j in range(z.shape[1])]).T
        z -= means[g]
        return z, 1, float(np.max(np.abs(means)))
    last_max = float("inf")
    for it in range(1, max_iter + 1):
        for g, counts in group_info:
            means = np.vstack([np.bincount(g, weights=z[:, j], minlength=len(counts)) / counts
                               for j in range(z.shape[1])]).T
            z -= means[g]
        # Check that *all* absorbed-group means are approximately zero.
        last_max = 0.0
        for g, counts in group_info:
            for j in range(z.shape[1]):
                mean = np.bincount(g, weights=z[:, j], minlength=len(counts)) / counts
                last_max = max(last_max, float(np.max(np.abs(mean))))
        if last_max < tol:
            return z, it, last_max
    raise RuntimeError(f"Two-way absorption did not converge in {max_iter} iterations; max group mean {last_max}")


def fe_rank(firm: np.ndarray, year: np.ndarray, two_way: bool) -> tuple[int, int]:
    """Rank of absorbed FE design, accounting for disconnected components."""
    y = int(year.max()) + 1
    if not two_way:
        return y, 1
    f = int(firm.max()) + 1
    rows = np.concatenate([firm, f + year])
    cols = np.concatenate([f + year, firm])
    graph = coo_matrix((np.ones(len(rows)), (rows, cols)), shape=(f + y, f + y)).tocsr()
    components, _ = connected_components(graph, directed=False)
    return f + y - int(components), int(components)


def estimate(sample: pd.DataFrame, window: str, spec: tuple, audit: dict) -> tuple[dict, list[dict], str]:
    model, two_way, controlled, winsor = spec
    terms = SENTIMENT + (CONTROLS if controlled else [])
    y_raw = sample[WINDOWS[window]].to_numpy(dtype=float)
    winsor_info = None
    if winsor:
        low, high = np.quantile(y_raw, [0.01, 0.99])
        y = np.clip(y_raw, low, high)
        winsor_info = {"percentiles": [1, 99], "lowerCARpp": float(low),
                       "upperCARpp": float(high), "belowLower": int((y_raw < low).sum()),
                       "aboveUpper": int((y_raw > high).sum())}
    else:
        y = y_raw
    x = sample[terms].to_numpy(dtype=float)
    firms, firm_ids = np.unique(sample["edinetCode"].to_numpy(), return_inverse=True)
    years, year_ids = np.unique(sample["fiscalYear"].to_numpy(), return_inverse=True)
    groups = [firm_ids, year_ids] if two_way else [year_ids]
    z, iterations, absorption_error = absorb(np.column_stack([y, x]), groups)
    yw, xw = z[:, 0], z[:, 1:]
    n, k = xw.shape
    fe_dof, components = fe_rank(firm_ids, year_ids, two_way)
    rank = int(np.linalg.matrix_rank(xw))
    if rank != k:
        raise ValueError(f"{window}/{model}: regressors rank deficient ({rank}/{k})")
    residual_df = n - fe_dof - k
    if residual_df <= 0:
        raise ValueError(f"{window}/{model}: insufficient residual degrees of freedom")
    beta = np.linalg.lstsq(xw, yw, rcond=None)[0]
    residuals = yw - xw @ beta
    inv = np.linalg.inv(xw.T @ xw)
    # One-way firm-clustered sandwich with CR1 correction. Effective absorbed
    # FE rank is used in the finite-sample correction (unlike naive demeaned OLS).
    scores = xw * residuals[:, None]
    cluster_scores = np.zeros((len(firms), k), dtype=float)
    np.add.at(cluster_scores, firm_ids, scores)
    meat = cluster_scores.T @ cluster_scores
    g = len(firms)
    cr1_factor = (g / (g - 1)) * ((n - 1) / residual_df)
    vcov = (inv @ meat @ inv) * cr1_factor
    vcov = (vcov + vcov.T) / 2
    se = np.sqrt(np.diag(vcov))
    if not np.isfinite(se).all() or (se <= 0).any():
        raise ValueError(f"{window}/{model}: invalid clustered standard errors")
    # Student-t inference using G-1 cluster degrees of freedom, conservative
    # relative to statsmodels' default asymptotic-normal cluster inference.
    df_t = g - 1
    t_stats = beta / se
    pvals = 2 * student_t.sf(np.abs(t_stats), df=df_t)
    critical = float(student_t.ppf(0.975, df=df_t))
    diff_vec = np.zeros(k)
    diff_vec[terms.index("novelSentiment")] = 1
    diff_vec[terms.index("persistentSentiment")] = -1
    diff = float(diff_vec @ beta)
    diff_se = float(np.sqrt(diff_vec @ vcov @ diff_vec))
    diff_t = diff / diff_se
    diff_p = float(2 * student_t.sf(abs(diff_t), df=df_t))
    ss_total = float(yw @ yw)
    ss_resid = float(residuals @ residuals)
    within_r2 = 1 - ss_resid / ss_total if ss_total > 0 else float("nan")
    name = f"{window}_{model}"
    info = {
        "model": name, "window": window, "specification": model,
        "formula": (f"{WINDOWS[window]}{'_winsor_1_99' if winsor else ''} ~ " + " + ".join(terms)
                    + (" + firm FE + fiscal-year FE" if two_way else " + fiscal-year FE")),
        "observations": n, "firms": g, "years": years.tolist(),
        "firmFE": two_way, "yearFE": True, "controls": controlled,
        "winsorizedCAR": winsor, "winsorDetails": winsor_info,
        "withinR2": float(within_r2), "withinR2Basis": "firm-and-year" if two_way else "year",
        "feRank": fe_dof, "feGraphComponents": components,
        "regressorRank": rank, "residualDegreesOfFreedom": residual_df,
        "clusterDegreesOfFreedom": df_t, "clusterCovariance": "CR1 firm clustered",
        "clusterFiniteSampleFactor": float(cr1_factor),
        "inferenceDistribution": "Student t with firms-1 degrees of freedom",
        "absorptionIterations": iterations, "absorptionMaxGroupMean": absorption_error,
        "testNovelEqualsPersistent": {
            "estimateNovelMinusPersistent": diff, "standardError": diff_se,
            "tStatistic": float(diff_t), "pValue": diff_p,
            "ci95Lower": diff - critical * diff_se,
            "ci95Upper": diff + critical * diff_se,
        },
    }
    rows = []
    for j, term in enumerate(terms):
        rows.append({
            "window": window, "model": name, "term": term,
            "coefficient": float(beta[j]), "stdErrorClusterFirm": float(se[j]),
            "tStatistic": float(t_stats[j]), "pValue": float(pvals[j]),
            "ci95Lower": float(beta[j] - critical * se[j]),
            "ci95Upper": float(beta[j] + critical * se[j]),
            "n": n, "firms": g, "withinR2": float(within_r2),
            "firmFE": two_way, "yearFE": True, "winsorizedCAR": winsor,
        })
    lines = [name, info["formula"],
             f"N={n:,}; firms={g:,}; years={len(years)}; within-R2={within_r2:.6f}; "
             f"absorbed FE rank={fe_dof}; residual df={residual_df}",
             "Firm-clustered CR1 SE; p-values/95% CI from t(firms-1).",
             "Term                         Coefficient       SE          p-value      95% CI"]
    for row in rows:
        lines.append(f"{row['term']:<28} {row['coefficient']:>10.6f} {row['stdErrorClusterFirm']:>11.6f} "
                     f"{row['pValue']:>12.6g}   [{row['ci95Lower']:.6f}, {row['ci95Upper']:.6f}]")
    lines.append(f"H0: novel=persistent; novel-persistent={diff:.6f}; SE={diff_se:.6f}; p={diff_p:.6g}")
    if winsor_info:
        lines.append(f"CAR winsor cutoffs (pp): [{winsor_info['lowerCARpp']:.6f}, "
                     f"{winsor_info['upperCARpp']:.6f}]")
    return info, rows, "\n".join(lines)


def compare_existing_baseline(coefficients: pd.DataFrame, root: Path) -> list[dict]:
    """Reconcile M1 estimates against frozen year-FE baseline if available."""
    path = root / "data/interim/paper2/analysis/stage8_regressions_baseline/baseline_coefficients.csv"
    if not path.is_file():
        return [{"status": "baseline_coefficients.csv unavailable; M1 comparison skipped"}]
    previous = pd.read_csv(path)
    comparisons = []
    for w in WINDOWS:
        current = coefficients[(coefficients["window"] == w) &
                               (coefficients["model"] == f"{w}_M1_yearFE")].set_index("term")
        old = previous[previous["model"] == f"{w}_baseline"].set_index("term")
        for term in SENTIMENT:
            if term not in old.index or term not in current.index:
                raise ValueError(f"Missing baseline comparison term: {w}/{term}")
            delta = float(current.loc[term, "coefficient"] - old.loc[term, "coefficient"])
            old_n = int(old.loc[term, "n"])
            new_n = int(current.loc[term, "n"])
            if old_n == new_n and abs(delta) > 1e-7:
                raise ValueError(f"M1 failed frozen baseline replication: {w}/{term} "
                                 f"delta={delta}, n={new_n}")
            comparisons.append({"window": w, "term": term, "previousN": old_n,
                                "newN": new_n, "coefficientDelta": delta,
                                "status": "matched" if old_n == new_n else "sample_changed"})
    return comparisons


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", default="~/paper2_local")
    p.add_argument("--panel", help="Stage 8 panel CSV (default: under --root)")
    p.add_argument("--output-dir", help="New directory (existing directory is refused)")
    args = p.parse_args()
    root = Path(args.root).expanduser().resolve()
    panel = (Path(args.panel).expanduser().resolve() if args.panel else
             root / "data/interim/paper2/analysis/stage8_panel/stage8_filing_panel.csv")
    outdir = (Path(args.output_dir).expanduser().resolve() if args.output_dir else
              root / "data/interim/paper2/analysis/stage8_regressions_twfe")
    try:
        if not panel.is_file():
            raise FileNotFoundError(panel)
        if outdir.exists():
            raise FileExistsError(f"Refusing existing output directory: {outdir}. Choose --output-dir.")
        frame = load_panel(panel)
        info_list, coef_rows, summaries, samples = [], [], [], []
        for w in WINDOWS:
            sample, sample_info = sample_for_window(frame, w)
            samples.append(sample_info)
            for spec in MODEL_SPECS:
                info, rows, summary = estimate(sample, w, spec, sample_info)
                info_list.append(info)
                coef_rows.extend(rows)
                summaries.append(summary)
        coef = pd.DataFrame(coef_rows)
        comparison = compare_existing_baseline(coef, root)
        report = {
            "createdUtc": datetime.now(timezone.utc).isoformat(),
            "panelFile": str(panel), "panelSha256": digest(panel),
            "panelRows": len(frame), "sampleAudit": samples,
            "baselineReplication": comparison, "models": info_list,
            "notes": [
                "Exploratory association, not causal identification.",
                "CAR is in percentage points; sentiment in [-1,1].",
                "Each window uses an identical complete-case sample for M1-M4.",
                "M1 includes fiscal-year FE; M2-M4 include firm and fiscal-year FE.",
                "M3 adds noveltyCNum and logLengthChange; M4 winsorizes only the CAR outcome.",
                "Unbalanced firm/year FE are absorbed by converged alternating projections.",
                "CR1 clustered SE uses the effective absorbed FE rank, including graph components.",
                "p-values and confidence intervals use Student t(firms-1), not normal z.",
                "Within R-squared is computed after absorption and is not directly comparable across FE sets.",
                "No firm economic controls or multiple-testing adjustment; no venue/timing audit performed.",
                "No original panel or Stage 6/7 files were modified.",
            ],
        }
        comparison_rows = []
        for info in info_list:
            rows = coef[coef["model"] == info["model"]].set_index("term")
            comparison_rows.append({
                "window": info["window"], "model": info["specification"],
                "n": info["observations"], "firms": info["firms"],
                "betaPersistent": float(rows.loc["persistentSentiment", "coefficient"]),
                "pPersistent": float(rows.loc["persistentSentiment", "pValue"]),
                "betaNovel": float(rows.loc["novelSentiment", "coefficient"]),
                "pNovel": float(rows.loc["novelSentiment", "pValue"]),
                "betaNovelMinusPersistent": info["testNovelEqualsPersistent"]["estimateNovelMinusPersistent"],
                "pNovelEqualsPersistent": info["testNovelEqualsPersistent"]["pValue"],
                "withinR2": info["withinR2"],
            })
        outdir.mkdir(parents=True, exist_ok=False)
        outputs = {
            "twfe_coefficients.csv": lambda f: coef.to_csv(f, index=False),
            "twfe_comparison.csv": lambda f: pd.DataFrame(comparison_rows).to_csv(f, index=False),
            "twfe_models.json": lambda f: json.dump(report, f, indent=2, ensure_ascii=False),
            "twfe_model_summaries.txt": lambda f: f.write("\n\n".join(summaries) + "\n"),
        }
        for name, write in outputs.items():
            with (outdir / name).open("x", encoding="utf-8", newline="") as f:
                write(f)
                if name.endswith(".json"):
                    f.write("\n")
        print("Created:")
        for name in outputs:
            print(" ", outdir / name)
        print("\nSTAGE 8 YEAR FE / FIRM+YEAR FE ROBUSTNESS (CAR in percentage points):")
        for row in comparison_rows:
            print(f"  {row['window']:5} {row['model']:<32} n={row['n']:<6} "
                  f"betaP={row['betaPersistent']:+.5f} (p={row['pPersistent']:.4g}) "
                  f"betaN={row['betaNovel']:+.5f} (p={row['pNovel']:.4g}) "
                  f"p(N=P)={row['pNovelEqualsPersistent']:.4g}")
        print("\nExisting files were not overwritten. No Stage 6/7 processing was run.")
        return 0
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
