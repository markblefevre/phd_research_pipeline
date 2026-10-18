#!/usr/bin/env python3
"""Stage 8: initial associations between novel/persistent sentiment and filing CAR.

Non-destructive. Baseline year FE + firm-clustered SE; no firm FE or causal claims.
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
import statsmodels.formula.api as smf

WINDOWS = {"0_1": "car_pp_0_1", "0_0": "car_pp_0_0", "m1_1": "car_pp_m1_1"}
PREDICTORS = ["persistentSentiment", "novelSentiment"]
CONTROLS = ["noveltyCNum", "logLengthChange"]


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def clean_input(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, low_memory=False, dtype={"prev_docID": "string", "curr_docID": "string", "edinetCode": "string"})
    required = ["prev_docID", "curr_docID", "edinetCode", "fiscalYear", *PREDICTORS,
                "novelMinusPersistentSentiment", *CONTROLS,
                *WINDOWS.values(), *(f"eventStudySample_{w}" for w in WINDOWS)]
    missing = sorted(set(required) - set(frame))
    if missing:
        raise ValueError(f"Missing panel columns: {missing}")
    if frame[["prev_docID", "curr_docID"]].isna().any().any() or frame.duplicated(["prev_docID", "curr_docID"]).any():
        raise ValueError("Panel pair keys must be non-null and unique")
    if frame["edinetCode"].isna().any():
        raise ValueError("Firm identifier missing")
    for name in [*PREDICTORS, "novelMinusPersistentSentiment", *CONTROLS, *WINDOWS.values()]:
        frame[name] = pd.to_numeric(frame[name], errors="raise").replace([np.inf, -np.inf], np.nan)
    for name in PREDICTORS:
        invalid = frame[name].notna() & ~frame[name].between(-1.000001, 1.000001)
        if invalid.any():
            raise ValueError(f"{name} out of [-1, 1] on {invalid.sum()} rows")
    expected = frame["novelSentiment"] - frame["persistentSentiment"]
    mismatch = (expected.notna() & frame["novelMinusPersistentSentiment"].notna() &
                ~np.isclose(expected, frame["novelMinusPersistentSentiment"], atol=1e-8))
    if mismatch.any():
        raise ValueError(f"Sentiment difference mismatch on {mismatch.sum()} rows")
    for window, car in WINDOWS.items():
        status = frame[f"eventStudySample_{window}"].astype("string").str.strip().str.lower()
        if not status.dropna().isin(["true", "false", "1", "0"]).all():
            raise ValueError(f"Unrecognized eventStudySample_{window} values")
        frame[f"_valid_{window}"] = status.isin(["true", "1"])
        if (frame[f"_valid_{window}"] & frame[car].isna()).any():
            raise ValueError(f"{window}: valid event with missing CAR")
        if ((~frame[f"_valid_{window}"]) & frame[car].notna()).any():
            raise ValueError(f"{window}: CAR present for excluded event")
    frame["fiscalYear"] = pd.to_numeric(frame["fiscalYear"], errors="raise")
    return frame


def run_model(frame: pd.DataFrame, window: str, controlled: bool) -> tuple[dict, pd.DataFrame, str]:
    car = WINDOWS[window]
    regressors = PREDICTORS + (CONTROLS if controlled else [])
    columns = [car, "edinetCode", "fiscalYear", *regressors]
    sample = frame.loc[frame[f"_valid_{window}"], columns].dropna().copy()
    if len(sample) < 100 or sample["edinetCode"].nunique() < 2:
        raise ValueError(f"Insufficient observations for {window} / controlled={controlled}")
    sample["fiscalYear"] = sample["fiscalYear"].astype(int).astype(str)
    formula = f"{car} ~ persistentSentiment + novelSentiment"
    if controlled:
        formula += " + noveltyCNum + logLengthChange"
    formula += " + C(fiscalYear)"
    fit = smf.ols(formula, data=sample).fit(cov_type="cluster", cov_kwds={
        "groups": sample["edinetCode"].astype(str), "use_correction": True,
    })
    # H0: novel coefficient equals persistent coefficient.
    test = fit.t_test("novelSentiment - persistentSentiment = 0")
    model_name = f"{window}_{'controlled' if controlled else 'baseline'}"
    info = {
        "model": model_name, "window": window, "controls": controlled,
        "formula": formula, "observations": int(fit.nobs),
        "firms": int(sample["edinetCode"].nunique()),
        "years": sorted(sample["fiscalYear"].unique().tolist()),
        "rSquared": float(fit.rsquared), "adjRSquared": float(fit.rsquared_adj),
        "testNovelEqualsPersistent": {
            "estimateNovelMinusPersistent": float(fit.params["novelSentiment"] - fit.params["persistentSentiment"]),
            "standardError": float(np.asarray(test.sd).item()),
            "tStatistic": float(np.asarray(test.tvalue).item()),
            "pValue": float(np.asarray(test.pvalue).item()),
        },
    }
    rows = []
    for name in regressors:
        ci = fit.conf_int().loc[name]
        rows.append({
            "model": model_name, "term": name, "coefficient": float(fit.params[name]),
            "stdErrorClusterFirm": float(fit.bse[name]), "tStatistic": float(fit.tvalues[name]),
            "pValue": float(fit.pvalues[name]), "ci95Lower": float(ci.iloc[0]),
            "ci95Upper": float(ci.iloc[1]), "n": int(fit.nobs),
            "firms": int(sample["edinetCode"].nunique()), "rSquared": float(fit.rsquared),
        })
    return info, pd.DataFrame(rows), fit.summary().as_text()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="~/paper2_local")
    parser.add_argument("--panel", help="Explicit Stage 8 panel CSV")
    parser.add_argument("--output-dir", help="New output directory; existing output files are never replaced")
    args = parser.parse_args()
    root = Path(args.root).expanduser().resolve()
    panel = Path(args.panel).expanduser().resolve() if args.panel else root / "data/interim/paper2/analysis/stage8_panel/stage8_filing_panel.csv"
    output = Path(args.output_dir).expanduser().resolve() if args.output_dir else root / "data/interim/paper2/analysis/stage8_regressions_baseline"
    targets = [output / "baseline_coefficients.csv", output / "baseline_models.json", output / "baseline_model_summaries.txt"]
    try:
        if not panel.is_file():
            raise FileNotFoundError(panel)
        existing = [str(p) for p in targets if p.exists()]
        if existing:
            raise FileExistsError(f"Refusing to overwrite: {existing}. Supply a new --output-dir.")
        frame = clean_input(panel)
        models = []
        coefficients = []
        summaries = []
        for window in WINDOWS:
            for controlled in (False, True):
                info, coef, summary = run_model(frame, window, controlled)
                models.append(info)
                coefficients.append(coef)
                summaries.append(f"{'=' * 85}\n{info['model']}\n{summary}\n")
        audit = {
            "createdUtc": datetime.now(timezone.utc).isoformat(),
            "panelFile": str(panel), "panelSha256": digest(panel),
            "panelRows": len(frame), "models": models,
            "notes": [
                "Exploratory associations, not causal identification.",
                "CAR dependent variables are in percentage points; sentiment regressors are [-1,1].",
                "OLS with fiscal-year fixed effects and SE clustered by edinetCode (firm).",
                "No firm fixed effects, economic controls, or multiple-testing correction yet.",
                "Controls: existing noveltyCNum and logLengthChange; do not treat as exogenous.",
                "Primary exploratory window [0,+1]; other windows are sensitivity checks.",
                "Nine empty-novel observations are omitted by complete-case filtering.",
                "Historical-venue TSE_UNSPECIFIED review is not certified complete by this script.",
            ],
        }
        output.mkdir(parents=True, exist_ok=True)
        with targets[0].open("x", encoding="utf-8", newline="") as f:
            pd.concat(coefficients, ignore_index=True).to_csv(f, index=False)
        with targets[1].open("x", encoding="utf-8") as f:
            json.dump(audit, f, indent=2, ensure_ascii=False)
            f.write("\n")
        with targets[2].open("x", encoding="utf-8") as f:
            f.write("\n".join(summaries))
        print("Created:")
        for p in targets:
            print(f"  {p}")
        print("\nBASELINE RESULTS (year FE, firm-clustered SE):")
        for info in models:
            if info["controls"]:
                continue
            df = next(x for x in coefficients if x["model"].iloc[0] == info["model"])
            p = df.set_index("term")
            print(f"  {info['window']}: n={info['observations']}, firms={info['firms']}, "
                  f"betaP={p.loc['persistentSentiment','coefficient']:.5f} (p={p.loc['persistentSentiment','pValue']:.4g}), "
                  f"betaN={p.loc['novelSentiment','coefficient']:.5f} (p={p.loc['novelSentiment','pValue']:.4g}), "
                  f"p(betaN=betaP)={info['testNovelEqualsPersistent']['pValue']:.4g}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
