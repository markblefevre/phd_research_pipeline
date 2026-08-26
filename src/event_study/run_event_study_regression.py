#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional

import pandas as pd
import statsmodels.api as sm

# ---- Imports in new repo layout ----
# project root finder for *legacy* input data
from src.utils.project_paths import get_project_root
from src.edinet.industry import attach_ticker_industry


def zscore(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mu = s.mean()
    sd = s.std(ddof=0)
    if sd == 0 or pd.isna(sd):
        return s
    return (s - mu) / sd


def fit_cluster_ols(
    df: pd.DataFrame,
    ycol: str,
    xcols: List[str],
    cluster_col: str = "Ticker",
    add_year_fe: bool = False,
    add_industry_fe: bool = False,
):
    """
    Clustered OLS with optional Year FE and Industry FE via dummy variables.
    """
    X_parts = [df[xcols]]

    if add_year_fe:
        if "EventDate" not in df.columns:
            raise ValueError("EventDate column required for Year FE.")
        year = pd.to_datetime(df["EventDate"], errors="coerce").dt.year
        year_dummies = pd.get_dummies(year, prefix="yr", drop_first=True)
        X_parts.append(year_dummies)

    if add_industry_fe:
        if "industry" not in df.columns:
            raise ValueError("industry column required for Industry FE.")
        ind_dummies = pd.get_dummies(df["industry"], prefix="ind", drop_first=True)
        X_parts.append(ind_dummies)

    X = pd.concat(X_parts, axis=1)
    X = sm.add_constant(X)

    # Harden against object dtype
    X = X.apply(pd.to_numeric, errors="coerce")
    y = pd.to_numeric(df[ycol], errors="coerce")

    ok = ~(X.isna().any(axis=1) | y.isna())
    X = X.loc[ok].astype(float)
    y = y.loc[ok].astype(float)

    model = sm.OLS(y, X).fit(
        cov_type="cluster",
        cov_kwds={"groups": df.loc[ok, cluster_col]},
    )
    return model


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def run_regression(
    windows: Optional[List[Tuple[int, int]]] = None,
    *,
    paper: str = "paper1",
    run_id: Optional[str] = None,
    # leave legacy panel path as default for now; easy to migrate later
    panel_csv: Optional[Path] = None,
    car_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Next-stage after run_event_study_all:
    - reads CAR files from outputs/<paper>/<run_id>/event_study/
    - runs regression grid
    - writes regression_summary.csv to the same event_study output directory
    """

    if windows is None:
        windows = [(0, 0), (0, 1), (-1, 1), (-2, 2), (-3, 3)]

    if run_id is None:
        # allow standalone runs; pipeline should pass run_id explicitly
        run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    # ---- Output + CAR input directory (NEW convention) ----
    out_dir = car_dir or (repo_root() / "outputs" / paper / run_id / "event_study")
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []

    for w0, w1 in windows:
        dataset_path = out_dir / f"regression_dataset_{w0}_{w1}.csv"
        
        if not dataset_path.exists():
            raise FileNotFoundError(
                f"Missing regression dataset for window ({w0},{w1}): {dataset_path}"
            )
        
        df = pd.read_csv(dataset_path)
        if "industry" not in df.columns:
            raise ValueError("Missing 'industry' in regression dataset")

        # Express CAR in percentage points for regression output.
        # This rescales coefficients and standard errors by 100,
        # while leaving t-statistics, p-values, R², and N unchanged.
        df["CAR"] = 100 * pd.to_numeric(df["CAR"], errors="coerce")

        df["EventDate"] = pd.to_datetime(df["EventDate"], errors="coerce")

        controls = ["ln_market_cap", "ln_volatility"]

        base_specs = {
            "GPT": ["gpt_z"],
            "LMMD": ["lmmd_z"],
            "GPT+LMMD": ["gpt_z", "lmmd_z"],
            "LMMD_Neg": ["neg_z"],
            "LMMD_Pos": ["pos_z"],
            "LMMD_PosNeg": ["neg_z", "pos_z"],
            "GPT+Neg+Pos": ["gpt_z", "neg_z", "pos_z"],
        }

        # Is disagreement priced
        base_specs.update({
            "DISAGREE": ["disagreement"],
            "LMMD+DISAGREE": ["lmmd_z", "disagreement"],
        })

        specs = {}
        
        for name, cols in base_specs.items():
            specs[name] = cols
            specs[name + "+Controls"] = cols + controls

        for name, xcols in specs.items():
            for add_year_fe in (False, True):
                for add_industry_fe in (False, True):
                    suffix = []
                    if add_year_fe:
                        suffix.append("YearFE")
                    if add_industry_fe:
                        suffix.append("IndFE")
                    spec_name = name if not suffix else f"{name}+" + "+".join(suffix)

                    m = fit_cluster_ols(
                        df=df,
                        ycol="CAR",
                        xcols=xcols,
                        cluster_col="Ticker",
                        add_year_fe=add_year_fe,
                        add_industry_fe=add_industry_fe,
                    )

                    out = {
                        "paper": paper,
                        "run_id": run_id,
                        "window_start": w0,
                        "window_end": w1,
                        "spec": spec_name,
                        "year_fe": add_year_fe,
                        "industry_fe": add_industry_fe,
                        "nobs": int(m.nobs),
                        "r2": float(m.rsquared),
                    }

                    for xc in xcols:
                        out[f"beta_{xc}"] = float(m.params.get(xc, float("nan")))
                        out[f"se_{xc}"] = float(m.bse.get(xc, float("nan")))
                        out[f"p_{xc}"] = float(m.pvalues.get(xc, float("nan")))

                    rows.append(out)

    res = pd.DataFrame(rows)
    res_out = out_dir / "regression_summary.csv"
    res.to_csv(res_out, index=False, encoding="utf-8-sig")
    print(f"[INFO] Saved: {res_out} ({len(res)} rows)")
    return res

