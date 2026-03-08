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
    # src/event_study/run_event_study_horserace.py -> parents[2] = repo root
    return Path(__file__).resolve().parents[2]


def run_horserace(
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
    - merges with sentiment panel
    - runs regression grid
    - writes horserace_summary.csv to the same event_study output directory
    """

    if windows is None:
        windows = [(0, 0), (0, 1), (-1, 1), (-2, 2), (-3, 3)]

    if run_id is None:
        # allow standalone runs; pipeline should pass run_id explicitly
        run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    # ---- Output + CAR input directory (NEW convention) ----
    out_dir = car_dir or (repo_root() / "outputs" / paper / run_id / "event_study")
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- Panel input (LEGACY for now) ----
    if panel_csv is None:
        panel_csv = repo_root() / "data" / "curated" / paper / "panel" / "mdna_summary_nikkei225_with_lmmd.csv"

    # Load event-level panel with all sentiments
    panel = pd.read_csv(panel_csv)
    panel = panel.rename(columns={"symbol": "Ticker", "filing_date": "EventDate"})
    panel["EventDate"] = pd.to_datetime(panel["EventDate"], errors="coerce")

    # Attach industry (uses legacy data_root mapping for now)
    panel = attach_ticker_industry(panel, get_project_root(), ticker_col="Ticker", label="both")

    rows = []

    for w0, w1 in windows:
        # CAR is sentiment-invariant; we use document_score file if it exists
        car_path = out_dir / f"car_results_all_{w0}_{w1}_document_score.csv"
        if not car_path.exists():
            raise FileNotFoundError(
                f"Missing CAR file for window ({w0},{w1}). Expected: {car_path}\n"
                f"Tip: run event study first for document_score with same paper/run_id."
            )

        car = pd.read_csv(car_path)
        car["EventDate"] = pd.to_datetime(car["EventDate"], errors="coerce")

        df = car.merge(
            panel[
                ["Ticker", "EventDate", "document_score", "lmmd_net", "neg_rate", "pos_rate", "industry"]
            ],
            on=["Ticker", "EventDate"],
            how="inner",
        )

        # Ensure numeric
        df["CAR"] = pd.to_numeric(df["CAR"], errors="coerce")
        for c in ["document_score", "lmmd_net", "neg_rate", "pos_rate"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["CAR", "document_score", "lmmd_net", "neg_rate", "pos_rate"])

        # Z-score within regression sample (for comparability)
        df["gpt_z"] = zscore(df["document_score"])
        df["lmmd_z"] = zscore(df["lmmd_net"])
        df["neg_z"] = zscore(df["neg_rate"])
        df["pos_z"] = zscore(df["pos_rate"])

        specs = {
            "GPT": ["gpt_z"],
            "LMMD": ["lmmd_z"],
            "GPT+LMMD": ["gpt_z", "lmmd_z"],
            "LMMD_Neg": ["neg_z"],
            "LMMD_Pos": ["pos_z"],
            "LMMD_PosNeg": ["neg_z", "pos_z"],
            "GPT+Neg+Pos": ["gpt_z", "neg_z", "pos_z"],
        }

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
    res_out = out_dir / "horserace_summary.csv"
    res.to_csv(res_out, index=False, encoding="utf-8-sig")
    print(f"[INFO] Saved: {res_out} ({len(res)} rows)")
    return res


if __name__ == "__main__":
    # Standalone execution (pipeline should pass run_id explicitly)
    run_horserace()
