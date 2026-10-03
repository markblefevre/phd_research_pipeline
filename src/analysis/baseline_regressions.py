from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf


def _offset_label(offset: int) -> str:
    return f"m{abs(offset)}" if offset < 0 else str(offset)


def _window_label(start: int, end: int) -> str:
    return f"{_offset_label(start)}_{_offset_label(end)}"


def _validate_windows(
    windows: Sequence[Sequence[int]],
) -> list[tuple[int, int]]:
    out = []
    for raw in windows:
        if len(raw) != 2:
            raise ValueError(f"Invalid CAR window {raw!r}; expected [start, end]")
        start, end = int(raw[0]), int(raw[1])
        if start > end:
            raise ValueError(f"Invalid CAR window [{start}, {end}]")
        out.append((start, end))
    if not out:
        raise ValueError("At least one regression CAR window is required")
    return out


SENTIMENT_COLUMNS = {
    "lmmd": {
        "level": "lmmd_sentiment_curr",
        "change": "lmmd_sentiment_change",
    },
    "financial_bert": {
        "level": "financial_bert_sentiment_curr",
        "change": "financial_bert_sentiment_change",
    },
    "llm": {
        "level": "llm_sentiment_curr",
        "change": "llm_sentiment_change",
    },
}


def _fit_one(
    df: pd.DataFrame,
    *,
    dependent: str,
    sentiment_col: str,
    novelty_col: str,
    length_control: str | None,
    include_year_fixed_effects: bool,
    cluster_by_firm: bool,
    firm_col: str,
):
    needed = [dependent, sentiment_col, novelty_col]
    if length_control:
        needed.append(length_control)
    if include_year_fixed_effects:
        needed.append("fiscalYear")
    if cluster_by_firm:
        needed.append(firm_col)

    missing = set(needed) - set(df.columns)
    if missing:
        raise ValueError(f"Regression input missing columns: {sorted(missing)}")

    d = df[needed].copy()
    numeric_cols = [dependent, sentiment_col, novelty_col]
    if length_control:
        numeric_cols.append(length_control)
    for c in numeric_cols:
        d[c] = pd.to_numeric(d[c], errors="coerce")

    d = d.replace([np.inf, -np.inf], np.nan).dropna()
    if len(d) < 100:
        raise ValueError(
            f"Regression {dependent}/{sentiment_col} has only {len(d):,} usable rows"
        )

    terms = [
        sentiment_col,
        novelty_col,
        f"{sentiment_col}:{novelty_col}",
    ]
    if length_control:
        terms.append(length_control)
    if include_year_fixed_effects:
        terms.append("C(fiscalYear)")

    formula = f"{dependent} ~ " + " + ".join(terms)
    model = smf.ols(formula=formula, data=d)
    fitted = (
        model.fit(cov_type="cluster", cov_kwds={"groups": d[firm_col]})
        if cluster_by_firm
        else model.fit(cov_type="HC1")
    )
    return fitted, formula


def run_baseline_regressions(
    panel: pd.DataFrame,
    *,
    windows: Sequence[Sequence[int]],
    novelty_column: str,
    length_control: str | None,
    sentiment_models: Sequence[str],
    include_year_fixed_effects: bool = True,
    include_industry_fixed_effects: bool = False,
    cluster_by_firm: bool = True,
    firm_col: str = "edinetCode",
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    if include_industry_fixed_effects:
        raise NotImplementedError(
            "Industry fixed effects requested, but Stage 8A does not yet have "
            "a frozen canonical industry classification."
        )

    windows_t = _validate_windows(windows)
    unknown = set(sentiment_models) - set(SENTIMENT_COLUMNS)
    if unknown:
        raise ValueError(f"Unknown sentiment model(s): {sorted(unknown)}")

    rows, coeff_rows = [], []

    for model_name in sentiment_models:
        for spec_name in ["level", "change"]:
            sentiment_col = SENTIMENT_COLUMNS[model_name][spec_name]

            for start, end in windows_t:
                wlabel = _window_label(start, end)
                dependent = f"car_{wlabel}"
                sample_col = f"eventStudySample_{wlabel}"

                if dependent not in panel.columns or sample_col not in panel.columns:
                    raise ValueError(
                        f"Empirical panel missing {dependent} or {sample_col}"
                    )

                sample = panel[sample_col].astype("boolean").fillna(False)
                d0 = panel.loc[sample].copy()

                fitted, formula = _fit_one(
                    d0,
                    dependent=dependent,
                    sentiment_col=sentiment_col,
                    novelty_col=novelty_column,
                    length_control=length_control,
                    include_year_fixed_effects=include_year_fixed_effects,
                    cluster_by_firm=cluster_by_firm,
                    firm_col=firm_col,
                )

                interaction = f"{sentiment_col}:{novelty_column}"
                rows.append(
                    {
                        "sentimentModel": model_name,
                        "sentimentSpecification": spec_name,
                        "sentimentColumn": sentiment_col,
                        "window": f"[{start},{end}]",
                        "windowLabel": wlabel,
                        "dependentVariable": dependent,
                        "n": int(fitted.nobs),
                        "rSquared": float(fitted.rsquared),
                        "adjRSquared": float(fitted.rsquared_adj),
                        "sentimentCoef": float(fitted.params[sentiment_col]),
                        "sentimentSE": float(fitted.bse[sentiment_col]),
                        "sentimentT": float(fitted.tvalues[sentiment_col]),
                        "sentimentP": float(fitted.pvalues[sentiment_col]),
                        "noveltyCoef": float(fitted.params[novelty_column]),
                        "noveltySE": float(fitted.bse[novelty_column]),
                        "noveltyT": float(fitted.tvalues[novelty_column]),
                        "noveltyP": float(fitted.pvalues[novelty_column]),
                        "interactionCoef": float(fitted.params[interaction]),
                        "interactionSE": float(fitted.bse[interaction]),
                        "interactionT": float(fitted.tvalues[interaction]),
                        "interactionP": float(fitted.pvalues[interaction]),
                        "yearFixedEffects": bool(include_year_fixed_effects),
                        "industryFixedEffects": False,
                        "clusterByFirm": bool(cluster_by_firm),
                        "firmClusterColumn": firm_col if cluster_by_firm else "",
                        "formula": formula,
                    }
                )

                for term in fitted.params.index:
                    coeff_rows.append(
                        {
                            "sentimentModel": model_name,
                            "sentimentSpecification": spec_name,
                            "window": f"[{start},{end}]",
                            "windowLabel": wlabel,
                            "term": str(term),
                            "coefficient": float(fitted.params[term]),
                            "stdError": float(fitted.bse[term]),
                            "tStatistic": float(fitted.tvalues[term]),
                            "pValue": float(fitted.pvalues[term]),
                        }
                    )

    summary_df = pd.DataFrame(rows)
    coefficients_df = pd.DataFrame(coeff_rows)
    metadata = {
        "stage": "8B_baseline_regressions",
        "purpose": "full-document benchmark regressions",
        "windows": [[int(a), int(b)] for a, b in windows_t],
        "noveltyColumn": novelty_column,
        "lengthControl": length_control,
        "sentimentModels": list(sentiment_models),
        "sentimentSpecifications": ["level", "change"],
        "yearFixedEffects": bool(include_year_fixed_effects),
        "industryFixedEffects": False,
        "clusterByFirm": bool(cluster_by_firm),
        "firmClusterColumn": firm_col if cluster_by_firm else None,
        "regressionCount": int(len(summary_df)),
        "models": summary_df.to_dict(orient="records"),
    }
    return summary_df, coefficients_df, metadata


def write_regression_outputs(
    summary_df: pd.DataFrame,
    coefficients_df: pd.DataFrame,
    metadata: dict,
    *,
    output_dir: str | Path,
) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(
        output_dir / "baseline_regression_summary.csv",
        index=False,
    )
    coefficients_df.to_csv(
        output_dir / "baseline_regression_coefficients.csv",
        index=False,
    )
    (output_dir / "baseline_regression_summary.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
