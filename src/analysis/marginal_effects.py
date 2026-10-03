from __future__ import annotations

import json
import math
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
    out: list[tuple[int, int]] = []
    for raw in windows:
        if len(raw) != 2:
            raise ValueError(f"Invalid CAR window {raw!r}; expected [start, end]")
        start, end = int(raw[0]), int(raw[1])
        if start > end:
            raise ValueError(f"Invalid CAR window [{start}, {end}]")
        out.append((start, end))
    if not out:
        raise ValueError("At least one CAR window is required")
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


def holm_adjust(p_values: Sequence[float]) -> np.ndarray:
    """Holm step-down FWER adjustment; valid under arbitrary dependence."""
    p = np.asarray(p_values, dtype=float)
    n = len(p)
    if n == 0:
        return p
    order = np.argsort(p)
    ranked = p[order]
    adjusted_ranked = np.empty(n, dtype=float)

    running = 0.0
    for i, raw in enumerate(ranked):
        adj = (n - i) * raw
        running = max(running, adj)
        adjusted_ranked[i] = min(running, 1.0)

    adjusted = np.empty(n, dtype=float)
    adjusted[order] = adjusted_ranked
    return adjusted


def bonferroni_adjust(p_values: Sequence[float]) -> np.ndarray:
    p = np.asarray(p_values, dtype=float)
    return np.minimum(p * len(p), 1.0)


def _fit_model(
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
        raise ValueError(f"Stage 8C input missing columns: {sorted(missing)}")

    d = df[needed].copy()
    numeric_cols = [dependent, sentiment_col, novelty_col]
    if length_control:
        numeric_cols.append(length_control)

    for col in numeric_cols:
        d[col] = pd.to_numeric(d[col], errors="coerce")

    d = d.replace([np.inf, -np.inf], np.nan).dropna()
    if len(d) < 100:
        raise ValueError(
            f"Stage 8C regression {dependent}/{sentiment_col} has only "
            f"{len(d):,} usable rows"
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
    return fitted, d, formula


def _z_critical(ci_level: float) -> float:
    # Avoid scipy dependency. 1.959963984540054 for 95%; otherwise use
    # statistics.NormalDist when available.
    if not 0 < ci_level < 1:
        raise ValueError("ci_level must be between 0 and 1")
    try:
        from statistics import NormalDist
        return float(NormalDist().inv_cdf(0.5 + ci_level / 2.0))
    except Exception:
        if abs(ci_level - 0.95) < 1e-12:
            return 1.959963984540054
        raise


def _marginal_effect_rows(
    fitted,
    d: pd.DataFrame,
    *,
    model_name: str,
    spec_name: str,
    window: tuple[int, int],
    sentiment_col: str,
    novelty_col: str,
    novelty_percentiles: Sequence[float],
    ci_level: float,
) -> list[dict]:
    interaction = f"{sentiment_col}:{novelty_col}"

    beta_s = float(fitted.params[sentiment_col])
    beta_i = float(fitted.params[interaction])
    cov = fitted.cov_params()
    var_s = float(cov.loc[sentiment_col, sentiment_col])
    var_i = float(cov.loc[interaction, interaction])
    cov_si = float(cov.loc[sentiment_col, interaction])

    sentiment_sd = float(d[sentiment_col].std(ddof=1))
    if not np.isfinite(sentiment_sd) or sentiment_sd <= 0:
        raise ValueError(
            f"Non-positive sentiment SD for {model_name}/{spec_name}/{window}"
        )

    z = _z_critical(ci_level)
    rows: list[dict] = []

    for pct in novelty_percentiles:
        if not 0 <= pct <= 1:
            raise ValueError(f"Invalid novelty percentile: {pct}")
        novelty_value = float(d[novelty_col].quantile(pct))

        effect_per_unit = beta_s + beta_i * novelty_value
        var_per_unit = (
            var_s
            + novelty_value * novelty_value * var_i
            + 2.0 * novelty_value * cov_si
        )
        se_per_unit = math.sqrt(max(var_per_unit, 0.0))

        effect_1sd = sentiment_sd * effect_per_unit
        se_1sd = sentiment_sd * se_per_unit
        lo = effect_1sd - z * se_1sd
        hi = effect_1sd + z * se_1sd
        t = effect_1sd / se_1sd if se_1sd > 0 else np.nan

        rows.append(
            {
                "sentimentModel": model_name,
                "sentimentSpecification": spec_name,
                "sentimentColumn": sentiment_col,
                "window": f"[{window[0]},{window[1]}]",
                "windowLabel": _window_label(*window),
                "noveltyPercentile": float(pct),
                "noveltyValue": novelty_value,
                "sentimentSD": sentiment_sd,
                "marginalEffectPerUnitSentiment": effect_per_unit,
                "marginalEffect1SD": effect_1sd,
                "marginalEffect1SD_bps": effect_1sd * 10000.0,
                "stdError1SD": se_1sd,
                "ciLower1SD": lo,
                "ciUpper1SD": hi,
                "ciLower1SD_bps": lo * 10000.0,
                "ciUpper1SD_bps": hi * 10000.0,
                "tStatistic": float(t),
                "ciLevel": float(ci_level),
                "n": int(fitted.nobs),
            }
        )
    return rows


def _interaction_rows(
    fitted,
    *,
    model_name: str,
    spec_name: str,
    window: tuple[int, int],
    sentiment_col: str,
    novelty_col: str,
    formula: str,
) -> dict:
    interaction = f"{sentiment_col}:{novelty_col}"
    return {
        "sentimentModel": model_name,
        "sentimentSpecification": spec_name,
        "sentimentColumn": sentiment_col,
        "window": f"[{window[0]},{window[1]}]",
        "windowLabel": _window_label(*window),
        "interactionTerm": interaction,
        "interactionCoef": float(fitted.params[interaction]),
        "interactionSE": float(fitted.bse[interaction]),
        "interactionT": float(fitted.tvalues[interaction]),
        "rawP": float(fitted.pvalues[interaction]),
        "n": int(fitted.nobs),
        "formula": formula,
    }


def _verify_against_stage8b(
    interaction_df: pd.DataFrame,
    baseline_summary: pd.DataFrame | None,
    *,
    tolerance: float,
) -> dict:
    if baseline_summary is None:
        return {
            "verifiedAgainstStage8B": False,
            "reason": "baseline summary not supplied",
        }

    required = {
        "sentimentModel",
        "sentimentSpecification",
        "window",
        "interactionCoef",
        "interactionP",
    }
    missing = required - set(baseline_summary.columns)
    if missing:
        raise ValueError(
            "Stage 8B summary missing columns required for verification: "
            f"{sorted(missing)}"
        )

    left = interaction_df[
        [
            "sentimentModel",
            "sentimentSpecification",
            "window",
            "interactionCoef",
            "rawP",
        ]
    ].copy()
    right = baseline_summary[
        [
            "sentimentModel",
            "sentimentSpecification",
            "window",
            "interactionCoef",
            "interactionP",
        ]
    ].copy()

    merged = left.merge(
        right,
        on=["sentimentModel", "sentimentSpecification", "window"],
        how="outer",
        suffixes=("_8c", "_8b"),
        indicator=True,
    )
    if not merged["_merge"].eq("both").all():
        raise ValueError(
            "Stage 8C regression set does not match Stage 8B summary exactly"
        )

    coef_diff = (
        pd.to_numeric(merged["interactionCoef_8c"], errors="coerce")
        - pd.to_numeric(merged["interactionCoef_8b"], errors="coerce")
    ).abs()
    p_diff = (
        pd.to_numeric(merged["rawP"], errors="coerce")
        - pd.to_numeric(merged["interactionP"], errors="coerce")
    ).abs()

    max_coef_diff = float(coef_diff.max())
    max_p_diff = float(p_diff.max())
    if max_coef_diff > tolerance or max_p_diff > tolerance:
        raise ValueError(
            "Stage 8C refit does not reproduce Stage 8B within tolerance: "
            f"max coef diff={max_coef_diff:.3g}, max p diff={max_p_diff:.3g}, "
            f"tolerance={tolerance:.3g}"
        )

    return {
        "verifiedAgainstStage8B": True,
        "tolerance": float(tolerance),
        "maxInteractionCoefficientDifference": max_coef_diff,
        "maxInteractionPDifference": max_p_diff,
        "regressionCount": int(len(merged)),
    }


def build_marginal_effects(
    panel: pd.DataFrame,
    *,
    windows: Sequence[Sequence[int]],
    novelty_column: str,
    length_control: str | None,
    sentiment_models: Sequence[str],
    novelty_percentiles: Sequence[float],
    include_year_fixed_effects: bool = True,
    include_industry_fixed_effects: bool = False,
    cluster_by_firm: bool = True,
    firm_col: str = "edinetCode",
    ci_level: float = 0.95,
    baseline_summary: pd.DataFrame | None = None,
    verification_tolerance: float = 1e-10,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Refit the frozen Stage 8B models solely to recover covariance matrices
    needed for marginal effects. Specifications are not changed.

    Returns:
      - marginal effects at requested novelty percentiles for every model/spec/window
      - interaction-level multiple-testing table with raw, Bonferroni, and Holm p-values
      - metadata / verification record
    """
    if include_industry_fixed_effects:
        raise NotImplementedError(
            "Industry fixed effects requested, but the current frozen Stage 8 "
            "benchmark does not yet include a canonical industry classification."
        )

    windows_t = _validate_windows(windows)
    unknown = set(sentiment_models) - set(SENTIMENT_COLUMNS)
    if unknown:
        raise ValueError(f"Unknown sentiment model(s): {sorted(unknown)}")

    marginal_rows: list[dict] = []
    interaction_rows: list[dict] = []

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

                fitted, d, formula = _fit_model(
                    d0,
                    dependent=dependent,
                    sentiment_col=sentiment_col,
                    novelty_col=novelty_column,
                    length_control=length_control,
                    include_year_fixed_effects=include_year_fixed_effects,
                    cluster_by_firm=cluster_by_firm,
                    firm_col=firm_col,
                )

                interaction_rows.append(
                    _interaction_rows(
                        fitted,
                        model_name=model_name,
                        spec_name=spec_name,
                        window=(start, end),
                        sentiment_col=sentiment_col,
                        novelty_col=novelty_column,
                        formula=formula,
                    )
                )

                marginal_rows.extend(
                    _marginal_effect_rows(
                        fitted,
                        d,
                        model_name=model_name,
                        spec_name=spec_name,
                        window=(start, end),
                        sentiment_col=sentiment_col,
                        novelty_col=novelty_column,
                        novelty_percentiles=novelty_percentiles,
                        ci_level=ci_level,
                    )
                )

    interaction_df = pd.DataFrame(interaction_rows)
    pvals = interaction_df["rawP"].to_numpy(dtype=float)
    interaction_df["bonferroniP"] = bonferroni_adjust(pvals)
    interaction_df["holmP"] = holm_adjust(pvals)
    interaction_df["rawSignificant05"] = interaction_df["rawP"] < 0.05
    interaction_df["bonferroniSignificant05"] = interaction_df["bonferroniP"] < 0.05
    interaction_df["holmSignificant05"] = interaction_df["holmP"] < 0.05

    verification = _verify_against_stage8b(
        interaction_df,
        baseline_summary,
        tolerance=verification_tolerance,
    )

    marginal_df = pd.DataFrame(marginal_rows)
    metadata = {
        "stage": "8C_marginal_effects",
        "purpose": (
            "Interpret the frozen full-document benchmark using standardized "
            "marginal effects and transparent multiple-testing adjustments."
        ),
        "regressionCount": int(len(interaction_df)),
        "marginalEffectRows": int(len(marginal_df)),
        "sentimentModels": list(sentiment_models),
        "sentimentSpecifications": ["level", "change"],
        "windows": [[int(a), int(b)] for a, b in windows_t],
        "noveltyColumn": novelty_column,
        "noveltyPercentiles": [float(x) for x in novelty_percentiles],
        "lengthControl": length_control,
        "yearFixedEffects": bool(include_year_fixed_effects),
        "industryFixedEffects": False,
        "clusterByFirm": bool(cluster_by_firm),
        "firmClusterColumn": firm_col if cluster_by_firm else None,
        "ciLevel": float(ci_level),
        "effectScaling": (
            "Marginal effect of a one-sample-standard-deviation increase in "
            "sentiment, holding other regressors fixed."
        ),
        "multipleTestingFamily": (
            "All configured sentiment-model × sentiment-specification × "
            "CAR-window interaction tests."
        ),
        "multipleTestingMethods": ["raw", "bonferroni", "holm"],
        "verification": verification,
    }
    return marginal_df, interaction_df, metadata


def write_marginal_effect_outputs(
    marginal_df: pd.DataFrame,
    interaction_df: pd.DataFrame,
    metadata: dict,
    *,
    output_dir: str | Path,
) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    marginal_df.to_csv(output_dir / "marginal_effects.csv", index=False)
    interaction_df.to_csv(
        output_dir / "interaction_multiple_testing.csv",
        index=False,
    )
    (output_dir / "marginal_effects_summary.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def generate_marginal_effect_plots(
    marginal_df: pd.DataFrame,
    *,
    figure_dir: str | Path,
    formats: Sequence[str] = ("png", "pdf"),
    dpi: int = 180,
    publication_specs: Sequence[str] = (
        "lmmd:change",
        "financial_bert:level",
        "llm:level",
    ),
) -> list[Path]:
    """
    Create diagnostic plots for all 18 regressions and publication-style plots
    for the pre-declared benchmark subset. No model is selected dynamically
    from p-values.
    """
    import matplotlib.pyplot as plt

    figure_dir = Path(figure_dir)
    diagnostics_dir = figure_dir / "diagnostics"
    paper_dir = figure_dir / "paper"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    paper_dir.mkdir(parents=True, exist_ok=True)

    saved: list[Path] = []

    def _save(fig, stem: Path):
        for fmt in formats:
            p = stem.with_suffix(f".{fmt}")
            fig.savefig(p, bbox_inches="tight", dpi=int(dpi))
            saved.append(p)
        plt.close(fig)

    def _plot_one(g: pd.DataFrame, stem: Path, title: str):
        g = g.sort_values("noveltyValue")
        x = g["noveltyValue"].to_numpy(dtype=float)
        y = g["marginalEffect1SD_bps"].to_numpy(dtype=float)
        lo = g["ciLower1SD_bps"].to_numpy(dtype=float)
        hi = g["ciUpper1SD_bps"].to_numpy(dtype=float)

        fig, ax = plt.subplots(figsize=(7.2, 4.6))
        ax.plot(x, y, marker="o")
        ax.fill_between(x, lo, hi, alpha=0.2)
        ax.axhline(0.0, linewidth=1)
        ax.set_xlabel("Textual novelty")
        ax.set_ylabel("Marginal effect of 1 SD sentiment increase (bp)")
        ax.set_title(title)
        _save(fig, stem)

    # All 18 diagnostic plots.
    group_cols = [
        "sentimentModel",
        "sentimentSpecification",
        "window",
        "windowLabel",
    ]
    for keys, g in marginal_df.groupby(group_cols, sort=False):
        model_name, spec_name, window, wlabel = keys
        title = (
            f"{model_name} {spec_name}: marginal sentiment effect, CAR {window}"
        )
        stem = diagnostics_dir / f"{model_name}_{spec_name}_car_{wlabel}"
        _plot_one(g, stem, title)

    # Pre-declared publication subset across all three CAR windows.
    # Produce both:
    #   (1) the original compact line-only comparison, and
    #   (2) a confidence-ribbon version using the already-computed 95% CIs.
    #
    # Nothing is selected from p-values; publication_specs is fixed in config.
    allowed = set(publication_specs)
    for (model_name, spec_name), g0 in marginal_df.groupby(
        ["sentimentModel", "sentimentSpecification"],
        sort=False,
    ):
        label = f"{model_name}:{spec_name}"
        if label not in allowed:
            continue

        # Original compact comparison.
        fig, ax = plt.subplots(figsize=(7.5, 4.8))
        for window, g in g0.groupby("window", sort=False):
            g = g.sort_values("noveltyValue")
            ax.plot(
                g["noveltyValue"],
                g["marginalEffect1SD_bps"],
                marker="o",
                label=f"CAR {window}",
            )
        ax.axhline(0.0, linewidth=1)
        ax.set_xlabel("Textual novelty")
        ax.set_ylabel("Marginal effect of 1 SD sentiment increase (bp)")
        ax.set_title(
            f"{model_name} {spec_name}: marginal effects across CAR windows"
        )
        ax.legend()
        _save(fig, paper_dir / f"{model_name}_{spec_name}_all_windows")

        # Paper-ready comparison with 95% confidence ribbons.
        fig, ax = plt.subplots(figsize=(7.5, 4.8))
        for window, g in g0.groupby("window", sort=False):
            g = g.sort_values("noveltyValue")
            x = g["noveltyValue"].to_numpy(dtype=float)
            y = g["marginalEffect1SD_bps"].to_numpy(dtype=float)
            lo = g["ciLower1SD_bps"].to_numpy(dtype=float)
            hi = g["ciUpper1SD_bps"].to_numpy(dtype=float)

            line = ax.plot(
                x,
                y,
                marker="o",
                label=f"CAR {window}",
            )[0]
            ax.fill_between(
                x,
                lo,
                hi,
                alpha=0.15,
                color=line.get_color(),
            )

        ax.axhline(0.0, linewidth=1)
        ax.set_xlabel("Textual novelty")
        ax.set_ylabel("Marginal effect of 1 SD sentiment increase (bp)")
        ax.set_title(
            f"{model_name} {spec_name}: marginal effects with 95% CI"
        )
        ax.legend()
        _save(fig, paper_dir / f"{model_name}_{spec_name}_all_windows_ci")

        # Cleaner one-window-at-a-time paper figures with a single CI ribbon.
        # These are especially useful when overlapping ribbons make the
        # all-windows figure visually busy.
        for window, g in g0.groupby("window", sort=False):
            g = g.sort_values("noveltyValue")
            x = g["noveltyValue"].to_numpy(dtype=float)
            y = g["marginalEffect1SD_bps"].to_numpy(dtype=float)
            lo = g["ciLower1SD_bps"].to_numpy(dtype=float)
            hi = g["ciUpper1SD_bps"].to_numpy(dtype=float)
            wlabel = str(g["windowLabel"].iloc[0])

            fig, ax = plt.subplots(figsize=(7.2, 4.6))
            line = ax.plot(x, y, marker="o")[0]
            ax.fill_between(
                x,
                lo,
                hi,
                alpha=0.20,
                color=line.get_color(),
            )
            ax.axhline(0.0, linewidth=1)
            ax.set_xlabel("Textual novelty")
            ax.set_ylabel("Marginal effect of 1 SD sentiment increase (bp)")
            ax.set_title(
                f"{model_name} {spec_name}: CAR {window}, 95% CI"
            )
            _save(
                fig,
                paper_dir / f"{model_name}_{spec_name}_car_{wlabel}_ci",
            )

    return saved


__all__ = [
    "build_marginal_effects",
    "write_marginal_effect_outputs",
    "generate_marginal_effect_plots",
    "holm_adjust",
    "bonferroni_adjust",
]
