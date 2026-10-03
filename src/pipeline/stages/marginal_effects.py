from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.analysis.marginal_effects import (
    build_marginal_effects,
    generate_marginal_effect_plots,
    write_marginal_effect_outputs,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve(root: Path, value: str | Path) -> Path:
    p = Path(value).expanduser()
    return p if p.is_absolute() else root / p


def run_stage_marginal_effects(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    root = _repo_root()
    c = cfg.get("marginal_effects", {})

    panel_csv = _resolve(
        root,
        c.get(
            "panel_csv",
            f"data/interim/{paper}/analysis/empirical_panel.csv",
        ),
    )
    baseline_summary_csv = _resolve(
        root,
        c.get(
            "baseline_summary_csv",
            f"data/interim/{paper}/regressions/baseline/"
            "baseline_regression_summary.csv",
        ),
    )
    output_dir = _resolve(
        root,
        c.get(
            "output_dir",
            f"data/interim/{paper}/regressions/marginal_effects",
        ),
    )
    figure_dir = _resolve(
        root,
        c.get(
            "figure_dir",
            f"outputs/{paper}/figures/regressions/marginal_effects",
        ),
    )

    windows = c.get("windows", [[0, 0], [0, 1], [-1, 1]])
    novelty_column = str(c.get("novelty_column", "noveltyCNum"))
    length_raw = c.get("length_control", "absLogLengthChange")
    length_control = str(length_raw) if length_raw not in (None, "") else None
    sentiment_models = list(
        c.get("sentiment_models", ["lmmd", "financial_bert", "llm"])
    )
    percentiles = list(c.get("percentiles", [0.25, 0.50, 0.75, 0.90, 0.95]))
    include_year_fe = bool(c.get("include_year_fixed_effects", True))
    include_industry_fe = bool(c.get("include_industry_fixed_effects", False))
    cluster_by_firm = bool(c.get("cluster_by_firm", True))
    firm_col = str(c.get("firm_cluster_column", "edinetCode"))
    ci_level = float(c.get("ci_level", 0.95))
    formats = tuple(c.get("formats", ["png", "pdf"]))
    dpi = int(c.get("dpi", 180))
    publication_specs = list(
        c.get(
            "publication_specs",
            ["lmmd:change", "financial_bert:level", "llm:level"],
        )
    )
    verification_tolerance = float(c.get("verification_tolerance", 1e-10))

    if not panel_csv.exists():
        raise FileNotFoundError(f"Missing Stage 8C panel: {panel_csv}")
    if not baseline_summary_csv.exists():
        raise FileNotFoundError(
            f"Missing Stage 8B regression summary: {baseline_summary_csv}"
        )

    panel = pd.read_csv(
        panel_csv,
        dtype={"edinetCode": "string", "secCode": "string"},
        low_memory=False,
    )
    baseline_summary = pd.read_csv(
        baseline_summary_csv,
        low_memory=False,
    )

    marginal_df, interaction_df, metadata = build_marginal_effects(
        panel,
        windows=windows,
        novelty_column=novelty_column,
        length_control=length_control,
        sentiment_models=sentiment_models,
        novelty_percentiles=percentiles,
        include_year_fixed_effects=include_year_fe,
        include_industry_fixed_effects=include_industry_fe,
        cluster_by_firm=cluster_by_firm,
        firm_col=firm_col,
        ci_level=ci_level,
        baseline_summary=baseline_summary,
        verification_tolerance=verification_tolerance,
    )

    write_marginal_effect_outputs(
        marginal_df,
        interaction_df,
        metadata,
        output_dir=output_dir,
    )

    plot_paths = generate_marginal_effect_plots(
        marginal_df,
        figure_dir=figure_dir,
        formats=formats,
        dpi=dpi,
        publication_specs=publication_specs,
    )

    logger.info(
        "Stage 8C marginal effects complete: regressions=%s rows=%s",
        len(interaction_df),
        len(marginal_df),
    )
    logger.info(
        "Stage 8C verification: %s",
        metadata["verification"],
    )
    logger.info(
        "Stage 8C multiple testing: Holm<0.05=%s Bonferroni<0.05=%s",
        int(interaction_df["holmSignificant05"].sum()),
        int(interaction_df["bonferroniSignificant05"].sum()),
    )
    logger.info(
        "Stage 8C outputs: tables=%s figures=%s (%s files)",
        output_dir,
        figure_dir,
        len(plot_paths),
    )
