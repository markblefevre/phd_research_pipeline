from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.analysis.baseline_regressions import (
    run_baseline_regressions,
    write_regression_outputs,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve(root: Path, value: str | Path) -> Path:
    p = Path(value).expanduser()
    return p if p.is_absolute() else root / p


def run_stage_baseline_regressions(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    root = _repo_root()
    c = cfg.get("baseline_regressions", {})

    input_csv = _resolve(
        root,
        c.get(
            "input_csv",
            f"data/interim/{paper}/analysis/empirical_panel.csv",
        ),
    )
    output_dir = _resolve(
        root,
        c.get(
            "output_dir",
            f"data/interim/{paper}/regressions/baseline",
        ),
    )
    windows = c.get("windows", [[0, 0], [0, 1], [-1, 1]])
    novelty_column = str(c.get("novelty_column", "noveltyCNum"))
    length_raw = c.get("length_control", "absLogLengthChange")
    length_control = str(length_raw) if length_raw not in (None, "") else None
    sentiment_models = list(
        c.get(
            "sentiment_models",
            ["lmmd", "financial_bert", "llm"],
        )
    )
    include_year_fe = bool(c.get("include_year_fixed_effects", True))
    include_industry_fe = bool(c.get("include_industry_fixed_effects", False))
    cluster_by_firm = bool(c.get("cluster_by_firm", True))
    firm_col = str(c.get("firm_cluster_column", "edinetCode"))

    if not input_csv.exists():
        raise FileNotFoundError(f"Missing Stage 8B input: {input_csv}")

    logger.info(
        "Stage 8B config: windows=%s novelty=%s length=%s sentiment=%s "
        "yearFE=%s industryFE=%s clusterFirm=%s firmCol=%s",
        windows,
        novelty_column,
        length_control,
        sentiment_models,
        include_year_fe,
        include_industry_fe,
        cluster_by_firm,
        firm_col,
    )

    panel = pd.read_csv(
        input_csv,
        dtype={"edinetCode": "string", "secCode": "string"},
        low_memory=False,
    )
    summary_df, coefficients_df, metadata = run_baseline_regressions(
        panel,
        windows=windows,
        novelty_column=novelty_column,
        length_control=length_control,
        sentiment_models=sentiment_models,
        include_year_fixed_effects=include_year_fe,
        include_industry_fixed_effects=include_industry_fe,
        cluster_by_firm=cluster_by_firm,
        firm_col=firm_col,
    )
    write_regression_outputs(
        summary_df,
        coefficients_df,
        metadata,
        output_dir=output_dir,
    )

    logger.info(
        "Stage 8B complete: regressions=%s output_dir=%s",
        len(summary_df),
        output_dir,
    )
    for row in summary_df.itertuples(index=False):
        logger.info(
            "Stage 8B %s/%s CAR%s: N=%s interaction=%+.6g t=%+.3f p=%.4g",
            row.sentimentModel,
            row.sentimentSpecification,
            row.window,
            row.n,
            row.interactionCoef,
            row.interactionT,
            row.interactionP,
        )
