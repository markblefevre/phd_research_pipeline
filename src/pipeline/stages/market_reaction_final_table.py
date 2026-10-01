from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.market_reaction.final_event_study import (
    build_final_event_study_summary,
    build_final_event_study_table,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve(root: Path, value: str | Path) -> Path:
    p = Path(value).expanduser()
    return p if p.is_absolute() else root / p


def run_stage_market_reaction_final_table(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    """
    Stage 7F pipeline adapter: canonical final market-reaction table and QC.
    """
    root = _repo_root()
    c = cfg.get("market_reaction_final_table", {})

    analysis_panel_csv = _resolve(
        root,
        c.get(
            "analysis_panel_csv",
            "data/interim/paper2/analysis/analysis_panel.csv",
        ),
    )
    eligibility_csv = _resolve(
        root,
        c.get(
            "eligibility_csv",
            "data/interim/paper2/market_reaction/stage7_event_eligibility.csv",
        ),
    )
    abnormal_returns_csv = _resolve(
        root,
        c.get(
            "abnormal_returns_csv",
            "data/interim/paper2/market_reaction/abnormal_returns.csv",
        ),
    )
    output_csv = _resolve(
        root,
        c.get(
            "output_csv",
            "data/interim/paper2/market_reaction/final_event_study_table.csv",
        ),
    )
    summary_json = _resolve(
        root,
        c.get(
            "summary_json",
            "data/interim/paper2/market_reaction/final_event_study_summary.json",
        ),
    )
    windows = c.get("windows", [[0, 0], [0, 1], [-1, 1]])

    logger.info("Stage 7F analysis panel: %s", analysis_panel_csv)
    logger.info("Stage 7F eligibility: %s", eligibility_csv)
    logger.info("Stage 7F abnormal returns: %s", abnormal_returns_csv)
    logger.info("Stage 7F CAR windows: %s", windows)

    for path in [analysis_panel_csv, eligibility_csv, abnormal_returns_csv]:
        if not path.exists():
            raise FileNotFoundError(f"Missing Stage 7F input: {path}")

    analysis_panel = pd.read_csv(
        analysis_panel_csv,
        dtype={"secCode": "string"},
        low_memory=False,
    )
    eligibility = pd.read_csv(
        eligibility_csv,
        dtype={"secCode": "string"},
        low_memory=False,
    )
    abnormal_returns = pd.read_csv(
        abnormal_returns_csv,
        dtype={"secCode": "string"},
        low_memory=False,
    )

    logger.info(
        "Stage 7F inputs: analysisPanel=%s eligibility=%s abnormalReturns=%s",
        len(analysis_panel),
        len(eligibility),
        len(abnormal_returns),
    )

    final_table = build_final_event_study_table(
        analysis_panel,
        eligibility,
        abnormal_returns,
        windows=windows,
    )
    summary = build_final_event_study_summary(
        final_table,
        windows=windows,
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    final_table.to_csv(output_csv, index=False)
    summary_json.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    logger.info(
        "Stage 7F wrote %s rows x %s cols -> %s",
        len(final_table),
        len(final_table.columns),
        output_csv,
    )
    logger.info(
        "Stage 7F research pairs=%s Stage7A eligible=%s Stage7D estimated=%s",
        summary["researchPairs"],
        summary["stage7AEligible"],
        summary["stage7DMarketModelsEstimated"],
    )

    for label, item in summary["windows"].items():
        logger.info(
            "Stage 7F final sample CAR[%s]: n=%s retentionResearch=%.4f "
            "retentionEligible=%.4f retentionEstimated=%.4f",
            label,
            item["finalSample"],
            item["retentionVsResearchPairs"],
            item["retentionVsStage7AEligible"],
            item["retentionVsStage7DEstimated"],
        )

    logger.info("Stage 7F summary -> %s", summary_json)
