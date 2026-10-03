from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.analysis.empirical_panel import build_empirical_panel, write_empirical_panel


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve(root: Path, value: str | Path) -> Path:
    p = Path(value).expanduser()
    return p if p.is_absolute() else root / p


def run_stage_empirical_panel(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    root = _repo_root()
    c = cfg.get("empirical_panel", {})

    analysis_panel_csv = _resolve(
        root,
        c.get(
            "analysis_panel_csv",
            f"data/interim/{paper}/analysis/analysis_panel.csv",
        ),
    )
    lmmd_csv = _resolve(
        root,
        c.get(
            "lmmd_csv",
            f"data/interim/{paper}/sentiment/lmmd/lmmd_sentiment.csv",
        ),
    )
    financial_bert_csv = _resolve(
        root,
        c.get(
            "financial_bert_csv",
            f"data/interim/{paper}/sentiment/financial_bert/"
            "financial_bert_sentiment.csv",
        ),
    )
    llm_csv = _resolve(
        root,
        c.get(
            "llm_csv",
            f"data/interim/{paper}/sentiment/llm/llm_sentiment.csv",
        ),
    )
    event_study_csv = _resolve(
        root,
        c.get(
            "event_study_csv",
            f"data/interim/{paper}/market_reaction/"
            "final_event_study_table.csv",
        ),
    )
    output_csv = _resolve(
        root,
        c.get(
            "output_csv",
            f"data/interim/{paper}/analysis/empirical_panel.csv",
        ),
    )
    summary_json = _resolve(
        root,
        c.get(
            "summary_json",
            f"data/interim/{paper}/analysis/empirical_panel_summary.json",
        ),
    )

    for p in [
        analysis_panel_csv,
        lmmd_csv,
        financial_bert_csv,
        llm_csv,
        event_study_csv,
    ]:
        if not p.exists():
            raise FileNotFoundError(f"Missing Stage 8A input: {p}")

    analysis_panel = pd.read_csv(
        analysis_panel_csv,
        dtype={
            "edinetCode": "string",
            "prev_docID": "string",
            "curr_docID": "string",
            "secCode": "string",
        },
        low_memory=False,
    )
    lmmd = pd.read_csv(
        lmmd_csv,
        dtype={"edinetCode": "string", "docID": "string"},
        low_memory=False,
    )
    bert = pd.read_csv(
        financial_bert_csv,
        dtype={"edinetCode": "string", "docID": "string"},
        low_memory=False,
    )
    llm = pd.read_csv(
        llm_csv,
        dtype={"docID": "string"},
        low_memory=False,
    )
    event_study = pd.read_csv(
        event_study_csv,
        dtype={
            "edinetCode": "string",
            "prev_docID": "string",
            "curr_docID": "string",
            "secCode": "string",
        },
        low_memory=False,
    )

    panel, summary = build_empirical_panel(
        analysis_panel,
        lmmd,
        bert,
        llm,
        event_study,
    )
    write_empirical_panel(
        panel,
        summary,
        output_csv=output_csv,
        summary_json=summary_json,
    )

    logger.info(
        "Stage 8A empirical panel: rows=%s cols=%s output=%s",
        len(panel),
        len(panel.columns),
        output_csv,
    )
    logger.info(
        "Stage 8A missing sentiment: %s",
        summary["missingSentimentCounts"],
    )
    logger.info("Stage 8A summary -> %s", summary_json)
