"""Pipeline adapter for Financial BERT publication/QC plots."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.financial_bert_plots import generate_financial_bert_plots


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_stage_financial_bert_plots(*, paper: str, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    t0 = time.perf_counter()
    status = "ok"
    try:
        root = _repo_root()
        plot_cfg = cfg.get("financial_bert_plots", {})
        bert_cfg = cfg.get("financial_bert_sentiment", {})
        edinet_cfg = cfg.get("edinet_download", {})
        lmmd_cfg = cfg.get("lmmd_sentiment", {})

        bert_csv = root / bert_cfg.get(
            "output_csv", f"data/interim/{paper}/sentiment/financial_bert/financial_bert_sentiment.csv"
        )
        filings_csv = root / edinet_cfg.get(
            "filings_csv", f"data/interim/{paper}/edinet/filings.csv"
        )
        lmmd_value = plot_cfg.get("lmmd_csv") or lmmd_cfg.get("output_csv")
        lmmd_csv = (root / lmmd_value) if lmmd_value else None
        output_dir = root / plot_cfg.get(
            "output_dir", f"outputs/{paper}/figures/sentiment/financial_bert"
        )
        formats = tuple(plot_cfg.get("formats", ["pdf", "png"]))

        annual = generate_financial_bert_plots(
            bert_csv=bert_csv,
            filings_csv=filings_csv,
            lmmd_csv=lmmd_csv,
            output_dir=output_dir,
            formats=formats,
            dpi=int(plot_cfg.get("dpi", 180)),
            min_year_observations=int(plot_cfg.get("min_year_observations", 25)),
        )
        logger.info("financial_bert_plots wrote %s annual rows under %s", len(annual), output_dir)
    except Exception:
        status = "failed"
        raise
    finally:
        logger.info(
            "Stage financial_bert_plots finished: status=%s elapsed=%.3fs",
            status, time.perf_counter() - t0,
        )
