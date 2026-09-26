"""Pipeline adapter for Stage 6B LMMD visualization / QC."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.lmmd_plots import build_lmmd_diagnostics


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_stage_lmmd_plots(*, paper: str, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    t0 = time.perf_counter()
    status = "ok"
    try:
        root = _repo_root()
        lmmd_cfg = cfg.get("lmmd_sentiment", {})
        plot_cfg = cfg.get("lmmd_plots", {})
        edinet_cfg = cfg.get("edinet_download", {})

        sentiment_csv = root / lmmd_cfg.get(
            "output_csv", f"data/interim/{paper}/sentiment/lmmd/lmmd_sentiment.csv"
        )
        filings_csv = root / edinet_cfg.get(
            "metadata_csv", f"data/interim/{paper}/edinet/filings.csv"
        )
        output_dir = root / plot_cfg.get(
            "output_dir", f"outputs/{paper}/figures/sentiment/lmmd"
        )

        logger.info("Stage lmmd_plots: sentiment_csv=%s", sentiment_csv)
        logger.info("Stage lmmd_plots: filings_csv=%s", filings_csv)
        logger.info("Stage lmmd_plots: output_dir=%s", output_dir)

        annual = build_lmmd_diagnostics(
            sentiment_csv=sentiment_csv,
            filings_csv=filings_csv,
            output_dir=output_dir,
            formats=list(plot_cfg.get("formats", ["png", "pdf"])),
            min_year_observations=int(plot_cfg.get("min_year_observations", 25)),
        )
        logger.info("lmmd_plots produced annual summary for %s fiscal years", len(annual))
    except Exception:
        status = "failed"
        raise
    finally:
        logger.info(
            "Stage lmmd_plots finished: status=%s elapsed=%.3fs",
            status, time.perf_counter() - t0,
        )
