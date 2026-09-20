"""Pipeline adapter for Stage 3: longitudinal matching."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.longitudinal_match import run_longitudinal_match


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_stage_longitudinal_match(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Run the Paper 2 longitudinal matching stage."""
    t0 = time.perf_counter()
    status = "ok"

    try:
        ed_cfg = cfg.get("edinet_download", {})
        md_cfg = cfg.get("mdna_extract", {})
        lm_cfg = cfg.get("longitudinal_match", {})
        root = repo_root()

        # Stage 3 consumes Stage 2's configured inputs/outputs by default.
        # Explicit [longitudinal_match] values still override them.
        filings_csv = root / lm_cfg.get(
            "filings_csv",
            md_cfg.get(
                "filings_csv",
                ed_cfg.get(
                    "metadata_csv",
                    f"data/interim/{paper}/edinet/filings.csv",
                ),
            ),
        )

        extraction_manifest = root / lm_cfg.get(
            "extraction_manifest",
            md_cfg.get(
                "manifest",
                f"data/interim/{paper}/mdna/extraction_manifest.csv",
            ),
        )

        output_dir = root / lm_cfg.get(
            "output_dir",
            f"data/interim/{paper}/longitudinal",
        )

        min_period_end_gap_days = int(
            lm_cfg.get("min_period_end_gap_days", 300)
        )
        max_period_end_gap_days = int(
            lm_cfg.get("max_period_end_gap_days", 430)
        )

        logger.info(
            "Stage longitudinal_match: filings_csv=%s",
            filings_csv,
        )
        logger.info(
            "Stage longitudinal_match: extraction_manifest=%s",
            extraction_manifest,
        )
        logger.info(
            "Stage longitudinal_match: output_dir=%s",
            output_dir,
        )
        logger.info(
            "Stage longitudinal_match: min_gap_days=%s max_gap_days=%s",
            min_period_end_gap_days,
            max_period_end_gap_days,
        )

        if not filings_csv.exists():
            raise FileNotFoundError(
                f"Missing filings_csv: {filings_csv}"
            )

        if not extraction_manifest.exists():
            raise FileNotFoundError(
                f"Missing extraction_manifest: {extraction_manifest}"
            )

        logger.info("[RUN] longitudinal_match")

        summary = run_longitudinal_match(
            filings_csv=filings_csv,
            extraction_manifest=extraction_manifest,
            output_dir=output_dir,
            min_period_end_gap_days=min_period_end_gap_days,
            max_period_end_gap_days=max_period_end_gap_days,
            logger=logger,
        )

        logger.info("longitudinal_match summary: %s", summary)

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info(
            "Stage longitudinal_match finished: status=%s elapsed=%.3fs",
            status,
            elapsed,
        )
