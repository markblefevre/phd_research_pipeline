"""Pipeline adapter for Stage 2: MD&A extraction."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.extract_mdna_batch import run_mdna_batch


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_stage_mdna_extract(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Run the Paper 2 MD&A extraction stage."""
    t0 = time.perf_counter()
    status = "ok"

    try:
        md_cfg = cfg.get("mdna_extract", {})
        root = repo_root()

        filings_csv = root / md_cfg.get(
            "filings_csv",
            f"data/interim/{paper}/edinet/filings.csv",
        )
        raw_dir = root / md_cfg.get(
            "raw_dir",
            f"data/raw/{paper}/edinet",
        )
        output_dir = root / md_cfg.get(
            "output_dir",
            f"data/interim/{paper}/mdna",
        )
        manifest_path = root / md_cfg.get(
            "manifest",
            f"data/interim/{paper}/mdna/extraction_manifest.csv",
        )

        checkpoint_every = int(md_cfg.get("checkpoint_every", 100))
        progress_every = int(md_cfg.get("progress_every", 100))
        retry_failures = bool(md_cfg.get("retry_failures", False))
        force = bool(md_cfg.get("force", False))

        limit_raw = md_cfg.get("limit")
        limit = int(limit_raw) if limit_raw is not None else None

        logger.info("Stage mdna_extract: filings_csv=%s", filings_csv)
        logger.info("Stage mdna_extract: raw_dir=%s", raw_dir)
        logger.info("Stage mdna_extract: output_dir=%s", output_dir)
        logger.info("Stage mdna_extract: manifest=%s", manifest_path)
        logger.info(
            "Stage mdna_extract: checkpoint_every=%s progress_every=%s "
            "retry_failures=%s force=%s limit=%s",
            checkpoint_every,
            progress_every,
            retry_failures,
            force,
            limit,
        )

        if not filings_csv.exists():
            raise FileNotFoundError(f"Missing filings_csv: {filings_csv}")
        if not raw_dir.exists():
            raise FileNotFoundError(f"Missing raw_dir: {raw_dir}")

        logger.info("[RUN] mdna_extract")

        summary = run_mdna_batch(
            filings_csv=filings_csv,
            raw_dir=raw_dir,
            output_dir=output_dir,
            manifest_path=manifest_path,
            checkpoint_every=checkpoint_every,
            progress_every=progress_every,
            retry_failures=retry_failures,
            force=force,
            limit=limit,
            logger=logger,
        )

        logger.info("mdna_extract summary: %s", summary)

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info(
            "Stage mdna_extract finished: status=%s elapsed=%.3fs",
            status,
            elapsed,
        )
