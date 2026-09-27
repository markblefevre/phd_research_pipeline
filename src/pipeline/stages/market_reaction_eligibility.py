"""Pipeline adapter for Stage 7A: market-reaction event eligibility."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.market_reaction.event_eligibility import build_stage7_event_eligibility


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_stage_market_reaction_eligibility(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        root = _repo_root()
        stage_cfg = cfg.get("market_reaction_eligibility", {})
        skip_if_exists = bool(stage_cfg.get("skip_if_exists", False))

        output_dir = root / stage_cfg.get(
            "output_dir",
            f"data/interim/{paper}/market_reaction",
        )
        master_csv = output_dir / "stage7_event_eligibility.csv"
        exclusion_csv = output_dir / "stage7_exclusion_audit.csv"
        summary_json = output_dir / "stage7_eligibility_summary.json"

        logger.info("Stage market_reaction_eligibility: output_dir=%s", output_dir)

        if (
            skip_if_exists
            and master_csv.exists()
            and exclusion_csv.exists()
            and summary_json.exists()
        ):
            status = "skipped"
            logger.info(
                "[SKIP] market_reaction_eligibility already complete at %s",
                output_dir,
            )
            return

        logger.info("[RUN] Stage 7A market-reaction event eligibility")

        result = build_stage7_event_eligibility(
            root=root,
            paper=paper,
        )

        logger.info(
            "Stage 7A eligibility: input=%s eligible=%s excluded=%s",
            result["input_events"],
            result["eligible_events"],
            result["excluded_events"],
        )
        logger.info(
            "Stage 7A exclusion reasons: %s",
            result["exclusion_reason_counts"],
        )
        logger.info(
            "Stage 7A canonical output: %s",
            result["master_path"],
        )

    except Exception:
        status = "failed"
        raise

    finally:
        logger.info(
            "Stage market_reaction_eligibility finished: status=%s elapsed=%.3fs",
            status,
            time.perf_counter() - t0,
        )
