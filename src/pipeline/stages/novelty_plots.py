from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.novelty_plots import run_novelty_plots


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_stage_novelty_plots(*, paper: str, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    """Pipeline-facing Stage 5 visualization/QC entry point."""
    t0 = time.perf_counter()
    status = "ok"
    try:
        root = _repo_root()
        plot_cfg = cfg.get("novelty_plots", {})
        novelty_cfg = cfg.get("textual_novelty", {})
        longitudinal_cfg = cfg.get("longitudinal_match", {})

        canonical_novelty_root = root / novelty_cfg.get(
            "output_dir", f"data/interim/{paper}/novelty"
        )
        novelty_root_cfg = plot_cfg.get("source_root") or novelty_cfg.get("work_output_dir")
        novelty_root = (
            Path(novelty_root_cfg).expanduser()
            if novelty_root_cfg else canonical_novelty_root
        )
        pairs_csv = root / longitudinal_cfg.get(
            "output_dir", f"data/interim/{paper}/longitudinal"
        ) / "research_eligible_pairs.csv"
        output_dir = root / plot_cfg.get(
            "output_dir", f"outputs/{paper}/figures/novelty"
        )

        formats = plot_cfg.get("formats", ["png", "pdf"])
        if isinstance(formats, str):
            formats = [formats]

        logger.info("Stage novelty_plots: novelty_root=%s", novelty_root)
        logger.info("Stage novelty_plots: pairs_csv=%s", pairs_csv)
        logger.info("Stage novelty_plots: output_dir=%s", output_dir)

        result = run_novelty_plots(
            novelty_root=novelty_root,
            pairs_csv=pairs_csv,
            output_dir=output_dir,
            baseline_variant=str(plot_cfg.get("baseline_variant", "sudachi_c_num")),
            raw_variant=str(plot_cfg.get("raw_variant", "sudachi_c_raw")),
            formats=formats,
            min_year_observations=int(plot_cfg.get("min_year_observations", 25)),
        )
        logger.info("novelty_plots annual summary: %s", result["annual_summary"])
        logger.info("novelty_plots paper figures: %s", result["paper_dir"])
        logger.info("novelty_plots diagnostics: %s", result["diagnostic_dir"])
    except Exception:
        status = "failed"
        raise
    finally:
        logger.info(
            "Stage novelty_plots finished: status=%s elapsed=%.3fs",
            status, time.perf_counter() - t0,
        )
