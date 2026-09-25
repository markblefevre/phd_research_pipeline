"""Pipeline adapter for Stage 6A: regression-ready analysis-panel foundation."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.analysis_panel import build_analysis_panel


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_stage_analysis_panel(*, paper: str, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    t0 = time.perf_counter()
    status = "ok"
    try:
        root = _repo_root()
        longitudinal_cfg = cfg.get("longitudinal_match", {})
        novelty_cfg = cfg.get("textual_novelty", {})
        panel_cfg = cfg.get("analysis_panel", {})

        pairs_csv = root / longitudinal_cfg.get(
            "output_dir", f"data/interim/{paper}/longitudinal"
        ) / "research_eligible_pairs.csv"

        canonical_novelty_root = root / novelty_cfg.get(
            "output_dir", f"data/interim/{paper}/novelty"
        )
        novelty_root_cfg = panel_cfg.get("novelty_source_root") or novelty_cfg.get("work_output_dir")
        novelty_root = (
            Path(novelty_root_cfg).expanduser()
            if novelty_root_cfg
            else canonical_novelty_root
        )

        diagnostics_cfg = panel_cfg.get("diagnostics_csv")
        diagnostics_csv = (
            Path(diagnostics_cfg).expanduser()
            if diagnostics_cfg
            else novelty_root / "pair_diagnostics.csv"
        )

        output_csv = root / panel_cfg.get(
            "output_csv", f"data/interim/{paper}/analysis/analysis_panel.csv"
        )
        metadata_json = output_csv.with_suffix(".metadata.json")

        logger.info("Stage analysis_panel: pairs_csv=%s", pairs_csv)
        logger.info("Stage analysis_panel: novelty_root=%s", novelty_root)
        logger.info("Stage analysis_panel: diagnostics_csv=%s", diagnostics_csv)
        logger.info("Stage analysis_panel: output_csv=%s", output_csv)

        panel = build_analysis_panel(
            pairs_csv=pairs_csv,
            novelty_root=novelty_root,
            output_csv=output_csv,
            diagnostics_csv=diagnostics_csv,
            metadata_json=metadata_json,
            baseline_variant=str(panel_cfg.get("baseline_variant", "sudachi_c_num")),
            raw_variant=str(panel_cfg.get("raw_variant", "sudachi_c_raw")),
        )

        logger.info(
            "analysis_panel produced %s rows x %s cols -> %s",
            len(panel), len(panel.columns), output_csv,
        )
        logger.info(
            "analysis_panel novelty: C-num mean=%.6f median=%.6f; C-raw mean=%.6f median=%.6f",
            panel["noveltyCNum"].mean(), panel["noveltyCNum"].median(),
            panel["noveltyCRaw"].mean(), panel["noveltyCRaw"].median(),
        )
    except Exception:
        status = "failed"
        raise
    finally:
        logger.info(
            "Stage analysis_panel finished: status=%s elapsed=%.3fs",
            status, time.perf_counter() - t0,
        )
