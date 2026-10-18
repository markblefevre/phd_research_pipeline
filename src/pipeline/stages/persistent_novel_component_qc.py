"""Pipeline adapter for Stage 6F-A component QC."""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.persistent_novel_component_qc import run_component_qc


def run_stage_persistent_novel_component_qc(*, paper: str, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    started = time.perf_counter()
    status = "ok"
    try:
        root = Path(__file__).resolve().parents[3]
        conf = cfg.get("persistent_novel_component_qc", {})
        source = root / conf.get("source_dir", f"data/interim/{paper}/alignment/persistent_novel/results_final_repaired")
        output = root / conf.get("output_dir", f"data/interim/{paper}/alignment/persistent_novel/component_qc")
        result = run_component_qc(
            documents_csv=source / "persistent_novel_documents.csv",
            sentences_csv=source / "persistent_novel_sentences.csv",
            stage6e_summary_json=source / "pair_level_classification_summary.json",
            output_dir=output,
        )
        logger.info("Stage 6F-A QC passed: %s documents, %s sentences; output=%s",
                    result["documents"], result["sentences"], output)
    except Exception:
        status = "failed"
        raise
    finally:
        logger.info("Stage persistent_novel_component_qc finished: status=%s elapsed=%.3fs",
                    status, time.perf_counter() - started)
