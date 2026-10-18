"""Pipeline adapter for Paper 2 Stage 6F-B (optional pipeline integration)."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.persistent_novel_component_sentiment import run_component_sentiment


def _resolve(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return (path if path.is_absolute() else root / path).resolve()


def run_stage_persistent_novel_component_sentiment(
    *, paper: str, cfg: Dict[str, Any], logger: logging.Logger
) -> Dict[str, Any]:
    root = Path(__file__).resolve().parents[3]
    settings = cfg.get("persistent_novel_component_sentiment", {})
    source = f"data/interim/{paper}/alignment/persistent_novel/results_final_repaired"
    return run_component_sentiment(
        documents_csv=_resolve(root, str(settings.get("documents_csv", source + "/persistent_novel_documents.csv"))),
        prompt_file=_resolve(root, str(settings.get("prompt_file", "configs/paper2/prompts/llm_sentiment_v2.md"))),
        qc_summary_json=_resolve(root, str(settings.get("qc_summary_json", f"data/interim/{paper}/alignment/persistent_novel/component_qc/component_qc_summary.json"))),
        output_dir=_resolve(root, str(settings.get("output_dir", f"data/interim/{paper}/sentiment/persistent_novel/llm_luna"))),
        model=str(settings.get("model", "gpt-6-luna")),
        reasoning_effort=str(settings.get("reasoning_effort", "none")),
        max_documents=settings.get("max_documents"),
        sample_documents=settings.get("sample_documents"),
        sample_seed=int(settings.get("sample_seed", 42)),
        max_concurrent_requests=int(settings.get("max_concurrent_requests", 1)),
        target_unit_chars=int(settings.get("target_unit_chars", 1050)),
        max_unit_chars=int(settings.get("max_unit_chars", 1400)),
        max_retries=int(settings.get("max_retries", 3)),
        retry_base_seconds=float(settings.get("retry_base_seconds", 2.0)),
        dry_run=bool(settings.get("dry_run", True)),
        resume=bool(settings.get("resume", True)),
        logger=logger,
    )
