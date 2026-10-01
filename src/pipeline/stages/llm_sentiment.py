"""Pipeline adapter for Paper 2 Stage 6D: OpenAI LLM sentiment."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.llm_sentiment import score_llm_corpus


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_path(root: Path, value: str | Path) -> Path:
    p = Path(str(value)).expanduser()
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def run_stage_llm_sentiment(
    *, paper: str, cfg: Dict[str, Any], logger: logging.Logger
) -> None:
    """Resolve pipeline configuration and invoke the Stage 6D doer."""
    t0 = time.perf_counter()
    status = "ok"

    try:
        root = _repo_root()
        stage_cfg = cfg.get("llm_sentiment", {})
        mdna_cfg = cfg.get("mdna_extract", {})
        token_cfg = cfg.get("text_tokenization", {})

        # Match Stage 6C's convention: prefer a configured local SSD MD&A copy,
        # then the Stage 4 working copy, then canonical Stage 2 extraction output.
        source_value = (
            stage_cfg.get("source_root")
            or token_cfg.get("source_root")
            or mdna_cfg.get("output_dir", f"data/interim/{paper}/mdna")
        )
        mdna_root = _resolve_path(root, source_value)

        prompt_value = stage_cfg.get(
            "prompt_file",
            "configs/paper2/prompts/llm_sentiment_v2.md",
        )
        prompt_file = _resolve_path(root, prompt_value)

        output_csv = _resolve_path(
            root,
            stage_cfg.get(
                "output_csv",
                f"data/interim/{paper}/sentiment/llm/llm_sentiment.csv",
            ),
        )
        raw_units_dir = _resolve_path(
            root,
            stage_cfg.get(
                "raw_units_dir",
                f"data/interim/{paper}/sentiment/llm/units",
            ),
        )
        metadata_json = output_csv.with_suffix(".metadata.json")

        doc_ids = stage_cfg.get("doc_ids")
        if doc_ids is not None:
            doc_ids = [str(x) for x in doc_ids]

        manifest_csv = None
        manifest_value = stage_cfg.get("manifest_csv")
        if manifest_value:
            manifest_csv = _resolve_path(root, manifest_value)

        max_documents = stage_cfg.get("max_documents")
        if max_documents is not None:
            max_documents = int(max_documents)

        max_concurrent_requests = int(stage_cfg.get("max_concurrent_requests", 1))

        model = str(stage_cfg.get("model", "gpt-6-sol"))
        reasoning_effort = str(stage_cfg.get("reasoning_effort", "none"))

        logger.info("Stage llm_sentiment: MD&A root=%s", mdna_root)
        logger.info("Stage llm_sentiment: prompt=%s", prompt_file)
        logger.info("Stage llm_sentiment: model=%s", model)
        logger.info("Stage llm_sentiment: reasoning_effort=%s", reasoning_effort)
        logger.info("Stage llm_sentiment: doc_ids=%s", doc_ids)
        logger.info("Stage llm_sentiment: max_documents=%s", max_documents)
        logger.info(
            "Stage llm_sentiment: max_concurrent_requests=%s",
            max_concurrent_requests,
        )
        logger.info("Stage llm_sentiment: output=%s", output_csv)

        out = score_llm_corpus(
            mdna_root=mdna_root,
            prompt_file=prompt_file,
            output_csv=output_csv,
            raw_units_dir=raw_units_dir,
            metadata_json=metadata_json,
            model=model,
            reasoning_effort=reasoning_effort,
            doc_ids=doc_ids,
            manifest_csv=manifest_csv,
            max_documents=max_documents,
            max_concurrent_requests=max_concurrent_requests,
            target_unit_chars=int(stage_cfg.get("target_unit_chars", 1050)),
            max_unit_chars=int(stage_cfg.get("max_unit_chars", 1400)),
            min_narrative_line_chars=int(
                stage_cfg.get("min_narrative_line_chars", 20)
            ),
            resume=bool(stage_cfg.get("resume", True)),
            overwrite=bool(stage_cfg.get("overwrite", False)),
            dry_run=bool(stage_cfg.get("dry_run", False)),
            max_retries=int(stage_cfg.get("max_retries", 3)),
            retry_base_seconds=float(stage_cfg.get("retry_base_seconds", 2.0)),
            repo_root=root,
            logger=logger,
        )

        if "llmNet" in out.columns and out["llmNet"].notna().any():
            logger.info(
                "llm_sentiment produced %d document scores; "
                "mean llmNet=%.6f median=%.6f",
                len(out),
                out["llmNet"].mean(),
                out["llmNet"].median(),
            )
        else:
            logger.info(
                "llm_sentiment produced %d prototype rows (dry-run/no scores)",
                len(out),
            )

    except Exception:
        status = "failed"
        raise
    finally:
        logger.info(
            "Stage llm_sentiment finished: status=%s elapsed=%.3fs",
            status,
            time.perf_counter() - t0,
        )
