"""Pipeline adapter for Paper 2 Stage 6E: persistent/novel MD&A split."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.persistent_novel_split import run_persistent_novel_split


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve(root: Path, value: str | Path) -> Path:
    p = Path(str(value)).expanduser()
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def run_stage_persistent_novel_split(
    *, paper: str, cfg: Dict[str, Any], logger: logging.Logger
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        root = _repo_root()
        stage_cfg = cfg.get("persistent_novel_split", {})
        lm_cfg = cfg.get("longitudinal_match", {})
        mdna_cfg = cfg.get("mdna_extract", {})
        token_cfg = cfg.get("text_tokenization", {})

        pairs_csv = _resolve(
            root,
            stage_cfg.get(
                "pairs_csv",
                f"data/interim/{paper}/longitudinal/research_eligible_pairs.csv",
            ),
        )
        source_value = (
            stage_cfg.get("source_root")
            or token_cfg.get("source_root")
            or mdna_cfg.get("output_dir", f"data/interim/{paper}/mdna")
        )
        mdna_root = _resolve(root, source_value)
        concepts_csv = _resolve(
            root,
            stage_cfg.get(
                "concepts_csv",
                f"data/interim/{paper}/alignment/edinet_concept_dictionary.csv",
            ),
        )
        idf_cache = _resolve(
            root,
            stage_cfg.get(
                "idf_cache",
                f"data/interim/{paper}/alignment/concept_alias_idf.csv",
            ),
        )
        prompt_file = _resolve(
            root,
            stage_cfg.get(
                "prompt_file",
                "configs/paper2/prompts/persistent_novel_classifier_v1_1.md",
            ),
        )
        output_dir = _resolve(
            root,
            stage_cfg.get(
                "output_dir",
                f"data/interim/{paper}/alignment/persistent_novel",
            ),
        )

        for label, path in (
            ("pairs_csv", pairs_csv),
            ("mdna_root", mdna_root),
            ("concepts_csv", concepts_csv),
            ("idf_cache", idf_cache),
            ("prompt_file", prompt_file),
        ):
            if not path.exists():
                raise FileNotFoundError(f"Stage 6E missing {label}: {path}")

        max_pairs = stage_cfg.get("max_pairs")
        max_sentences = stage_cfg.get("max_sentences")
        max_pairs = int(max_pairs) if max_pairs is not None else None
        max_sentences = (
            int(max_sentences) if max_sentences is not None else None
        )

        logger.info("Stage persistent_novel_split: pairs=%s", pairs_csv)
        logger.info("Stage persistent_novel_split: MD&A root=%s", mdna_root)
        logger.info("Stage persistent_novel_split: concepts=%s", concepts_csv)
        logger.info("Stage persistent_novel_split: IDF cache=%s", idf_cache)
        logger.info("Stage persistent_novel_split: prompt=%s", prompt_file)
        logger.info("Stage persistent_novel_split: output=%s", output_dir)
        logger.info(
            "Stage persistent_novel_split: retrieval top-k=%s rerank top-k=%s lambda=%s",
            int(stage_cfg.get("retrieval_top_k", 10)),
            int(stage_cfg.get("rerank_top_k", 3)),
            float(stage_cfg.get("rerank_lambda", 0.02)),
        )
        logger.info(
            "Stage persistent_novel_split: classifier=%s retrieval_only=%s concurrency=%s",
            str(stage_cfg.get("classifier_model", "gpt-6-luna")),
            bool(stage_cfg.get("retrieval_only", True)),
            int(stage_cfg.get("max_concurrent_requests", 4)),
        )

        summary = run_persistent_novel_split(
            pairs_csv=pairs_csv,
            mdna_root=mdna_root,
            concepts_csv=concepts_csv,
            idf_cache=idf_cache,
            prompt_file=prompt_file,
            output_dir=output_dir,
            retriever_model=str(
                stage_cfg.get("retriever_model", "cl-nagoya/ruri-v3-310m")
            ),
            retrieval_top_k=int(stage_cfg.get("retrieval_top_k", 10)),
            rerank_top_k=int(stage_cfg.get("rerank_top_k", 3)),
            rerank_lambda=float(stage_cfg.get("rerank_lambda", 0.02)),
            min_sentence_chars=int(stage_cfg.get("min_sentence_chars", 12)),
            embedding_batch_size=int(
                stage_cfg.get("embedding_batch_size", 32)
            ),
            device=str(stage_cfg.get("device", "auto")),
            classifier_model=str(
                stage_cfg.get("classifier_model", "gpt-6-luna")
            ),
            reasoning_effort=str(stage_cfg.get("reasoning_effort", "none")),
            max_concurrent_requests=int(
                stage_cfg.get("max_concurrent_requests", 4)
            ),
            max_retries=int(stage_cfg.get("max_retries", 3)),
            retry_base_seconds=float(
                stage_cfg.get("retry_base_seconds", 2.0)
            ),
            resume=bool(stage_cfg.get("resume", True)),
            overwrite=bool(stage_cfg.get("overwrite", False)),
            retrieval_only=bool(stage_cfg.get("retrieval_only", True)),
            max_pairs=max_pairs,
            max_sentences=max_sentences,
            logger=logger,
        )
        logger.info("persistent_novel_split summary: %s", summary)

    except Exception:
        status = "failed"
        raise
    finally:
        logger.info(
            "Stage persistent_novel_split finished: status=%s elapsed=%.3fs",
            status,
            time.perf_counter() - t0,
        )
