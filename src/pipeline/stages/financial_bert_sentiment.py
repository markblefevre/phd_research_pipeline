"""Pipeline adapter for Paper 2 Stage 6C Japanese Financial BERT sentiment."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.financial_bert_sentiment import (
    DEFAULT_BACKBONE,
    score_financial_bert_corpus,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_model_ref(root: Path, value: str) -> str:
    value = str(value).strip()
    if not value:
        raise ValueError("Empty BERT model reference")
    expanded = Path(value).expanduser()
    obvious_local = expanded.is_absolute() or value.startswith((".", "~", "models/", "data/", "outputs/"))
    if obvious_local:
        path = expanded if expanded.is_absolute() else root / expanded
        path = path.resolve()
        if not path.exists():
            raise FileNotFoundError(f"Configured local model does not exist: {path}")
        return str(path)
    return value


def _resolve_data_path(root: Path, value: str | Path) -> Path:
    p = Path(str(value)).expanduser()
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def run_stage_financial_bert_sentiment(
    *, paper: str, cfg: Dict[str, Any], logger: logging.Logger
) -> None:
    t0 = time.perf_counter()
    status = "ok"
    try:
        root = _repo_root()
        token_cfg = cfg.get("text_tokenization", {})
        mdna_cfg = cfg.get("mdna_extract", {})
        bert_cfg = cfg.get("financial_bert_sentiment", {})

        universe_variant = str(bert_cfg.get("universe_variant", "sudachi_c_raw"))
        token_dir = _resolve_data_path(
            root, token_cfg.get("output_dir", f"data/interim/{paper}/tokens")
        )
        manifest_csv = token_dir / universe_variant / "manifest.csv"

        mdna_root_value = bert_cfg.get("source_root") or mdna_cfg.get(
            "output_dir", f"data/interim/{paper}/mdna"
        )
        mdna_root = _resolve_data_path(root, mdna_root_value)

        backbone_model = str(bert_cfg.get("backbone_model", DEFAULT_BACKBONE))
        positive_ref = bert_cfg.get("positive_model")
        negative_ref = bert_cfg.get("negative_model")
        if not positive_ref or not negative_ref:
            raise ValueError(
                "[financial_bert_sentiment] requires positive_model and negative_model. "
                "These must be separately fine-tuned binary checkpoints derived from the Izumi backbone."
            )
        positive_model = _resolve_model_ref(root, str(positive_ref))
        negative_model = _resolve_model_ref(root, str(negative_ref))

        output_csv = _resolve_data_path(
            root,
            bert_cfg.get(
                "output_csv",
                f"data/interim/{paper}/sentiment/financial_bert/financial_bert_sentiment.csv",
            ),
        )
        metadata_json = output_csv.with_suffix(".metadata.json")

        logger.info("Stage financial_bert_sentiment: universe manifest=%s", manifest_csv)
        logger.info("Stage financial_bert_sentiment: MD&A root=%s", mdna_root)
        logger.info("Stage financial_bert_sentiment: backbone=%s", backbone_model)
        tokenizer_ref_cfg = bert_cfg.get("tokenizer_model")
        tokenizer_model = (
            _resolve_model_ref(root, str(tokenizer_ref_cfg)) if tokenizer_ref_cfg else positive_model
        )

        logger.info("Stage financial_bert_sentiment: positive model=%s", positive_model)
        logger.info("Stage financial_bert_sentiment: negative model=%s", negative_model)
        logger.info("Stage financial_bert_sentiment: tokenizer model=%s", tokenizer_model)
        logger.info("Stage financial_bert_sentiment: output=%s", output_csv)

        out = score_financial_bert_corpus(
            manifest_csv=manifest_csv,
            mdna_root=mdna_root,
            output_csv=output_csv,
            metadata_json=metadata_json,
            positive_model_ref=positive_model,
            negative_model_ref=negative_model,
            backbone_model=backbone_model,
            tokenizer_model_ref=tokenizer_model,
            universe_variant=universe_variant,
            min_chars=int(bert_cfg.get("min_chars", 2)),
            max_length=int(bert_cfg.get("max_length", 512)),
            threshold=float(bert_cfg.get("threshold", 0.5)),
            inference_batch_size=int(bert_cfg.get("inference_batch_size", 32)),
            document_block_size=int(bert_cfg.get("document_block_size", 16)),
            device=str(bert_cfg.get("device", "auto")),
            positive_absent_id=bert_cfg.get("positive_absent_id"),
            positive_present_id=bert_cfg.get("positive_present_id"),
            negative_absent_id=bert_cfg.get("negative_absent_id"),
            negative_present_id=bert_cfg.get("negative_present_id"),
            resume=bool(bert_cfg.get("resume", True)),
            overwrite=bool(bert_cfg.get("overwrite", False)),
            max_documents=bert_cfg.get("max_documents"),
            logger=logger,
        )
        logger.info(
            "financial_bert_sentiment produced %s document scores; mean bertNet=%.6f median=%.6f",
            len(out), out["bertNet"].mean(), out["bertNet"].median(),
        )
    except Exception:
        status = "failed"
        raise
    finally:
        logger.info(
            "Stage financial_bert_sentiment finished: status=%s elapsed=%.3fs",
            status, time.perf_counter() - t0,
        )
