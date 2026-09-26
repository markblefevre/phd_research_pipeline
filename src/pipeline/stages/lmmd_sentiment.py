"""Pipeline adapter for Stage 6B: LMMD document-level sentiment."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.lmmd_sentiment import score_lmmd_corpus


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_stage_lmmd_sentiment(*, paper: str, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    t0 = time.perf_counter()
    status = "ok"
    try:
        root = _repo_root()
        token_cfg = cfg.get("text_tokenization", {})
        lmmd_cfg = cfg.get("lmmd_sentiment", {})

        variant = str(lmmd_cfg.get("variant", "sudachi_c_raw"))
        canonical_token_dir = root / token_cfg.get("output_dir", f"data/interim/{paper}/tokens")
        manifest_csv = canonical_token_dir / variant / "manifest.csv"

        token_root_cfg = lmmd_cfg.get("token_root") or token_cfg.get("work_output_dir")
        token_root_base = Path(token_root_cfg).expanduser() if token_root_cfg else canonical_token_dir
        token_root = token_root_base / variant

        dict_cfg = lmmd_cfg.get("lmmd_dict_csv")
        if not dict_cfg:
            raise ValueError("Config error: [lmmd_sentiment] requires lmmd_dict_csv")
        lmmd_dict_csv = root / dict_cfg

        output_csv = root / lmmd_cfg.get(
            "output_csv", f"data/interim/{paper}/sentiment/lmmd/lmmd_sentiment.csv"
        )
        metadata_json = output_csv.with_suffix(".metadata.json")

        logger.info("Stage lmmd_sentiment: manifest_csv=%s", manifest_csv)
        logger.info("Stage lmmd_sentiment: token_root=%s", token_root)
        logger.info("Stage lmmd_sentiment: lmmd_dict_csv=%s", lmmd_dict_csv)
        logger.info("Stage lmmd_sentiment: output_csv=%s", output_csv)

        out = score_lmmd_corpus(
            manifest_csv=manifest_csv,
            token_root=token_root,
            lmmd_dict_csv=lmmd_dict_csv,
            output_csv=output_csv,
            metadata_json=metadata_json,
            token_col=str(lmmd_cfg.get("token_col", "GPT_JA")),
            variant=variant,
        )
        logger.info(
            "lmmd_sentiment produced %s document scores; mean lmmdNet=%.6f median=%.6f",
            len(out), out["lmmdNet"].mean(), out["lmmdNet"].median(),
        )
    except Exception:
        status = "failed"
        raise
    finally:
        logger.info(
            "Stage lmmd_sentiment finished: status=%s elapsed=%.3fs",
            status, time.perf_counter() - t0,
        )
