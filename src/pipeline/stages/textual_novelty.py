from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.textual_novelty import (
    run_pair_diagnostics,
    run_textual_novelty,
)


def _repo_root() -> Path:
    """Return the repository root for this stage adapter."""
    # src/pipeline/stages/textual_novelty.py -> parents[3] is repo root
    return Path(__file__).resolve().parents[3]


def run_stage_textual_novelty(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    """
    Pipeline-facing Stage 5 entry point.

    Fits corpus-wide TF-IDF on the validated Stage 4 document universe and
    computes adjacent-period cosine similarity / novelty for the frozen
    Stage 3 research-eligible pair manifest.

    Stage 5 follows the same canonical-vs-physical path pattern as Stage 4:
    canonical paths remain repo/NAS-relative, while optional local SSD roots
    are used for high-I/O reads and writes.
    """
    t0 = time.perf_counter()
    status = "ok"

    try:
        root = _repo_root()

        longitudinal_cfg = cfg.get("longitudinal_match", {})
        token_cfg = cfg.get("text_tokenization", {})
        novelty_cfg = cfg.get("textual_novelty", {})

        longitudinal_dir = root / longitudinal_cfg.get(
            "output_dir",
            f"data/interim/{paper}/longitudinal",
        )
        pairs_csv = longitudinal_dir / "research_eligible_pairs.csv"

        canonical_token_root = root / token_cfg.get(
            "output_dir",
            f"data/interim/{paper}/tokens",
        )

        source_root_cfg = novelty_cfg.get("source_root")
        source_root = (
            Path(source_root_cfg).expanduser()
            if source_root_cfg
            else canonical_token_root
        )


        output_dir = root / novelty_cfg.get(
            "output_dir",
            f"data/interim/{paper}/novelty",
        )

        work_output_dir_cfg = novelty_cfg.get("work_output_dir")
        work_output_dir = (
            Path(work_output_dir_cfg).expanduser()
            if work_output_dir_cfg
            else None
        )

        variants = novelty_cfg.get(
            "variants",
            ["sudachi_c_num"],
        )
        
        if isinstance(variants, str):
            variants = [variants]
        min_df = int(novelty_cfg.get("min_df", 2))
        max_df = float(novelty_cfg.get("max_df", 1.0))
        ngram_min = int(novelty_cfg.get("ngram_min", 1))
        ngram_max = int(novelty_cfg.get("ngram_max", 1))
        sublinear_tf = bool(
            novelty_cfg.get("sublinear_tf", False)
        )

        logger.info(
            "Stage textual_novelty: pairs_csv=%s",
            pairs_csv,
        )
        logger.info(
            "Stage textual_novelty: source_root=%s",
            source_root,
        )
        logger.info(
            "Stage textual_novelty: output_dir=%s",
            output_dir,
        )
        logger.info(
            "Stage textual_novelty: work_output_dir=%s",
            work_output_dir,
        )
        logger.info(
            "Stage textual_novelty: variants=%s",
            variants,
        )
        logger.info(
            "Stage textual_novelty: "
            "min_df=%s max_df=%s ngram_range=(%s,%s) "
            "sublinear_tf=%s",
            min_df,
            max_df,
            ngram_min,
            ngram_max,
            sublinear_tf,
        )

        if not pairs_csv.exists():
            raise FileNotFoundError(
                f"Missing Stage 3 research-eligible pairs: {pairs_csv}"
            )

        logger.info("[RUN] textual_novelty pair diagnostics")
        diagnostics = run_pair_diagnostics(
            pairs_csv=pairs_csv,
            output_dir=output_dir,
            work_output_dir=work_output_dir,
        )
        logger.info(
            "textual_novelty pair diagnostics produced %s rows",
            len(diagnostics),
        )
        logger.info(
            "textual_novelty length diagnostics: "
            "median ratio=%.6f median abs log change=%.6f",
            diagnostics["lengthRatio"].median(),
            diagnostics["absLogLengthChange"].median(),
        )

        for variant in variants:
            logger.info(
                "[RUN] textual_novelty variant=%s",
                variant,
            )
        
            result = run_textual_novelty(
                pairs_csv=pairs_csv,
                output_dir=output_dir,
                repo_root=root,
                variant=variant,
                source_root=source_root,
                work_output_dir=work_output_dir,
                min_df=min_df,
                max_df=max_df,
                ngram_min=ngram_min,
                ngram_max=ngram_max,
                sublinear_tf=sublinear_tf,
            )
        
            logger.info(
                "textual_novelty variant=%s produced %s pair rows",
                variant,
                len(result),
            )
        
            logger.info(
                "textual_novelty variant=%s "
                "cosine mean=%.6f median=%.6f",
                variant,
                result["cosineSimilarity"].mean(),
                result["cosineSimilarity"].median(),
            )
        
            logger.info(
                "textual_novelty variant=%s "
                "novelty mean=%.6f median=%.6f",
                variant,
                result["novelty"].mean(),
                result["novelty"].median(),
            )

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info(
            "Stage textual_novelty finished: "
            "status=%s elapsed=%.3fs",
            status,
            elapsed,
        )
