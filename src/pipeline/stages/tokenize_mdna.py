from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from src.mdna_analysis.tokenize_mdna import run_tokenization
from src.mdna_analysis.normalize_tokens import run_numeric_normalization
from src.mdna_analysis.validate_tokenization import validate_tokenization_outputs


def _repo_root() -> Path:
    """Return the repository root for this stage adapter."""
    return Path(__file__).resolve().parents[3]


def run_stage_text_tokenization(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    """
    Pipeline-facing Stage 4 entry point.

    Stage 4 has three internal phases:
      4A. Tokenize requested raw Sudachi variants.
      4B. Optionally derive <NUM>-normalized variants.
      4C. Optionally validate the complete Stage 4 output family.
    """
    t0 = time.perf_counter()
    status = "ok"

    try:
        root = _repo_root()
        stage_cfg = cfg.get("text_tokenization", {})
        longitudinal_cfg = cfg.get("longitudinal_match", {})

        longitudinal_dir = root / longitudinal_cfg.get(
            "output_dir", f"data/interim/{paper}/longitudinal"
        )
        pairs_csv = longitudinal_dir / "research_eligible_pairs.csv"

        output_dir = root / stage_cfg.get(
            "output_dir", f"data/interim/{paper}/tokens"
        )

        max_workers = int(stage_cfg.get("max_workers", 6))
        overwrite = bool(stage_cfg.get("overwrite", False))
        variants = stage_cfg.get("variants", ["sudachi_c_raw"])
        derive_num_variants = bool(stage_cfg.get("derive_num_variants", True))
        run_qc = bool(stage_cfg.get("run_qc", True))

        if isinstance(variants, str):
            variants = [variants]

        source_root_cfg = stage_cfg.get("source_root")
        source_root = Path(source_root_cfg).expanduser() if source_root_cfg else None

        work_output_dir_cfg = stage_cfg.get("work_output_dir")
        work_output_dir = (
            Path(work_output_dir_cfg).expanduser() if work_output_dir_cfg else None
        )

        logger.info("Stage text_tokenization: pairs_csv=%s", pairs_csv)
        logger.info("Stage text_tokenization: output_dir=%s", output_dir)
        logger.info("Stage text_tokenization: source_root=%s", source_root)
        logger.info("Stage text_tokenization: work_output_dir=%s", work_output_dir)
        logger.info("Stage text_tokenization: max_workers=%s", max_workers)
        logger.info("Stage text_tokenization: overwrite=%s", overwrite)
        logger.info("Stage text_tokenization: variants=%s", variants)
        logger.info(
            "Stage text_tokenization: derive_num_variants=%s", derive_num_variants
        )
        logger.info("Stage text_tokenization: run_qc=%s", run_qc)

        if not pairs_csv.exists():
            raise FileNotFoundError(
                f"Missing Stage 3 research-eligible pairs: {pairs_csv}"
            )

        # 4A. Raw Sudachi tokenization
        logger.info("[RUN] text_tokenization")
        for variant in variants:
            if not variant.endswith("_raw"):
                raise ValueError(
                    f"Stage 4 raw variant must end in '_raw': {variant}"
                )

            logger.info("[RUN] text_tokenization variant=%s", variant)
            manifest = run_tokenization(
                pairs_csv=pairs_csv,
                output_dir=output_dir,
                repo_root=root,
                max_workers=max_workers,
                overwrite=overwrite,
                source_root=source_root,
                work_output_dir=work_output_dir,
                variant=variant,
            )

            counts = manifest["status"].value_counts(dropna=False).to_dict()
            logger.info(
                "text_tokenization variant=%s produced %s manifest rows; "
                "status_counts=%s",
                variant,
                len(manifest),
                counts,
            )

            failures = manifest[
                manifest["status"].isin(["missing_source", "error"])
            ]
            if not failures.empty:
                manifest_path = (
                    (work_output_dir or output_dir) / variant / "manifest.csv"
                )
                raise RuntimeError(
                    "Stage text_tokenization variant="
                    f"{variant} completed with {len(failures)} failed document(s). "
                    f"See {manifest_path}"
                )

        # 4B. Derived <NUM> variants
        num_variants: list[str] = []
        if derive_num_variants:
            logger.info("[RUN] numeric_normalization")
            for source_variant in variants:
                output_variant = source_variant.removesuffix("_raw") + "_num"
                num_variants.append(output_variant)

                logger.info(
                    "[RUN] numeric_normalization %s -> %s",
                    source_variant,
                    output_variant,
                )

                manifest = run_numeric_normalization(
                    output_dir=output_dir,
                    repo_root=root,
                    source_variant=source_variant,
                    output_variant=output_variant,
                    work_output_dir=work_output_dir,
                    max_workers=max_workers,
                    overwrite=overwrite,
                )

                counts = manifest["status"].value_counts(dropna=False).to_dict()
                replacements = int(manifest["replacementCount"].fillna(0).sum())
                logger.info(
                    "numeric_normalization variant=%s produced %s rows; "
                    "status_counts=%s replacements=%s",
                    output_variant,
                    len(manifest),
                    counts,
                    replacements,
                )

                failures = manifest[
                    manifest["status"].isin(["missing_source", "error"])
                ]
                if not failures.empty:
                    manifest_path = (
                        (work_output_dir or output_dir)
                        / output_variant
                        / "manifest.csv"
                    )
                    raise RuntimeError(
                        "Numeric normalization variant="
                        f"{output_variant} completed with "
                        f"{len(failures)} failed document(s). "
                        f"See {manifest_path}"
                    )

        # 4C. QC
        if run_qc:
            logger.info("[RUN] qc_check")
            validate_tokenization_outputs(
                token_root=(work_output_dir or output_dir),
                raw_variants=variants,
                num_variants=num_variants,
            )

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info(
            "Stage text_tokenization finished: status=%s elapsed=%.3fs",
            status,
            elapsed,
        )
