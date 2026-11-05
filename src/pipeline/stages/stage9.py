"""Stage 9 pipeline adapter. Consumes frozen Stage 8 artifacts; no Stage 8 imports."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
from src.mdna_analysis.stage9 import build_dataset, run_models, sha256


def run_stage9_pipeline(*, paper, cfg, logger, root, config_path):
    if paper != "paper2":
        raise ValueError("Stage 9 supports paper2 only")
    out = (Path(root) / cfg["stage9_reporting"]["output_dir"]).resolve()
    if out.exists():
        raise FileExistsError(f"Refusing to overwrite Stage 9 output: {out}")
    logger.info("Stage 9: validate and join frozen sources")
    dataset, missing, provenance = build_dataset(cfg, Path(root))
    logger.info("Stage 9: estimate baseline and momentum robustness on common samples")
    summary, coef, contrasts = run_models(dataset, cfg)
    # Only publish after all computations and invariants have completed.
    out.mkdir(parents=True, exist_ok=False)
    dataset.to_csv(out / "regression_dataset.csv", index=False)
    missing.to_csv(out / "missingness.csv", index=False)
    summary.to_csv(out / "model_summary.csv", index=False)
    coef.to_csv(out / "coefficients.csv", index=False)
    contrasts.to_csv(out / "coefficient_contrasts.csv", index=False)
    (out / "provenance.json").write_text(json.dumps({
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "config": str(config_path),
        "config_sha256": sha256(config_path),
        **provenance,
    }, indent=2) + "\n", encoding="utf-8")
    logger.info("Stage 9 complete: %s", out)
    logger.info("\n%s", summary[["model", "N", "firms", "joint_wald_p"]].to_string(index=False))
