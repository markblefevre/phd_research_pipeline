#!/usr/bin/env python3
"""Standalone Stage 6F-B entrypoint; does not execute or edit run_pipeline.py.

Usage (from repo root):
  python -m scripts.paper2.run_component_sentiment --max-documents 3
  python -m scripts.paper2.run_component_sentiment --sample-documents 10 --live
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from src.pipeline.stages.persistent_novel_component_sentiment import (
    run_stage_persistent_novel_component_sentiment,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 6F-B component sentiment (non-overwriting)")
    parser.add_argument("--max-documents", type=int, help="First N pairs, useful for smoke tests")
    parser.add_argument("--sample-documents", type=int, help="Deterministic uniform sample of N pairs")
    parser.add_argument("--sample-seed", type=int, default=42)
    parser.add_argument("--max-concurrent-requests", type=int)
    parser.add_argument("--live", action="store_true", help="Explicitly enable paid OpenAI scoring")
    args = parser.parse_args()
    if args.max_documents and args.sample_documents:
        parser.error("Choose --max-documents OR --sample-documents")
    root = Path(__file__).resolve().parents[2]
    config_path = root / "configs/paper2/persistent_novel_component_sentiment.toml"
    with config_path.open("rb") as fh:
        cfg = tomllib.load(fh)
    stage_cfg = cfg["persistent_novel_component_sentiment"]
    if args.max_documents is not None:
        stage_cfg["max_documents"] = args.max_documents
    if args.sample_documents is not None:
        stage_cfg["sample_documents"] = args.sample_documents
    stage_cfg["sample_seed"] = args.sample_seed
    if args.max_concurrent_requests is not None:
        stage_cfg["max_concurrent_requests"] = args.max_concurrent_requests
    # CLI flag is required for live scoring, irrespective of TOML content.
    stage_cfg["dry_run"] = not args.live
    if args.live and not (args.max_documents or args.sample_documents):
        parser.error("For safety, --live requires --max-documents or --sample-documents")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    result = run_stage_persistent_novel_component_sentiment(
        paper="paper2", cfg=cfg, logger=logging.getLogger("stage6fb")
    )
    print(json.dumps({k: result[k] for k in (
        "documents", "dry_run", "model", "status_counts", "new_input_tokens",
        "new_output_tokens", "results_csv", "run_id")}, indent=2))


if __name__ == "__main__":
    main()
