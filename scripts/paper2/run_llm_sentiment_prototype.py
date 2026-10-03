#!/usr/bin/env python3
"""Run only Paper 2 Stage 6D on the four Toyota/MUFG prototype MD&As.

This intentionally mimics the production run_pipeline.py call boundary:
    run_stage_llm_sentiment(paper=paper, cfg=cfg, logger=logger)

It is therefore disposable as an orchestrator but not as a methodology/code path:
the same adapter and doer can later be called directly from run_pipeline.py.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

try:
    import tomllib
except ModuleNotFoundError:  # Python <=3.10
    import tomli as tomllib

from src.pipeline.stages.llm_sentiment import run_stage_llm_sentiment


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_toml(path: Path) -> Dict[str, Any]:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def setup_logging(log_file: Path) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


def parse_args() -> argparse.Namespace:
    root = repo_root()
    ap = argparse.ArgumentParser(
        description="Prototype Paper 2 Stage 6D on Toyota/MUFG MD&A."
    )
    ap.add_argument(
        "--config",
        type=Path,
        default=root / "configs/paper2/llm_sentiment_prototype.toml",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Clean/build units and write previews without calling OpenAI.",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Ignore saved unit JSON and rescore documents.",
    )
    ap.add_argument(
        "--model",
        type=str,
        default=None,
        help="Override configured model, e.g. gpt-6-luna.",
    )
    ap.add_argument(
        "--source-root",
        type=Path,
        default=None,
        help="Override local MD&A root.",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    config_path = args.config.expanduser().resolve()

    if not config_path.exists():
        print(f"[ERROR] Missing config: {config_path}")
        return 2

    cfg = load_toml(config_path)
    paper = str(cfg.get("run", {}).get("paper", "paper2"))
    stage_cfg = cfg.setdefault("llm_sentiment", {})

    if args.dry_run:
        stage_cfg["dry_run"] = True
    if args.overwrite:
        stage_cfg["overwrite"] = True
        stage_cfg["resume"] = False
    if args.model:
        stage_cfg["model"] = args.model
    if args.source_root:
        stage_cfg["source_root"] = str(args.source_root.expanduser().resolve())

    run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_file = root / "outputs" / paper / run_id / "logs" / "llm_sentiment_prototype.log"
    logger = setup_logging(log_file)

    logger.info("LLM prototype start: paper=%s", paper)
    logger.info("Repo root: %s", root)
    logger.info("Config: %s", config_path)
    logger.info("Overrides: dry_run=%s overwrite=%s model=%s source_root=%s",
                args.dry_run, args.overwrite, args.model, args.source_root)

    run_stage_llm_sentiment(paper=paper, cfg=cfg, logger=logger)

    logger.info("LLM prototype complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
