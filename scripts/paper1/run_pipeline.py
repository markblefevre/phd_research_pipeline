#!/usr/bin/env python3
from __future__ import annotations

import sys
import json
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    import tomli as tomllib  # Python <=3.10


# ---- Import your stage(s) ----
# This assumes you can run: python -m src.event_study.run_event_study_all
from src.event_study.run_event_study_all import run_event_study_all


# ----------------------------
# Helpers
# ----------------------------

def repo_root() -> Path:
    # scripts/paper1/run_pipeline.py -> parents[2] is repo root
    return Path(__file__).resolve().parents[2]


def load_toml(path: Path) -> Dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def now_run_id() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def get_git_commit_short(root: Path) -> str | None:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(root),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or None
    except Exception:
        return None


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def setup_logging(log_file: Path) -> logging.Logger:
    ensure_dir(log_file.parent)

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if re-run in same interpreter session (Spyder)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


def write_run_metadata(
    run_dir: Path,
    cfg: Dict[str, Any],
    run_id: str,
    paper: str,
    logger: logging.Logger,
) -> None:
    ensure_dir(run_dir)

    # command used
    (run_dir / "command.txt").write_text(" ".join(sys.argv) + "\n", encoding="utf-8")

    # git info
    commit = get_git_commit_short(repo_root())
    (run_dir / "git.txt").write_text((commit or "unknown") + "\n", encoding="utf-8")

    # resolved config snapshot (json is easiest without extra deps)
    snapshot = {
        "paper": paper,
        "run_id": run_id,
        "config": cfg,
    }
    (run_dir / "config_resolved.json").write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    logger.info("Run metadata written to %s", run_dir)


def parse_windows(raw: Any) -> List[Tuple[int, int]]:
    """
    raw should look like: [[0,0],[0,1],[-1,1]]
    """
    windows: List[Tuple[int, int]] = []
    if raw is None:
        return windows
    for w in raw:
        if not (isinstance(w, (list, tuple)) and len(w) == 2):
            raise ValueError(f"Invalid window entry: {w!r} (expected [a,b])")
        windows.append((int(w[0]), int(w[1])))
    return windows


# ----------------------------
# Stage: Event study
# ----------------------------

def event_study_out_dir(root: Path, paper: str, run_id: str) -> Path:
    return root / "outputs" / paper / run_id / "event_study"


def event_study_done(out_dir: Path, sentiment_col: str) -> bool:
    # Your stage writes this file:
    # regression_summary_{sentiment_col}.csv
    return (out_dir / f"regression_summary_{sentiment_col}.csv").exists()


def run_stage_event_study(
    *,
    paper: str,
    run_id: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    es_cfg = cfg.get("event_study", {})
    sentiment_cols = es_cfg.get("sentiment_cols", ["document_score"])
    windows = parse_windows(es_cfg.get("windows", [[0, 0], [0, 1], [-1, 1]]))
    skip_if_exists = bool(es_cfg.get("skip_if_exists", True))

    out_dir = ensure_dir(event_study_out_dir(repo_root(), paper, run_id))

    logger.info("Stage event_study: out_dir=%s", out_dir)
    logger.info("Stage event_study: windows=%s", windows)
    logger.info("Stage event_study: sentiment_cols=%s", sentiment_cols)

    for col in sentiment_cols:
        if skip_if_exists and event_study_done(out_dir, col):
            logger.info("[SKIP] event_study already done for sentiment_col=%s", col)
            continue

        logger.info("[RUN] event_study sentiment_col=%s", col)
        run_event_study_all(
            windows=windows,
            sentiment_col=col,
            paper=paper,
            run_id=run_id,
        )


# ----------------------------
# Main pipeline
# ----------------------------

def main() -> int:
    root = repo_root()
    config_path = root / "configs" / "paper1" / "pipeline.toml"
    if not config_path.exists():
        print(f"[ERROR] Missing config: {config_path}")
        return 2

    cfg = load_toml(config_path)

    # Run identity: ONE run_id for ALL stages
    run_cfg = cfg.get("run", {})
    paper = run_cfg.get("paper", "paper1")

    run_id_cfg = run_cfg.get("run_id", "auto")
    run_id = run_id_cfg if (isinstance(run_id_cfg, str) and run_id_cfg != "auto") else now_run_id()

    # Standard run folders
    out_base = ensure_dir(root / "outputs" / paper / run_id)
    run_dir = ensure_dir(root / "runs" / paper / run_id)
    log_file = out_base / "logs" / "run.log"

    logger = setup_logging(log_file)
    logger.info("Pipeline start: paper=%s run_id=%s", paper, run_id)
    logger.info("Repo root: %s", root)
    logger.info("Config path: %s", config_path)

    write_run_metadata(run_dir, cfg, run_id, paper, logger)

    stages = cfg.get("stages", {})
    if not isinstance(stages, dict):
        logger.error("Config error: [stages] must be a table/dict")
        return 2

    # Stage execution order (expand as you add stages)
    if bool(stages.get("event_study", False)):
        run_stage_event_study(paper=paper, run_id=run_id, cfg=cfg, logger=logger)
    else:
        logger.info("Stage event_study disabled")

    logger.info("Pipeline done: paper=%s run_id=%s", paper, run_id)
    logger.info("Outputs base: %s", out_base)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
