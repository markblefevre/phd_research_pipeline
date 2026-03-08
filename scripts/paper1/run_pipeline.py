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
import time

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    import tomli as tomllib  # Python <=3.10


# ---- Import your stage(s) ----
# This assumes you can run: python -m src.event_study.run_event_study_all
from src.event_study.run_market_model import run_market_model
from src.event_study.run_event_study_all import run_event_study_all
from src.event_study.run_event_study_horserace import run_horserace
from src.panel.run_build_panel import build_panel


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
        logger.handlers.clear()

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
# Stage: Build Panel (GPT and LMMD)
# ----------------------------
import time  # make sure this is at top of file

def run_stage_build_panel(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        bp_cfg = cfg.get("build_panel", {})
        skip_if_exists = bool(bp_cfg.get("skip_if_exists", True))
        root = repo_root()

        gpt_rel = bp_cfg.get("gpt_csv")
        lmmd_rel = bp_cfg.get("lmmd_csv")
        out_rel = bp_cfg.get("out_csv")
        if not (gpt_rel and lmmd_rel and out_rel):
            raise ValueError("Config error: [build_panel] requires gpt_csv, lmmd_csv, out_csv")

        # Convert TOML strings -> absolute Paths
        gpt_csv = root / gpt_rel
        lmmd_csv = root / lmmd_rel
        out_csv = root / out_rel

        logger.info("Stage build_panel: gpt_csv=%s", gpt_csv)
        logger.info("Stage build_panel: lmmd_csv=%s", lmmd_csv)
        logger.info("Stage build_panel: out_csv=%s", out_csv)

        if skip_if_exists and out_csv.exists():
            status = "skipped"
            logger.info("[SKIP] build_panel already done at %s", out_csv)
            return

        if not gpt_csv.exists():
            raise FileNotFoundError(f"Missing GPT input: {gpt_csv}")
        if not lmmd_csv.exists():
            raise FileNotFoundError(f"Missing LMMD input: {lmmd_csv}")

        logger.info("[RUN] build_panel")
        df = build_panel(gpt_csv=gpt_csv, lmmd_csv=lmmd_csv, out_csv=out_csv)
        logger.info("build_panel rows=%s cols=%s -> %s", df.shape[0], df.shape[1], out_csv)

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage build_panel finished: status=%s elapsed=%.3fs", status, elapsed)


# ----------------------------
# Stage: Market model (alphas/betas)
# ----------------------------

def market_model_done(out_csv: Path) -> bool:
    return out_csv.exists()


def run_stage_market_model(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        mm_cfg = cfg.get("market_model", {})
        skip_if_exists = bool(mm_cfg.get("skip_if_exists", True))

        root = repo_root()

        market_csv = mm_cfg.get("market_csv", f"data/curated/{paper}/prices/TOPIX_prices.csv")
        stocks_csv = mm_cfg.get("stocks_csv", f"data/curated/{paper}/prices/prices_long.csv")
        out_csv = mm_cfg.get("out_csv", f"data/curated/{paper}/event_study/alphas_betas.csv")

        market_csv_p = root / market_csv
        stocks_csv_p = root / stocks_csv
        out_csv_p = root / out_csv

        logger.info("Stage market_model: market_csv=%s", market_csv_p)
        logger.info("Stage market_model: stocks_csv=%s", stocks_csv_p)
        logger.info("Stage market_model: out_csv=%s", out_csv_p)

        if skip_if_exists and market_model_done(out_csv_p):
            status = "skipped"
            logger.info("[SKIP] market_model already done at %s", out_csv_p)
            return

        if not market_csv_p.exists():
            raise FileNotFoundError(f"Missing market_csv: {market_csv_p}")
        if not stocks_csv_p.exists():
            raise FileNotFoundError(f"Missing stocks_csv: {stocks_csv_p}")

        logger.info("[RUN] market_model")
        res = run_market_model(
            market_csv=market_csv_p,
            stocks_csv=stocks_csv_p,
            out_csv=out_csv_p,
        )
        logger.info("market_model produced %s rows -> %s", res.shape[0], out_csv_p)

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage market_model finished: status=%s elapsed=%.3fs", status, elapsed)
    
# ----------------------------
# Stage: Event study
# ----------------------------

def event_study_out_dir(root: Path, paper: str, run_id: str) -> Path:
    return root / "outputs" / paper / run_id / "event_study"


def event_study_done(out_dir: Path, sentiment_col: str) -> bool:
    # Your stage writes this file:
    # regression_summary_{sentiment_col}.csv
    return (out_dir / f"regression_summary_{sentiment_col}.csv").exists()


def horserace_out_path(root: Path, paper: str, run_id: str) -> Path:
    return root / "outputs" / paper / run_id / "event_study" / "horserace_summary.csv"


def horserace_done(root: Path, paper: str, run_id: str) -> bool:
    return horserace_out_path(root, paper, run_id).exists()


def run_stage_event_study(
    *,
    paper: str,
    run_id: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        es_cfg = cfg.get("event_study", {})
        sentiment_cols = es_cfg.get("sentiment_cols", ["document_score"])
        windows = parse_windows(es_cfg.get("windows", [[0, 0], [0, 1], [-1, 1]]))
        skip_if_exists = bool(es_cfg.get("skip_if_exists", True))

        out_dir = ensure_dir(event_study_out_dir(repo_root(), paper, run_id))

        logger.info("Stage event_study: out_dir=%s", out_dir)
        logger.info("Stage event_study: windows=%s", windows)
        logger.info("Stage event_study: sentiment_cols=%s", sentiment_cols)

        ran_any = False
        all_skipped = True

        for col in sentiment_cols:
            if skip_if_exists and event_study_done(out_dir, col):
                logger.info("[SKIP] event_study already done for sentiment_col=%s", col)
                continue

            all_skipped = False
            ran_any = True

            inputs = es_cfg.get("inputs", {})
            root = repo_root()

            sentiment_csv = inputs.get("sentiment_csv")
            alphas_betas_csv = inputs.get("alphas_betas_csv")
            prices_csv = inputs.get("prices_csv")
            market_csv = inputs.get("market_csv")

            sentiment_csv = (root / sentiment_csv) if sentiment_csv else None
            alphas_betas_csv = (root / alphas_betas_csv) if alphas_betas_csv else None
            prices_csv = (root / prices_csv) if prices_csv else None
            market_csv = (root / market_csv) if market_csv else None

            logger.info("[RUN] event_study sentiment_col=%s", col)
            run_event_study_all(
                windows=windows,
                sentiment_col=col,
                paper=paper,
                run_id=run_id,
                sentiment_csv=sentiment_csv,
                alphas_betas_csv=alphas_betas_csv,
                prices_csv=prices_csv,
                market_csv=market_csv,
            )

        if all_skipped and not ran_any:
            status = "skipped"

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage event_study finished: status=%s elapsed=%.3fs", status, elapsed)


def run_stage_horserace(
    *,
    paper: str,
    run_id: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
    windows: List[Tuple[int, int]],
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        hs_cfg = cfg.get("horserace", {})
        skip_if_exists = bool(hs_cfg.get("skip_if_exists", True))

        hs_windows = hs_cfg.get("windows", None)
        if hs_windows is not None:
            windows = parse_windows(hs_windows)

        out_path = horserace_out_path(repo_root(), paper, run_id)

        logger.info("Stage horserace: out_path=%s", out_path)
        logger.info("Stage horserace: windows=%s", windows)

        if skip_if_exists and out_path.exists():
            status = "skipped"
            logger.info("[SKIP] horserace already done at %s", out_path)
            return

        inputs = hs_cfg.get("inputs", {})
        panel_csv = inputs.get("panel_csv")
        if panel_csv:
            panel_csv = repo_root() / panel_csv

        logger.info("[RUN] horserace")
        run_horserace(
            windows=windows,
            paper=paper,
            run_id=run_id,
            panel_csv=panel_csv,
        )

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage horserace finished: status=%s elapsed=%.3fs", status, elapsed)


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
    # Share windows from event_study to horserace
    es_cfg = cfg.get("event_study", {})
    windows = parse_windows(es_cfg.get("windows", [[0, 0], [0, 1], [-1, 1]]))

    # Stage execution order (expand as you add stages)
    ## STAGE build_panel (must run before event_study)
    if bool(stages.get("build_panel", False)):
        run_stage_build_panel(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage build_panel disabled")
        
    ## STAGE market_model (must run before event_study if alphas_betas not present)
    if bool(stages.get("market_model", False)):
        run_stage_market_model(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage market_model disabled")

    ## STAGE event_study
    if bool(stages.get("event_study", False)):
        run_stage_event_study(paper=paper, run_id=run_id, cfg=cfg, logger=logger)
    else:
        logger.info("Stage event_study disabled")

    ## STAGE horserace
    if bool(stages.get("horserace", False)):
        run_stage_horserace(paper=paper, run_id=run_id, cfg=cfg, logger=logger, windows=windows)
    else:
        logger.info("Stage horserace disabled")
    
    logger.info("Pipeline done: paper=%s run_id=%s", paper, run_id)
    logger.info("Outputs base: %s", out_base)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
