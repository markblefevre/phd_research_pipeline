#!/usr/bin/env python3
from __future__ import annotations

import sys
import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    import tomli as tomllib  # Python <=3.10


# ---- Import your stage(s) ----
from src.pipeline.stages.edinet_download import run_stage_edinet_download
from src.pipeline.stages.mdna_extract import run_stage_mdna_extract
from src.pipeline.stages.longitudinal_match import run_stage_longitudinal_match
from src.pipeline.stages.tokenize_mdna import run_stage_text_tokenization
from src.pipeline.stages.textual_novelty import run_stage_textual_novelty
from src.pipeline.stages.novelty_plots import run_stage_novelty_plots
from src.pipeline.stages.analysis_panel import run_stage_analysis_panel
from src.pipeline.stages.lmmd_sentiment import run_stage_lmmd_sentiment
from src.pipeline.stages.lmmd_plots import run_stage_lmmd_plots
from src.pipeline.stages.financial_bert_sentiment import run_stage_financial_bert_sentiment
from src.pipeline.stages.financial_bert_plots import run_stage_financial_bert_plots
from src.pipeline.stages.llm_sentiment import run_stage_llm_sentiment
from src.pipeline.stages.llm_plots import run_stage_llm_plots
from src.pipeline.stages.market_reaction_eligibility import run_stage_market_reaction_eligibility
from src.pipeline.stages.market_reaction_event_dates import run_stage_market_reaction_event_dates
from src.pipeline.stages.market_reaction_topix import run_stage_market_reaction_topix
from src.pipeline.stages.market_reaction_market_model import run_stage_market_reaction_market_model
from src.pipeline.stages.market_reaction_abnormal_returns import (run_stage_market_reaction_abnormal_returns)
from src.pipeline.stages.market_reaction_final_table import (run_stage_market_reaction_final_table)
from src.pipeline.stages.empirical_panel import (run_stage_empirical_panel)
from src.pipeline.stages.baseline_regressions import (run_stage_baseline_regressions)
from src.pipeline.stages.marginal_effects import run_stage_marginal_effects

# ----------------------------
# Helpers
# ----------------------------

def repo_root() -> Path:
    # scripts/paper2/run_pipeline.py -> parents[2] is repo root
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


# ----------------------------
# Main pipeline
# ----------------------------

def main() -> int:
    root = repo_root()
    config_path = root / "configs" / "paper2" / "pipeline.toml"
    if not config_path.exists():
        print(f"[ERROR] Missing config: {config_path}")
        return 2

    cfg = load_toml(config_path)
    print(f"Loaded config: {config_path}")
    print(f"Paper: {cfg.get('run', {}).get('paper')}")
    print(f"Run ID: {cfg.get('run', {}).get('run_id')}")
    print(f"Stages: {cfg.get('stages', {})}")


    # Run identity: ONE run_id for ALL stages
    run_cfg = cfg.get("run", {})
    paper = run_cfg.get("paper", "paper2")

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
    # Stage1: EDINET download
    if bool(stages.get("edinet_download", False)):
        run_stage_edinet_download(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage edinet_download disabled")

    # Stage2: MDNA extract
    if bool(stages.get("mdna_extract", False)):
        run_stage_mdna_extract(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage mdna_extract disabled")

    # Stage 3: Longitudinal matching
    if bool(stages.get("longitudinal_match", False)):
        run_stage_longitudinal_match(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage longitudinal_match disabled")

    # Stage 4: Japanese text tokenization / preprocessing
    if bool(stages.get("text_tokenization", False)):
        run_stage_text_tokenization(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage text_tokenization disabled")
    
    # Stage 5: Corpus-wide TF-IDF textual novelty
    if bool(stages.get("textual_novelty", False)):
        run_stage_textual_novelty(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage textual_novelty disabled")

    # Stage 5 visualization / QC (consumes frozen Stage 3 + Stage 5 outputs)
    if bool(stages.get("novelty_plots", False)):
        run_stage_novelty_plots(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage novelty_plots disabled")

    # Stage 6A: frozen pair-level analysis-panel foundation
    if bool(stages.get("analysis_panel", False)):
        run_stage_analysis_panel(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage analysis_panel disabled")

    # Stage 6B: document-level LMMD lexical sentiment benchmark
    if bool(stages.get("lmmd_sentiment", False)):
        run_stage_lmmd_sentiment(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage lmmd_sentiment disabled")
    if bool(stages.get("lmmd_plots", False)):
        run_stage_lmmd_plots(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage lmmd_plots disabled")

    # Stage 6C: Japanese Financial BERT sentiment
    if stages.get("financial_bert_sentiment", False):
        run_stage_financial_bert_sentiment(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage financial_bert_sentiment disabled")
    if stages.get("financial_bert_plots", False):
        run_stage_financial_bert_plots(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage financial_bert_plots disabled")

    # Stage 6D: LLM sentiment
    if bool(stages.get("llm_sentiment", False)):
        run_stage_llm_sentiment(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage llm_sentiment disabled")
    # Stage 6D: LLM visualization / QC
    if bool(stages.get("llm_plots", False)):
        run_stage_llm_plots(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage llm_plots disabled")
        
    # Stage 7A: market-reaction event eligibility
    if bool(stages.get("market_reaction_eligibility", False)):
        run_stage_market_reaction_eligibility(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage market_reaction_eligibility disabled")
    # Stage 7B: timestamp-aware event trading dates
    if bool(stages.get("market_reaction_event_dates", False)):
        run_stage_market_reaction_event_dates(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage market_reaction_event_dates disabled")
    # Stage 7C
    if bool(stages.get("market_reaction_topix", False)):
        run_stage_market_reaction_topix(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage market_reaction_topix disabled")
    # Stage 7D: event-specific market-model estimation
    if bool(stages.get("market_reaction_market_model", False)):
        run_stage_market_reaction_market_model(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage market_reaction_market_model disabled")
    # Stage 7E: abnormal returns / CARs
    if bool(stages.get("market_reaction_abnormal_returns", False)):
        run_stage_market_reaction_abnormal_returns(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage market_reaction_abnormal_returns disabled")
    # Stage 7F: final market-reaction table / QC
    if bool(stages.get("market_reaction_final_table", False)):
        run_stage_market_reaction_final_table(
            paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage market_reaction_final_table disabled")

    # Stage 8A: empirical analysis panel
    if bool(stages.get("empirical_panel", False)):
        run_stage_empirical_panel(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage empirical_panel disabled")
    # Stage 8B: baseline regressions
    if bool(stages.get("baseline_regressions", False)):
        run_stage_baseline_regressions(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage baseline_regressions disabled")
    # Stage 8C: standardized marginal effects and multiple-testing diagnostics
    if bool(stages.get("marginal_effects", False)):
        run_stage_marginal_effects(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage marginal_effects disabled")
    
    logger.info("Pipeline done: paper=%s run_id=%s", paper, run_id)
    logger.info("Outputs base: %s", out_base)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
