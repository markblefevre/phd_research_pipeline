"""Pipeline adapter for Stage 6D LLM visualization / QC."""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any
from src.mdna_analysis.llm_plots import generate_llm_plots


def _root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve(root: Path, path: str) -> Path:
    p = Path(path).expanduser()
    return p if p.is_absolute() else root / p


def run_stage_llm_plots(*, paper: str, cfg: dict[str, Any], logger: logging.Logger) -> None:
    start = time.perf_counter()
    status = 'ok'
    try:
        root = _root()
        sc = cfg.get('llm_sentiment', {})
        pc = cfg.get('llm_plots', {})
        ec = cfg.get('edinet_download', {})
        llm = _resolve(root, sc.get('output_csv', f'data/interim/{paper}/sentiment/llm/llm_sentiment.csv'))
        filings = _resolve(root, ec.get('metadata_csv', f'data/interim/{paper}/edinet/filings.csv'))
        out = _resolve(root, pc.get('output_dir', f'outputs/{paper}/figures/sentiment/llm'))
        lmmd_path = _resolve(root, cfg.get('lmmd_sentiment', {}).get('output_csv',
                                 f'data/interim/{paper}/sentiment/lmmd/lmmd_sentiment.csv'))
        bert_path = _resolve(root, cfg.get('financial_bert_sentiment', {}).get('output_csv',
                                 f'data/interim/{paper}/sentiment/financial_bert/financial_bert_sentiment.csv'))
        # Optional comparisons: missing files should not block the LLM-only plots.
        logger.info('Stage llm_plots: input=%s filings=%s output=%s', llm, filings, out)
        annual = generate_llm_plots(
            llm_csv=llm, filings_csv=filings, output_dir=out,
            lmmd_csv=lmmd_path if lmmd_path.exists() else None,
            bert_csv=bert_path if bert_path.exists() else None,
            formats=tuple(pc.get('formats', ['pdf', 'png'])),
            dpi=int(pc.get('dpi', 180)),
            min_year_observations=int(pc.get('min_year_observations', 25)))
        logger.info('llm_plots produced annual summary for %d fiscal years', len(annual))
    except Exception:
        status = 'failed'
        logger.exception('Stage llm_plots failed')
        raise
    finally:
        logger.info('Stage llm_plots finished: status=%s elapsed=%.3fs',
                    status, time.perf_counter() - start)
