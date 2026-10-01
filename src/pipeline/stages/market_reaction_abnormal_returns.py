from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from src.market_reaction.abnormal_returns import (
    build_abnormal_returns_summary,
    compute_abnormal_returns,
)
from src.market_reaction.market_model import (
    build_stock_return_index,
    load_topix_returns,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve(root: Path, value: str | Path) -> Path:
    p = Path(value).expanduser()
    return p if p.is_absolute() else root / p


def run_stage_market_reaction_abnormal_returns(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    """
    Stage 7E pipeline adapter.

    Reads the frozen/current Stage 7B event dates, Stage 7D market-model
    estimates, Stage 7C TOPIX returns, and raw J-Quants stock-price files.
    Writes event-level abnormal returns/CARs plus a compact JSON QC summary.
    """
    root = _repo_root()
    c = cfg.get("market_reaction_abnormal_returns", {})

    event_dates_csv = _resolve(
        root,
        c.get(
            "event_dates_csv",
            "data/interim/paper2/market_reaction/stage7_event_dates.csv",
        ),
    )
    market_model_csv = _resolve(
        root,
        c.get(
            "market_model_csv",
            "data/interim/paper2/market_reaction/market_model_estimates.csv",
        ),
    )
    topix_returns_csv = _resolve(
        root,
        c.get(
            "topix_returns_csv",
            "data/interim/paper2/market_reaction/topix_returns.csv",
        ),
    )
    prices_dir = _resolve(
        root,
        c.get("prices_dir", "data/raw/paper2/prices"),
    )
    output_csv = _resolve(
        root,
        c.get(
            "output_csv",
            "data/interim/paper2/market_reaction/abnormal_returns.csv",
        ),
    )
    summary_json = _resolve(
        root,
        c.get(
            "summary_json",
            "data/interim/paper2/market_reaction/abnormal_returns_summary.json",
        ),
    )
    windows = c.get("windows", [[0, 0], [0, 1], [-1, 1]])

    logger.info("Stage 7E event dates: %s", event_dates_csv)
    logger.info("Stage 7E market models: %s", market_model_csv)
    logger.info("Stage 7E TOPIX returns: %s", topix_returns_csv)
    logger.info("Stage 7E price directory: %s", prices_dir)
    logger.info("Stage 7E CAR windows: %s", windows)

    for path in [event_dates_csv, market_model_csv, topix_returns_csv]:
        if not path.exists():
            raise FileNotFoundError(f"Missing Stage 7E input: {path}")
    if not prices_dir.exists():
        raise FileNotFoundError(f"Missing Stage 7E price directory: {prices_dir}")

    price_files = sorted(prices_dir.glob("equities_bars_daily_*.csv.gz"))
    if not price_files:
        raise FileNotFoundError(
            f"No equities_bars_daily_*.csv.gz files found in {prices_dir}"
        )

    events = pd.read_csv(event_dates_csv, low_memory=False)
    market_models = pd.read_csv(market_model_csv, low_memory=False)
    topix_df = load_topix_returns(topix_returns_csv)

    logger.info(
        "Stage 7E inputs: events=%s marketModels=%s priceFiles=%s",
        len(events),
        len(market_models),
        len(price_files),
    )

    stock_index, stock_qc = build_stock_return_index(price_files, topix_df)

    results = compute_abnormal_returns(
        events,
        market_models,
        topix_df,
        stock_index,
        windows=windows,
    )

    summary = build_abnormal_returns_summary(
        results,
        stock_qc,
        windows=windows,
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    results.to_csv(output_csv, index=False)
    summary_json.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    logger.info(
        "Stage 7E wrote %s rows x %s cols -> %s",
        len(results),
        len(results.columns),
        output_csv,
    )

    for label, item in summary["windows"].items():
        logger.info(
            "Stage 7E CAR[%s]: valid=%s invalid=%s retention=%.4f",
            label,
            item["eventsValid"],
            item["eventsInvalid"],
            item["retentionRate"],
        )

    logger.info("Stage 7E summary -> %s", summary_json)
