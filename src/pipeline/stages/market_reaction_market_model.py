from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.market_reaction.market_model import (
    build_market_model_summary,
    build_stock_return_index,
    estimate_event_market_models,
    load_topix_returns,
)


def run_stage_market_reaction_market_model(
    *,
    paper: str,
    cfg: dict,
    logger,
) -> pd.DataFrame:
    """
    Stage 7D pipeline adapter.

    Inputs
    ------
    Stage 7B:
        stage7_event_dates.csv

    Stage 7C:
        topix_returns.csv

    Raw J-Quants equities:
        data/raw/paper2/prices/equities_bars_daily_*.csv.gz

    Outputs
    -------
        market_model_estimates.csv
        market_model_summary.json
    """
    root = Path(__file__).resolve().parents[3]
    section = cfg.get("market_reaction_market_model", {})
    
    event_dates_csv = root / section.get(
        "event_dates_csv",
        f"data/interim/{paper}/market_reaction/stage7_event_dates.csv",
    )
    
    topix_returns_csv = root / section.get(
        "topix_returns_csv",
        f"data/interim/{paper}/market_reaction/topix_returns.csv",
    )
    
    prices_dir = root / section.get(
        "prices_dir",
        f"data/raw/{paper}/prices",
    )
    
    output_csv = root / section.get(
        "output_csv",
        f"data/interim/{paper}/market_reaction/market_model_estimates.csv",
    )
    
    summary_json = root / section.get(
        "summary_json",
        f"data/interim/{paper}/market_reaction/market_model_summary.json",
    )
    
    estimation_start = int(section.get("estimation_start", -120))
    estimation_end = int(section.get("estimation_end", -20))
    min_observations = int(section.get("min_observations", 60))

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    events = pd.read_csv(
        event_dates_csv,
        dtype={
            "edinetCode": "string",
            "curr_docID": "string",
            "secCode": "string",
        },
        low_memory=False,
    )

    # Stage 7B output should contain eligible events only. If an eligibility
    # flag is present, enforce it defensively rather than silently estimating
    # excluded observations.
    if "eventEligible" in events.columns:
        eligible = events["eventEligible"]
        if eligible.dtype == bool:
            events = events.loc[eligible].copy()
        else:
            events = events.loc[
                eligible.astype("string").str.lower().isin(
                    {"true", "1", "yes"}
                )
            ].copy()

    topix = load_topix_returns(topix_returns_csv)

    price_files = sorted(
        prices_dir.glob("equities_bars_daily_*.csv.gz")
    )
    if not price_files:
        raise FileNotFoundError(
            f"No J-Quants equity files found under {prices_dir}"
        )

    stock_index, stock_qc = build_stock_return_index(
        price_files,
        topix,
    )

    estimates = estimate_event_market_models(
        events,
        topix,
        stock_index,
        estimation_start=estimation_start,
        estimation_end=estimation_end,
        min_observations=min_observations,
    )

    estimates.to_csv(
        output_csv,
        index=False,
        encoding="utf-8",
    )

    summary = build_market_model_summary(
        estimates,
        stock_qc,
        estimation_start=estimation_start,
        estimation_end=estimation_end,
        min_observations=min_observations,
    )

    summary_json.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    estimated_count = int(
        (estimates["marketModelStatus"] == "estimated").sum()
    )
    not_estimated_count = len(estimates) - estimated_count
    
    logger.info(
        "Stage 7D market model: input=%s estimated=%s not_estimated=%s",
        f"{len(estimates):,}",
        f"{estimated_count:,}",
        f"{not_estimated_count:,}",
    )
    logger.info("Stage 7D output: %s", output_csv)
    logger.info("Stage 7D summary: %s", summary_json)

    return estimates
