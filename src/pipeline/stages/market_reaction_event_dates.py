"""Pipeline adapter for Paper 2 Stage 7B event trading-date construction."""

from __future__ import annotations

from pathlib import Path

from src.market_reaction.event_trading_dates import build_stage7_event_trading_dates


def run_stage_market_reaction_event_dates(*, paper: str, cfg: dict, logger) -> dict:
    section = cfg.get("market_reaction_event_dates", {})
    root = Path(__file__).resolve().parents[3]

    kwargs = {
        "root": root,
        "paper": paper,
        "expected_eligible_events": section.get("expected_eligible_events", 32134),
    }

    if section.get("input_path"):
        kwargs["input_path"] = root / section["input_path"]
    if section.get("price_dir"):
        kwargs["price_dir"] = root / section["price_dir"]
    if section.get("output_dir"):
        kwargs["output_dir"] = root / section["output_dir"]

    logger.info("Stage 7B: constructing timestamp-aware event trading dates")
    result = build_stage7_event_trading_dates(**kwargs)

    logger.info(
        "Stage 7B complete: events=%s rules=%s output=%s",
        result["output_events"],
        result["rule_counts"],
        result["output_path"],
    )

    return result
