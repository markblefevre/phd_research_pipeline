from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd

from src.market.topix import (
    build_topix_returns,
    fetch_topix_jquants,
    write_topix_raw,
    write_topix_returns,
)


def run_stage_market_reaction_topix(
    *,
    paper: str,
    cfg: dict[str, Any],
    logger: logging.Logger,
) -> dict:
    """
    Stage 7C:
        acquire official TOPIX,
        construct market returns,
        validate coverage against Stage 7B.
    """

    root = Path(__file__).resolve().parents[3]

    stage_cfg = cfg.get(
        "market_reaction_topix",
        {},
    )

    event_dates_path = (
        root
        / stage_cfg.get(
            "event_dates_path",
            (
                f"data/interim/{paper}/"
                "market_reaction/"
                "stage7_event_dates.csv"
            ),
        )
    )

    raw_path = (
        root
        / stage_cfg.get(
            "raw_path",
            (
                f"data/raw/{paper}/"
                "market/topix_daily.csv"
            ),
        )
    )

    output_path = (
        root
        / stage_cfg.get(
            "output_path",
            (
                f"data/interim/{paper}/"
                "market_reaction/"
                "topix_returns.csv"
            ),
        )
    )

    summary_path = (
        root
        / stage_cfg.get(
            "summary_path",
            (
                f"data/interim/{paper}/"
                "market_reaction/"
                "topix_returns_summary.json"
            ),
        )
    )

    start = stage_cfg.get(
        "start",
        "2016-01-01",
    )

    end = stage_cfg.get(
        "end",
        None,
    )

    api_key_env = stage_cfg.get(
        "api_key_env",
        "JQUANTS_API_KEY",
    )

    refresh_raw = bool(
        stage_cfg.get(
            "refresh_raw",
            False,
        )
    )

    logger.info(
        "Stage 7C TOPIX: raw=%s",
        raw_path,
    )

    logger.info(
        "Stage 7C TOPIX: output=%s",
        output_path,
    )

    logger.info(
        "Stage 7C TOPIX: start=%s end=%s",
        start,
        end,
    )

    # ---------------------------------------------------------
    # Acquire or reuse raw TOPIX
    # ---------------------------------------------------------

    if raw_path.exists() and not refresh_raw:

        logger.info(
            "[REUSE] existing raw TOPIX: %s",
            raw_path,
        )

        raw = pd.read_csv(
            raw_path,
            low_memory=False,
        )

        raw["tradingDate"] = pd.to_datetime(
            raw["tradingDate"]
        )

    else:

        api_key = os.getenv(api_key_env)

        if not api_key:
            raise RuntimeError(
                f"Environment variable "
                f"{api_key_env!r} is not set"
            )

        logger.info(
            "[FETCH] official J-Quants TOPIX"
        )

        raw = fetch_topix_jquants(
            api_key=api_key,
            start=start,
            end=end,
        )

        write_topix_raw(
            raw,
            raw_path,
        )

    # ---------------------------------------------------------
    # Build market returns
    # ---------------------------------------------------------

    topix = build_topix_returns(raw)

    # ---------------------------------------------------------
    # Stage 7B coverage validation
    # ---------------------------------------------------------

    if not event_dates_path.exists():
        raise FileNotFoundError(
            f"Missing Stage 7B event dates: "
            f"{event_dates_path}"
        )

    events = pd.read_csv(
        event_dates_path,
        low_memory=False,
    )

    events["eventTradingDate"] = (
        pd.to_datetime(
            events["eventTradingDate"],
            errors="raise",
        )
        .dt.normalize()
    )

    topix_dates = set(
        pd.to_datetime(
            topix["tradingDate"]
        ).dt.normalize()
    )

    event_dates = set(
        events["eventTradingDate"]
    )

    missing_event_dates = sorted(
        event_dates - topix_dates
    )

    if missing_event_dates:
        sample = [
            str(x.date())
            for x in missing_event_dates[:20]
        ]

        raise AssertionError(
            "TOPIX does not cover all Stage 7B "
            f"eventTradingDate values. "
            f"Missing={len(missing_event_dates):,}; "
            f"sample={sample}"
        )

    # ---------------------------------------------------------
    # Additional QC
    # ---------------------------------------------------------

    if topix["tradingDate"].duplicated().any():
        raise AssertionError(
            "Duplicate TOPIX trading dates"
        )

    if not topix["tradingDate"].is_monotonic_increasing:
        raise AssertionError(
            "TOPIX trading dates not sorted"
        )

    if topix["topixClose"].isna().any():
        raise AssertionError(
            "Missing TOPIX close values"
        )

    write_topix_returns(
        topix,
        output_path,
    )

    summary = {
        "stage": "7C_topix_market_returns",
        "source": "J-Quants official TOPIX",
        "raw_rows": int(len(raw)),
        "output_rows": int(len(topix)),
        "first_trading_date": (
            topix["tradingDate"]
            .min()
            .strftime("%Y-%m-%d")
        ),
        "last_trading_date": (
            topix["tradingDate"]
            .max()
            .strftime("%Y-%m-%d")
        ),
        "stage7b_events": int(len(events)),
        "unique_stage7b_event_dates": int(
            events[
                "eventTradingDate"
            ].nunique()
        ),
        "missing_stage7b_event_dates": int(
            len(missing_event_dates)
        ),
        "missing_close_values": int(
            topix["topixClose"]
            .isna()
            .sum()
        ),
        "simple_return_missing": int(
            topix["topixReturnSimple"]
            .isna()
            .sum()
        ),
        "log_return_missing": int(
            topix["topixReturnLog"]
            .isna()
            .sum()
        ),
        "raw_path": str(
            raw_path.relative_to(root)
        ),
        "output_path": str(
            output_path.relative_to(root)
        ),
    }

    summary_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary_path.write_text(
        json.dumps(
            summary,
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    logger.info(
        "Stage 7C TOPIX complete: "
        "rows=%s dates=%s to %s "
        "missing_event_dates=%s",
        summary["output_rows"],
        summary["first_trading_date"],
        summary["last_trading_date"],
        summary[
            "missing_stage7b_event_dates"
        ],
    )

    return summary