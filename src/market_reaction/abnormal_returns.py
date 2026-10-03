from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from src.market_reaction.market_model import (
    StockReturnSeries,
    _normalize_sec_code,
    build_stock_return_index,
    load_topix_returns,
)


def _offset_label(offset: int) -> str:
    if offset < 0:
        return f"m{abs(offset)}"
    return str(offset)


def _window_label(start: int, end: int) -> str:
    return f"{_offset_label(start)}_{_offset_label(end)}"


def _validate_windows(windows: Sequence[Sequence[int]]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for raw in windows:
        if len(raw) != 2:
            raise ValueError(f"Invalid CAR window {raw!r}; expected [start, end]")
        start, end = int(raw[0]), int(raw[1])
        if start > end:
            raise ValueError(f"Invalid CAR window [{start}, {end}]; start > end")
        out.append((start, end))
    if not out:
        raise ValueError("At least one CAR window is required")
    return out


def compute_abnormal_returns(
    events: pd.DataFrame,
    market_models: pd.DataFrame,
    topix_df: pd.DataFrame,
    stock_index: dict[str, StockReturnSeries],
    *,
    windows: Sequence[Sequence[int]] = ((0, 0), (0, 1), (-1, 1)),
) -> pd.DataFrame:
    """
    Compute event-window abnormal returns and CARs for Stage 7E.

    Frozen definitions
    ------------------
    AR_i,t = R_i,t - (alpha_i + beta_i * R_m,t)
    CAR_i[a,b] = sum(AR_i,t), t=a,...,b

    A CAR is valid only when every required stock return and market return in
    the requested market-calendar window is available. Missing sessions are
    never bridged and partial CARs are never computed.

    The output preserves every Stage 7B event. Events without an estimated
    Stage 7D market model receive window-specific non-valid statuses.
    """
    windows_t = _validate_windows(windows)

    event_required = {
        "edinetCode",
        "curr_docID",
        "secCode",
        "eventTradingDate",
    }
    model_required = event_required | {
        "marketModelStatus",
        "alpha",
        "beta",
        "estimationObs",
    }

    missing = event_required - set(events.columns)
    if missing:
        raise ValueError(
            f"Missing required Stage 7B event columns: {sorted(missing)}"
        )

    missing = model_required - set(market_models.columns)
    if missing:
        raise ValueError(
            f"Missing required Stage 7D market-model columns: {sorted(missing)}"
        )

    keys = ["edinetCode", "curr_docID", "secCode", "eventTradingDate"]

    ev = events.copy()
    mm = market_models.copy()

    ev["secCode"] = _normalize_sec_code(ev["secCode"])
    mm["secCode"] = _normalize_sec_code(mm["secCode"])

    ev["eventTradingDate"] = pd.to_datetime(
        ev["eventTradingDate"], errors="raise"
    ).dt.normalize()
    mm["eventTradingDate"] = pd.to_datetime(
        mm["eventTradingDate"], errors="raise"
    ).dt.normalize()

    if ev.duplicated(keys).any():
        raise ValueError("Stage 7B events contain duplicate event keys")
    if mm.duplicated(keys).any():
        raise ValueError("Stage 7D market models contain duplicate event keys")

    model_cols = keys + [
        c
        for c in [
            "estimationStartDate",
            "estimationEndDate",
            "estimationObs",
            "alpha",
            "beta",
            "rSquared",
            "marketModelStatus",
        ]
        if c in mm.columns
    ]

    out = ev.merge(
        mm[model_cols],
        on=keys,
        how="left",
        validate="one_to_one",
        indicator=True,
    )

    if (out["_merge"] != "both").any():
        n = int((out["_merge"] != "both").sum())
        raise ValueError(
            f"{n:,} Stage 7B event(s) have no matching Stage 7D market-model row"
        )
    out = out.drop(columns="_merge")

    topix_dates = pd.DatetimeIndex(topix_df["tradingDate"])
    date_to_session = {d: int(i) for i, d in enumerate(topix_dates)}
    market_returns = topix_df["topixReturnSimple"].to_numpy(dtype=np.float64)

    # Only calculate event-time offsets actually required by configured windows.
    offsets = sorted({
        t
        for start, end in windows_t
        for t in range(start, end + 1)
    })

    # Pre-create event-time columns.
    for t in offsets:
        label = _offset_label(t)
        out[f"stockReturn_{label}"] = np.nan
        out[f"marketReturn_{label}"] = np.nan
        out[f"abnormalReturn_{label}"] = np.nan

    # Work security-by-security so the compact Stage 7D return arrays are reused.
    for sec_code, idx in out.groupby("secCode", sort=False, observed=True).groups.items():
        series = stock_index.get(str(sec_code))

        for row_idx in idx:
            row = out.loc[row_idx]

            if row["marketModelStatus"] != "estimated":
                continue

            alpha = row["alpha"]
            beta = row["beta"]
            if not np.isfinite(alpha) or not np.isfinite(beta):
                continue

            event_idx = date_to_session.get(row["eventTradingDate"])
            if event_idx is None or series is None:
                continue

            for t in offsets:
                target_idx = event_idx + t
                if target_idx < 0 or target_idx >= len(topix_df):
                    continue

                market_ret = market_returns[target_idx]
                label = _offset_label(t)
                if np.isfinite(market_ret):
                    out.at[row_idx, f"marketReturn_{label}"] = market_ret

                pos = int(np.searchsorted(series.session_index, target_idx))
                if (
                    pos >= len(series.session_index)
                    or int(series.session_index[pos]) != target_idx
                ):
                    continue

                stock_ret = float(series.returns[pos])
                if not np.isfinite(stock_ret):
                    continue

                out.at[row_idx, f"stockReturn_{label}"] = stock_ret

                if np.isfinite(market_ret):
                    out.at[row_idx, f"abnormalReturn_{label}"] = (
                        stock_ret - (float(alpha) + float(beta) * market_ret)
                    )

    # CARs are all-or-nothing within each configured window.
    for start, end in windows_t:
        wlabel = _window_label(start, end)
        ar_cols = [
            f"abnormalReturn_{_offset_label(t)}"
            for t in range(start, end + 1)
        ]

        car_col = f"car_{wlabel}"
        valid_col = f"carValid_{wlabel}"
        status_col = f"carStatus_{wlabel}"

        out[car_col] = np.nan
        out[valid_col] = False
        out[status_col] = "unclassified"

        for row_idx, row in out.iterrows():
            mm_status = row["marketModelStatus"]
            if mm_status != "estimated":
                out.at[row_idx, status_col] = "market_model_not_estimated"
                continue

            event_idx = date_to_session.get(row["eventTradingDate"])
            if event_idx is None:
                out.at[row_idx, status_col] = "event_date_not_in_topix_calendar"
                continue

            if event_idx + start < 0 or event_idx + end >= len(topix_df):
                out.at[row_idx, status_col] = "event_window_outside_topix_calendar"
                continue

            if str(row["secCode"]) not in stock_index:
                out.at[row_idx, status_col] = "security_not_in_stock_return_index"
                continue

            missing_market = any(
                not np.isfinite(row[f"marketReturn_{_offset_label(t)}"])
                for t in range(start, end + 1)
            )
            if missing_market:
                out.at[row_idx, status_col] = "missing_market_return"
                continue

            missing_stock = any(
                not np.isfinite(row[f"stockReturn_{_offset_label(t)}"])
                for t in range(start, end + 1)
            )
            if missing_stock:
                out.at[row_idx, status_col] = "missing_stock_return"
                continue

            values = row[ar_cols].to_numpy(dtype=np.float64)
            if not np.isfinite(values).all():
                out.at[row_idx, status_col] = "abnormal_return_not_finite"
                continue

            out.at[row_idx, car_col] = float(values.sum())
            out.at[row_idx, valid_col] = True
            out.at[row_idx, status_col] = "ok"

    return out.reset_index(drop=True)


def build_abnormal_returns_summary(
    results: pd.DataFrame,
    stock_qc: dict,
    *,
    windows: Sequence[Sequence[int]],
) -> dict:
    windows_t = _validate_windows(windows)

    summary: dict = {
        "stage": "7E_abnormal_returns_and_CAR",
        "abnormalReturnDefinition": (
            "AR_i,t = R_i,t - (alpha_i + beta_i * R_m,t)"
        ),
        "carDefinition": "CAR_i[a,b] = sum_t AR_i,t",
        "partialCarsAllowed": False,
        "eventsInput": int(len(results)),
        "marketModelStatusCounts": {
            str(k): int(v)
            for k, v in results["marketModelStatus"]
            .value_counts(dropna=False)
            .to_dict()
            .items()
        },
        "stockReturnIndex": stock_qc,
        "windows": {},
    }

    for start, end in windows_t:
        wlabel = _window_label(start, end)
        car_col = f"car_{wlabel}"
        valid_col = f"carValid_{wlabel}"
        status_col = f"carStatus_{wlabel}"

        valid = results[valid_col].fillna(False).astype(bool)
        cars = pd.to_numeric(results.loc[valid, car_col], errors="coerce").dropna()

        item = {
            "start": int(start),
            "end": int(end),
            "eventsValid": int(valid.sum()),
            "eventsInvalid": int((~valid).sum()),
            "retentionRate": float(valid.mean()) if len(valid) else np.nan,
            "statusCounts": {
                str(k): int(v)
                for k, v in results[status_col]
                .value_counts(dropna=False)
                .to_dict()
                .items()
            },
        }

        if not cars.empty:
            desc = cars.describe(
                percentiles=[0.01, 0.05, 0.50, 0.95, 0.99]
            ).to_dict()
            item["carDistribution"] = {
                str(k): float(v) for k, v in desc.items()
            }

        summary["windows"][wlabel] = item

    return summary
