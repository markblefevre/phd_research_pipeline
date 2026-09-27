"""
Stage 7B: timestamp-aware market event trading dates.
"""

from __future__ import annotations

import json
from bisect import bisect_right
from pathlib import Path
from typing import Iterable

import pandas as pd

_CLOSE_CHANGE_DATE = pd.Timestamp("2024-11-05").date()
_OLD_CLOSE = pd.Timedelta(hours=15)
_NEW_CLOSE = pd.Timedelta(hours=15, minutes=30)

_DATE_COLUMNS = ("Date", "date", "TradingDate", "tradingDate", "trading_date")
_SUBMIT_COLUMNS = ("curr_submitDateTime", "submitDateTime", "currSubmitDateTime", "submissionDateTime")
_ELIGIBLE_COLUMNS = ("eventEligible", "eligible", "isEligible", "marketReactionEligible", "stage7Eligible")
_EXCLUSION_COLUMNS = ("finalExclusionReason", "exclusionReason", "exclusion_reason", "stage7ExclusionReason")

def _first_existing(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    cols = set(columns)
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None


def _to_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)

    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().all():
        return numeric.ne(0)

    normalized = series.astype("string").str.strip().str.lower()
    true_values = {"true", "t", "yes", "y", "1", "eligible", "include", "included"}
    false_values = {"false", "f", "no", "n", "0", "ineligible", "exclude", "excluded", ""}
    bad = ~(normalized.isin(true_values | false_values) | normalized.isna())
    if bad.any():
        examples = sorted(normalized[bad].dropna().unique().tolist())[:10]
        raise ValueError(f"Unrecognized eligibility values: {examples}")
    return normalized.isin(true_values)


def select_stage7a_eligible(events: pd.DataFrame) -> pd.DataFrame:
    eligible_col = _first_existing(events.columns, _ELIGIBLE_COLUMNS)
    if eligible_col is not None:
        return events.loc[_to_bool(events[eligible_col])].copy()

    exclusion_col = _first_existing(events.columns, _EXCLUSION_COLUMNS)
    if exclusion_col is not None:
        reason = events[exclusion_col].astype("string").fillna("").str.strip()
        return events.loc[reason.eq("")].copy()

    raise KeyError("Could not identify Stage 7A eligibility columns.")


def _read_dates_from_csv(path: Path) -> pd.Series:
    header = pd.read_csv(path, nrows=0)
    date_col = _first_existing(header.columns, _DATE_COLUMNS)
    if date_col is None:
        return pd.Series(dtype="datetime64[ns]")
    values = pd.read_csv(path, usecols=[date_col])[date_col]
    return pd.to_datetime(values, errors="coerce").dt.normalize()


def build_tse_trading_calendar(price_dir: Path) -> list[pd.Timestamp]:
    files = sorted(Path(price_dir).glob("equities_bars_daily_*.csv.gz"))
    if not files:
        raise FileNotFoundError(f"No CSV price files found under {price_dir}")

    chunks = []
    for path in files:
        dates = _read_dates_from_csv(path)
        if not dates.empty:
            chunks.append(dates.dropna())

    if not chunks:
        raise RuntimeError("No recognized trading-date columns found in price files.")

    calendar = (
        pd.concat(chunks, ignore_index=True)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    weekend = [d for d in calendar if d.weekday() >= 5]
    if weekend:
        raise AssertionError(f"Trading calendar contains weekend dates, e.g. {weekend[:5]}")

    return calendar


def market_close_timedelta(submission_date) -> pd.Timedelta:
    return _NEW_CLOSE if submission_date >= _CLOSE_CHANGE_DATE else _OLD_CLOSE


def assign_event_trading_dates(
    eligible_events: pd.DataFrame,
    trading_calendar: list[pd.Timestamp],
) -> pd.DataFrame:
    out = eligible_events.copy()

    submit_col = _first_existing(out.columns, _SUBMIT_COLUMNS)
    if submit_col is None:
        raise KeyError(f"Could not find submission timestamp column among {_SUBMIT_COLUMNS}")

    submit = pd.to_datetime(out[submit_col], errors="coerce")
    if submit.isna().any():
        raise ValueError(f"{int(submit.isna().sum())} eligible events have invalid submission timestamps.")

    out["submitDateTime"] = submit
    out["submissionDate"] = submit.dt.normalize()
    out["submissionTime"] = submit.dt.strftime("%H:%M:%S")

    calendar = [pd.Timestamp(d).normalize() for d in trading_calendar]
    calendar_set = set(calendar)

    event_dates = []
    close_times = []
    rules = []
    on_trading_day = []
    after_close_flags = []

    for ts in submit:
        submission_day = ts.normalize()
        close_delta = market_close_timedelta(ts.date())
        close_ts = submission_day + close_delta

        close_times.append(
            f"{int(close_delta.components.hours):02d}:{int(close_delta.components.minutes):02d}:00"
        )

        is_trading_day = submission_day in calendar_set
        on_trading_day.append(is_trading_day)

        if not is_trading_day:
            idx = bisect_right(calendar, submission_day)
            if idx >= len(calendar):
                raise ValueError(f"No trading date after {submission_day.date()}")
            event_dates.append(calendar[idx])
            rules.append("next_trading_day_nontrading_date")
            after_close_flags.append(False)
            continue

        after_close = ts > close_ts
        after_close_flags.append(after_close)

        if after_close:
            idx = bisect_right(calendar, submission_day)
            if idx >= len(calendar):
                raise ValueError(f"No next trading date after {submission_day.date()}")
            event_dates.append(calendar[idx])
            rules.append("next_trading_day_after_close")
        else:
            event_dates.append(submission_day)
            rules.append("same_day_at_or_before_close")

    out["marketCloseTime"] = close_times
    out["submissionOnTradingDay"] = on_trading_day
    out["afterClose"] = after_close_flags
    out["eventTradingDate"] = pd.to_datetime(event_dates)
    out["eventDateShiftDays"] = (out["eventTradingDate"] - out["submissionDate"]).dt.days
    out["eventDateRule"] = rules

    assert out["eventTradingDate"].notna().all()
    assert (out["eventTradingDate"] >= out["submissionDate"]).all()

    return out


def build_stage7_event_trading_dates(
    *,
    root: Path,
    paper: str = "paper2",
    input_path: Path | None = None,
    price_dir: Path | None = None,
    output_dir: Path | None = None,
    expected_eligible_events: int | None = 32134,
) -> dict:
    root = Path(root)

    if input_path is None:
        input_path = root / f"data/interim/{paper}/market_reaction/stage7_event_eligibility.csv"
    if price_dir is None:
        price_dir = root / f"data/raw/{paper}/prices"
    if output_dir is None:
        output_dir = root / f"data/interim/{paper}/market_reaction"

    events = pd.read_csv(input_path, low_memory=False)
    eligible = select_stage7a_eligible(events)

    if expected_eligible_events is not None and len(eligible) != expected_eligible_events:
        raise AssertionError(
            f"Expected {expected_eligible_events:,} Stage 7A-eligible events, found {len(eligible):,}."
        )

    calendar = build_tse_trading_calendar(price_dir)
    out = assign_event_trading_dates(eligible, calendar)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "stage7_event_dates.csv"
    summary_path = output_dir / "stage7_event_dates_summary.json"

    out.to_csv(output_path, index=False)

    rule_counts = {str(k): int(v) for k, v in out["eventDateRule"].value_counts(dropna=False).items()}

    summary = {
        "stage": "7B_event_trading_dates",
        "input_stage7a_rows": int(len(events)),
        "eligible_input_events": int(len(eligible)),
        "output_events": int(len(out)),
        "trading_calendar_first_date": str(calendar[0].date()),
        "trading_calendar_last_date": str(calendar[-1].date()),
        "trading_calendar_sessions": int(len(calendar)),
        "old_close_time_jst": "15:00:00",
        "new_close_time_jst": "15:30:00",
        "close_change_effective_date": str(_CLOSE_CHANGE_DATE),
        "event_date_rule_counts": rule_counts,
        "max_event_date_shift_calendar_days": int(out["eventDateShiftDays"].max()),
        "missing_event_trading_dates": int(out["eventTradingDate"].isna().sum()),
    }

    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    return {
        "output_path": str(output_path),
        "summary_path": str(summary_path),
        "input_events": int(len(eligible)),
        "output_events": int(len(out)),
        "rule_counts": rule_counts,
    }
