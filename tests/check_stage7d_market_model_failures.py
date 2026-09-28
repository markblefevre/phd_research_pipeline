#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_ESTIMATES = (
    "data/interim/paper2/market_reaction/market_model_estimates.csv"
)
DEFAULT_TOPIX = (
    "data/interim/paper2/market_reaction/topix_returns.csv"
)
DEFAULT_PRICES = "data/raw/paper2/prices"
DEFAULT_OUTPUT = (
    "data/interim/paper2/market_reaction/"
    "diagnostics/stage7d_market_model_failure_audit.csv"
)


def repo_root() -> Path:
    # tests/check_stage7d_market_model_failures.py -> repo root
    return Path(__file__).resolve().parents[1]


def normalize_sec_code(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip()
    return out.str.replace(r"\.0$", "", regex=True)


def load_topix(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    required = {"tradingDate", "topixReturnSimple"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{path} missing TOPIX columns: {sorted(missing)}"
        )

    out = df[["tradingDate", "topixReturnSimple"]].copy()
    out["tradingDate"] = pd.to_datetime(
        out["tradingDate"], errors="raise"
    ).dt.normalize()
    out["topixReturnSimple"] = pd.to_numeric(
        out["topixReturnSimple"], errors="coerce"
    )

    out = (
        out.sort_values("tradingDate")
        .drop_duplicates("tradingDate", keep="last")
        .reset_index(drop=True)
    )
    out["topixSessionIndex"] = np.arange(len(out), dtype=np.int32)
    return out


def load_failed_events(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        dtype={
            "edinetCode": "string",
            "curr_docID": "string",
            "secCode": "string",
        },
        low_memory=False,
    )

    required = {
        "edinetCode",
        "curr_docID",
        "secCode",
        "eventTradingDate",
        "estimationObs",
        "marketModelStatus",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{path} missing market-model columns: {sorted(missing)}"
        )

    df["secCode"] = normalize_sec_code(df["secCode"])
    df["eventTradingDate"] = pd.to_datetime(
        df["eventTradingDate"], errors="raise"
    ).dt.normalize()

    return df.loc[
        df["marketModelStatus"] != "estimated"
    ].copy()


def load_relevant_prices(
    price_files: list[Path],
    relevant_codes: set[str],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for path in price_files:
        d = pd.read_csv(
            path,
            usecols=["Date", "Code", "C", "AdjFactor"],
            dtype={"Code": "string"},
            low_memory=False,
        )
        d["Code"] = normalize_sec_code(d["Code"])
        d = d.loc[d["Code"].isin(relevant_codes)].copy()

        if not d.empty:
            d["Date"] = pd.to_datetime(
                d["Date"], errors="coerce"
            ).dt.normalize()
            d["C"] = pd.to_numeric(d["C"], errors="coerce")
            d["AdjFactor"] = pd.to_numeric(
                d["AdjFactor"], errors="coerce"
            )
            frames.append(d)

    if not frames:
        return pd.DataFrame(
            columns=["Date", "Code", "C", "AdjFactor"]
        )

    return pd.concat(frames, ignore_index=True)


def build_global_tse_calendar(price_files: list[Path]) -> pd.DatetimeIndex:
    dates: list[pd.Series] = []

    for path in price_files:
        d = pd.read_csv(
            path,
            usecols=["Date"],
            low_memory=False,
        )
        dates.append(
            pd.to_datetime(
                d["Date"], errors="coerce"
            ).dropna()
        )

    all_dates = pd.concat(dates, ignore_index=True)

    return pd.DatetimeIndex(
        all_dates.drop_duplicates().sort_values()
    )


def build_security_return_table(
    prices: pd.DataFrame,
    tse_date_to_pos: pd.Series,
    topix_date_to_pos: pd.Series,
) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(
            columns=[
                "Code",
                "Date",
                "C",
                "AdjFactor",
                "tseSessionIndex",
                "prevDate",
                "prevClose",
                "prevTseSessionIndex",
                "isConsecutive",
                "stockReturn",
                "topixSessionIndex",
            ]
        )

    p = prices.copy()
    p["tseSessionIndex"] = p["Date"].map(tse_date_to_pos)
    p = p.sort_values(["Code", "Date"], kind="mergesort")

    valid_close = (
        p["C"].notna()
        & np.isfinite(p["C"])
        & (p["C"] > 0)
    )

    v = p.loc[
        valid_close,
        ["Code", "Date", "C", "AdjFactor", "tseSessionIndex"],
    ].copy()

    g = v.groupby("Code", sort=False, observed=True)
    v["prevDate"] = g["Date"].shift(1)
    v["prevClose"] = g["C"].shift(1)
    v["prevTseSessionIndex"] = g["tseSessionIndex"].shift(1)

    v["isConsecutive"] = (
        v["prevTseSessionIndex"].notna()
        & (
            v["tseSessionIndex"]
            == v["prevTseSessionIndex"] + 1
        )
    )

    factor = v["AdjFactor"].fillna(1.0)
    valid_factor = np.isfinite(factor) & (factor > 0)

    v["stockReturn"] = np.nan
    calc = v["isConsecutive"] & valid_factor

    v.loc[calc, "stockReturn"] = (
        v.loc[calc, "C"]
        / (
            v.loc[calc, "prevClose"]
            * factor.loc[calc]
        )
        - 1.0
    )

    v["topixSessionIndex"] = v["Date"].map(
        topix_date_to_pos
    )

    return v


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit Paper 2 Stage 7D market-model failures without "
            "changing production outputs."
        )
    )
    parser.add_argument(
        "--estimates",
        default=DEFAULT_ESTIMATES,
        help="Stage 7D market_model_estimates.csv",
    )
    parser.add_argument(
        "--topix",
        default=DEFAULT_TOPIX,
        help="Stage 7C topix_returns.csv",
    )
    parser.add_argument(
        "--prices-dir",
        default=DEFAULT_PRICES,
        help="Directory containing J-Quants equities_bars_daily_*.csv.gz",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Failure-audit CSV output path",
    )
    parser.add_argument(
        "--estimation-start",
        type=int,
        default=-120,
    )
    parser.add_argument(
        "--estimation-end",
        type=int,
        default=-20,
    )
    args = parser.parse_args()

    root = repo_root()

    estimates_path = root / args.estimates
    topix_path = root / args.topix
    prices_dir = root / args.prices_dir
    output_path = root / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    price_files = sorted(
        prices_dir.glob("equities_bars_daily_*.csv.gz")
    )
    if not price_files:
        raise FileNotFoundError(
            f"No J-Quants price files found under {prices_dir}"
        )

    failed = load_failed_events(estimates_path)

    print("=== Stage 7D failure audit ===")
    print(f"Failed events:       {len(failed):,}")
    print(
        failed["marketModelStatus"]
        .value_counts()
        .to_string()
    )
    print()

    relevant_codes = set(
        failed["secCode"].dropna().astype(str)
    )

    # Build the same global TSE calendar used by production.
    tse_dates = build_global_tse_calendar(price_files)
    tse_date_to_pos = pd.Series(
        np.arange(len(tse_dates), dtype=np.int32),
        index=tse_dates,
    )

    topix = load_topix(topix_path)
    topix_dates = pd.DatetimeIndex(topix["tradingDate"])
    topix_date_to_pos = pd.Series(
        topix["topixSessionIndex"].to_numpy(dtype=np.int32),
        index=topix_dates,
    )

    relevant_prices = load_relevant_prices(
        price_files,
        relevant_codes,
    )

    returns = build_security_return_table(
        relevant_prices,
        tse_date_to_pos,
        topix_date_to_pos,
    )

    raw_by_code = {
        str(code): grp.sort_values("Date")
        for code, grp in relevant_prices.groupby(
            "Code", sort=False, observed=True
        )
    }
    ret_by_code = {
        str(code): grp.sort_values("Date")
        for code, grp in returns.groupby(
            "Code", sort=False, observed=True
        )
    }

    topix_lookup = {
        d: int(i)
        for i, d in enumerate(topix_dates)
    }

    rows: list[dict] = []

    for row in failed.itertuples(index=False):
        code = str(row.secCode)
        event_date = row.eventTradingDate
        status = row.marketModelStatus

        raw = raw_by_code.get(code)
        ret = ret_by_code.get(code)

        event_idx = topix_lookup.get(event_date)

        if event_idx is None:
            start_date = pd.NaT
            end_date = pd.NaT
        else:
            start_idx = event_idx + args.estimation_start
            end_idx = event_idx + args.estimation_end

            if (
                start_idx >= 0
                and end_idx < len(topix_dates)
            ):
                start_date = topix_dates[start_idx]
                end_date = topix_dates[end_idx]
            else:
                start_date = pd.NaT
                end_date = pd.NaT

        if raw is None or raw.empty:
            raw_rows = 0
            valid_close_rows = 0
            first_price_date = pd.NaT
            last_price_date = pd.NaT
            raw_window_rows = 0
            valid_close_window_rows = 0
        else:
            raw_rows = len(raw)
            valid_close = (
                raw["C"].notna()
                & np.isfinite(raw["C"])
                & (raw["C"] > 0)
            )
            valid_close_rows = int(valid_close.sum())
            first_price_date = raw["Date"].min()
            last_price_date = raw["Date"].max()

            if pd.notna(start_date):
                in_window = raw["Date"].between(
                    start_date, end_date
                )
                raw_window_rows = int(in_window.sum())
                valid_close_window_rows = int(
                    (in_window & valid_close).sum()
                )
            else:
                raw_window_rows = 0
                valid_close_window_rows = 0

        if ret is None or ret.empty:
            consecutive_returns_total = 0
            usable_topix_returns_total = 0
            consecutive_window = 0
            usable_window = 0
            nonunit_adjustments_window = 0
        else:
            consecutive_returns_total = int(
                ret["stockReturn"].notna().sum()
            )
            usable_topix_returns_total = int(
                (
                    ret["stockReturn"].notna()
                    & ret["topixSessionIndex"].notna()
                ).sum()
            )

            if pd.notna(start_date):
                rw = ret["Date"].between(
                    start_date, end_date
                )
                consecutive_window = int(
                    (
                        rw
                        & ret["stockReturn"].notna()
                    ).sum()
                )
                usable_window = int(
                    (
                        rw
                        & ret["stockReturn"].notna()
                        & ret["topixSessionIndex"].notna()
                    ).sum()
                )
                nonunit_adjustments_window = int(
                    (
                        rw
                        & ret["AdjFactor"].notna()
                        & (ret["AdjFactor"] != 1.0)
                    ).sum()
                )
            else:
                consecutive_window = 0
                usable_window = 0
                nonunit_adjustments_window = 0

        if raw_rows == 0:
            explanation = "no_raw_jquants_rows_for_security"
        elif valid_close_rows == 0:
            explanation = "raw_rows_but_no_valid_close"
        elif event_idx is None:
            explanation = "event_date_not_in_topix_calendar"
        elif pd.isna(start_date):
            explanation = "estimation_window_outside_topix_history"
        elif usable_window == 0:
            explanation = (
                "price_history_exists_but_no_usable_consecutive_returns_"
                "in_estimation_window"
            )
        elif usable_window < 60:
            explanation = (
                "fewer_than_60_usable_consecutive_stock_topix_returns"
            )
        else:
            explanation = (
                "unexpected_failure_needs_manual_review"
            )

        rows.append(
            {
                "edinetCode": row.edinetCode,
                "curr_docID": row.curr_docID,
                "secCode": code,
                "eventTradingDate": event_date,
                "marketModelStatus": status,
                "productionEstimationObs": int(
                    row.estimationObs
                ),
                "estimationStartDate": start_date,
                "estimationEndDate": end_date,
                "rawSecurityRows": raw_rows,
                "validCloseRows": valid_close_rows,
                "firstPriceDate": first_price_date,
                "lastPriceDate": last_price_date,
                "rawRowsInWindow": raw_window_rows,
                "validCloseRowsInWindow": (
                    valid_close_window_rows
                ),
                "consecutiveReturnsTotal": (
                    consecutive_returns_total
                ),
                "usableTopixReturnsTotal": (
                    usable_topix_returns_total
                ),
                "consecutiveReturnsInWindow": (
                    consecutive_window
                ),
                "usablePairedReturnsInWindow": usable_window,
                "nonunitAdjustmentRowsInWindow": (
                    nonunit_adjustments_window
                ),
                "explanation": explanation,
            }
        )

    audit = pd.DataFrame(rows)

    audit.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
    )

    print("Explanation counts:")
    print(
        audit["explanation"]
        .value_counts()
        .to_string()
    )
    print()

    print("Production vs reconstructed observation-count check:")
    obs_match = (
        audit["productionEstimationObs"]
        == audit["usablePairedReturnsInWindow"]
    )
    print(f"matching rows: {int(obs_match.sum()):,}/{len(audit):,}")
    if not obs_match.all():
        print("MISMATCH sample:")
        print(
            audit.loc[
                ~obs_match,
                [
                    "secCode",
                    "eventTradingDate",
                    "productionEstimationObs",
                    "usablePairedReturnsInWindow",
                ],
            ]
            .head(20)
            .to_string(index=False)
        )
    print()

    special = audit.loc[
        audit["marketModelStatus"]
        == "security_not_in_stock_return_index"
    ]

    if not special.empty:
        print("security_not_in_stock_return_index:")
        print(
            special[
                [
                    "secCode",
                    "eventTradingDate",
                    "rawSecurityRows",
                    "validCloseRows",
                    "firstPriceDate",
                    "lastPriceDate",
                    "consecutiveReturnsTotal",
                    "usableTopixReturnsTotal",
                    "explanation",
                ]
            ].to_string(index=False)
        )
        print()

    insufficient = audit.loc[
        audit["marketModelStatus"]
        == "insufficient_paired_observations"
    ]

    if not insufficient.empty:
        by_security = (
            insufficient.groupby("secCode", observed=True)
            .agg(
                failedEvents=("secCode", "size"),
                minObs=("usablePairedReturnsInWindow", "min"),
                medianObs=("usablePairedReturnsInWindow", "median"),
                maxObs=("usablePairedReturnsInWindow", "max"),
                firstPriceDate=("firstPriceDate", "min"),
                lastPriceDate=("lastPriceDate", "max"),
            )
            .sort_values(
                ["failedEvents", "medianObs"],
                ascending=[False, True],
            )
        )

        print("Top securities with insufficient observations:")
        print(by_security.head(25).to_string())
        print()

    print(f"Audit written: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
