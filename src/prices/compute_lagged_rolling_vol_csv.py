from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def compute_lagged_rolling_vol_csv(
    input_csv: str | Path,
    output_csv: str | Path,
    price_col: str = "adj_close",
    symbol_col: str = "symbol",
    date_col: str = "date",
    offset: int = 11,
    windows: int | Iterable[int] = 20,
    return_col: str = "ret",
    ddof: int = 1,
) -> pd.DataFrame:
    """
    Read a CSV of daily prices, compute per-symbol daily returns and lagged rolling
    volatility columns, then write the result to a new CSV.

    Parameters
    ----------
    input_csv : str | Path
        Input CSV path. Must contain at least date_col, symbol_col, and price_col.
    output_csv : str | Path
        Output CSV path.
    price_col : str
        Price column used to compute returns. Default is 'adj_close'.
    symbol_col : str
        Symbol/ticker column. Default is 'symbol'.
    date_col : str
        Date column. Default is 'date'.
    offset : int
        Number of trading rows to exclude immediately before the current row/date.
        Example: offset=11 means the rolling window ends 11 trading days earlier.
    windows : int | Iterable[int]
        Window length(s) for rolling standard deviation. Can be a scalar like 20
        or a list like [20, 60, 120].
    return_col : str
        Name of the output return column. Default is 'ret'.
    ddof : int
        Delta degrees of freedom for std. Pandas default sample std uses ddof=1.

    Returns
    -------
    pd.DataFrame
        DataFrame that was also written to output_csv.

    Notes
    -----
    For a given row/date t:
      - ret[t] = price[t] / price[t-1] - 1
      - vol_20_11[t] uses exactly 20 return observations ending at t-11,
        i.e. returns from t-30 through t-11 in trading-row terms.
    """
    input_csv = Path(input_csv)
    output_csv = Path(output_csv)

    if isinstance(windows, int):
        window_list = [windows]
    else:
        window_list = [int(w) for w in windows]

    if not window_list:
        raise ValueError("windows must contain at least one positive integer")
    if any(w <= 0 for w in window_list):
        raise ValueError(f"All windows must be positive. Got: {window_list}")
    if offset < 0:
        raise ValueError(f"offset must be >= 0. Got: {offset}")

    df = pd.read_csv(input_csv)

    required = {date_col, symbol_col, price_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}. "
            f"Available columns: {list(df.columns)}"
        )

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    bad_dates = int(df[date_col].isna().sum())
    if bad_dates:
        raise ValueError(f"Found {bad_dates} rows with invalid {date_col}")

    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.sort_values([symbol_col, date_col]).reset_index(drop=True)

    # Daily simple returns by symbol
    df[return_col] = df.groupby(symbol_col, sort=False)[price_col].pct_change()

    # Lagged rolling std of returns by symbol
    # shift(offset) means for row t, the most recent included return is t-offset.
    # rolling(window) then uses exactly `window` samples ending there.
    grouped_ret = df.groupby(symbol_col, sort=False)[return_col]

    for w in window_list:
        out_col = f"vol_{w}_{offset}"
        df[out_col] = (
            grouped_ret.transform(lambda s: s.shift(offset).rolling(window=w, min_periods=w).std(ddof=ddof))
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)

    return df