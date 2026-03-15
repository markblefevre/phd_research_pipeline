from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple

import pandas as pd

try:
    import yfinance as yf
except Exception:
    yf = None


PRICE_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "adj_close", "volume"]
DIVIDEND_COLUMNS = ["date", "symbol", "dividend"]
SPLIT_COLUMNS = ["date", "symbol", "split_ratio"]


def read_symbols(csv_path: str | Path, symbols_col: str = "symbol") -> List[str]:
    df = pd.read_csv(csv_path)
    if symbols_col not in df.columns:
        raise ValueError(f"Column '{symbols_col}' not found in {csv_path}. Columns: {list(df.columns)}")

    symbols = (
        df[symbols_col]
        .astype(str)
        .str.strip()
        .replace({"": pd.NA})
        .dropna()
        .unique()
        .tolist()
    )
    if not symbols:
        raise ValueError(f"No symbols found in column '{symbols_col}' of {csv_path}")
    return symbols


def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def download_prices_chunk(
    symbols: List[str],
    start: Optional[str],
    end: Optional[str],
    retries: int = 1,
) -> pd.DataFrame:
    """
    Download OHLCV + Adj Close for a list of symbols using yfinance.
    Returns a tidy/long DataFrame with columns:
      date, symbol, open, high, low, close, adj_close, volume
    """
    if yf is None:
        raise ImportError("This module requires the 'yfinance' package. Install with: pip install yfinance")

    for attempt in range(retries + 1):
        try:
            data = yf.download(
                tickers=symbols,
                start=start,
                end=end,
                auto_adjust=False,
                group_by="ticker",
                threads=True,
                progress=False,
            )
            break
        except Exception:
            if attempt < retries:
                continue
            raise

    if isinstance(data.columns, pd.MultiIndex):
        frames = []
        for sym in symbols:
            if sym not in data.columns.levels[0]:
                continue
            sub = data[sym].copy()
            sub = sub.rename(
                columns={
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Adj Close": "adj_close",
                    "Volume": "volume",
                }
            )
            sub["symbol"] = sym
            sub.index.name = "date"
            frames.append(sub.reset_index())

        if not frames:
            return pd.DataFrame(columns=PRICE_COLUMNS)

        tidy = pd.concat(frames, ignore_index=True)

    else:
        sub = data.copy()
        sub = sub.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
        )
        sub["symbol"] = symbols[0]
        sub.index.name = "date"
        tidy = sub.reset_index()

    for c in PRICE_COLUMNS:
        if c not in tidy.columns:
            tidy[c] = pd.NA

    tidy = tidy[PRICE_COLUMNS].copy()
    tidy["date"] = pd.to_datetime(tidy["date"]).dt.tz_localize(None)
    tidy = tidy.sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"]).reset_index(drop=True)
    return tidy


def fetch_events_for_symbol(sym: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return (dividends_df, splits_df) for a symbol using yfinance.Ticker.
    dividends_df columns: date, symbol, dividend
    splits_df columns: date, symbol, split_ratio
    """
    if yf is None:
        raise ImportError("This module requires the 'yfinance' package. Install with: pip install yfinance")

    t = yf.Ticker(sym)
    div = getattr(t, "dividends", pd.Series(dtype="float64"))
    spl = getattr(t, "splits", pd.Series(dtype="float64"))

    if isinstance(div, pd.Series) and div.index.size > 0:
        df_div = div.rename("dividend").to_frame()
        df_div["symbol"] = sym
        df_div.index.name = "date"
        df_div = df_div.reset_index()
        df_div["date"] = pd.to_datetime(df_div["date"]).dt.tz_localize(None)
        df_div = df_div[DIVIDEND_COLUMNS].sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"])
    else:
        df_div = pd.DataFrame(columns=DIVIDEND_COLUMNS)

    if isinstance(spl, pd.Series) and spl.index.size > 0:
        df_spl = spl.rename("split_ratio").to_frame()
        df_spl["symbol"] = sym
        df_spl.index.name = "date"
        df_spl = df_spl.reset_index()
        df_spl["date"] = pd.to_datetime(df_spl["date"]).dt.tz_localize(None)
        df_spl = df_spl[SPLIT_COLUMNS].sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"])
    else:
        df_spl = pd.DataFrame(columns=SPLIT_COLUMNS)

    return df_div, df_spl


def run_fetch_jpx_prices(
    input_csv: str | Path,
    outdir: str | Path,
    symbols_col: str = "symbol",
    start: str | None = "2015-01-01",
    end: str | None = None,
    chunk_size: int = 60,
    retries: int = 1,
) -> dict[str, Any]:
    """
    Fetch daily prices, dividends, and splits for a symbol universe and write curated outputs.

    Returns a summary dict with output paths and row counts.
    """
    if yf is None:
        raise ImportError("This module requires the 'yfinance' package. Install with: pip install yfinance")

    input_csv = Path(input_csv)
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    symbols = read_symbols(input_csv, symbols_col=symbols_col)

    price_frames: list[pd.DataFrame] = []
    for batch in chunked(symbols, chunk_size):
        df_batch = download_prices_chunk(batch, start=start, end=end, retries=retries)
        if not df_batch.empty:
            price_frames.append(df_batch)

    prices = (
        pd.concat(price_frames, ignore_index=True)
        if price_frames
        else pd.DataFrame(columns=PRICE_COLUMNS)
    )
    prices = prices.sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"]).reset_index(drop=True)

    div_frames: list[pd.DataFrame] = []
    spl_frames: list[pd.DataFrame] = []
    failed_event_symbols: list[str] = []

    for sym in symbols:
        try:
            d, s = fetch_events_for_symbol(sym)
            if not d.empty:
                div_frames.append(d)
            if not s.empty:
                spl_frames.append(s)
        except Exception:
            failed_event_symbols.append(sym)
            continue

    dividends = (
        pd.concat(div_frames, ignore_index=True)
        if div_frames
        else pd.DataFrame(columns=DIVIDEND_COLUMNS)
    )
    dividends = (
        dividends.sort_values(["symbol", "date"])
        .drop_duplicates(subset=["symbol", "date"])
        .reset_index(drop=True)
    )

    splits = (
        pd.concat(spl_frames, ignore_index=True)
        if spl_frames
        else pd.DataFrame(columns=SPLIT_COLUMNS)
    )
    splits = (
        splits.sort_values(["symbol", "date"])
        .drop_duplicates(subset=["symbol", "date"])
        .reset_index(drop=True)
    )

    prices_csv = outdir / "prices_long.csv"
    dividends_csv = outdir / "dividends.csv"
    splits_csv = outdir / "splits.csv"

    prices.to_csv(prices_csv, index=False, encoding="utf-8")
    dividends.to_csv(dividends_csv, index=False, encoding="utf-8")
    splits.to_csv(splits_csv, index=False, encoding="utf-8")

    try:
        prices.to_parquet(outdir / "prices_long.parquet", index=False)
        dividends.to_parquet(outdir / "dividends.parquet", index=False)
        splits.to_parquet(outdir / "splits.parquet", index=False)
    except Exception:
        pass

    symbols_with_prices = prices["symbol"].dropna().astype(str).nunique() if not prices.empty else 0

    return {
        "input_csv": str(input_csv),
        "outdir": str(outdir),
        "n_symbols_requested": len(symbols),
        "n_symbols_with_prices": int(symbols_with_prices),
        "n_symbols_missing_prices": int(len(symbols) - symbols_with_prices),
        "n_event_failures": len(failed_event_symbols),
        "failed_event_symbols": failed_event_symbols,
        "prices_csv": str(prices_csv),
        "dividends_csv": str(dividends_csv),
        "splits_csv": str(splits_csv),
        "prices_rows": int(len(prices)),
        "dividends_rows": int(len(dividends)),
        "splits_rows": int(len(splits)),
    }