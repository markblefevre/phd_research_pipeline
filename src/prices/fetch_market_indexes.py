#!/usr/bin/env python3
"""
Fetch Nikkei 225 (Yahoo) and TOPIX (J-Quants / Yahoo / ETF) with JST trading_date.

Outputs (default: out/):
  out/N225_prices.csv
  out/TOPIX_prices.csv

Columns:
  date, symbol, series_kind, open, high, low, close, adj_close, volume, trading_date

Examples:
  python fetch_market_indexes.py --start 2024-01-01 --topix-source jpx
  python fetch_market_indexes.py --start 2024-01-01 --topix-source auto
  python fetch_market_indexes.py --start 2024-01-01 --topix-source yahoo

Environment:
  JQUANTS_API_KEY=...

Dependencies:
  pandas, yfinance, requests
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

try:
    import yfinance as yf
except Exception:
    yf = None

try:
    import requests
except Exception:
    requests = None


# -------------------------------
# Config
# -------------------------------

NIKKEI_CANDIDATES: List[Tuple[str, str]] = [
    ("NIKKEI_INDEX", "^N225"),
    ("NIKKEI_ETF_NOMURA", "1321.T"),
    ("NIKKEI_ETF_ISHARES", "1329.T"),
]

TOPIX_CANDIDATES_YAHOO: List[Tuple[str, str]] = [
    ("TOPIX_INDEX_JP", "998405.T"),
    ("TOPIX_INDEX_US", "^TOPX"),
    ("TOPIX_ETF_NOMURA", "1306.T"),
    ("TOPIX_ETF_ISHARES", "1475.T"),
]

TOPIX_CANDIDATES_ETF: List[Tuple[str, str]] = [
    ("TOPIX_ETF_NOMURA", "1306.T"),
    ("TOPIX_ETF_ISHARES", "1475.T"),
]

JPX_BASE = "https://api.jquants.com"
JPX_TOPIX = "/v2/indices/bars/daily/topix"


# -------------------------------
# Common utilities
# -------------------------------

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["open", "high", "low", "close", "adj_close", "volume"]:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def _add_trading_date_jst(date_like: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Return:
      - date_export_naive: naive UTC-like timestamp for CSV export
      - trading_date_JST: date in Asia/Tokyo
    """
    dt = pd.to_datetime(date_like, errors="coerce")
    if getattr(dt.dtype, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC")
    dt_jst = dt.dt.tz_convert("Asia/Tokyo")
    trading_date = dt_jst.dt.normalize().dt.date
    date_export = dt.dt.tz_convert(None)
    return date_export, trading_date


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _standardize_final(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _ensure_cols(df)
    if "date" not in df.columns:
        raise ValueError("Output frame missing required 'date' column")
    if "symbol" not in df.columns:
        raise ValueError("Output frame missing required 'symbol' column")
    if "series_kind" not in df.columns:
        raise ValueError("Output frame missing required 'series_kind' column")

    date_export, trading_date = _add_trading_date_jst(df["date"])
    df["date"] = date_export
    df["trading_date"] = trading_date

    out_cols = [
        "date",
        "symbol",
        "series_kind",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "trading_date",
    ]
    out = df[out_cols].copy()
    out = out.sort_values("date").dropna(subset=["date"]).reset_index(drop=True)
    return out


# -------------------------------
# Yahoo / ETF fetchers
# -------------------------------

def _patch_timezone_if_needed(ticker_obj, ticker: str) -> None:
    # Workaround for 998405.T missing tz
    if ticker == "998405.T":
        try:
            if not hasattr(ticker_obj, "_tz") or ticker_obj._tz is None:
                ticker_obj._tz = "Asia/Tokyo"
            if hasattr(ticker_obj, "fast_info") and ticker_obj.fast_info is not None:
                try:
                    ticker_obj.fast_info["timezone"] = "Asia/Tokyo"
                except Exception:
                    pass
        except Exception:
            pass


def _yf_history_then_download(ticker: str, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    if yf is None:
        raise RuntimeError("yfinance is required; install with pip install yfinance")

    t = yf.Ticker(ticker)
    _patch_timezone_if_needed(t, ticker)

    try:
        df = t.history(
            start=start,
            end=end,
            interval="1d",
            actions=False,
            auto_adjust=False,
            repair=True,
        )
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        try:
            df = yf.download(
                ticker,
                start=start,
                end=end,
                progress=False,
                auto_adjust=False,
                group_by="ticker",
                threads=False,
                repair=True,
            )
        except Exception:
            df = pd.DataFrame()

    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _standardize_yf(df: pd.DataFrame, ticker: str, kind: str) -> pd.DataFrame:
    rename = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
        "Date": "date",
        "Datetime": "date",
    }
    df = df.rename(columns=rename)
    if "date" not in df.columns:
        df = df.reset_index().rename(columns=rename)

    df = df.copy()
    df["symbol"] = ticker
    df["series_kind"] = kind
    return _standardize_final(df)


def fetch_with_fallback_yahoo(
    candidates: List[Tuple[str, str]],
    start: Optional[str],
    end: Optional[str],
) -> pd.DataFrame:
    last_err = None
    for kind, ticker in candidates:
        try:
            df_raw = _yf_history_then_download(ticker, start, end)
            if not df_raw.empty:
                return _standardize_yf(df_raw, ticker, kind)
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(
        "Yahoo/ETF: no data from candidates: "
        + ", ".join([f"{k}:{t}" for k, t in candidates])
        + (f"\nLast error: {last_err}" if last_err else "")
    )


# -------------------------------
# JPX (J-Quants) fetcher
# -------------------------------

def _extract_rows_topix(js: dict) -> list:
    """
    Be tolerant about response payload shape.
    """
    if not isinstance(js, dict):
        return []
    for key in ("daily_topix", "topix", "data"):
        rows = js.get(key)
        if isinstance(rows, list):
            return rows
    return []


def _normalize_jpx_topix_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]

    rename_map = {
        "o": "open",
        "openprice": "open",
        "open": "open",
        "h": "high",
        "highprice": "high",
        "high": "high",
        "l": "low",
        "lowprice": "low",
        "low": "low",
        "c": "close",
        "closeprice": "close",
        "close": "close",
        "code": "symbol",
        "date": "date",
    }
    df = df.rename(columns=rename_map)

    if "symbol" not in df.columns:
        df["symbol"] = "TOPIX"

    df["series_kind"] = "TOPIX"
    df["adj_close"] = df["close"] if "close" in df.columns else pd.NA
    df["volume"] = pd.NA

    return _standardize_final(df)


def fetch_topix_jpx(
    api_key: str,
    start: Optional[str],
    end: Optional[str],
    verbose: bool = False,
) -> pd.DataFrame:
    if requests is None:
        raise RuntimeError("requests is required for JPX/J-Quants access; install with pip install requests")

    headers = {"x-api-key": api_key}
    params = {}
    if start:
        params["from"] = start  # keep YYYY-MM-DD
    if end:
        params["to"] = end

    url = JPX_BASE + JPX_TOPIX
    all_rows = []

    while True:
        r = requests.get(url, headers=headers, params=params, timeout=30)

        if verbose:
            print("JPX URL:", url)
            print("JPX params:", params)
            print("JPX status:", r.status_code)
            if r.status_code != 200:
                print("JPX body:", r.text[:1000])

        if r.status_code == 403:
            raise RuntimeError("JPX forbidden: check API key / subscription / endpoint access")
        if r.status_code == 401:
            raise RuntimeError("JPX unauthorized: check API key")
        if r.status_code >= 400:
            raise RuntimeError(f"JPX HTTP {r.status_code}: {r.text[:500]}")

        js = r.json() or {}
        rows = _extract_rows_topix(js)
        all_rows.extend(rows)

        nxt = js.get("pagination_key")
        if not nxt:
            break

        params["pagination_key"] = nxt
        time.sleep(0.2)

    if not all_rows:
        raise RuntimeError("JPX TOPIX: no data returned for requested window")

    df = pd.DataFrame(all_rows)
    return _normalize_jpx_topix_df(df)


def run_fetch_market_indexes(
    *,
    outdir: str | Path,
    start: str | None,
    end: str | None,
    topix_source: str = "jpx",
    jpx_api_key: str | None = None,
    verbose_jpx: bool = False,
) -> dict:
    outdir = Path(outdir)

    nik = fetch_with_fallback_yahoo(NIKKEI_CANDIDATES, start, end)
    _write_csv(nik, outdir / "N225_prices.csv")

    topix_df = None
    err_msgs: list[str] = []

    def try_jpx() -> pd.DataFrame:
        if not jpx_api_key:
            raise RuntimeError("JPX requires jpx_api_key or env JQUANTS_API_KEY")
        return fetch_topix_jpx(
            api_key=jpx_api_key,
            start=start,
            end=end,
            verbose=verbose_jpx,
        )

    if topix_source in ("auto", "jpx"):
        try:
            topix_df = try_jpx()
        except Exception as e:
            err_msgs.append(f"JPX failed: {e}")

    if topix_df is None and topix_source in ("auto", "yahoo"):
        try:
            topix_df = fetch_with_fallback_yahoo(TOPIX_CANDIDATES_YAHOO, start, end)
        except Exception as e:
            err_msgs.append(f"Yahoo failed: {e}")

    if topix_df is None and topix_source in ("auto", "etf"):
        try:
            topix_df = fetch_with_fallback_yahoo(TOPIX_CANDIDATES_ETF, start, end)
        except Exception as e:
            err_msgs.append(f"ETF failed: {e}")

    if topix_df is None:
        raise RuntimeError("All TOPIX sources failed:\n" + "\n".join(err_msgs))

    _write_csv(topix_df, outdir / "TOPIX_prices.csv")

    return {
        "outdir": str(outdir),
        "n225_csv": str(outdir / "N225_prices.csv"),
        "topix_csv": str(outdir / "TOPIX_prices.csv"),
        "n225_rows": int(len(nik)),
        "topix_rows": int(len(topix_df)),
        "topix_source_used": str(topix_df["series_kind"].iat[-1]) if not topix_df.empty else None,
    }

# -------------------------------
# Main
# -------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch Nikkei 225 (Yahoo) and TOPIX (JPX/Yahoo/ETF) with JST trading_date."
    )
    ap.add_argument("--start", type=str, default="2024-01-01", help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD")
    ap.add_argument("--outdir", type=str, default="out", help="Output directory")
    ap.add_argument(
        "--topix-source",
        choices=["auto", "jpx", "yahoo", "etf"],
        default="jpx",
        help="TOPIX data source preference",
    )
    ap.add_argument(
        "--jpx-api-key",
        type=str,
        default=os.getenv("JQUANTS_API_KEY"),
        help="JPX J-Quants API key (or set JQUANTS_API_KEY)",
    )
    ap.add_argument(
        "--verbose-jpx",
        action="store_true",
        help="Print JPX request/response debug info",
    )
    args = ap.parse_args()

    outdir = Path(args.outdir)

    # Nikkei 225
    print("=== Fetching Nikkei 225 (Yahoo) ===")
    nik = fetch_with_fallback_yahoo(NIKKEI_CANDIDATES, args.start, args.end)
    _write_csv(nik, outdir / "N225_prices.csv")
    print(
        f"  -> {len(nik)} rows -> {outdir / 'N225_prices.csv'} "
        f"(source: {nik['series_kind'].iat[-1]} / {nik['symbol'].iat[-1]})"
    )

    # TOPIX
    print(f"=== Fetching TOPIX ({args.topix_source.upper()}) ===")
    topix_df = None
    err_msgs: list[str] = []

    def try_jpx() -> pd.DataFrame:
        if not args.jpx_api_key:
            raise RuntimeError("JPX requires --jpx-api-key or env JQUANTS_API_KEY")
        return fetch_topix_jpx(
            api_key=args.jpx_api_key,
            start=args.start,
            end=args.end,
            verbose=args.verbose_jpx,
        )

    if args.topix_source in ("auto", "jpx"):
        try:
            topix_df = try_jpx()
        except Exception as e:
            err_msgs.append(f"JPX failed: {e}")

    if topix_df is None and args.topix_source in ("auto", "yahoo"):
        try:
            topix_df = fetch_with_fallback_yahoo(TOPIX_CANDIDATES_YAHOO, args.start, args.end)
        except Exception as e:
            err_msgs.append(f"Yahoo failed: {e}")

    if topix_df is None and args.topix_source in ("auto", "etf"):
        try:
            topix_df = fetch_with_fallback_yahoo(TOPIX_CANDIDATES_ETF, args.start, args.end)
        except Exception as e:
            err_msgs.append(f"ETF failed: {e}")

    if topix_df is None:
        raise RuntimeError("All TOPIX sources failed:\n" + "\n".join(err_msgs))

    _write_csv(topix_df, outdir / "TOPIX_prices.csv")
    print(
        f"  -> {len(topix_df)} rows -> {outdir / 'TOPIX_prices.csv'} "
        f"(source: {topix_df['series_kind'].iat[-1]} / {topix_df['symbol'].iat[-1]})"
    )

    print("Done.")


if __name__ == "__main__":
    main()