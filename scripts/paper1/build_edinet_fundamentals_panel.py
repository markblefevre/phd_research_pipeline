#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import math

import pandas as pd

from edinet import extract_values, taxonomy_info
from edinet.financial.standards.canonical_keys import CK
from edinet.financial.statements import build_statements
from edinet.xbrl.facts import build_line_items
from edinet.xbrl.parser import parse_xbrl_facts
from edinet.xbrl.contexts import structure_contexts
from edinet.xbrl.taxonomy import TaxonomyResolver

from src.utils.project_paths import get_project_root


INPUT_CSV = Path("data/curated/paper1/panel/lmmd_scores_nikkei225.csv")
PRICES_CSV = Path("data/curated/paper1/prices/prices_long.csv")
XBRL_ROOT = Path("data/interim/paper1/data")
OUT_CSV = Path("data/curated/paper1/panel/edinet_fundamentals_from_xbrl.csv")
QC_JSON = Path("data/curated/paper1/panel/edinet_fundamentals_from_xbrl.qc.json")


def to_int(x):
    try:
        if pd.isna(x):
            return None
        return int(float(str(x).replace(",", "")))
    except Exception:
        return None


def to_float(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def extract_treasury_shares(items) -> str | None:
    values = []

    for item in items:
        concept = str(getattr(item, "concept", "")).lower()
        value = getattr(item, "value", None)

        if "totalnumberofsharesheldtreasurysharesetc" in concept:
            if value is not None:
                try:
                    v = int(value)
                    if v > 0:
                        values.append(v)
                except Exception:
                    pass

    if not values:
        return None

    return str(max(values))


def extract_ck_values(
    xbrl_path: Path,
    resolver: TaxonomyResolver,
    taxonomy_root: Path,
) -> dict[str, Any]:
    out = {
        "total_shares_issued": None,
        "total_assets": None,
        "net_assets": None,
        "treasury_stock": None,
        "treasury_shares": None,
        "extract_error": None,
    }

    try:
        xbrl_bytes = xbrl_path.read_bytes()
        parsed = parse_xbrl_facts(xbrl_bytes, source_path=str(xbrl_path), strict=False)

        facts = parsed.facts
        raw_contexts = parsed.contexts
        context_map = structure_contexts(raw_contexts)

        items = build_line_items(
            facts=facts,
            context_map=context_map,
            resolver=resolver,
        )

        treasury_shares = extract_treasury_shares(items)

        stmts = build_statements(
            items,
            facts=tuple(facts),
            contexts=context_map,
            taxonomy_root=taxonomy_root,
            resolver=resolver,
        )

        result = extract_values(
            stmts,
            [CK.TOTAL_SHARES_ISSUED, CK.TOTAL_ASSETS, CK.NET_ASSETS, CK.TREASURY_STOCK],
        )

        def get_value(key):
            item = result.get(key)
            return None if item is None else str(item.value)

        out["total_shares_issued"] = get_value(CK.TOTAL_SHARES_ISSUED)
        out["total_assets"] = get_value(CK.TOTAL_ASSETS)
        out["net_assets"] = get_value(CK.NET_ASSETS)
        out["treasury_stock"] = get_value(CK.TREASURY_STOCK)
        out["treasury_shares"] = treasury_shares

    except Exception as e:
        out["extract_error"] = str(e)

    return out


def load_prices(prices_csv: Path) -> pd.DataFrame:
    prices = pd.read_csv(prices_csv)
    prices["date"] = pd.to_datetime(prices["date"])
    prices["symbol"] = prices["symbol"].astype(str).str.strip()

    prices = prices.sort_values(["symbol", "date"]).reset_index(drop=True)

    keep_cols = ["date", "symbol", "close", "adj_close"]
    return prices[keep_cols].copy()


def attach_prices_asof(fund_df: pd.DataFrame, prices_df: pd.DataFrame) -> pd.DataFrame:
    # 🔥 DROP existing price columns to avoid suffix mess
    fund_df = fund_df.drop(columns=["price_date", "close", "adj_close"], errors="ignore")

    out_frames = []

    for symbol, g in fund_df.groupby("symbol", dropna=False):
        g = g.sort_values("filing_date_parsed").copy()

        p = (
            prices_df.loc[prices_df["symbol"] == symbol, ["date", "close", "adj_close"]]
            .sort_values("date")
            .copy()
        )

        if p.empty:
            g["price_date"] = pd.NaT
            g["close"] = pd.NA
            g["adj_close"] = pd.NA
            out_frames.append(g)
            continue

        merged = pd.merge_asof(
            g,
            p,
            left_on="filing_date_parsed",
            right_on="date",
            direction="backward",
        )

        merged = merged.rename(columns={"date": "price_date"})
        out_frames.append(merged)

    return pd.concat(out_frames, ignore_index=True)


def build_qc(df: pd.DataFrame) -> dict:
    df_num = df.copy()

    for col in [
        "total_shares_issued",
        "treasury_shares",
        "shares_outstanding",
        "close",
        "market_cap",
        "ln_market_cap",
    ]:
        if col in df_num.columns:
            df_num[col] = pd.to_numeric(df_num[col], errors="coerce")

    qc = {
        "n_obs": int(len(df)),
        "coverage": {
            "total_shares_issued": float(df_num["total_shares_issued"].notna().mean()),
            "treasury_shares": float(df_num["treasury_shares"].notna().mean()),
            "close": float(df_num["close"].notna().mean()),
            "market_cap": float(df_num["market_cap"].notna().mean()),
            "ln_market_cap": float(df_num["ln_market_cap"].notna().mean()),
        },
        "shares_outstanding": {
            "min": float(df_num["shares_outstanding"].min()),
            "p1": float(df_num["shares_outstanding"].quantile(0.01)),
            "median": float(df_num["shares_outstanding"].median()),
            "mean": float(df_num["shares_outstanding"].mean()),
            "p99": float(df_num["shares_outstanding"].quantile(0.99)),
            "max": float(df_num["shares_outstanding"].max()),
        },
        "market_cap": {
            "min": float(df_num["market_cap"].min()),
            "p1": float(df_num["market_cap"].quantile(0.01)),
            "median": float(df_num["market_cap"].median()),
            "mean": float(df_num["market_cap"].mean()),
            "p99": float(df_num["market_cap"].quantile(0.99)),
            "max": float(df_num["market_cap"].max()),
        },
        "ln_market_cap": {
            "min": float(df_num["ln_market_cap"].min()),
            "p1": float(df_num["ln_market_cap"].quantile(0.01)),
            "median": float(df_num["ln_market_cap"].median()),
            "mean": float(df_num["ln_market_cap"].mean()),
            "p99": float(df_num["ln_market_cap"].quantile(0.99)),
            "max": float(df_num["ln_market_cap"].max()),
        },
        "sanity_checks": {
            "non_positive_shares_outstanding": int((df_num["shares_outstanding"] <= 0).sum()),
            "missing_prices": int(df_num["close"].isna().sum()),
            "non_positive_market_cap": int((df_num["market_cap"] <= 0).sum()),
            "missing_ln_market_cap": int(df_num["ln_market_cap"].isna().sum()),
        },
    }

    return qc


def main() -> None:
    repo_root = get_project_root()

    in_csv = repo_root / INPUT_CSV
    prices_csv = repo_root / PRICES_CSV
    xbrl_root = repo_root / XBRL_ROOT
    out_csv = repo_root / OUT_CSV
    qc_json = repo_root / QC_JSON

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    qc_json.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_csv)
    df["filing_date_parsed"] = pd.to_datetime(df["filing_date_parsed"])

    prices_df = load_prices(prices_csv)

    taxonomy_root = Path(taxonomy_info().path)
    resolver = TaxonomyResolver(taxonomy_root)

    rows = []

    for _, row in df.iterrows():
        filename = str(row["filename"])
        edinet_code = str(row["edinet_code"]).strip()
        symbol = str(row["symbol"]).strip()
        filing_date = row["filing_date_parsed"]

        xbrl_path = xbrl_root / edinet_code / filename

        rec = {
            "filename": filename,
            "edinet_code": edinet_code,
            "symbol": symbol,
            "filing_date_parsed": filing_date,
            "xbrl_path": str(xbrl_path),
            "extract_status": "ok",
            "extract_error": None,
            "total_shares_issued": None,
            "total_assets": None,
            "net_assets": None,
            "treasury_stock": None,
            "treasury_shares": None,
            "shares_outstanding": None,
            "price_date": pd.NaT,
            "close": None,
            "adj_close": None,
            "market_cap": None,
            "ln_market_cap": None,
        }

        vals = extract_ck_values(xbrl_path, resolver, taxonomy_root)
        rec.update(vals)

        if any(rec[k] is not None for k in ["total_shares_issued", "total_assets", "net_assets"]):
            rec["extract_status"] = "ok" if vals["extract_error"] is None else "partial_error"
        else:
            rec["extract_status"] = "error"

        issued = to_int(rec["total_shares_issued"])

        # Consistent choice for the paper: use issued shares for all firms.
        if issued is not None and issued > 0:
            rec["shares_outstanding"] = issued

        rows.append(rec)

    out = pd.DataFrame(rows)

    out = attach_prices_asof(out, prices_df)

    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["adj_close"] = pd.to_numeric(out["adj_close"], errors="coerce")
    out["shares_outstanding"] = pd.to_numeric(out["shares_outstanding"], errors="coerce")

    valid_mc = (
        out["shares_outstanding"].notna()
        & out["close"].notna()
        & (out["shares_outstanding"] > 0)
        & (out["close"] > 0)
    )

    out.loc[valid_mc, "market_cap"] = (
        out.loc[valid_mc, "shares_outstanding"] * out.loc[valid_mc, "close"]
    )
    out.loc[valid_mc, "ln_market_cap"] = out.loc[valid_mc, "market_cap"].map(math.log)

    out.to_csv(out_csv, index=False)

    qc = build_qc(out)
    with open(qc_json, "w", encoding="utf-8") as f:
        json.dump(qc, f, indent=2)

    print(f"Wrote: {out_csv}")
    print(f"Wrote QC: {qc_json}")
    print(out["extract_status"].value_counts(dropna=False))


if __name__ == "__main__":
    main()