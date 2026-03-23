#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import json
import pandas as pd
import numpy as np


DEFAULT_DROPNA_COLS = ("document_score", "lmmd_net", "pos_rate", "neg_rate")
GPT_NEUTRAL_EPS = 0.10


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    try:
        return obj.item()  # numpy scalar -> native Python scalar
    except Exception:
        return str(obj)


def _write_qc_json(path: Path, qc: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(qc)
    payload["written_at"] = datetime.now().isoformat(timespec="seconds")
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default) + "\n",
        encoding="utf-8",
    )


def _safe_float(value: Any) -> Optional[float]:
    if pd.isna(value):
        return None
    return float(value)


def _safe_corr(s1: pd.Series, s2: pd.Series) -> Optional[float]:
    df = pd.concat([s1, s2], axis=1).dropna()
    if len(df) < 2:
        return None
    return _safe_float(df.iloc[:, 0].corr(df.iloc[:, 1]))


def _series_summary(s: pd.Series) -> Dict[str, Any]:
    s = pd.to_numeric(s, errors="coerce")
    return {
        "mean": _safe_float(s.mean()),
        "median": _safe_float(s.median()),
        "std": _safe_float(s.std(ddof=0)),
        "min": _safe_float(s.min()),
        "p10": _safe_float(s.quantile(0.10)),
        "p25": _safe_float(s.quantile(0.25)),
        "p50": _safe_float(s.quantile(0.50)),
        "p75": _safe_float(s.quantile(0.75)),
        "p90": _safe_float(s.quantile(0.90)),
        "max": _safe_float(s.max()),
        "share_negative": _safe_float((s < 0).mean()),
        "share_zero": _safe_float((s == 0).mean()),
        "share_positive": _safe_float((s > 0).mean()),
    }


def _classify_sentiment(series: pd.Series, eps: float) -> pd.Series:
    out = pd.Series("neutral", index=series.index, dtype="string")
    out = out.mask(series > eps, "positive")
    out = out.mask(series < -eps, "negative")
    return out


def _normalize_input(df: pd.DataFrame, *, name: str, source_path: Path) -> pd.DataFrame:
    df = df.copy()

    if "filing_date" not in df.columns and "filing_date_parsed" in df.columns:
        df = df.rename(columns={"filing_date_parsed": "filing_date"})

    if "filing_date" not in df.columns:
        raise ValueError(f"{name} panel missing 'filing_date' column: {source_path}")
    if "symbol" not in df.columns:
        raise ValueError(f"{name} panel missing 'symbol' column: {source_path}")

    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df = df.dropna(subset=["symbol", "filing_date"]).copy()
    df["symbol"] = df["symbol"].astype("string")
    return df


def _add_disagreement_features(merged: pd.DataFrame) -> Dict[str, float]:
    merged["polarity_conflict"] = (
        ((merged["lmmd_net"] > 0) & (merged["document_score"] < 0)) |
        ((merged["lmmd_net"] < 0) & (merged["document_score"] > 0))
    ).astype(int)

    merged["same_sign_nonzero"] = (
        ((merged["lmmd_net"] > 0) & (merged["document_score"] > 0)) |
        ((merged["lmmd_net"] < 0) & (merged["document_score"] < 0))
    ).astype(int)

    merged["sent_disagreement_raw"] = merged["lmmd_net"] - merged["document_score"]
    merged["sent_disagreement_abs"] = merged["sent_disagreement_raw"].abs()

    doc_mean = merged["document_score"].mean()
    lmmd_mean = merged["lmmd_net"].mean()
    doc_std = merged["document_score"].std(ddof=0)
    lmmd_std = merged["lmmd_net"].std(ddof=0)

    if pd.notna(doc_std) and doc_std > 0:
        merged["document_score_z"] = (merged["document_score"] - doc_mean) / doc_std
    else:
        merged["document_score_z"] = 0.0

    if pd.notna(lmmd_std) and lmmd_std > 0:
        merged["lmmd_net_z"] = (merged["lmmd_net"] - lmmd_mean) / lmmd_std
    else:
        merged["lmmd_net_z"] = 0.0

    merged["sent_disagreement_z"] = merged["lmmd_net_z"] - merged["document_score_z"]
    merged["sent_disagreement_abs_z"] = merged["sent_disagreement_z"].abs()

    if "num_chunks" in merged.columns:
        merged["gpt_valid"] = (merged["num_chunks"] > 0).astype(int)
    else:
        merged["gpt_valid"] = 1

    lmmd_eps = merged["lmmd_net"].abs().median()

    merged["document_sent_class"] = _classify_sentiment(merged["document_score"], GPT_NEUTRAL_EPS)
    merged["lmmd_sent_class"] = _classify_sentiment(merged["lmmd_net"], lmmd_eps)

    merged["polarity_conflict_thresh"] = (
        ((merged["document_sent_class"] == "positive") & (merged["lmmd_sent_class"] == "negative")) |
        ((merged["document_sent_class"] == "negative") & (merged["lmmd_sent_class"] == "positive"))
    ).astype(int)

    merged["same_polarity_thresh"] = (
        ((merged["document_sent_class"] == "positive") & (merged["lmmd_sent_class"] == "positive")) |
        ((merged["document_sent_class"] == "negative") & (merged["lmmd_sent_class"] == "negative"))
    ).astype(int)

    merged["either_neutral_thresh"] = (
        (merged["document_sent_class"] == "neutral") |
        (merged["lmmd_sent_class"] == "neutral")
    ).astype(int)

    return {
        "gpt_eps": GPT_NEUTRAL_EPS,
        "lmmd_eps": float(lmmd_eps),
    }


def _example_columns(df: pd.DataFrame) -> list[str]:
    preferred = [
        "filename_x",
        "filename_y",
        "symbol",
        "edinet_code_x",
        "edinet_code_y",
        "filing_date",
        "document_score",
        "lmmd_net",
        "document_sent_class",
        "lmmd_sent_class",
        "sent_disagreement_raw",
        "sent_disagreement_abs",
        "sent_disagreement_z",
        "sent_disagreement_abs_z",
        "polarity_conflict",
        "polarity_conflict_thresh",
        "same_sign_nonzero",
        "same_polarity_thresh",
        "token_count",
        "num_chunks",
    ]
    return [c for c in preferred if c in df.columns]


def _build_qc(merged: pd.DataFrame, *, gpt_csv: Path, lmmd_csv: Path, gpt_df: pd.DataFrame, lmmd_df: pd.DataFrame, eps: Dict[str, float]) -> Dict[str, Any]:
    gpt_keys = gpt_df[["symbol", "filing_date"]]
    lmmd_keys = lmmd_df[["symbol", "filing_date"]]

    qc: Dict[str, Any] = {
        "inputs": {
            "gpt_csv": str(gpt_csv),
            "lmmd_csv": str(lmmd_csv),
        },
        "rows": {
            "gpt": int(len(gpt_df)),
            "lmmd": int(len(lmmd_df)),
            "merged_pre_dropna": None,
            "merged_post_dropna": int(len(merged)),
        },
        "keys": {
            "gpt_unique_keys": int(gpt_keys.drop_duplicates().shape[0]),
            "lmmd_unique_keys": int(lmmd_keys.drop_duplicates().shape[0]),
            "gpt_duplicate_keys": int(len(gpt_df) - gpt_keys.drop_duplicates().shape[0]),
            "lmmd_duplicate_keys": int(len(lmmd_df) - lmmd_keys.drop_duplicates().shape[0]),
        },
        "neutral_bands": {
            "document_score": {
                "epsilon": float(eps["gpt_eps"]),
                "share_negative": _safe_float((merged["document_sent_class"] == "negative").mean()),
                "share_neutral": _safe_float((merged["document_sent_class"] == "neutral").mean()),
                "share_positive": _safe_float((merged["document_sent_class"] == "positive").mean()),
            },
            "lmmd_net": {
                "epsilon": float(eps["lmmd_eps"]),
                "share_negative": _safe_float((merged["lmmd_sent_class"] == "negative").mean()),
                "share_neutral": _safe_float((merged["lmmd_sent_class"] == "neutral").mean()),
                "share_positive": _safe_float((merged["lmmd_sent_class"] == "positive").mean()),
            },
        },
        "score_distributions": {
            "document_score": _series_summary(merged["document_score"]),
            "lmmd_net": _series_summary(merged["lmmd_net"]),
        },
    }

    qc["disagreement"] = {
        "polarity_conflict_count": int(merged["polarity_conflict"].sum()),
        "polarity_conflict_rate": _safe_float(merged["polarity_conflict"].mean()),
        "same_sign_nonzero_count": int(merged["same_sign_nonzero"].sum()),
        "same_sign_nonzero_rate": _safe_float(merged["same_sign_nonzero"].mean()),
        "sent_disagreement_abs_mean": _safe_float(merged["sent_disagreement_abs"].mean()),
        "sent_disagreement_abs_median": _safe_float(merged["sent_disagreement_abs"].median()),
        "sent_disagreement_abs_z_mean": _safe_float(merged["sent_disagreement_abs_z"].mean()),
        "sent_disagreement_abs_z_median": _safe_float(merged["sent_disagreement_abs_z"].median()),
        "sent_disagreement_abs_z_p90": _safe_float(merged["sent_disagreement_abs_z"].quantile(0.90)),
        "sent_disagreement_abs_z_p95": _safe_float(merged["sent_disagreement_abs_z"].quantile(0.95)),
        "corr_document_score_lmmd_net": _safe_corr(merged["document_score"], merged["lmmd_net"]),
        "polarity_conflict_thresh_count": int(merged["polarity_conflict_thresh"].sum()),
        "polarity_conflict_thresh_rate": _safe_float(merged["polarity_conflict_thresh"].mean()),
        "same_polarity_thresh_count": int(merged["same_polarity_thresh"].sum()),
        "same_polarity_thresh_rate": _safe_float(merged["same_polarity_thresh"].mean()),
        "either_neutral_thresh_count": int(merged["either_neutral_thresh"].sum()),
        "either_neutral_thresh_rate": _safe_float(merged["either_neutral_thresh"].mean()),
    }

    if "token_count" in merged.columns:
        qc["disagreement"]["corr_abs_disagreement_z_token_count"] = _safe_corr(
            merged["sent_disagreement_abs_z"], merged["token_count"]
        )

    if "num_chunks" in merged.columns:
        qc["disagreement"]["corr_abs_disagreement_z_num_chunks"] = _safe_corr(
            merged["sent_disagreement_abs_z"], merged["num_chunks"]
        )

    if "gpt_valid" in merged.columns:
        qc["disagreement"]["gpt_valid_count"] = int(merged["gpt_valid"].sum())
        qc["disagreement"]["gpt_valid_rate"] = _safe_float(merged["gpt_valid"].mean())

    top_cols = _example_columns(merged)

    qc["disagreement"]["top_abs_disagreement_examples"] = (
        merged.sort_values("sent_disagreement_abs_z", ascending=False)[top_cols]
        .head(10)
        .to_dict(orient="records")
    )

    qc["disagreement"]["top_polarity_conflict_examples"] = (
        merged.loc[merged["polarity_conflict"] == 1, top_cols]
        .sort_values("sent_disagreement_abs_z", ascending=False)
        .head(10)
        .to_dict(orient="records")
    )

    valid = merged["gpt_valid"] == 1 if "gpt_valid" in merged.columns else pd.Series(True, index=merged.index)

    qc["disagreement_valid_gpt"] = {
        "rows": int(valid.sum()),
        "polarity_conflict_rate": _safe_float(merged.loc[valid, "polarity_conflict"].mean()),
        "polarity_conflict_thresh_rate": _safe_float(merged.loc[valid, "polarity_conflict_thresh"].mean()),
        "sent_disagreement_abs_z_mean": _safe_float(merged.loc[valid, "sent_disagreement_abs_z"].mean()),
        "sent_disagreement_abs_z_median": _safe_float(merged.loc[valid, "sent_disagreement_abs_z"].median()),
        "corr_document_score_lmmd_net": _safe_corr(
            merged.loc[valid, "document_score"],
            merged.loc[valid, "lmmd_net"],
        ),
    }

    qc["disagreement"]["top_polarity_conflict_thresh_examples"] = (
        merged.loc[valid & (merged["polarity_conflict_thresh"] == 1), top_cols]
        .sort_values("sent_disagreement_abs_z", ascending=False)
        .head(10)
        .to_dict(orient="records")
    )

    qc["controls"] = {
        "ln_market_cap_coverage": float(merged["ln_market_cap"].notna().mean()) if "ln_market_cap" in merged else None,
        "volatility_coverage": float(merged["volatility"].notna().mean()) if "volatility" in merged else None,
        "ln_volatility_coverage": float(merged["ln_volatility"].notna().mean()) if "ln_volatility" in merged else None
    }

    return qc


# derived from old merge_gpt_lmmd_panel.py
def build_panel(
    *,
    gpt_csv,
    lmmd_csv,
    fundamentals_csv,
    price_features_csv,
    out_csv: Path | str,
    how: str = "inner",
    dropna_cols: Optional[Iterable[str]] = DEFAULT_DROPNA_COLS,
    qc_json: Path | str | None = None,
) -> pd.DataFrame:
    """
    Merge GPT sentiment panel with LMMD scores into a single regression-ready panel.

    Goal: match legacy merge_gpt_lmmd_panel.py output structure where possible
    while adding disagreement features and QC outputs.
    """
    gpt_csv = Path(gpt_csv)
    lmmd_csv = Path(lmmd_csv)
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if qc_json is None:
        qc_json = out_csv.parent / f"{out_csv.name}.qc.json"
    qc_json = Path(qc_json)

    gpt_df = pd.read_csv(gpt_csv, dtype={"symbol": "string"})
    lmmd_df = pd.read_csv(lmmd_csv, dtype={"symbol": "string"})

    gpt_df = _normalize_input(gpt_df, name="GPT", source_path=gpt_csv)
    lmmd_df = _normalize_input(lmmd_df, name="LMMD", source_path=lmmd_csv)

    merged_pre = pd.merge(
        gpt_df,
        lmmd_df,
        on=["symbol", "filing_date"],
        how=how,
    )

    merged = merged_pre.copy()

    if fundamentals_csv is not None:
        fundamentals = pd.read_csv(fundamentals_csv, dtype={"symbol": "string"})
        fundamentals = _normalize_input(fundamentals, name="FUNDAMENTALS", source_path=Path(fundamentals_csv))
    
        fundamentals = fundamentals.rename(columns={"filing_date": "filing_date"})
    
        keep_cols = ["symbol", "filing_date", "market_cap", "ln_market_cap"]
        fundamentals = fundamentals[[c for c in keep_cols if c in fundamentals.columns]]
    
        merged = pd.merge(
            merged,
            fundamentals,
            on=["symbol", "filing_date"],
            how="left",
        )

    if price_features_csv is not None:
        pf = pd.read_csv(price_features_csv, dtype={"symbol": "string"})
        pf["date"] = pd.to_datetime(pf["date"])
    
        merged = merged.sort_values(["filing_date", "symbol"]).reset_index(drop=True)
        pf = pf.sort_values(["date", "symbol"]).reset_index(drop=True)
    
        merged = pd.merge_asof(
            merged,
            pf,
            left_on="filing_date",
            right_on="date",
            by="symbol",
            direction="backward",
        )

    if "vol_60_11" in merged.columns:
        merged["volatility"] = pd.to_numeric(merged["vol_60_11"], errors="coerce")
        merged.loc[merged["volatility"] <= 0, "volatility"] = np.nan
        merged["ln_volatility"] = np.log(merged["volatility"])

    merged = merged.drop(columns=["date"], errors="ignore")

    if dropna_cols:
        missing = [c for c in dropna_cols if c not in merged.columns]
        if missing:
            raise ValueError(f"Merged panel missing expected columns {missing}. Columns={list(merged.columns)}")
        merged = merged.dropna(subset=list(dropna_cols)).copy()

    eps = _add_disagreement_features(merged)
    qc = _build_qc(
        merged,
        gpt_csv=gpt_csv,
        lmmd_csv=lmmd_csv,
        gpt_df=gpt_df,
        lmmd_df=lmmd_df,
        eps=eps,
    )
    qc["rows"]["merged_pre_dropna"] = int(len(merged_pre))

    merged = merged.drop(
        columns=[
            "filename_x",
            "filename_y",
            "edinet_code_x",
            "edinet_code_y",
            "filing_date_parsed",
            "status",
            "pos_count",
            "neg_count",
        ],
        errors="ignore",
    )

    qc["outputs"] = {
        "out_csv": str(out_csv),
        "qc_json": str(qc_json),
        "columns": int(len(merged.columns)),
        "column_names": list(merged.columns),
    }

    if dropna_cols:
        qc["missing_after_dropna"] = {c: int(merged[c].isna().sum()) for c in dropna_cols}

    merged.to_csv(out_csv, index=False, encoding="utf-8-sig")
    _write_qc_json(qc_json, qc)

    return merged