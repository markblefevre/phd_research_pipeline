#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime
import json

import pandas as pd

from sudachipy import tokenizer
from src.utils.text_utils import tokenize_ja_safe  # adjust if your path differs


def load_lmmd_ja_dict(dict_csv: Path, *, token_col: str = "GPT_JA") -> tuple[set[str], set[str]]:
    lmmd = pd.read_csv(dict_csv)
    if token_col not in lmmd.columns:
        raise ValueError(f"LMMD dict missing token_col={token_col}. Columns={list(lmmd.columns)}")
    if "Positive" not in lmmd.columns or "Negative" not in lmmd.columns:
        raise ValueError("LMMD dict missing Positive/Negative columns")

    pos_set = set(lmmd.loc[lmmd["Positive"] > 0, token_col].dropna().astype(str))
    neg_set = set(lmmd.loc[lmmd["Negative"] > 0, token_col].dropna().astype(str))
    return pos_set, neg_set


def score_tokens(tokens: list[str], pos_set: set[str], neg_set: set[str]) -> Dict[str, Any]:
    n = len(tokens)
    if n == 0:
        return dict(
            status="empty_text",
            token_count=0,
            pos_count=0,
            neg_count=0,
            pos_rate=0.0,
            neg_rate=0.0,
            lmmd_net=0.0,
        )

    pos_count = sum(1 for t in tokens if t in pos_set)
    neg_count = sum(1 for t in tokens if t in neg_set)
    pos_rate = pos_count / n
    neg_rate = neg_count / n
    return dict(
        status="ok",
        token_count=n,
        pos_count=pos_count,
        neg_count=neg_count,
        pos_rate=pos_rate,
        neg_rate=neg_rate,
        lmmd_net=pos_rate - neg_rate,
    )


def write_qc_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["written_at"] = datetime.now().isoformat(timespec="seconds")
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def run_lmmd_score(
    *,
    index_csv: Path | str,
    mdna_dir: Path | str,
    lmmd_dict_csv: Path | str,
    out_csv: Path | str,
    qc_json: Optional[Path | str] = None,
    token_col: str = "GPT_JA",
) -> pd.DataFrame:
    """
    Build LMMD scores for the filings listed in index_csv.

    index_csv must include: filename, edinet_code, symbol, filing_date_parsed (or filing_date)
    mdna file expected at: mdna_dir/<edinet_code>/<filename>.mdna.txt
      e.g. .../E00990/S100NQ14_1.mdna.txt
    """
    index_csv = Path(index_csv)
    mdna_dir = Path(mdna_dir)
    lmmd_dict_csv = Path(lmmd_dict_csv)
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if qc_json is None:
        qc_json = out_csv.parent / f"{out_csv.name}.qc.json"
    qc_json = Path(qc_json)

    df = pd.read_csv(index_csv, dtype={"edinet_code": "string", "symbol": "string", "filename": "string"})

    required = {"filename", "edinet_code", "symbol"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Index missing required columns {missing}. Columns={list(df.columns)}")

    # normalize date column like legacy
    if "filing_date" not in df.columns and "filing_date_parsed" in df.columns:
        df = df.rename(columns={"filing_date_parsed": "filing_date"})
    if "filing_date" not in df.columns:
        raise ValueError(f"Index missing filing_date/filing_date_parsed: {index_csv}")

    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df = df.dropna(subset=["filename", "edinet_code", "symbol", "filing_date"])

    pos_set, neg_set = load_lmmd_ja_dict(lmmd_dict_csv, token_col=token_col)

    rows = []
    status_counts: Dict[str, int] = {}

    for r in df.itertuples(index=False):
        edinet_code = str(r.edinet_code)
        filename = str(r.filename)
        stem = filename[:-5] if filename.endswith(".xbrl") else filename
        mdna_path = mdna_dir / edinet_code / f"{stem}.mdna.txt"

        base = {
            "filename": filename,
            "edinet_code": edinet_code,
            "symbol": str(r.symbol),
            "filing_date_parsed": r.filing_date.strftime("%Y-%m-%d"),  # keep legacy column name
        }

        if not mdna_path.exists():
            out = dict(
                status="missing_mdna_txt",
                token_count=0,
                pos_count=0,
                neg_count=0,
                pos_rate=0.0,
                neg_rate=0.0,
                lmmd_net=0.0,
            )
        else:
            text = mdna_path.read_text(encoding="utf-8", errors="ignore")
            tokens = tokenize_ja_safe(
                text,
                split_mode=tokenizer.Tokenizer.SplitMode.C,
                max_bytes=48000,
                normalize=True,
            )
            out = score_tokens(tokens, pos_set, neg_set)

        status_counts[out["status"]] = status_counts.get(out["status"], 0) + 1
        rows.append({**base, **out})

    out_df = pd.DataFrame(rows)

    out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    qc = {
        "inputs": {
            "index_csv": str(index_csv),
            "mdna_dir": str(mdna_dir),
            "lmmd_dict_csv": str(lmmd_dict_csv),
            "token_col": token_col,
        },
        "rows": {
            "index_rows": int(len(df)),
            "output_rows": int(len(out_df)),
        },
        "status_counts": status_counts,
        "outputs": {
            "out_csv": str(out_csv),
            "qc_json": str(qc_json),
        },
    }
    write_qc_json(qc_json, qc)

    return out_df
