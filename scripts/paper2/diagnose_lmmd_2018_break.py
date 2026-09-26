#!/usr/bin/env python3
"""Standalone diagnostic for the FY2017 -> FY2018 LMMD sentiment break.

Outputs:
  summary.json
  annual_document_summary.csv
  within_firm_2017_2018.csv
  leave_one_out_2017_2018.csv
  term_frequency_2017_2018.csv
  top_positive_term_changes.csv
  top_negative_term_changes.csv

This script is diagnostic only. It does not modify canonical pipeline outputs.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


DOC_KEY = ["edinetCode", "docID"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--sentiment-csv", required=True)
    p.add_argument("--filings-csv", required=True)
    p.add_argument("--lmmd-csv", required=True)
    p.add_argument("--token-root", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--translation-col", default="GPT_JA")
    p.add_argument("--year-a", type=int, default=2017)
    p.add_argument("--year-b", type=int, default=2018)
    p.add_argument("--top-n", type=int, default=50)
    return p.parse_args()


def load_lmmd_sets(path: Path, token_col: str) -> tuple[set[str], set[str]]:
    d = pd.read_csv(path, low_memory=False)
    required = {token_col, "Positive", "Negative"}
    missing = required - set(d.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {sorted(missing)}")
    translated = d[token_col].notna() & d[token_col].astype(str).str.strip().ne("")
    pos = set(d.loc[translated & (d["Positive"] > 0), token_col].astype(str).str.strip())
    neg = set(d.loc[translated & (d["Negative"] > 0), token_col].astype(str).str.strip())
    return pos, neg


def scan_terms(
    docs: pd.DataFrame,
    token_root: Path,
    pos_set: set[str],
    neg_set: set[str],
) -> tuple[Counter, Counter, int]:
    pos_counts: Counter[str] = Counter()
    neg_counts: Counter[str] = Counter()
    total_tokens = 0

    for r in docs.itertuples(index=False):
        path = token_root / str(r.edinetCode) / f"{r.docID}.tokens.txt"
        if not path.exists():
            raise FileNotFoundError(path)
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                token = line.rstrip("\r\n")
                if not token:
                    continue
                total_tokens += 1
                if token in pos_set:
                    pos_counts[token] += 1
                if token in neg_set:
                    neg_counts[token] += 1
    return pos_counts, neg_counts, total_tokens


def safe_rate(count: int, total: int) -> float:
    return 1_000_000.0 * count / total if total else np.nan


def count_target_terms_by_document(
    docs: pd.DataFrame,
    token_root: Path,
    targets: set[str],
) -> pd.DataFrame:
    """Count selected diagnostic terms in each document."""
    rows = []
    for r in docs.itertuples(index=False):
        path = token_root / str(r.edinetCode) / f"{r.docID}.tokens.txt"
        if not path.exists():
            raise FileNotFoundError(path)
        counts = Counter()
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                token = line.rstrip("\r\n")
                if token in targets:
                    counts[token] += 1
        row = {"edinetCode": str(r.edinetCode), "docID": str(r.docID)}
        for term in targets:
            row[f"count_{term}"] = int(counts.get(term, 0))
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()

    sentiment_csv = Path(args.sentiment_csv).expanduser().resolve()
    filings_csv = Path(args.filings_csv).expanduser().resolve()
    lmmd_csv = Path(args.lmmd_csv).expanduser().resolve()
    token_root = Path(args.token_root).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    y0, y1 = args.year_a, args.year_b
    if y0 == y1:
        raise ValueError("year-a and year-b must differ")

    s = pd.read_csv(
        sentiment_csv,
        dtype={"edinetCode": "string", "docID": "string"},
        low_memory=False,
    )
    required_s = {
        *DOC_KEY, "tokenCount", "lmmdPositiveCount", "lmmdNegativeCount",
        "lmmdPositiveRate", "lmmdNegativeRate", "lmmdNet",
    }
    missing = required_s - set(s.columns)
    if missing:
        raise ValueError(f"{sentiment_csv} missing columns: {sorted(missing)}")
    if s.duplicated(DOC_KEY).any():
        raise ValueError("Sentiment file contains duplicate document keys")

    f = pd.read_csv(
        filings_csv,
        dtype={"edinetCode": "string", "docID": "string"},
        low_memory=False,
    )
    required_f = {*DOC_KEY, "periodEnd"}
    missing = required_f - set(f.columns)
    if missing:
        raise ValueError(f"{filings_csv} missing columns: {sorted(missing)}")

    # A document can only have one periodEnd for this diagnostic.
    f = f[DOC_KEY + ["periodEnd"]].drop_duplicates()
    conflicting = f.groupby(DOC_KEY, dropna=False)["periodEnd"].nunique(dropna=False)
    if (conflicting > 1).any():
        raise ValueError(
            f"Filing metadata has {(conflicting > 1).sum():,} document key(s) "
            "with conflicting periodEnd values"
        )
    f = f.drop_duplicates(DOC_KEY)

    df = s.merge(f, on=DOC_KEY, how="left", validate="one_to_one")
    if df["periodEnd"].isna().any():
        raise ValueError(
            f"{int(df['periodEnd'].isna().sum()):,} sentiment documents lack periodEnd"
        )
    df["periodEnd"] = pd.to_datetime(df["periodEnd"], errors="coerce")
    if df["periodEnd"].isna().any():
        raise ValueError("Could not parse one or more periodEnd values")
    df["fiscalYear"] = df["periodEnd"].dt.year.astype(int)

    focus = df.loc[df["fiscalYear"].isin([y0, y1])].copy()
    if focus.empty or set(focus["fiscalYear"].unique()) != {y0, y1}:
        raise ValueError(f"Need documents in both FY{y0} and FY{y1}")

    # 1) Document-level annual summaries.
    annual = (
        focus.groupby("fiscalYear", as_index=False)
        .agg(
            n=("lmmdNet", "size"),
            tokenCount=("tokenCount", "sum"),
            lmmdMean=("lmmdNet", "mean"),
            lmmdMedian=("lmmdNet", "median"),
            lmmdStd=("lmmdNet", "std"),
            positiveRateMean=("lmmdPositiveRate", "mean"),
            positiveRateMedian=("lmmdPositiveRate", "median"),
            negativeRateMean=("lmmdNegativeRate", "mean"),
            negativeRateMedian=("lmmdNegativeRate", "median"),
        )
        .sort_values("fiscalYear")
    )
    annual.to_csv(out_dir / "annual_document_summary.csv", index=False)

    # 2) Within-firm comparison. Require exactly one document per firm/year.
    counts = (
        focus.groupby(["edinetCode", "fiscalYear"])
        .size()
        .rename("n")
        .reset_index()
    )
    eligible_codes = counts.groupby("edinetCode").filter(
        lambda x: len(x) == 2 and (x["n"] == 1).all()
    )["edinetCode"].unique()

    wf = focus.loc[focus["edinetCode"].isin(eligible_codes)].copy()
    keep = [
        "edinetCode", "docID", "fiscalYear", "tokenCount",
        "lmmdPositiveRate", "lmmdNegativeRate", "lmmdNet",
    ]
    a = wf.loc[wf["fiscalYear"].eq(y0), keep].drop(columns="fiscalYear").copy()
    b = wf.loc[wf["fiscalYear"].eq(y1), keep].drop(columns="fiscalYear").copy()
    a = a.rename(columns={c: f"{c}_{y0}" for c in a.columns if c != "edinetCode"})
    b = b.rename(columns={c: f"{c}_{y1}" for c in b.columns if c != "edinetCode"})
    paired = a.merge(b, on="edinetCode", how="inner", validate="one_to_one")

    paired["deltaLmmdNet"] = paired[f"lmmdNet_{y1}"] - paired[f"lmmdNet_{y0}"]
    paired["deltaPositiveRate"] = (
        paired[f"lmmdPositiveRate_{y1}"] - paired[f"lmmdPositiveRate_{y0}"]
    )
    paired["deltaNegativeRate"] = (
        paired[f"lmmdNegativeRate_{y1}"] - paired[f"lmmdNegativeRate_{y0}"]
    )
    paired["logTokenCountChange"] = np.log(
        paired[f"tokenCount_{y1}"] / paired[f"tokenCount_{y0}"]
    )
    paired.to_csv(out_dir / "within_firm_2017_2018.csv", index=False)

    # 3) Leave-one-term-out counterfactuals for the two dominant break terms.
    # 実績 is positive in the translated LMMD; 減少 is negative.
    target_terms = {"実績", "減少"}
    target_counts = count_target_terms_by_document(
        focus[DOC_KEY], token_root, target_terms
    )
    focus_loo = focus.merge(target_counts, on=DOC_KEY, how="left", validate="one_to_one")

    # Removing a positive hit lowers net sentiment by count/tokenCount.
    focus_loo["lmmdNet_excl_実績"] = (
        focus_loo["lmmdNet"] - focus_loo["count_実績"] / focus_loo["tokenCount"]
    )
    # Removing a negative hit raises net sentiment by count/tokenCount.
    focus_loo["lmmdNet_excl_減少"] = (
        focus_loo["lmmdNet"] + focus_loo["count_減少"] / focus_loo["tokenCount"]
    )
    focus_loo["lmmdNet_excl_both"] = (
        focus_loo["lmmdNet"]
        - focus_loo["count_実績"] / focus_loo["tokenCount"]
        + focus_loo["count_減少"] / focus_loo["tokenCount"]
    )

    loo_specs = {
        "baseline": "lmmdNet",
        "exclude実績": "lmmdNet_excl_実績",
        "exclude減少": "lmmdNet_excl_減少",
        "excludeBoth": "lmmdNet_excl_both",
    }
    loo_rows = []
    for spec, col in loo_specs.items():
        tmp = focus_loo.loc[
            focus_loo["edinetCode"].isin(eligible_codes),
            ["edinetCode", "fiscalYear", col],
        ]
        va = tmp.loc[tmp["fiscalYear"].eq(y0), ["edinetCode", col]].rename(
            columns={col: "scoreA"}
        )
        vb = tmp.loc[tmp["fiscalYear"].eq(y1), ["edinetCode", col]].rename(
            columns={col: "scoreB"}
        )
        vp = va.merge(vb, on="edinetCode", how="inner", validate="one_to_one")
        vp["delta"] = vp["scoreB"] - vp["scoreA"]
        loo_rows.append({
            "specification": spec,
            "pairedFirmCount": int(len(vp)),
            f"meanScore{y0}": float(vp["scoreA"].mean()),
            f"meanScore{y1}": float(vp["scoreB"].mean()),
            "meanDelta": float(vp["delta"].mean()),
            "medianDelta": float(vp["delta"].median()),
            "shareMorePositivePct": float(100 * (vp["delta"] > 0).mean()),
            "shareLessPositivePct": float(100 * (vp["delta"] < 0).mean()),
            "shareUnchangedPct": float(100 * (vp["delta"] == 0).mean()),
        })

    loo = pd.DataFrame(loo_rows)
    loo.to_csv(out_dir / "leave_one_out_2017_2018.csv", index=False)

    # 4) Term-frequency decomposition, normalized per million corpus tokens.
    pos_set, neg_set = load_lmmd_sets(lmmd_csv, args.translation_col)

    docs_a = focus.loc[focus["fiscalYear"].eq(y0), DOC_KEY]
    docs_b = focus.loc[focus["fiscalYear"].eq(y1), DOC_KEY]
    pos_a, neg_a, tok_a = scan_terms(docs_a, token_root, pos_set, neg_set)
    pos_b, neg_b, tok_b = scan_terms(docs_b, token_root, pos_set, neg_set)

    terms = sorted((pos_set | neg_set))
    term_rows = []
    for term in terms:
        for polarity, ca, cb in (
            ("positive", pos_a.get(term, 0), pos_b.get(term, 0)),
            ("negative", neg_a.get(term, 0), neg_b.get(term, 0)),
        ):
            # Preserve the Paper 1 semantics: an overlap term appears once in each polarity.
            if polarity == "positive" and term not in pos_set:
                continue
            if polarity == "negative" and term not in neg_set:
                continue
            ra = safe_rate(ca, tok_a)
            rb = safe_rate(cb, tok_b)
            term_rows.append({
                "term": term,
                "polarity": polarity,
                f"count{y0}": ca,
                f"ratePerMillion{y0}": ra,
                f"count{y1}": cb,
                f"ratePerMillion{y1}": rb,
                "rateChangePerMillion": rb - ra,
            })

    terms_df = pd.DataFrame(term_rows)
    terms_df["absRateChangePerMillion"] = terms_df["rateChangePerMillion"].abs()
    terms_df = terms_df.sort_values(
        ["absRateChangePerMillion", "polarity", "term"],
        ascending=[False, True, True],
    )
    terms_df.to_csv(out_dir / "term_frequency_2017_2018.csv", index=False)

    (
        terms_df.loc[terms_df["polarity"].eq("positive")]
        .sort_values("absRateChangePerMillion", ascending=False)
        .head(args.top_n)
        .to_csv(out_dir / "top_positive_term_changes.csv", index=False)
    )
    (
        terms_df.loc[terms_df["polarity"].eq("negative")]
        .sort_values("absRateChangePerMillion", ascending=False)
        .head(args.top_n)
        .to_csv(out_dir / "top_negative_term_changes.csv", index=False)
    )

    summary = {
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "diagnostic": "lmmd_fiscal_year_break",
        "yearA": y0,
        "yearB": y1,
        "inputs": {
            "sentimentCsv": str(sentiment_csv),
            "filingsCsv": str(filings_csv),
            "lmmdCsv": str(lmmd_csv),
            "tokenRoot": str(token_root),
        },
        "annual": {
            str(int(r.fiscalYear)): {
                "n": int(r.n),
                "tokenCount": int(r.tokenCount),
                "lmmdMean": float(r.lmmdMean),
                "lmmdMedian": float(r.lmmdMedian),
                "lmmdStd": float(r.lmmdStd),
                "positiveRateMean": float(r.positiveRateMean),
                "negativeRateMean": float(r.negativeRateMean),
            }
            for r in annual.itertuples(index=False)
        },
        "withinFirm": {
            "pairedFirmCount": int(len(paired)),
            "meanDeltaLmmdNet": float(paired["deltaLmmdNet"].mean()),
            "medianDeltaLmmdNet": float(paired["deltaLmmdNet"].median()),
            "meanDeltaPositiveRate": float(paired["deltaPositiveRate"].mean()),
            "medianDeltaPositiveRate": float(paired["deltaPositiveRate"].median()),
            "meanDeltaNegativeRate": float(paired["deltaNegativeRate"].mean()),
            "medianDeltaNegativeRate": float(paired["deltaNegativeRate"].median()),
            "shareMorePositivePct": float(100 * (paired["deltaLmmdNet"] > 0).mean()),
            "shareLessPositivePct": float(100 * (paired["deltaLmmdNet"] < 0).mean()),
            "shareUnchangedPct": float(100 * (paired["deltaLmmdNet"] == 0).mean()),
            "meanLogTokenCountChange": float(paired["logTokenCountChange"].mean()),
            "medianLogTokenCountChange": float(paired["logTokenCountChange"].median()),
            "corrDeltaLmmdVsLogTokenCountChange": (
                float(paired[["deltaLmmdNet", "logTokenCountChange"]].corr().iloc[0, 1])
                if len(paired) > 1 else None
            ),
        },
        "leaveOneOut": {
            str(r.specification): {
                "pairedFirmCount": int(r.pairedFirmCount),
                f"meanScore{y0}": float(getattr(r, f"meanScore{y0}")),
                f"meanScore{y1}": float(getattr(r, f"meanScore{y1}")),
                "meanDelta": float(r.meanDelta),
                "medianDelta": float(r.medianDelta),
                "shareMorePositivePct": float(r.shareMorePositivePct),
                "shareLessPositivePct": float(r.shareLessPositivePct),
                "shareUnchangedPct": float(r.shareUnchangedPct),
            }
            for r in loo.itertuples(index=False)
        },
        "termDecomposition": {
            f"totalTokens{y0}": int(tok_a),
            f"totalTokens{y1}": int(tok_b),
            f"positiveHits{y0}": int(sum(pos_a.values())),
            f"positiveHits{y1}": int(sum(pos_b.values())),
            f"negativeHits{y0}": int(sum(neg_a.values())),
            f"negativeHits{y1}": int(sum(neg_b.values())),
            "dictionaryPositiveTerms": int(len(pos_set)),
            "dictionaryNegativeTerms": int(len(neg_set)),
            "positiveNegativeOverlapCount": int(len(pos_set & neg_set)),
        },
        "outputs": {
            "annualDocumentSummaryCsv": str(out_dir / "annual_document_summary.csv"),
            "withinFirmCsv": str(out_dir / "within_firm_2017_2018.csv"),
            "leaveOneOutCsv": str(out_dir / "leave_one_out_2017_2018.csv"),
            "termFrequencyCsv": str(out_dir / "term_frequency_2017_2018.csv"),
            "topPositiveTermChangesCsv": str(out_dir / "top_positive_term_changes.csv"),
            "topNegativeTermChangesCsv": str(out_dir / "top_negative_term_changes.csv"),
        },
    }

    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
