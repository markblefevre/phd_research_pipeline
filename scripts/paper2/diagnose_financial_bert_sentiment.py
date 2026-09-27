#!/usr/bin/env python3
"""Annual, LMMD-comparison, and 2017->2018 diagnostics for Financial BERT."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DOC_KEY = ["edinetCode", "docID"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bert-csv", required=True, type=Path)
    p.add_argument("--filings-csv", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--lmmd-csv", type=Path, default=None)
    p.add_argument("--year-a", type=int, default=2017)
    p.add_argument("--year-b", type=int, default=2018)
    return p.parse_args()


def corr_pair(df: pd.DataFrame, a: str, b: str, method: str) -> float | None:
    clean = df[[a, b]].dropna()
    if len(clean) < 3 or clean[a].nunique() < 2 or clean[b].nunique() < 2:
        return None
    return float(clean.corr(method=method).iloc[0, 1])


def main() -> int:
    args = parse_args()
    out_dir = args.out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    bert = pd.read_csv(
        args.bert_csv.expanduser().resolve(),
        dtype={"edinetCode": "string", "docID": "string"}, low_memory=False,
    )
    filings = pd.read_csv(
        args.filings_csv.expanduser().resolve(),
        dtype={"edinetCode": "string", "docID": "string"}, low_memory=False,
    )
    required = {
        *DOC_KEY, "bertNet", "bertProbabilityNet", "bertSentenceCount",
        "bertPositiveSentenceRate", "bertNegativeSentenceRate",
        "bertMeanPositiveProbability", "bertMeanNegativeProbability",
        "bertUnknownTokenRate",
    }
    missing = required - set(bert.columns)
    if missing:
        raise ValueError(f"BERT CSV missing columns: {sorted(missing)}")
    if bert.duplicated(DOC_KEY).any():
        raise ValueError("BERT CSV contains duplicate document keys")
    if filings.duplicated("docID").any():
        raise ValueError("filings.csv contains duplicate docIDs")

    meta_cols = ["docID", "periodEnd"]
    if "filerName" in filings.columns:
        meta_cols.append("filerName")
    df = bert.merge(filings[meta_cols], on="docID", how="left", validate="one_to_one")
    df["periodEnd"] = pd.to_datetime(df["periodEnd"], errors="coerce")
    if df["periodEnd"].isna().any():
        raise ValueError(f"Invalid/missing periodEnd for {int(df['periodEnd'].isna().sum()):,} rows")
    df["fiscalYear"] = df["periodEnd"].dt.year.astype(int)

    annual = df.groupby("fiscalYear").agg(
        n=("bertNet", "size"),
        bertMean=("bertNet", "mean"),
        bertMedian=("bertNet", "median"),
        bertStd=("bertNet", "std"),
        positiveSentenceRateMean=("bertPositiveSentenceRate", "mean"),
        negativeSentenceRateMean=("bertNegativeSentenceRate", "mean"),
        probabilityNetMean=("bertProbabilityNet", "mean"),
        unknownTokenRateMean=("bertUnknownTokenRate", "mean"),
        sentenceCountMedian=("bertSentenceCount", "median"),
    ).reset_index()
    annual.to_csv(out_dir / "annual_financial_bert_summary.csv", index=False)

    y0, y1 = args.year_a, args.year_b
    focus = df.loc[df["fiscalYear"].isin([y0, y1])].copy()
    counts = focus.groupby(["edinetCode", "fiscalYear"]).size().unstack(fill_value=0)
    eligible = counts.index[(counts.get(y0, 0) == 1) & (counts.get(y1, 0) == 1)]
    fields = [
        "bertNet", "bertProbabilityNet", "bertPositiveSentenceRate",
        "bertNegativeSentenceRate", "bertMeanPositiveProbability",
        "bertMeanNegativeProbability", "bertSentenceCount", "bertUnknownTokenRate",
    ]
    a = focus.loc[
        (focus["fiscalYear"] == y0) & focus["edinetCode"].isin(eligible),
        ["edinetCode", *fields],
    ]
    b = focus.loc[
        (focus["fiscalYear"] == y1) & focus["edinetCode"].isin(eligible),
        ["edinetCode", *fields],
    ]
    paired = a.merge(b, on="edinetCode", suffixes=(f"_{y0}", f"_{y1}"), validate="one_to_one")
    for col in fields:
        paired[f"delta_{col}"] = paired[f"{col}_{y1}"] - paired[f"{col}_{y0}"]
    paired.to_csv(out_dir / f"within_firm_{y0}_{y1}.csv", index=False)

    summary = {
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "diagnostic": "financial_bert_sentiment",
        "yearA": y0,
        "yearB": y1,
        "documentCount": int(len(df)),
        "annualCsv": str(out_dir / "annual_financial_bert_summary.csv"),
        "withinFirm": {
            "pairedFirmCount": int(len(paired)),
            "meanDeltaBertNet": float(paired["delta_bertNet"].mean()),
            "medianDeltaBertNet": float(paired["delta_bertNet"].median()),
            "shareMorePositivePct": float(100 * (paired["delta_bertNet"] > 0).mean()),
            "shareLessPositivePct": float(100 * (paired["delta_bertNet"] < 0).mean()),
            "meanDeltaPositiveSentenceRate": float(paired["delta_bertPositiveSentenceRate"].mean()),
            "meanDeltaNegativeSentenceRate": float(paired["delta_bertNegativeSentenceRate"].mean()),
            "meanDeltaProbabilityNet": float(paired["delta_bertProbabilityNet"].mean()),
            "medianSentenceCountChange": float(paired["delta_bertSentenceCount"].median()),
        },
    }

    if args.lmmd_csv is not None:
        lmmd = pd.read_csv(
            args.lmmd_csv.expanduser().resolve(),
            dtype={"edinetCode": "string", "docID": "string"}, low_memory=False,
        )
        if "lmmdNet" not in lmmd.columns:
            raise ValueError("LMMD CSV missing lmmdNet")
        if lmmd.duplicated(DOC_KEY).any():
            raise ValueError("LMMD CSV contains duplicate document keys")
        both = df[DOC_KEY + ["fiscalYear", "bertNet", "bertProbabilityNet"]].merge(
            lmmd[DOC_KEY + ["lmmdNet"]], on=DOC_KEY, how="inner", validate="one_to_one"
        )
        both.to_csv(out_dir / "bert_lmmd_document_comparison.csv", index=False)
        summary["lmmdComparison"] = {
            "matchedDocuments": int(len(both)),
            "pearsonHardNet": corr_pair(both, "bertNet", "lmmdNet", "pearson"),
            "spearmanHardNet": corr_pair(both, "bertNet", "lmmdNet", "spearman"),
            "pearsonProbabilityNet": corr_pair(both, "bertProbabilityNet", "lmmdNet", "pearson"),
            "spearmanProbabilityNet": corr_pair(both, "bertProbabilityNet", "lmmdNet", "spearman"),
            "annual": [],
        }
        for year, g in both.groupby("fiscalYear"):
            summary["lmmdComparison"]["annual"].append({
                "fiscalYear": int(year),
                "n": int(len(g)),
                "bertMean": float(g["bertNet"].mean()),
                "lmmdMean": float(g["lmmdNet"].mean()),
                "pearson": corr_pair(g, "bertNet", "lmmdNet", "pearson"),
            })

    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
