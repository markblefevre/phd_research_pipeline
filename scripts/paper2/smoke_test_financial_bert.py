#!/usr/bin/env python3
"""Small end-to-end smoke test for fine-tuned dual Financial BERT classifiers."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.mdna_analysis.financial_bert_sentiment import score_financial_bert_corpus


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--manifest-csv", required=True, type=Path)
    p.add_argument("--mdna-root", required=True, type=Path)
    p.add_argument("--positive-model", required=True)
    p.add_argument("--negative-model", required=True)
    p.add_argument("--output-dir", required=True, type=Path)
    p.add_argument("--documents", type=int, default=25)
    p.add_argument("--device", default="auto")
    p.add_argument("--batch-size", type=int, default=16)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = args.output_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out = score_financial_bert_corpus(
        manifest_csv=args.manifest_csv,
        mdna_root=args.mdna_root,
        output_csv=out_dir / "financial_bert_smoke.csv",
        metadata_json=out_dir / "financial_bert_smoke.metadata.json",
        positive_model_ref=args.positive_model,
        negative_model_ref=args.negative_model,
        tokenizer_model_ref=args.positive_model,
        max_documents=args.documents,
        inference_batch_size=args.batch_size,
        document_block_size=min(args.documents, 8),
        device=args.device,
        overwrite=True,
    )
    cols = [
        "edinetCode", "docID", "bertSentenceCount",
        "bertPositiveSentenceRate", "bertNegativeSentenceRate", "bertNet",
        "bertUnknownTokenRate",
    ]
    print(out[cols].to_string(index=False))
    print("\nSummary")
    print(out[["bertNet", "bertPositiveSentenceRate", "bertNegativeSentenceRate"]].describe())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
