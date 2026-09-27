#!/usr/bin/env python3
"""Convert chABSA JSON annotations to Paper 2 sentence-level binary labels.

For each source sentence, the output records two independent targets:

    positiveLabel = 1 if any opinion is positive, else 0
    negativeLabel = 1 if any opinion is negative, else 0

This mirrors the construction described by Nakatsuka & Suimon (2024) for
fine-tuning Japanese Financial BERT with chABSA. Neutral annotations are not a
third sentence class: a sentence can be positive, negative, both, or neither.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--chabsa-root", required=True, type=Path)
    p.add_argument("--output-csv", required=True, type=Path)
    p.add_argument(
        "--pattern", default="*_ann.json",
        help="Recursive filename pattern. Falls back to all JSON files if no match is found.",
    )
    return p.parse_args()


def find_annotation_files(root: Path, pattern: str) -> list[Path]:
    files = sorted(p for p in root.rglob(pattern) if p.is_file())
    if not files:
        files = sorted(p for p in root.rglob("*.json") if p.is_file())
    return files


def convert(root: Path, pattern: str) -> tuple[pd.DataFrame, dict]:
    files = find_annotation_files(root, pattern)
    if not files:
        raise FileNotFoundError(f"No chABSA JSON annotation files found under {root}")

    rows: list[dict] = []
    polarity_counter: Counter[str] = Counter()
    empty_sentences = 0
    for path in files:
        obj = json.loads(path.read_text(encoding="utf-8"))
        header = obj.get("header", {}) or {}
        sentences = obj.get("sentences")
        if not isinstance(sentences, list):
            raise ValueError(f"Expected 'sentences' list in {path}")
        document_id = str(header.get("document_id") or header.get("edi_id") or path.stem)
        edinet_code = str(header.get("edi_id") or header.get("document_id") or "")
        company_name = str(header.get("document_name") or "")
        category33 = str(header.get("category33") or "")
        category17 = str(header.get("category17") or "")

        for s in sentences:
            text = str(s.get("sentence") or "").strip()
            if not text:
                empty_sentences += 1
                continue
            opinions = s.get("opinions") or []
            if not isinstance(opinions, list):
                raise ValueError(f"Expected opinions list in {path}, sentence={s.get('sentence_id')}")
            polarities = [str(o.get("polarity") or "").strip().lower() for o in opinions]
            for pol in polarities:
                if pol:
                    polarity_counter[pol] += 1
            positive_count = sum(pol == "positive" for pol in polarities)
            negative_count = sum(pol == "negative" for pol in polarities)
            neutral_count = sum(pol == "neutral" for pol in polarities)
            rows.append(
                {
                    "sourceFile": path.name,
                    "documentId": document_id,
                    "edinetCode": edinet_code,
                    "companyName": company_name,
                    "category33": category33,
                    "category17": category17,
                    "sentenceId": s.get("sentence_id"),
                    "text": text,
                    "opinionCount": len(opinions),
                    "positiveOpinionCount": positive_count,
                    "negativeOpinionCount": negative_count,
                    "neutralOpinionCount": neutral_count,
                    "positiveLabel": int(positive_count > 0),
                    "negativeLabel": int(negative_count > 0),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("chABSA conversion produced zero sentence rows")
    if df[["documentId", "sentenceId"]].duplicated().any():
        raise ValueError("Duplicate (documentId, sentenceId) keys in prepared chABSA data")

    joint = (
        df["positiveLabel"].astype(str)
        + df["negativeLabel"].astype(str)
    ).map({"00": "neither", "10": "positive_only", "01": "negative_only", "11": "both"})
    df["jointLabel"] = joint
    summary = {
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "sourceRoot": str(root),
        "annotationFileCount": len(files),
        "sentenceCount": int(len(df)),
        "documentCount": int(df["documentId"].nunique()),
        "emptySentencesSkipped": int(empty_sentences),
        "positiveSentenceCount": int(df["positiveLabel"].sum()),
        "negativeSentenceCount": int(df["negativeLabel"].sum()),
        "positiveSentencePct": float(100 * df["positiveLabel"].mean()),
        "negativeSentencePct": float(100 * df["negativeLabel"].mean()),
        "jointLabelCounts": {str(k): int(v) for k, v in df["jointLabel"].value_counts().items()},
        "opinionPolarityCounts": dict(polarity_counter),
    }
    return df, summary


def main() -> int:
    args = parse_args()
    root = args.chabsa_root.expanduser().resolve()
    output = args.output_csv.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(root)
    output.parent.mkdir(parents=True, exist_ok=True)
    df, summary = convert(root, args.pattern)
    df.to_csv(output, index=False, encoding="utf-8")
    meta = output.with_suffix(".metadata.json")
    meta.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
