#!/usr/bin/env python3
"""
QC the GPT-translated Loughran-McDonald sentiment dictionary against the
Paper 2 Stage 4 Sudachi-C raw-token corpus.

This is a diagnostic only. It does not modify the dictionary or write
production sentiment scores.

Outputs:
    summary.json
    dictionary_sentiment_entries.csv
    translation_duplicates.csv
    polarity_conflicts.csv
    unmatched_dictionary_terms.csv
    matched_dictionary_terms.csv
    document_coverage.csv
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--lmmd-csv", required=True, type=Path)
    p.add_argument("--manifest-csv", required=True, type=Path)
    p.add_argument(
        "--token-root",
        required=True,
        type=Path,
        help="Physical root containing <edinetCode>/<docID>.tokens.txt",
    )
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--translation-col", default="GPT_JA")
    return p.parse_args()


def clean_translation(value) -> str | None:
    if pd.isna(value):
        return None
    s = str(value).strip()
    return s if s else None


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Dictionary QC
    # ------------------------------------------------------------------
    lm = pd.read_csv(args.lmmd_csv)

    required = {"Word", "Positive", "Negative", args.translation_col}
    missing = required - set(lm.columns)
    if missing:
        raise ValueError(f"LMMD missing required columns: {sorted(missing)}")

    lm["_ja"] = lm[args.translation_col].map(clean_translation)
    lm["_positive"] = pd.to_numeric(lm["Positive"], errors="coerce").fillna(0) > 0
    lm["_negative"] = pd.to_numeric(lm["Negative"], errors="coerce").fillna(0) > 0
    lm["_sentiment"] = lm["_positive"] | lm["_negative"]

    sent = lm.loc[lm["_sentiment"]].copy()
    sent["polarity"] = sent.apply(
        lambda r: (
            "both" if r["_positive"] and r["_negative"]
            else "positive" if r["_positive"]
            else "negative"
        ),
        axis=1,
    )

    sent_out = sent[
        ["Word", args.translation_col, "Positive", "Negative", "polarity"]
    ].copy()
    sent_out.to_csv(args.out_dir / "dictionary_sentiment_entries.csv", index=False)

    translated = sent.loc[sent["_ja"].notna()].copy()

    # Translation duplicates within/across source words.
    dup_counts = (
        translated.groupby("_ja", dropna=False)
        .agg(
            englishEntries=("Word", "size"),
            englishWords=("Word", lambda x: " | ".join(map(str, x))),
            positiveEntries=("_positive", "sum"),
            negativeEntries=("_negative", "sum"),
        )
        .reset_index()
        .rename(columns={"_ja": "GPT_JA"})
    )
    duplicates = dup_counts.loc[dup_counts["englishEntries"] > 1].copy()
    duplicates.sort_values(
        ["englishEntries", "GPT_JA"], ascending=[False, True]
    ).to_csv(args.out_dir / "translation_duplicates.csv", index=False)

    pos_set = set(translated.loc[translated["_positive"], "_ja"])
    neg_set = set(translated.loc[translated["_negative"], "_ja"])
    overlap = sorted(pos_set & neg_set)

    conflict_rows = []
    for term in overlap:
        rows = translated.loc[translated["_ja"] == term]
        conflict_rows.append(
            {
                "GPT_JA": term,
                "positiveEnglishWords": " | ".join(
                    rows.loc[rows["_positive"], "Word"].astype(str)
                ),
                "negativeEnglishWords": " | ".join(
                    rows.loc[rows["_negative"], "Word"].astype(str)
                ),
            }
        )
    pd.DataFrame(
        conflict_rows,
        columns=["GPT_JA", "positiveEnglishWords", "negativeEnglishWords"],
    ).to_csv(args.out_dir / "polarity_conflicts.csv", index=False)

    # ------------------------------------------------------------------
    # Corpus QC
    # ------------------------------------------------------------------
    manifest = pd.read_csv(args.manifest_csv)

    manifest_required = {"edinetCode", "docID"}
    missing = manifest_required - set(manifest.columns)
    if missing:
        raise ValueError(f"Manifest missing required columns: {sorted(missing)}")

    if "status" in manifest.columns:
        valid_statuses = {"success", "skipped_existing"}
        manifest = manifest.loc[
            manifest["status"].astype(str).str.lower().isin(valid_statuses)
        ].copy()

    if manifest.duplicated(["edinetCode", "docID"]).any():
        raise ValueError("Duplicate (edinetCode, docID) keys in token manifest")

    sentiment_terms = pos_set | neg_set
    corpus_term_counts: Counter[str] = Counter()
    doc_rows = []
    missing_token_files = []

    for row in manifest.itertuples(index=False):
        edinet = str(row.edinetCode)
        docid = str(row.docID)
        token_file = args.token_root / edinet / f"{docid}.tokens.txt"

        if not token_file.is_file():
            missing_token_files.append(
                {"edinetCode": edinet, "docID": docid, "path": str(token_file)}
            )
            continue

        pos_hits = 0
        neg_hits = 0
        either_hits = 0
        token_count = 0

        with token_file.open("r", encoding="utf-8") as fh:
            for line in fh:
                token = line.rstrip("\r\n")
                if not token:
                    continue
                token_count += 1

                is_pos = token in pos_set
                is_neg = token in neg_set

                if is_pos:
                    pos_hits += 1
                if is_neg:
                    neg_hits += 1
                if is_pos or is_neg:
                    either_hits += 1
                    corpus_term_counts[token] += 1

        doc_rows.append(
            {
                "edinetCode": edinet,
                "docID": docid,
                "tokenCountObserved": token_count,
                "positiveHits": pos_hits,
                "negativeHits": neg_hits,
                "sentimentHits": either_hits,
                "hasPositive": pos_hits > 0,
                "hasNegative": neg_hits > 0,
                "hasSentiment": either_hits > 0,
                "positiveRate": pos_hits / token_count if token_count else 0.0,
                "negativeRate": neg_hits / token_count if token_count else 0.0,
                "sentimentRate": either_hits / token_count if token_count else 0.0,
                "lmmdNet": (pos_hits - neg_hits) / token_count if token_count else 0.0,
            }
        )

    docs = pd.DataFrame(doc_rows)
    docs.to_csv(args.out_dir / "document_coverage.csv", index=False)
    pd.DataFrame(
        missing_token_files,
        columns=["edinetCode", "docID", "path"],
    ).to_csv(args.out_dir / "missing_token_files.csv", index=False)

    matched_terms = []
    unmatched_terms = []

    # Preserve dictionary polarity membership, but report corpus counts once per
    # unique Japanese translated term.
    for term in sorted(sentiment_terms):
        rec = {
            "GPT_JA": term,
            "positive": term in pos_set,
            "negative": term in neg_set,
            "corpusTokenCount": int(corpus_term_counts.get(term, 0)),
        }
        if rec["corpusTokenCount"] > 0:
            matched_terms.append(rec)
        else:
            unmatched_terms.append(rec)

    matched_df = pd.DataFrame(matched_terms)
    unmatched_df = pd.DataFrame(unmatched_terms)

    if not matched_df.empty:
        matched_df = matched_df.sort_values(
            ["corpusTokenCount", "GPT_JA"], ascending=[False, True]
        )
    matched_df.to_csv(args.out_dir / "matched_dictionary_terms.csv", index=False)
    unmatched_df.to_csv(args.out_dir / "unmatched_dictionary_terms.csv", index=False)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    missing_trans = sent["_ja"].isna()

    summary = {
        "dictionary": {
            "totalRows": int(len(lm)),
            "sentimentRows": int(len(sent)),
            "positiveRows": int(sent["_positive"].sum()),
            "negativeRows": int(sent["_negative"].sum()),
            "sourceRowsBothPositiveAndNegative": int(
                (sent["_positive"] & sent["_negative"]).sum()
            ),
            "missingTranslationsAmongSentimentRows": int(missing_trans.sum()),
            "translatedSentimentRows": int((~missing_trans).sum()),
            "uniqueTranslatedSentimentTerms": int(len(sentiment_terms)),
            "uniquePositiveTerms": int(len(pos_set)),
            "uniqueNegativeTerms": int(len(neg_set)),
            "positiveNegativeTranslationOverlapCount": int(len(overlap)),
            "positiveNegativeTranslationOverlap": overlap,
            "duplicateJapaneseTranslationCount": int(len(duplicates)),
        },
        "corpus": {
            "manifestRowsUsed": int(len(manifest)),
            "documentsRead": int(len(docs)),
            "missingTokenFiles": int(len(missing_token_files)),
            "totalTokensObserved": int(docs["tokenCountObserved"].sum()) if len(docs) else 0,
            "matchedUniqueSentimentTerms": int(len(matched_df)),
            "unmatchedUniqueSentimentTerms": int(len(unmatched_df)),
            "dictionaryTermCoveragePct": (
                100.0 * len(matched_df) / len(sentiment_terms)
                if sentiment_terms else None
            ),
            "documentsWithPositivePct": (
                100.0 * docs["hasPositive"].mean() if len(docs) else None
            ),
            "documentsWithNegativePct": (
                100.0 * docs["hasNegative"].mean() if len(docs) else None
            ),
            "documentsWithAnySentimentPct": (
                100.0 * docs["hasSentiment"].mean() if len(docs) else None
            ),
            "positiveHits": int(docs["positiveHits"].sum()) if len(docs) else 0,
            "negativeHits": int(docs["negativeHits"].sum()) if len(docs) else 0,
            "sentimentHits": int(docs["sentimentHits"].sum()) if len(docs) else 0,
            "medianSentimentHitsPerDocument": (
                float(docs["sentimentHits"].median()) if len(docs) else None
            ),
            "meanSentimentHitsPerDocument": (
                float(docs["sentimentHits"].mean()) if len(docs) else None
            ),
            "medianLmmdNet": float(docs["lmmdNet"].median()) if len(docs) else None,
            "meanLmmdNet": float(docs["lmmdNet"].mean()) if len(docs) else None,
        },
    }

    with (args.out_dir / "summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"\nQC outputs written to: {args.out_dir}")


if __name__ == "__main__":
    main()
