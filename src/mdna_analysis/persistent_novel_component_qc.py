"""Stage 6F-A: read-only reconciliation of frozen Stage 6E component outputs."""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

EXPECTED_PAIRS = 33046
EXPECTED_SENTENCES = 3463989


def _int(row, key):
    return int(row[key])


def run_component_qc(*, documents_csv: Path, sentences_csv: Path,
                     stage6e_summary_json: Path, output_dir: Path) -> dict:
    """Stream both CSVs; verify exact per-document sentence order and reconstructed texts."""
    documents_csv, sentences_csv = Path(documents_csv), Path(sentences_csv)
    stage6e_summary_json, output_dir = Path(stage6e_summary_json), Path(output_dir)
    summary6e = json.loads(stage6e_summary_json.read_text(encoding="utf-8"))
    if summary6e.get("failure_rows") != 0:
        raise ValueError("Stage 6E summary is not failure-free")
    if summary6e.get("pairs_assembled") != EXPECTED_PAIRS or summary6e.get("sentences_assembled") != EXPECTED_SENTENCES:
        raise ValueError(f"Unexpected Stage 6E universe: {summary6e}")

    output_dir.mkdir(parents=True, exist_ok=True)
    failures_path = output_dir / "component_qc_failures.csv"
    distribution_path = output_dir / "component_size_distribution.csv"
    summary_path = output_dir / "component_qc_summary.json"
    counts = Counter()
    shares = []
    seen = set()
    errors = 0
    examples = []

    with documents_csv.open(newline="", encoding="utf-8") as df, \
         sentences_csv.open(newline="", encoding="utf-8") as sf, \
         failures_path.open("w", newline="", encoding="utf-8") as ff, \
         distribution_path.open("w", newline="", encoding="utf-8") as distf:
        dr, sr = csv.DictReader(df), csv.DictReader(sf)
        fw = csv.writer(ff)
        fw.writerow(["curr_docID", "check", "detail"])
        dw = csv.writer(distf)
        dw.writerow(["curr_docID", "currentSentenceCount", "persistentSentenceCount",
                     "novelSentenceCount", "persistentChars", "novelChars",
                     "persistentShareChars", "novelShareChars"])

        def fail(docid, check, detail):
            nonlocal errors
            errors += 1
            fw.writerow([docid, check, detail])
            if len(examples) < 10:
                examples.append({"curr_docID": docid, "check": check, "detail": detail})

        sent = next(sr, None)
        for doc in dr:
            docid = doc["curr_docID"]
            counts["documents"] += 1
            if docid in seen:
                fail(docid, "unique_curr_docID", "duplicate document")
            seen.add(docid)
            persistent, novel = [], []
            expected_idx = 0
            while sent is not None and sent["curr_docID"] == docid:
                if sent["prev_docID"] != doc["prev_docID"]:
                    fail(docid, "prev_docID", "sentence/document prior document differs")
                try:
                    idx = _int(sent, "current_sentence_index")
                    if idx != expected_idx:
                        fail(docid, "sentence_index", f"expected {expected_idx}, got {idx}")
                except (ValueError, KeyError) as exc:
                    fail(docid, "sentence_index", str(exc))
                label = sent["label"]
                if label == "persistent":
                    persistent.append(sent["current_sentence"])
                elif label == "novel":
                    novel.append(sent["current_sentence"])
                else:
                    fail(docid, "label", f"invalid: {label}")
                expected_idx += 1
                counts["sentences"] += 1
                sent = next(sr, None)

            ptext, ntext = "\n".join(persistent), "\n".join(novel)
            for field, actual in (("currentSentenceCount", expected_idx),
                                  ("persistentSentenceCount", len(persistent)),
                                  ("novelSentenceCount", len(novel)),
                                  ("persistentChars", len(ptext)),
                                  ("novelChars", len(ntext))):
                try:
                    if _int(doc, field) != actual:
                        fail(docid, field, f"reported={doc[field]}, actual={actual}")
                except (ValueError, KeyError) as exc:
                    fail(docid, field, str(exc))
            if doc["persistentText"] != ptext:
                fail(docid, "persistentText", "does not match ordered persistent sentences")
            if doc["novelText"] != ntext:
                fail(docid, "novelText", "does not match ordered novel sentences")
            total = len(ptext) + len(ntext)
            pshare = len(ptext) / total if total else 0.0
            nshare = len(ntext) / total if total else 0.0
            for field, actual in (("persistentShareChars", pshare), ("novelShareChars", nshare)):
                try:
                    if abs(float(doc[field]) - actual) > 1e-9:
                        fail(docid, field, f"reported={doc[field]}, actual={actual}")
                except (ValueError, KeyError) as exc:
                    fail(docid, field, str(exc))
            if not persistent:
                counts["empty_persistent"] += 1
            if not novel:
                counts["empty_novel"] += 1
            if len(ptext) < 100:
                counts["persistent_under_100_chars"] += 1
            if len(ntext) < 100:
                counts["novel_under_100_chars"] += 1
            if not total:
                counts["empty_both"] += 1
            shares.append(nshare)
            dw.writerow([docid, expected_idx, len(persistent), len(novel), len(ptext), len(ntext), pshare, nshare])

        if sent is not None:
            fail(sent["curr_docID"], "unmatched_sentences", "sentence rows remain after last document")
        if counts["documents"] != EXPECTED_PAIRS:
            fail("*", "document_total", f"{counts['documents']} != {EXPECTED_PAIRS}")
        if counts["sentences"] != EXPECTED_SENTENCES:
            fail("*", "sentence_total", f"{counts['sentences']} != {EXPECTED_SENTENCES}")

    sorted_shares = sorted(shares)
    def quantile(q):
        if not sorted_shares:
            return None
        if q <= 0:
            return sorted_shares[0]
        if q >= 1:
            return sorted_shares[-1]
        pos = (len(sorted_shares) - 1) * q
        lo = int(pos)
        hi = min(lo + 1, len(sorted_shares) - 1)
        return sorted_shares[lo] * (hi - pos) + sorted_shares[hi] * (pos - lo)

    result = {
        "stage": "persistent_novel_component_qc", "createdAt": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if errors == 0 else "failed", "failures": errors,
        "documents": counts["documents"], "sentences": counts["sentences"],
        "emptyPersistent": counts["empty_persistent"], "emptyNovel": counts["empty_novel"],
        "emptyBoth": counts["empty_both"],
        "persistentUnder100Chars": counts["persistent_under_100_chars"],
        "novelUnder100Chars": counts["novel_under_100_chars"],
        "novelShareCharsQuantiles": {str(q): quantile(q) for q in (0, .01, .05, .25, .5, .75, .95, .99, 1)},
        "inputs": {"documentsCsv": str(documents_csv), "sentencesCsv": str(sentences_csv),
                   "stage6eSummaryJson": str(stage6e_summary_json)},
        "failureExamples": examples,
    }
    summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if errors:
        raise ValueError(f"Stage 6F-A QC failed with {errors} errors; see {failures_path}")
    return result
