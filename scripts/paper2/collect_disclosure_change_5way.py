#!/usr/bin/env python3
"""Collect isolated five-label Stage 6E responses; emit two-group text for Stage 6F.

READS only a prepared request directory, its matching retrieval JSONL, and
Batch-style output JSONL files. Creates a new output directory; never overwrites.
Failed/incomplete pairs are quarantined, not partially aggregated.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

LABELS = {"unchanged", "revised", "new_candidate", "non_substantive", "uncertain"}
GROUP = {"unchanged": "unchanged", "revised": "changed", "new_candidate": "changed",
         "non_substantive": None, "uncertain": None}


def output_text(body: dict) -> str:
    if isinstance(body.get("output_text"), str):
        return body["output_text"]
    return "\n".join(str(c["text"]) for item in body.get("output", []) or []
                     for c in item.get("content", []) or []
                     if isinstance(c, dict) and isinstance(c.get("text"), str))


def parse(body: dict, expected: set[int], n_prior: int) -> dict[int, dict]:
    raw = output_text(body).strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    obj = json.loads(raw)
    if not isinstance(obj, dict) or not isinstance(obj.get("classifications"), list):
        raise ValueError("Missing classifications array")
    answers = {}
    for entry in obj["classifications"]:
        if not isinstance(entry, dict) or type(entry.get("current_index")) is not int:
            raise ValueError("Non-integer current_index")
        i = entry["current_index"]
        if i in answers:
            raise ValueError(f"Duplicate C{i}")
        label = str(entry.get("label", "")).strip().lower()
        confidence = str(entry.get("confidence", "")).strip().lower()
        if label not in LABELS or confidence not in {"high", "mid", "low"}:
            raise ValueError(f"C{i}: invalid label or confidence: {label}, {confidence}")
        p = entry.get("prior_index_used")
        if p is not None and (type(p) is not int or not (0 <= p < n_prior)):
            raise ValueError(f"C{i}: invalid prior_index_used {p}")
        reason = entry.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError(f"C{i}: missing reason")
        answers[i] = {"label": label, "confidence": confidence,
                      "prior_index_used": p, "reason": reason.strip()}
    if set(answers) != expected:
        raise ValueError(f"Coverage mismatch: missing={sorted(expected-set(answers))[:10]}, "
                         f"extra={sorted(set(answers)-expected)[:10]}")
    return answers


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_requests(directory: Path) -> tuple[dict, dict]:
    manifest_path = directory / "batch_build_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = {}
    for part in manifest["parts"]:
        path = directory / Path(part["file"]).name
        with path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                req = json.loads(line)
                cid = req["custom_id"]
                if not cid.startswith("dc5::"):
                    raise ValueError(f"Unexpected request id: {cid}")
                doc = cid.split("dc5::", 1)[1]
                if doc in expected:
                    raise ValueError(f"Duplicate request: {doc}")
                if hashlib.sha256(req["body"]["instructions"].encode("utf-8")).hexdigest() != manifest["prompt_sha256"]:
                    raise ValueError(f"Prompt hash mismatch for {doc}")
                expected[doc] = cid
    if len(expected) != manifest["total_requests"]:
        raise ValueError("Manifest request count mismatch")
    return manifest, expected


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--request-dir", type=Path, required=True)
    ap.add_argument("--batch-output-dir", type=Path, required=True)
    ap.add_argument("--retrieval-jsonl", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    args = ap.parse_args()
    for p in (args.request_dir, args.batch_output_dir):
        if not p.is_dir():
            ap.error(f"Directory not found: {p}")
    if args.output_dir.exists():
        ap.error(f"Refusing to reuse output directory: {args.output_dir}")
    manifest, expected = load_requests(args.request_dir)
    if file_sha256(args.retrieval_jsonl) != manifest["retrieval_jsonl_sha256"]:
        ap.error("Retrieval file SHA-256 does not match the request manifest")
    responses = {}
    failures = []
    for p in sorted(args.batch_output_dir.glob("*_output.jsonl")):
        with p.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                record = json.loads(line)
                cid = record.get("custom_id", "")
                if not cid.startswith("dc5::"):
                    continue  # Ignore unrelated production batch responses.
                doc = cid.split("dc5::", 1)[1]
                if doc not in expected:
                    raise ValueError(f"Unexpected response doc: {doc}")
                if doc in responses:
                    raise ValueError(f"Duplicate response: {doc}")
                responses[doc] = record
    if not responses:
        ap.error("No matching dc5::* responses found in *_output.jsonl files")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    sentence_fields = ["prev_docID", "curr_docID", "current_sentence_index", "current_sentence",
                       "label", "regression_group", "confidence", "prior_index_used", "reason",
                       "top1_prior_index", "top2_prior_index", "top3_prior_index"]
    document_fields = ["prev_docID", "curr_docID", "currentSentenceCount", "unchangedSentenceCount",
                       "revisedSentenceCount", "newCandidateSentenceCount", "nonSubstantiveSentenceCount",
                       "uncertainSentenceCount", "changedSentenceCount", "substantiveSentenceCount",
                       "unchangedChars", "changedChars", "unchangedShareChars", "changedShareChars",
                       "unchangedText", "changedText", "revisedText", "newCandidateText"]
    total = Counter()
    processed = set()
    with (args.output_dir / "disclosure_change_5way_sentences.csv").open("x", newline="", encoding="utf-8") as sf, \
         (args.output_dir / "disclosure_change_2group_documents.csv").open("x", newline="", encoding="utf-8") as df, \
         (args.output_dir / "classified_pairs_5way.jsonl").open("x", encoding="utf-8") as cf:
        sw = csv.DictWriter(sf, fieldnames=sentence_fields); sw.writeheader()
        dw = csv.DictWriter(df, fieldnames=document_fields); dw.writeheader()
        with args.retrieval_jsonl.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                pair = json.loads(line)
                doc = str(pair["curr_docID"])
                if doc not in expected:
                    continue
                if doc in processed:
                    raise ValueError(f"Duplicate retrieval pair: {doc}")
                processed.add(doc)
                record = responses.get(doc)
                if record is None:
                    failures.append({"curr_docID": doc, "error": "Missing response"})
                    continue
                response = record.get("response") or {}
                if response.get("status_code") != 200:
                    failures.append({"curr_docID": doc, "error": f"Non-200 response: {response.get('status_code')}"})
                    continue
                items = sorted(pair["sentences"], key=lambda x: int(x["current_sentence_index"]))
                indices = {int(x["current_sentence_index"]) for x in items}
                try:
                    answer = parse(response.get("body") or {}, indices, int(pair["prior_sentence_count"]))
                except Exception as exc:
                    failures.append({"curr_docID": doc, "error": f"{type(exc).__name__}: {exc}"})
                    continue
                texts = {label: [] for label in LABELS}
                classified = []
                for item in items:
                    i = int(item["current_sentence_index"])
                    a = answer[i]
                    s = str(item["current_sentence"])
                    texts[a["label"]].append(s)
                    cands = item.get("reranked_candidates", [])[:3]
                    row = {"prev_docID": pair["prev_docID"], "curr_docID": doc,
                           "current_sentence_index": i, "current_sentence": s,
                           **a, "regression_group": GROUP[a["label"]] or ""}
                    for rank in range(3):
                        row[f"top{rank+1}_prior_index"] = cands[rank].get("prior_index") if rank < len(cands) else ""
                    sw.writerow(row)
                    classified.append({"current_index": i, **a})
                    total[a["label"]] += 1
                u = texts["unchanged"]
                r = texts["revised"]
                n = texts["new_candidate"]
                # Pool sentence-level texts IN ORIGINAL ORDER, not concatenated category means.
                c = [str(item["current_sentence"]) for item in items
                     if answer[int(item["current_sentence_index"])]["label"] in {"revised", "new_candidate"}]
                utext, ctext = "\n".join(u), "\n".join(c)
                uc, cc = len(utext), len(ctext)
                denominator = uc + cc
                dw.writerow({
                    "prev_docID": pair["prev_docID"], "curr_docID": doc,
                    "currentSentenceCount": len(items),
                    "unchangedSentenceCount": len(u), "revisedSentenceCount": len(r),
                    "newCandidateSentenceCount": len(n),
                    "nonSubstantiveSentenceCount": len(texts["non_substantive"]),
                    "uncertainSentenceCount": len(texts["uncertain"]),
                    "changedSentenceCount": len(c), "substantiveSentenceCount": len(u)+len(c),
                    "unchangedChars": uc, "changedChars": cc,
                    "unchangedShareChars": uc / denominator if denominator else "",
                    "changedShareChars": cc / denominator if denominator else "",
                    "unchangedText": utext, "changedText": ctext,
                    "revisedText": "\n".join(r), "newCandidateText": "\n".join(n),
                })
                cf.write(json.dumps({"prev_docID": pair["prev_docID"], "curr_docID": doc,
                                     "classifications": classified}, ensure_ascii=False) + "\n")
                total["pairs"] += 1
    for doc in expected.keys() - processed:
        failures.append({"curr_docID": doc, "error": "Requested doc not present in retrieval JSONL"})
    with (args.output_dir / "classification_failures.csv").open("x", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["curr_docID", "error"]); w.writeheader(); w.writerows(failures)
    summary = {
        "purpose": "five_labels_two_sentiment_groups",
        "prompt_sha256": manifest["prompt_sha256"],
        "requests": len(expected), "responses": len(responses),
        "pairs_valid": total["pairs"], "sentences_valid": sum(total[l] for l in LABELS),
        "label_counts": {label: total[label] for label in sorted(LABELS)},
        "failed_pairs": len(failures), "complete": not failures and total["pairs"] == len(expected),
        "source_request_manifest": str(args.request_dir / "batch_build_manifest.json"),
        "new_outputs_only": True,
    }
    with (args.output_dir / "classification_summary.json").open("x", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(json.dumps(summary, indent=2))
    if not summary["complete"]:
        print("INCOMPLETE: Do not use for Stage 6F or regressions; repair failures first.", file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
