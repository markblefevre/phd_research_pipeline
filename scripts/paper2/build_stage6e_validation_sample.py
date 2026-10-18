#!/usr/bin/env python3
"""Reproducible, read-only sampling of Stage 6E sentence classifications.

Creates a 100-sentence population-random sample and 100 diagnostic sentences
(25 in each of four mutually exclusive diagnostic strata). Reconstructs the
prior-year text using the EXACT splitter from the production Stage 6E batch
builder, checks each sampled current sentence against the source document,
and attaches top-3 retrieved prior sentences.

No API calls, embedding computation, classification, or modification of source
files. Output is written only to a new validation directory.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

FIELDS = [
    "prev_docID", "curr_docID", "current_sentence_index", "current_sentence",
    "label", "confidence", "reason",
    "top1_prior_index", "top1_cosine", "top1_rerank_score",
    "top2_prior_index", "top2_cosine", "top2_rerank_score",
    "top3_prior_index", "top3_cosine", "top3_rerank_score",
]
STRATA = [
    "high_similarity_novel", "low_similarity_persistent",
    "lower_confidence_novel", "lower_confidence_persistent",
]


def load_production_splitter(script_path: Path):
    if not script_path.is_file():
        raise FileNotFoundError(f"Production splitter not found: {script_path}")
    spec = importlib.util.spec_from_file_location("stage6e_production_builder", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.split_japanese_sentences, module.normalize_sentence


def retain_smallest(old: pd.DataFrame | None, new: pd.DataFrame, n: int):
    if new.empty:
        return old
    candidates = new.nsmallest(min(n, len(new)), "_random_key")
    if old is not None:
        candidates = pd.concat([old, candidates], ignore_index=True)
    return candidates.nsmallest(min(n, len(candidates)), "_random_key").copy()


def sample_rows(path: Path, seed: int, chunk_size: int):
    rng = np.random.default_rng(seed)
    pools = {"representative": None, **{x: None for x in STRATA}}
    total = 0
    stratum_counts = {x: 0 for x in STRATA}
    for chunk in pd.read_csv(path, usecols=FIELDS, chunksize=chunk_size, low_memory=False):
        n = len(chunk)
        chunk["_row_number"] = np.arange(total, total + n, dtype=np.int64)
        chunk["_random_key"] = rng.random(n)
        total += n
        chunk["label"] = chunk["label"].astype("string").str.lower().str.strip()
        chunk["confidence"] = chunk["confidence"].astype("string").str.lower().str.strip()
        chunk["top1_cosine"] = pd.to_numeric(chunk["top1_cosine"], errors="coerce")
        novel = chunk["label"].eq("novel").fillna(False)
        persistent = chunk["label"].eq("persistent").fillna(False)
        high = chunk["confidence"].eq("high").fillna(False)
        masks = {
            "high_similarity_novel": novel & high & (chunk["top1_cosine"] >= 0.97),
            "low_similarity_persistent": persistent & high & (chunk["top1_cosine"] < 0.90),
            "lower_confidence_novel": novel & ~high,
            "lower_confidence_persistent": persistent & ~high,
        }
        pools["representative"] = retain_smallest(pools["representative"], chunk, 100)
        for name, mask in masks.items():
            stratum_counts[name] += int(mask.sum())
            pools[name] = retain_smallest(pools[name], chunk.loc[mask], 160)
    if total < 200:
        raise ValueError(f"At least 200 rows required; found {total}")
    selected = []
    used = set()
    for name, count in [("representative", 100)] + [(x, 25) for x in STRATA]:
        pool = pools[name]
        if pool is None:
            raise ValueError(f"No observations in {name}")
        available = pool.loc[~pool["_row_number"].isin(used)].head(count).copy()
        if len(available) != count:
            raise ValueError(f"Only {len(available)} disjoint observations for {name}; expected {count}")
        available.insert(0, "sample_group", name)
        selected.append(available)
        used.update(available["_row_number"].tolist())
    result = pd.concat(selected, ignore_index=True)
    assert len(result) == 200 and result["_row_number"].nunique() == 200
    return result, total, stratum_counts


def build_doc_index(root: Path, wanted: set[str]):
    if not root.is_dir():
        raise FileNotFoundError(f"MD&A root not found: {root}")
    result = {}
    duplicates = set()
    for path in root.rglob("*.txt"):
        doc_id = path.stem
        if doc_id in wanted:
            if doc_id in result:
                duplicates.add(doc_id)
            else:
                result[doc_id] = path
    if duplicates:
        raise ValueError(f"Ambiguous MD&A text files for: {sorted(duplicates)[:10]}")
    missing = wanted - set(result)
    if missing:
        raise FileNotFoundError(f"Missing {len(missing)} MD&A text files, e.g. {sorted(missing)[:10]}")
    return result


def read_retrieval_metadata(path: Path, wanted_current: set[str]):
    meta = {}
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            obj = json.loads(line)
            curr = str(obj.get("curr_docID", ""))
            if curr in wanted_current:
                if curr in meta:
                    raise ValueError(f"Duplicate retrieval record for {curr}")
                meta[curr] = obj
    missing = wanted_current - set(meta)
    if missing:
        raise ValueError(f"Missing {len(missing)} retrieval records: {sorted(missing)[:10]}")
    return meta


def attach_prior(sample: pd.DataFrame, mdna_root: Path, production_script: Path,
                 retrieval_jsonl: Path | None):
    splitter, normalize = load_production_splitter(production_script)
    wanted = set(sample["prev_docID"].astype(str)) | set(sample["curr_docID"].astype(str))
    doc_index = build_doc_index(mdna_root, wanted)
    metadata = (read_retrieval_metadata(retrieval_jsonl,
                set(sample["curr_docID"].astype(str))) if retrieval_jsonl else {})
    cache = {}
    def sentences(doc_id, min_chars):
        key = (doc_id, min_chars)
        if key not in cache:
            text = doc_index[doc_id].read_text(encoding="utf-8-sig", errors="replace")
            cache[key] = splitter(text, min_chars=min_chars)
        return cache[key]

    out = []
    for row in sample.to_dict("records"):
        curr = str(row["curr_docID"])
        prev = str(row["prev_docID"])
        rec = metadata.get(curr)
        if rec and str(rec["prev_docID"]) != prev:
            raise ValueError(f"Pair mismatch: {prev} -> {curr}")
        min_chars = int(rec.get("min_sentence_chars", 12)) if rec else 12
        current = sentences(curr, min_chars)
        prior = sentences(prev, min_chars)
        idx = int(row["current_sentence_index"])
        if not (0 <= idx < len(current)):
            raise ValueError(f"Current index out of range: {curr} C{idx} / {len(current)}")
        if normalize(current[idx]) != normalize(str(row["current_sentence"])):
            raise ValueError(
                f"Current sentence mismatch: {curr} C{idx}. "
                "Do not trust prior indices until splitter/source is reconciled."
            )
        if rec:
            expected = int(rec.get("prior_sentence_count", len(prior)))
            if len(prior) != expected:
                raise ValueError(f"Prior count mismatch: {curr}: {len(prior)} != {expected}")
            # Check that the retrieval record itself agrees with the sampled current text.
            items = rec.get("sentences", [])
            matches = [x for x in items if int(x["current_sentence_index"]) == idx]
            if len(matches) != 1 or normalize(matches[0]["current_sentence"]) != normalize(str(row["current_sentence"])):
                raise ValueError(f"Retrieval record mismatch: {curr} C{idx}")
        row["prior_sentence_count_reconstructed"] = len(prior)
        row["current_sentence_count_reconstructed"] = len(current)
        row["source_min_sentence_chars"] = min_chars
        row["retrieval_count_verified"] = bool(rec)
        for k in (1, 2, 3):
            raw = row.get(f"top{k}_prior_index")
            if pd.isna(raw):
                row[f"top{k}_prior_sentence"] = ""
                continue
            pi = int(raw)
            if not (0 <= pi < len(prior)):
                raise ValueError(f"Prior index out of range: {curr} C{idx} P{pi} / {len(prior)}")
            row[f"top{k}_prior_sentence"] = prior[pi]
        out.append(row)
    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sentences", type=Path, default=Path(
        "data/interim/paper2/alignment/persistent_novel/results_final_repaired/persistent_novel_sentences.csv"))
    ap.add_argument("--mdna-root", type=Path, default=Path("data/interim/paper2/mdna"))
    ap.add_argument("--production-script", type=Path, default=Path("scripts/paper2/build_stage6e_pair_batches.py"))
    ap.add_argument("--retrieval-jsonl", type=Path, default=None,
                    help="Optional frozen Stage 6E retrieval JSONL for additional count checks")
    ap.add_argument("--output-dir", type=Path, default=Path(
        "data/interim/paper2/alignment/persistent_novel/validation"))
    ap.add_argument("--seed", type=int, default=20261014)
    ap.add_argument("--chunk-size", type=int, default=150_000)
    ap.add_argument("--skip-prior", action="store_true", help="Sampling only; omit prior text reconstruction")
    args = ap.parse_args()

    sample, population_n, stratum_counts = sample_rows(args.sentences, args.seed, args.chunk_size)
    if not args.skip_prior:
        sample = attach_prior(sample, args.mdna_root, args.production_script,
                              args.retrieval_jsonl)
    sample["human_3way_label"] = ""  # unchanged / revised / entirely_new
    sample["human_binary_label"] = ""  # persistent / novel
    sample["economically_material_change"] = ""  # yes / no / unclear
    sample["human_notes"] = ""
    sample = sample.drop(columns=["_random_key"])
    sample = sample.rename(columns={"_row_number": "source_csv_row_number_zero_based"})
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_csv = args.output_dir / f"stage6e_validation_sample_200_seed{args.seed}.csv"
    out_json = args.output_dir / f"stage6e_validation_sample_200_seed{args.seed}_manifest.json"
    if out_csv.exists() or out_json.exists():
        raise FileExistsError(f"Output exists. Refusing to overwrite: {out_csv} / {out_json}")
    manifest = {
        "source_csv": str(args.sentences), "population_n": population_n,
        "seed": args.seed, "sample_n": len(sample),
        "representative_n": 100, "targeted_n_per_group": 25,
        "targeted_population_counts": stratum_counts,
        "sampling_note": "Representative is a simple random sample; targeted cases are deliberately enriched and must not be pooled to estimate population accuracy.",
        "source_reconstruction": "production Stage 6E splitter; exact current-sentence equality required" if not args.skip_prior else "not performed",
        "retrieval_metadata_verified": bool(args.retrieval_jsonl and not args.skip_prior),
        "annotation_note": "Independent human labels should be assigned blind to the model label where practical; do not interpret cosine similarity as ground truth.",
    }
    sample.to_csv(out_csv, index=False, encoding="utf-8-sig")
    out_json.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Sample saved: {out_csv}")
    print(f"Manifest saved: {out_json}")
    print(f"Population: {population_n:,}; sample: {len(sample)}")
    print(sample["sample_group"].value_counts().to_string())
    print(f"Previous-report text attached: {not args.skip_prior}")
    if not args.retrieval_jsonl and not args.skip_prior:
        print("NOTE: Sentence equality verified, but retrieval prior-count check not performed; provide --retrieval-jsonl for stronger validation.")

if __name__ == "__main__":
    main()
