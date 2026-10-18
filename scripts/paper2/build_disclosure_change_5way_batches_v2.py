#!/usr/bin/env python3
"""Build isolated five-way Stage 6E Batch requests; makes NO API calls.

Uses the ORIGINAL Stage 6E splitter and pair-input builder, loaded by path.
Creates a NEW directory atomically; never writes original Stage 6E paths.
Defaults to 3 pairs; --all requires an explicit decision.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def original_builder(path: Path):
    spec = importlib.util.spec_from_file_location("stage6e_frozen_builder", path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot import original builder {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--retrieval-jsonl", type=Path, required=True)
    ap.add_argument("--mdna-root", type=Path, default=Path("~/paper2_stage4/mdna"))
    ap.add_argument("--original-builder", type=Path,
                    default=Path("scripts/paper2/build_stage6e_pair_batches.py"))
    ap.add_argument("--prompt", type=Path,
                    default=Path("configs/paper2/prompts/disclosure_change_pair_classifier_5way_v1.md"))
    ap.add_argument("--output-dir", type=Path,
                    default=Path("data/interim/paper2/alignment/disclosure_change_5way_v1/batch_requests_smoke"))
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--all", action="store_true", help="Build requests for all pairs (still no API calls)")
    mode.add_argument("--max-pairs", type=int, default=None, help="Default 3 for a smoke test")
    ap.add_argument("--model", default="gpt-6-luna")
    ap.add_argument("--max-batch-bytes", type=int, default=180_000_000)
    ap.add_argument("--max-requests-per-batch", type=int, default=45_000)
    args = ap.parse_args()
    args.mdna_root = args.mdna_root.expanduser()
    args.retrieval_jsonl = args.retrieval_jsonl.expanduser()
    if not args.all and (args.max_pairs or 3) < 1:
        ap.error("--max-pairs must be positive")
    if args.max_batch_bytes < 1_000 or args.max_batch_bytes > 190_000_000:
        ap.error("--max-batch-bytes must be between 1000 and 190000000")
    if args.max_requests_per_batch < 1 or args.max_requests_per_batch > 49_999:
        ap.error("--max-requests-per-batch must be between 1 and 49999")
    for path in (args.retrieval_jsonl, args.prompt, args.original_builder):
        if not path.is_file():
            ap.error(f"Missing required input: {path}")
    if not args.mdna_root.is_dir():
        ap.error(f"Missing MD&A directory: {args.mdna_root}")
    # The exact output directory must not exist, even if empty.
    if args.output_dir.exists():
        ap.error(f"Refusing to reuse output directory: {args.output_dir}")
    original = original_builder(args.original_builder)
    # The frozen production builder expects a docID -> Path index, not a root Path.
    # Build it once for this run; do not modify the frozen builder.
    mdna_index = original.build_mdna_index(args.mdna_root)
    instructions = args.prompt.read_text(encoding="utf-8-sig")
    prompt_hash = hashlib.sha256(instructions.encode("utf-8")).hexdigest()
    args.output_dir.mkdir(parents=True, exist_ok=False)
    parts = []
    n_pairs = n_bytes = part_n = part_bytes = 0
    seen_ids = set()
    fh = None

    def start_part():
        nonlocal fh, part_n, part_bytes
        if fh is not None:
            fh.close()
        p = args.output_dir / f"disclosure_change_5way_{len(parts)+1:03d}.jsonl"
        fh = p.open("xb")
        parts.append({"file": str(p), "requests": 0, "bytes": 0})
        part_n = part_bytes = 0

    try:
        with args.retrieval_jsonl.open(encoding="utf-8") as source:
            for line in source:
                if not line.strip():
                    continue
                pair = json.loads(line)
                if str(pair.get("status", "")).lower() in {"error", "failed", "failure"}:
                    continue
                doc = str(pair["curr_docID"])
                if doc in seen_ids:
                    raise ValueError(f"Duplicate current document: {doc}")
                seen_ids.add(doc)
                prior_text = original.read_mdna(mdna_index, str(pair["prev_docID"]))
                prior = original.split_japanese_sentences(
                    prior_text, int(pair.get("min_sentence_chars", 12)))
                expected_prior = int(pair.get("prior_sentence_count", len(prior)))
                if len(prior) != expected_prior:
                    raise ValueError(f"{doc}: reconstructed prior sentences {len(prior)} != {expected_prior}")
                sentences = pair["sentences"]
                indices = sorted(int(s["current_sentence_index"]) for s in sentences)
                if indices != list(range(len(indices))):
                    raise ValueError(f"{doc}: non-contiguous or duplicate current sentence indices")
                if len(indices) != int(pair.get("current_sentence_count", len(indices))):
                    raise ValueError(f"{doc}: current sentence count mismatch")
                request = {
                    "custom_id": f"dc5::{doc}", "method": "POST", "url": "/v1/responses",
                    "body": {"model": args.model, "instructions": instructions,
                             "input": original.build_pair_input(pair, prior), "store": False}
                }
                payload = (json.dumps(request, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
                if len(payload) > args.max_batch_bytes:
                    raise ValueError(f"{doc}: request alone exceeds batch byte cap")
                if fh is None or (part_n and (part_bytes + len(payload) > args.max_batch_bytes
                                                or part_n >= args.max_requests_per_batch)):
                    start_part()
                fh.write(payload)
                part_n += 1
                part_bytes += len(payload)
                parts[-1]["requests"] = part_n
                parts[-1]["bytes"] = part_bytes
                n_pairs += 1
                n_bytes += len(payload)
                if n_pairs % 250 == 0:
                    print(f"Prepared {n_pairs:,} pairs", flush=True)
                if not args.all and n_pairs >= (args.max_pairs or 3):
                    break
    finally:
        if fh is not None:
            fh.close()
    manifest = {
        "purpose": "isolated_five_label_classification_two_group_regression",
        "model": args.model, "prompt": str(args.prompt), "prompt_sha256": prompt_hash,
        "original_builder": str(args.original_builder),
        "original_builder_sha256": sha256(args.original_builder),
        "retrieval_jsonl": str(args.retrieval_jsonl),
        "retrieval_jsonl_sha256": sha256(args.retrieval_jsonl),
        "mdna_root": str(args.mdna_root), "complete_prior_document_context": True,
        "labels": ["unchanged", "revised", "new_candidate", "non_substantive", "uncertain"],
        "regression_groups": {"unchanged": "unchanged", "revised": "changed",
                              "new_candidate": "changed", "non_substantive": None,
                              "uncertain": None},
        "total_requests": n_pairs, "total_bytes": n_bytes, "parts": parts,
        "makes_api_calls": False,
    }
    with (args.output_dir / "batch_build_manifest.json").open("x", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"Prepared {n_pairs:,} requests ({n_bytes / 1e6:.1f} MB) in {args.output_dir}")
    print("No API calls were made. Do not submit a full batch until the 3-pair smoke test is validated.")


if __name__ == "__main__":
    main()
