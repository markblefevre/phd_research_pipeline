#!/usr/bin/env python3
"""Explicitly paid, resumable, synchronous 3-pair Stage 6E smoke run.

Requires --run. Reads the safe batch-builder request files; writes only to a
new, separate output directory. Does not submit a Batch job or alter production.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--request-dir", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--run", action="store_true", help="Authorize paid OpenAI API calls")
    args = ap.parse_args()
    manifest = json.loads((args.request_dir / "batch_build_manifest.json").read_text(encoding="utf-8"))
    if manifest["total_requests"] > 3:
        ap.error("Smoke runner refuses more than 3 pairs; use reviewed batch workflow for production")
    requests = []
    for part in manifest["parts"]:
        with (args.request_dir / Path(part["file"]).name).open(encoding="utf-8") as f:
            requests.extend(json.loads(line) for line in f if line.strip())
    if not args.run:
        print(f"PREVIEW: {len(requests)} pairs, model={manifest['model']}, prompt_sha256={manifest['prompt_sha256']}")
        print("No API calls. Add --run to authorize paid synchronous requests.")
        return
    if not os.environ.get("OPENAI_API_KEY"):
        ap.error("OPENAI_API_KEY is not set")
    try:
        from openai import OpenAI
    except ImportError as exc:
        ap.error(f"OpenAI Python SDK not installed: {exc}")
    if args.output_dir.exists():
        existing = json.loads((args.output_dir / "run_manifest.json").read_text(encoding="utf-8"))
        if existing != {"source_manifest": manifest, "mode": "paid_synchronous_smoke"}:
            ap.error("Existing output dir does not match the request manifest")
    else:
        args.output_dir.mkdir(parents=True, exist_ok=False)
        with (args.output_dir / "run_manifest.json").open("x", encoding="utf-8") as f:
            json.dump({"source_manifest": manifest, "mode": "paid_synchronous_smoke"}, f, indent=2)
    dest = args.output_dir / "smoke_output.jsonl"
    completed = set()
    if dest.exists():
        with dest.open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    obj = json.loads(line)
                    if obj["custom_id"] in completed:
                        raise ValueError("Duplicate checkpoint custom_id")
                    completed.add(obj["custom_id"])
    client = OpenAI()
    for idx, req in enumerate(requests, 1):
        cid = req["custom_id"]
        if cid in completed:
            print(f"[{idx}/{len(requests)}] Reused {cid}")
            continue
        print(f"[{idx}/{len(requests)}] Calling {cid} ...", flush=True)
        response = client.responses.create(**req["body"])
        item = {"custom_id": cid, "response": {"status_code": 200, "body": response.model_dump()}, "error": None}
        with dest.open("a", encoding="utf-8") as f:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            f.flush()
        print(f"  Stored {cid} (response status={getattr(response, 'status', 'unknown')})", flush=True)
    print(f"Smoke responses: {dest}")


if __name__ == "__main__":
    main()
