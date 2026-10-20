#!/usr/bin/env python3
"""Paper 2 Stage 8E: three-way five-way-classification component tone scoring.

Imports the frozen Stage 6D unit builder, scorer and aggregation; never modifies
Stage 6F-B or any existing pipeline artifacts. Reads document-level five-way CSV.

Usage from repository root:
  python scripts/paper2/run_stage8e_three_component_sentiment.py --inventory
  python scripts/paper2/run_stage8e_three_component_sentiment.py --sample-documents 100
  python scripts/paper2/run_stage8e_three_component_sentiment.py --sample-documents 100 --live

Dry run is the default. --live requires OPENAI_API_KEY and explicitly requests API calls.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import random
import sys
import uuid
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.mdna_analysis.llm_sentiment import (  # noqa: E402
    OpenAI, UnitScore, aggregate_document, build_narrative_units, score_units_openai,
)

VERSION = "stage8e_three_component_v1"
COMPONENTS = {"unchanged": "unchangedText", "revised": "revisedText", "new": "newCandidateText"}
REQUIRED = {"curr_docID", "prev_docID", *COMPONENTS.values()}
DEFAULT_CSV = ROOT / "data/interim/paper2/alignment/disclosure_change_5way_v1/collected_repaired_prompt_v2/disclosure_change_2group_documents.csv"
DEFAULT_PROMPT = ROOT / "configs/paper2/prompts/llm_sentiment_v2.md"
DEFAULT_OUTPUT = ROOT / "data/interim/paper2/sentiment/three_component_stage8e"
PRODUCTION_PROMPT_HASH = "326e4698aed9575f16e26ed33e097fc0c3d4f68be1e6c774672fcd013e4801ff"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def iter_rows(path: Path) -> Iterator[dict[str, str]]:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            break
        except OverflowError:
            limit //= 10
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = REQUIRED - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")
        for row in reader:
            if not row["curr_docID"] or not row["prev_docID"]:
                raise ValueError("Missing document ID")
            yield row


def selected_rows(path: Path, sample: int | None, maximum: int | None, seed: int) -> Iterator[dict[str, str]]:
    if sample is not None:
        rng = random.Random(seed)
        reservoir: list[tuple[int, dict[str, str]]] = []
        for idx, row in enumerate(iter_rows(path)):
            if idx < sample:
                reservoir.append((idx, row))
            else:
                slot = rng.randrange(idx + 1)
                if slot < sample:
                    reservoir[slot] = (idx, row)
        for _, row in sorted(reservoir):
            yield row
    else:
        for idx, row in enumerate(iter_rows(path)):
            if maximum is not None and idx >= maximum:
                break
            yield row


def inventory(path: Path) -> None:
    n = 0
    counts = Counter()
    chars = Counter()
    all_three = 0
    none = 0
    by_pattern = Counter()
    ids: set[str] = set()
    for row in iter_rows(path):
        doc_id = row["curr_docID"].strip()
        if doc_id in ids:
            raise ValueError(f"Duplicate curr_docID: {doc_id}")
        ids.add(doc_id)
        n += 1
        present = tuple(c for c, field in COMPONENTS.items() if (row[field] or "").strip())
        by_pattern["+".join(present) or "none"] += 1
        all_three += len(present) == 3
        none += len(present) == 0
        for c in present:
            counts[c] += 1
            chars[c] += len(row[COMPONENTS[c]])
    print(f"Documents: {n:,}; all three present: {all_three:,} ({all_three/n:.1%}); no component: {none:,}")
    for c in COMPONENTS:
        print(f"{c:10} present={counts[c]:>7,} ({counts[c]/n:.1%})  chars={chars[c]:>12,}")
    print("Presence patterns:")
    for pattern, number in by_pattern.most_common():
        print(f"  {pattern:30} {number:>7,}")


def atomic_new_json(path: Path, obj: dict[str, Any]) -> None:
    """Write a new JSON file without overwriting (compatible with SMB NAS mounts).

    Exclusive creation prevents overwriting existing audits. Unlike a hard-link
    publish, this is not atomic to readers; a failed write is cleaned up, and
    interrupted writes will fail JSON/resume validation rather than be trusted.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    content = (json.dumps(obj, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    with path.open("xb") as fh:  # O_EXCL: never overwrite an existing audit
        try:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        except BaseException:
            # The caller must never interpret a partially written file as valid.
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            raise


def validate_resume(path: Path, expected: dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    mismatches = [key for key, val in expected.items() if payload.get(key) != val]
    if mismatches:
        raise ValueError(f"Audit mismatch at {path}: {mismatches}")
    from src.mdna_analysis.llm_sentiment import NarrativeUnit
    units = [NarrativeUnit(id=int(u["id"]), text=u["text"], chars=int(u["chars"])) for u in payload["units"]]
    scores = [UnitScore.model_validate(u["score"]) for u in payload["units"]]
    aggregate = aggregate_document(units, scores)
    if aggregate != payload["aggregation"]:
        raise ValueError(f"Audit aggregation mismatch: {path}")
    return aggregate


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--documents-csv", type=Path, default=DEFAULT_CSV)
    p.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT)
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--inventory", action="store_true", help="Count component coverage without calling API")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--sample-documents", type=int)
    group.add_argument("--max-documents", type=int)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--live", action="store_true", help="Explicitly enable paid API calls; default is dry run")
    p.add_argument("--model", default="gpt-6-luna")
    p.add_argument("--reasoning-effort", default="none")
    p.add_argument("--target-unit-chars", type=int, default=1050)
    p.add_argument("--max-unit-chars", type=int, default=1400)
    p.add_argument("--max-concurrent-requests", type=int, default=1)
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--retry-base-seconds", type=float, default=2.0)
    args = p.parse_args()
    if args.inventory:
        inventory(args.documents_csv)
        return
    if args.sample_documents is not None and args.sample_documents <= 0:
        p.error("--sample-documents must be positive")
    if args.max_documents is not None and args.max_documents <= 0:
        p.error("--max-documents must be positive")
    if args.max_concurrent_requests <= 0:
        p.error("--max-concurrent-requests must be positive")
    prompt = args.prompt.read_text(encoding="utf-8").strip()
    prompt_hash = sha(prompt)
    if prompt_hash != PRODUCTION_PROMPT_HASH:
        raise ValueError(f"Prompt does not match production Stage 6F-B: {prompt_hash}")
    if args.live and (OpenAI is None or not os.environ.get("OPENAI_API_KEY")):
        raise RuntimeError("Live scoring requires OpenAI SDK and OPENAI_API_KEY")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid.uuid4().hex[:8]
    run_dir = args.output_dir / "runs" / run_id
    raw_dir = args.output_dir / "raw_components"
    log = logging.getLogger("stage8e")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import threading
    local = threading.local()

    def get_client() -> Any:
        client = getattr(local, "client", None)
        if client is None:
            client = OpenAI()
            local.client = client
        return client

    def process(row: dict[str, str]) -> dict[str, Any]:
        doc = row["curr_docID"].strip()
        prev = row["prev_docID"].strip()
        out: dict[str, Any] = {"curr_docID": doc, "prev_docID": prev, "model": args.model,
                               "promptSha256": prompt_hash}
        for component, field in COMPONENTS.items():
            value = row[field] or ""
            out[f"{component}Chars"] = len(value)
            out[f"{component}TextSha256"] = sha(value)
            out[f"{component}NewInputTokens"] = 0
            out[f"{component}NewOutputTokens"] = 0
            out[f"{component}Sentiment"] = None
            out[f"{component}LlmNumUnits"] = 0
            if not value.strip():
                out[f"{component}Status"] = "empty"
                continue
            audit_path = raw_dir / component / f"{doc}.json"
            expected = {"version": VERSION, "curr_docID": doc, "prev_docID": prev,
                        "component": component, "text_sha256": sha(value),
                        "prompt_sha256": prompt_hash, "model_requested": args.model,
                        "reasoning_effort": args.reasoning_effort,
                        "target_unit_chars": args.target_unit_chars,
                        "max_unit_chars": args.max_unit_chars}
            if audit_path.exists():
                agg = validate_resume(audit_path, expected)
                out.update({component + key[0].upper() + key[1:]: v for key, v in agg.items()})
                out[f"{component}Sentiment"] = agg["llmNet"]
                out[f"{component}Status"] = "resumed"
                continue
            units = build_narrative_units(value, target_chars=args.target_unit_chars,
                                          max_chars=args.max_unit_chars)
            out[f"{component}LlmNumUnits"] = len(units)
            if not units:
                out[f"{component}Status"] = "empty"
                continue
            if not args.live:
                out[f"{component}Status"] = "dry_run"
                continue
            try:
                scores, response = score_units_openai(
                    client=get_client(), units=units, prompt_text=prompt,
                    model=args.model, reasoning_effort=args.reasoning_effort,
                    max_retries=args.max_retries, retry_base_seconds=args.retry_base_seconds,
                    logger=log)
                agg = aggregate_document(units, scores)
                by_id = {s.id: s for s in scores}
                payload = {**expected, "prompt_file": str(args.prompt),
                           "created_at_utc": now(), "aggregation": agg,
                           "response": response,
                           "units": [{"id": u.id, "text": u.text, "chars": u.chars,
                                      "score": by_id[u.id].model_dump()} for u in units]}
                atomic_new_json(audit_path, payload)
                out.update({component + key[0].upper() + key[1:]: v for key, v in agg.items()})
                out[f"{component}Sentiment"] = agg["llmNet"]
                out[f"{component}Status"] = "scored"
                usage = response.get("usage", {}) or {}
                out[f"{component}NewInputTokens"] = int(usage.get("input_tokens") or 0)
                out[f"{component}NewOutputTokens"] = int(usage.get("output_tokens") or 0)
            except FileExistsError:
                raise
            except Exception as exc:
                log.exception("Scoring failed: doc=%s component=%s", doc, component)
                out[f"{component}Status"] = "failed"
                out[f"{component}Error"] = f"{type(exc).__name__}: {exc}"[:500]
        return out

    results: list[tuple[int, dict[str, Any]]] = []
    seen: set[str] = set()
    with ThreadPoolExecutor(max_workers=args.max_concurrent_requests) as pool:
        pending: dict[Any, int] = {}
        def drain() -> None:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                idx = pending.pop(future)
                results.append((idx, future.result()))
                if len(results) % 100 == 0:
                    log.info("Completed %d documents", len(results))
        for idx, row in enumerate(selected_rows(args.documents_csv, args.sample_documents,
                                                args.max_documents, args.seed)):
            doc = row["curr_docID"].strip()
            if doc in seen:
                raise ValueError(f"Duplicate curr_docID: {doc}")
            seen.add(doc)
            pending[pool.submit(process, row)] = idx
            if len(pending) >= args.max_concurrent_requests * 2:
                drain()
        while pending:
            drain()
    rows = [r for _, r in sorted(results)]
    if not rows:
        raise ValueError("No documents selected")
    run_dir.mkdir(parents=True, exist_ok=False)
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with (run_dir / "three_component_sentiment.csv").open("x", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    counts = {c: dict(Counter(r[f"{c}Status"] for r in rows)) for c in COMPONENTS}
    meta = {"version": VERSION, "run_id": run_id, "created_at_utc": now(),
            "source_csv": str(args.documents_csv), "documents": len(rows),
            "model": args.model, "prompt_sha256": prompt_hash,
            "reasoning_effort": args.reasoning_effort, "dry_run": not args.live,
            "target_unit_chars": args.target_unit_chars,
            "max_unit_chars": args.max_unit_chars,
            "sample_documents": args.sample_documents, "seed": args.seed,
            "status_counts": counts,
            "new_input_tokens": sum(r[f"{c}NewInputTokens"] for r in rows for c in COMPONENTS),
            "new_output_tokens": sum(r[f"{c}NewOutputTokens"] for r in rows for c in COMPONENTS)}
    atomic_new_json(run_dir / "manifest.json", meta)
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    print(f"Results: {run_dir / 'three_component_sentiment.csv'}")


if __name__ == "__main__":
    main()
