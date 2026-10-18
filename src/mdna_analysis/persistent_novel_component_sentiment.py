"""Paper 2 Stage 6F-B: score frozen Stage 6E persistent and novel components.

Read-only inputs: Stage 6E persistent_novel_documents.csv, Stage 6D v2 prompt.
Outputs are *new* per-component audit JSON files and unique per-run snapshots.
No source file or existing score is overwritten, even when resume is enabled.

Reuses Stage 6D's Japanese financial-tone schema, unit builder, API scorer,
and character-weighted deterministic aggregation. Does not re-clean the
already validated Stage 6E sentence-classified text.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import random
import sys
import tempfile
import threading
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator

from src.mdna_analysis.llm_sentiment import (
    OpenAI,
    UnitScore,
    aggregate_document,
    build_narrative_units,
    score_units_openai,
)

COMPONENTS = ("persistent", "novel")
REQUIRED_COLUMNS = {"curr_docID", "prev_docID", "persistentText", "novelText"}
VERSION = "stage6fb_v1"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_bytes(payload: Dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _write_new(path: Path, content: bytes) -> None:
    """Publish a complete new file atomically; refuse any existing target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".stage6fb_", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(content)
            out.flush()
            os.fsync(out.fileno())
        os.link(tmp_name, path)  # atomic create-if-absent on the same filesystem
    finally:
        Path(tmp_name).unlink(missing_ok=True)


def _iter_pairs(csv_path: Path) -> Iterator[Dict[str, str]]:
    # Stage 6E text fields can be larger than the standard csv field limit.
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            break
        except OverflowError:
            limit //= 10
    with csv_path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Stage 6E documents CSV missing columns: {sorted(missing)}")
        for row in reader:
            if not row["curr_docID"] or not row["prev_docID"]:
                raise ValueError("Missing curr_docID or prev_docID in Stage 6E input")
            yield row


def _select_pairs(csv_path: Path, *, max_documents: int | None,
                  sample_documents: int | None, seed: int) -> Iterator[Dict[str, str]]:
    if max_documents is not None and sample_documents is not None:
        raise ValueError("Choose either max_documents or sample_documents, not both")
    if max_documents is not None:
        for idx, row in enumerate(_iter_pairs(csv_path)):
            if idx >= max_documents:
                break
            yield row
    elif sample_documents is not None:
        # Uniform deterministic reservoir sample; retains source order afterward.
        rng = random.Random(seed)
        sample: list[tuple[int, Dict[str, str]]] = []
        for idx, row in enumerate(_iter_pairs(csv_path)):
            if idx < sample_documents:
                sample.append((idx, row))
            else:
                replacement = rng.randrange(idx + 1)
                if replacement < sample_documents:
                    sample[replacement] = (idx, row)
        for _, row in sorted(sample, key=lambda item: item[0]):
            yield row
    else:
        yield from _iter_pairs(csv_path)


def _load_resume(path: Path, *, doc_id: str, prev_doc_id: str,
                 component: str, text_hash: str, prompt_hash: str,
                 model: str, reasoning_effort: str, target_unit_chars: int,
                 max_unit_chars: int) -> tuple[Dict[str, Any], Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = {
        "version": VERSION, "curr_docID": doc_id, "prev_docID": prev_doc_id,
        "component": component, "text_sha256": text_hash,
        "prompt_sha256": prompt_hash, "model_requested": model,
        "reasoning_effort": reasoning_effort,
        "target_unit_chars": target_unit_chars, "max_unit_chars": max_unit_chars,
    }
    mismatches = [key for key, value in expected.items() if payload.get(key) != value]
    if mismatches:
        raise ValueError(
            f"Existing audit file {path} does not match this run: {mismatches}. "
            "It has NOT been changed. Use a separate output directory or investigate."
        )
    # Audit file is the source of truth for completed scoring. Recompute to
    # catch incomplete or corrupted files rather than trusting stored aggregate.
    from src.mdna_analysis.llm_sentiment import NarrativeUnit
    units = [NarrativeUnit(id=int(u["id"]), text=u["text"], chars=int(u["chars"]))
             for u in payload["units"]]
    scores = [UnitScore.model_validate(u["score"]) for u in payload["units"]]
    aggregation = aggregate_document(units, scores)
    if payload.get("aggregation") != aggregation:
        raise ValueError(f"Stored aggregation differs from recomputed values: {path}")
    return aggregation, payload.get("response", {}).get("usage", {})


def run_component_sentiment(
    *, documents_csv: Path, prompt_file: Path, output_dir: Path,
    model: str = "gpt-6-luna", reasoning_effort: str = "none",
    max_documents: int | None = None, sample_documents: int | None = None,
    sample_seed: int = 42, max_concurrent_requests: int = 1,
    target_unit_chars: int = 1050, max_unit_chars: int = 1400,
    max_retries: int = 3, retry_base_seconds: float = 2.0,
    dry_run: bool = True, resume: bool = True,
    qc_summary_json: Path | None = None,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    """Score components; return metadata with unique, non-overwriting output paths.

    Failed components are recorded in a unique run snapshot. Successful
    components are retained and resumed on a later run. An existing audit JSON
    with changed model, prompt, or text causes a hard failure, not replacement.
    """
    logger = logger or logging.getLogger(__name__)
    documents_csv = Path(documents_csv).expanduser().resolve()
    prompt_file = Path(prompt_file).expanduser().resolve()
    output_dir = Path(output_dir).expanduser().resolve()
    if not documents_csv.is_file():
        raise FileNotFoundError(documents_csv)
    if not prompt_file.is_file():
        raise FileNotFoundError(prompt_file)
    if max_documents is not None and max_documents < 1:
        raise ValueError("max_documents must be >= 1")
    if sample_documents is not None and sample_documents < 1:
        raise ValueError("sample_documents must be >= 1")
    if max_concurrent_requests < 1:
        raise ValueError("max_concurrent_requests must be >= 1")
    if not 0 < target_unit_chars <= max_unit_chars:
        raise ValueError("Require 0 < target_unit_chars <= max_unit_chars")
    if max_retries < 1:
        raise ValueError("max_retries must be >= 1")
    if not dry_run:
        if qc_summary_json is None:
            raise ValueError("Live scoring requires the Stage 6F-A QC summary path")
        qc_path = Path(qc_summary_json).expanduser().resolve()
        qc = json.loads(qc_path.read_text(encoding="utf-8"))
        if (qc.get("status") != "passed" or qc.get("failures") != 0
                or qc.get("documents") != 33046 or qc.get("sentences") != 3463989):
            raise ValueError(f"Stage 6F-A QC has not passed the frozen universe: {qc_path}")
        source_in_qc = qc.get("inputs", {}).get("documentsCsv")
        if not source_in_qc or Path(source_in_qc).expanduser().resolve() != documents_csv:
            raise ValueError("QC summary refers to a different Stage 6E documents CSV")
    if not dry_run and OpenAI is None:
        raise RuntimeError("OpenAI SDK is required for live scoring")
    if not dry_run and not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required for live scoring")
    if not dry_run and not resume:
        raise ValueError("For safety, live Stage 6F-B requires resume=true")

    prompt = prompt_file.read_text(encoding="utf-8").strip()
    prompt_hash = _sha(prompt)
    # Unique run folder, not a fixed filename. No existing snapshots are touched.
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid.uuid4().hex[:8]
    run_dir = output_dir / "runs" / run_id
    raw_dir = output_dir / "raw_components"
    thread_local = threading.local()

    def get_client() -> Any:
        client = getattr(thread_local, "client", None)
        if client is None:
            client = OpenAI()
            thread_local.client = client
        return client

    def process(row: Dict[str, str]) -> Dict[str, Any]:
        doc_id = row["curr_docID"].strip()
        prev_doc_id = row["prev_docID"].strip()
        result: Dict[str, Any] = {"curr_docID": doc_id, "prev_docID": prev_doc_id,
                                  "model": model, "promptSha256": prompt_hash}
        for component in COMPONENTS:
            component_text = row[f"{component}Text"] or ""
            result[f"{component}Chars"] = len(component_text)
            result[f"{component}TextSha256"] = _sha(component_text)
            result[f"{component}Status"] = "pending"
            result[f"{component}NewInputTokens"] = 0
            result[f"{component}NewOutputTokens"] = 0
            if not component_text.strip():
                result[f"{component}Status"] = "empty"
                result[f"{component}LlmNumUnits"] = 0
                continue
            audit_path = raw_dir / component / f"{doc_id}.json"
            if audit_path.exists():
                if not resume:
                    raise FileExistsError(f"Existing audit file (resume=false): {audit_path}")
                aggregation, _ = _load_resume(
                    audit_path, doc_id=doc_id, prev_doc_id=prev_doc_id,
                    component=component, text_hash=_sha(component_text),
                    prompt_hash=prompt_hash, model=model,
                    reasoning_effort=reasoning_effort,
                    target_unit_chars=target_unit_chars, max_unit_chars=max_unit_chars,
                )
                result[f"{component}Status"] = "resumed"
                result.update({component + key[0].upper() + key[1:]: value
                               for key, value in aggregation.items()})
                result[f"{component}Sentiment"] = aggregation["llmNet"]
                continue

            # Stage 6E's text has already passed sentence-level QC. Re-cleaning
            # would silently change its membership; preserve text as-is.
            units = build_narrative_units(component_text, target_chars=target_unit_chars,
                                          max_chars=max_unit_chars)
            if not units:
                result[f"{component}Status"] = "empty"
                result[f"{component}LlmNumUnits"] = 0
                continue
            result[f"{component}LlmNumUnits"] = len(units)
            if dry_run:
                result[f"{component}Status"] = "dry_run"
                continue

            try:
                scores, response = score_units_openai(
                    client=get_client(), units=units, prompt_text=prompt,
                    model=model, reasoning_effort=reasoning_effort,
                    max_retries=max_retries, retry_base_seconds=retry_base_seconds,
                    logger=logger,
                )
                aggregation = aggregate_document(units, scores)
                by_id = {s.id: s for s in scores}
                payload = {
                    "version": VERSION, "curr_docID": doc_id, "prev_docID": prev_doc_id,
                    "component": component, "text_sha256": _sha(component_text),
                    "prompt_file": str(prompt_file), "prompt_sha256": prompt_hash,
                    "model_requested": model, "reasoning_effort": reasoning_effort,
                    "target_unit_chars": target_unit_chars, "max_unit_chars": max_unit_chars,
                    "created_at_utc": _now(), "aggregation": aggregation,
                    "response": response,
                    "units": [{"id": u.id, "text": u.text, "chars": u.chars,
                               "score": by_id[u.id].model_dump()}
                              for u in units],
                }
                _write_new(audit_path, _json_bytes(payload))
                result.update({component + key[0].upper() + key[1:]: value
                               for key, value in aggregation.items()})
                result[f"{component}Sentiment"] = aggregation["llmNet"]
                result[f"{component}Status"] = "scored"
                usage = response.get("usage", {}) or {}
                result[f"{component}NewInputTokens"] = int(usage.get("input_tokens") or 0)
                result[f"{component}NewOutputTokens"] = int(usage.get("output_tokens") or 0)
            except FileExistsError:
                # Existing output must never be overwritten.
                raise
            except Exception as exc:
                logger.exception("Stage 6F-B failed: docID=%s component=%s", doc_id, component)
                result[f"{component}Status"] = "failed"
                result[f"{component}Error"] = f"{type(exc).__name__}: {exc}"[:500]

        p = result.get("persistentSentiment")
        n = result.get("novelSentiment")
        result["novelMinusPersistentSentiment"] = (n - p) if p is not None and n is not None else None
        return result

    started = _now()
    results: list[tuple[int, Dict[str, Any]]] = []
    # Bound memory use even for a 33,046-row full run.
    with ThreadPoolExecutor(max_workers=max_concurrent_requests) as pool:
        pending: dict[Any, int] = {}
        pairs = _select_pairs(documents_csv, max_documents=max_documents,
                              sample_documents=sample_documents, seed=sample_seed)
        idx = 0
        def drain_one() -> None:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                result_idx = pending.pop(future)
                results.append((result_idx, future.result()))
        for pair in pairs:
            pending[pool.submit(process, pair)] = idx
            idx += 1
            if len(pending) >= max_concurrent_requests * 2:
                drain_one()
        while pending:
            drain_one()

    rows = [row for _, row in sorted(results, key=lambda x: x[0])]
    if not rows:
        raise ValueError("No Stage 6E document pairs selected")
    if len({r["curr_docID"] for r in rows}) != len(rows):
        raise ValueError("Duplicate curr_docID in selected Stage 6E documents")

    run_dir.mkdir(parents=True, exist_ok=False)
    result_path = run_dir / "component_sentiment.csv"
    fields = list(dict.fromkeys(key for row in rows for key in row))
    # CSV is written once into a newly created run directory.
    with result_path.open("x", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    status_counts = {c: {status: sum(row.get(f"{c}Status") == status for row in rows)
                         for status in ("scored", "resumed", "empty", "dry_run", "failed")}
                     for c in COMPONENTS}
    metadata = {
        "version": VERSION, "started_at_utc": started, "finished_at_utc": _now(),
        "run_id": run_id, "documents": len(rows), "dry_run": dry_run,
        "source_csv": str(documents_csv), "prompt_file": str(prompt_file),
        "prompt_sha256": prompt_hash, "model": model,
        "reasoning_effort": reasoning_effort, "max_documents": max_documents,
        "sample_documents": sample_documents, "sample_seed": sample_seed,
        "max_concurrent_requests": max_concurrent_requests,
        "target_unit_chars": target_unit_chars, "max_unit_chars": max_unit_chars,
        "aggregation": "Stage 6D non-whitespace narrative-character weighted",
        "component_qc": str(Path(qc_summary_json).expanduser().resolve()) if qc_summary_json else None,
        "status_counts": status_counts,
        "new_input_tokens": sum(row[f"{c}NewInputTokens"] for row in rows for c in COMPONENTS),
        "new_output_tokens": sum(row[f"{c}NewOutputTokens"] for row in rows for c in COMPONENTS),
        "results_csv": str(result_path), "raw_components_dir": str(raw_dir),
    }
    _write_new(run_dir / "metadata.json", _json_bytes(metadata))
    logger.info("Stage 6F-B complete: %d pairs; new input=%d output=%d tokens; %s",
                len(rows), metadata["new_input_tokens"], metadata["new_output_tokens"], result_path)
    return metadata
