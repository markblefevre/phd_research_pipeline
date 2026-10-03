"""Paper 2 Stage 6D: LLM-based Japanese MD&A financial sentiment.

The LLM scores narrative units only. All document-level aggregation is deterministic
and performed in Python.

Primary outputs:
    llmPositive
    llmNegative
    llmNet = llmPositive - llmNegative

Design principles:
- Original Japanese MD&A only; no translation.
- Sentiment is measured independently of prior-year text, novelty, and returns.
- Positive and negative intensity are independent 0..4 ordinal scores.
- Obvious flattened-table/extraction noise is filtered deterministically.
- Narrative units are sentence-aware.
- Raw unit-level inputs and outputs are retained for auditability.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import subprocess
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Literal, Sequence

import pandas as pd
from pydantic import BaseModel, Field

try:
    from openai import OpenAI
    import openai
except ImportError:  # Allow dry-run/unit tests without API dependency installed.
    OpenAI = None  # type: ignore[assignment,misc]
    openai = None  # type: ignore[assignment]


TemporalFocus = Literal["realized", "forward", "mixed", "atemporal"]


class UnitScore(BaseModel):
    id: int
    positive: int = Field(ge=0, le=4)
    negative: int = Field(ge=0, le=4)
    temporal_focus: TemporalFocus
    risk_related: bool


class DocumentScoringResponse(BaseModel):
    units: List[UnitScore]


@dataclass(frozen=True)
class NarrativeUnit:
    id: int
    text: str
    chars: int


@dataclass
class CleaningDiagnostics:
    raw_chars: int
    normalized_chars: int
    total_lines: int
    retained_lines: int
    discarded_lines: int
    narrative_chars: int
    retention_share: float
    discarded_examples: List[str]


_JP_SENT_END = re.compile(r"(?<=[。！？])")
_WHITESPACE = re.compile(r"[ \t\u3000]+")
_JP_CHAR = re.compile(r"[\u3040-\u30ff\u3400-\u9fff]")
_LATIN = re.compile(r"[A-Za-z]")
_DIGIT = re.compile(r"[0-9０-９]")
_SENTENCE_PUNCT = re.compile(r"[。！？]")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _git_commit(root: Path | None) -> str | None:
    if root is None:
        return None
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(root),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or None
    except Exception:
        return None


def _nonspace_chars(text: str) -> int:
    return len(re.sub(r"\s+", "", text))


def normalize_mdna_text(text: str) -> str:
    """Normalize whitespace without changing substantive Japanese text."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u00a0", " ").replace("\u3000", " ")
    lines: List[str] = []
    for raw in text.splitlines():
        line = _WHITESPACE.sub(" ", raw).strip()
        lines.append(line)
    return "\n".join(lines)


def _looks_like_narrative(line: str, *, min_line_chars: int) -> bool:
    """Conservative heuristic for narrative prose vs flattened table cells.

    The goal is not to infer economic importance. It only removes obvious extraction
    artifacts and table fragments. Repeated/boilerplate narrative remains eligible.
    """
    s = line.strip()
    if not s:
        return False

    compact = re.sub(r"\s+", "", s)
    n = len(compact)
    if n < min_line_chars:
        return False

    jp = len(_JP_CHAR.findall(compact))
    latin = len(_LATIN.findall(compact))
    digits = len(_DIGIT.findall(compact))
    lexical = jp + latin

    # Pure/near-pure numbers, percentages, dashes, and symbols are table cells.
    if lexical == 0:
        return False

    # Sentence punctuation is the strongest signal that the line is prose.
    if _SENTENCE_PUNCT.search(compact):
        return True

    # Long textual lines without Japanese full stops occur in EDINET extraction.
    # Retain only when lexical content materially dominates digits.
    if n >= 40 and lexical / max(n, 1) >= 0.45 and digits / max(n, 1) <= 0.35:
        return True

    return False


def clean_mdna_narrative(
    text: str,
    *,
    min_line_chars: int = 20,
    discarded_example_limit: int = 25,
) -> tuple[str, CleaningDiagnostics]:
    """Return conservative narrative-only text plus auditable diagnostics."""
    normalized = normalize_mdna_text(text)
    raw_lines = normalized.splitlines()
    retained: List[str] = []
    discarded_examples: List[str] = []
    discarded = 0

    for line in raw_lines:
        if _looks_like_narrative(line, min_line_chars=min_line_chars):
            retained.append(line)
        elif line.strip():
            discarded += 1
            if len(discarded_examples) < discarded_example_limit:
                discarded_examples.append(line[:180])

    narrative = "\n".join(retained).strip()
    raw_chars = _nonspace_chars(text)
    narrative_chars = _nonspace_chars(narrative)
    diag = CleaningDiagnostics(
        raw_chars=raw_chars,
        normalized_chars=_nonspace_chars(normalized),
        total_lines=len(raw_lines),
        retained_lines=len(retained),
        discarded_lines=discarded,
        narrative_chars=narrative_chars,
        retention_share=(narrative_chars / raw_chars) if raw_chars else 0.0,
        discarded_examples=discarded_examples,
    )
    return narrative, diag


def split_japanese_sentences(text: str) -> List[str]:
    """Sentence split while preserving sentence-final punctuation."""
    out: List[str] = []
    for paragraph in text.splitlines():
        p = paragraph.strip()
        if not p:
            continue
        pieces = [x.strip() for x in _JP_SENT_END.split(p) if x.strip()]
        out.extend(pieces)
    return out


def build_narrative_units(
    narrative_text: str,
    *,
    target_chars: int = 1050,
    max_chars: int = 1400,
) -> List[NarrativeUnit]:
    """Group complete sentences into stable, sentence-aware scoring units.

    A single sentence longer than max_chars is retained intact rather than silently
    truncated or split mid-sentence.
    """
    if target_chars <= 0 or max_chars <= 0:
        raise ValueError("target_chars and max_chars must be positive")
    if target_chars > max_chars:
        raise ValueError("target_chars must be <= max_chars")

    sentences = split_japanese_sentences(narrative_text)
    grouped: List[str] = []
    current: List[str] = []
    current_chars = 0

    for sentence in sentences:
        s_chars = _nonspace_chars(sentence)

        if not current:
            current = [sentence]
            current_chars = s_chars
            # Long single sentence is intentionally kept whole.
            if current_chars >= max_chars:
                grouped.append("".join(current))
                current = []
                current_chars = 0
            continue

        proposed = current_chars + s_chars

        # Prefer units near target size, but never exceed max when avoidable.
        if proposed > max_chars or (current_chars >= target_chars and proposed > target_chars):
            grouped.append("".join(current))
            current = [sentence]
            current_chars = s_chars
            if current_chars >= max_chars:
                grouped.append("".join(current))
                current = []
                current_chars = 0
        else:
            current.append(sentence)
            current_chars = proposed

    if current:
        grouped.append("".join(current))

    return [
        NarrativeUnit(id=i, text=text, chars=_nonspace_chars(text))
        for i, text in enumerate(grouped, start=1)
        if text.strip()
    ]


def _render_user_input(units: Sequence[NarrativeUnit]) -> str:
    pieces = [
        "以下の番号付きユニットを、それぞれ独立して採点してください。\n"
        "返却する id は入力 id と完全に一致させてください。\n"
    ]
    for unit in units:
        pieces.append(f"\n<unit id=\"{unit.id}\">\n{unit.text}\n</unit>\n")
    return "".join(pieces)


def _validate_scores(
    units: Sequence[NarrativeUnit],
    parsed: DocumentScoringResponse,
) -> List[UnitScore]:
    expected = [u.id for u in units]
    scores = list(parsed.units)
    got = [s.id for s in scores]

    if len(got) != len(set(got)):
        raise ValueError(f"Duplicate unit ids returned by model: {got}")
    if set(got) != set(expected):
        raise ValueError(
            "Model returned unexpected unit ids. "
            f"expected={expected} got={got}"
        )

    by_id = {s.id: s for s in scores}
    return [by_id[i] for i in expected]


def score_units_openai(
    *,
    client: Any,
    units: Sequence[NarrativeUnit],
    prompt_text: str,
    model: str,
    reasoning_effort: str = "none",
    max_retries: int = 3,
    retry_base_seconds: float = 2.0,
    logger: logging.Logger | None = None,
) -> tuple[List[UnitScore], Dict[str, Any]]:
    """Score one document's units using strict Pydantic Structured Outputs."""
    user_text = _render_user_input(units)
    last_exc: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            kwargs: Dict[str, Any] = {
                "model": model,
                "input": [
                    {"role": "system", "content": prompt_text},
                    {"role": "user", "content": user_text},
                ],
                "text_format": DocumentScoringResponse,
            }
            if reasoning_effort:
                kwargs["reasoning"] = {"effort": reasoning_effort}

            response = client.responses.parse(**kwargs)
            parsed = response.output_parsed
            if parsed is None:
                refusal = getattr(response, "refusal", None)
                raise RuntimeError(
                    "OpenAI returned no parsed structured output"
                    + (f"; refusal={refusal}" if refusal else "")
                )

            scores = _validate_scores(units, parsed)

            usage = getattr(response, "usage", None)
            usage_dict = {
                "input_tokens": getattr(usage, "input_tokens", None),
                "output_tokens": getattr(usage, "output_tokens", None),
                "total_tokens": getattr(usage, "total_tokens", None),
            }
            response_meta = {
                "response_id": getattr(response, "id", None),
                "model": getattr(response, "model", model),
                "usage": usage_dict,
            }
            return scores, response_meta

        except Exception as exc:
            last_exc = exc
            if logger:
                logger.warning(
                    "OpenAI scoring attempt %d/%d failed: %s",
                    attempt,
                    max_retries,
                    exc,
                )
            if attempt >= max_retries:
                break
            time.sleep(retry_base_seconds * (2 ** (attempt - 1)))

    assert last_exc is not None
    raise last_exc


def aggregate_document(
    units: Sequence[NarrativeUnit],
    scores: Sequence[UnitScore],
) -> Dict[str, float | int]:
    if not units:
        raise ValueError("Cannot aggregate an empty unit list")
    if len(units) != len(scores):
        raise ValueError("Unit/score length mismatch")

    score_by_id = {s.id: s for s in scores}
    weights = [u.chars for u in units]
    total_weight = sum(weights)
    if total_weight <= 0:
        raise ValueError("Narrative units have zero total character weight")

    pos = [score_by_id[u.id].positive / 4.0 for u in units]
    neg = [score_by_id[u.id].negative / 4.0 for u in units]
    net = [p - n for p, n in zip(pos, neg)]

    weighted_pos = sum(w * p for w, p in zip(weights, pos)) / total_weight
    weighted_neg = sum(w * n for w, n in zip(weights, neg)) / total_weight
    equal_pos = sum(pos) / len(pos)
    equal_neg = sum(neg) / len(neg)

    forward_chars = sum(
        u.chars
        for u in units
        if score_by_id[u.id].temporal_focus in ("forward", "mixed")
    )
    risk_chars = sum(
        u.chars for u in units if score_by_id[u.id].risk_related
    )

    return {
        "llmPositive": weighted_pos,
        "llmNegative": weighted_neg,
        "llmNet": weighted_pos - weighted_neg,
        "llmPositiveEqualWeight": equal_pos,
        "llmNegativeEqualWeight": equal_neg,
        "llmNetEqualWeight": equal_pos - equal_neg,
        "llmMedianUnitNet": float(median(net)),
        "llmNumUnits": len(units),
        "llmNarrativeChars": total_weight,
        "llmForwardShare": forward_chars / total_weight,
        "llmRiskShare": risk_chars / total_weight,
    }


def _find_doc_file(mdna_root: Path, doc_id: str) -> Path:
    exact = list(mdna_root.rglob(f"{doc_id}.txt"))
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise RuntimeError(f"Multiple exact MD&A files found for {doc_id}: {exact}")

    candidates = [
        p for p in mdna_root.rglob(f"{doc_id}*.txt")
        if p.is_file() and not p.name.startswith(".")
    ]
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise FileNotFoundError(
            f"No MD&A .txt file found recursively under {mdna_root} for docID={doc_id}"
        )
    raise RuntimeError(f"Multiple candidate MD&A files found for {doc_id}: {candidates}")


def discover_documents(
    *,
    mdna_root: Path,
    doc_ids: Sequence[str] | None = None,
    manifest_csv: Path | None = None,
    max_documents: int | None = None,
) -> List[tuple[str, Path]]:
    """Discover prototype documents by docID or production documents from a manifest.

    Prototype `doc_ids` retain the flexible recursive lookup used for smoke tests.
    Production discovery uses the frozen manifest's `edinetCode` + `docID` fields
    to construct the exact local-SSD MD&A path directly, avoiding recursive crawling.
    """
    if doc_ids:
        ids = [str(doc_id) for doc_id in doc_ids]
        if max_documents is not None:
            ids = ids[:max_documents]
        return [(doc_id, _find_doc_file(mdna_root, doc_id)) for doc_id in ids]

    if manifest_csv is None:
        raise ValueError("Either doc_ids or manifest_csv must be provided")
    if not manifest_csv.exists():
        raise FileNotFoundError(manifest_csv)

    manifest = pd.read_csv(manifest_csv, dtype=str)

    required = {"docID", "edinetCode"}
    missing = required - set(manifest.columns)
    if missing:
        raise ValueError(
            f"Manifest missing required columns {sorted(missing)}; "
            f"columns={list(manifest.columns)}"
        )

    rows = manifest.dropna(subset=["docID", "edinetCode"]).copy()
    rows["docID"] = rows["docID"].astype(str).str.strip()
    rows["edinetCode"] = rows["edinetCode"].astype(str).str.strip()
    rows = rows[(rows["docID"] != "") & (rows["edinetCode"] != "")]

    if max_documents is not None:
        rows = rows.iloc[:max_documents]

    documents: List[tuple[str, Path]] = []
    for row in rows.itertuples(index=False):
        doc_id = str(row.docID)
        edinet_code = str(row.edinetCode)
        source_path = mdna_root / edinet_code / f"{doc_id}.txt"

        if not source_path.exists():
            raise FileNotFoundError(
                f"MD&A file not found for docID={doc_id}, "
                f"edinetCode={edinet_code}: {source_path}"
            )

        documents.append((doc_id, source_path))

    return documents


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(path)


def _load_existing_document(path: Path) -> tuple[List[NarrativeUnit], List[UnitScore], Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    units = [
        NarrativeUnit(
            id=int(x["id"]),
            text=str(x["text"]),
            chars=int(x["chars"]),
        )
        for x in payload["units"]
    ]
    scores = [
        UnitScore.model_validate(x["score"])
        for x in payload["units"]
    ]
    return units, scores, payload


def score_llm_corpus(
    *,
    mdna_root: Path,
    prompt_file: Path,
    output_csv: Path,
    raw_units_dir: Path,
    model: str,
    reasoning_effort: str = "none",
    doc_ids: Sequence[str] | None = None,
    manifest_csv: Path | None = None,
    max_documents: int | None = None,
    max_concurrent_requests: int = 1,
    target_unit_chars: int = 1050,
    max_unit_chars: int = 1400,
    min_narrative_line_chars: int = 20,
    resume: bool = True,
    overwrite: bool = False,
    dry_run: bool = False,
    max_retries: int = 3,
    retry_base_seconds: float = 2.0,
    metadata_json: Path | None = None,
    repo_root: Path | None = None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Score a corpus or prototype subset and write document-level results."""
    logger = logger or logging.getLogger(__name__)

    mdna_root = mdna_root.expanduser().resolve()
    prompt_file = prompt_file.expanduser().resolve()
    output_csv = output_csv.expanduser().resolve()
    raw_units_dir = raw_units_dir.expanduser().resolve()
    metadata_json = (
        metadata_json.expanduser().resolve()
        if metadata_json is not None
        else output_csv.with_suffix(".metadata.json")
    )

    if not mdna_root.exists():
        raise FileNotFoundError(f"MD&A root does not exist: {mdna_root}")
    if not prompt_file.exists():
        raise FileNotFoundError(f"Prompt file does not exist: {prompt_file}")

    if not dry_run and OpenAI is None:
        raise RuntimeError(
            "The OpenAI Python SDK is required for API scoring. "
            "Install/upgrade openai, or run with dry_run=true."
        )
    if not dry_run and not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required unless dry_run=true")

    prompt_text = prompt_file.read_text(encoding="utf-8").strip()
    prompt_sha256 = _sha256_text(prompt_text)

    if max_documents is not None and max_documents <= 0:
        raise ValueError("max_documents must be positive when provided")
    if max_concurrent_requests <= 0:
        raise ValueError("max_concurrent_requests must be positive")

    documents = discover_documents(
        mdna_root=mdna_root,
        doc_ids=doc_ids,
        manifest_csv=manifest_csv,
        max_documents=max_documents,
    )
    if not documents:
        raise RuntimeError("No documents selected for Stage 6D")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    raw_units_dir.mkdir(parents=True, exist_ok=True)

    thread_state = threading.local()

    def _get_client() -> Any:
        if dry_run:
            return None
        client = getattr(thread_state, "client", None)
        if client is None:
            client = OpenAI()  # type: ignore[operator]
            thread_state.client = client
        return client

    rows: List[Dict[str, Any]] = []
    total_input_tokens = 0
    total_output_tokens = 0
    started = datetime.now(timezone.utc)

    logger.info(
        "LLM sentiment start: docs=%d model=%s dry_run=%s concurrency=%d prompt=%s",
        len(documents),
        model,
        dry_run,
        max_concurrent_requests,
        prompt_file,
    )

    def _process_document(
        idx: int,
        doc_id: str,
        source_path: Path,
    ) -> tuple[int, Dict[str, Any], int, int]:
        raw_json = raw_units_dir / f"{doc_id}.json"

        if raw_json.exists() and resume and not overwrite and not dry_run:
            logger.info("[%d/%d] %s resume from %s", idx, len(documents), doc_id, raw_json)
            units, scores, payload = _load_existing_document(raw_json)
            aggregate = aggregate_document(units, scores)
            cleaning = payload.get("cleaning", {})
            usage = payload.get("response", {}).get("usage", {}) or {}
            row = {
                "docID": doc_id,
                "sourcePath": str(source_path),
                "status": "resumed",
                "rawChars": cleaning.get("raw_chars"),
                "narrativeChars": cleaning.get("narrative_chars"),
                "retentionShare": cleaning.get("retention_share"),
                **aggregate,
            }
            return (
                idx,
                row,
                int(usage.get("input_tokens") or 0),
                int(usage.get("output_tokens") or 0),
            )

        text = source_path.read_text(encoding="utf-8")
        narrative, cleaning_diag = clean_mdna_narrative(
            text,
            min_line_chars=min_narrative_line_chars,
        )
        units = build_narrative_units(
            narrative,
            target_chars=target_unit_chars,
            max_chars=max_unit_chars,
        )

        if not units:
            raise RuntimeError(f"No narrative units survived cleaning for {doc_id}")

        logger.info(
            "[%d/%d] %s raw=%d narrative=%d retention=%.3f units=%d",
            idx,
            len(documents),
            doc_id,
            cleaning_diag.raw_chars,
            cleaning_diag.narrative_chars,
            cleaning_diag.retention_share,
            len(units),
        )

        if dry_run:
            preview_payload = {
                "docID": doc_id,
                "source_path": str(source_path),
                "source_sha256": _sha256_file(source_path),
                "dry_run": True,
                "prompt_file": str(prompt_file),
                "prompt_sha256": prompt_sha256,
                "cleaning": asdict(cleaning_diag),
                "units": [asdict(u) for u in units],
            }
            _write_json(raw_json.with_name(f"{doc_id}.dry_run.json"), preview_payload)
            row = {
                "docID": doc_id,
                "sourcePath": str(source_path),
                "status": "dry_run",
                "rawChars": cleaning_diag.raw_chars,
                "narrativeChars": cleaning_diag.narrative_chars,
                "retentionShare": cleaning_diag.retention_share,
                "llmNumUnits": len(units),
            }
            return idx, row, 0, 0

        client = _get_client()
        scores, response_meta = score_units_openai(
            client=client,
            units=units,
            prompt_text=prompt_text,
            model=model,
            reasoning_effort=reasoning_effort,
            max_retries=max_retries,
            retry_base_seconds=retry_base_seconds,
            logger=logger,
        )
        aggregate = aggregate_document(units, scores)

        usage = response_meta.get("usage", {})
        input_tokens = int(usage.get("input_tokens") or 0)
        output_tokens = int(usage.get("output_tokens") or 0)

        score_by_id = {s.id: s for s in scores}
        payload = {
            "docID": doc_id,
            "source_path": str(source_path),
            "source_sha256": _sha256_file(source_path),
            "prompt_file": str(prompt_file),
            "prompt_sha256": prompt_sha256,
            "model_requested": model,
            "reasoning_effort": reasoning_effort,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "cleaning": asdict(cleaning_diag),
            "aggregation": aggregate,
            "response": response_meta,
            "units": [
                {
                    **asdict(unit),
                    "score": score_by_id[unit.id].model_dump(),
                    "unitNet": (
                        score_by_id[unit.id].positive
                        - score_by_id[unit.id].negative
                    ) / 4.0,
                }
                for unit in units
            ],
        }
        _write_json(raw_json, payload)

        row = {
            "docID": doc_id,
            "sourcePath": str(source_path),
            "status": "scored",
            "rawChars": cleaning_diag.raw_chars,
            "narrativeChars": cleaning_diag.narrative_chars,
            "retentionShare": cleaning_diag.retention_share,
            **aggregate,
        }
        return idx, row, input_tokens, output_tokens

    results_by_index: Dict[int, Dict[str, Any]] = {}

    if max_concurrent_requests == 1:
        for idx, (doc_id, source_path) in enumerate(documents, start=1):
            result_idx, row, input_tokens, output_tokens = _process_document(
                idx, doc_id, source_path
            )
            results_by_index[result_idx] = row
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
    else:
        with ThreadPoolExecutor(max_workers=max_concurrent_requests) as executor:
            futures = {
                executor.submit(_process_document, idx, doc_id, source_path): idx
                for idx, (doc_id, source_path) in enumerate(documents, start=1)
            }
            for future in as_completed(futures):
                result_idx, row, input_tokens, output_tokens = future.result()
                results_by_index[result_idx] = row
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

    rows = [results_by_index[i] for i in sorted(results_by_index)]

    out = pd.DataFrame(rows)
    out.to_csv(output_csv, index=False, encoding="utf-8")

    finished = datetime.now(timezone.utc)
    meta = {
        "stage": "llm_sentiment",
        "started_at_utc": started.isoformat(),
        "finished_at_utc": finished.isoformat(),
        "elapsed_seconds": (finished - started).total_seconds(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "dry_run": dry_run,
        "prompt_file": str(prompt_file),
        "prompt_sha256": prompt_sha256,
        "mdna_root": str(mdna_root),
        "document_count": len(documents),
        "doc_ids": [doc_id for doc_id, _ in documents],
        "max_documents": max_documents,
        "max_concurrent_requests": max_concurrent_requests,
        "target_unit_chars": target_unit_chars,
        "max_unit_chars": max_unit_chars,
        "min_narrative_line_chars": min_narrative_line_chars,
        "aggregation": "non-whitespace narrative-character weighted",
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "openai_python_version": getattr(openai, "__version__", None) if openai else None,
        "git_commit": _git_commit(repo_root),
        "output_csv": str(output_csv),
        "raw_units_dir": str(raw_units_dir),
    }
    _write_json(metadata_json, meta)

    logger.info(
        "LLM sentiment finished: docs=%d input_tokens=%d output_tokens=%d output=%s",
        len(out),
        total_input_tokens,
        total_output_tokens,
        output_csv,
    )
    return out
