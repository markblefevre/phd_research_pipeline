"""Core implementation for Paper 2 Stage 6E: persistent/novel MD&A split.

Frozen production architecture (2026-10-06):

    current sentence
        -> Ruri top-10 prior-year sentence retrieval
        -> corpus-IDF EDINET/IFRS concept reranking
           score = cosine + 0.02 * weighted_jaccard
        -> top 3 candidates
        -> GPT-6 Luna + persistent_novel_classifier_v1_1.md
        -> persistent / novel

The implementation deliberately separates retrieval from classification.  A
retrieval-only first pass can be run over the complete corpus without API calls
to establish the number of sentence classifications and inspect retrieval QC
before enabling the LLM step.
"""

from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import logging
import math
import re
import time
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


MIN_SENTENCE_CHARS = 12
VALID_LABELS = {"persistent", "novel"}
VALID_CONFIDENCE = {"high", "mid", "low"}


def _atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _append_jsonl(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        fh.flush()


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSONL at {path}:{line_no}: {exc}"
                ) from exc


def _completed_pair_ids(path: Path) -> set[str]:
    return {
        str(row["curr_docID"])
        for row in _iter_jsonl(path) or []
        if row.get("status") == "ok" and row.get("curr_docID")
    }


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def normalize_sentence(text: str) -> str:
    text = str(text).replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def split_japanese_sentences(
    text: str,
    min_chars: int = MIN_SENTENCE_CHARS,
) -> list[str]:
    """Canonical splitter used by the alignment benchmark."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    chunks = re.split(r"(?<=[。！？!?])|\n+", text)
    sentences = [normalize_sentence(x) for x in chunks]
    return [x for x in sentences if len(x) >= min_chars]


def _norm_match(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = unicodedata.normalize("NFKC", str(value))
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s).strip()
    return re.sub(r"\s+", "", s)


class AliasMatcher:
    """Literal EDINET/IFRS Japanese alias matcher bucketed by first char."""

    def __init__(self, aliases: Iterable[str]):
        self.by_first: dict[str, list[str]] = defaultdict(list)
        for alias in sorted(set(aliases), key=lambda s: (-len(s), s)):
            if alias:
                self.by_first[alias[0]].append(alias)

    def match(self, text: str) -> set[str]:
        t = _norm_match(text)
        if not t:
            return set()
        matched: set[str] = set()
        for ch in set(t):
            for alias in self.by_first.get(ch, []):
                if alias in t:
                    matched.add(alias)
        return matched


def load_alias_matcher(
    concepts_csv: Path,
    idf_cache: Path,
    min_alias_chars: int = 2,
) -> tuple[AliasMatcher, dict[str, float]]:
    """Load the frozen EDINET/IFRS alias dictionary and corpus-IDF cache."""
    if not concepts_csv.exists():
        raise FileNotFoundError(f"Concept dictionary not found: {concepts_csv}")
    if not idf_cache.exists():
        raise FileNotFoundError(
            f"Corpus-IDF cache not found: {idf_cache}. "
            "Build it with evaluate_concept_reranking.py before Stage 6E."
        )

    concepts = pd.read_csv(concepts_csv)
    required = {"language", "label_normalized"}
    missing = required - set(concepts.columns)
    if missing:
        raise ValueError(
            f"Concept dictionary missing columns: {sorted(missing)}"
        )

    c = concepts.copy()
    c = c[c["language"].astype(str).str.lower().eq("ja")]
    if "match_eligible" in c.columns:
        eligible = c["match_eligible"].astype(str).str.lower()
        c = c[eligible.isin(["true", "1"])]
    c["alias"] = c["label_normalized"].map(_norm_match)
    aliases = c.loc[c["alias"].str.len() >= min_alias_chars, "alias"].unique()

    idf = pd.read_csv(idf_cache)
    required_idf = {"alias", "idf"}
    missing_idf = required_idf - set(idf.columns)
    if missing_idf:
        raise ValueError(f"IDF cache missing columns: {sorted(missing_idf)}")

    idf_map = dict(zip(idf["alias"].astype(str), idf["idf"].astype(float)))
    return AliasMatcher(aliases), idf_map


def _weighted_jaccard(
    current_aliases: set[str],
    candidate_aliases: set[str],
    idf_map: dict[str, float],
) -> float:
    union = current_aliases | candidate_aliases
    if not union:
        return 0.0
    shared = current_aliases & candidate_aliases
    shared_weight = sum(idf_map.get(a, 1.0) for a in shared)
    union_weight = sum(idf_map.get(a, 1.0) for a in union)
    return shared_weight / union_weight if union_weight > 0 else 0.0


def _choose_device(requested: str) -> str:
    if requested != "auto":
        return requested
    import torch

    if torch.cuda.is_available():
        return "cuda"
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def _resolve_mdna_path(root: Path, edinet_code: str, doc_id: str) -> Path:
    candidates = [
        root / edinet_code / f"{doc_id}.txt",
        root / doc_id / f"{doc_id}.txt",
        root / f"{doc_id}.txt",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Could not locate MD&A text for {edinet_code}/{doc_id} below {root}"
    )


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig", errors="replace")


def _top_k(
    query_embeddings: np.ndarray,
    candidate_embeddings: np.ndarray,
    k: int,
) -> tuple[np.ndarray, np.ndarray]:
    if candidate_embeddings.shape[0] == 0:
        raise ValueError("Cannot retrieve from an empty prior sentence set.")
    k = min(k, candidate_embeddings.shape[0])
    scores = query_embeddings @ candidate_embeddings.T
    part = np.argpartition(-scores, kth=k - 1, axis=1)[:, :k]
    part_scores = np.take_along_axis(scores, part, axis=1)
    order = np.argsort(-part_scores, axis=1)
    return (
        np.take_along_axis(part, order, axis=1),
        np.take_along_axis(part_scores, order, axis=1),
    )


def _rerank_one(
    current_sentence: str,
    prior_sentences: list[str],
    indices: list[int],
    cosines: list[float],
    matcher: AliasMatcher,
    idf_map: dict[str, float],
    lam: float,
) -> list[dict[str, Any]]:
    current_aliases = matcher.match(current_sentence)
    rows: list[dict[str, Any]] = []

    for baseline_rank, (idx, cosine) in enumerate(
        zip(indices, cosines), start=1
    ):
        candidate = prior_sentences[int(idx)]
        candidate_aliases = matcher.match(candidate)
        wj = _weighted_jaccard(current_aliases, candidate_aliases, idf_map)
        rows.append(
            {
                "prior_index": int(idx),
                "cosine": float(cosine),
                "weighted_jaccard": float(wj),
                "rerank_score": float(cosine) + lam * float(wj),
                "baseline_rank": baseline_rank,
            }
        )

    rows.sort(
        key=lambda r: (
            -r["rerank_score"],
            -r["cosine"],
            r["baseline_rank"],
        )
    )
    for rank, row in enumerate(rows, start=1):
        row["rerank_rank"] = rank
    return rows


def build_retrieval_pairs(
    *,
    pairs_csv: Path,
    mdna_root: Path,
    concepts_csv: Path,
    idf_cache: Path,
    retrieval_jsonl: Path,
    retriever_model: str = "cl-nagoya/ruri-v3-310m",
    retrieval_top_k: int = 10,
    rerank_top_k: int = 3,
    rerank_lambda: float = 0.02,
    min_sentence_chars: int = MIN_SENTENCE_CHARS,
    embedding_batch_size: int = 32,
    device: str = "auto",
    resume: bool = True,
    overwrite: bool = False,
    max_pairs: int | None = None,
    max_sentences: int | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Create one atomic JSONL record per longitudinal pair."""
    log = logger or logging.getLogger(__name__)

    if retrieval_top_k < rerank_top_k:
        raise ValueError("retrieval_top_k must be >= rerank_top_k")
    if overwrite and retrieval_jsonl.exists():
        retrieval_jsonl.unlink()
    completed = _completed_pair_ids(retrieval_jsonl) if resume else set()

    pairs = pd.read_csv(pairs_csv, dtype=str, encoding="utf-8-sig")
    required = {"edinetCode", "prev_docID", "curr_docID"}
    missing = required - set(pairs.columns)
    if missing:
        raise ValueError(f"Pair manifest missing columns: {sorted(missing)}")
    if max_pairs is not None:
        pairs = pairs.head(int(max_pairs)).copy()

    matcher, idf_map = load_alias_matcher(concepts_csv, idf_cache)

    from sentence_transformers import SentenceTransformer

    resolved_device = _choose_device(device)
    log.info("Stage 6E retrieval: loading %s on %s", retriever_model, resolved_device)
    model = SentenceTransformer(retriever_model, device=resolved_device)

    pair_count = 0
    sentence_count = 0
    skipped_pairs = 0

    for pair_no, row in enumerate(pairs.itertuples(index=False), start=1):
        edinet = str(row.edinetCode)
        prev_doc = str(row.prev_docID)
        curr_doc = str(row.curr_docID)

        if curr_doc in completed:
            skipped_pairs += 1
            continue

        prev_path = _resolve_mdna_path(mdna_root, edinet, prev_doc)
        curr_path = _resolve_mdna_path(mdna_root, edinet, curr_doc)
        prior_sentences = split_japanese_sentences(
            _read_text(prev_path), min_chars=min_sentence_chars
        )
        current_sentences = split_japanese_sentences(
            _read_text(curr_path), min_chars=min_sentence_chars
        )

        if not prior_sentences or not current_sentences:
            payload = {
                "status": "error",
                "edinetCode": edinet,
                "prev_docID": prev_doc,
                "curr_docID": curr_doc,
                "error": "empty_prior_or_current_sentence_set",
            }
            _append_jsonl(payload, retrieval_jsonl)
            log.warning("Stage 6E retrieval empty sentence set: %s", payload)
            continue

        if max_sentences is not None:
            remaining = int(max_sentences) - sentence_count
            if remaining <= 0:
                break
            current_sentences = current_sentences[:remaining]

        prior_e = np.asarray(
            model.encode(
                prior_sentences,
                batch_size=embedding_batch_size,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
        )
        current_e = np.asarray(
            model.encode(
                current_sentences,
                batch_size=embedding_batch_size,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
        )
        idx, scores = _top_k(current_e, prior_e, retrieval_top_k)

        sentence_rows = []
        for sent_idx, current_sentence in enumerate(current_sentences):
            base_idx = [int(x) for x in idx[sent_idx].tolist()]
            base_cos = [float(x) for x in scores[sent_idx].tolist()]
            reranked = _rerank_one(
                current_sentence,
                prior_sentences,
                base_idx,
                base_cos,
                matcher,
                idf_map,
                rerank_lambda,
            )
            sentence_rows.append(
                {
                    "current_sentence_index": sent_idx,
                    "current_sentence": current_sentence,
                    "baseline_candidates": [
                        {"prior_index": i, "cosine": c}
                        for i, c in zip(base_idx, base_cos)
                    ],
                    "reranked_candidates": reranked,
                    "classifier_candidate_indices": [
                        r["prior_index"] for r in reranked[:rerank_top_k]
                    ],
                }
            )

        payload = {
            "status": "ok",
            "edinetCode": edinet,
            "prev_docID": prev_doc,
            "curr_docID": curr_doc,
            "prior_sentence_count": len(prior_sentences),
            "current_sentence_count": len(current_sentences),
            "retriever_model": retriever_model,
            "retrieval_top_k": retrieval_top_k,
            "rerank_top_k": rerank_top_k,
            "rerank_variant": "cosine_plus_idf_jaccard",
            "rerank_lambda": rerank_lambda,
            "min_sentence_chars": min_sentence_chars,
            "sentences": sentence_rows,
        }
        _append_jsonl(payload, retrieval_jsonl)

        pair_count += 1
        sentence_count += len(sentence_rows)
        if pair_no % 25 == 0 or pair_no == len(pairs):
            log.info(
                "Stage 6E retrieval progress: pair=%d/%d new_pairs=%d sentences=%d",
                pair_no,
                len(pairs),
                pair_count,
                sentence_count,
            )

        if max_sentences is not None and sentence_count >= int(max_sentences):
            break

    return {
        "pairs_available": int(len(pairs)),
        "pairs_written": pair_count,
        "pairs_skipped_resume": skipped_pairs,
        "sentences_written": sentence_count,
        "retrieval_jsonl": str(retrieval_jsonl),
        "device": resolved_device,
    }


def _extract_output_text(response) -> str:
    text = getattr(response, "output_text", None)
    if text:
        return text
    pieces: list[str] = []
    for item in getattr(response, "output", []) or []:
        for content in getattr(item, "content", []) or []:
            t = getattr(content, "text", None)
            if t:
                pieces.append(t)
    return "\n".join(pieces)


def _parse_llm_json(text: str) -> dict[str, str]:
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", s, flags=re.S)
        if not m:
            raise
        obj = json.loads(m.group(0))

    label = str(obj.get("label", "")).strip().lower()
    confidence = str(obj.get("confidence", "")).strip().lower()
    reason = str(obj.get("reason", "")).strip()
    if label not in VALID_LABELS:
        raise ValueError(f"Invalid label: {label!r}")
    if confidence not in VALID_CONFIDENCE:
        raise ValueError(f"Invalid confidence: {confidence!r}")
    return {"label": label, "confidence": confidence, "reason": reason}


def _call_classifier(
    *,
    client,
    model: str,
    prompt: str,
    current_sentence: str,
    prior_context: str,
    reasoning_effort: str,
    max_retries: int,
    retry_base_seconds: float,
) -> dict[str, Any]:
    user_input = (
        "## CURRENT-YEAR SENTENCE\n"
        f"{current_sentence}\n\n"
        "## RETRIEVED PRIOR-YEAR CANDIDATES\n"
        f"{prior_context}\n"
    )
    kwargs: dict[str, Any] = {
        "model": model,
        "instructions": prompt,
        "input": user_input,
    }
    if reasoning_effort != "none":
        kwargs["reasoning"] = {"effort": reasoning_effort}

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = client.responses.create(**kwargs)
            parsed = _parse_llm_json(_extract_output_text(response))
            usage = getattr(response, "usage", None)
            return {
                **parsed,
                "input_tokens": getattr(usage, "input_tokens", None)
                if usage
                else None,
                "output_tokens": getattr(usage, "output_tokens", None)
                if usage
                else None,
                "total_tokens": getattr(usage, "total_tokens", None)
                if usage
                else None,
            }
        except Exception as exc:  # API/parse failures use same retry policy.
            last_exc = exc
            if attempt < max_retries:
                time.sleep(min(retry_base_seconds * (2 ** (attempt - 1)), 8.0))
    assert last_exc is not None
    raise last_exc


def _classifier_context(
    retrieval_sentence: dict[str, Any],
    prior_sentences: list[str],
    rerank_top_k: int,
) -> str:
    blocks: list[str] = []
    for rank, candidate in enumerate(
        retrieval_sentence["reranked_candidates"][:rerank_top_k], start=1
    ):
        idx = int(candidate["prior_index"])
        blocks.append(
            f"### Candidate {rank}\n"
            f"[prior_index={idx}; cosine={float(candidate['cosine']):.6f}; "
            f"rerank_score={float(candidate['rerank_score']):.6f}; "
            f"weighted_jaccard={float(candidate['weighted_jaccard']):.6f}]\n"
            f"{prior_sentences[idx]}"
        )
    return "\n\n".join(blocks)


def classify_retrieval_pairs(
    *,
    retrieval_jsonl: Path,
    classified_jsonl: Path,
    mdna_root: Path,
    prompt_file: Path,
    classifier_model: str = "gpt-6-luna",
    reasoning_effort: str = "none",
    rerank_top_k: int = 3,
    max_concurrent_requests: int = 4,
    max_retries: int = 3,
    retry_base_seconds: float = 2.0,
    resume: bool = True,
    overwrite: bool = False,
    max_pairs: int | None = None,
    max_sentences: int | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Classify retrieval output one pair at a time; JSONL append is atomic per pair."""
    log = logger or logging.getLogger(__name__)
    if overwrite and classified_jsonl.exists():
        classified_jsonl.unlink()
    completed = _completed_pair_ids(classified_jsonl) if resume else set()

    prompt = prompt_file.read_text(encoding="utf-8")
    from openai import OpenAI

    client = OpenAI()
    pair_count = 0
    sentence_count = 0
    error_count = 0

    for row in _iter_jsonl(retrieval_jsonl) or []:
        if row.get("status") != "ok":
            continue
        curr_doc = str(row["curr_docID"])
        if curr_doc in completed:
            continue
        if max_pairs is not None and pair_count >= int(max_pairs):
            break

        edinet = str(row["edinetCode"])
        prev_doc = str(row["prev_docID"])
        prev_path = _resolve_mdna_path(mdna_root, edinet, prev_doc)
        prior_sentences = split_japanese_sentences(
            _read_text(prev_path),
            min_chars=int(row.get("min_sentence_chars", MIN_SENTENCE_CHARS)),
        )

        retrieval_sentences = list(row.get("sentences", []))
        if max_sentences is not None:
            remaining = int(max_sentences) - sentence_count
            if remaining <= 0:
                break
            retrieval_sentences = retrieval_sentences[:remaining]

        jobs: list[dict[str, Any]] = []
        for sent in retrieval_sentences:
            context = _classifier_context(sent, prior_sentences, rerank_top_k)
            jobs.append(
                {
                    "current_sentence_index": int(sent["current_sentence_index"]),
                    "current_sentence": str(sent["current_sentence"]),
                    "prior_context": context,
                    "classifier_candidate_indices": sent[
                        "classifier_candidate_indices"
                    ][:rerank_top_k],
                }
            )

        def worker(job: dict[str, Any]) -> dict[str, Any]:
            try:
                result = _call_classifier(
                    client=client,
                    model=classifier_model,
                    prompt=prompt,
                    current_sentence=job["current_sentence"],
                    prior_context=job["prior_context"],
                    reasoning_effort=reasoning_effort,
                    max_retries=max_retries,
                    retry_base_seconds=retry_base_seconds,
                )
                return {**job, "status": "ok", **result, "error": ""}
            except Exception as exc:
                return {
                    **job,
                    "status": "error",
                    "label": "",
                    "confidence": "",
                    "reason": "",
                    "input_tokens": None,
                    "output_tokens": None,
                    "total_tokens": None,
                    "error": f"{type(exc).__name__}: {exc}",
                }

        results: list[dict[str, Any]] = []
        with cf.ThreadPoolExecutor(max_workers=max_concurrent_requests) as ex:
            futures = [ex.submit(worker, job) for job in jobs]
            for fut in cf.as_completed(futures):
                results.append(fut.result())

        results.sort(key=lambda x: x["current_sentence_index"])
        pair_errors = sum(r["status"] != "ok" for r in results)
        error_count += pair_errors
        pair_status = "ok" if pair_errors == 0 else "partial_error"

        payload = {
            "status": pair_status,
            "edinetCode": edinet,
            "prev_docID": prev_doc,
            "curr_docID": curr_doc,
            "classifier_model": classifier_model,
            "prompt_file": str(prompt_file),
            "prompt_sha256": _sha256(prompt_file),
            "sentences": results,
        }
        _append_jsonl(payload, classified_jsonl)

        pair_count += 1
        sentence_count += len(results)
        log.info(
            "Stage 6E classify: pair=%d curr=%s sentences=%d errors=%d",
            pair_count,
            curr_doc,
            len(results),
            pair_errors,
        )

        if max_sentences is not None and sentence_count >= int(max_sentences):
            break

    return {
        "pairs_classified": pair_count,
        "sentences_classified": sentence_count,
        "sentence_errors": error_count,
        "classified_jsonl": str(classified_jsonl),
    }


def export_classification_outputs(
    *,
    classified_jsonl: Path,
    sentence_csv: Path,
    document_csv: Path,
) -> dict[str, Any]:
    """Explode pair JSONL into audit-friendly sentence and document CSVs."""
    sentence_rows: list[dict[str, Any]] = []
    document_rows: list[dict[str, Any]] = []

    # Resume can append a replacement record for a previously partial pair.
    # Keep the last record for each current document when exporting.
    latest_pairs: dict[str, dict[str, Any]] = {}
    for pair in _iter_jsonl(classified_jsonl) or []:
        if pair.get("status") not in {"ok", "partial_error"}:
            continue
        latest_pairs[str(pair.get("curr_docID"))] = pair

    for pair in latest_pairs.values():

        persistent: list[str] = []
        novel: list[str] = []
        ok_count = 0
        err_count = 0

        for sent in pair.get("sentences", []):
            row = {
                "edinetCode": pair.get("edinetCode"),
                "prev_docID": pair.get("prev_docID"),
                "curr_docID": pair.get("curr_docID"),
                "currentSentenceIndex": sent.get("current_sentence_index"),
                "currentSentence": sent.get("current_sentence"),
                "label": sent.get("label"),
                "confidence": sent.get("confidence"),
                "reason": sent.get("reason"),
                "candidatePriorIndices": ",".join(
                    str(x) for x in sent.get("classifier_candidate_indices", [])
                ),
                "status": sent.get("status"),
                "inputTokens": sent.get("input_tokens"),
                "outputTokens": sent.get("output_tokens"),
                "totalTokens": sent.get("total_tokens"),
                "error": sent.get("error"),
            }
            sentence_rows.append(row)

            if sent.get("status") == "ok":
                ok_count += 1
                if sent.get("label") == "persistent":
                    persistent.append(str(sent.get("current_sentence", "")))
                elif sent.get("label") == "novel":
                    novel.append(str(sent.get("current_sentence", "")))
            else:
                err_count += 1

        persistent_text = "\n".join(persistent)
        novel_text = "\n".join(novel)
        persistent_chars = sum(len(s) for s in persistent)
        novel_chars = sum(len(s) for s in novel)
        classified_chars = persistent_chars + novel_chars

        document_rows.append(
            {
                "edinetCode": pair.get("edinetCode"),
                "prev_docID": pair.get("prev_docID"),
                "curr_docID": pair.get("curr_docID"),
                "status": "ok" if err_count == 0 else "partial_error",
                "classifiedSentenceCount": ok_count,
                "errorSentenceCount": err_count,
                "persistentSentenceCount": len(persistent),
                "novelSentenceCount": len(novel),
                "persistentChars": persistent_chars,
                "novelChars": novel_chars,
                "persistentShareChars": (
                    persistent_chars / classified_chars if classified_chars else math.nan
                ),
                "novelShareChars": (
                    novel_chars / classified_chars if classified_chars else math.nan
                ),
                "persistentText": persistent_text,
                "novelText": novel_text,
            }
        )

    sentence_df = pd.DataFrame(sentence_rows)
    document_df = pd.DataFrame(document_rows)
    sentence_csv.parent.mkdir(parents=True, exist_ok=True)
    document_csv.parent.mkdir(parents=True, exist_ok=True)
    sentence_df.to_csv(sentence_csv, index=False, encoding="utf-8-sig")
    document_df.to_csv(document_csv, index=False, encoding="utf-8-sig")

    return {
        "sentence_rows": int(len(sentence_df)),
        "document_rows": int(len(document_df)),
        "sentence_csv": str(sentence_csv),
        "document_csv": str(document_csv),
    }


def run_persistent_novel_split(
    *,
    pairs_csv: Path,
    mdna_root: Path,
    concepts_csv: Path,
    idf_cache: Path,
    prompt_file: Path,
    output_dir: Path,
    retriever_model: str = "cl-nagoya/ruri-v3-310m",
    retrieval_top_k: int = 10,
    rerank_top_k: int = 3,
    rerank_lambda: float = 0.02,
    min_sentence_chars: int = MIN_SENTENCE_CHARS,
    embedding_batch_size: int = 32,
    device: str = "auto",
    classifier_model: str = "gpt-6-luna",
    reasoning_effort: str = "none",
    max_concurrent_requests: int = 4,
    max_retries: int = 3,
    retry_base_seconds: float = 2.0,
    resume: bool = True,
    overwrite: bool = False,
    retrieval_only: bool = True,
    max_pairs: int | None = None,
    max_sentences: int | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run Stage 6E retrieval, and optionally classification + aggregation."""
    log = logger or logging.getLogger(__name__)
    output_dir.mkdir(parents=True, exist_ok=True)

    retrieval_jsonl = output_dir / "retrieval_pairs.jsonl"
    classified_jsonl = output_dir / "classified_pairs.jsonl"
    sentence_csv = output_dir / "persistent_novel_sentences.csv"
    document_csv = output_dir / "persistent_novel_documents.csv"
    metadata_json = output_dir / "persistent_novel_split.metadata.json"

    retrieval_summary = build_retrieval_pairs(
        pairs_csv=pairs_csv,
        mdna_root=mdna_root,
        concepts_csv=concepts_csv,
        idf_cache=idf_cache,
        retrieval_jsonl=retrieval_jsonl,
        retriever_model=retriever_model,
        retrieval_top_k=retrieval_top_k,
        rerank_top_k=rerank_top_k,
        rerank_lambda=rerank_lambda,
        min_sentence_chars=min_sentence_chars,
        embedding_batch_size=embedding_batch_size,
        device=device,
        resume=resume,
        overwrite=overwrite,
        max_pairs=max_pairs,
        max_sentences=max_sentences,
        logger=log,
    )

    payload: dict[str, Any] = {
        "stage": "persistent_novel_split",
        "generated_at": pd.Timestamp.now().isoformat(),
        "pairs_csv": str(pairs_csv),
        "mdna_root": str(mdna_root),
        "concepts_csv": str(concepts_csv),
        "idf_cache": str(idf_cache),
        "prompt_file": str(prompt_file),
        "prompt_sha256": _sha256(prompt_file) if prompt_file.exists() else None,
        "retriever_model": retriever_model,
        "retrieval_top_k": retrieval_top_k,
        "rerank_top_k": rerank_top_k,
        "rerank_variant": "cosine_plus_idf_jaccard",
        "rerank_lambda": rerank_lambda,
        "classifier_model": classifier_model,
        "reasoning_effort": reasoning_effort,
        "retrieval_only": retrieval_only,
        "retrieval": retrieval_summary,
    }

    if retrieval_only:
        _atomic_json(payload, metadata_json)
        return payload

    classify_summary = classify_retrieval_pairs(
        retrieval_jsonl=retrieval_jsonl,
        classified_jsonl=classified_jsonl,
        mdna_root=mdna_root,
        prompt_file=prompt_file,
        classifier_model=classifier_model,
        reasoning_effort=reasoning_effort,
        rerank_top_k=rerank_top_k,
        max_concurrent_requests=max_concurrent_requests,
        max_retries=max_retries,
        retry_base_seconds=retry_base_seconds,
        resume=resume,
        overwrite=overwrite,
        max_pairs=max_pairs,
        max_sentences=max_sentences,
        logger=log,
    )
    export_summary = export_classification_outputs(
        classified_jsonl=classified_jsonl,
        sentence_csv=sentence_csv,
        document_csv=document_csv,
    )
    payload["classification"] = classify_summary
    payload["exports"] = export_summary
    _atomic_json(payload, metadata_json)
    return payload
