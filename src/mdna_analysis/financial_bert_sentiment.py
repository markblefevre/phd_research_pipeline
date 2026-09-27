"""Japanese Financial BERT sentiment scoring for Paper 2.

Design
------
The stage deliberately separates the pretrained financial-language backbone
(`izumi-lab/bert-base-japanese-fin-additional`) from the sentiment task.
The raw Izumi checkpoint is *not* itself a sentiment classifier and is never
used directly to generate sentiment scores.

Following Nakatsuka & Suimon (2024), the production sentiment model consists
of two independently fine-tuned binary classifiers derived from the Izumi
backbone:

* positive classifier: sentence contains at least one positive chABSA opinion;
* negative classifier: sentence contains at least one negative chABSA opinion.

Each MD&A is split into sentences.  The two classifiers independently label
those sentences.  The literature-aligned primary document score is

    bertNet = (positive_sentence_count - negative_sentence_count) / sentence_count

A sentence may therefore be positive, negative, both, or neither.  Mean class
probabilities are also retained as diagnostics / robustness variables.

The research universe is inherited from the validated Stage 4 manifest, but
original Stage 2 MD&A text is retokenized with the BERT model's own Japanese
MeCab/IPADIC + WordPiece tokenizer.  Sudachi tokens are not reused for BERT.

Long sentences are *not truncated*.  They are divided into contiguous model
pieces no longer than the model maximum. Piece-level probabilities are
weighted by content-token count and aggregated back to the original source
sentence before applying the binary decision threshold.

The corpus scorer is resumable. Completed document blocks are written to a
partial CSV together with a run signature. A resumed run refuses to combine
results generated under different models or inference settings.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

DOC_KEY = ["edinetCode", "docID"]
VALID_MANIFEST_STATUSES = {"success", "skipped_existing"}
DEFAULT_BACKBONE = "izumi-lab/bert-base-japanese-fin-additional"

# Split after common Japanese/ASCII sentence-final punctuation or line breaks.
# The punctuation remains attached to the preceding sentence because the split
# position is after the punctuation mark.
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[。！？!?])\s*|\n+")
_SPACE_RE = re.compile(r"[ \t\u3000]+")


@dataclass(frozen=True)
class BinaryLabelIds:
    absent: int
    present: int

    def validate(self, num_labels: int) -> None:
        if self.absent == self.present:
            raise ValueError("Binary class IDs must be distinct")
        if min(self.absent, self.present) < 0 or max(self.absent, self.present) >= num_labels:
            raise ValueError(
                f"Binary class IDs absent={self.absent}, present={self.present} "
                f"invalid for num_labels={num_labels}"
            )


@dataclass(frozen=True)
class InferenceSettings:
    backbone_model: str
    tokenizer_model: str
    positive_model: str
    negative_model: str
    universe_variant: str
    segmentation: str
    min_chars: int
    max_length: int
    threshold: float
    inference_batch_size: int


@dataclass
class _SentenceAccumulator:
    weighted_positive_probability: float = 0.0
    weighted_negative_probability: float = 0.0
    weight: int = 0
    piece_count: int = 0


@dataclass
class _DocumentAccumulator:
    source_char_count: int = 0
    sentence_count: int = 0
    model_piece_count: int = 0
    overlong_sentence_count: int = 0
    content_token_count: int = 0
    unknown_token_count: int = 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _lazy_import_ml():
    try:
        import torch
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError("financial_bert_sentiment requires PyTorch") from exc
    try:
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "financial_bert_sentiment requires transformers. Install torch, "
            "transformers, fugashi, and ipadic before running the BERT stage."
        ) from exc
    return torch, AutoTokenizer, AutoModelForSequenceClassification


def choose_device(torch: Any, requested: str = "auto") -> str:
    requested = str(requested).strip().lower()
    if requested != "auto":
        return requested
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    if torch.cuda.is_available():
        return "cuda"
    return "cpu"


def split_sentences(text: str, *, min_chars: int = 2) -> list[str]:
    """Split Japanese disclosure text into source sentences.

    Empty fragments and extremely short formatting remnants are discarded.
    If the text is nonempty but no fragment clears ``min_chars``, the entire
    stripped text is retained as one source sentence so the document is not
    silently lost.
    """
    text = str(text).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return []
    parts = _SENTENCE_SPLIT_RE.split(text)
    out: list[str] = []
    for part in parts:
        cleaned = _SPACE_RE.sub(" ", part).strip()
        if len(cleaned) >= int(min_chars):
            out.append(cleaned)
    return out or [text]


def _normalise_label_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def resolve_binary_label_ids(
    config: Any,
    *,
    absent_id: int | None = None,
    present_id: int | None = None,
) -> BinaryLabelIds:
    """Resolve absent/present IDs without silently guessing generic labels."""
    num_labels = int(getattr(config, "num_labels", 0))
    if num_labels != 2:
        raise ValueError(f"Expected a binary classifier (num_labels=2), found {num_labels}")

    if absent_id is not None or present_id is not None:
        if absent_id is None or present_id is None:
            raise ValueError("Configure both absent_id and present_id, or neither")
        result = BinaryLabelIds(int(absent_id), int(present_id))
        result.validate(num_labels)
        return result

    raw = getattr(config, "id2label", None) or {}
    id2label = {int(k): _normalise_label_name(v) for k, v in raw.items()}
    absent_names = {"absent", "false", "no", "negativeclass", "class0"}
    present_names = {"present", "true", "yes", "positiveclass", "class1"}
    absent_hits = [idx for idx, label in id2label.items() if label in absent_names]
    present_hits = [idx for idx, label in id2label.items() if label in present_names]
    if len(absent_hits) == 1 and len(present_hits) == 1:
        result = BinaryLabelIds(absent_hits[0], present_hits[0])
        result.validate(num_labels)
        return result

    raise ValueError(
        "Could not safely infer binary class IDs from model.config.id2label="
        f"{raw!r}. Paper 2 checkpoints should use 0='absent', 1='present'. "
        "For an external checkpoint with generic LABEL_0/LABEL_1, configure "
        "absent_id and present_id explicitly."
    )


def _read_research_universe(manifest_csv: Path, variant: str) -> pd.DataFrame:
    manifest = pd.read_csv(
        manifest_csv,
        dtype={"edinetCode": "string", "docID": "string", "status": "string", "variant": "string"},
        low_memory=False,
    )
    required = {*DOC_KEY, "status", "variant"}
    missing = required - set(manifest.columns)
    if missing:
        raise ValueError(f"{manifest_csv} missing columns: {sorted(missing)}")
    manifest = manifest.loc[manifest["variant"].astype(str).eq(str(variant))].copy()
    manifest = manifest.loc[
        manifest["status"].astype(str).str.lower().isin(VALID_MANIFEST_STATUSES)
    ].copy()
    if manifest.empty:
        raise ValueError(f"No valid rows for variant={variant!r} in {manifest_csv}")
    if manifest.duplicated(DOC_KEY).any():
        raise ValueError(
            f"Manifest contains {int(manifest.duplicated(DOC_KEY).sum()):,} duplicate document keys"
        )
    if manifest[DOC_KEY].isna().any().any():
        raise ValueError("Manifest contains missing edinetCode/docID")
    return manifest[DOC_KEY].reset_index(drop=True)


def _model_ref_is_raw_backbone(model_ref: str, backbone_ref: str) -> bool:
    return str(model_ref).rstrip("/") == str(backbone_ref).rstrip("/")


def _simple_file_fingerprint(path: Path) -> str:
    """Cheap local-checkpoint fingerprint for resume protection.

    Hash model config/tokenizer metadata and incorporate weight filename/size/mtime
    without reading hundreds of MB of weights on every run.
    """
    h = hashlib.sha256()
    for name in ("config.json", "tokenizer_config.json", "special_tokens_map.json"):
        p = path / name
        if p.exists():
            h.update(name.encode())
            h.update(p.read_bytes())
    for pattern in ("*.safetensors", "pytorch_model*.bin"):
        for p in sorted(path.glob(pattern)):
            st = p.stat()
            h.update(f"{p.name}:{st.st_size}:{st.st_mtime_ns}".encode())
    return h.hexdigest()


def _model_fingerprint(ref: str) -> str:
    p = Path(str(ref)).expanduser()
    if p.exists() and p.is_dir():
        return f"local:{p.resolve()}:{_simple_file_fingerprint(p)}"
    return f"hf:{ref}"


def _run_signature(settings: InferenceSettings) -> dict[str, Any]:
    data = asdict(settings)
    data["positiveModelFingerprint"] = _model_fingerprint(settings.positive_model)
    data["negativeModelFingerprint"] = _model_fingerprint(settings.negative_model)
    payload = json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")
    data["signatureSha256"] = hashlib.sha256(payload).hexdigest()
    return data


def _prepare_piece_features(tokenizer: Any, content_ids: Sequence[int]) -> dict[str, list[int]]:
    # Some Japanese BERT tokenizers/models do not need token_type_ids, but the
    # tokenizer knows what the underlying model normally accepts. Generating
    # them here is harmless for BERT and keeps the feature construction generic.
    return tokenizer.prepare_for_model(
        list(content_ids),
        add_special_tokens=True,
        truncation=False,
        return_attention_mask=True,
        return_token_type_ids=True,
    )


def _tokenize_sentence_pieces(
    tokenizer: Any,
    sentences: Sequence[str],
    *,
    max_content_tokens: int,
) -> tuple[list[tuple[int, list[int]]], int, int, int]:
    """Return (sentence_index, piece_ids) without truncating source sentences."""
    if not sentences:
        return [], 0, 0, 0
    encoded = tokenizer(
        list(sentences),
        add_special_tokens=False,
        padding=False,
        truncation=False,
        return_attention_mask=False,
        return_token_type_ids=False,
    )
    all_ids = encoded["input_ids"]
    if all_ids and isinstance(all_ids[0], int):
        all_ids = [all_ids]

    unk_id = getattr(tokenizer, "unk_token_id", None)
    pieces: list[tuple[int, list[int]]] = []
    content_tokens = 0
    unknown_tokens = 0
    overlong = 0
    for sentence_idx, ids in enumerate(all_ids):
        ids = list(ids)
        if not ids:
            continue
        content_tokens += len(ids)
        if unk_id is not None:
            unknown_tokens += sum(1 for x in ids if int(x) == int(unk_id))
        if len(ids) > max_content_tokens:
            overlong += 1
        for start in range(0, len(ids), max_content_tokens):
            piece = ids[start : start + max_content_tokens]
            if piece:
                pieces.append((sentence_idx, piece))
    return pieces, content_tokens, unknown_tokens, overlong


def _softmax_present_probability(torch: Any, logits: Any, present_id: int) -> np.ndarray:
    probs = torch.softmax(logits, dim=-1)[:, int(present_id)]
    return probs.detach().float().cpu().numpy()


def _score_document_block(
    docs: pd.DataFrame,
    *,
    mdna_root: Path,
    tokenizer: Any,
    positive_model: Any,
    negative_model: Any,
    positive_labels: BinaryLabelIds,
    negative_labels: BinaryLabelIds,
    torch: Any,
    device: str,
    min_chars: int,
    max_length: int,
    threshold: float,
    inference_batch_size: int,
) -> list[dict[str, Any]]:
    special_tokens = int(tokenizer.num_special_tokens_to_add(pair=False))
    max_content_tokens = int(max_length) - special_tokens
    if max_content_tokens < 8:
        raise ValueError(
            f"max_length={max_length} leaves only {max_content_tokens} content tokens"
        )

    # Keep one source-sentence accumulator per document so overlong sentences
    # can be reconstructed after piece-level inference.
    doc_meta: list[_DocumentAccumulator] = []
    sentence_accumulators: list[list[_SentenceAccumulator]] = []
    feature_records: list[tuple[int, int, int, dict[str, list[int]]]] = []

    for doc_idx, r in enumerate(docs.itertuples(index=False)):
        edinet = str(r.edinetCode)
        docid = str(r.docID)
        path = mdna_root / edinet / f"{docid}.txt"
        if not path.exists():
            raise FileNotFoundError(f"Missing MD&A source text: {path}")
        text = path.read_text(encoding="utf-8")
        sentences = split_sentences(text, min_chars=min_chars)
        if not sentences:
            raise ValueError(f"Empty MD&A after sentence splitting: {path}")

        pieces, token_count, unk_count, overlong = _tokenize_sentence_pieces(
            tokenizer, sentences, max_content_tokens=max_content_tokens
        )
        if not pieces:
            raise ValueError(f"Tokenizer produced zero model pieces: {path}")

        meta = _DocumentAccumulator(
            source_char_count=len(text),
            sentence_count=len(sentences),
            model_piece_count=len(pieces),
            overlong_sentence_count=overlong,
            content_token_count=token_count,
            unknown_token_count=unk_count,
        )
        doc_meta.append(meta)
        sentence_accumulators.append([_SentenceAccumulator() for _ in sentences])
        for sentence_idx, piece_ids in pieces:
            feature_records.append(
                (
                    doc_idx,
                    sentence_idx,
                    len(piece_ids),
                    _prepare_piece_features(tokenizer, piece_ids),
                )
            )

    positive_model.eval()
    negative_model.eval()
    with torch.inference_mode():
        for start in range(0, len(feature_records), int(inference_batch_size)):
            batch_records = feature_records[start : start + int(inference_batch_size)]
            features = [x[3] for x in batch_records]
            batch = tokenizer.pad(features, padding=True, return_tensors="pt")
            batch = {k: v.to(device) for k, v in batch.items()}

            positive_logits = positive_model(**batch).logits
            negative_logits = negative_model(**batch).logits
            pos_probs = _softmax_present_probability(torch, positive_logits, positive_labels.present)
            neg_probs = _softmax_present_probability(torch, negative_logits, negative_labels.present)

            for rec, pos_prob, neg_prob in zip(batch_records, pos_probs, neg_probs):
                doc_idx, sentence_idx, weight, _ = rec
                acc = sentence_accumulators[doc_idx][sentence_idx]
                acc.weighted_positive_probability += float(pos_prob) * int(weight)
                acc.weighted_negative_probability += float(neg_prob) * int(weight)
                acc.weight += int(weight)
                acc.piece_count += 1

    rows: list[dict[str, Any]] = []
    for doc_idx, r in enumerate(docs.itertuples(index=False)):
        sent_accs = sentence_accumulators[doc_idx]
        pos_probs: list[float] = []
        neg_probs: list[float] = []
        for sidx, acc in enumerate(sent_accs):
            if acc.weight <= 0:
                # A nonempty sentence producing no tokens is unusual and should
                # not silently change N in the literature-aligned score.
                raise ValueError(
                    f"Sentence {sidx} in {r.edinetCode}/{r.docID} produced zero content tokens"
                )
            pos_probs.append(acc.weighted_positive_probability / acc.weight)
            neg_probs.append(acc.weighted_negative_probability / acc.weight)

        pos_flags = np.asarray(pos_probs) >= float(threshold)
        neg_flags = np.asarray(neg_probs) >= float(threshold)
        n = len(pos_probs)
        m_pos = int(pos_flags.sum())
        m_neg = int(neg_flags.sum())
        m_both = int(np.logical_and(pos_flags, neg_flags).sum())
        m_neither = int(np.logical_and(~pos_flags, ~neg_flags).sum())
        m_pos_only = int(np.logical_and(pos_flags, ~neg_flags).sum())
        m_neg_only = int(np.logical_and(~pos_flags, neg_flags).sum())
        meta = doc_meta[doc_idx]

        rows.append(
            {
                "edinetCode": str(r.edinetCode),
                "docID": str(r.docID),
                "sourceCharCount": int(meta.source_char_count),
                "bertSentenceCount": int(n),
                "bertModelPieceCount": int(meta.model_piece_count),
                "bertOverlongSentenceCount": int(meta.overlong_sentence_count),
                "bertContentTokenCount": int(meta.content_token_count),
                "bertUnknownTokenCount": int(meta.unknown_token_count),
                "bertUnknownTokenRate": (
                    float(meta.unknown_token_count / meta.content_token_count)
                    if meta.content_token_count
                    else math.nan
                ),
                "bertPositiveSentenceCount": m_pos,
                "bertNegativeSentenceCount": m_neg,
                "bertPositiveOnlySentenceCount": m_pos_only,
                "bertNegativeOnlySentenceCount": m_neg_only,
                "bertBothSentenceCount": m_both,
                "bertNeitherSentenceCount": m_neither,
                "bertPositiveSentenceRate": float(m_pos / n),
                "bertNegativeSentenceRate": float(m_neg / n),
                "bertBothSentenceRate": float(m_both / n),
                "bertNeitherSentenceRate": float(m_neither / n),
                # Primary literature-aligned score (Nakatsuka & Suimon 2024).
                "bertNet": float((m_pos - m_neg) / n),
                # Probability-based secondary measures preserve classifier
                # confidence without replacing the published hard-label score.
                "bertMeanPositiveProbability": float(np.mean(pos_probs)),
                "bertMeanNegativeProbability": float(np.mean(neg_probs)),
                "bertProbabilityNet": float(np.mean(pos_probs) - np.mean(neg_probs)),
            }
        )
    return rows


def _safe_summary(series: pd.Series) -> dict[str, float]:
    q = series.quantile([0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99])
    return {
        "mean": float(series.mean()),
        "std": float(series.std()),
        "min": float(series.min()),
        "p01": float(q.loc[0.01]),
        "p05": float(q.loc[0.05]),
        "p10": float(q.loc[0.10]),
        "p25": float(q.loc[0.25]),
        "median": float(q.loc[0.50]),
        "p75": float(q.loc[0.75]),
        "p90": float(q.loc[0.90]),
        "p95": float(q.loc[0.95]),
        "p99": float(q.loc[0.99]),
        "max": float(series.max()),
    }


def _atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False, encoding="utf-8")
    os.replace(tmp, path)


def _atomic_write_json(obj: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def score_financial_bert_corpus(
    *,
    manifest_csv: Path,
    mdna_root: Path,
    output_csv: Path,
    metadata_json: Path | None,
    positive_model_ref: str,
    negative_model_ref: str,
    backbone_model: str = DEFAULT_BACKBONE,
    tokenizer_model_ref: str | None = None,
    universe_variant: str = "sudachi_c_raw",
    min_chars: int = 2,
    max_length: int = 512,
    threshold: float = 0.5,
    inference_batch_size: int = 32,
    document_block_size: int = 16,
    device: str = "auto",
    positive_absent_id: int | None = None,
    positive_present_id: int | None = None,
    negative_absent_id: int | None = None,
    negative_present_id: int | None = None,
    resume: bool = True,
    overwrite: bool = False,
    max_documents: int | None = None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Score the frozen Paper 2 document universe with two binary BERT models."""
    logger = logger or logging.getLogger(__name__)
    manifest_csv = Path(manifest_csv).expanduser().resolve()
    mdna_root = Path(mdna_root).expanduser().resolve()
    output_csv = Path(output_csv).expanduser().resolve()
    metadata_json = (
        Path(metadata_json).expanduser().resolve()
        if metadata_json is not None
        else output_csv.with_suffix(".metadata.json")
    )

    if not manifest_csv.exists():
        raise FileNotFoundError(manifest_csv)
    if not mdna_root.exists():
        raise FileNotFoundError(mdna_root)
    if _model_ref_is_raw_backbone(positive_model_ref, backbone_model) or _model_ref_is_raw_backbone(
        negative_model_ref, backbone_model
    ):
        raise ValueError(
            "The raw Izumi financial BERT checkpoint is a pretrained backbone, not a sentiment "
            "classifier. Configure two fine-tuned binary checkpoints (positive and negative)."
        )
    if str(positive_model_ref) == str(negative_model_ref):
        raise ValueError("positive_model_ref and negative_model_ref must be distinct checkpoints")
    if not 0.0 < float(threshold) < 1.0:
        raise ValueError("threshold must lie strictly between 0 and 1")
    if int(inference_batch_size) < 1 or int(document_block_size) < 1:
        raise ValueError("batch sizes must be >= 1")

    universe = _read_research_universe(manifest_csv, universe_variant)
    if max_documents is not None:
        universe = universe.head(int(max_documents)).copy()
    universe = universe.reset_index(drop=True)

    tokenizer_model_ref = str(tokenizer_model_ref or positive_model_ref)
    settings = InferenceSettings(
        backbone_model=str(backbone_model),
        tokenizer_model=str(tokenizer_model_ref),
        positive_model=str(positive_model_ref),
        negative_model=str(negative_model_ref),
        universe_variant=str(universe_variant),
        segmentation="sentence",
        min_chars=int(min_chars),
        max_length=int(max_length),
        threshold=float(threshold),
        inference_batch_size=int(inference_batch_size),
    )
    signature = _run_signature(settings)

    partial_csv = output_csv.with_suffix(".partial.csv")
    partial_meta = output_csv.with_suffix(".partial.metadata.json")
    existing = pd.DataFrame()

    if output_csv.exists() and not overwrite:
        completed = pd.read_csv(
            output_csv, dtype={"edinetCode": "string", "docID": "string"}, low_memory=False
        )
        if len(completed) == len(universe) and not completed.duplicated(DOC_KEY).any():
            merged = universe.merge(completed[DOC_KEY], on=DOC_KEY, how="left", indicator=True)
            if merged["_merge"].eq("both").all():
                logger.info("Financial BERT output already complete; returning existing file")
                return completed
        raise FileExistsError(
            f"Output exists but is not a complete match to the requested universe: {output_csv}. "
            "Use overwrite=true to rebuild."
        )

    if overwrite:
        for p in (output_csv, metadata_json, partial_csv, partial_meta):
            if p.exists():
                p.unlink()
    elif resume and partial_csv.exists():
        if not partial_meta.exists():
            raise RuntimeError(f"Partial output exists without resume metadata: {partial_csv}")
        prior_meta = json.loads(partial_meta.read_text(encoding="utf-8"))
        prior_sig = prior_meta.get("runSignature", {})
        if prior_sig.get("signatureSha256") != signature.get("signatureSha256"):
            raise RuntimeError(
                "Refusing to resume Financial BERT scoring because model/settings signature changed. "
                "Use overwrite=true to start a clean run."
            )
        existing = pd.read_csv(
            partial_csv, dtype={"edinetCode": "string", "docID": "string"}, low_memory=False
        )
        if existing.duplicated(DOC_KEY).any():
            raise RuntimeError("Partial BERT output contains duplicate document keys")
        logger.info("Resuming Financial BERT scoring from %s completed documents", len(existing))

    done_keys = set(zip(existing.get("edinetCode", []), existing.get("docID", [])))
    todo_mask = [
        (str(e), str(d)) not in done_keys
        for e, d in zip(universe["edinetCode"].astype(str), universe["docID"].astype(str))
    ]
    todo = universe.loc[todo_mask].reset_index(drop=True)

    torch, AutoTokenizer, AutoModelForSequenceClassification = _lazy_import_ml()
    chosen_device = choose_device(torch, device)
    logger.info("Loading Financial BERT tokenizer from %s", tokenizer_model_ref)
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_model_ref, use_fast=False)

    model_max = getattr(tokenizer, "model_max_length", None)
    # Hugging Face uses huge sentinel values for effectively-unbounded tokenizers.
    if model_max is not None and int(model_max) < 1_000_000 and int(max_length) > int(model_max):
        raise ValueError(
            f"Configured max_length={max_length} exceeds tokenizer model_max_length={model_max}"
        )

    logger.info("Loading positive classifier from %s", positive_model_ref)
    positive_model = AutoModelForSequenceClassification.from_pretrained(positive_model_ref)
    logger.info("Loading negative classifier from %s", negative_model_ref)
    negative_model = AutoModelForSequenceClassification.from_pretrained(negative_model_ref)
    positive_labels = resolve_binary_label_ids(
        positive_model.config, absent_id=positive_absent_id, present_id=positive_present_id
    )
    negative_labels = resolve_binary_label_ids(
        negative_model.config, absent_id=negative_absent_id, present_id=negative_present_id
    )
    positive_model.to(chosen_device)
    negative_model.to(chosen_device)

    rows: list[dict[str, Any]] = existing.to_dict("records") if not existing.empty else []
    total = len(universe)
    completed_before = len(rows)

    for start in range(0, len(todo), int(document_block_size)):
        block = todo.iloc[start : start + int(document_block_size)].copy()
        block_rows = _score_document_block(
            block,
            mdna_root=mdna_root,
            tokenizer=tokenizer,
            positive_model=positive_model,
            negative_model=negative_model,
            positive_labels=positive_labels,
            negative_labels=negative_labels,
            torch=torch,
            device=chosen_device,
            min_chars=int(min_chars),
            max_length=int(max_length),
            threshold=float(threshold),
            inference_batch_size=int(inference_batch_size),
        )
        rows.extend(block_rows)
        current = pd.DataFrame(rows)
        if current.duplicated(DOC_KEY).any():
            raise RuntimeError("Internal error: duplicate document keys after BERT scoring block")
        _atomic_write_csv(current, partial_csv)
        _atomic_write_json(
            {
                "updatedAt": _utc_now(),
                "runSignature": signature,
                "device": chosen_device,
                "requestedDocumentCount": total,
                "completedDocumentCount": len(current),
            },
            partial_meta,
        )
        logger.info(
            "Financial BERT progress: %s/%s documents (%.1f%%)",
            len(current),
            total,
            100.0 * len(current) / total,
        )

    out = pd.DataFrame(rows)
    if len(out) != total:
        raise RuntimeError(f"Expected {total:,} BERT rows, produced {len(out):,}")
    if out.duplicated(DOC_KEY).any():
        raise RuntimeError("BERT output contains duplicate document keys")

    joined = universe.merge(out, on=DOC_KEY, how="left", validate="one_to_one")
    score_cols = [
        "bertSentenceCount",
        "bertPositiveSentenceRate",
        "bertNegativeSentenceRate",
        "bertNet",
        "bertMeanPositiveProbability",
        "bertMeanNegativeProbability",
        "bertProbabilityNet",
    ]
    if joined[score_cols].isna().any().any():
        raise RuntimeError("BERT output has missing required score values after universe join")
    out = joined

    metadata = {
        "createdAt": _utc_now(),
        "stage": "financial_bert_sentiment",
        "method": "dual_binary_sentence_classifier",
        "primaryScoreDefinition": "(positiveSentenceCount - negativeSentenceCount) / sentenceCount",
        "literatureAlignment": "Nakatsuka & Suimon (2024)",
        "backboneModel": str(backbone_model),
        "tokenizerModel": str(tokenizer_model_ref),
        "positiveModel": str(positive_model_ref),
        "negativeModel": str(negative_model_ref),
        "runSignature": signature,
        "universeVariant": str(universe_variant),
        "documentCount": int(len(out)),
        "device": chosen_device,
        "maxLength": int(max_length),
        "threshold": float(threshold),
        "inferenceBatchSize": int(inference_batch_size),
        "documentBlockSize": int(document_block_size),
        "completedBeforeResume": int(completed_before),
        "totalSourceSentences": int(out["bertSentenceCount"].sum()),
        "totalModelPieces": int(out["bertModelPieceCount"].sum()),
        "overlongSentenceCount": int(out["bertOverlongSentenceCount"].sum()),
        "totalContentTokens": int(out["bertContentTokenCount"].sum()),
        "totalUnknownTokens": int(out["bertUnknownTokenCount"].sum()),
        "meanUnknownTokenRate": float(out["bertUnknownTokenRate"].mean()),
        "meanPositiveSentenceRate": float(out["bertPositiveSentenceRate"].mean()),
        "meanNegativeSentenceRate": float(out["bertNegativeSentenceRate"].mean()),
        "meanBothSentenceRate": float(out["bertBothSentenceRate"].mean()),
        "meanNeitherSentenceRate": float(out["bertNeitherSentenceRate"].mean()),
        "bertNetDistribution": _safe_summary(out["bertNet"]),
        "bertProbabilityNetDistribution": _safe_summary(out["bertProbabilityNet"]),
    }

    _atomic_write_csv(out, output_csv)
    _atomic_write_json(metadata, metadata_json)
    for p in (partial_csv, partial_meta):
        if p.exists():
            p.unlink()
    return out


__all__ = [
    "DEFAULT_BACKBONE",
    "BinaryLabelIds",
    "InferenceSettings",
    "choose_device",
    "resolve_binary_label_ids",
    "score_financial_bert_corpus",
    "split_sentences",
]
