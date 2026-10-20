#!/usr/bin/env python3
"""
End-to-end persistent-vs-novel benchmark using Ruri Top-K candidates reranked
with corpus-IDF-weighted EDINET/IFRS concept overlap before GPT classification.

This is intentionally separate from evaluate_persistent_novel_llm.py so the
validated raw-Ruri benchmark remains unchanged.

Expected workflow
-----------------
1. Use evaluate_concept_reranking.py to select a reranking specification based
   on retrieval metrics (especially Recall@3 because the classifier receives
   three candidates).
2. Run this script with that fixed variant/lambda.
3. Compare its classification metrics against the frozen raw-Ruri Top-3 v1.1
   benchmark.

Example
-------
python scripts/paper2/evaluate_persistent_novel_llm_reranked.py \
  --benchmark data/manual/paper2/alignment/japanese_mdna_alignment_gold_completed_top10.xlsx \
  --concepts data/interim/paper2/alignment/edinet_concept_dictionary.csv \
  --idf-cache data/interim/paper2/alignment/concept_alias_idf.csv \
  --prompt configs/paper2/prompts/persistent_novel_classifier_v1_1.md \
  --model gpt-6-luna \
  --candidate-model ruri \
  --retrieval-top-k 10 \
  --max-candidates 3 \
  --rerank-variant cosine_plus_idf_jaccard \
  --rerank-lambda 0.02 \
  --output-dir data/interim/paper2/alignment/persistent_novel_eval_v1_1_idf \
  --max-concurrent-requests 4
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
from openai import OpenAI

# Allow imports from this scripts/paper2 directory when invoked from repo root.
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from evaluate_concept_reranking import (  # noqa: E402
    AliasMatcher,
    load_alias_table,
    load_or_build_idf,
    load_prior_sentences,
    norm_text,
    parse_csv_numbers,
    rank_candidates,
    select_canonical_splitter,
    weighted_overlap_features,
)


def clean(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def extract_output_text(response) -> str:
    text = getattr(response, "output_text", None)
    if text:
        return text
    pieces = []
    for item in getattr(response, "output", []) or []:
        for content in getattr(item, "content", []) or []:
            t = getattr(content, "text", None)
            if t:
                pieces.append(t)
    return "\n".join(pieces)


def parse_json(text: str) -> dict:
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

    label = clean(obj.get("label")).lower()
    confidence = clean(obj.get("confidence")).lower()
    reason = clean(obj.get("reason"))

    if label not in {"persistent", "novel"}:
        raise ValueError(f"Invalid label: {label!r}")
    if confidence not in {"high", "mid", "low"}:
        raise ValueError(f"Invalid confidence: {confidence!r}")

    return {"label": label, "confidence": confidence, "reason": reason}


def call_model(
    client: OpenAI,
    model: str,
    prompt: str,
    current_sentence: str,
    prior_context: str,
    reasoning_effort: str,
    max_retries: int,
):
    user_input = (
        "## CURRENT-YEAR SENTENCE\n"
        f"{current_sentence}\n\n"
        "## RETRIEVED PRIOR-YEAR CANDIDATES\n"
        f"{prior_context}\n"
    )

    kwargs = {
        "model": model,
        "instructions": prompt,
        "input": user_input,
    }
    if reasoning_effort != "none":
        kwargs["reasoning"] = {"effort": reasoning_effort}

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            response = client.responses.create(**kwargs)
            parsed = parse_json(extract_output_text(response))
            usage = getattr(response, "usage", None)
            return {
                **parsed,
                "input_tokens": getattr(usage, "input_tokens", None) if usage else None,
                "output_tokens": getattr(usage, "output_tokens", None) if usage else None,
                "total_tokens": getattr(usage, "total_tokens", None) if usage else None,
            }
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                time.sleep(min(2 ** (attempt - 1), 8))
    raise last_exc


def class_metrics(df: pd.DataFrame, positive: str) -> dict:
    y = df["manual_label"]
    p = df["predicted_label"]

    tp = int(((y == positive) & (p == positive)).sum())
    fp = int(((y != positive) & (p == positive)).sum())
    fn = int(((y == positive) & (p != positive)).sum())
    tn = int(((y != positive) & (p != positive)).sum())

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    return {
        "class": positive,
        "support": int((y == positive).sum()),
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def score(pred: pd.DataFrame):
    ok = pred[pred["status"].eq("ok")].copy()
    if ok.empty:
        raise ValueError("No successful predictions.")

    ok["manual_label"] = ok["manual_label"].str.lower()
    ok["predicted_label"] = ok["predicted_label"].str.lower()

    per_class = pd.DataFrame([
        class_metrics(ok, "persistent"),
        class_metrics(ok, "novel"),
    ])

    accuracy = float((ok["manual_label"] == ok["predicted_label"]).mean())

    summary = pd.DataFrame([{
        "n_scored": len(ok),
        "accuracy": accuracy,
        "macro_precision": float(per_class["precision"].mean()),
        "macro_recall": float(per_class["recall"].mean()),
        "macro_f1": float(per_class["f1"].mean()),
        "persistent_f1": float(
            per_class.loc[per_class["class"].eq("persistent"), "f1"].iloc[0]
        ),
        "novel_f1": float(
            per_class.loc[per_class["class"].eq("novel"), "f1"].iloc[0]
        ),
    }])

    cm = pd.crosstab(
        ok["manual_label"],
        ok["predicted_label"],
        rownames=["actual"],
        colnames=["predicted"],
        dropna=False,
    ).reindex(
        index=["persistent", "novel"],
        columns=["persistent", "novel"],
        fill_value=0,
    ).reset_index()

    return summary, per_class, cm


def build_reranked_context(
    row: pd.Series,
    *,
    company: str,
    prior_sentences: dict[str, list[str]],
    matcher: AliasMatcher,
    idf_map: dict[str, float],
    candidate_model: str,
    retrieval_top_k: int,
    max_candidates: int,
    rerank_variant: str,
    rerank_lambda: float,
) -> tuple[str, str, str]:
    idx_col = f"{candidate_model}_topk_prior_indices"
    cos_col = f"{candidate_model}_topk_cosines"

    indices = parse_csv_numbers(row.get(idx_col), int)[:retrieval_top_k]
    cosines = parse_csv_numbers(row.get(cos_col), float)[:retrieval_top_k]

    if not indices:
        raise ValueError(
            f"{row.get('annotation_id')}: no candidates in {idx_col}"
        )
    if len(indices) != len(cosines):
        raise ValueError(
            f"{row.get('annotation_id')}: candidate index/cosine length mismatch "
            f"({len(indices)} vs {len(cosines)})"
        )
    if company not in prior_sentences:
        raise KeyError(f"No reconstructed prior sentences for company={company!r}")

    current = clean(row["current_sentence"])
    current_aliases = matcher.match(current)

    records = []
    for baseline_rank, (idx, cosine) in enumerate(
        zip(indices, cosines), start=1
    ):
        if idx < 0 or idx >= len(prior_sentences[company]):
            raise IndexError(
                f"{row.get('annotation_id')}: prior index {idx} out of range"
            )

        sentence = prior_sentences[company][idx]
        candidate_aliases = matcher.match(sentence)
        features = weighted_overlap_features(
            current_aliases=current_aliases,
            candidate_aliases=candidate_aliases,
            idf_map=idf_map,
        )

        records.append({
            "prior_index": idx,
            "prior_sentence": sentence,
            "cosine": float(cosine),
            "baseline_rank": baseline_rank,
            **features,
        })

    candidates = pd.DataFrame(records)
    ranked = rank_candidates(
        candidates,
        rerank_variant,
        rerank_lambda,
    )

    top = ranked.head(max_candidates).copy()

    blocks = []
    for rank, r in enumerate(top.itertuples(index=False), start=1):
        blocks.append(
            f"### Candidate {rank}\n"
            f"[prior_index={int(r.prior_index)}; cosine={float(r.cosine):.6f}; "
            f"rerank_score={float(r.rerank_score):.6f}; "
            f"weighted_jaccard={float(r.weighted_jaccard):.6f}; "
            f"weighted_containment={float(r.weighted_containment):.6f}]\n"
            f"{r.prior_sentence}"
        )

    baseline_indices = ",".join(str(int(x)) for x in indices)
    reranked_indices = ",".join(
        str(int(x)) for x in ranked["prior_index"].tolist()
    )

    return "\n\n".join(blocks), baseline_indices, reranked_indices


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--benchmark", type=Path, required=True)
    ap.add_argument("--concepts", type=Path, required=True)
    ap.add_argument("--idf-cache", type=Path, required=True)
    ap.add_argument("--idf-corpus", type=Path, default=None)
    ap.add_argument("--idf-glob", default="*.txt")
    ap.add_argument("--rebuild-idf", action="store_true")
    ap.add_argument("--min-alias-chars", type=int, default=2)
    ap.add_argument("--repo-root", type=Path, default=None)

    ap.add_argument("--prompt", type=Path, required=True)
    ap.add_argument("--model", default="gpt-6-luna")
    ap.add_argument(
        "--candidate-model",
        choices=["ruri", "sarashina"],
        default="ruri",
    )
    ap.add_argument("--retrieval-top-k", type=int, default=10)
    ap.add_argument("--max-candidates", type=int, default=3)

    ap.add_argument(
        "--rerank-variant",
        choices=[
            "cosine",
            "idf_jaccard_first",
            "idf_containment_first",
            "cosine_plus_idf_jaccard",
            "cosine_plus_idf_containment",
        ],
        default="cosine_plus_idf_jaccard",
    )
    ap.add_argument("--rerank-lambda", type=float, default=0.02)

    ap.add_argument("--max-concurrent-requests", type=int, default=4)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument(
        "--reasoning-effort",
        choices=["none", "low", "medium", "high"],
        default="none",
    )
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--max-rows", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")

    args = ap.parse_args()

    repo_root = (args.repo_root or Path.cwd()).resolve()

    xls = pd.ExcelFile(args.benchmark)
    sheet = (
        "Gold Annotation"
        if "Gold Annotation" in xls.sheet_names
        else "Annotation"
    )
    df = pd.read_excel(args.benchmark, sheet_name=sheet)
    metadata = pd.read_excel(args.benchmark, sheet_name="Metadata")

    df["manual_label"] = df["manual_label"].astype(str).str.lower().str.strip()
    df = df[df["manual_label"].isin(["persistent", "novel"])].copy()
    if args.max_rows is not None:
        df = df.head(args.max_rows).copy()

    concepts = pd.read_csv(args.concepts)

    splitter_label, splitter = select_canonical_splitter(
        benchmark_path=args.benchmark,
        metadata=metadata,
        repo_root=repo_root,
    )
    prior = load_prior_sentences(
        benchmark_path=args.benchmark,
        metadata=metadata,
        repo_root=repo_root,
        splitter=splitter,
    )
    print(f"Validated/reconstructed prior sentences with: {splitter_label}")

    alias_table = load_alias_table(
        concepts=concepts,
        min_alias_chars=args.min_alias_chars,
    )
    matcher = AliasMatcher(alias_table)

    idf = load_or_build_idf(
        alias_table=alias_table,
        matcher=matcher,
        corpus_root=args.idf_corpus,
        corpus_glob=args.idf_glob,
        cache_path=args.idf_cache,
        rebuild=args.rebuild_idf,
    )
    idf_map = dict(
        zip(idf["alias"].astype(str), idf["idf"].astype(float))
    )

    prompt = args.prompt.read_text(encoding="utf-8")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    pred_path = args.output_dir / "persistent_novel_predictions.csv"
    summary_path = args.output_dir / "persistent_novel_metrics_summary.csv"
    class_path = args.output_dir / "persistent_novel_metrics_by_class.csv"
    cm_path = args.output_dir / "persistent_novel_confusion_matrix.csv"
    errors_path = args.output_dir / "persistent_novel_errors.csv"

    existing = {}
    if args.resume and pred_path.exists():
        old = pd.read_csv(pred_path)
        for _, r in old[old["status"].eq("ok")].iterrows():
            existing[str(r["annotation_id"])] = r.to_dict()
        print(f"Resume: {len(existing):,} successful rows loaded.")

    jobs = []
    output_rows = []

    for _, row in df.iterrows():
        aid = clean(row["annotation_id"])

        if aid in existing:
            output_rows.append(existing[aid])
            continue

        company = clean(row["company"])
        context, baseline_indices, reranked_indices = build_reranked_context(
            row,
            company=company,
            prior_sentences=prior,
            matcher=matcher,
            idf_map=idf_map,
            candidate_model=args.candidate_model,
            retrieval_top_k=args.retrieval_top_k,
            max_candidates=args.max_candidates,
            rerank_variant=args.rerank_variant,
            rerank_lambda=args.rerank_lambda,
        )

        jobs.append({
            "annotation_id": aid,
            "company": company,
            "current_sentence": clean(row["current_sentence"]),
            "manual_label": clean(row["manual_label"]).lower(),
            "manual_confidence": clean(row.get("manual_confidence")),
            "prior_context": context,
            "candidate_indices_baseline": baseline_indices,
            "candidate_indices_reranked": reranked_indices,
            "rerank_variant": args.rerank_variant,
            "rerank_lambda": args.rerank_lambda,
        })

    print(f"Rows to classify: {len(df):,}")
    print(f"Candidate model: {args.candidate_model}")
    print(f"Retrieval Top-K pool: {args.retrieval_top_k}")
    print(f"Candidates sent to classifier: {args.max_candidates}")
    print(f"Rerank variant: {args.rerank_variant}")
    print(f"Rerank lambda: {args.rerank_lambda}")
    print(f"Classifier model: {args.model}")

    if args.dry_run:
        for job in jobs[:5]:
            print("\n" + "=" * 80)
            print(job["annotation_id"], "gold=", job["manual_label"])
            print("baseline:", job["candidate_indices_baseline"])
            print("reranked:", job["candidate_indices_reranked"])
            print(job["current_sentence"])
            print(job["prior_context"])
        return

    client = OpenAI()

    def worker(job):
        try:
            result = call_model(
                client=client,
                model=args.model,
                prompt=prompt,
                current_sentence=job["current_sentence"],
                prior_context=job["prior_context"],
                reasoning_effort=args.reasoning_effort,
                max_retries=args.max_retries,
            )
            return {
                **job,
                "status": "ok",
                "predicted_label": result["label"],
                "predicted_confidence": result["confidence"],
                "predicted_reason": result["reason"],
                "input_tokens": result["input_tokens"],
                "output_tokens": result["output_tokens"],
                "total_tokens": result["total_tokens"],
                "error": "",
            }
        except Exception as exc:
            return {
                **job,
                "status": "error",
                "predicted_label": "",
                "predicted_confidence": "",
                "predicted_reason": "",
                "input_tokens": None,
                "output_tokens": None,
                "total_tokens": None,
                "error": f"{type(exc).__name__}: {exc}",
            }

    total = len(jobs)
    if total:
        with cf.ThreadPoolExecutor(
            max_workers=args.max_concurrent_requests
        ) as ex:
            futures = [ex.submit(worker, job) for job in jobs]

            for i, fut in enumerate(cf.as_completed(futures), start=1):
                result = fut.result()
                output_rows.append(result)

                print(
                    f"[{i}/{total}] {result['annotation_id']} "
                    f"gold={result['manual_label']} "
                    f"pred={result['predicted_label'] or 'ERROR'}"
                )

                pd.DataFrame(output_rows).to_csv(
                    pred_path,
                    index=False,
                    encoding="utf-8-sig",
                )

    pred = pd.DataFrame(output_rows)
    order = {
        str(aid): i
        for i, aid in enumerate(df["annotation_id"].astype(str))
    }
    pred["_order"] = pred["annotation_id"].astype(str).map(order)
    pred = (
        pred.sort_values("_order")
        .drop(columns="_order")
        .reset_index(drop=True)
    )
    pred.to_csv(pred_path, index=False, encoding="utf-8-sig")

    summary, per_class, cm = score(pred)

    summary.insert(0, "model", args.model)
    summary.insert(1, "candidate_model", args.candidate_model)
    summary.insert(2, "retrieval_top_k", args.retrieval_top_k)
    summary.insert(3, "max_candidates", args.max_candidates)
    summary.insert(4, "rerank_variant", args.rerank_variant)
    summary.insert(5, "rerank_lambda", args.rerank_lambda)

    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    per_class.to_csv(class_path, index=False, encoding="utf-8-sig")
    cm.to_csv(cm_path, index=False, encoding="utf-8-sig")

    errors = pred[
        pred["status"].eq("ok")
        & pred["manual_label"].ne(pred["predicted_label"])
    ].copy()
    errors.to_csv(errors_path, index=False, encoding="utf-8-sig")

    print("\n=== Persistent vs Novel LLM benchmark: IDF-reranked ===")
    print(
        summary.to_string(
            index=False,
            float_format=lambda x: f"{x:.4f}",
        )
    )
    print("\nPer class:")
    print(
        per_class.to_string(
            index=False,
            float_format=lambda x: f"{x:.4f}",
        )
    )
    print("\nConfusion matrix:")
    print(cm.to_string(index=False))

    print(f"\npredictions: {pred_path}")
    print(f"summary:     {summary_path}")
    print(f"by class:    {class_path}")
    print(f"confusion:   {cm_path}")
    print(f"errors:      {errors_path}")


if __name__ == "__main__":
    main()
