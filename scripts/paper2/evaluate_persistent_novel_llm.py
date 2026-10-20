#!/usr/bin/env python3
"""
Benchmark an LLM on persistent-vs-novel classification using the completed
alignment gold workbook.

This first end-to-end benchmark intentionally uses the candidate sentence text
already stored in the workbook. That isolates classifier quality from any new
retrieval/context changes.

Example:
python scripts/paper2/evaluate_persistent_novel_llm.py \
  --benchmark japanese_mdna_alignment_gold_completed.xlsx \
  --prompt configs/paper2/prompts/persistent_novel_classifier_v1.md \
  --model gpt-6-luna \
  --candidate-model ruri \
  --output-dir data/interim/paper2/alignment/persistent_novel_eval \
  --max-concurrent-requests 4 \
  --resume
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
import time
from pathlib import Path

import pandas as pd
from openai import OpenAI


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


def build_context(row: pd.Series, model: str, max_candidates: int) -> str:
    blocks = []
    for rank in range(1, max_candidates + 1):
        idx = clean(row.get(f"{model}_candidate_{rank}_index"))
        sent = clean(row.get(f"{model}_candidate_{rank}_sentence"))
        cosine = clean(row.get(f"{model}_candidate_{rank}_cosine"))
        if not sent:
            continue
        blocks.append(
            f"### Candidate {rank}\n"
            f"[prior_index={idx}; cosine={cosine}]\n"
            f"{sent}"
        )
    return "\n\n".join(blocks)


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
        "persistent_f1": float(per_class.loc[per_class["class"].eq("persistent"), "f1"].iloc[0]),
        "novel_f1": float(per_class.loc[per_class["class"].eq("novel"), "f1"].iloc[0]),
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--benchmark", type=Path, required=True)
    ap.add_argument("--prompt", type=Path, required=True)
    ap.add_argument("--model", default="gpt-6-luna")
    ap.add_argument("--candidate-model", choices=["ruri", "sarashina"], default="ruri")
    ap.add_argument("--max-candidates", type=int, default=3)
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

    xls = pd.ExcelFile(args.benchmark)
    sheet = "Gold Annotation" if "Gold Annotation" in xls.sheet_names else "Annotation"
    df = pd.read_excel(args.benchmark, sheet_name=sheet)

    df["manual_label"] = df["manual_label"].astype(str).str.lower().str.strip()
    df = df[df["manual_label"].isin(["persistent", "novel"])].copy()
    if args.max_rows is not None:
        df = df.head(args.max_rows).copy()

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

        context = build_context(row, args.candidate_model, args.max_candidates)

        jobs.append({
            "annotation_id": aid,
            "company": clean(row["company"]),
            "current_sentence": clean(row["current_sentence"]),
            "manual_label": clean(row["manual_label"]).lower(),
            "manual_confidence": clean(row.get("manual_confidence")),
            "prior_context": context,
        })

    print(f"Rows to classify: {len(df):,}")
    print(f"Candidate model: {args.candidate_model}")
    print(f"Model: {args.model}")

    if args.dry_run:
        for job in jobs[:5]:
            print("\n" + "=" * 80)
            print(job["annotation_id"], "gold=", job["manual_label"])
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
        with cf.ThreadPoolExecutor(max_workers=args.max_concurrent_requests) as ex:
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
                    pred_path, index=False, encoding="utf-8-sig"
                )

    pred = pd.DataFrame(output_rows)
    order = {str(aid): i for i, aid in enumerate(df["annotation_id"].astype(str))}
    pred["_order"] = pred["annotation_id"].astype(str).map(order)
    pred = pred.sort_values("_order").drop(columns="_order").reset_index(drop=True)
    pred.to_csv(pred_path, index=False, encoding="utf-8-sig")

    summary, per_class, cm = score(pred)

    summary.insert(0, "model", args.model)
    summary.insert(1, "candidate_model", args.candidate_model)
    summary.insert(2, "max_candidates", args.max_candidates)

    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    per_class.to_csv(class_path, index=False, encoding="utf-8-sig")
    cm.to_csv(cm_path, index=False, encoding="utf-8-sig")

    errors = pred[
        pred["status"].eq("ok")
        & pred["manual_label"].ne(pred["predicted_label"])
    ].copy()
    errors.to_csv(errors_path, index=False, encoding="utf-8-sig")

    print("\n=== Persistent vs Novel LLM benchmark ===")
    print(summary.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\nPer class:")
    print(per_class.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\nConfusion matrix:")
    print(cm.to_string(index=False))

    print(f"\npredictions: {pred_path}")
    print(f"summary:     {summary_path}")
    print(f"by class:    {class_path}")
    print(f"confusion:   {cm_path}")
    print(f"errors:      {errors_path}")


if __name__ == "__main__":
    main()
