#!/usr/bin/env python3
"""
Benchmark pair-level Luna classification for Stage 6E.

Experimental architecture:
    one firm-year pair = one Luna request

Each request contains:
  * every prior-year MD&A sentence, indexed P0..Pn
  * every current-year MD&A sentence, indexed C0..Cm
  * top-3 reranked prior indices for every current sentence
  * one shared persistent/novel instruction

The model returns one label for every current sentence. Only the 150 manually
labeled Toyota/MUFG benchmark sentences are scored.

This is benchmark-only code. It does NOT modify production Stage 6E outputs.

Default behavior is PREVIEW ONLY. Add --run to make the two Luna API calls.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from pathlib import Path

import pandas as pd
from openai import OpenAI


DEFAULT_PAIRS = {
    "Toyota": ("S100LO6W", "S100OC13"),
    "MUFG": ("S100LT95", "S100OG6F"),
}


def clean(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def normalize_sentence(text: str) -> str:
    text = str(text).replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def split_japanese_sentences(text: str, min_chars: int = 12) -> list[str]:
    """Canonical conservative splitter used by the alignment benchmark."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    chunks = re.split(r"(?<=[。！？!?])|\n+", text)
    sentences = [normalize_sentence(x) for x in chunks]
    return [x for x in sentences if len(x) >= min_chars]


def read_mdna(mdna_root: Path, doc_id: str) -> str:
    candidates = [
        mdna_root / f"{doc_id}.txt",
        mdna_root / doc_id / f"{doc_id}.txt",
        mdna_root / doc_id,
    ]
    for p in candidates:
        if p.is_file():
            return p.read_text(encoding="utf-8-sig", errors="replace")
    hits = list(mdna_root.rglob(f"{doc_id}.txt"))
    if hits:
        return hits[0].read_text(encoding="utf-8-sig", errors="replace")
    raise FileNotFoundError(f"Could not find {doc_id} under {mdna_root}")


def find_retrieval_pairs(path: Path, wanted_curr_ids: set[str]) -> dict[str, dict]:
    found: dict[str, dict] = {}
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            obj = json.loads(line)
            curr = str(obj.get("curr_docID", ""))
            if curr in wanted_curr_ids:
                found[curr] = obj
                if len(found) == len(wanted_curr_ids):
                    break
    missing = wanted_curr_ids - set(found)
    if missing:
        raise ValueError(f"Could not find retrieval records for: {sorted(missing)}")
    return found


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


def parse_json_object(text: str) -> dict:
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", s, flags=re.S)
        if not m:
            raise
        return json.loads(m.group(0))


def validate_predictions(obj: dict, expected_indices: set[int]) -> list[dict]:
    rows = obj.get("classifications")
    if not isinstance(rows, list):
        raise ValueError("Response must contain a 'classifications' array.")

    parsed = []
    seen = set()
    for row in rows:
        idx = int(row["current_index"])
        label = clean(row.get("label")).lower()
        confidence = clean(row.get("confidence")).lower()
        reason = clean(row.get("reason"))

        if idx in seen:
            raise ValueError(f"Duplicate current_index C{idx}")
        if label not in {"persistent", "novel"}:
            raise ValueError(f"C{idx}: invalid label {label!r}")
        if confidence not in {"high", "mid", "low"}:
            raise ValueError(f"C{idx}: invalid confidence {confidence!r}")

        seen.add(idx)
        parsed.append({
            "current_sentence_index": idx,
            "predicted_label": label,
            "predicted_confidence": confidence,
            "predicted_reason": reason,
        })

    missing = expected_indices - seen
    extra = seen - expected_indices
    if missing or extra:
        raise ValueError(
            f"Response coverage mismatch: missing={sorted(missing)[:20]} "
            f"extra={sorted(extra)[:20]}"
        )
    return parsed


def build_pair_input(
    company: str,
    pair: dict,
    prior_sentences: list[str],
) -> tuple[str, list[dict]]:
    current_items = sorted(
        pair["sentences"],
        key=lambda x: int(x["current_sentence_index"]),
    )

    prior_block = "\n".join(
        f"P{i}\t{s}" for i, s in enumerate(prior_sentences)
    )
    current_block = "\n".join(
        f"C{int(x['current_sentence_index'])}\t{x['current_sentence']}"
        for x in current_items
    )
    retrieval_block = "\n".join(
        "C{} -> {}".format(
            int(x["current_sentence_index"]),
            ",".join(
                f"P{int(c['prior_index'])}"
                for c in x["reranked_candidates"][:3]
            ),
        )
        for x in current_items
    )

    user_input = f"""## FIRM-YEAR PAIR
Company: {company}
Prior document: {pair['prev_docID']}
Current document: {pair['curr_docID']}

## PRIOR-YEAR MD&A
Each sentence appears exactly once and is identified by P-index.

{prior_block}

## CURRENT-YEAR MD&A
Classify every C-index below.

{current_block}

## RETRIEVAL MAP
For each current sentence, these are the three prior-year sentences selected
by the frozen Ruri + IDF reranking system. Use them as the primary comparison
evidence. The complete indexed documents above are available for surrounding
context and disambiguation.

{retrieval_block}
"""
    return user_input, current_items


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
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": precision, "recall": recall, "f1": f1,
    }


def score(df: pd.DataFrame):
    per_class = pd.DataFrame([
        class_metrics(df, "persistent"),
        class_metrics(df, "novel"),
    ])
    summary = pd.DataFrame([{
        "n_scored": len(df),
        "accuracy": float((df["manual_label"] == df["predicted_label"]).mean()),
        "macro_precision": float(per_class["precision"].mean()),
        "macro_recall": float(per_class["recall"].mean()),
        "macro_f1": float(per_class["f1"].mean()),
        "persistent_f1": float(per_class.loc[
            per_class["class"].eq("persistent"), "f1"
        ].iloc[0]),
        "novel_f1": float(per_class.loc[
            per_class["class"].eq("novel"), "f1"
        ].iloc[0]),
    }])
    cm = pd.crosstab(
        df["manual_label"], df["predicted_label"],
        rownames=["actual"], colnames=["predicted"], dropna=False,
    ).reindex(
        index=["persistent", "novel"],
        columns=["persistent", "novel"],
        fill_value=0,
    ).reset_index()
    return summary, per_class, cm


def token_count(text: str) -> int | None:
    try:
        import tiktoken
        enc = tiktoken.get_encoding("o200k_base")
        return len(enc.encode(text))
    except Exception:
        return None


def call_pair(
    client: OpenAI,
    model: str,
    instructions: str,
    user_input: str,
    expected_indices: set[int],
    reasoning_effort: str,
    max_retries: int,
):
    kwargs = {
        "model": model,
        "instructions": instructions,
        "input": user_input,
    }
    if reasoning_effort != "none":
        kwargs["reasoning"] = {"effort": reasoning_effort}

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            response = client.responses.create(**kwargs)
            obj = parse_json_object(extract_output_text(response))
            rows = validate_predictions(obj, expected_indices)
            usage = getattr(response, "usage", None)
            return rows, {
                "input_tokens": getattr(usage, "input_tokens", None) if usage else None,
                "output_tokens": getattr(usage, "output_tokens", None) if usage else None,
                "total_tokens": getattr(usage, "total_tokens", None) if usage else None,
            }, extract_output_text(response)
        except Exception as exc:
            last_exc = exc
            print(f"Attempt {attempt}/{max_retries} failed: {exc}", flush=True)
            if attempt < max_retries:
                time.sleep(min(2 ** (attempt - 1), 8))
    raise last_exc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--benchmark", type=Path, default=Path(
        "data/manual/paper2/alignment/japanese_mdna_alignment_gold_completed_top10.xlsx"
    ))
    ap.add_argument("--retrieval-jsonl", type=Path, default=Path(
        "data/interim/paper2/alignment/persistent_novel/retrieval_pairs.jsonl"
    ))
    ap.add_argument("--mdna-root", type=Path, default=Path("~/paper2_stage4/mdna"))
    ap.add_argument("--prompt", type=Path, default=Path(
        "configs/paper2/prompts/persistent_novel_pair_classifier_test_v1.md"
    ))
    ap.add_argument("--model", default="gpt-6-luna")
    ap.add_argument("--reasoning-effort", choices=["none","low","medium","high"], default="none")
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--output-dir", type=Path, default=Path(
        "data/interim/paper2/alignment/persistent_novel_pair_test_v1"
    ))
    ap.add_argument(
        "--run", action="store_true",
        help="Actually call Luna. Without this flag the script only builds/validates previews."
    )
    args = ap.parse_args()

    args.mdna_root = args.mdna_root.expanduser()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    xls = pd.ExcelFile(args.benchmark)
    sheet = "Gold Annotation" if "Gold Annotation" in xls.sheet_names else "Annotation"
    gold = pd.read_excel(args.benchmark, sheet_name=sheet)
    gold["manual_label"] = gold["manual_label"].astype(str).str.lower().str.strip()
    gold = gold[gold["manual_label"].isin(["persistent", "novel"])].copy()
    gold["current_sentence_index"] = pd.to_numeric(
        gold["current_sentence_index"], errors="raise"
    ).astype(int)

    wanted_curr = {curr for _, curr in DEFAULT_PAIRS.values()}
    print(f"Scanning retrieval JSONL for {len(wanted_curr)} benchmark pairs...", flush=True)
    retrieval = find_retrieval_pairs(args.retrieval_jsonl.expanduser(), wanted_curr)

    instructions = args.prompt.read_text(encoding="utf-8-sig")
    all_pair_data = {}

    for company, (prev_id, curr_id) in DEFAULT_PAIRS.items():
        pair = retrieval[curr_id]
        if str(pair["prev_docID"]) != prev_id:
            raise ValueError(
                f"{company}: expected prior {prev_id}, retrieval has {pair['prev_docID']}"
            )

        min_chars = int(pair.get("min_sentence_chars", 12))
        prior_text = read_mdna(args.mdna_root, prev_id)
        prior_sentences = split_japanese_sentences(prior_text, min_chars=min_chars)
        expected_prior = int(pair.get("prior_sentence_count", len(prior_sentences)))
        if len(prior_sentences) != expected_prior:
            raise ValueError(
                f"{company}: reconstructed {len(prior_sentences)} prior sentences "
                f"but retrieval expects {expected_prior}. Stop: sentence indices would be unsafe."
            )

        user_input, current_items = build_pair_input(company, pair, prior_sentences)

        # Verify benchmark sentence indices/text align exactly with retrieval record.
        current_lookup = {
            int(x["current_sentence_index"]): normalize_sentence(x["current_sentence"])
            for x in current_items
        }
        company_gold = gold[gold["company"].astype(str).str.casefold() == company.casefold()].copy()
        if company_gold.empty:
            raise ValueError(f"No gold rows found for {company}")

        for _, row in company_gold.iterrows():
            idx = int(row["current_sentence_index"])
            expected_text = normalize_sentence(row["current_sentence"])
            actual_text = current_lookup.get(idx)
            if actual_text != expected_text:
                raise ValueError(
                    f"{company} C{idx}: benchmark/retrieval sentence mismatch.\n"
                    f"GOLD: {expected_text}\nRETRIEVAL: {actual_text}"
                )

        preview_path = args.output_dir / f"{company.lower()}_pair_prompt_preview.txt"
        preview_path.write_text(
            "=== INSTRUCTIONS ===\n" + instructions +
            "\n\n=== USER INPUT ===\n" + user_input,
            encoding="utf-8",
        )

        input_est = token_count(instructions + "\n" + user_input)
        all_pair_data[company] = {
            "pair": pair,
            "user_input": user_input,
            "current_items": current_items,
            "gold": company_gold,
            "input_estimate": input_est,
        }

        print(
            f"{company}: prior={len(prior_sentences):,} current={len(current_items):,} "
            f"gold={len(company_gold):,} "
            f"estimated_input_tokens={input_est if input_est is not None else 'n/a'}"
        )
        print(f"  preview: {preview_path}")

    print()
    print("Benchmark architecture validation passed.")
    if not args.run:
        print("PREVIEW ONLY: no API calls made.")
        print("Inspect the two *_pair_prompt_preview.txt files, then rerun with --run.")
        return

    client = OpenAI()
    prediction_frames = []
    usage_rows = []

    for company, data in all_pair_data.items():
        expected_indices = {
            int(x["current_sentence_index"]) for x in data["current_items"]
        }
        print(f"\nCalling {args.model} once for {company} ({len(expected_indices)} sentences)...", flush=True)
        rows, usage, raw = call_pair(
            client=client,
            model=args.model,
            instructions=instructions,
            user_input=data["user_input"],
            expected_indices=expected_indices,
            reasoning_effort=args.reasoning_effort,
            max_retries=args.max_retries,
        )

        raw_path = args.output_dir / f"{company.lower()}_raw_response.json"
        raw_path.write_text(raw, encoding="utf-8")

        pred = pd.DataFrame(rows)
        pred["company"] = company
        pred["curr_docID"] = data["pair"]["curr_docID"]
        pred["prev_docID"] = data["pair"]["prev_docID"]

        # Score only manually labeled benchmark rows.
        scored = data["gold"].merge(
            pred,
            on="current_sentence_index",
            how="left",
            validate="one_to_one",
        )
        if scored["predicted_label"].isna().any():
            raise ValueError(f"{company}: missing predictions after merge.")

        prediction_frames.append(scored)
        usage_rows.append({
            "company": company,
            "prev_docID": data["pair"]["prev_docID"],
            "curr_docID": data["pair"]["curr_docID"],
            "current_sentences_classified": len(expected_indices),
            "gold_sentences_scored": len(scored),
            "estimated_input_tokens": data["input_estimate"],
            **usage,
        })
        print(
            f"{company}: input={usage.get('input_tokens')} "
            f"output={usage.get('output_tokens')} total={usage.get('total_tokens')}"
        )

    scored = pd.concat(prediction_frames, ignore_index=True)
    scored["manual_label"] = scored["manual_label"].str.lower()
    scored["predicted_label"] = scored["predicted_label"].str.lower()

    summary, per_class, cm = score(scored)
    errors = scored[scored["manual_label"] != scored["predicted_label"]].copy()

    scored.to_csv(args.output_dir / "pair_level_predictions_gold150.csv", index=False)
    pd.DataFrame(usage_rows).to_csv(args.output_dir / "pair_level_usage.csv", index=False)
    summary.to_csv(args.output_dir / "pair_level_metrics_summary.csv", index=False)
    per_class.to_csv(args.output_dir / "pair_level_metrics_by_class.csv", index=False)
    cm.to_csv(args.output_dir / "pair_level_confusion_matrix.csv", index=False)
    errors.to_csv(args.output_dir / "pair_level_errors.csv", index=False)

    print("\nPAIR-LEVEL BENCHMARK")
    print(summary.to_string(index=False))
    print("\nBy class:")
    print(per_class.to_string(index=False))
    print("\nConfusion matrix:")
    print(cm.to_string(index=False))
    print(f"\nErrors: {len(errors)}/{len(scored)}")
    print(f"Outputs: {args.output_dir}")
    print("\nFrozen sentence-level comparator: accuracy=0.9600, macro-F1=0.9596")


if __name__ == "__main__":
    main()
