#!/usr/bin/env python3
"""
Prepare a gold-alignment annotation workbook for the Paper 2 retrieval benchmark.

The existing benchmark has complete persistent/novel labels, but its
true_prior_sentence_index / true_prior_sentence fields are blank. This script
expands the stored top-k Ruri and Sarashina candidate indices into actual prior
sentence text so the correct prior-year counterpart can be annotated explicitly.

Example
-------
python scripts/paper2/prepare_alignment_gold_annotation.py \
  --benchmark japanese_mdna_retrieval_context_test_completed.xlsx \
  --output japanese_mdna_alignment_gold_candidates.xlsx
"""

from __future__ import annotations

import argparse
import importlib.util
import inspect
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd


def norm_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return unicodedata.normalize("NFKC", str(value)).strip()


def norm_match(value) -> str:
    return re.sub(r"\s+", "", norm_text(value))


def parse_csv_numbers(value, cast=int) -> list:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    return [cast(x.strip()) for x in str(value).split(",") if x.strip()]


def _load_module_from_path(path: Path):
    module_name = f"_alignment_gold_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def find_splitter(repo_root: Path):
    candidates = [
        repo_root / "scripts/paper2/build_alignment_benchmark.py",
        repo_root / "scripts/paper2/retrieve_alignment_candidates.py",
    ]
    for path in candidates:
        if not path.exists():
            continue
        module = _load_module_from_path(path)
        if hasattr(module, "split_japanese_sentences"):
            return f"{path.name}:split_japanese_sentences", module.split_japanese_sentences
    raise FileNotFoundError(
        "Could not find split_japanese_sentences in the benchmark scripts."
    )


def resolve_prior_path(raw_path: str, benchmark_path: Path, repo_root: Path) -> Path:
    p = Path(str(raw_path)).expanduser()
    if p.exists():
        return p

    candidate = repo_root / p
    if candidate.exists():
        return candidate

    parts = list(p.parts)
    if "data" in parts:
        i = parts.index("data")
        candidate = repo_root.joinpath(*parts[i:])
        if candidate.exists():
            return candidate

    candidate = benchmark_path.parent / p
    if candidate.exists():
        return candidate

    raise FileNotFoundError(f"Could not resolve prior_path={raw_path!r}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--benchmark", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--repo-root", type=Path, default=None)
    args = ap.parse_args()

    repo_root = (args.repo_root or Path.cwd()).resolve()

    ann = pd.read_excel(args.benchmark, sheet_name="Annotation")
    metadata = pd.read_excel(args.benchmark, sheet_name="Metadata")

    splitter_label, splitter = find_splitter(repo_root)
    print(f"Using splitter: {splitter_label}")

    prior_by_company = {}
    for _, row in metadata.iterrows():
        company = norm_text(row["company"])
        path = resolve_prior_path(row["prior_path"], args.benchmark, repo_root)
        text = path.read_text(encoding="utf-8")
        sentences = list(splitter(text))

        expected = int(row["n_prior_sentences"])
        if len(sentences) != expected:
            raise ValueError(
                f"{company}: splitter produced {len(sentences)} sentences; "
                f"Metadata expects {expected}"
            )
        prior_by_company[company] = sentences

    # Validate existing known A1 sentence/index pairs.
    for _, row in ann.iterrows():
        company = norm_text(row["company"])
        prior = prior_by_company[company]
        for idx_col, sent_col in [
            ("ruri_a1_prior_index", "ruri_a1_prior_sentence"),
            ("sarashina_a1_prior_index", "sarashina_a1_prior_sentence"),
            ("tfidf_prior_index", "tfidf_prior_sentence"),
        ]:
            if idx_col not in ann.columns or sent_col not in ann.columns:
                continue
            if pd.isna(row[idx_col]) or pd.isna(row[sent_col]):
                continue
            idx = int(row[idx_col])
            if norm_match(prior[idx]) != norm_match(row[sent_col]):
                raise ValueError(
                    f"{row['annotation_id']}: {idx_col} does not match reconstructed text"
                )

    rows = []
    for _, row in ann.iterrows():
        rec = row.to_dict()
        company = norm_text(row["company"])
        prior = prior_by_company[company]

        ruri_idx = parse_csv_numbers(row.get("ruri_topk_prior_indices"), int)
        ruri_cos = parse_csv_numbers(row.get("ruri_topk_cosines"), float)
        sar_idx = parse_csv_numbers(row.get("sarashina_topk_prior_indices"), int)
        sar_cos = parse_csv_numbers(row.get("sarashina_topk_cosines"), float)

        # Expand model top-k candidates to actual text.
        for model, indices, cosines in [
            ("ruri", ruri_idx, ruri_cos),
            ("sarashina", sar_idx, sar_cos),
        ]:
            for rank in range(1, 4):
                j = rank - 1
                rec[f"{model}_candidate_{rank}_index"] = (
                    indices[j] if j < len(indices) else pd.NA
                )
                rec[f"{model}_candidate_{rank}_cosine"] = (
                    cosines[j] if j < len(cosines) else pd.NA
                )
                rec[f"{model}_candidate_{rank}_sentence"] = (
                    prior[indices[j]] if j < len(indices) else ""
                )

        # Build a deduplicated union of all retrieved candidates.
        union = []
        seen = set()
        for source, indices in [("ruri", ruri_idx), ("sarashina", sar_idx)]:
            for rank, idx in enumerate(indices, start=1):
                if idx not in seen:
                    seen.add(idx)
                    union.append((idx, source, rank, prior[idx]))

        rec["candidate_union"] = "\n\n".join(
            f"[index={idx}; first_source={source}; rank={rank}]\n{sentence}"
            for idx, source, rank, sentence in union
        )

        # Gold fields. Preserve them if already present, otherwise blank.
        rec["true_prior_sentence_index"] = row.get("true_prior_sentence_index", pd.NA)
        rec["true_prior_sentence"] = row.get("true_prior_sentence", "")
        rec["gold_alignment_confidence"] = ""
        rec["gold_alignment_notes"] = ""

        rows.append(rec)

    out_df = pd.DataFrame(rows)

    # Put the annotation-facing columns first.
    front = [
        "annotation_id",
        "company",
        "current_sentence",
        "manual_label",
        "manual_confidence",
        "manual_notes",
        "candidate_union",
        "true_prior_sentence_index",
        "true_prior_sentence",
        "gold_alignment_confidence",
        "gold_alignment_notes",
        "ruri_candidate_1_index",
        "ruri_candidate_1_cosine",
        "ruri_candidate_1_sentence",
        "ruri_candidate_2_index",
        "ruri_candidate_2_cosine",
        "ruri_candidate_2_sentence",
        "ruri_candidate_3_index",
        "ruri_candidate_3_cosine",
        "ruri_candidate_3_sentence",
        "sarashina_candidate_1_index",
        "sarashina_candidate_1_cosine",
        "sarashina_candidate_1_sentence",
        "sarashina_candidate_2_index",
        "sarashina_candidate_2_cosine",
        "sarashina_candidate_2_sentence",
        "sarashina_candidate_3_index",
        "sarashina_candidate_3_cosine",
        "sarashina_candidate_3_sentence",
    ]
    front = [c for c in front if c in out_df.columns]
    rest = [c for c in out_df.columns if c not in front]
    out_df = out_df[front + rest]

    args.output.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="Gold Annotation", index=False)
        metadata.to_excel(writer, sheet_name="Metadata", index=False)

        guide = pd.DataFrame({
            "field": [
                "true_prior_sentence_index",
                "true_prior_sentence",
                "gold_alignment_confidence",
                "gold_alignment_notes",
            ],
            "instruction": [
                "Index of the prior-year sentence that is the best semantic/economic counterpart to the current sentence.",
                "Copy the corresponding prior sentence text. Leave blank only if no defensible counterpart exists.",
                "Suggested values: high / mid / low.",
                "Brief reason, especially when choosing a lower-cosine candidate or when no counterpart exists.",
            ],
        })
        guide.to_excel(writer, sheet_name="Guide", index=False)

        ws = writer.book["Gold Annotation"]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        # Practical widths; long text remains inspectable in Excel.
        widths = {
            "A": 14, "B": 14, "C": 70, "D": 14, "E": 16, "F": 60,
            "G": 100, "H": 22, "I": 70, "J": 20, "K": 60,
        }
        for col, width in widths.items():
            ws.column_dimensions[col].width = width

    print(f"rows={len(out_df):,}")
    print(f"output={args.output}")
    print("Next: annotate true_prior_sentence_index / true_prior_sentence, then rerun evaluate_concept_reranking.py")


if __name__ == "__main__":
    main()
