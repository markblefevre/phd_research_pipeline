#!/usr/bin/env python3
"""
Evaluate EDINET/IFRS concept-aware reranking on the 150-row MD&A alignment benchmark.

What this script does
---------------------
1. Loads the completed benchmark workbook.
2. Uses the Metadata sheet to locate each company's prior-year MD&A text.
3. Reconstructs the prior sentence list and validates its indices against the
   benchmark before evaluating anything.
4. Loads the EDINET + IFRS concept/alias dictionary.
5. Detects taxonomy concepts in the current sentence and each existing top-k
   embedding candidate.
6. Re-ranks the existing candidate pool using transparent concept-aware rules.
7. Reports Recall@1, Recall@3, and truncated MRR@3, plus rows fixed/worsened.
8. Writes row-level CSV diagnostics and a summary CSV.

Important limitation
--------------------
The benchmark workbook currently stores only the top-3 embedding candidate
indices/cosines. Therefore concept-aware *reranking of this workbook* can improve
Recall@1 and MRR@3, but Recall@3 cannot change because the candidate set itself
does not change. If concept-aware reranking looks useful, the next production
test should regenerate a larger pool (e.g. top 10) and rerank that pool.

Example
-------
python scripts/paper2/evaluate_concept_reranking.py \
  --benchmark japanese_mdna_retrieval_context_test_completed.xlsx \
  --concepts data/interim/paper2/alignment/edinet_concept_dictionary.csv \
  --output-dir data/interim/paper2/alignment/concept_reranking_eval
"""

from __future__ import annotations

import argparse
import importlib.util
import inspect
import math
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Text normalization / sentence reconstruction
# ---------------------------------------------------------------------------

def norm_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = unicodedata.normalize("NFKC", str(value))
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def norm_match(value) -> str:
    """Aggressive normalization for comparing benchmark sentence strings."""
    s = norm_text(value)
    s = re.sub(r"\s+", "", s)
    return s


def fallback_split_japanese_sentences(text: str) -> list[str]:
    """
    Reconstruct the benchmark sentence units.

    The benchmark was built from normalized MD&A prose, not physical text
    lines. Newlines in extracted EDINET text often reflect HTML/table layout
    rather than linguistic sentence boundaries, so collapse all whitespace
    first and split only after sentence-final punctuation.

    Short headings/table labels that lack final punctuation remain attached to
    the following sentence, matching the benchmark's prose-oriented splitting
    much more closely than line-based splitting.

    The caller still validates both the expected sentence count and known
    indexed sentence strings before evaluation proceeds.
    """
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return []

    parts = re.split(r"(?<=[。！？!?])\s*", text)
    return [norm_text(p) for p in parts if norm_text(p)]



def _load_module_from_path(path: Path):
    module_name = f"_alignment_splitter_{path.stem}"
    spec = importlib.util.spec_from_file_location(
        module_name,
        path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec from {path}")

    module = importlib.util.module_from_spec(spec)

    # Python 3.12 dataclasses (and some other decorators) expect the module
    # to already exist in sys.modules while class definitions execute.
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise

    return module


def discover_canonical_splitters(repo_root: Path) -> list[tuple[str, callable]]:
    """
    Find plausible sentence splitters in the scripts that created/tested the
    benchmark. We only consider module-defined functions whose names contain
    'sentence' or 'split' and that can be called with one text argument.
    """
    candidate_files = [
        repo_root / "scripts/paper2/build_alignment_benchmark.py",
        repo_root / "scripts/paper2/test_retrieval_context.py",
    ]

    found: list[tuple[str, callable]] = []

    for path in candidate_files:
        if not path.exists():
            continue

        module = _load_module_from_path(path)

        for name, func in inspect.getmembers(module, inspect.isfunction):
            if func.__module__ != module.__name__:
                continue

            lname = name.lower()
            if "sentence" not in lname and "split" not in lname:
                continue

            try:
                sig = inspect.signature(func)
            except (TypeError, ValueError):
                continue

            required = [
                p for p in sig.parameters.values()
                if p.default is inspect._empty
                and p.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
            ]
            has_required_kwonly = any(
                p.default is inspect._empty
                and p.kind is inspect.Parameter.KEYWORD_ONLY
                for p in sig.parameters.values()
            )

            if len(required) == 1 and not has_required_kwonly:
                found.append((f"{path.name}:{name}", func))

    return found


def select_canonical_splitter(
    benchmark_path: Path,
    metadata: pd.DataFrame,
    repo_root: Path,
) -> tuple[str, callable]:
    """
    Select the exact splitter empirically.

    A candidate must reproduce *every* Metadata n_prior_sentences count.
    This is safer than guessing punctuation/newline rules.
    """
    candidates = discover_canonical_splitters(repo_root)

    if not candidates:
        raise ValueError(
            "Could not find a plausible sentence splitter in "
            "scripts/paper2/build_alignment_benchmark.py or "
            "scripts/paper2/test_retrieval_context.py."
        )

    # Load each unique prior document once.
    docs: list[tuple[str, str, int]] = []
    for _, row in metadata.iterrows():
        company = norm_text(row["company"])
        prior_path = resolve_prior_path(
            row["prior_path"],
            benchmark_path,
            repo_root,
        )
        raw = prior_path.read_text(encoding="utf-8")
        expected = int(row["n_prior_sentences"])
        docs.append((company, raw, expected))

    passing: list[tuple[str, callable]] = []
    diagnostics = []

    for label, func in candidates:
        counts = []
        ok = True

        for company, raw, expected in docs:
            try:
                result = func(raw)
            except Exception as exc:
                diagnostics.append(
                    f"{label}: raised {type(exc).__name__}: {exc}"
                )
                ok = False
                break

            if not isinstance(result, (list, tuple)):
                diagnostics.append(
                    f"{label}: returned {type(result).__name__}, not list/tuple"
                )
                ok = False
                break

            got = len(result)
            counts.append(f"{company}={got}/{expected}")
            if got != expected:
                ok = False

        diagnostics.append(f"{label}: " + ", ".join(counts))
        if ok:
            passing.append((label, func))

    if len(passing) == 1:
        print(f"Using canonical benchmark splitter: {passing[0][0]}")
        return passing[0]

    if len(passing) > 1:
        print(
            "Multiple splitters reproduce all benchmark sentence counts; "
            f"using {passing[0][0]}"
        )
        return passing[0]

    raise ValueError(
        "No discovered splitter reproduced the benchmark Metadata counts.\n"
        "Candidate diagnostics:\n  " + "\n  ".join(diagnostics)
    )


# ---------------------------------------------------------------------------
# Concept dictionary
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AliasRecord:
    concept_key: str
    source: str
    alias: str


class ConceptMatcher:
    """
    Literal longest-alias concept matcher.

    Aliases are grouped by first character to avoid checking every taxonomy
    alias against every sentence. Matching is deliberately transparent for the
    first benchmark experiment.
    """

    def __init__(self, concepts: pd.DataFrame, min_alias_chars: int = 2):
        required = {"concept_key", "source", "language", "label_normalized"}
        missing = required - set(concepts.columns)
        if missing:
            raise ValueError(f"Concept dictionary missing columns: {sorted(missing)}")

        c = concepts.copy()
        c = c[c["language"].astype(str).str.lower().eq("ja")]
        if "match_eligible" in c.columns:
            c = c[c["match_eligible"].astype(str).str.lower().isin(["true", "1"])]

        records: list[AliasRecord] = []
        seen = set()

        for _, row in c.iterrows():
            alias = norm_match(row["label_normalized"])
            concept_key = norm_text(row["concept_key"])
            source = norm_text(row["source"])
            if len(alias) < min_alias_chars or not concept_key:
                continue
            key = (concept_key, source, alias)
            if key in seen:
                continue
            seen.add(key)
            records.append(AliasRecord(concept_key, source, alias))

        # Longest aliases first. This does not suppress nested concepts; it only
        # makes diagnostics easier to read.
        records.sort(key=lambda r: (-len(r.alias), r.alias, r.concept_key))

        self.by_first: dict[str, list[AliasRecord]] = {}
        for rec in records:
            self.by_first.setdefault(rec.alias[0], []).append(rec)

    def match(self, text: str) -> tuple[set[str], set[str], list[str]]:
        t = norm_match(text)
        if not t:
            return set(), set(), []

        concepts: set[str] = set()
        sources: set[str] = set()
        aliases: list[str] = []
        alias_seen = set()

        for ch in set(t):
            for rec in self.by_first.get(ch, []):
                if rec.alias in t:
                    concepts.add(rec.concept_key)
                    sources.add(rec.source)
                    if rec.alias not in alias_seen:
                        alias_seen.add(rec.alias)
                        aliases.append(rec.alias)

        aliases.sort(key=lambda s: (-len(s), s))
        return concepts, sources, aliases


# ---------------------------------------------------------------------------
# Candidate features / ranking
# ---------------------------------------------------------------------------

def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 0.0
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def containment(a: set[str], b: set[str]) -> float:
    """
    Fraction of current-sentence concepts recovered by candidate.
    This is asymmetric by design.
    """
    if not a:
        return 0.0
    return len(a & b) / len(a)


def parse_csv_numbers(value, cast=float) -> list:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    out = []
    for part in str(value).split(","):
        part = part.strip()
        if part:
            out.append(cast(part))
    return out


def rank_candidates(cands: pd.DataFrame, variant: str, lam: float = 0.05) -> pd.DataFrame:
    x = cands.copy()

    if variant == "cosine":
        x["rerank_score"] = x["cosine"]

    elif variant == "concept_jaccard_first":
        # Transparent "concept-first" diagnostic; intentionally strong.
        x["rerank_score"] = x["concept_jaccard"]

    elif variant == "concept_containment_first":
        x["rerank_score"] = x["concept_containment"]

    elif variant == "cosine_plus_jaccard":
        x["rerank_score"] = x["cosine"] + lam * x["concept_jaccard"]

    elif variant == "cosine_plus_containment":
        x["rerank_score"] = x["cosine"] + lam * x["concept_containment"]

    else:
        raise ValueError(f"Unknown ranking variant: {variant}")

    # Stable tie-break back to cosine, then original embedding rank.
    return x.sort_values(
        ["rerank_score", "cosine", "baseline_rank"],
        ascending=[False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)


def reciprocal_rank(ranked_indices: list[int], gold: int) -> float:
    for rank, idx in enumerate(ranked_indices, start=1):
        if idx == gold:
            return 1.0 / rank
    return 0.0


# ---------------------------------------------------------------------------
# Workbook / prior text loading
# ---------------------------------------------------------------------------

def resolve_prior_path(raw_path: str, benchmark_path: Path, repo_root: Path | None) -> Path:
    p = Path(str(raw_path)).expanduser()
    if p.exists():
        return p

    if repo_root is not None:
        # Metadata may contain repo-relative paths.
        candidate = repo_root / p
        if candidate.exists():
            return candidate

        # Or only a tail such as data/... may be portable.
        parts = list(p.parts)
        if "data" in parts:
            i = parts.index("data")
            candidate = repo_root.joinpath(*parts[i:])
            if candidate.exists():
                return candidate

    # Last attempt relative to workbook.
    candidate = benchmark_path.parent / p
    if candidate.exists():
        return candidate

    raise FileNotFoundError(
        f"Could not resolve prior_path={raw_path!r}. "
        "Use --repo-root if the workbook contains repo-relative paths."
    )


def load_prior_sentences(
    benchmark_path: Path,
    metadata: pd.DataFrame,
    repo_root: Path,
    splitter,
) -> dict[str, list[str]]:
    result = {}

    for _, row in metadata.iterrows():
        company = norm_text(row["company"])
        prior_path = resolve_prior_path(row["prior_path"], benchmark_path, repo_root)
        text = prior_path.read_text(encoding="utf-8")
        sentences = list(splitter(text))

        expected = int(row["n_prior_sentences"])
        if len(sentences) != expected:
            raise ValueError(
                f"{company}: reconstructed {len(sentences)} prior sentences, "
                f"but Metadata says {expected}. "
                "Use the same sentence splitter as the benchmark builder before "
                "evaluating reranking."
            )

        result[company] = sentences

    return result


def validate_sentence_indices(annotation: pd.DataFrame, prior: dict[str, list[str]]) -> None:
    """
    Verify benchmark indices against all sentence strings available in workbook.
    """
    checks = [
        ("true_prior_sentence_index", "true_prior_sentence"),
        ("ruri_a1_prior_index", "ruri_a1_prior_sentence"),
        ("sarashina_a1_prior_index", "sarashina_a1_prior_sentence"),
        ("tfidf_prior_index", "tfidf_prior_sentence"),
    ]

    mismatches = []

    for _, row in annotation.iterrows():
        company = norm_text(row["company"])
        sentences = prior[company]

        for idx_col, text_col in checks:
            if idx_col not in row or text_col not in row:
                continue
            idx_val = row[idx_col]
            text_val = row[text_col]

            if pd.isna(idx_val) or pd.isna(text_val):
                continue

            idx = int(idx_val)
            if idx < 0 or idx >= len(sentences):
                mismatches.append((row["annotation_id"], idx_col, idx, "out_of_range"))
                continue

            if norm_match(sentences[idx]) != norm_match(text_val):
                mismatches.append(
                    (
                        row["annotation_id"],
                        idx_col,
                        idx,
                        f"{sentences[idx][:60]} != {str(text_val)[:60]}",
                    )
                )

    if mismatches:
        sample = "\n".join(map(str, mismatches[:10]))
        raise ValueError(
            f"Sentence reconstruction validation failed for {len(mismatches)} checks.\n"
            f"First mismatches:\n{sample}\n"
            "Do not evaluate reranking until sentence indexing is reconciled."
        )


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_model(
    annotation: pd.DataFrame,
    prior: dict[str, list[str]],
    matcher: ConceptMatcher,
    model: str,
    lambdas: Iterable[float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    topk_idx_col = f"{model}_topk_prior_indices"
    topk_cos_col = f"{model}_topk_cosines"

    variants: list[tuple[str, float | None]] = [
        ("cosine", None),
        ("concept_jaccard_first", None),
        ("concept_containment_first", None),
    ]
    for lam in lambdas:
        variants.append(("cosine_plus_jaccard", lam))
        variants.append(("cosine_plus_containment", lam))

    diag_rows = []
    metric_rows = []

    # Precompute current concepts.
    current_cache = {}
    candidate_cache = {}

    for _, row in annotation.iterrows():
        ann_id = norm_text(row["annotation_id"])
        company = norm_text(row["company"])
        current = norm_text(row["current_sentence"])
        gold_val = row["true_prior_sentence_index"]

        if pd.isna(gold_val):
            continue
        gold = int(gold_val)

        indices = parse_csv_numbers(row[topk_idx_col], int)
        cosines = parse_csv_numbers(row[topk_cos_col], float)

        if not indices or len(indices) != len(cosines):
            continue

        cur_key = (company, current)
        if cur_key not in current_cache:
            current_cache[cur_key] = matcher.match(current)
        cur_concepts, cur_sources, cur_aliases = current_cache[cur_key]

        cand_records = []
        for base_rank, (idx, cosine) in enumerate(zip(indices, cosines), start=1):
            sentence = prior[company][idx]
            ck = (company, idx)
            if ck not in candidate_cache:
                candidate_cache[ck] = matcher.match(sentence)
            cand_concepts, cand_sources, cand_aliases = candidate_cache[ck]

            cand_records.append({
                "prior_index": idx,
                "prior_sentence": sentence,
                "cosine": float(cosine),
                "baseline_rank": base_rank,
                "shared_concept_count": len(cur_concepts & cand_concepts),
                "concept_jaccard": jaccard(cur_concepts, cand_concepts),
                "concept_containment": containment(cur_concepts, cand_concepts),
                "candidate_concept_count": len(cand_concepts),
                "candidate_aliases": " | ".join(cand_aliases[:20]),
            })

        cands = pd.DataFrame(cand_records)

        baseline = rank_candidates(cands, "cosine")
        baseline_indices = baseline["prior_index"].astype(int).tolist()
        baseline_rank1 = baseline_indices[0]
        baseline_rr = reciprocal_rank(baseline_indices, gold)
        gold_in_pool = gold in baseline_indices

        for variant, lam in variants:
            ranked = rank_candidates(cands, variant, 0.0 if lam is None else lam)
            ranked_indices = ranked["prior_index"].astype(int).tolist()
            rank1 = ranked_indices[0]
            rr = reciprocal_rank(ranked_indices, gold)

            label = variant if lam is None else f"{variant}_{lam:g}"

            top = ranked.iloc[0]
            diag_rows.append({
                "annotation_id": ann_id,
                "company": company,
                "model": model,
                "variant": label,
                "current_sentence": current,
                "gold_prior_index": gold,
                "gold_in_candidate_pool": gold_in_pool,
                "baseline_top1_index": baseline_rank1,
                "reranked_top1_index": rank1,
                "baseline_top1_correct": baseline_rank1 == gold,
                "reranked_top1_correct": rank1 == gold,
                "fixed_top1": baseline_rank1 != gold and rank1 == gold,
                "worsened_top1": baseline_rank1 == gold and rank1 != gold,
                "baseline_rr_at3": baseline_rr,
                "reranked_rr_at3": rr,
                "current_concept_count": len(cur_concepts),
                "current_aliases": " | ".join(cur_aliases[:25]),
                "top_candidate_cosine": float(top["cosine"]),
                "top_candidate_shared_concepts": int(top["shared_concept_count"]),
                "top_candidate_jaccard": float(top["concept_jaccard"]),
                "top_candidate_containment": float(top["concept_containment"]),
                "top_candidate_sentence": top["prior_sentence"],
                "candidate_indices_baseline": ",".join(map(str, baseline_indices)),
                "candidate_indices_reranked": ",".join(map(str, ranked_indices)),
            })

    diag = pd.DataFrame(diag_rows)

    if diag.empty:
        return diag, pd.DataFrame()

    for variant, g in diag.groupby("variant", sort=False):
        n = len(g)
        metric_rows.append({
            "model": model,
            "variant": variant,
            "n": n,
            "recall_at_1": float(g["reranked_top1_correct"].mean()),
            "recall_at_3": float(g["gold_in_candidate_pool"].mean()),
            "mrr_at_3": float(g["reranked_rr_at3"].mean()),
            "fixed_top1": int(g["fixed_top1"].sum()),
            "worsened_top1": int(g["worsened_top1"].sum()),
            "net_top1_change": int(g["fixed_top1"].sum() - g["worsened_top1"].sum()),
            "rows_with_current_concepts": int((g["current_concept_count"] > 0).sum()),
            "current_concept_coverage": float((g["current_concept_count"] > 0).mean()),
        })

    return diag, pd.DataFrame(metric_rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--benchmark", type=Path, required=True)
    ap.add_argument("--concepts", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--repo-root", type=Path, default=None)
    ap.add_argument(
        "--models",
        nargs="+",
        default=["ruri", "sarashina"],
        choices=["ruri", "sarashina"],
    )
    ap.add_argument(
        "--lambdas",
        nargs="+",
        type=float,
        default=[0.01, 0.02, 0.05, 0.10],
        help="Exploratory weights for cosine + concept feature variants.",
    )
    ap.add_argument("--min-alias-chars", type=int, default=2)
    args = ap.parse_args()

    xls = pd.ExcelFile(args.benchmark)
    if "Gold Annotation" in xls.sheet_names:
        annotation_sheet = "Gold Annotation"
    elif "Annotation" in xls.sheet_names:
        annotation_sheet = "Annotation"
    else:
        raise ValueError(
            "Benchmark workbook must contain either 'Gold Annotation' "
            "or 'Annotation' sheet. Found: "
            + ", ".join(xls.sheet_names)
        )

    print(f"Using annotation sheet: {annotation_sheet}")
    annotation = pd.read_excel(args.benchmark, sheet_name=annotation_sheet)
    metadata = pd.read_excel(args.benchmark, sheet_name="Metadata")
    concepts = pd.read_csv(args.concepts)

    repo_root = (args.repo_root or Path.cwd()).resolve()

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
    validate_sentence_indices(annotation, prior)
    print(f"Validated sentence indexing with: {splitter_label}")

    matcher = ConceptMatcher(concepts, min_alias_chars=args.min_alias_chars)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    all_diag = []
    all_metrics = []

    for model in args.models:
        diag, metrics = evaluate_model(
            annotation=annotation,
            prior=prior,
            matcher=matcher,
            model=model,
            lambdas=args.lambdas,
        )
        all_diag.append(diag)
        all_metrics.append(metrics)

    diag = pd.concat(all_diag, ignore_index=True)
    metrics = pd.concat(all_metrics, ignore_index=True)

    diag_path = args.output_dir / "concept_reranking_diagnostics.csv"
    metrics_path = args.output_dir / "concept_reranking_summary.csv"
    changed_path = args.output_dir / "concept_reranking_changed_top1.csv"

    diag.to_csv(diag_path, index=False, encoding="utf-8-sig")
    metrics.to_csv(metrics_path, index=False, encoding="utf-8-sig")

    changed = diag[diag["fixed_top1"] | diag["worsened_top1"]].copy()
    changed.to_csv(changed_path, index=False, encoding="utf-8-sig")

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 180)

    print("\n=== Concept-aware reranking summary ===")
    display_cols = [
        "model",
        "variant",
        "n",
        "recall_at_1",
        "recall_at_3",
        "mrr_at_3",
        "fixed_top1",
        "worsened_top1",
        "net_top1_change",
        "current_concept_coverage",
    ]
    print(metrics[display_cols].to_string(index=False, float_format=lambda x: f"{x:.4f}"))

    print("\nNOTE: Recall@3 is unchanged by reranking because the workbook contains only")
    print("the existing top-3 candidate pool. A larger top-k retrieval run is needed")
    print("to test whether concept-aware reranking improves candidate recall itself.")

    print(f"\nsummary:     {metrics_path}")
    print(f"diagnostics: {diag_path}")
    print(f"changed:     {changed_path}")


if __name__ == "__main__":
    main()
