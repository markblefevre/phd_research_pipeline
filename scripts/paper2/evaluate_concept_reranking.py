#!/usr/bin/env python3
"""
Evaluate corpus-IDF-weighted EDINET/IFRS concept reranking.

Changes from the first evaluator
--------------------------------
1. Taxonomy concepts are collapsed to normalized Japanese aliases.
   Multiple EDINET/IFRS concept IDs mapping to the same surface label count once.

2. Concept overlap is weighted using document frequency from the actual Paper 2
   MD&A corpus:
       idf(alias) = log((N + 1) / (df(alias) + 1)) + 1

3. Very common aliases receive little weight automatically.

4. Cosine remains the primary retrieval signal:
       score = cosine + lambda * weighted_overlap

5. IDF results are cached. The corpus scan is done only when the cache is absent
   or --rebuild-idf is supplied.

6. The script still validates sentence indexing against the exact splitter used
   by the benchmark-generation code.

7. Retrieval metrics are evaluated after reranking the saved candidate pool:
       Recall@1, Recall@3, Recall@10, and MRR@10.
   Recall@3 is therefore allowed to improve when the benchmark stores more than
   three retrieval candidates (for example, a Top-10 candidate pool).

Example
-------
python scripts/paper2/evaluate_concept_reranking.py \
  --benchmark japanese_mdna_alignment_gold_completed_top10.xlsx \
  --concepts data/interim/paper2/alignment/edinet_concept_dictionary.csv \
  --idf-corpus data/interim/paper2/mdna \
  --idf-cache data/interim/paper2/alignment/concept_alias_idf.csv \
  --output-dir data/interim/paper2/alignment/concept_reranking_eval_idf_top10

If your canonical MD&A text lives elsewhere, point --idf-corpus at that directory.
All .txt files below it are scanned recursively.

The benchmark itself need not be inside the IDF corpus.
"""

from __future__ import annotations

import argparse
import importlib.util
import inspect
import math
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Normalization
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
    """
    Normalize Japanese text for literal alias matching.
    Whitespace is removed because EDINET layout whitespace is not semantic.
    """
    s = norm_text(value)
    return re.sub(r"\s+", "", s)


def parse_csv_numbers(value, cast=float) -> list:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    return [cast(x.strip()) for x in str(value).split(",") if x.strip()]


# ---------------------------------------------------------------------------
# Canonical benchmark splitter discovery
# ---------------------------------------------------------------------------

def _load_module_from_path(path: Path):
    module_name = f"_alignment_splitter_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module spec from {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def discover_canonical_splitters(repo_root: Path) -> list[tuple[str, callable]]:
    candidate_files = [
        repo_root / "scripts/paper2/build_alignment_benchmark.py",
        repo_root / "scripts/paper2/retrieve_alignment_candidates.py",
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

    raise FileNotFoundError(
        f"Could not resolve prior_path={raw_path!r}. "
        "Use --repo-root if the workbook contains repo-relative paths."
    )


def select_canonical_splitter(
    benchmark_path: Path,
    metadata: pd.DataFrame,
    repo_root: Path,
) -> tuple[str, callable]:

    candidates = discover_canonical_splitters(repo_root)
    if not candidates:
        raise ValueError(
            "Could not find a plausible sentence splitter in "
            "build_alignment_benchmark.py or retrieve_alignment_candidates.py."
        )

    docs = []
    for _, row in metadata.iterrows():
        company = norm_text(row["company"])
        prior_path = resolve_prior_path(
            row["prior_path"], benchmark_path, repo_root
        )
        raw = prior_path.read_text(encoding="utf-8")
        expected = int(row["n_prior_sentences"])
        docs.append((company, raw, expected))

    passing = []
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
                ok = False
                break

            got = len(result)
            counts.append(f"{company}={got}/{expected}")
            if got != expected:
                ok = False

        diagnostics.append(f"{label}: " + ", ".join(counts))
        if ok:
            passing.append((label, func))

    if not passing:
        raise ValueError(
            "No discovered splitter reproduced benchmark counts.\n  "
            + "\n  ".join(diagnostics)
        )

    if len(passing) > 1:
        print(
            "Multiple splitters reproduce all benchmark sentence counts; "
            f"using {passing[0][0]}"
        )
    else:
        print(f"Using canonical benchmark splitter: {passing[0][0]}")

    return passing[0]


def load_prior_sentences(
    benchmark_path: Path,
    metadata: pd.DataFrame,
    repo_root: Path,
    splitter,
) -> dict[str, list[str]]:

    result = {}

    for _, row in metadata.iterrows():
        company = norm_text(row["company"])
        prior_path = resolve_prior_path(
            row["prior_path"], benchmark_path, repo_root
        )
        text = prior_path.read_text(encoding="utf-8")
        sentences = list(splitter(text))

        expected = int(row["n_prior_sentences"])
        if len(sentences) != expected:
            raise ValueError(
                f"{company}: reconstructed {len(sentences)} prior sentences, "
                f"but Metadata says {expected}."
            )

        result[company] = sentences

    return result


def validate_sentence_indices(
    annotation: pd.DataFrame,
    prior: dict[str, list[str]],
) -> None:

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
            if idx_col not in annotation.columns or text_col not in annotation.columns:
                continue

            idx_val = row[idx_col]
            text_val = row[text_col]

            if pd.isna(idx_val) or pd.isna(text_val) or norm_text(text_val) == "":
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
        raise ValueError(
            f"Sentence validation failed for {len(mismatches)} checks.\n"
            + "\n".join(map(str, mismatches[:10]))
        )


# ---------------------------------------------------------------------------
# Alias dictionary
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AliasRecord:
    alias: str
    sources: tuple[str, ...]


def load_alias_table(
    concepts: pd.DataFrame,
    min_alias_chars: int,
) -> pd.DataFrame:
    """
    Collapse multiple taxonomy concept IDs to one normalized Japanese alias.
    """
    required = {"source", "language", "label_normalized"}
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

    c["alias"] = c["label_normalized"].map(norm_match)
    c = c[c["alias"].str.len() >= min_alias_chars].copy()

    alias_table = (
        c.groupby("alias", as_index=False)
        .agg(
            taxonomy_concept_ids=(
                "concept_key",
                lambda s: " | ".join(sorted(set(map(str, s)))),
            ),
            sources=(
                "source",
                lambda s: " | ".join(sorted(set(map(str, s)))),
            ),
            taxonomy_id_count=("concept_key", "nunique"),
        )
    )

    return alias_table


class AliasMatcher:
    """
    Literal alias matcher collapsed at normalized Japanese surface form.

    Aliases are bucketed by first character. This avoids comparing all ~12k
    aliases against every sentence.
    """

    def __init__(self, alias_table: pd.DataFrame):
        self.by_first: dict[str, list[str]] = defaultdict(list)

        aliases = sorted(
            alias_table["alias"].astype(str).unique(),
            key=lambda s: (-len(s), s),
        )

        for alias in aliases:
            if alias:
                self.by_first[alias[0]].append(alias)

    def match(self, text: str) -> set[str]:
        t = norm_match(text)
        if not t:
            return set()

        matched = set()
        for ch in set(t):
            for alias in self.by_first.get(ch, []):
                if alias in t:
                    matched.add(alias)

        return matched


# ---------------------------------------------------------------------------
# Corpus IDF
# ---------------------------------------------------------------------------

def iter_corpus_files(root: Path, glob_pattern: str) -> list[Path]:
    files = sorted(root.rglob(glob_pattern))
    if not files:
        raise FileNotFoundError(
            f"No files matching {glob_pattern!r} found under {root}"
        )
    return files


def build_alias_idf(
    matcher: AliasMatcher,
    alias_table: pd.DataFrame,
    corpus_root: Path,
    corpus_glob: str,
    cache_path: Path,
    progress_every: int = 1000,
) -> pd.DataFrame:
    """
    Compute document frequency across MD&A documents.

    Each alias counts at most once per document.
    """
    files = iter_corpus_files(corpus_root, corpus_glob)
    n_docs = len(files)

    df_counts: dict[str, int] = defaultdict(int)

    print(f"Building corpus IDF from {n_docs:,} documents...")

    for i, path in enumerate(files, start=1):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = path.read_text(encoding="utf-8", errors="ignore")

        aliases = matcher.match(text)
        for alias in aliases:
            df_counts[alias] += 1

        if progress_every and (i % progress_every == 0 or i == n_docs):
            print(f"  IDF progress: {i:,}/{n_docs:,}")

    out = alias_table.copy()
    out["n_docs"] = n_docs
    out["doc_freq"] = out["alias"].map(df_counts).fillna(0).astype(int)

    # Standard smoothed IDF.
    out["idf"] = np.log(
        (out["n_docs"] + 1.0) / (out["doc_freq"] + 1.0)
    ) + 1.0

    # Share is useful diagnostically.
    out["doc_share"] = out["doc_freq"] / float(n_docs)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(cache_path, index=False, encoding="utf-8-sig")

    print(f"IDF cache written: {cache_path}")
    return out


def load_or_build_idf(
    alias_table: pd.DataFrame,
    matcher: AliasMatcher,
    corpus_root: Path | None,
    corpus_glob: str,
    cache_path: Path,
    rebuild: bool,
) -> pd.DataFrame:

    if cache_path.exists() and not rebuild:
        print(f"Loading cached corpus IDF: {cache_path}")
        idf = pd.read_csv(cache_path)

        required = {"alias", "idf", "doc_freq", "n_docs"}
        missing = required - set(idf.columns)
        if missing:
            raise ValueError(
                f"IDF cache missing columns: {sorted(missing)}"
            )
        return idf

    if corpus_root is None:
        raise ValueError(
            "--idf-corpus is required when IDF cache does not exist "
            "or --rebuild-idf is supplied."
        )

    return build_alias_idf(
        matcher=matcher,
        alias_table=alias_table,
        corpus_root=corpus_root,
        corpus_glob=corpus_glob,
        cache_path=cache_path,
    )


# ---------------------------------------------------------------------------
# Weighted overlap
# ---------------------------------------------------------------------------

def weighted_overlap_features(
    current_aliases: set[str],
    candidate_aliases: set[str],
    idf_map: dict[str, float],
) -> dict[str, float]:

    shared = current_aliases & candidate_aliases

    current_weight = sum(idf_map.get(a, 1.0) for a in current_aliases)
    candidate_weight = sum(idf_map.get(a, 1.0) for a in candidate_aliases)
    shared_weight = sum(idf_map.get(a, 1.0) for a in shared)
    union_weight = sum(
        idf_map.get(a, 1.0)
        for a in (current_aliases | candidate_aliases)
    )

    weighted_containment = (
        shared_weight / current_weight
        if current_weight > 0
        else 0.0
    )

    weighted_jaccard = (
        shared_weight / union_weight
        if union_weight > 0
        else 0.0
    )

    return {
        "shared_alias_count": len(shared),
        "shared_idf_weight": shared_weight,
        "weighted_containment": weighted_containment,
        "weighted_jaccard": weighted_jaccard,
    }


def rank_candidates(
    cands: pd.DataFrame,
    variant: str,
    lam: float = 0.02,
) -> pd.DataFrame:

    x = cands.copy()

    if variant == "cosine":
        x["rerank_score"] = x["cosine"]

    elif variant == "idf_jaccard_first":
        x["rerank_score"] = x["weighted_jaccard"]

    elif variant == "idf_containment_first":
        x["rerank_score"] = x["weighted_containment"]

    elif variant == "cosine_plus_idf_jaccard":
        x["rerank_score"] = x["cosine"] + lam * x["weighted_jaccard"]

    elif variant == "cosine_plus_idf_containment":
        x["rerank_score"] = x["cosine"] + lam * x["weighted_containment"]

    else:
        raise ValueError(f"Unknown ranking variant: {variant}")

    return x.sort_values(
        ["rerank_score", "cosine", "baseline_rank"],
        ascending=[False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)


def hit_at_k(
    ranked_indices: list[int],
    gold: int,
    k: int,
) -> bool:
    """Return True when gold is present in the first k ranked candidates."""
    return gold in ranked_indices[:k]


def reciprocal_rank_at_k(
    ranked_indices: list[int],
    gold: int,
    k: int,
) -> float:
    """Reciprocal rank truncated at k; returns 0 when gold is below k."""
    for rank, idx in enumerate(ranked_indices[:k], start=1):
        if idx == gold:
            return 1.0 / rank
    return 0.0


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_model(
    annotation: pd.DataFrame,
    prior: dict[str, list[str]],
    matcher: AliasMatcher,
    idf_map: dict[str, float],
    model: str,
    lambdas: Iterable[float],
) -> tuple[pd.DataFrame, pd.DataFrame]:

    topk_idx_col = f"{model}_topk_prior_indices"
    topk_cos_col = f"{model}_topk_cosines"

    variants: list[tuple[str, float | None]] = [
        ("cosine", None),
        ("idf_jaccard_first", None),
        ("idf_containment_first", None),
    ]

    for lam in lambdas:
        variants.append(("cosine_plus_idf_jaccard", lam))
        variants.append(("cosine_plus_idf_containment", lam))

    diag_rows = []
    metric_rows = []

    current_cache = {}
    candidate_cache = {}

    for _, row in annotation.iterrows():
        ann_id = norm_text(row["annotation_id"])
        company = norm_text(row["company"])
        current = norm_text(row["current_sentence"])
        gold_val = row["true_prior_sentence_index"]

        # Intentional no-match gold cases are excluded from retrieval ranking
        # metrics. They can be analyzed separately later.
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

        cur_aliases = current_cache[cur_key]

        cand_records = []

        for base_rank, (idx, cosine) in enumerate(
            zip(indices, cosines),
            start=1,
        ):
            sentence = prior[company][idx]
            ck = (company, idx)

            if ck not in candidate_cache:
                candidate_cache[ck] = matcher.match(sentence)

            cand_aliases = candidate_cache[ck]
            features = weighted_overlap_features(
                current_aliases=cur_aliases,
                candidate_aliases=cand_aliases,
                idf_map=idf_map,
            )

            shared = sorted(
                cur_aliases & cand_aliases,
                key=lambda a: (-idf_map.get(a, 1.0), a),
            )

            cand_records.append({
                "prior_index": idx,
                "prior_sentence": sentence,
                "cosine": float(cosine),
                "baseline_rank": base_rank,
                "current_alias_count": len(cur_aliases),
                "candidate_alias_count": len(cand_aliases),
                "shared_aliases": " | ".join(shared),
                **features,
            })

        cands = pd.DataFrame(cand_records)

        baseline = rank_candidates(cands, "cosine")
        baseline_indices = baseline["prior_index"].astype(int).tolist()
        baseline_rank1 = baseline_indices[0]
        candidate_pool_size = len(baseline_indices)

        baseline_hit_1 = hit_at_k(baseline_indices, gold, 1)
        baseline_hit_3 = hit_at_k(baseline_indices, gold, 3)
        baseline_hit_10 = hit_at_k(baseline_indices, gold, 10)
        baseline_rr_10 = reciprocal_rank_at_k(baseline_indices, gold, 10)
        gold_in_pool = gold in baseline_indices

        current_aliases_ranked = sorted(
            cur_aliases,
            key=lambda a: (-idf_map.get(a, 1.0), a),
        )

        for variant, lam in variants:
            ranked = rank_candidates(
                cands,
                variant,
                0.0 if lam is None else lam,
            )
            ranked_indices = ranked["prior_index"].astype(int).tolist()
            rank1 = ranked_indices[0]
            reranked_hit_1 = hit_at_k(ranked_indices, gold, 1)
            reranked_hit_3 = hit_at_k(ranked_indices, gold, 3)
            reranked_hit_10 = hit_at_k(ranked_indices, gold, 10)
            reranked_rr_10 = reciprocal_rank_at_k(
                ranked_indices,
                gold,
                10,
            )

            label = (
                variant
                if lam is None
                else f"{variant}_{lam:g}"
            )

            top = ranked.iloc[0]

            diag_rows.append({
                "annotation_id": ann_id,
                "company": company,
                "model": model,
                "variant": label,
                "current_sentence": current,
                "gold_prior_index": gold,
                "candidate_pool_size": candidate_pool_size,
                "gold_in_candidate_pool": gold_in_pool,
                "baseline_top1_index": baseline_rank1,
                "reranked_top1_index": rank1,
                "baseline_top1_correct": baseline_hit_1,
                "reranked_top1_correct": reranked_hit_1,
                "baseline_hit_at_3": baseline_hit_3,
                "reranked_hit_at_3": reranked_hit_3,
                "baseline_hit_at_10": baseline_hit_10,
                "reranked_hit_at_10": reranked_hit_10,
                "fixed_top1": (not baseline_hit_1) and reranked_hit_1,
                "worsened_top1": baseline_hit_1 and (not reranked_hit_1),
                "baseline_rr_at_10": baseline_rr_10,
                "reranked_rr_at_10": reranked_rr_10,
                "current_alias_count": len(cur_aliases),
                "current_aliases_by_idf": " | ".join(
                    f"{a}:{idf_map.get(a, 1.0):.3f}"
                    for a in current_aliases_ranked[:30]
                ),
                "top_candidate_cosine": float(top["cosine"]),
                "top_candidate_shared_aliases": top["shared_aliases"],
                "top_candidate_shared_idf_weight": float(
                    top["shared_idf_weight"]
                ),
                "top_candidate_weighted_jaccard": float(
                    top["weighted_jaccard"]
                ),
                "top_candidate_weighted_containment": float(
                    top["weighted_containment"]
                ),
                "top_candidate_sentence": top["prior_sentence"],
                "candidate_indices_baseline": ",".join(
                    map(str, baseline_indices)
                ),
                "candidate_indices_reranked": ",".join(
                    map(str, ranked_indices)
                ),
            })

    diag = pd.DataFrame(diag_rows)

    if diag.empty:
        return diag, pd.DataFrame()

    for variant, g in diag.groupby("variant", sort=False):
        metric_rows.append({
            "model": model,
            "variant": variant,
            "n": len(g),
            "recall_at_1": float(
                g["reranked_top1_correct"].mean()
            ),
            "recall_at_3": float(
                g["reranked_hit_at_3"].mean()
            ),
            "recall_at_10": float(
                g["reranked_hit_at_10"].mean()
            ),
            "mrr_at_10": float(
                g["reranked_rr_at_10"].mean()
            ),
            "fixed_top1": int(g["fixed_top1"].sum()),
            "worsened_top1": int(g["worsened_top1"].sum()),
            "net_top1_change": int(
                g["fixed_top1"].sum()
                - g["worsened_top1"].sum()
            ),
            "rows_with_current_aliases": int(
                (g["current_alias_count"] > 0).sum()
            ),
            "current_alias_coverage": float(
                (g["current_alias_count"] > 0).mean()
            ),
        })

    return diag, pd.DataFrame(metric_rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument("--benchmark", type=Path, required=True)
    ap.add_argument("--concepts", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--repo-root", type=Path, default=None)

    ap.add_argument(
        "--idf-corpus",
        type=Path,
        default=None,
        help="Directory containing canonical MD&A .txt documents.",
    )
    ap.add_argument(
        "--idf-cache",
        type=Path,
        default=Path(
            "data/interim/paper2/alignment/concept_alias_idf.csv"
        ),
    )
    ap.add_argument(
        "--idf-glob",
        default="*.txt",
        help="Recursive corpus file glob; default: *.txt",
    )
    ap.add_argument(
        "--rebuild-idf",
        action="store_true",
    )

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
        default=[0.005, 0.01, 0.02, 0.05, 0.10],
    )
    ap.add_argument(
        "--min-alias-chars",
        type=int,
        default=2,
    )

    args = ap.parse_args()

    xls = pd.ExcelFile(args.benchmark)

    if "Gold Annotation" in xls.sheet_names:
        annotation_sheet = "Gold Annotation"
    elif "Annotation" in xls.sheet_names:
        annotation_sheet = "Annotation"
    else:
        raise ValueError(
            "Benchmark workbook must contain either 'Gold Annotation' "
            "or 'Annotation'. Found: "
            + ", ".join(xls.sheet_names)
        )

    print(f"Using annotation sheet: {annotation_sheet}")

    annotation = pd.read_excel(
        args.benchmark,
        sheet_name=annotation_sheet,
    )
    metadata = pd.read_excel(
        args.benchmark,
        sheet_name="Metadata",
    )
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

    alias_table = load_alias_table(
        concepts=concepts,
        min_alias_chars=args.min_alias_chars,
    )

    print(
        f"Collapsed taxonomy dictionary to "
        f"{len(alias_table):,} unique Japanese aliases."
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
        zip(
            idf["alias"].astype(str),
            idf["idf"].astype(float),
        )
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    all_diag = []
    all_metrics = []

    for model in args.models:
        diag, metrics = evaluate_model(
            annotation=annotation,
            prior=prior,
            matcher=matcher,
            idf_map=idf_map,
            model=model,
            lambdas=args.lambdas,
        )
        all_diag.append(diag)
        all_metrics.append(metrics)

    diag = pd.concat(all_diag, ignore_index=True)
    metrics = pd.concat(all_metrics, ignore_index=True)

    diag_path = (
        args.output_dir
        / "concept_reranking_diagnostics.csv"
    )
    metrics_path = (
        args.output_dir
        / "concept_reranking_summary.csv"
    )
    changed_path = (
        args.output_dir
        / "concept_reranking_changed_top1.csv"
    )

    diag.to_csv(
        diag_path,
        index=False,
        encoding="utf-8-sig",
    )
    metrics.to_csv(
        metrics_path,
        index=False,
        encoding="utf-8-sig",
    )

    changed = diag[
        diag["fixed_top1"] | diag["worsened_top1"]
    ].copy()

    changed.to_csv(
        changed_path,
        index=False,
        encoding="utf-8-sig",
    )

    # Save compact IDF diagnostics: rarest/highest and commonest/lowest.
    idf_diag_path = (
        args.output_dir
        / "concept_alias_idf_diagnostics.csv"
    )

    idf_diag = pd.concat(
        [
            idf.sort_values(
                ["idf", "doc_freq"],
                ascending=[False, True],
            ).head(100),
            idf.sort_values(
                ["idf", "doc_freq"],
                ascending=[True, False],
            ).head(100),
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["alias"])

    idf_diag.to_csv(
        idf_diag_path,
        index=False,
        encoding="utf-8-sig",
    )

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    print("\n=== Corpus-IDF concept reranking summary ===")

    display_cols = [
        "model",
        "variant",
        "n",
        "recall_at_1",
        "recall_at_3",
        "recall_at_10",
        "mrr_at_10",
        "fixed_top1",
        "worsened_top1",
        "net_top1_change",
        "current_alias_coverage",
    ]

    print(
        metrics[display_cols].to_string(
            index=False,
            float_format=lambda x: f"{x:.4f}",
        )
    )

    pool_sizes = sorted(
        int(x)
        for x in diag["candidate_pool_size"].dropna().unique()
    )
    print(
        "\nNOTE: Recall@3 is computed after reranking the saved "
        f"candidate pool (observed pool sizes: {pool_sizes}). "
        "Recall@10 measures whether the gold sentence remains within "
        "the first 10 reranked candidates."
    )

    print(f"\nsummary:         {metrics_path}")
    print(f"diagnostics:     {diag_path}")
    print(f"changed:         {changed_path}")
    print(f"idf diagnostics: {idf_diag_path}")
    print(f"idf cache:       {args.idf_cache}")


if __name__ == "__main__":
    main()
