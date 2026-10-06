#!/usr/bin/env python3
"""Build a human-review benchmark for Japanese MD&A sentence alignment.

Purpose
-------
This script builds an annotation workbook for evaluating sentence-level
INFORMATION NOVELTY.

It deliberately separates two tasks:

1. Alignment
   For each sampled current-year sentence, find plausible prior-year
   counterparts using:
       - character n-gram TF-IDF cosine
       - Sonoisa sentence embeddings
       - Ruri v3 sentence embeddings
       - Sarashina sentence embeddings

2. Human information-novelty coding
   After reviewing the candidate alignments, the human annotator chooses the
   best prior-year counterpart and labels the current-year disclosure:
       - persistent
       - novel

"Novel" means newly introduced OR substantially revised economic information.
A sentence can therefore be lexically/semantically very similar to last year's
sentence yet still be novel when its substantive information changes, e.g.
増益 <-> 減益 or 増加 <-> 減少.

No market-return information is used.

Example
-------
python build_alignment_benchmark.py \
    --pair Toyota:/path/to/prior.txt:/path/to/current.txt \
    --pair MUFG:/path/to/prior.txt:/path/to/current.txt \
    --output japanese_mdna_alignment_candidates.xlsx \
    --sample-per-company 75 \
    --seed 42
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


MODELS = {
    "ruri": "cl-nagoya/ruri-v3-310m",
    "sarashina": "sbintuitions/sarashina-embedding-v2-1b",
}

TERMINATORS = "。！？!?"
MIN_SENTENCE_CHARS = 12


@dataclass
class FilingPair:
    company: str
    prior_path: Path
    current_path: Path


def parse_pair(value: str) -> FilingPair:
    """Parse COMPANY:PRIOR_PATH:CURRENT_PATH.

    split(':', 2) is intentional. On macOS/Linux this handles ordinary paths.
    For Windows drive-letter paths, prefer running under WSL or adapt this
    parser to use a config file.
    """
    parts = value.split(":", 2)
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            "--pair must be COMPANY:PRIOR_PATH:CURRENT_PATH"
        )
    company, prior, current = parts
    return FilingPair(company.strip(), Path(prior), Path(current))


def read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(path)
    # utf-8-sig safely handles a possible BOM.
    return path.read_text(encoding="utf-8-sig", errors="replace")


def normalize_sentence(text: str) -> str:
    text = str(text).replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def split_japanese_sentences(text: str, min_chars: int = MIN_SENTENCE_CHARS) -> list[str]:
    """Conservative Japanese sentence splitter.

    Keeps terminal punctuation. Empty/very short fragments are removed.
    If your pipeline already has a canonical sentence splitter, replace this
    function with that implementation so the benchmark uses identical units.
    """
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    chunks = re.split(r"(?<=[。！？!?])|\n+", text)
    sentences = [normalize_sentence(x) for x in chunks]
    return [x for x in sentences if len(x) >= min_chars]


def tfidf_best_matches(
    current: list[str],
    prior: list[str],
) -> tuple[np.ndarray, np.ndarray]:
    """Return best prior index and cosine for each current sentence."""
    vectorizer = TfidfVectorizer(
        analyzer="char",
        ngram_range=(2, 5),
        min_df=1,
        sublinear_tf=True,
        norm="l2",
    )
    all_text = prior + current
    matrix = vectorizer.fit_transform(all_text)
    prior_x = matrix[: len(prior)]
    current_x = matrix[len(prior):]
    sims = cosine_similarity(current_x, prior_x)
    best_idx = np.argmax(sims, axis=1)
    best_sim = sims[np.arange(len(current)), best_idx]
    return best_idx, best_sim


def embedding_best_matches(
    model_name: str,
    current: list[str],
    prior: list[str],
    batch_size: int,
) -> tuple[np.ndarray, np.ndarray, str]:
    """Return best prior index and cosine using normalized sentence embeddings."""
    from sentence_transformers import SentenceTransformer
    import torch

    if torch.cuda.is_available():
        device = "cuda"
    elif getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        device = "mps"
    else:
        device = "cpu"

    print(f"Loading {model_name} on {device}")
    model = SentenceTransformer(model_name, device=device)

    prior_e = np.asarray(
        model.encode(
            prior,
            batch_size=batch_size,
            normalize_embeddings=True,
            show_progress_bar=True,
        )
    )
    current_e = np.asarray(
        model.encode(
            current,
            batch_size=batch_size,
            normalize_embeddings=True,
            show_progress_bar=True,
        )
    )

    # Avoid constructing one huge current x prior matrix.
    best_idx = np.empty(len(current), dtype=int)
    best_sim = np.empty(len(current), dtype=float)

    block = max(1, min(256, len(current)))
    prior_t = prior_e.T

    for start in range(0, len(current), block):
        stop = min(start + block, len(current))
        sims = current_e[start:stop] @ prior_t
        idx = np.argmax(sims, axis=1)
        val = sims[np.arange(stop - start), idx]
        best_idx[start:stop] = idx
        best_sim[start:stop] = val

    return best_idx, best_sim, device


def similarity_stratum(sim: float) -> str:
    if sim < 0.35:
        return "low"
    if sim < 0.70:
        return "mid"
    return "high"


def stratified_sample(
    df: pd.DataFrame,
    n: int,
    seed: int,
) -> pd.DataFrame:
    """Sample across TF-IDF low/mid/high strata as evenly as possible."""
    if n >= len(df):
        return df.copy()

    rng = np.random.default_rng(seed)
    groups = {k: g.index.to_numpy() for k, g in df.groupby("stratum")}
    strata = [s for s in ["low", "mid", "high"] if s in groups]

    base = n // len(strata)
    remainder = n % len(strata)
    selected: list[int] = []

    for i, stratum in enumerate(strata):
        target = base + (1 if i < remainder else 0)
        pool = groups[stratum]
        take = min(target, len(pool))
        if take:
            selected.extend(rng.choice(pool, size=take, replace=False).tolist())

    # Fill any shortfall from all unselected rows.
    if len(selected) < n:
        remaining = np.setdiff1d(df.index.to_numpy(), np.asarray(selected))
        extra = rng.choice(remaining, size=n - len(selected), replace=False)
        selected.extend(extra.tolist())

    return df.loc[selected].sort_values("current_sentence_index").copy()


def add_candidate_columns(
    df: pd.DataFrame,
    prior: list[str],
    method: str,
    idx: np.ndarray,
    sim: np.ndarray,
) -> None:
    df[f"{method}_prior_index"] = idx
    df[f"{method}_prior_sentence"] = [prior[i] for i in idx]
    df[f"{method}_cosine"] = sim


def build_company(
    pair: FilingPair,
    batch_size: int,
    sample_n: int,
    seed: int,
) -> tuple[pd.DataFrame, dict]:
    print("\n" + "=" * 80)
    print(pair.company)
    print("=" * 80)

    prior = split_japanese_sentences(read_text(pair.prior_path))
    current = split_japanese_sentences(read_text(pair.current_path))

    if not prior or not current:
        raise ValueError(
            f"{pair.company}: no usable sentences after splitting "
            f"(prior={len(prior)}, current={len(current)})"
        )

    print(f"Prior sentences:   {len(prior):,}")
    print(f"Current sentences: {len(current):,}")

    tf_idx, tf_sim = tfidf_best_matches(current, prior)

    df = pd.DataFrame(
        {
            "company": pair.company,
            "current_sentence_index": np.arange(len(current)),
            "current_sentence": current,
            "current_chars": [len(x) for x in current],
            "tfidf_prior_index": tf_idx,
            "tfidf_prior_sentence": [prior[i] for i in tf_idx],
            "tfidf_cosine": tf_sim,
            "stratum": [similarity_stratum(x) for x in tf_sim],
        }
    )

    devices = {}
    for short, model_name in MODELS.items():
        idx, sim, device = embedding_best_matches(
            model_name, current, prior, batch_size
        )
        add_candidate_columns(df, prior, short, idx, sim)
        devices[short] = device

    sampled = stratified_sample(df, sample_n, seed)
    sampled.insert(
        0,
        "annotation_id",
        [f"{pair.company[:3].upper()}_{i:03d}" for i in range(1, len(sampled) + 1)],
    )

    # Human-review fields. Do not prefill labels: these are the gold standard.
    sampled["true_prior_sentence_index"] = ""
    sampled["true_prior_sentence"] = ""
    sampled["manual_label"] = ""
    sampled["manual_confidence"] = ""
    sampled["manual_notes"] = ""

    # Flag agreement among automated alignment methods.
    candidate_index_cols = [
        "tfidf_prior_index",
        *[f"{model_name}_prior_index" for model_name in MODELS],
    ]
    sampled["candidate_unique_count"] = sampled[candidate_index_cols].nunique(axis=1)
    sampled["all_methods_agree"] = sampled["candidate_unique_count"].eq(1)

    metadata = {
        "company": pair.company,
        "prior_path": str(pair.prior_path),
        "current_path": str(pair.current_path),
        "n_prior_sentences": len(prior),
        "n_current_sentences": len(current),
        "n_sampled": len(sampled),
        **{f"{k}_device": v for k, v in devices.items()},
    }
    return sampled, metadata


def write_workbook(
    annotation: pd.DataFrame,
    metadata: list[dict],
    output: Path,
) -> None:
    from openpyxl import load_workbook
    from openpyxl.formatting.rule import FormulaRule
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.worksheet.datavalidation import DataValidation

    output.parent.mkdir(parents=True, exist_ok=True)

    guide = pd.DataFrame(
        [
            ["Purpose", "Human gold standard for sentence alignment and INFORMATION NOVELTY."],
            ["Alignment task", "Choose the best prior-year counterpart for each current-year sentence. Automated candidates are suggestions, not truth."],
            ["Persistent", "Substantially the same economic information as the prior-year counterpart. Routine year/date/amount updates may remain persistent when the substantive message is unchanged."],
            ["Novel", "Newly introduced OR substantially revised economic information. Direction, driver, scope, state, risk, or substantive message materially changes."],
            ["Important", "High cosine similarity does NOT automatically mean persistent. 増益↔減益, 増加↔減少, deterioration↔recovery can be novel despite near-identical wording."],
            ["TF-IDF", "Character n-gram cosine. Used as one alignment candidate and for sampling strata."],
            ["Embeddings", "Sonoisa, Ruri, and Sarashina independently search all prior-year sentences using cosine similarity of normalized sentence embeddings."],
            ["true_prior_sentence_index", "Enter the prior-year sentence index you judge to be the best counterpart. Prefer one of the candidate indices, but you may enter another after checking context."],
            ["true_prior_sentence", "Optional human-readable copy of the selected prior sentence. Can be filled later by code from the chosen index."],
            ["manual_label", "persistent or novel. Code economic/information persistence, not merely wording similarity."],
            ["manual_confidence", "high, medium, or low."],
            ["Workflow", "First validate alignment. Then assign persistent/novel. Freeze human labels before benchmarking model performance."],
            ["Separation from document novelty", "This workbook measures sentence-level INFORMATION NOVELTY. Whole-MD&A cosine distance remains the separate document-level TEXTUAL NOVELTY measure."],
        ],
        columns=["item", "guidance"],
    )

    meta_df = pd.DataFrame(metadata)

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        annotation.to_excel(writer, sheet_name="Annotation", index=False)
        guide.to_excel(writer, sheet_name="Guide", index=False)
        meta_df.to_excel(writer, sheet_name="Metadata", index=False)

    wb = load_workbook(output)
    ws = wb["Annotation"]

    # Freeze current sentence identifiers while reviewing many candidate columns.
    ws.freeze_panes = "D2"
    ws.auto_filter.ref = ws.dimensions

    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(vertical="center", wrap_text=True)

    # Widths.
    widths = {
        "A": 16, "B": 16, "C": 18, "D": 70, "E": 14,
    }
    for col in range(1, ws.max_column + 1):
        letter = ws.cell(1, col).column_letter
        header = str(ws.cell(1, col).value or "")
        if "sentence" in header:
            ws.column_dimensions[letter].width = 70
        elif "cosine" in header:
            ws.column_dimensions[letter].width = 14
        elif "index" in header:
            ws.column_dimensions[letter].width = 18
        else:
            ws.column_dimensions[letter].width = widths.get(letter, 20)

    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)

    headers = {cell.value: cell.column for cell in ws[1]}
    label_col = ws.cell(1, headers["manual_label"]).column_letter
    conf_col = ws.cell(1, headers["manual_confidence"]).column_letter

    dv_label = DataValidation(
        type="list", formula1='"persistent,novel"', allow_blank=True
    )
    dv_conf = DataValidation(
        type="list", formula1='"high,medium,low"', allow_blank=True
    )
    ws.add_data_validation(dv_label)
    ws.add_data_validation(dv_conf)
    dv_label.add(f"{label_col}2:{label_col}{ws.max_row}")
    dv_conf.add(f"{conf_col}2:{conf_col}{ws.max_row}")

    green = PatternFill("solid", fgColor="E2F0D9")
    orange = PatternFill("solid", fgColor="FCE4D6")
    yellow = PatternFill("solid", fgColor="FFF2CC")

    ws.conditional_formatting.add(
        f"{label_col}2:{label_col}{ws.max_row}",
        FormulaRule(formula=[f'{label_col}2="persistent"'], fill=green),
    )
    ws.conditional_formatting.add(
        f"{label_col}2:{label_col}{ws.max_row}",
        FormulaRule(formula=[f'{label_col}2="novel"'], fill=orange),
    )
    agree_col = ws.cell(1, headers["all_methods_agree"]).column_letter
    ws.conditional_formatting.add(
        f"{agree_col}2:{agree_col}{ws.max_row}",
        FormulaRule(formula=[f'{agree_col}2=FALSE'], fill=yellow),
    )

    # Guide formatting.
    g = wb["Guide"]
    g.freeze_panes = "A2"
    g.column_dimensions["A"].width = 28
    g.column_dimensions["B"].width = 110
    for cell in g[1]:
        cell.fill = header_fill
        cell.font = header_font
    for row in g.iter_rows():
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)

    m = wb["Metadata"]
    for cell in m[1]:
        cell.fill = header_fill
        cell.font = header_font
    for col in range(1, m.max_column + 1):
        m.column_dimensions[m.cell(1, col).column_letter].width = 28

    wb.save(output)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build Japanese MD&A sentence-alignment annotation workbook."
    )
    ap.add_argument(
        "--pair",
        action="append",
        type=parse_pair,
        required=True,
        help="Repeatable COMPANY:PRIOR_PATH:CURRENT_PATH specification.",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=Path("japanese_mdna_alignment_candidates.xlsx"),
    )
    ap.add_argument("--sample-per-company", type=int, default=75)
    ap.add_argument("--batch-size", type=int, default=32)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    frames = []
    metadata = []
    for i, pair in enumerate(args.pair):
        frame, meta = build_company(
            pair,
            batch_size=args.batch_size,
            sample_n=args.sample_per_company,
            seed=args.seed + i,
        )
        frames.append(frame)
        metadata.append(meta)

    annotation = pd.concat(frames, ignore_index=True)
    # Globally stable annotation IDs.
    annotation["annotation_id"] = [
        f"A{i:03d}" for i in range(1, len(annotation) + 1)
    ]

    write_workbook(annotation, metadata, args.output)

    print("\n" + "=" * 80)
    print(f"Wrote: {args.output}")
    print(f"Annotation rows: {len(annotation):,}")
    print("\nCandidate agreement:")
    print(annotation["all_methods_agree"].value_counts().to_string())
    print(
        "\nNext: manually validate true_prior_sentence_index, then assign "
        "manual_label = persistent/novel."
    )


if __name__ == "__main__":
    main()
