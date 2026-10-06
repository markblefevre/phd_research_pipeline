#!/usr/bin/env python3
"""Benchmark Japanese sentence-embedding models for MD&A year-over-year alignment.

The economic classification is binary:

    persistent
        Current-year disclosure that is substantially unchanged in
        economic meaning relative to the prior-year disclosure.

    novel
        Newly introduced OR substantially revised current-year disclosure.

The annotation workbook may retain the more detailed manual labels:

    new
    revised
    persistent
    ambiguous

For the primary benchmark:
    new        -> novel
    revised    -> novel
    persistent -> persistent
    ambiguous  -> excluded

The benchmark asks whether embedding similarity can distinguish economically
persistent disclosure from newly introduced or substantially revised
disclosure language.

This script is intentionally independent of market-return outcomes.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import pointbiserialr
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)


MODELS = {
    "sonoisa_v2": "sonoisa/sentence-bert-base-ja-mean-tokens-v2",
    "ruri_v3_310m": "cl-nagoya/ruri-v3-310m",
    "sarashina_v2_1b": "sbintuitions/sarashina-embedding-v2-1b",
}


# ---------------------------------------------------------------------------
# Manual annotation -> economic classification
#
# Novel includes both completely new language and substantially revised
# language. Ambiguous observations are retained in the annotation workbook
# but excluded from the primary binary benchmark.
# ---------------------------------------------------------------------------

MANUAL_TO_BINARY_LABEL = {
    "new": "novel",
    "revised": "novel",
    "novel": "novel",
    "persistent": "persistent",
}

BINARY_LABEL_TO_INT = {
    "novel": 0,
    "persistent": 1,
}

INT_TO_BINARY_LABEL = {
    0: "novel",
    1: "persistent",
}


def load_annotations(path: Path) -> pd.DataFrame:
    """Load manual annotations and construct the binary economic label."""

    if path.suffix.lower() == ".xlsx":
        df = pd.read_excel(path, sheet_name="Annotation")
    else:
        df = pd.read_csv(path)

    needed = {
        "company",
        "current_sentence",
        "best_prior_sentence",
        "manual_label",
    }

    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Normalize labels to avoid problems caused by capitalization or
    # accidental whitespace in the annotation workbook.
    df["manual_label"] = (
        df["manual_label"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # Preserve the original manual label and derive the binary economic label.
    df["binary_label"] = df["manual_label"].map(MANUAL_TO_BINARY_LABEL)

    n_total = len(df)
    n_ambiguous_or_other = df["binary_label"].isna().sum()

    # Ambiguous/unrecognized observations are not used to fit the benchmark.
    df = df[df["binary_label"].notna()].copy()

    if df.empty:
        raise ValueError(
            "No usable manually labeled rows found. "
            "Expected labels such as new, revised, novel, persistent, "
            "or ambiguous."
        )

    print(
        f"Loaded {n_total:,} annotation rows; "
        f"using {len(df):,} for the binary benchmark and "
        f"excluding {n_ambiguous_or_other:,} ambiguous/unrecognized rows."
    )

    print("\nManual labels used:")
    print(df["manual_label"].value_counts().to_string())

    print("\nBinary labels:")
    print(df["binary_label"].value_counts().to_string())

    return df


def encode_pair_texts(
    model_name: str,
    current: list[str],
    prior: list[str],
    batch_size: int,
):
    """Encode current-year and matched prior-year sentences."""

    from sentence_transformers import SentenceTransformer
    import torch

    if torch.cuda.is_available():
        device = "cuda"
    elif (
        getattr(torch.backends, "mps", None)
        and torch.backends.mps.is_available()
    ):
        device = "mps"
    else:
        device = "cpu"

    model = SentenceTransformer(model_name, device=device)

    # For semantic similarity, Ruri v3 documentation specifies the empty
    # prefix. We therefore encode the raw sentence text here.
    cur = model.encode(
        current,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=True,
    )

    prv = model.encode(
        prior,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=True,
    )

    return np.asarray(cur), np.asarray(prv), device


def fit_threshold(sim: np.ndarray, y: np.ndarray):
    """Grid-search one similarity threshold using F1.

    Encoding:
        novel      = 0
        persistent = 1

    Classification rule:
        similarity < threshold  -> novel
        similarity >= threshold -> persistent

    This is suitable for pilot model comparison only.

    For the final study, the threshold should be frozen using a
    train/validation split or cross-validation before any market-return
    regressions are estimated.
    """

    thresholds = np.unique(
        np.quantile(sim, np.linspace(0.05, 0.95, 181))
    )

    best = None

    for threshold in thresholds:
        pred = (sim >= threshold).astype(int)

        # Positive class is persistent (= 1).
        score = f1_score(y, pred, pos_label=1)

        if best is None or score > best[0]:
            best = (score, threshold, pred)

    if best is None:
        raise ValueError("Unable to estimate a classification threshold.")

    return best


def main():
    ap = argparse.ArgumentParser(
        description=(
            "Benchmark Japanese embedding models for binary "
            "persistent-vs-novel MD&A classification."
        )
    )

    ap.add_argument(
        "annotations",
        type=Path,
        help="Annotation workbook (.xlsx) or CSV.",
    )

    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path("alignment_benchmark_results"),
        help="Directory for benchmark results.",
    )

    ap.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Embedding batch size.",
    )

    ap.add_argument(
        "--models",
        nargs="*",
        choices=list(MODELS),
        default=list(MODELS),
        help="Embedding models to benchmark.",
    )

    args = ap.parse_args()

    df = load_annotations(args.annotations)

    args.output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    y = (
        df["binary_label"]
        .map(BINARY_LABEL_TO_INT)
        .to_numpy()
    )

    rows = []

    for short in args.models:
        model_name = MODELS[short]

        print("\n" + "=" * 72)
        print(f"Model: {short}")
        print(f"Model ID: {model_name}")
        print("=" * 72)

        cur, prv, device = encode_pair_texts(
            model_name,
            df["current_sentence"].astype(str).tolist(),
            df["best_prior_sentence"].astype(str).tolist(),
            args.batch_size,
        )

        # Embeddings are normalized, so their dot product is cosine similarity.
        sim = np.sum(cur * prv, axis=1)

        f1, threshold, pred = fit_threshold(sim, y)

        accuracy = accuracy_score(y, pred)

        precision = precision_score(
            y,
            pred,
            pos_label=1,
            zero_division=0,
        )

        recall = recall_score(
            y,
            pred,
            pos_label=1,
            zero_division=0,
        )

        # Higher similarity should correspond to greater probability of
        # persistent disclosure, so similarity itself can be used as the
        # ROC-AUC score.
        try:
            roc_auc = roc_auc_score(y, sim)
        except ValueError:
            roc_auc = np.nan

        # Point-biserial correlation is the natural analogue of correlation
        # between a continuous similarity score and a binary label.
        try:
            correlation, correlation_p = pointbiserialr(y, sim)
        except ValueError:
            correlation = np.nan
            correlation_p = np.nan

        cm = confusion_matrix(
            y,
            pred,
            labels=[0, 1],
        )

        tn, fp, fn, tp = cm.ravel()

        rows.append(
            {
                "model": short,
                "model_id": model_name,
                "device": device,
                "n": len(df),
                "threshold_novel_persistent": threshold,
                "f1_persistent_in_sample_pilot": f1,
                "accuracy_in_sample_pilot": accuracy,
                "precision_persistent_in_sample_pilot": precision,
                "recall_persistent_in_sample_pilot": recall,
                "roc_auc_similarity": roc_auc,
                "pointbiserial_similarity_vs_label": correlation,
                "pointbiserial_p": correlation_p,
                "true_novel_pred_novel": tn,
                "true_novel_pred_persistent": fp,
                "true_persistent_pred_novel": fn,
                "true_persistent_pred_persistent": tp,
            }
        )

        # ---------------------------------------------------------------
        # Row-level output
        #
        # Keep the detailed original manual label. This lets us inspect
        # whether errors are disproportionately associated with sentences
        # that humans originally classified as "revised".
        # ---------------------------------------------------------------

        output_columns = [
            col
            for col in [
                "annotation_id",
                "company",
                "current_sentence",
                "best_prior_sentence",
                "manual_label",
                "binary_label",
            ]
            if col in df.columns
        ]

        out = df[output_columns].copy()

        out["similarity"] = sim
        out["threshold"] = threshold
        out["predicted_binary"] = pred
        out["predicted_label"] = (
            pd.Series(pred, index=out.index)
            .map(INT_TO_BINARY_LABEL)
        )

        out["correct"] = (
            out["binary_label"] == out["predicted_label"]
        )

        output_path = (
            args.output_dir
            / f"{short}_row_scores.csv"
        )

        out.to_csv(
            output_path,
            index=False,
            encoding="utf-8-sig",
        )

        print(f"\nThreshold: {threshold:.6f}")
        print(f"F1 (persistent): {f1:.4f}")
        print(f"Accuracy:        {accuracy:.4f}")
        print(f"Precision:       {precision:.4f}")
        print(f"Recall:          {recall:.4f}")
        print(f"ROC-AUC:         {roc_auc:.4f}")

        print("\nConfusion matrix:")
        print("                         Predicted")
        print("                    Novel    Persistent")
        print(
            f"Actual Novel       {tn:8d} {fp:13d}"
        )
        print(
            f"Actual Persistent  {fn:8d} {tp:13d}"
        )

        print(f"\nWrote: {output_path}")

    summary = pd.DataFrame(rows).sort_values(
        [
            "f1_persistent_in_sample_pilot",
            "accuracy_in_sample_pilot",
        ],
        ascending=False,
    )

    summary_path = (
        args.output_dir
        / "model_summary.csv"
    )

    summary.to_csv(
        summary_path,
        index=False,
        encoding="utf-8-sig",
    )

    print("\n" + "=" * 72)
    print("MODEL SUMMARY")
    print("=" * 72)

    print(
        summary.to_string(
            index=False,
            float_format=lambda x: f"{x:.4f}",
        )
    )

    print(f"\nWrote: {summary_path}")

    print(
        "\nNOTE: Thresholds and classification statistics above are "
        "in-sample pilot results. Before production use, select the model "
        "and freeze the persistent/novel threshold using a held-out "
        "validation scheme or cross-validation. Do not tune the threshold "
        "using market-return outcomes."
    )


if __name__ == "__main__":
    main()
