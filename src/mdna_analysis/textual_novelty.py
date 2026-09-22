from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer


KEY = ["edinetCode", "docID"]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _identity_split(text: str) -> list[str]:
    """
    Reconstruct the Stage 4 token sequence.

    Stage 4 writes exactly one pre-tokenized token per line.  ``str.split()``
    therefore recovers the intended tokens without asking scikit-learn to do
    any additional Japanese tokenization.
    """
    return text.split()


def _load_variant_manifest(
    physical_token_root: Path,
    variant: str,
) -> pd.DataFrame:
    manifest_csv = physical_token_root / variant / "manifest.csv"

    if not manifest_csv.exists():
        raise FileNotFoundError(
            f"Missing Stage 4 manifest: {manifest_csv}"
        )

    df = pd.read_csv(
        manifest_csv,
        dtype={
            "edinetCode": "string",
            "docID": "string",
            "variant": "string",
            "status": "string",
        },
    )

    required = {
        "edinetCode",
        "docID",
        "variant",
        "status",
        "tokenCount",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in {manifest_csv}: "
            f"{sorted(missing)}"
        )

    duplicates = int(df.duplicated(KEY).sum())
    if duplicates:
        raise ValueError(
            f"{variant} manifest contains {duplicates:,} duplicate "
            "(edinetCode, docID) rows"
        )

    failures = df[df["status"].isin(["missing_source", "error"])]
    if not failures.empty:
        raise ValueError(
            f"{variant} manifest contains "
            f"{len(failures):,} failed document(s)"
        )

    values = set(df["variant"].dropna().astype(str))
    if values != {variant}:
        raise ValueError(
            f"Variant mismatch in {manifest_csv}: "
            f"expected {variant!r}, found {sorted(values)}"
        )

    return df.sort_values(KEY, kind="stable").reset_index(drop=True)


def _build_token_file_list(
    physical_token_root: Path,
    variant: str,
    manifest: pd.DataFrame,
) -> list[str]:
    files: list[str] = []
    missing: list[str] = []

    for row in manifest.itertuples(index=False):
        path = (
            physical_token_root
            / variant
            / str(row.edinetCode)
            / f"{row.docID}.tokens.txt"
        )

        if not path.exists():
            missing.append(str(path))

        files.append(str(path))

    if missing:
        preview = "\n".join(missing[:10])
        raise FileNotFoundError(
            f"{variant} is missing {len(missing):,} token file(s). "
            f"First examples:\n{preview}"
        )

    return files



def run_pair_diagnostics(
    *,
    pairs_csv: str | Path,
    output_dir: str | Path,
    work_output_dir: str | Path | None = None,
) -> pd.DataFrame:
    """
    Compute representation-independent pair diagnostics from the frozen Stage 3
    research-eligible pair manifest.

    Stage 3 already carries exact extracted MD&A character counts in
    ``prev_textChars`` and ``curr_textChars``.  Stage 5 therefore derives its
    length diagnostics directly from those columns rather than rereading the
    Stage 2 text files.

    Output columns added:

        prevMdnaLength
        currMdnaLength
        lengthRatio
        logLengthChange
        absLogLengthChange

    where:

        lengthRatio = currMdnaLength / prevMdnaLength
        logLengthChange = log(currMdnaLength) - log(prevMdnaLength)
    """
    pairs_csv = Path(pairs_csv).expanduser().resolve()
    output_dir = Path(output_dir)

    physical_output_root = (
        Path(work_output_dir).expanduser().resolve()
        if work_output_dir is not None
        else output_dir
    )
    physical_output_root.mkdir(parents=True, exist_ok=True)

    pairs = pd.read_csv(
        pairs_csv,
        dtype={
            "edinetCode": "string",
            "prev_docID": "string",
            "curr_docID": "string",
        },
        low_memory=False,
    )

    required_pair_cols = {
        "edinetCode",
        "prev_docID",
        "curr_docID",
        "prev_textChars",
        "curr_textChars",
    }
    missing = required_pair_cols - set(pairs.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in {pairs_csv}: {sorted(missing)}"
        )

    duplicate_pairs = int(
        pairs.duplicated(
            ["edinetCode", "prev_docID", "curr_docID"]
        ).sum()
    )
    if duplicate_pairs:
        raise ValueError(
            f"Pair manifest contains {duplicate_pairs:,} duplicate pair row(s)"
        )

    prev_lengths = pd.to_numeric(
        pairs["prev_textChars"],
        errors="coerce",
    )
    curr_lengths = pd.to_numeric(
        pairs["curr_textChars"],
        errors="coerce",
    )

    invalid_prev = int(prev_lengths.isna().sum() + (prev_lengths <= 0).sum())
    invalid_curr = int(curr_lengths.isna().sum() + (curr_lengths <= 0).sum())
    if invalid_prev or invalid_curr:
        raise ValueError(
            "Stage 3 text-character counts must be positive numeric values: "
            f"invalid_prev={invalid_prev:,}, invalid_curr={invalid_curr:,}"
        )

    result = pairs.copy()
    result["prevMdnaLength"] = prev_lengths.astype(np.int64)
    result["currMdnaLength"] = curr_lengths.astype(np.int64)
    result["lengthRatio"] = (
        result["currMdnaLength"] / result["prevMdnaLength"]
    )
    result["logLengthChange"] = (
        np.log(result["currMdnaLength"])
        - np.log(result["prevMdnaLength"])
    )
    result["absLogLengthChange"] = result["logLengthChange"].abs()

    diagnostics_output = physical_output_root / "pair_diagnostics.csv"
    result.to_csv(
        diagnostics_output,
        index=False,
        encoding="utf-8",
    )

    print(
        f"Pair diagnostics written: {diagnostics_output} "
        f"rows={len(result):,}"
    )
    print(
        "Length diagnostics: "
        f"median_ratio={result['lengthRatio'].median():.6f} "
        f"median_abs_log_change={result['absLogLengthChange'].median():.6f} "
        f"p99_abs_log_change={result['absLogLengthChange'].quantile(0.99):.6f}"
    )

    return result

def run_textual_novelty(
    *,
    pairs_csv: str | Path,
    output_dir: str | Path,
    repo_root: str | Path,
    variant: str = "sudachi_c_num",
    source_root: str | Path | None = None,
    work_output_dir: str | Path | None = None,
    min_df: int = 2,
    max_df: float = 1.0,
    ngram_min: int = 1,
    ngram_max: int = 1,
    sublinear_tf: bool = False,
) -> pd.DataFrame:
    """
    Construct corpus-wide TF-IDF vectors and adjacent-period textual novelty.

    Canonical paths:
        output_dir
            Repository/NAS location used for provenance and downstream handoff.

    Physical paths:
        source_root
            Optional local SSD root containing Stage 4 token variants.
            If omitted, Stage 4 tokens are read from the canonical token root
            inferred as ``repo_root / data/interim/...`` by the adapter.

        work_output_dir
            Optional local SSD root for Stage 5 outputs.  If omitted, outputs
            are written directly under canonical ``output_dir``.

    TF-IDF is fit once on the unique Stage 4 document universe.  Stage 3's
    research-eligible pair manifest is then used only to choose which vector
    pairs receive cosine-similarity calculations.

    With L2-normalized TF-IDF vectors:

        cosine_similarity(i, j) = x_i dot x_j
        novelty(i, j) = 1 - cosine_similarity(i, j)
    """
    pairs_csv = Path(pairs_csv).expanduser().resolve()
    output_dir = Path(output_dir)
    repo_root = Path(repo_root).expanduser().resolve()

    physical_token_root = (
        Path(source_root).expanduser().resolve()
        if source_root is not None
        else None
    )
    if physical_token_root is None:
        raise ValueError(
            "run_textual_novelty requires source_root to identify the "
            "physical Stage 4 token root"
        )

    physical_output_root = (
        Path(work_output_dir).expanduser().resolve()
        if work_output_dir is not None
        else output_dir
    )

    canonical_variant_dir = output_dir / variant
    physical_variant_dir = physical_output_root / variant
    physical_variant_dir.mkdir(parents=True, exist_ok=True)

    try:
        canonical_variant_rel = canonical_variant_dir.relative_to(
            repo_root
        ).as_posix()
    except ValueError as exc:
        raise ValueError(
            "Canonical Stage 5 output_dir must be inside repo_root: "
            f"output_dir={output_dir}, repo_root={repo_root}"
        ) from exc

    manifest = _load_variant_manifest(
        physical_token_root,
        variant,
    )
    token_files = _build_token_file_list(
        physical_token_root,
        variant,
        manifest,
    )

    vectorizer = TfidfVectorizer(
        input="filename",
        encoding="utf-8",
        decode_error="ignore",
        tokenizer=_identity_split,
        preprocessor=None,
        token_pattern=None,
        lowercase=False,
        ngram_range=(ngram_min, ngram_max),
        min_df=min_df,
        max_df=max_df,
        sublinear_tf=sublinear_tf,
        norm="l2",
        dtype=np.float32,
    )

    print(
        f"Fitting TF-IDF: variant={variant} "
        f"documents={len(token_files):,}"
    )

    matrix = vectorizer.fit_transform(token_files)

    print(
        f"TF-IDF complete: shape={matrix.shape} "
        f"nnz={matrix.nnz:,}"
    )

    doc_to_row = {
        (str(row.edinetCode), str(row.docID)): i
        for i, row in enumerate(manifest.itertuples(index=False))
    }

    pairs = pd.read_csv(
        pairs_csv,
        dtype={
            "edinetCode": "string",
            "prev_docID": "string",
            "curr_docID": "string",
        },
    )

    required_pair_cols = {
        "edinetCode",
        "prev_docID",
        "curr_docID",
    }
    missing = required_pair_cols - set(pairs.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in {pairs_csv}: "
            f"{sorted(missing)}"
        )

    prev_rows: list[int] = []
    curr_rows: list[int] = []
    missing_pair_docs: list[tuple[str, str]] = []

    for row in pairs.itertuples(index=False):
        edinet_code = str(row.edinetCode)
        prev_key = (edinet_code, str(row.prev_docID))
        curr_key = (edinet_code, str(row.curr_docID))

        prev_idx = doc_to_row.get(prev_key)
        curr_idx = doc_to_row.get(curr_key)

        if prev_idx is None:
            missing_pair_docs.append(prev_key)
        if curr_idx is None:
            missing_pair_docs.append(curr_key)

        if prev_idx is not None and curr_idx is not None:
            prev_rows.append(prev_idx)
            curr_rows.append(curr_idx)

    if missing_pair_docs:
        examples = ", ".join(
            f"{edinet}/{docid}"
            for edinet, docid in missing_pair_docs[:10]
        )
        raise ValueError(
            f"{len(missing_pair_docs):,} pair document reference(s) "
            f"were absent from the Stage 4 variant universe. "
            f"Examples: {examples}"
        )

    prev_matrix = matrix[prev_rows]
    curr_matrix = matrix[curr_rows]

    # Vectorized row-wise sparse dot products; no N x N matrix is formed.
    cosine = np.asarray(
        prev_matrix.multiply(curr_matrix).sum(axis=1)
    ).ravel()

    cosine = np.clip(cosine, 0.0, 1.0)
    novelty = 1.0 - cosine

    result = pairs.copy()
    result["variant"] = variant
    result["cosineSimilarity"] = cosine
    result["novelty"] = novelty

    pair_output = physical_variant_dir / "pair_novelty.csv"
    result.to_csv(
        pair_output,
        index=False,
        encoding="utf-8",
    )

    feature_names = vectorizer.get_feature_names_out()

    metadata = {
        "createdAt": _utc_now(),
        "variant": variant,
        "documentCount": int(matrix.shape[0]),
        "pairCount": int(len(result)),
        "vocabularySize": int(matrix.shape[1]),
        "nonzeroEntries": int(matrix.nnz),
        "dtype": str(matrix.dtype),
        "canonicalOutputDir": canonical_variant_rel,
        "outputs": {
            "pairNovelty": (
                f"{canonical_variant_rel}/pair_novelty.csv"
            ),
            "metadata": (
                f"{canonical_variant_rel}/tfidf_metadata.json"
            ),
        },
        "tfidf": {
            "scope": "corpus_wide_unique_documents",
            "ngramRange": [ngram_min, ngram_max],
            "minDf": min_df,
            "maxDf": max_df,
            "sublinearTf": sublinear_tf,
            "norm": "l2",
            "lowercase": False,
            "numberHandling": (
                "numeric magnitudes collapsed to <NUM>; "
                "percentages to <NUM>%; yen amounts to <NUM>円"
                if variant.endswith("_num")
                else "raw numeric tokens retained"
            ),
        },
        "similarity": {
            "measure": "cosine",
            "noveltyDefinition": "1 - cosineSimilarity",
        },
        "cosineSummary": {
            "min": float(result["cosineSimilarity"].min()),
            "p01": float(result["cosineSimilarity"].quantile(0.01)),
            "p05": float(result["cosineSimilarity"].quantile(0.05)),
            "median": float(result["cosineSimilarity"].median()),
            "mean": float(result["cosineSimilarity"].mean()),
            "p95": float(result["cosineSimilarity"].quantile(0.95)),
            "p99": float(result["cosineSimilarity"].quantile(0.99)),
            "max": float(result["cosineSimilarity"].max()),
        },
        "noveltySummary": {
            "min": float(result["novelty"].min()),
            "p01": float(result["novelty"].quantile(0.01)),
            "p05": float(result["novelty"].quantile(0.05)),
            "median": float(result["novelty"].median()),
            "mean": float(result["novelty"].mean()),
            "p95": float(result["novelty"].quantile(0.95)),
            "p99": float(result["novelty"].quantile(0.99)),
            "max": float(result["novelty"].max()),
        },
        "vocabularyPreview": feature_names[:50].tolist(),
    }

    metadata_output = (
        physical_variant_dir / "tfidf_metadata.json"
    )
    with metadata_output.open("w", encoding="utf-8") as f:
        json.dump(
            metadata,
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(
        f"Pair novelty written: {pair_output} "
        f"rows={len(result):,}"
    )
    print(
        f"Metadata written: {metadata_output}"
    )

    return result
