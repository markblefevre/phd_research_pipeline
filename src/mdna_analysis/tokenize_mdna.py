from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
import unicodedata
import time

import pandas as pd
from sudachipy import SplitMode

from src.utils.text_utils import tokenize_ja_safe


VARIANT_CONFIG = {
    "sudachi_a_raw": SplitMode.A,
    "sudachi_b_raw": SplitMode.B,
    "sudachi_c_raw": SplitMode.C,
}


def keep_token(token: str) -> bool:
    """
    Keep lexical/content tokens while dropping whitespace-only and
    punctuation/symbol-only tokens.

    Punctuation is deliberately preserved *during* Sudachi tokenization and
    filtered only afterward.
    """
    stripped = token.strip()
    if not stripped:
        return False

    # Drop a token only when every character is punctuation or a symbol.
    if all(unicodedata.category(ch)[0] in {"P", "S"} for ch in stripped):
        return False

    return True


def preprocess_tokens(tokens: Iterable[str]) -> list[str]:
    """Apply Stage 4 post-tokenization filtering."""
    return [tok.strip() for tok in tokens if keep_token(tok)]


def _process_document(job: dict, variant: str, overwrite: bool = False) -> dict:
    """
    Tokenize one MD&A document.

    Each process owns its own imported Sudachi state, which avoids sharing
    tokenizer instances across workers.
    """
    source_path = Path(job["source_path"])
    token_path = Path(job["token_path"])

    base = {
        "docID": job["doc_id"],
        "edinetCode": job["edinet_code"],
        # Persist logical/canonical paths, not temporary physical scratch paths.
        "sourcePath": job["source_manifest_path"],
        "tokenPath": job["token_manifest_path"],
        "variant": variant,
    }

    try:
        if not source_path.exists():
            return {
                **base,
                "status": "missing_source",
                "textChars": 0,
                "textBytes": 0,
                "tokenCount": 0,
                "error": f"Source file not found: {source_path}",
                "processedAt": _utc_now(),
            }

        if token_path.exists() and not overwrite:
            text = source_path.read_text(encoding="utf-8", errors="ignore")
            with token_path.open("r", encoding="utf-8") as f:
                token_count = sum(1 for line in f if line.rstrip("\n"))

            return {
                **base,
                "status": "skipped_existing",
                "textChars": len(text),
                "textBytes": len(text.encode("utf-8")),
                "tokenCount": token_count,
                "error": "",
                "processedAt": _utc_now(),
            }

        text = source_path.read_text(encoding="utf-8", errors="ignore")

        tokens = tokenize_ja_safe(
            text,
            split_mode=VARIANT_CONFIG[variant],
            max_bytes=48000,
            normalize=True,
            prefer_natural_boundaries=True,
        )
        tokens = preprocess_tokens(tokens)

        token_path.parent.mkdir(parents=True, exist_ok=True)
        with token_path.open("w", encoding="utf-8", newline="\n") as f:
            for token in tokens:
                f.write(token)
                f.write("\n")

        return {
            **base,
            "status": "ok",
            "textChars": len(text),
            "textBytes": len(text.encode("utf-8")),
            "tokenCount": len(tokens),
            "error": "",
            "processedAt": _utc_now(),
        }

    except Exception as exc:
        return {
            **base,
            "status": "error",
            "textChars": 0,
            "textBytes": 0,
            "tokenCount": 0,
            "error": repr(exc),
            "processedAt": _utc_now(),
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_document_jobs(
    pairs_csv: str | Path,
    output_dir: str | Path,
    repo_root: str | Path,
    source_root: str | Path | None = None,
    work_output_dir: str | Path | None = None,
    variant: str = "sudachi_c_raw",
) -> list[dict]:
    """
    Build the unique document universe from research-eligible adjacent pairs.

    Stage 3 stores canonical source paths.  For I/O, relative source paths are
    resolved against ``repo_root`` unless ``source_root`` is supplied.  A
    supplied ``source_root`` is treated as an alternate physical MD&A root
    containing ``<edinetCode>/<docID>.txt`` (for example, a local SSD scratch
    copy).

    ``output_dir`` remains the canonical Stage 4 output location.  If
    ``work_output_dir`` is supplied, token files are physically written there
    while the manifest still records canonical repo-relative token paths.
    """
    pairs_csv = Path(pairs_csv)
    if variant not in VARIANT_CONFIG:
        raise ValueError(
            f"Unsupported tokenization variant {variant!r}; "
            f"expected one of {sorted(VARIANT_CONFIG)}"
        )

    output_dir = Path(output_dir)
    repo_root = Path(repo_root).expanduser().resolve()

    physical_source_root = (
        Path(source_root).expanduser().resolve()
        if source_root is not None
        else None
    )
    physical_output_dir = (
        Path(work_output_dir).expanduser().resolve()
        if work_output_dir is not None
        else output_dir
    )

    df = pd.read_csv(
        pairs_csv,
        dtype={
            "edinetCode": "string",
            "prev_docID": "string",
            "curr_docID": "string",
            "prev_outputPath": "string",
            "curr_outputPath": "string",
        },
    )

    required = {
        "edinetCode",
        "prev_docID",
        "curr_docID",
        "prev_outputPath",
        "curr_outputPath",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in {pairs_csv}: {sorted(missing)}"
        )

    # Map each unique issuer/document pair to its canonical Stage 2 text path.
    docs: dict[tuple[str, str], str] = {}

    for row in df[
        [
            "edinetCode",
            "prev_docID",
            "curr_docID",
            "prev_outputPath",
            "curr_outputPath",
        ]
    ].itertuples(index=False):
        edinet_code = str(row.edinetCode)

        candidates = (
            (str(row.prev_docID), str(row.prev_outputPath)),
            (str(row.curr_docID), str(row.curr_outputPath)),
        )

        for doc_id, source_path in candidates:
            key = (edinet_code, doc_id)

            if key in docs and docs[key] != source_path:
                raise ValueError(
                    "Conflicting source paths for "
                    f"edinetCode={edinet_code}, docID={doc_id}: "
                    f"{docs[key]!r} vs {source_path!r}"
                )

            docs[key] = source_path

    jobs = []

    for (edinet_code, doc_id), source_manifest_path in sorted(docs.items()):
        logical_source = Path(source_manifest_path)

        if physical_source_root is not None:
            source_path = physical_source_root / edinet_code / f"{doc_id}.txt"
        elif logical_source.is_absolute():
            source_path = logical_source
        else:
            source_path = repo_root / logical_source

        canonical_token_path = (
            output_dir / variant / edinet_code / f"{doc_id}.tokens.txt"
        )
        physical_token_path = (
            physical_output_dir / variant / edinet_code / f"{doc_id}.tokens.txt"
        )

        try:
            token_manifest_path = canonical_token_path.relative_to(
                repo_root
            ).as_posix()
        except ValueError as exc:
            raise ValueError(
                "Canonical Stage 4 output_dir must be inside repo_root: "
                f"output_dir={output_dir}, repo_root={repo_root}"
            ) from exc

        jobs.append(
            {
                "edinet_code": edinet_code,
                "doc_id": doc_id,
                "source_path": str(source_path),
                "source_manifest_path": source_manifest_path,
                "token_path": str(physical_token_path),
                "token_manifest_path": token_manifest_path,
            }
        )

    return jobs

def run_tokenization(
    pairs_csv: str | Path,
    output_dir: str | Path,
    repo_root: str | Path,
    manifest_csv: str | Path | None = None,
    max_workers: int = 6,
    overwrite: bool = False,
    source_root: str | Path | None = None,
    work_output_dir: str | Path | None = None,
    variant: str = "sudachi_c_raw",
) -> pd.DataFrame:
    """
    Run one Stage 4 tokenization variant for the unique eligible MD&A universe.

    Supported variants map directly to Sudachi split modes:
      - sudachi_a_raw -> SplitMode.A
      - sudachi_b_raw -> SplitMode.B
      - sudachi_c_raw -> SplitMode.C

    All variants use NFKC normalization, natural-boundary chunking, retain raw
    numbers, and remove whitespace-only and punctuation/symbol-only tokens.
    """
    output_dir = Path(output_dir)
    repo_root = Path(repo_root).expanduser().resolve()

    physical_output_dir = (
        Path(work_output_dir).expanduser().resolve()
        if work_output_dir is not None
        else output_dir
    )
    physical_output_dir.mkdir(parents=True, exist_ok=True)

    if manifest_csv is None:
        manifest_csv = physical_output_dir / variant / "manifest.csv"
    else:
        manifest_csv = Path(manifest_csv).expanduser()

    jobs = build_document_jobs(
        pairs_csv=pairs_csv,
        output_dir=output_dir,
        repo_root=repo_root,
        source_root=source_root,
        work_output_dir=physical_output_dir,
        variant=variant,
    )

    rows: list[dict] = []

    start_time = time.perf_counter()
    total = len(jobs)

    if max_workers <= 1:
        for completed, job in enumerate(jobs, start=1):
            rows.append(_process_document(job, variant, overwrite=overwrite))

            if completed % 100 == 0 or completed == total:
                elapsed = time.perf_counter() - start_time
                rate = completed / elapsed if elapsed > 0 else 0.0
                remaining = (total - completed) / rate if rate > 0 else 0.0

                print(
                    f"Tokenization progress: "
                    f"{completed:,}/{total:,} "
                    f"({100 * completed / total:.1f}%) | "
                    f"{rate:.1f} docs/s | "
                    f"elapsed={elapsed / 60:.1f} min | "
                    f"remaining={remaining / 60:.1f} min"
                )
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_process_document, job, variant, overwrite): job
                for job in jobs
            }

            for completed, future in enumerate(as_completed(futures), start=1):
                rows.append(future.result())

                if completed % 100 == 0 or completed == total:
                    elapsed = time.perf_counter() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0.0
                    remaining = (total - completed) / rate if rate > 0 else 0.0

                    print(
                        f"Tokenization progress: "
                        f"{completed:,}/{total:,} "
                        f"({100 * completed / total:.1f}%) | "
                        f"{rate:.1f} docs/s | "
                        f"elapsed={elapsed / 60:.1f} min | "
                        f"remaining={remaining / 60:.1f} min"
                    )

    manifest = pd.DataFrame(rows).sort_values(
        ["edinetCode", "docID"],
        kind="stable",
    )

    manifest_csv.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(manifest_csv, index=False, encoding="utf-8")

    return manifest
