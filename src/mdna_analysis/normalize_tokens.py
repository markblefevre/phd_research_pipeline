from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import re
import time

import pandas as pd


NUM_TOKEN = "<NUM>"

# Arabic numeric atom after Stage 4 NFKC normalization.
NUMERIC_ATOM = r"""
    [+-]?
    (?:
        \d{1,3}(?:,\d{3})+(?:\.\d+)?
        |
        \d+(?:\.\d+)?
        |
        \.\d+
    )
"""

# Japanese scale markers are part of numeric magnitude, not semantic units.
# Consecutive markers are allowed because forms such as 百万, 千万, 千億 occur.
SCALE_MARKERS = ("十", "百", "千", "万", "億", "兆")
_SCALE_PATTERN = "|".join(re.escape(x) for x in SCALE_MARKERS)

# Examples matched:
#   123
#   1億
#   1億2万
#   3百万
#   1,002億7千7百万
#   1兆1千億
#   1万1,400
NUMERIC_MAGNITUDE_PATTERN = rf"""
    (?:
        {NUMERIC_ATOM}(?:(?:{_SCALE_PATTERN}))+ 
    )*
    {NUMERIC_ATOM}?
"""

NUMERIC_MAGNITUDE_RE = re.compile(
    rf"^(?={NUMERIC_ATOM}|.*(?:{_SCALE_PATTERN}))"
    rf"(?:{NUMERIC_MAGNITUDE_PATTERN})$",
    re.VERBOSE,
)

PERCENT_RE = re.compile(
    rf"^(?P<magnitude>{NUMERIC_MAGNITUDE_PATTERN})(?:%|％)$",
    re.VERBOSE,
)

YEN_RE = re.compile(
    rf"^(?P<magnitude>{NUMERIC_MAGNITUDE_PATTERN})円$",
    re.VERBOSE,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_numeric_token(token: str) -> bool:
    """Return True when the entire token is a numeric magnitude."""
    if not token:
        return False
    return bool(NUMERIC_MAGNITUDE_RE.fullmatch(token))


def normalize_numeric_token(token: str) -> str:
    """
    Collapse numeric expressions to semantic placeholders.

    Any numeric magnitude:
        123                 -> <NUM>
        1億                 -> <NUM>
        1億2万              -> <NUM>
        1,002億7千7百万     -> <NUM>

    Any percentage:
        12%                 -> <NUM>%
        12.5％              -> <NUM>%

    Any yen-denominated amount:
        100円               -> <NUM>円
        3百万円             -> <NUM>円
        1億2万円            -> <NUM>円

    Other mixed expressions are left unchanged.
    """
    if token and PERCENT_RE.fullmatch(token):
        return f"{NUM_TOKEN}%"

    if token and YEN_RE.fullmatch(token):
        return f"{NUM_TOKEN}円"

    if is_numeric_token(token):
        return NUM_TOKEN

    return token


def is_normalized_numeric_token(token: str) -> bool:
    """Return True for tokens produced by numeric normalization."""
    return token in {
        NUM_TOKEN,
        f"{NUM_TOKEN}%",
        f"{NUM_TOKEN}円",
    }


def _normalize_document(job: dict, overwrite: bool = False) -> dict:
    """Derive one numeric-normalized token file from one raw token file."""
    source_path = Path(job["source_path"])
    token_path = Path(job["token_path"])

    base = {
        "docID": job["doc_id"],
        "edinetCode": job["edinet_code"],
        "sourceVariant": job["source_variant"],
        "variant": job["output_variant"],
        "sourcePath": job["source_manifest_path"],
        "tokenPath": job["token_manifest_path"],
    }

    try:
        if not source_path.exists():
            return {
                **base,
                "status": "missing_source",
                "tokenCount": 0,
                "replacementCount": 0,
                "replacementPct": 0.0,
                "error": f"Source token file not found: {source_path}",
                "processedAt": _utc_now(),
            }

        if token_path.exists() and not overwrite:
            token_count = 0
            replacement_count = 0
            with token_path.open("r", encoding="utf-8") as f:
                for line in f:
                    token = line.rstrip("\n")
                    token_count += 1
                    if is_normalized_numeric_token(token):
                        replacement_count += 1

            replacement_pct = replacement_count / token_count if token_count else 0.0
            return {
                **base,
                "status": "skipped_existing",
                "tokenCount": token_count,
                "replacementCount": replacement_count,
                "replacementPct": replacement_pct,
                "error": "",
                "processedAt": _utc_now(),
            }

        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_count = 0
        replacement_count = 0

        with source_path.open("r", encoding="utf-8") as src, token_path.open(
            "w", encoding="utf-8", newline="\n"
        ) as dst:
            for line in src:
                token = line.rstrip("\n")
                token_count += 1
                normalized = normalize_numeric_token(token)
                if normalized != token:
                    replacement_count += 1
                dst.write(normalized)
                dst.write("\n")

        replacement_pct = replacement_count / token_count if token_count else 0.0
        return {
            **base,
            "status": "ok",
            "tokenCount": token_count,
            "replacementCount": replacement_count,
            "replacementPct": replacement_pct,
            "error": "",
            "processedAt": _utc_now(),
        }

    except Exception as exc:
        return {
            **base,
            "status": "error",
            "tokenCount": 0,
            "replacementCount": 0,
            "replacementPct": 0.0,
            "error": repr(exc),
            "processedAt": _utc_now(),
        }


def build_normalization_jobs(
    *,
    output_dir: str | Path,
    repo_root: str | Path,
    source_variant: str,
    output_variant: str,
    work_output_dir: str | Path | None = None,
) -> list[dict]:
    """Build normalization jobs from the source variant manifest."""
    output_dir = Path(output_dir)
    repo_root = Path(repo_root).expanduser().resolve()
    physical_root = (
        Path(work_output_dir).expanduser().resolve()
        if work_output_dir is not None
        else output_dir
    )

    source_manifest_csv = physical_root / source_variant / "manifest.csv"
    if not source_manifest_csv.exists():
        raise FileNotFoundError(f"Missing source variant manifest: {source_manifest_csv}")

    source_manifest = pd.read_csv(
        source_manifest_csv,
        dtype={
            "docID": "string",
            "edinetCode": "string",
            "sourcePath": "string",
            "tokenPath": "string",
            "variant": "string",
            "status": "string",
        },
    )

    required = {"docID", "edinetCode", "tokenPath", "variant", "status"}
    missing = required - set(source_manifest.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in {source_manifest_csv}: {sorted(missing)}"
        )

    if source_manifest.duplicated(["edinetCode", "docID"]).any():
        raise ValueError(f"Duplicate documents in source manifest: {source_manifest_csv}")

    bad = source_manifest[source_manifest["status"].isin(["missing_source", "error"])]
    if not bad.empty:
        raise ValueError(
            f"Source variant {source_variant} manifest contains {len(bad)} failed document(s)"
        )

    variant_values = set(source_manifest["variant"].dropna().astype(str))
    if variant_values != {source_variant}:
        raise ValueError(
            f"Source manifest variant mismatch: expected {source_variant!r}, "
            f"found {sorted(variant_values)}"
        )

    jobs: list[dict] = []
    for row in source_manifest.itertuples(index=False):
        edinet_code = str(row.edinetCode)
        doc_id = str(row.docID)

        physical_source_path = (
            physical_root / source_variant / edinet_code / f"{doc_id}.tokens.txt"
        )
        physical_token_path = (
            physical_root / output_variant / edinet_code / f"{doc_id}.tokens.txt"
        )

        canonical_source_path = (
            output_dir / source_variant / edinet_code / f"{doc_id}.tokens.txt"
        )
        canonical_token_path = (
            output_dir / output_variant / edinet_code / f"{doc_id}.tokens.txt"
        )

        try:
            source_manifest_path = canonical_source_path.relative_to(repo_root).as_posix()
            token_manifest_path = canonical_token_path.relative_to(repo_root).as_posix()
        except ValueError as exc:
            raise ValueError(
                "Canonical Stage 4 output_dir must be inside repo_root: "
                f"output_dir={output_dir}, repo_root={repo_root}"
            ) from exc

        jobs.append(
            {
                "doc_id": doc_id,
                "edinet_code": edinet_code,
                "source_variant": source_variant,
                "output_variant": output_variant,
                "source_path": str(physical_source_path),
                "token_path": str(physical_token_path),
                "source_manifest_path": source_manifest_path,
                "token_manifest_path": token_manifest_path,
            }
        )

    return jobs


def run_numeric_normalization(
    *,
    output_dir: str | Path,
    repo_root: str | Path,
    source_variant: str,
    output_variant: str,
    work_output_dir: str | Path | None = None,
    manifest_csv: str | Path | None = None,
    max_workers: int = 6,
    overwrite: bool = False,
) -> pd.DataFrame:
    """Derive one <NUM>-normalized Stage 4 variant from one raw variant."""
    output_dir = Path(output_dir)
    repo_root = Path(repo_root).expanduser().resolve()
    physical_root = (
        Path(work_output_dir).expanduser().resolve()
        if work_output_dir is not None
        else output_dir
    )
    physical_root.mkdir(parents=True, exist_ok=True)

    if manifest_csv is None:
        manifest_csv = physical_root / output_variant / "manifest.csv"
    else:
        manifest_csv = Path(manifest_csv).expanduser()

    jobs = build_normalization_jobs(
        output_dir=output_dir,
        repo_root=repo_root,
        source_variant=source_variant,
        output_variant=output_variant,
        work_output_dir=physical_root,
    )

    rows: list[dict] = []
    total = len(jobs)
    start_time = time.perf_counter()

    def report(completed: int) -> None:
        if completed % 100 != 0 and completed != total:
            return
        elapsed = time.perf_counter() - start_time
        rate = completed / elapsed if elapsed > 0 else 0.0
        remaining = (total - completed) / rate if rate > 0 else 0.0
        print(
            f"Numeric normalization progress [{output_variant}]: "
            f"{completed:,}/{total:,} "
            f"({100 * completed / total:.1f}%) | "
            f"{rate:.1f} docs/s | "
            f"elapsed={elapsed / 60:.1f} min | "
            f"remaining={remaining / 60:.1f} min"
        )

    if max_workers <= 1:
        for completed, job in enumerate(jobs, start=1):
            rows.append(_normalize_document(job, overwrite=overwrite))
            report(completed)
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_normalize_document, job, overwrite): job
                for job in jobs
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                rows.append(future.result())
                report(completed)

    manifest = pd.DataFrame(rows).sort_values(
        ["edinetCode", "docID"], kind="stable"
    )
    manifest_csv.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(manifest_csv, index=False, encoding="utf-8")
    return manifest
