#!/usr/bin/env python3
"""
Batch-extract canonical MD&A text for Paper 2 from downloaded EDINET ZIP files.

Inputs
------
filings.csv produced by Stage 1, containing at minimum:
    docID, edinetCode

Expected raw ZIP layout
-----------------------
data/raw/paper2/edinet/<edinetCode>/<docID>/<docID>.zip

Outputs
-------
1. Canonical MD&A text:
   data/interim/paper2/mdna/<edinetCode>/<docID>.txt

2. Extraction manifest:
   data/interim/paper2/mdna/extraction_manifest.csv

Design
------
- Never walks the raw EDINET directory tree.
- Constructs each ZIP path directly from filings.csv.
- Uses mdna_extraction.extract_mdna_from_zip for the validated lxml parser.
- Is resumable from extraction_manifest.csv.
- Writes text and manifest atomically.
- Records missing ZIPs and extraction failures instead of aborting the batch.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from mdna_analysis.mdna_extraction import extract_mdna_from_zip


MANIFEST_FIELDS = [
    "docID",
    "edinetCode",
    "secCode",
    "filerName",
    "periodStart",
    "periodEnd",
    "submitDateTime",
    "zipPath",
    "outputPath",
    "status",
    "method",
    "matchedLocalName",
    "fallbackScore",
    "xbrlMember",
    "textChars",
    "error",
    "processedAt",
]


def _read_filings(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"filings.csv not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"No header found in {path}")

        required = {"docID", "edinetCode"}
        missing = required - set(reader.fieldnames)
        if missing:
            raise ValueError(
                f"{path} missing required columns: {sorted(missing)}"
            )

        rows = [dict(row) for row in reader]

    return rows


def _load_existing_manifest(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return {}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return {
            str(row.get("docID") or "").strip(): dict(row)
            for row in reader
            if str(row.get("docID") or "").strip()
        }


def _write_manifest_atomic(
    rows_by_docid: dict[str, dict[str, Any]],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")

    rows = list(rows_by_docid.values())
    rows.sort(
        key=lambda row: (
            str(row.get("submitDateTime") or ""),
            str(row.get("docID") or ""),
        )
    )

    try:
        with temp.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=MANIFEST_FIELDS,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(rows)

        temp.replace(path)
    finally:
        if temp.exists():
            temp.unlink()


def _write_text_atomic(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")

    try:
        temp.write_text(text.rstrip() + "\n", encoding="utf-8")
        temp.replace(path)
    finally:
        if temp.exists():
            temp.unlink()


def _copy_filing_fields(row: dict[str, str]) -> dict[str, Any]:
    return {
        "docID": str(row.get("docID") or "").strip(),
        "edinetCode": str(row.get("edinetCode") or "").strip(),
        "secCode": str(row.get("secCode") or "").strip(),
        "filerName": str(row.get("filerName") or "").strip(),
        "periodStart": str(row.get("periodStart") or "").strip(),
        "periodEnd": str(row.get("periodEnd") or "").strip(),
        "submitDateTime": str(row.get("submitDateTime") or "").strip(),
    }


def _is_successful_prior_result(
    prior: dict[str, str] | None,
    output_path: Path,
) -> bool:
    if not prior:
        return False

    if str(prior.get("status") or "") != "ok":
        return False

    # For successful rows, require the canonical output to still exist and be non-empty.
    try:
        return output_path.stat().st_size > 0
    except FileNotFoundError:
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch-extract Paper 2 MD&A text from EDINET ZIP files."
    )
    parser.add_argument(
        "--filings",
        type=Path,
        default=Path("data/interim/paper2/edinet/filings.csv"),
        help="Stage 1 filing manifest.",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw/paper2/edinet"),
        help="Root directory containing downloaded EDINET ZIPs.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/interim/paper2/mdna"),
        help="Root directory for canonical MD&A text.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("data/interim/paper2/mdna/extraction_manifest.csv"),
        help="Extraction manifest path.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=100,
        help="Atomically rewrite extraction manifest every N processed rows.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Log progress every N input rows.",
    )
    parser.add_argument(
        "--retry-failures",
        action="store_true",
        help="Retry rows already present in the manifest with non-ok status.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess all filings, including prior successful extractions.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional test limit on number of filing rows processed.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.checkpoint_every < 1:
        raise SystemExit("--checkpoint-every must be >= 1")
    if args.progress_every < 1:
        raise SystemExit("--progress-every must be >= 1")
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be >= 1")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logger = logging.getLogger("extract_mdna_batch")

    filings = _read_filings(args.filings)
    if args.limit is not None:
        filings = filings[: args.limit]

    manifest = _load_existing_manifest(args.manifest)

    logger.info(
        "Loaded filings=%s existing_manifest_rows=%s",
        len(filings),
        len(manifest),
    )

    counters = {
        "processed": 0,
        "ok": 0,
        "skipped_ok": 0,
        "skipped_failure": 0,
        "missing_zip": 0,
        "failed": 0,
    }

    started = time.perf_counter()

    try:
        for index, filing in enumerate(filings, start=1):
            base = _copy_filing_fields(filing)
            doc_id = base["docID"]
            edinet_code = base["edinetCode"]

            if not doc_id or not edinet_code:
                logger.warning(
                    "Skipping malformed manifest row index=%s docID=%r edinetCode=%r",
                    index,
                    doc_id,
                    edinet_code,
                )
                continue

            zip_path = (
                args.raw_dir
                / edinet_code
                / doc_id
                / f"{doc_id}.zip"
            )
            output_path = (
                args.output_dir
                / edinet_code
                / f"{doc_id}.txt"
            )

            prior = manifest.get(doc_id)

            if not args.force and _is_successful_prior_result(prior, output_path):
                counters["skipped_ok"] += 1
                if index % args.progress_every == 0:
                    logger.info(
                        "Progress %s/%s ok=%s skipped_ok=%s failed=%s",
                        index,
                        len(filings),
                        counters["ok"],
                        counters["skipped_ok"],
                        counters["failed"] + counters["missing_zip"],
                    )
                continue

            if (
                not args.force
                and prior
                and str(prior.get("status") or "") != "ok"
                and not args.retry_failures
            ):
                counters["skipped_failure"] += 1
                continue

            processed_at = datetime.now().isoformat(timespec="seconds")

            # Do not pre-walk or search the raw tree. We know the exact expected path.
            # We do one targeted stat here only to distinguish a missing raw ZIP cleanly.
            try:
                if zip_path.stat().st_size <= 0:
                    raise FileNotFoundError(f"ZIP is empty: {zip_path}")
            except FileNotFoundError as exc:
                manifest[doc_id] = {
                    **base,
                    "zipPath": str(zip_path),
                    "outputPath": str(output_path),
                    "status": "missing_zip",
                    "method": "",
                    "matchedLocalName": "",
                    "fallbackScore": "",
                    "xbrlMember": "",
                    "textChars": 0,
                    "error": str(exc),
                    "processedAt": processed_at,
                }
                counters["processed"] += 1
                counters["missing_zip"] += 1
                continue

            result = extract_mdna_from_zip(zip_path)

            if result.status == "ok" and result.mdna_text:
                _write_text_atomic(result.mdna_text, output_path)
                counters["ok"] += 1
            else:
                counters["failed"] += 1

            manifest[doc_id] = {
                **base,
                "zipPath": str(zip_path),
                "outputPath": str(output_path),
                "status": result.status,
                "method": result.method or "",
                "matchedLocalName": result.matched_local_name or "",
                "fallbackScore": (
                    result.fallback_score
                    if result.fallback_score is not None
                    else ""
                ),
                "xbrlMember": result.xbrl_member or "",
                "textChars": len(result.mdna_text) if result.mdna_text else 0,
                "error": result.error or "",
                "processedAt": processed_at,
            }
            counters["processed"] += 1

            if counters["processed"] % args.checkpoint_every == 0:
                _write_manifest_atomic(manifest, args.manifest)

            if index % args.progress_every == 0:
                elapsed = time.perf_counter() - started
                logger.info(
                    "Progress %s/%s processed=%s ok=%s missing_zip=%s "
                    "failed=%s skipped_ok=%s elapsed=%.1fs",
                    index,
                    len(filings),
                    counters["processed"],
                    counters["ok"],
                    counters["missing_zip"],
                    counters["failed"],
                    counters["skipped_ok"],
                    elapsed,
                )

    except KeyboardInterrupt:
        logger.warning("Interrupted; writing extraction manifest before exit.")
        _write_manifest_atomic(manifest, args.manifest)
        return 130

    _write_manifest_atomic(manifest, args.manifest)

    elapsed = time.perf_counter() - started

    print()
    print("MD&A extraction summary")
    print("=" * 24)
    print(f"filings input:       {len(filings):,}")
    print(f"processed this run:  {counters['processed']:,}")
    print(f"successful:          {counters['ok']:,}")
    print(f"missing ZIP:         {counters['missing_zip']:,}")
    print(f"other failures:      {counters['failed']:,}")
    print(f"skipped successful:  {counters['skipped_ok']:,}")
    print(f"skipped failures:    {counters['skipped_failure']:,}")
    print(f"manifest rows:       {len(manifest):,}")
    print(f"elapsed seconds:     {elapsed:,.1f}")
    print(f"manifest:            {args.manifest}")
    print(f"text root:           {args.output_dir}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
