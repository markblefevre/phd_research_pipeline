"""Batch extraction of canonical MD&A text from downloaded EDINET filings.

This module contains the reusable batch implementation used by both the Paper 2
pipeline and the command-line wrapper. Individual ZIP parsing remains in
``mdna_analysis.mdna_extraction``.
"""

from __future__ import annotations

import csv
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .mdna_extraction import extract_mdna_from_zip


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
            raise ValueError(f"{path} missing required columns: {sorted(missing)}")

        return [dict(row) for row in reader]


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

    try:
        return output_path.stat().st_size > 0
    except FileNotFoundError:
        return False


def run_mdna_batch(
    *,
    filings_csv: Path,
    raw_dir: Path,
    output_dir: Path,
    manifest_path: Path,
    checkpoint_every: int = 100,
    progress_every: int = 100,
    retry_failures: bool = False,
    force: bool = False,
    limit: int | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Extract canonical MD&A text for all filings in a Stage 1 manifest.

    The extraction manifest makes the operation resumable. Prior successful rows
    are skipped only when their canonical text output still exists and is non-empty.
    Failed rows are skipped unless ``retry_failures`` is enabled.
    """
    if checkpoint_every < 1:
        raise ValueError("checkpoint_every must be >= 1")
    if progress_every < 1:
        raise ValueError("progress_every must be >= 1")
    if limit is not None and limit < 1:
        raise ValueError("limit must be >= 1")

    filings_csv = Path(filings_csv)
    raw_dir = Path(raw_dir)
    output_dir = Path(output_dir)
    manifest_path = Path(manifest_path)
    logger = logger or logging.getLogger(__name__)

    filings = _read_filings(filings_csv)
    if limit is not None:
        filings = filings[:limit]

    manifest = _load_existing_manifest(manifest_path)

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

            zip_path = raw_dir / edinet_code / doc_id / f"{doc_id}.zip"
            output_path = output_dir / edinet_code / f"{doc_id}.txt"
            prior = manifest.get(doc_id)

            if not force and _is_successful_prior_result(prior, output_path):
                counters["skipped_ok"] += 1
                if index % progress_every == 0:
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
                not force
                and prior
                and str(prior.get("status") or "") != "ok"
                and not retry_failures
            ):
                counters["skipped_failure"] += 1
                continue

            processed_at = datetime.now().isoformat(timespec="seconds")

            # Construct the exact expected raw path; never walk the EDINET tree.
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

                if counters["processed"] % checkpoint_every == 0:
                    _write_manifest_atomic(manifest, manifest_path)
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

            if counters["processed"] % checkpoint_every == 0:
                _write_manifest_atomic(manifest, manifest_path)

            if index % progress_every == 0:
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
        _write_manifest_atomic(manifest, manifest_path)
        raise

    _write_manifest_atomic(manifest, manifest_path)

    elapsed = time.perf_counter() - started
    return {
        "filings_input": len(filings),
        "processed": counters["processed"],
        "successful": counters["ok"],
        "missing_zip": counters["missing_zip"],
        "failed": counters["failed"],
        "skipped_successful": counters["skipped_ok"],
        "skipped_failures": counters["skipped_failure"],
        "manifest_rows": len(manifest),
        "elapsed_seconds": elapsed,
        "manifest": str(manifest_path),
        "output_dir": str(output_dir),
    }


def print_mdna_batch_summary(summary: dict[str, Any]) -> None:
    """Print the human-readable summary used by the standalone CLI."""
    print()
    print("MD&A extraction summary")
    print("=" * 24)
    print(f"filings input:       {summary['filings_input']:,}")
    print(f"processed this run:  {summary['processed']:,}")
    print(f"successful:          {summary['successful']:,}")
    print(f"missing ZIP:         {summary['missing_zip']:,}")
    print(f"other failures:      {summary['failed']:,}")
    print(f"skipped successful:  {summary['skipped_successful']:,}")
    print(f"skipped failures:    {summary['skipped_failures']:,}")
    print(f"manifest rows:       {summary['manifest_rows']:,}")
    print(f"elapsed seconds:     {summary['elapsed_seconds']:,.1f}")
    print(f"manifest:            {summary['manifest']}")
    print(f"text root:           {summary['output_dir']}")
