#!/usr/bin/env python3
"""
Download EDINET filings using the EDINET API v2.

Designed to be called from scripts/paper2/run_pipeline.py via
run_edinet_download(...), but can also be adapted for standalone use.

Primary use for Paper 2:
- enumerate filings by submission date
- keep Annual Securities Reports (docTypeCode == "120")
- restrict the Paper 2 universe to listed companies
- optionally restrict to a supplied set of EDINET codes
- save filing metadata
- download raw filing ZIPs for downstream MD&A extraction
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import requests


BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"
DOCUMENT_LIST_URL = f"{BASE_URL}/documents.json"
DOCUMENT_DOWNLOAD_URL = f"{BASE_URL}/documents"


def _date_range(start_date: str, end_date: str) -> Iterable[date]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if end < start:
        raise ValueError(f"end_date {end_date} is before start_date {start_date}")

    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    retries: int,
    logger: logging.Logger,
    **kwargs: Any,
) -> requests.Response:
    last_exc: Exception | None = None

    for attempt in range(retries + 1):
        try:
            response = session.request(method, url, timeout=60, **kwargs)

            if response.status_code == 429:
                wait = min(2 ** attempt, 30)
                logger.warning("EDINET rate limited request; sleeping %ss", wait)
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= retries:
                raise

            wait = min(2 ** attempt, 30)
            logger.warning(
                "EDINET request failed (%s); retry %s/%s in %ss",
                exc,
                attempt + 1,
                retries,
                wait,
            )
            time.sleep(wait)

    assert last_exc is not None
    raise last_exc


def _load_edinet_codes(path: Path | None) -> set[str] | None:
    """
    Return a set of EDINET codes, or None for an unrestricted universe.

    Accepts a CSV containing a column named one of:
    edinet_code, edinetCode, EDINETCode, EDINET_CODE.
    """
    if path is None:
        return None

    if not path.exists():
        raise FileNotFoundError(f"EDINET codes CSV not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"No header found in EDINET codes CSV: {path}")

        candidates = ("edinet_code", "edinetCode", "EDINETCode", "EDINET_CODE")
        col = next((c for c in candidates if c in reader.fieldnames), None)
        if col is None:
            raise ValueError(
                f"Could not find EDINET-code column in {path}. "
                f"Expected one of {candidates}; found {reader.fieldnames}"
            )

        return {
            str(row[col]).strip()
            for row in reader
            if row.get(col) and str(row[col]).strip()
        }


def _write_metadata(rows: list[dict[str, Any]], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        # Still create a useful file.
        out_csv.write_text("", encoding="utf-8")
        return

    preferred = [
        "docID",
        "edinetCode",
        "secCode",
        "JCN",
        "filerName",
        "fundCode",
        "ordinanceCode",
        "formCode",
        "docTypeCode",
        "periodStart",
        "periodEnd",
        "submitDateTime",
        "docDescription",
        "issuerEdinetCode",
        "subjectEdinetCode",
        "subsidiaryEdinetCode",
        "currentReportReason",
        "parentDocID",
        "opeDateTime",
        "withdrawalStatus",
        "docInfoEditStatus",
        "disclosureStatus",
        "xbrlFlag",
        "pdfFlag",
        "attachDocFlag",
        "englishDocFlag",
        "csvFlag",
        "legalStatus",
    ]

    all_keys = set().union(*(row.keys() for row in rows))
    fieldnames = [k for k in preferred if k in all_keys]
    fieldnames += sorted(all_keys - set(fieldnames))

    # Write atomically so an interrupted run cannot leave a truncated CSV.
    tmp = out_csv.with_suffix(out_csv.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(out_csv)


def _load_last_completed_date(
    checkpoint_path: Path,
    *,
    metadata_csv: Path,
    resume: bool,
    logger: logging.Logger,
) -> date | None:
    """Return the last fully completed listing date, if the checkpoint is usable."""
    if not resume or not metadata_csv.exists() or not checkpoint_path.exists():
        return None

    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        value = payload.get("last_completed_date")
        if not value:
            return None
        completed = datetime.strptime(value, "%Y-%m-%d").date()
        logger.info("Loaded EDINET checkpoint: last_completed_date=%s", completed)
        return completed
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("Ignoring invalid EDINET checkpoint %s: %s", checkpoint_path, exc)
        return None


def _write_checkpoint(checkpoint_path: Path, completed_date: date) -> None:
    """Atomically record the last fully completed EDINET listing date."""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_completed_date": completed_date.isoformat(),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }

    tmp = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    tmp.replace(checkpoint_path)


def _download_document(
    *,
    session: requests.Session,
    doc_id: str,
    doc_type: int,
    suffix: str,
    out_path: Path,
    api_key: str,
    retries: int,
    skip_if_exists: bool,
    logger: logging.Logger,
) -> str:
    if skip_if_exists and out_path.exists() and out_path.stat().st_size > 0:
        return "skipped"

    out_path.parent.mkdir(parents=True, exist_ok=True)

    url = f"{DOCUMENT_DOWNLOAD_URL}/{doc_id}"
    response = _request_with_retries(
        session,
        "GET",
        url,
        retries=retries,
        logger=logger,
        params={
            "type": doc_type,
            "Subscription-Key": api_key,
        },
    )

    tmp = out_path.with_suffix(out_path.suffix + ".part")
    tmp.write_bytes(response.content)
    tmp.replace(out_path)
    return "downloaded"


def run_edinet_download(
    *,
    start_date: str,
    end_date: str,
    raw_dir: Path,
    metadata_csv: Path,
    api_key_env: str = "EDINET_API_KEY",
    doc_type_code: str = "120",
    edinet_codes_csv: Path | None = None,
    skip_if_exists: bool = True,
    resume: bool = True,
    retries: int = 3,
    request_sleep_seconds: float = 0.25,
    progress_every: int = 25,
    metadata_checkpoint_every: int = 25,
    checkpoint_path: Path | None = None,
    download_zip: bool = True,
    download_pdf: bool = False,
    download_csv: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Enumerate EDINET filings over a date range and download selected filings.

    EDINET document download types used here:
      1 = filing ZIP
      2 = PDF
      5 = CSV ZIP

    Paper 2 uses Annual Securities Reports for listed companies only.
    """
    logger = logger or logging.getLogger(__name__)

    api_key = os.getenv(api_key_env)
    if not api_key:
        raise RuntimeError(
            f"EDINET API key not found. Set environment variable {api_key_env}."
        )

    raw_dir = Path(raw_dir)
    metadata_csv = Path(metadata_csv)
    edinet_codes = _load_edinet_codes(edinet_codes_csv)

    raw_dir.mkdir(parents=True, exist_ok=True)
    metadata_csv.parent.mkdir(parents=True, exist_ok=True)

    if checkpoint_path is None:
        checkpoint_path = metadata_csv.with_name("download_checkpoint.json")
    else:
        checkpoint_path = Path(checkpoint_path)

    last_completed_date = _load_last_completed_date(
        checkpoint_path,
        metadata_csv=metadata_csv,
        resume=resume,
        logger=logger,
    )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "EDHEC-PhD-Research/1.0",
            "Accept": "application/json",
        }
    )

    selected_rows: list[dict[str, Any]] = []
    seen_doc_ids: set[str] = set()

    # If resuming, retain previously collected metadata and avoid duplicate rows.
    if resume and metadata_csv.exists() and metadata_csv.stat().st_size > 0:
        with metadata_csv.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                selected_rows.append(dict(row))
                doc_id = row.get("docID")
                if doc_id:
                    seen_doc_ids.add(doc_id)

        logger.info(
            "Loaded %s existing metadata rows from %s",
            len(selected_rows),
            metadata_csv,
        )

    dates_checked = 0
    filings_matched = 0
    downloaded = 0
    skipped = 0

    requested_start = datetime.strptime(start_date, "%Y-%m-%d").date()
    requested_end = datetime.strptime(end_date, "%Y-%m-%d").date()

    effective_start = requested_start
    if last_completed_date is not None and last_completed_date >= requested_start:
        effective_start = last_completed_date + timedelta(days=1)

    if effective_start > requested_end:
        logger.info(
            "EDINET download already complete through %s for requested range %s..%s",
            last_completed_date,
            requested_start,
            requested_end,
        )

    date_iter = (
        _date_range(effective_start.isoformat(), end_date)
        if effective_start <= requested_end
        else []
    )

    for d in date_iter:
        dates_checked += 1
        d_str = d.isoformat()

        date_matched = 0
        date_downloaded = 0
        date_skipped = 0

        logger.info("EDINET listing date=%s", d_str)

        response = _request_with_retries(
            session,
            "GET",
            DOCUMENT_LIST_URL,
            retries=retries,
            logger=logger,
            params={
                "date": d_str,
                "type": 2,
                "Subscription-Key": api_key,
            },
        )

        payload = response.json()
        results = payload.get("results") or []
        logger.info("EDINET date=%s returned %s filing records", d_str, len(results))

        for filing in results:
            # Annual Securities Reports only.
            if str(filing.get("docTypeCode") or "") != str(doc_type_code):
                continue

            # Paper 2 universe: listed companies only.
            # Require corporate-disclosure filings and a populated security code.
            if str(filing.get("ordinanceCode") or "") != "010":
                continue

            if not str(filing.get("secCode") or "").strip():
                continue

            edinet_code = str(filing.get("edinetCode") or "").strip()
            if edinet_codes is not None and edinet_code not in edinet_codes:
                continue

            doc_id = str(filing.get("docID") or "").strip()
            if not doc_id:
                continue

            filings_matched += 1
            date_matched += 1

            if doc_id not in seen_doc_ids:
                selected_rows.append(filing)
                seen_doc_ids.add(doc_id)

            # Keep each company's documents together.
            company_dir = raw_dir / (edinet_code or "_no_edinet_code") / doc_id

            if download_zip:
                status = _download_document(
                    session=session,
                    doc_id=doc_id,
                    doc_type=1,
                    suffix=".zip",
                    out_path=company_dir / f"{doc_id}.zip",
                    api_key=api_key,
                    retries=retries,
                    skip_if_exists=skip_if_exists,
                    logger=logger,
                )
                if status == "downloaded":
                    downloaded += 1
                    date_downloaded += 1
                else:
                    skipped += 1
                    date_skipped += 1

            if download_pdf and str(filing.get("pdfFlag") or "") == "1":
                status = _download_document(
                    session=session,
                    doc_id=doc_id,
                    doc_type=2,
                    suffix=".pdf",
                    out_path=company_dir / f"{doc_id}.pdf",
                    api_key=api_key,
                    retries=retries,
                    skip_if_exists=skip_if_exists,
                    logger=logger,
                )
                if status == "downloaded":
                    downloaded += 1
                    date_downloaded += 1
                else:
                    skipped += 1
                    date_skipped += 1

            if download_csv and str(filing.get("csvFlag") or "") == "1":
                status = _download_document(
                    session=session,
                    doc_id=doc_id,
                    doc_type=5,
                    suffix="_csv.zip",
                    out_path=company_dir / f"{doc_id}_csv.zip",
                    api_key=api_key,
                    retries=retries,
                    skip_if_exists=skip_if_exists,
                    logger=logger,
                )
                if status == "downloaded":
                    downloaded += 1
                    date_downloaded += 1
                else:
                    skipped += 1
                    date_skipped += 1

            if progress_every > 0 and date_matched % progress_every == 0:
                logger.info(
                    "EDINET progress: date=%s matched=%s downloaded=%s skipped=%s",
                    d_str,
                    date_matched,
                    date_downloaded,
                    date_skipped,
                )

            if (
                metadata_checkpoint_every > 0
                and date_matched % metadata_checkpoint_every == 0
            ):
                _write_metadata(selected_rows, metadata_csv)

            if request_sleep_seconds > 0:
                time.sleep(request_sleep_seconds)

        # Mark a date complete only after all matching filings are processed
        # and the metadata snapshot has been safely written.
        _write_metadata(selected_rows, metadata_csv)
        _write_checkpoint(checkpoint_path, d)

        logger.info(
            "EDINET date complete: date=%s matched=%s downloaded=%s skipped=%s",
            d_str,
            date_matched,
            date_downloaded,
            date_skipped,
        )

    summary = {
        "start_date": start_date,
        "end_date": end_date,
        "effective_start_date": (
            effective_start.isoformat() if effective_start <= requested_end else None
        ),
        "dates_checked": dates_checked,
        "filings_matched": filings_matched,
        "metadata_rows": len(selected_rows),
        "files_downloaded": downloaded,
        "files_skipped": skipped,
        "metadata_csv": str(metadata_csv),
        "checkpoint_path": str(checkpoint_path),
        "raw_dir": str(raw_dir),
    }

    logger.info("EDINET download summary: %s", json.dumps(summary, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    raise SystemExit(
        "This module is intended to be run through scripts/paper2/run_pipeline.py"
    )
