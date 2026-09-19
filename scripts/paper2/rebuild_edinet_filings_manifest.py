#!/usr/bin/env python3
"""
Rebuild the Paper 2 EDINET filings manifest from EDINET API v2 metadata only.

This utility is intentionally separate from the production downloader. It:

- scans EDINET document listings over an explicit date range;
- keeps Annual Securities Reports (docTypeCode == "120");
- restricts the Paper 2 universe to listed-company filings using the same
  rules as the production downloader:
    * ordinanceCode == "010"
    * secCode is populated
- optionally restricts to a supplied set of EDINET codes;
- deduplicates on docID;
- writes a new manifest atomically;
- never downloads ZIP/PDF/CSV filing documents;
- never reads or writes download_checkpoint.json.

The default output is deliberately NOT filings.csv so that a damaged or
existing production manifest cannot be overwritten accidentally.

Example
-------
export EDINET_API_KEY="your-key"

PYTHONPATH=src python scripts/paper2/rebuild_edinet_filings_manifest.py \
    --start-date 2016-09-20 \
    --end-date 2026-06-24 \
    --output data/interim/paper2/edinet/filings.rebuilt.csv \
    --request-sleep-seconds 0.10

After the rebuild, compare the summary with the expected historical counts
before replacing the production filings.csv.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import requests


BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"
DOCUMENT_LIST_URL = f"{BASE_URL}/documents.json"

PREFERRED_FIELDS = [
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


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}; expected YYYY-MM-DD"
        ) from exc


def _date_range(start: date, end: date) -> Iterable[date]:
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")

    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _load_edinet_codes(path: Path | None) -> set[str] | None:
    """
    Return a set of EDINET codes, or None for an unrestricted universe.

    Accepts a CSV containing one of:
      edinet_code, edinetCode, EDINETCode, EDINET_CODE

    This option is normally unnecessary for the Paper 2 historical rebuild.
    In particular, do not use a current-state EDINET code list as a historical
    listing-status filter.
    """
    if path is None:
        return None

    if not path.exists():
        raise FileNotFoundError(f"EDINET codes CSV not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"No header found in EDINET codes CSV: {path}")

        candidates = ("edinet_code", "edinetCode", "EDINETCode", "EDINET_CODE")
        column = next((c for c in candidates if c in reader.fieldnames), None)
        if column is None:
            raise ValueError(
                f"Could not find EDINET-code column in {path}. "
                f"Expected one of {candidates}; found {reader.fieldnames}"
            )

        return {
            str(row[column]).strip()
            for row in reader
            if row.get(column) and str(row[column]).strip()
        }


def _request_listing(
    session: requests.Session,
    filing_date: date,
    *,
    api_key: str,
    retries: int,
    logger: logging.Logger,
) -> list[dict[str, Any]]:
    """
    Retrieve one day's EDINET filing metadata with exponential backoff.

    Rate limits (HTTP 429) and transient request/server failures are retried.
    """
    params = {
        "date": filing_date.isoformat(),
        "type": 2,
        "Subscription-Key": api_key,
    }

    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            response = session.get(
                DOCUMENT_LIST_URL,
                params=params,
                timeout=60,
            )

            if response.status_code == 429:
                if attempt >= retries:
                    raise RuntimeError(
                        f"EDINET rate limit persisted after {retries + 1} attempts "
                        f"for {filing_date}"
                    )

                wait = min(2**attempt, 30)
                logger.warning(
                    "EDINET rate limited date=%s; retrying in %ss",
                    filing_date,
                    wait,
                )
                time.sleep(wait)
                continue

            if 500 <= response.status_code < 600:
                if attempt >= retries:
                    response.raise_for_status()

                wait = min(2**attempt, 30)
                logger.warning(
                    "EDINET server error HTTP %s date=%s; retrying in %ss",
                    response.status_code,
                    filing_date,
                    wait,
                )
                time.sleep(wait)
                continue

            response.raise_for_status()

            payload = response.json()
            results = payload.get("results") or []
            if not isinstance(results, list):
                raise RuntimeError(
                    f"Unexpected EDINET results type for {filing_date}: "
                    f"{type(results).__name__}"
                )

            return results

        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt >= retries:
                raise

            wait = min(2**attempt, 30)
            logger.warning(
                "EDINET request failed date=%s (%s); retry %s/%s in %ss",
                filing_date,
                exc,
                attempt + 1,
                retries,
                wait,
            )
            time.sleep(wait)

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"EDINET request failed for {filing_date}")


def _matches_paper2_universe(
    filing: dict[str, Any],
    *,
    doc_type_code: str,
    edinet_codes: set[str] | None,
) -> bool:
    """Apply the same Paper 2 filing filters as the production downloader."""
    if str(filing.get("docTypeCode") or "") != doc_type_code:
        return False

    # Corporate-disclosure filings only.
    if str(filing.get("ordinanceCode") or "") != "010":
        return False

    # Paper 2 listed-company proxy used by the production downloader.
    if not str(filing.get("secCode") or "").strip():
        return False

    edinet_code = str(filing.get("edinetCode") or "").strip()
    if edinet_codes is not None and edinet_code not in edinet_codes:
        return False

    if not str(filing.get("docID") or "").strip():
        return False

    return True


def _fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return PREFERRED_FIELDS.copy()

    all_keys = set().union(*(row.keys() for row in rows))
    fields = [name for name in PREFERRED_FIELDS if name in all_keys]
    fields.extend(sorted(all_keys - set(fields)))
    return fields


def _write_csv_atomic(
    rows: list[dict[str, Any]],
    output: Path,
    *,
    overwrite: bool,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    if output.exists() and not overwrite:
        raise FileExistsError(
            f"Output already exists: {output}\n"
            "Use --force only if you intentionally want to replace it."
        )

    temp = output.with_suffix(output.suffix + ".tmp")
    fields = _fieldnames(rows)

    try:
        with temp.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fields,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(rows)

        temp.replace(output)
    finally:
        if temp.exists():
            temp.unlink()


def _value_range(rows: list[dict[str, Any]], field: str) -> tuple[str | None, str | None]:
    values = [
        str(row.get(field) or "").strip()
        for row in rows
        if str(row.get(field) or "").strip()
    ]
    if not values:
        return None, None
    return min(values), max(values)


def _print_summary(
    rows: list[dict[str, Any]],
    *,
    start_date: date,
    end_date: date,
    dates_checked: int,
    api_records_returned: int,
    matched_before_dedup: int,
    duplicate_docids_skipped: int,
    output: Path,
) -> None:
    edinet_codes = {
        str(row.get("edinetCode") or "").strip()
        for row in rows
        if str(row.get("edinetCode") or "").strip()
    }
    sec_codes = {
        str(row.get("secCode") or "").strip()
        for row in rows
        if str(row.get("secCode") or "").strip()
    }

    firm_year_keys = [
        (
            str(row.get("edinetCode") or "").strip(),
            str(row.get("periodEnd") or "").strip(),
        )
        for row in rows
        if str(row.get("edinetCode") or "").strip()
        and str(row.get("periodEnd") or "").strip()
    ]
    unique_firm_years = len(set(firm_year_keys))
    duplicate_firm_year_observations = len(firm_year_keys) - unique_firm_years

    submit_min, submit_max = _value_range(rows, "submitDateTime")
    period_min, period_max = _value_range(rows, "periodEnd")

    print()
    print("EDINET manifest rebuild summary")
    print("=" * 32)
    print(f"requested dates:                 {start_date} .. {end_date}")
    print(f"dates checked:                   {dates_checked:,}")
    print(f"API records returned:            {api_records_returned:,}")
    print(f"Paper 2 matches before dedup:    {matched_before_dedup:,}")
    print(f"duplicate docIDs skipped:        {duplicate_docids_skipped:,}")
    print(f"manifest rows:                   {len(rows):,}")
    print(f"unique docIDs:                   {len(rows):,}")
    print(f"unique EDINET codes:             {len(edinet_codes):,}")
    print(f"unique securities codes:         {len(sec_codes):,}")
    print(f"unique firm-years:               {unique_firm_years:,}")
    print(f"duplicate firm-year observations:{duplicate_firm_year_observations:>9,}")
    print(f"submission range:                {submit_min} .. {submit_max}")
    print(f"fiscal period-end range:         {period_min} .. {period_max}")
    print(f"output:                          {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild the Paper 2 EDINET filings manifest from listing metadata "
            "without downloading filing documents."
        )
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=_parse_date,
        help="First EDINET listing date to scan (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=_parse_date,
        help="Last EDINET listing date to scan (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/interim/paper2/edinet/filings.rebuilt.csv"),
        help=(
            "Output CSV. Defaults to "
            "data/interim/paper2/edinet/filings.rebuilt.csv."
        ),
    )
    parser.add_argument(
        "--api-key-env",
        default="EDINET_API_KEY",
        help="Environment variable containing the EDINET API key.",
    )
    parser.add_argument(
        "--doc-type-code",
        default="120",
        help='EDINET document type code; default "120" (Annual Securities Report).',
    )
    parser.add_argument(
        "--edinet-codes-csv",
        type=Path,
        default=None,
        help=(
            "Optional EDINET-code allowlist CSV. Normally omit for the historical "
            "Paper 2 rebuild to avoid current-state survivorship filtering."
        ),
    )
    parser.add_argument(
        "--request-sleep-seconds",
        type=float,
        default=0.10,
        help=(
            "Pause after each DAILY listing request, not after each filing. "
            "Default: 0.10."
        ),
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Number of retries after the initial request. Default: 4.",
    )
    parser.add_argument(
        "--progress-every-days",
        type=int,
        default=25,
        help="Log cumulative progress every N dates. Default: 25.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow replacement of an existing --output file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.end_date < args.start_date:
        raise SystemExit(
            f"--end-date {args.end_date} is before --start-date {args.start_date}"
        )
    if args.request_sleep_seconds < 0:
        raise SystemExit("--request-sleep-seconds must be >= 0")
    if args.retries < 0:
        raise SystemExit("--retries must be >= 0")
    if args.progress_every_days < 0:
        raise SystemExit("--progress-every-days must be >= 0")

    api_key = os.getenv(args.api_key_env)
    if not api_key:
        raise SystemExit(
            f"EDINET API key not found. Set environment variable {args.api_key_env}."
        )

    if args.output.exists() and not args.force:
        raise SystemExit(
            f"Refusing to overwrite existing output: {args.output}\n"
            "Choose a different --output or pass --force intentionally."
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logger = logging.getLogger("rebuild_edinet_filings_manifest")

    edinet_codes = _load_edinet_codes(args.edinet_codes_csv)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "EDHEC-PhD-Research/1.0",
            "Accept": "application/json",
        }
    )

    selected_by_docid: dict[str, dict[str, Any]] = {}

    dates_checked = 0
    api_records_returned = 0
    matched_before_dedup = 0
    duplicate_docids_skipped = 0

    total_days = (args.end_date - args.start_date).days + 1

    try:
        for filing_date in _date_range(args.start_date, args.end_date):
            dates_checked += 1

            results = _request_listing(
                session,
                filing_date,
                api_key=api_key,
                retries=args.retries,
                logger=logger,
            )
            api_records_returned += len(results)

            date_matched = 0

            for filing in results:
                if not _matches_paper2_universe(
                    filing,
                    doc_type_code=str(args.doc_type_code),
                    edinet_codes=edinet_codes,
                ):
                    continue

                matched_before_dedup += 1
                date_matched += 1

                doc_id = str(filing["docID"]).strip()
                if doc_id in selected_by_docid:
                    duplicate_docids_skipped += 1
                    continue

                # Copy the API row so later processing cannot mutate requests data.
                selected_by_docid[doc_id] = dict(filing)

            if date_matched:
                logger.info(
                    "EDINET date=%s returned=%s matched=%s cumulative_rows=%s",
                    filing_date,
                    len(results),
                    date_matched,
                    len(selected_by_docid),
                )

            if (
                args.progress_every_days > 0
                and dates_checked % args.progress_every_days == 0
            ):
                logger.info(
                    "EDINET rebuild progress: %s/%s dates (%.1f%%), rows=%s",
                    dates_checked,
                    total_days,
                    100.0 * dates_checked / total_days,
                    len(selected_by_docid),
                )

            # Throttle once per API request/date. The production downloader's
            # filing-level delay is intentionally not reproduced here.
            if args.request_sleep_seconds > 0:
                time.sleep(args.request_sleep_seconds)

    finally:
        session.close()

    rows = list(selected_by_docid.values())

    # Deterministic ordering makes rebuilds easy to diff.
    rows.sort(
        key=lambda row: (
            str(row.get("submitDateTime") or ""),
            str(row.get("docID") or ""),
        )
    )

    _write_csv_atomic(rows, args.output, overwrite=args.force)

    _print_summary(
        rows,
        start_date=args.start_date,
        end_date=args.end_date,
        dates_checked=dates_checked,
        api_records_returned=api_records_returned,
        matched_before_dedup=matched_before_dedup,
        duplicate_docids_skipped=duplicate_docids_skipped,
        output=args.output,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
