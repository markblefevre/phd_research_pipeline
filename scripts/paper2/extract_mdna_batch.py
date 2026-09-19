#!/usr/bin/env python3
"""CLI wrapper for the reusable Paper 2 MD&A batch extractor."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from mdna_analysis.extract_mdna_batch import (
    print_mdna_batch_summary,
    run_mdna_batch,
)


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

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logger = logging.getLogger("extract_mdna_batch")

    try:
        summary = run_mdna_batch(
            filings_csv=args.filings,
            raw_dir=args.raw_dir,
            output_dir=args.output_dir,
            manifest_path=args.manifest,
            checkpoint_every=args.checkpoint_every,
            progress_every=args.progress_every,
            retry_failures=args.retry_failures,
            force=args.force,
            limit=args.limit,
            logger=logger,
        )
    except KeyboardInterrupt:
        return 130

    print_mdna_batch_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
