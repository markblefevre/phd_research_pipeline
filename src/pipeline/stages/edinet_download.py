#!/usr/bin/env python3
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.edinet.download_edinet_reports import run_edinet_download

def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]

# Stage 1: EDINET Download
def run_stage_edinet_download(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        ed_cfg = cfg.get("edinet_download", {})
        root = repo_root()

        api_key_env = ed_cfg.get("api_key_env", "EDINET_API_KEY")
        doc_type_code = str(ed_cfg.get("doc_type_code", "120"))

        start_date = ed_cfg.get("start_date")
        end_date = ed_cfg.get("end_date")

        if not start_date or not end_date:
            raise ValueError(
                "Config error: [edinet_download] requires start_date and end_date"
            )

        raw_dir = root / ed_cfg.get(
            "raw_dir",
            f"data/raw/{paper}/edinet",
        )

        metadata_csv = root / ed_cfg.get(
            "metadata_csv",
            f"data/interim/{paper}/edinet/filings.csv",
        )

        edinet_codes_rel = ed_cfg.get("edinet_codes_csv")
        edinet_codes_csv = (
            root / edinet_codes_rel
            if edinet_codes_rel
            else None
        )

        skip_if_exists = bool(ed_cfg.get("skip_if_exists", True))
        resume = bool(ed_cfg.get("resume", True))
        retries = int(ed_cfg.get("retries", 3))
        request_sleep_seconds = float(
            ed_cfg.get("request_sleep_seconds", 0.25)
        )

        download_zip = bool(ed_cfg.get("download_zip", True))
        download_pdf = bool(ed_cfg.get("download_pdf", False))
        download_csv = bool(ed_cfg.get("download_csv", False))

        logger.info("Stage edinet_download: start=%s end=%s", start_date, end_date)
        logger.info("Stage edinet_download: doc_type_code=%s", doc_type_code)
        logger.info("Stage edinet_download: raw_dir=%s", raw_dir)
        logger.info("Stage edinet_download: metadata_csv=%s", metadata_csv)
        logger.info("Stage edinet_download: api_key_env=%s", api_key_env)

        logger.info("[RUN] edinet_download")

        summary = run_edinet_download(
            start_date=start_date,
            end_date=end_date,
            raw_dir=raw_dir,
            metadata_csv=metadata_csv,
            api_key_env=api_key_env,
            doc_type_code=doc_type_code,
            edinet_codes_csv=edinet_codes_csv,
            skip_if_exists=skip_if_exists,
            resume=resume,
            retries=retries,
            request_sleep_seconds=request_sleep_seconds,
            download_zip=download_zip,
            download_pdf=download_pdf,
            download_csv=download_csv,
            logger=logger,
        )

        logger.info("edinet_download summary: %s", summary)

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info(
            "Stage edinet_download finished: status=%s elapsed=%.3fs",
            status,
            elapsed,
        )