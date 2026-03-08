#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download EDINET Code List ZIPs (Japanese + English), optionally unzip.

Defaults:
- Writes *_latest.zip aliases (for convenience)
- Unzips the latest aliases into <outdir>/<base>_latest/ directories

Also supports reproducible date-stamped snapshots (YYYYMMDD) via --stamp.

Example:
  python download_edinet_codelists.py --outdir data/reference

Reproducible snapshot + latest:
  python download_edinet_codelists.py --outdir data/reference --stamp date

Notes:
  Fixed URLs:
    Japanese: https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip
    English:  https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelisteng/Edinetcode.zip
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


URLS = {
    "ja": "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip",
    "en": "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelisteng/Edinetcode.zip",
}


@dataclass
class DownloadResult:
    label: str
    url: str
    path: Path
    bytes: int
    sha256: str
    downloaded_at: str  # ISO8601


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def build_filename(base_name: str, stamp: str, suffix: str = ".zip") -> str:
    """
    base_name: e.g. "Edinetcode_ja" or "Edinetcode_en"
    stamp: "", "YYYYMMDD", or "YYYYMMDD_HHMMSS"
    """
    if stamp:
        return f"{base_name}_{stamp}{suffix}"
    return f"{base_name}{suffix}"


def download_with_retries(
    url: str,
    dest_path: Path,
    timeout: int,
    retries: int,
    backoff: float,
    user_agent: str,
) -> None:
    """
    Downloads URL to dest_path atomically via a temp file.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 2):  # retries=0 => 1 attempt
        try:
            req = Request(url, headers={"User-Agent": user_agent})
            with urlopen(req, timeout=timeout) as resp, tmp_path.open("wb") as out:
                shutil.copyfileobj(resp, out, length=1024 * 1024)

            os.replace(tmp_path, dest_path)  # atomic
            return
        except (HTTPError, URLError, TimeoutError, OSError) as e:
            last_err = e
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass

            if attempt <= retries + 1:
                time.sleep(backoff * (2 ** (attempt - 1)))

    raise RuntimeError(f"Failed to download {url} after {retries + 1} attempts: {last_err}") from last_err


def write_latest_alias(src: Path, latest_path: Path, overwrite: bool) -> None:
    if latest_path.exists() and not overwrite:
        return
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, latest_path)


def safe_unzip(zip_path: Path, out_dir: Path, overwrite: bool) -> list[str]:
    """
    Safely unzip zip_path into out_dir.

    - Prevents Zip Slip (path traversal).
    - If overwrite=False, refuses to overwrite existing files.
    Returns the list of extracted relative file paths.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[str] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            # Skip directories
            if member.is_dir():
                continue

            # Normalize and prevent path traversal
            member_name = member.filename.replace("\\", "/")
            target_path = (out_dir / member_name).resolve()

            if not str(target_path).startswith(str(out_dir.resolve()) + os.sep):
                raise RuntimeError(f"Refusing to extract suspicious path: {member.filename}")

            target_path.parent.mkdir(parents=True, exist_ok=True)

            if target_path.exists() and not overwrite:
                raise RuntimeError(f"Refusing to overwrite existing file: {target_path}")

            with zf.open(member, "r") as src, target_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)

            extracted.append(member_name)

    return extracted


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Download EDINET code list ZIPs (JA+EN) and optionally unzip.")
    p.add_argument("--outdir", type=Path, required=True, help="Output directory for downloaded ZIPs.")

    p.add_argument(
        "--stamp",
        choices=["date", "datetime", "none"],
        default="date",
        help="Filename stamping for reproducibility (default: date).",
    )

    # Changed default: do latest alias by default
    p.add_argument(
        "--latest-alias",
        action=argparse.BooleanOptionalAction,
        default=True,
        help='Write convenience copies named "*_latest.zip" (default: true). Use --no-latest-alias to disable.',
    )

    # New: unzip by default
    p.add_argument(
        "--unzip",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Unzip downloaded ZIPs (default: true). Use --no-unzip to disable.",
    )

    # Where to unzip:
    # - default: unzip the latest aliases (stable paths)
    # - optionally: unzip both snapshot + latest, or snapshot-only
    p.add_argument(
        "--unzip-target",
        choices=["latest", "stamped", "both"],
        default="latest",
        help='Which ZIP(s) to unzip (default: "latest").',
    )

    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files (ZIPs and extracted files).",
    )
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds (default: 60).")
    p.add_argument("--retries", type=int, default=3, help="Retry count on failure (default: 3).")
    p.add_argument("--backoff", type=float, default=1.0, help="Backoff base seconds (default: 1.0).")
    p.add_argument(
        "--user-agent",
        type=str,
        default="edinet-codelist-downloader/1.1",
        help="User-Agent header (default: edinet-codelist-downloader/1.1).",
    )
    p.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Optional path to write a JSON manifest (default: <outdir>/edinet_codelist_manifest.json).",
    )

    args = p.parse_args(argv)

    now = datetime.now()
    if args.stamp == "date":
        stamp = now.strftime("%Y%m%d")
    elif args.stamp == "datetime":
        stamp = now.strftime("%Y%m%d_%H%M%S")
    else:
        stamp = ""

    manifest_path = args.manifest or (args.outdir / "edinet_codelist_manifest.json")

    results: Dict[str, DownloadResult] = {}
    zipped_paths: Dict[str, Dict[str, Path]] = {}  # label -> {"stamped": Path, "latest": Path?}

    for label, url in URLS.items():
        base_name = f"Edinetcode_{label}"

        # Always produce a stamped snapshot (unless stamp=none)
        stamped_name = build_filename(base_name, stamp, ".zip")
        stamped_path = args.outdir / stamped_name

        if stamped_path.exists() and not args.overwrite:
            size = stamped_path.stat().st_size
            h = sha256_file(stamped_path)
        else:
            download_with_retries(
                url=url,
                dest_path=stamped_path,
                timeout=args.timeout,
                retries=args.retries,
                backoff=args.backoff,
                user_agent=args.user_agent,
            )
            size = stamped_path.stat().st_size
            h = sha256_file(stamped_path)

        downloaded_at = now.isoformat(timespec="seconds")
        results[label] = DownloadResult(
            label=label, url=url, path=stamped_path, bytes=size, sha256=h, downloaded_at=downloaded_at
        )

        zipped_paths[label] = {"stamped": stamped_path}

        # Latest alias (default on)
        if args.latest_alias:
            latest_zip = args.outdir / f"{base_name}_latest.zip"
            write_latest_alias(stamped_path, latest_zip, overwrite=args.overwrite)
            zipped_paths[label]["latest"] = latest_zip

        print(f"[OK] {label}: {stamped_path.name}  ({size:,} bytes)  sha256={h[:12]}...")

    # Unzip (default on)
    unzip_report: Dict[str, Dict[str, object]] = {}
    if args.unzip:
        for label, paths in zipped_paths.items():
            base_name = f"Edinetcode_{label}"
            targets: list[tuple[str, Path]] = []

            if args.unzip_target in ("stamped", "both"):
                targets.append(("stamped", paths["stamped"]))

            if args.unzip_target in ("latest", "both"):
                if not args.latest_alias or "latest" not in paths:
                    raise RuntimeError('Cannot unzip "latest" because --no-latest-alias was set.')
                targets.append(("latest", paths["latest"]))

            for which, zip_path in targets:
                out_dir = args.outdir / f"{base_name}_{which}"
                extracted = safe_unzip(zip_path, out_dir, overwrite=args.overwrite)
                unzip_report.setdefault(label, {})[which] = {
                    "zip": str(zip_path),
                    "out_dir": str(out_dir),
                    "files": extracted,
                }
                print(f"[OK] unzip {label}/{which}: {zip_path.name} -> {out_dir} ({len(extracted)} files)")

    # Write manifest
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at": now.isoformat(timespec="seconds"),
        "stamp_mode": args.stamp,
        "latest_alias": args.latest_alias,
        "unzip": args.unzip,
        "unzip_target": args.unzip_target,
        "files": {
            k: {
                "url": v.url,
                "path": str(v.path),
                "bytes": v.bytes,
                "sha256": v.sha256,
                "downloaded_at": v.downloaded_at,
                "latest_alias_path": str(zipped_paths[k].get("latest")) if args.latest_alias else None,
            }
            for k, v in results.items()
        },
        "unzipped": unzip_report,
    }
    manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] manifest: {manifest_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
