#!/usr/bin/env python3
"""Stage 8D: resumable J-Quants V2 historical bulk-data acquisition.

Downloads original monthly gzip archives for valuation, financial summaries,
and security master. No Stage 7 or Stage 8 frozen output is modified.

Examples:
  python scripts/paper2/fetch_jquants_controls.py --dry-run
  python scripts/paper2/fetch_jquants_controls.py --dataset valuation --limit 2
  python scripts/paper2/fetch_jquants_controls.py

Requires JQUANTS_API_KEY in the environment and requests installed.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://api.jquants.com/v2"
DATASETS = {
    "valuation": "equities/valuation",
    "summary": "fins/summary",
    "master": "equities/master",
}
DEFAULT_ROOT = Path("data/raw/paper2/controls/jquants")
MONTH_RE = re.compile(r"_(\d{6})\.csv\.gz$")


def session_with_retries(*, api_key: str | None = None) -> requests.Session:
    s = requests.Session()
    if api_key is not None:
        s.headers.update({"x-api-key": api_key})
    retry = Retry(
        total=5,
        connect=5,
        read=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(("GET",)),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    return s


def api_json(session: requests.Session, route: str, params: dict) -> dict:
    try:
        response = session.get(BASE + route, params=params, timeout=(20, 60))
    except requests.RequestException as exc:
        raise RuntimeError(f"J-Quants {route} request failed ({type(exc).__name__})") from None
    if response.status_code != 200:
        raise RuntimeError(f"J-Quants {route} returned HTTP {response.status_code}: {response.text[:250]}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected JSON response from {route}")
    return payload


def list_historical(session: requests.Session, endpoint: str, start: str, through: str) -> list[dict]:
    params = {"endpoint": "/" + endpoint}
    found = []
    seen_pages = set()
    while True:
        payload = api_json(session, "/bulk/list", params)
        for item in payload.get("data", []):
            key = item.get("Key", "")
            if not isinstance(key, str) or not key.startswith(endpoint + "/historical/"):
                continue
            path = PurePosixPath(key)
            if path.is_absolute() or ".." in path.parts:
                raise RuntimeError("Unsafe file key returned by J-Quants")
            match = MONTH_RE.search(path.name)
            if not match:
                continue
            month = match.group(1)
            if not (start <= month <= through):
                continue
            if len(path.parts) < 5 or path.parts[-2] != month[:4]:
                raise RuntimeError(f"Unexpected historical file key: {key}")
            size = item.get("Size")
            if size is None or int(size) <= 0:
                raise RuntimeError(f"Invalid expected size for {key}")
            found.append({"Key": key, "Size": int(size)})
        next_key = payload.get("pagination_key")
        if not next_key:
            break
        if next_key in seen_pages:
            raise RuntimeError("Repeated pagination key in /bulk/list")
        seen_pages.add(next_key)
        params["pagination_key"] = next_key
    by_key = {}
    for item in found:
        key = item["Key"]
        if key in by_key and by_key[key] != item["Size"]:
            raise RuntimeError(f"Conflicting sizes in listing for {key}")
        by_key[key] = item["Size"]
    return [{"Key": key, "Size": by_key[key]} for key in sorted(by_key)]


def sha256_and_test_gzip(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    # Read through the entire gzip to validate its CRC/trailer.
    with gzip.open(path, "rb") as stream:
        for _ in iter(lambda: stream.read(1024 * 1024), b""):
            pass
    return digest.hexdigest()


def append_manifest(root: Path, record: dict) -> None:
    manifest = root / "manifests" / "acquisition.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with manifest.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")


def acquire_one(api: requests.Session, download_session: requests.Session,
                root: Path, item: dict) -> str:
    key = item["Key"]
    expected = item["Size"]
    target = root.joinpath(*PurePosixPath(key).parts)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        actual = target.stat().st_size
        if actual != expected:
            raise RuntimeError(f"Existing file size mismatch; refusing overwrite: {target} ({actual} != {expected})")
        try:
            digest = sha256_and_test_gzip(target)
        except (OSError, EOFError) as exc:
            raise RuntimeError(f"Existing file fails gzip validation; refusing overwrite: {target}") from exc
        status = "verified_existing"
    else:
        response = api_json(api, "/bulk/get", {"key": key})
        signed_url = response.get("url")
        if not isinstance(signed_url, str) or not signed_url.startswith("https://"):
            raise RuntimeError(f"No valid signed download URL returned for {key}")
        temporary = target.with_name(target.name + ".part")
        # Signed URL is accessed WITHOUT the J-Quants API-key header.
        try:
            try:
                with download_session.get(signed_url, stream=True, timeout=(20, 120)) as result:
                    if result.status_code != 200:
                        raise RuntimeError(f"Signed download returned HTTP {result.status_code} for {key}")
                    with temporary.open("wb") as stream:
                        for chunk in result.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                stream.write(chunk)
            except requests.RequestException as exc:
                # Do not print the signed URL in exception output.
                raise RuntimeError(f"Signed download failed for {key} ({type(exc).__name__})") from None
            actual = temporary.stat().st_size
            if actual != expected:
                raise RuntimeError(f"Downloaded size mismatch for {key}: {actual} != {expected}")
            digest = sha256_and_test_gzip(temporary)
            if target.exists():
                raise RuntimeError(f"Target appeared during download; refusing overwrite: {target}")
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        status = "downloaded"

    append_manifest(root, {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "key": key,
        "bytes": expected,
        "sha256": digest,
        "status": status,
        "source": "J-Quants V2 bulk",
    })
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("all", *DATASETS), default="all")
    parser.add_argument("--start", default="201610", help="First YYYYMM (inclusive)")
    parser.add_argument("--through", default="202609", help="Last YYYYMM (inclusive)")
    parser.add_argument("--limit", type=int, default=0, help="Max files to process; 0 means all")
    parser.add_argument("--dry-run", action="store_true", help="List planned files without writing")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()
    if not re.fullmatch(r"\d{6}", args.start) or not re.fullmatch(r"\d{6}", args.through) or args.start > args.through:
        parser.error("--start and --through must be ordered YYYYMM values")
    if args.limit < 0:
        parser.error("--limit must be nonnegative")
    key = os.environ.get("JQUANTS_API_KEY")
    if not key:
        parser.error("JQUANTS_API_KEY environment variable is not set")

    api = session_with_retries(api_key=key)
    download_session = session_with_retries()
    selected = DATASETS if args.dataset == "all" else {args.dataset: DATASETS[args.dataset]}
    items = []
    for name, endpoint in selected.items():
        dataset_items = list_historical(api, endpoint, args.start, args.through)
        size = sum(item["Size"] for item in dataset_items)
        print(f"{name:10s}: {len(dataset_items):3d} historical files, {size / 1e6:.1f} MB compressed", flush=True)
        items.extend(dataset_items)
    if args.limit:
        items = items[:args.limit]
    print(f"Selected: {len(items)} files; output root: {args.root}", flush=True)
    if args.dry_run:
        for item in items[:12]:
            print("  ", item["Key"], item["Size"], "bytes")
        if len(items) > 12:
            print(f"  ... and {len(items) - 12} more")
        print("DRY RUN: no files written")
        return 0

    counts = {"downloaded": 0, "verified_existing": 0}
    for index, item in enumerate(items, 1):
        print(f"[{index}/{len(items)}] {item['Key']}", flush=True)
        status = acquire_one(api, download_session, args.root, item)
        counts[status] += 1
        print(f"  {status}", flush=True)
        time.sleep(0.2)
    print(f"COMPLETE: {counts['downloaded']} downloaded; {counts['verified_existing']} existing files verified")
    print(f"Manifest: {args.root / 'manifests' / 'acquisition.jsonl'}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
