#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path
import pandas as pd

# --- CONFIG (edit if needed) ---
PAPER = "paper1"

INDEX_CSV = Path(r"\\Nas\nas1\Documents\Education\2021 EDHEC Exec PhD\4 Research\Code\out\mdna_summary_nikkei225_filtered.csv")
OLD_DATA_ROOT = Path(r"\\Nas\nas1\Documents\Education\2021 EDHEC Exec PhD\4 Research\Data")

NEW_INTERIM_ROOT = Path(r"\\Nas\nas1\Documents\Education\2021 EDHEC Exec PhD\5 Pipeline\data\interim")
# Keep the same structure under Data/, but paper-scoped:
NEW_DATA_ROOT = NEW_INTERIM_ROOT / PAPER / "data"

DRY_RUN = False            # set False to actually copy
OVERWRITE = False         # False = skip existing files; True = overwrite
COPY_MODE = "copy2"       # "copy2" preserves mtime; good default


def copytree_selective(src: Path, dst: Path, *, overwrite: bool) -> tuple[int, int]:
    """
    Recursively copy src -> dst.
    Returns (files_copied, files_skipped).
    """
    files_copied = 0
    files_skipped = 0

    for p in src.rglob("*"):
        rel = p.relative_to(src)
        target = dst / rel

        if p.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue

        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists() and not overwrite:
            files_skipped += 1
            continue

        if COPY_MODE == "copy2":
            shutil.copy2(p, target)
        else:
            shutil.copy(p, target)
        files_copied += 1

    return files_copied, files_skipped


def main() -> int:
    if not INDEX_CSV.exists():
        raise FileNotFoundError(f"Missing index csv: {INDEX_CSV}")
    if not OLD_DATA_ROOT.exists():
        raise FileNotFoundError(f"Missing old data root: {OLD_DATA_ROOT}")

    NEW_DATA_ROOT.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INDEX_CSV, dtype={"edinet_code": "string"})
    if "edinet_code" not in df.columns:
        raise ValueError(f"Index file missing 'edinet_code' column: {INDEX_CSV}")

    edinet_codes = sorted(set(df["edinet_code"].dropna().astype(str).str.strip()))
    print(f"Paper: {PAPER}")
    print(f"Unique edinet_code count: {len(edinet_codes)}")
    print(f"Destination root: {NEW_DATA_ROOT}")

    missing_dirs = []
    total_copied = 0
    total_skipped = 0

    for i, code in enumerate(edinet_codes, start=1):
        src_dir = OLD_DATA_ROOT / code
        dst_dir = NEW_DATA_ROOT / code

        if not src_dir.exists():
            missing_dirs.append(code)
            print(f"[{i}/{len(edinet_codes)}] MISSING: {src_dir}")
            continue

        if DRY_RUN:
            n_files = sum(1 for p in src_dir.rglob("*") if p.is_file())
            print(f"[{i}/{len(edinet_codes)}] DRY-RUN would copy {n_files} files: {src_dir} -> {dst_dir}")
            continue

        print(f"[{i}/{len(edinet_codes)}] Copying: {src_dir} -> {dst_dir}")
        copied, skipped = copytree_selective(src_dir, dst_dir, overwrite=OVERWRITE)
        total_copied += copied
        total_skipped += skipped
        print(f"    done: copied={copied} skipped={skipped}")

    print("\n--- Summary ---")
    print(f"Dry-run: {DRY_RUN}")
    if not DRY_RUN:
        print(f"Total copied:  {total_copied}")
        print(f"Total skipped: {total_skipped}")
    if missing_dirs:
        print(f"Missing edinet_code dirs ({len(missing_dirs)}): {missing_dirs[:20]}" + (" ..." if len(missing_dirs) > 20 else ""))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
