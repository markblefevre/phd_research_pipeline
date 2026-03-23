#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path
import pandas as pd

# --- CONFIG ---
PAPER = "paper1"

OLD_DATA_ROOT = Path(r"\\Nas\nas1\Documents\Education\2021 EDHEC Exec PhD\4 Research\Data")
NEW_DATA_ROOT = Path(r"\\Nas\nas1\Documents\Education\2021 EDHEC Exec PhD\5 Pipeline\data\interim") / PAPER / "data"

# Reference file that contains ticker ↔ EDINET mapping
EDINET_REF_CSV = Path(r"\\Nas\nas1\Documents\Education\2021 EDHEC Exec PhD\4 Research\Data\reference\edinet\Edinetcode_en_latest\EdinetcodeDlInfo.csv")

# Missing tickers (your list)
MISSING_TICKERS = [
    "5831.T",
    "6526.T",
    "8591.T",
    "8604.T",
    "8750.T",
    "9147.T",
]

DRY_RUN = False
OVERWRITE = False


def copytree_selective(src: Path, dst: Path, *, overwrite: bool) -> tuple[int, int]:
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

        shutil.copy2(p, target)
        files_copied += 1

    return files_copied, files_skipped


def main() -> int:
    if not EDINET_REF_CSV.exists():
        raise FileNotFoundError(f"Missing EDINET reference file: {EDINET_REF_CSV}")

    ref = pd.read_csv(EDINET_REF_CSV, dtype=str, encoding="cp932", skiprows=1)

    # Normalize columns
    ref.columns = [c.lower() for c in ref.columns]

    # Try to find ticker/security code column
    ticker_col = None
    for c in ref.columns:
        if "Securities Identification Code" in c or "code" in c:
            ticker_col = c
    if ticker_col is None:
        raise ValueError("Could not find security code column in EDINET reference")

    edinet_col = None
    for c in ref.columns:
        if "edinet" in c:
            edinet_col = c
    if edinet_col is None:
        raise ValueError("Could not find edinet code column")

    # Normalize ticker format (strip .T)
    ref["ticker"] = ref[ticker_col].str.strip().str.replace(".0", "", regex=False)
    ref["ticker"] = ref["ticker"].astype(str)
    ref["ticker"] = ref["ticker"].str[:4] + ".T"

    ticker_to_edinet = dict(zip(ref["ticker"], ref[edinet_col]))

    NEW_DATA_ROOT.mkdir(parents=True, exist_ok=True)

    total_copied = 0
    total_skipped = 0

    print(f"Destination: {NEW_DATA_ROOT}\n")

    for t in MISSING_TICKERS:
        sec_code = t
        edinet_code = ticker_to_edinet.get(sec_code)

        if not edinet_code or pd.isna(edinet_code):
            print(f"[WARN] No EDINET code for {t}")
            continue

        src_dir = OLD_DATA_ROOT / edinet_code
        dst_dir = NEW_DATA_ROOT / edinet_code

        if not src_dir.exists():
            print(f"[MISSING SRC] {t} → {src_dir}")
            continue

        if DRY_RUN:
            print(f"[DRY] {t} → {edinet_code} ({src_dir})")
            continue

        print(f"[COPY] {t} → {edinet_code}")
        copied, skipped = copytree_selective(src_dir, dst_dir, overwrite=OVERWRITE)

        total_copied += copied
        total_skipped += skipped

        print(f"    copied={copied} skipped={skipped}")

    print("\n--- Summary ---")
    print(f"Total copied:  {total_copied}")
    print(f"Total skipped: {total_skipped}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())