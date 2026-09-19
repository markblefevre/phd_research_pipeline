#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from mdna_analysis.mdna_extraction import extract_mdna_from_zip, write_mdna_text


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract MD&A from one EDINET filing ZIP.")
    parser.add_argument("zip_path", type=Path)
    parser.add_argument("--output", type=Path, help="Optional output .txt path")
    parser.add_argument("--preview-chars", type=int, default=1000)
    args = parser.parse_args()

    result = extract_mdna_from_zip(args.zip_path)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))

    if result.mdna_text:
        print("\nMD&A preview:\n")
        print(result.mdna_text[: args.preview_chars])
        if args.output:
            write_mdna_text(result, args.output)
            print(f"\nWrote: {args.output}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
