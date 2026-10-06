#!/usr/bin/env python3
"""
Build a machine-usable concept/alias dictionary from official FSA EDINET
taxonomy files, including the IFRS element list bundled with the EDINET release.

Inputs:
  - 1e_ElementList.xlsx
  - 1f_AccountList.xlsx
  - 1g_IFRS_ElementList.xlsx

Example:
    python scripts/paper2/build_edinet_concept_dictionary.py \
        --element-list data/reference/edinet_taxonomy/2026/1e_ElementList.xlsx \
        --account-list data/reference/edinet_taxonomy/2026/1f_AccountList.xlsx \
        --ifrs-list data/reference/edinet_taxonomy/2026/1g_IFRS_ElementList.xlsx \
        --taxonomy-year 2026 \
        --output data/interim/paper2/alignment/edinet_concept_dictionary.csv
"""

from __future__ import annotations

import argparse
import re
import unicodedata
import warnings
from pathlib import Path

import pandas as pd


HEADER_TERMS = (
    "標準ラベル",
    "要素名",
    "名前空間",
    "namespace",
    "element",
)

EN_HINTS = ("英語", "英文")
LABEL_HINTS = ("ラベル", "label")


def norm_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = unicodedata.normalize("NFKC", str(value)).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_match_text(value) -> str:
    s = norm_text(value)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[［\[](?:テキストブロック|目次項目)[］\]]$", "", s)
    return s


def locate_header(raw: pd.DataFrame, max_rows: int = 40) -> int | None:
    best_row = None
    best_score = 0
    for r in range(min(max_rows, len(raw))):
        vals = [norm_text(v) for v in raw.iloc[r].tolist()]
        joined = " | ".join(vals)
        score = sum(term.lower() in joined.lower() for term in HEADER_TERMS)
        if score > best_score:
            best_row, best_score = r, score
    return best_row if best_score >= 2 else None


def make_unique_columns(values) -> list[str]:
    seen = {}
    out = []
    for i, value in enumerate(values):
        base = norm_text(value) or f"unnamed_{i}"
        n = seen.get(base, 0)
        seen[base] = n + 1
        out.append(base if n == 0 else f"{base}__{n+1}")
    return out


def find_columns(columns: list[str], needles: tuple[str, ...]) -> list[str]:
    ans = []
    for col in columns:
        lc = col.lower()
        if any(n.lower() in lc for n in needles):
            ans.append(col)
    return ans


def choose_first(row: pd.Series, columns: list[str]) -> str:
    for col in columns:
        value = norm_text(row.get(col, ""))
        if value:
            return value
    return ""


def parse_workbook(path: Path, source: str, taxonomy_year: int) -> pd.DataFrame:
    # The FSA workbooks contain print-area definitions that openpyxl may warn
    # about. They are irrelevant to cell extraction, so suppress only that warning.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"Print area cannot be set to Defined name:.*",
            category=UserWarning,
        )
        xls = pd.ExcelFile(path)

    records = []

    for sheet_name in xls.sheet_names:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"Print area cannot be set to Defined name:.*",
                category=UserWarning,
            )
            raw = pd.read_excel(
                path,
                sheet_name=sheet_name,
                header=None,
                dtype=object,
            )

        header_row = locate_header(raw)
        if header_row is None:
            continue

        cols = make_unique_columns(raw.iloc[header_row].tolist())
        df = raw.iloc[header_row + 1 :].copy()
        df.columns = cols
        df = df.dropna(how="all")

        element_cols = find_columns(cols, ("要素名", "element name", "elementname"))
        namespace_cols = find_columns(cols, ("名前空間", "namespace"))
        std_label_cols = [
            c for c in cols
            if ("標準ラベル" in c or "standard label" in c.lower())
        ]
        all_label_cols = [
            c for c in cols if any(h.lower() in c.lower() for h in LABEL_HINTS)
        ]

        if not element_cols and not std_label_cols:
            continue

        for _, row in df.iterrows():
            element_name = choose_first(row, element_cols)
            namespace = choose_first(row, namespace_cols)

            if not element_name and not any(
                norm_text(row.get(c, "")) for c in all_label_cols
            ):
                continue

            concept_key = (
                f"{namespace}:{element_name}"
                if namespace and element_name
                else element_name
            )

            canonical_ja = ""
            canonical_en = ""
            for c in std_label_cols:
                val = norm_text(row.get(c, ""))
                if not val:
                    continue
                if any(h in c for h in EN_HINTS) or "english" in c.lower():
                    canonical_en = canonical_en or val
                else:
                    canonical_ja = canonical_ja or val

            emitted = set()
            for c in all_label_cols:
                label = norm_text(row.get(c, ""))
                if not label:
                    continue

                key = (c, label)
                if key in emitted:
                    continue
                emitted.add(key)

                language = "en" if (
                    any(h in c for h in EN_HINTS) or "english" in c.lower()
                ) else "ja"

                records.append({
                    "taxonomy_year": taxonomy_year,
                    "source": source,
                    "sheet": sheet_name,
                    "concept_key": concept_key,
                    "namespace": namespace,
                    "element_name": element_name,
                    "canonical_label_ja": canonical_ja,
                    "canonical_label_en": canonical_en,
                    "label_type": c,
                    "language": language,
                    "label": label,
                    "label_normalized": norm_match_text(label),
                })

    out = pd.DataFrame.from_records(records)
    if out.empty:
        return out

    return out.drop_duplicates(
        subset=["taxonomy_year", "source", "concept_key", "label_type", "label"]
    ).reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account-list", type=Path)
    ap.add_argument("--element-list", type=Path)
    ap.add_argument("--ifrs-list", type=Path)
    ap.add_argument("--taxonomy-year", type=int, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()

    frames = []

    if args.account_list:
        frames.append(
            parse_workbook(args.account_list, "account_list", args.taxonomy_year)
        )

    if args.element_list:
        frames.append(
            parse_workbook(args.element_list, "element_list", args.taxonomy_year)
        )

    if args.ifrs_list:
        frames.append(
            parse_workbook(args.ifrs_list, "ifrs_element_list", args.taxonomy_year)
        )

    if not frames:
        raise SystemExit(
            "Provide at least one of --account-list, --element-list, or --ifrs-list."
        )

    out = pd.concat(frames, ignore_index=True)
    out = out[out["label_normalized"].str.len() > 0].copy()

    # Avoid ultra-short ambiguous matches in sentence-level retrieval.
    out["match_eligible"] = out["label_normalized"].str.len() >= 2

    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False, encoding="utf-8-sig")

    print(f"rows={len(out):,}")
    print(f"unique_concepts={out['concept_key'].replace('', pd.NA).nunique(dropna=True):,}")
    print(
        "unique_japanese_aliases="
        f"{out.loc[out['language'].eq('ja'), 'label_normalized'].nunique():,}"
    )
    print("by_source:")
    for source, count in out.groupby("source").size().sort_index().items():
        print(f"  {source}={count:,}")
    print(f"output={args.output}")


if __name__ == "__main__":
    main()
