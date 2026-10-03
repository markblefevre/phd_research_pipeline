#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Build a Python target_edinet_codes list from a constituent file and the EDINET code list.

Inputs:
  1. A constituent/list file containing Japanese securities codes.
  2. The EDINET code list, e.g. EdinetcodeDlInfo.csv / .xlsx.

Output:
  A Python file containing:
      target_edinet_codes = [...]
      target_securities_codes = [...]
      sample_name = "..."

Notes:
  - Securities codes are normalized to the first four digits.
  - EDINET codes look like E00000.
  - CSV and Excel files are both supported.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


EDINET_CODE_RE = re.compile(r"E\d{5}")
SECURITY_CODE_RE = re.compile(r"\d{4,5}")


def read_table(path: Path) -> pd.DataFrame:
    """Read CSV or Excel with a few common Japanese encodings."""
    suffix = path.suffix.lower()

    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return pd.read_excel(path, dtype=str)

    if suffix == ".csv":
        for encoding in ("utf-8-sig", "cp932", "shift_jis", "utf-8"):
            try:
                return pd.read_csv(path, dtype=str, encoding=encoding)
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError(f"Could not decode CSV: {path}")

    raise ValueError(f"Unsupported file type: {path}")


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names while preserving the original data."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def normalize_security_code(value) -> str | None:
    """Return a four-digit Japanese securities code, or None."""
    if pd.isna(value):
        return None

    text = str(value).strip()

    # Avoid Excel-style decimals such as 7203.0
    text = re.sub(r"\.0$", "", text)

    match = SECURITY_CODE_RE.search(text)
    if not match:
        return None

    code = match.group(0)

    # EDINET's securities-code field is often five digits, where the first
    # four are the listed company code.
    return code[:4]


def normalize_edinet_code(value) -> str | None:
    """Return an EDINET code such as E02144, or None."""
    if pd.isna(value):
        return None

    match = EDINET_CODE_RE.search(str(value).strip())
    return match.group(0) if match else None


def find_column(df: pd.DataFrame, preferred_names: list[str], fallback_regex: str | None = None) -> str:
    """Find a column using preferred names first, then a regex fallback."""
    normalized = {str(c).strip().lower(): c for c in df.columns}

    for name in preferred_names:
        key = name.strip().lower()
        if key in normalized:
            return normalized[key]

    if fallback_regex:
        pattern = re.compile(fallback_regex, re.IGNORECASE)
        for col in df.columns:
            if pattern.search(str(col)):
                return col

    raise ValueError(
        "Could not identify required column. Available columns are:\n"
        + "\n".join(f"  - {c}" for c in df.columns)
    )


def find_first_code_column(df: pd.DataFrame) -> str:
    """
    Find the most likely securities-code column.

    This intentionally tries common JPX/Nikkei/EDINET names before falling
    back to any column whose name looks code-related.
    """
    candidates = [
        "コード",
        "銘柄コード",
        "証券コード",
        "Local Code",
        "Code",
        "code",
        "Securities Code",
        "security_code",
        "securities_code",
    ]

    return find_column(
        df,
        preferred_names=candidates,
        fallback_regex=r"(証券|銘柄|local|security|securities|code|コード)",
    )


def load_edinet_mapping(edinet_code_list: Path) -> pd.DataFrame:
    """Load the EDINET code list and return securities_code -> edinet_code mapping."""
    df = flatten_columns(read_table(edinet_code_list))

    edinet_col = find_column(
        df,
        preferred_names=[
            "ＥＤＩＮＥＴコード",
            "EDINETコード",
            "EDINET Code",
            "edinet_code",
            "ＥＤＩＮＥＴコード、提出者名",
        ],
        fallback_regex=r"(edinet|ＥＤＩＮＥＴ)",
    )

    sec_col = find_column(
        df,
        preferred_names=[
            "証券コード",
            "Securities Code",
            "security_code",
            "securities_code",
            "コード",
        ],
        fallback_regex=r"(証券|security|securities)",
    )

    out = pd.DataFrame(
        {
            "edinet_code": df[edinet_col].map(normalize_edinet_code),
            "security_code": df[sec_col].map(normalize_security_code),
        }
    )

    out = out.dropna(subset=["edinet_code", "security_code"])
    out = out.drop_duplicates(subset=["security_code", "edinet_code"])
    return out


def load_constituent_codes(constituent_file: Path) -> list[str]:
    """Load securities codes from a constituent/list file."""
    df = flatten_columns(read_table(constituent_file))
    code_col = find_first_code_column(df)

    codes = (
        df[code_col]
        .map(normalize_security_code)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    if not codes:
        raise ValueError(f"No securities codes found in {constituent_file}")

    return codes


def write_python_list(
    output_file: Path,
    sample_name: str,
    edinet_codes: list[str],
    securities_codes: list[str],
) -> None:
    """Write a Python module containing target_edinet_codes."""
    output_file.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# -*- coding: utf-8 -*-",
        '"""Auto-generated target EDINET code list."""',
        "",
        f"sample_name = {sample_name!r}",
        "",
        "target_edinet_codes = [",
    ]

    lines.extend(f"    {code!r}," for code in edinet_codes)
    lines.extend(
        [
            "]",
            "",
            "target_securities_codes = [",
        ]
    )
    lines.extend(f"    {code!r}," for code in securities_codes)
    lines.append("]")
    lines.append("")

    output_file.write_text("\n".join(lines), encoding="utf-8")


def build_target_file(
    *,
    sample_name: str,
    constituent_file: Path,
    edinet_code_list: Path,
    output_file: Path,
) -> None:
    constituent_codes = load_constituent_codes(constituent_file)
    mapping = load_edinet_mapping(edinet_code_list)

    constituent_df = pd.DataFrame({"security_code": constituent_codes})
    matched = constituent_df.merge(mapping, on="security_code", how="left")

    missing = matched[matched["edinet_code"].isna()]["security_code"].tolist()
    matched = matched.dropna(subset=["edinet_code"])

    # A listed security should normally map to one EDINET code. Keep unique codes.
    edinet_codes = sorted(matched["edinet_code"].drop_duplicates().tolist())
    securities_codes = sorted(matched["security_code"].drop_duplicates().tolist())

    write_python_list(
        output_file=output_file,
        sample_name=sample_name,
        edinet_codes=edinet_codes,
        securities_codes=securities_codes,
    )

    print(f"Sample: {sample_name}")
    print(f"Input securities codes: {len(constituent_codes)}")
    print(f"Matched EDINET codes: {len(edinet_codes)}")
    print(f"Missing securities-code matches: {len(missing)}")

    if missing:
        print("First missing securities codes:")
        for code in missing[:25]:
            print(f"  {code}")

    print(f"Wrote: {output_file}")


def load_prime_market_codes(listed_company_file: Path) -> list[str]:
    """
    Load TSE Prime Market securities codes from the JPX listed-company file.

    The JPX listed-company file usually has columns like:
      - コード / Code
      - 市場・商品区分 / Market/Product Segment
    """
    df = flatten_columns(read_table(listed_company_file))

    code_col = find_first_code_column(df)
    market_col = find_column(
        df,
        preferred_names=[
            "市場・商品区分",
            "Market/Product Segment",
            "Market Segment",
            "market_segment",
            "市場区分",
        ],
        fallback_regex=r"(市場|market|segment)",
    )

    prime_mask = df[market_col].astype(str).str.contains("Prime|プライム", case=False, na=False)

    codes = (
        df.loc[prime_mask, code_col]
        .map(normalize_security_code)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    if not codes:
        raise ValueError(
            "No Prime Market securities codes found. Check the market column "
            f"and values in {listed_company_file}."
        )

    return codes


def build_prime_target_file(
    *,
    listed_company_file: Path,
    edinet_code_list: Path,
    output_file: Path,
) -> None:
    prime_codes = load_prime_market_codes(listed_company_file)
    mapping = load_edinet_mapping(edinet_code_list)

    prime_df = pd.DataFrame({"security_code": prime_codes})
    matched = prime_df.merge(mapping, on="security_code", how="left")

    missing = matched[matched["edinet_code"].isna()]["security_code"].tolist()
    matched = matched.dropna(subset=["edinet_code"])

    edinet_codes = sorted(matched["edinet_code"].drop_duplicates().tolist())
    securities_codes = sorted(matched["security_code"].drop_duplicates().tolist())

    write_python_list(
        output_file=output_file,
        sample_name="tse_prime",
        edinet_codes=edinet_codes,
        securities_codes=securities_codes,
    )

    print("Sample: tse_prime")
    print(f"Input Prime securities codes: {len(prime_codes)}")
    print(f"Matched EDINET codes: {len(edinet_codes)}")
    print(f"Missing securities-code matches: {len(missing)}")

    if missing:
        print("First missing securities codes:")
        for code in missing[:25]:
            print(f"  {code}")

    print(f"Wrote: {output_file}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build target_edinet_codes_prime.py from the JPX listed-company file."
    )
    parser.add_argument(
        "--listed-companies",
        required=True,
        type=Path,
        help="JPX listed-company CSV/XLSX file containing market segments and securities codes.",
    )
    parser.add_argument(
        "--edinet-codes",
        required=True,
        type=Path,
        help="EDINET code list file, e.g. EdinetcodeDlInfo.csv or .xlsx.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target_edinet_codes_prime.py"),
        help="Output Python file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_prime_target_file(
        listed_company_file=args.listed_companies,
        edinet_code_list=args.edinet_codes,
        output_file=args.output,
    )


if __name__ == "__main__":
    main()
