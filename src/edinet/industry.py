from __future__ import annotations

from pathlib import Path
import pandas as pd

from .paths import edinet_codelist_csv
from .read_edinet_codelist_csv import read_jpn_csv_sjis, read_eng_csv_sjis


def load_latest_industry_map(repo_root: Path) -> pd.DataFrame:
    """
    Load latest EDINET codelists (JP + EN) using canonical paths.py helpers,
    and return a mapping:
        edinet_code -> industry_jp, industry_en

    Parameters
    ----------
    repo_root : Path
        Repository root directory.

    Returns
    -------
    pd.DataFrame
        Columns: edinet_code, industry_jp, industry_en
    """
    repo_root = Path(repo_root).resolve()

    ja_csv = edinet_codelist_csv(repo_root, lang="ja")
    en_csv = edinet_codelist_csv(repo_root, lang="en")

    # Readers take (directory, filename)
    _, jpn_df = read_jpn_csv_sjis(ja_csv.parent, ja_csv.name)
    _, eng_df = read_eng_csv_sjis(en_csv.parent, en_csv.name)

    required_jp = {"ＥＤＩＮＥＴコード", "提出者業種"}
    required_en = {"EDINET Code", "Submitter's industry"}
    
    missing_jp = required_jp - set(jpn_df.columns)
    missing_en = required_en - set(eng_df.columns)
    
    if missing_jp:
        raise KeyError(f"JP codelist missing columns {missing_jp}. Loaded from: {ja_csv}. "
                       f"Have: {list(jpn_df.columns)}")
    if missing_en:
        raise KeyError(f"EN codelist missing columns {missing_en}. Loaded from: {en_csv}. "
                       f"Have: {list(eng_df.columns)}")

    j_map = (
        jpn_df[["ＥＤＩＮＥＴコード", "提出者業種"]]
        .rename(columns={"ＥＤＩＮＥＴコード": "edinet_code", "提出者業種": "industry_jp"})
        .copy()
    )
    e_map = (
        eng_df[["EDINET Code", "Submitter's industry"]]
        .rename(columns={"EDINET Code": "edinet_code", "Submitter's industry": "industry_en"})
        .copy()
    )

    # Key hygiene
    for df in (j_map, e_map):
        df["edinet_code"] = df["edinet_code"].astype(str).str.strip()

    # Outer merge to keep maximum coverage (safe, then left-merge into panel)
    out = j_map.merge(e_map, on="edinet_code", how="outer")
    return out


def attach_edinet_industry(
    panel: pd.DataFrame,
    repo_root: Path,
    *,
    label: str = "jp",
    create_canonical: bool = True,
    canonical_col: str = "industry",
) -> pd.DataFrame:
    """
    Merge EDINET industry into `panel` by edinet_code.

    Parameters
    ----------
    panel : pd.DataFrame
        Must contain 'edinet_code'.
    repo_root : Path
        Repository root directory (used with paths.py helpers).
    label : {'jp','en','both'}
        Which label(s) to keep. If 'both', keeps both industry_jp and industry_en.
    create_canonical : bool
        If True, also creates `canonical_col` for regression use.
    canonical_col : str
        Name for the canonical industry column (default 'industry').

    Returns
    -------
    pd.DataFrame
        Panel with industry columns merged.
    """
    if "edinet_code" not in panel.columns:
        raise KeyError("attach_edinet_industry requires panel to have an 'edinet_code' column")

    industry_map = load_latest_industry_map(repo_root)

    out = panel.copy()
    out["edinet_code"] = out["edinet_code"].astype(str).str.strip()
    out = out.merge(industry_map, on="edinet_code", how="left")

    label = label.lower()
    if label not in {"jp", "en", "both"}:
        raise ValueError("label must be one of {'jp','en','both'}")

    if label == "jp":
        out = out.drop(columns=["industry_en"], errors="ignore")
        if create_canonical:
            out[canonical_col] = out["industry_jp"]
    elif label == "en":
        out = out.drop(columns=["industry_jp"], errors="ignore")
        if create_canonical:
            out[canonical_col] = out["industry_en"]
    else:  # both
        if create_canonical:
            # Default canonical: JP (aligns with EDINET docs / JP filings)
            out[canonical_col] = out["industry_jp"]

    return out


def attach_ticker_industry(
    panel: pd.DataFrame,
    repo_root: Path,
    *,
    ticker_col: str = "Ticker",
    label: str = "jp",
    create_canonical: bool = True,
    canonical_col: str = "industry",
) -> pd.DataFrame:
    """
    Attach EDINET industry to a panel using ticker (e.g. 4151.T).

    Mapping path:
        Ticker -> 4-digit securities code ->
        EDINET codelist (証券コード / Securities Identification Code) ->
        edinet_code + industry_jp + industry_en

    Parameters
    ----------
    panel : pd.DataFrame
        Must contain ticker_col.
    repo_root : Path
        Repository root.
    ticker_col : str
        Column containing ticker symbol like 4151.T.
    label : {'jp','en','both'}
        Which language(s) to retain.
    """

    if ticker_col not in panel.columns:
        raise KeyError(f"{ticker_col} not found in panel")

    out = panel.copy()

    # --- Extract 4-digit securities code ---
    out["_sec_code"] = (
        out[ticker_col]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    # --- Load EDINET industry map (JP + EN) ---
    industry_map = load_latest_industry_map(repo_root)

    # --- Load JP + EN raw codelists to get securities code ---
    ja_csv = edinet_codelist_csv(repo_root, lang="ja")
    en_csv = edinet_codelist_csv(repo_root, lang="en")

    _, jpn_df = read_jpn_csv_sjis(ja_csv.parent, ja_csv.name)
    _, eng_df = read_eng_csv_sjis(en_csv.parent, en_csv.name)

    j_cols = jpn_df[["ＥＤＩＮＥＴコード", "証券コード"]].rename(
        columns={"ＥＤＩＮＥＴコード": "edinet_code", "証券コード": "_sec_code"}
    )

    j_cols["_sec_code"] = (
        j_cols["_sec_code"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]   # take first 4 digits only
        .astype(str)
        .str.zfill(4)
    )

    # Join securities code -> edinet_code
    out = out.merge(j_cols, on="_sec_code", how="left")

    # Join edinet_code -> industry_jp, industry_en
    out = out.merge(industry_map, on="edinet_code", how="left")

    # --- Label handling ---
    label = label.lower()
    if label not in {"jp", "en", "both"}:
        raise ValueError("label must be one of {'jp','en','both'}")

    if label == "jp":
        out = out.drop(columns=["industry_en"], errors="ignore")
        if create_canonical:
            out[canonical_col] = out["industry_jp"]

    elif label == "en":
        out = out.drop(columns=["industry_jp"], errors="ignore")
        if create_canonical:
            out[canonical_col] = out["industry_en"]

    else:  # both
        if create_canonical:
            out[canonical_col] = out["industry_jp"]

    return out.drop(columns=["_sec_code"])
