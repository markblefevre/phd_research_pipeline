# -*- coding: utf-8 -*-
"""
Lookup helpers for mapping between EDINET Code and Securities Identification Code (SIC)
"""
from pathlib import Path
import pandas as pd

# === Utility for code normalization ==========================================
def _norm_sic(x) -> str:
    """Normalize a Securities Identification Code to 4-digit string."""
    return (
        str(x).strip()
        .replace("\u3000", "")  # full-width space
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
        .replace(".", "")
        .rstrip("0")  # handle trailing zero anomaly
        .zfill(4)
    )


# === Lookup functions ========================================================
def get_sic_from_edinet(edinet_code: str, merged_df: pd.DataFrame) -> str | None:
    """
    From an EDINET Code (e.g., 'E12345'), return the 4-digit Securities ID Code (SIC).
    Returns None if not found.
    """
    if "EDINET Code" not in merged_df.columns:
        raise KeyError("Merged DataFrame missing 'EDINET Code' column.")
    if "Securities Identification Code" not in merged_df.columns:
        raise KeyError("Merged DataFrame missing 'Securities Identification Code' column.")

    edinet_code = str(edinet_code).strip().upper()
    hits = merged_df.loc[
        merged_df["EDINET Code"].astype(str).str.upper().eq(edinet_code),
        "Securities Identification Code"
    ].dropna()

    if hits.empty:
        return None
    return _norm_sic(hits.iloc[0])


def get_edinet_from_sic(sic: str | int, merged_df: pd.DataFrame) -> str | None:
    """
    From a 4-digit Securities ID Code (SIC), return the EDINET Code (e.g., 'E12345').
    Returns None if not found.
    """
    if "EDINET Code" not in merged_df.columns:
        raise KeyError("Merged DataFrame missing 'EDINET Code' column.")
    if "Securities Identification Code" not in merged_df.columns:
        raise KeyError("Merged DataFrame missing 'Securities Identification Code' column.")

    sic = _norm_sic(sic)
    hits = merged_df.loc[
        merged_df["Securities Identification Code"].astype(str).apply(_norm_sic).eq(sic),
        "EDINET Code"
    ].dropna()

    if hits.empty:
        return None
    return str(hits.iloc[0]).strip()

# === New lookups: EDINET ↔ Symbol ===========================================

def get_symbol_from_edinet(edinet_code: str, merged_df: pd.DataFrame) -> str | None:
    """
    From an EDINET Code (e.g., 'E02144'), return the Nikkei/Yahoo symbol (e.g., '7203.T').
    Returns None if not found.
    """
    if "EDINET Code" not in merged_df.columns:
        raise KeyError("Merged DataFrame missing 'EDINET Code' column.")
    if "symbol" not in merged_df.columns:
        raise KeyError("Merged DataFrame missing 'symbol' column")

    edinet_code = str(edinet_code).strip().upper()
    hits = merged_df.loc[
        merged_df["EDINET Code"].astype(str).str.upper().eq(edinet_code),
        "symbol"
    ].dropna()

    if hits.empty:
        return None
    return str(hits.iloc[0]).strip()


def get_edinet_from_symbol(symbol: str, merged_df: pd.DataFrame) -> str | None:
    """
    From a Yahoo/Nikkei symbol (e.g., '7203.T'), return the EDINET Code (e.g., 'E02144').
    Returns None if not found.
    """
    if "EDINET Code" not in merged_df.columns:
        raise KeyError("Merged DataFrame missing 'EDINET Code' column.")
    if "symbol" not in merged_df.columns:
        raise KeyError("Merged DataFrame missing 'symbol' column")

    symbol = str(symbol).strip().upper()
    hits = merged_df.loc[
        merged_df["symbol"].astype(str).str.upper().eq(symbol),
        "EDINET Code"
    ].dropna()

    if hits.empty:
        return None
    return str(hits.iloc[0]).strip()
