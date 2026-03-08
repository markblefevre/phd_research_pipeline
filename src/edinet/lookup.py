# -*- coding: utf-8 -*-
"""
Lookup helpers for mapping between EDINET Code and Securities Identification Code (SIC)
using a merged Nikkei225–EDINET DataFrame.
"""
from pathlib import Path
import pandas as pd

from src.nikkei.read_n225_csv import read_n225_csv
from src.edinet.read_edinet_codelist_csv import read_eng_csv_sjis, filter_eng_listedcompanies
from src.nikkei.join_n225_edinet import join_n225_edinet  # reuse existing merge function

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
        raise KeyError("Merged DataFrame missing 'symbol' column (check N225 merge).")

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
        raise KeyError("Merged DataFrame missing 'symbol' column (check N225 merge).")

    symbol = str(symbol).strip().upper()
    hits = merged_df.loc[
        merged_df["symbol"].astype(str).str.upper().eq(symbol),
        "EDINET Code"
    ].dropna()

    if hits.empty:
        return None
    return str(hits.iloc[0]).strip()

# === Minimal test main =======================================================
if __name__ == "__main__":
    # NAS or local paths
    n225_path = Path("//nas/nas1/Documents/Education/2021 EDHEC Exec PhD/4 Research/nikkei/nikkei225_all.csv")
    edinet_dir = Path("//nas/nas1/Documents/Education/2021 EDHEC Exec PhD/4 Research/EDINET Information")

    # Load source data
    n225_df = read_n225_csv(n225_path)
    _, edinet_df = read_eng_csv_sjis(edinet_dir, Path("EdinetcodeDlInfoENG.csv"))
    edinet_df = filter_eng_listedcompanies(edinet_df)

    # Merge (reuse your join_n225_edinet)
    merged_df = join_n225_edinet(n225_df, edinet_df)
    print(f"Merged DataFrame: {len(merged_df)} rows")

    # Sample test
    sample = merged_df.dropna(subset=["EDINET Code", "Securities Identification Code"]).head(1)
    if sample.empty:
        print("No valid sample rows found after merge.")
    else:
        sample_edinet = sample["EDINET Code"].iloc[0]
        sample_sic = sample["Securities Identification Code"].iloc[0]
        sample_symbol = sample["symbol"].iloc[0]

        print(f"\nTesting EDINET ↔ SIC lookup:")
        out_sic = get_sic_from_edinet(sample_edinet, merged_df)
        out_edinet = get_edinet_from_sic(sample_sic, merged_df)
        print(f"EDINET → SIC: {sample_edinet}  →  {out_sic}")
        print(f"SIC → EDINET: {sample_sic}  →  {out_edinet}")

        print(f"\nTesting EDINET ↔ smbol lookup:")
        out_symbol = get_symbol_from_edinet(sample_edinet, merged_df)
        out_edinet2 = get_edinet_from_symbol(sample_symbol, merged_df)
        print(f"EDINET → SIC: {sample_edinet}  →  {out_symbol}")
        print(f"SIC → EDINET: {sample_symbol}  →  {out_edinet}")

        if out_sic and out_edinet and out_symbol and out_edinet2:
            print("Round-trip lookup OK ✅")
        else:
            print("Round-trip lookup failed ❌")
