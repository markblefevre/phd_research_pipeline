from pathlib import Path
import pytest

from utils.edinet.read_edinet_codelist_csv import (
    read_eng_csv_sjis,
    filter_eng_listedcompanies,
)
from utils.nikkei.read_n225_csv import read_n225_csv
from utils.nikkei.join_n225_edinet import join_n225_edinet

from utils.edinet.lookup import (
    get_sic_from_edinet,
    get_edinet_from_sic,
    get_symbol_from_edinet,
    get_edinet_from_symbol,
)

pytestmark = pytest.mark.integration


def _repo_root() -> Path:
    return Path.cwd().resolve()


def _edinet_en_latest_dir() -> Path:
    root = _repo_root()
    return root / "data" / "reference" / "edinet" / "Edinetcode_en_latest"


def _n225_ref_csv() -> Path:
    """
    Adjust this path to wherever your repo stores the Nikkei 225 reference CSV.
    Common candidates:
      data/reference/nikkei/n225.csv
      data/reference/nikkei/N225_latest.csv
      data/reference/nikkei/n225_latest.csv
    """
    root = _repo_root()
    candidates = [
        root / "nikkei" / "nikkei225_all.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    pytest.skip("No Nikkei 225 reference CSV found under data/reference/nikkei (update _n225_ref_csv()).")


def test_lookup_pipeline_from_reference_data():
    # --- EDINET EN latest ---
    en_dir = _edinet_en_latest_dir()
    en_csv = en_dir / "EdinetcodeDlInfo.csv"
    assert en_dir.exists(), f"Missing directory: {en_dir}"
    assert en_csv.exists(), f"Missing CSV: {en_csv}"

    _, edinet_df = read_eng_csv_sjis(en_dir, "EdinetcodeDlInfo.csv")
    listed_df = filter_eng_listedcompanies(edinet_df)

    # --- Nikkei 225 reference ---
    n225_csv = _n225_ref_csv()
    n225_df = read_n225_csv(n225_csv)

    # --- Join ---
    merged = join_n225_edinet(n225_df, listed_df)

    # Basic sanity
    assert len(merged) > 0
    assert "EDINET Code" in merged.columns
    assert "Securities Identification Code" in merged.columns

    # --- Representative lookups (pick first non-null values) ---
    row = merged.dropna(subset=["EDINET Code", "Securities Identification Code"]).iloc[0]
    edinet = str(row["EDINET Code"])
    sic = str(row["Securities Identification Code"])

    assert get_sic_from_edinet(edinet, merged) is not None
    assert get_edinet_from_sic(sic, merged) is not None

    # symbol lookups only if symbols exist in merged
    if "symbol" in merged.columns and merged["symbol"].notna().any():
        row2 = merged.dropna(subset=["EDINET Code", "symbol"]).iloc[0]
        edinet2 = str(row2["EDINET Code"])
        sym = str(row2["symbol"])

        assert get_symbol_from_edinet(edinet2, merged) is not None
        assert get_edinet_from_symbol(sym, merged) is not None
