import pandas as pd
import pytest

from utils.edinet.lookup import (
    _norm_sic,
    get_sic_from_edinet,
    get_edinet_from_sic,
    get_symbol_from_edinet,
    get_edinet_from_symbol,
)

@pytest.fixture
def merged_df():
    return pd.DataFrame({
        "EDINET Code": ["E02144", "E99999"],
        "Securities Identification Code": ["7203", "1301-"],
        "symbol": ["7203.T", "1301.T"],
    })

def test_norm_sic_basic():
    assert _norm_sic(" 72-03 ") == "7203"

def test_edinet_to_sic(merged_df):
    assert get_sic_from_edinet("e02144", merged_df) == "7203"

def test_sic_to_edinet(merged_df):
    assert get_edinet_from_sic("7203", merged_df) == "E02144"

def test_edinet_to_symbol(merged_df):
    assert get_symbol_from_edinet("E02144", merged_df) == "7203.T"

def test_symbol_to_edinet(merged_df):
    assert get_edinet_from_symbol("7203.t", merged_df) == "E02144"

def test_missing_returns_none(merged_df):
    assert get_sic_from_edinet("E00000", merged_df) is None
    assert get_edinet_from_sic("0000", merged_df) is None
