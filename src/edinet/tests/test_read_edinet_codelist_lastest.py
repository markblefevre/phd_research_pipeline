from pathlib import Path
import pytest

from utils.edinet.read_edinet_codelist_csv import (
    read_eng_csv_sjis,
    read_jpn_csv_sjis,
    filter_eng_listedcompanies,
    filter_jpn_listedcompanies,
)

pytestmark = pytest.mark.integration


def _repo_root() -> Path:
    # Run pytest from repo root
    return Path.cwd().resolve()


def _latest_dirs():
    root = _repo_root()
    edinet_ref = root / "data" / "reference" / "edinet"
    ja_dir = edinet_ref / "Edinetcode_ja_latest"
    en_dir = edinet_ref / "Edinetcode_en_latest"
    ja_csv = ja_dir / "EdinetcodeDlInfo.csv"
    en_csv = en_dir / "EdinetcodeDlInfo.csv"
    return ja_dir, en_dir, ja_csv, en_csv


def test_latest_files_exist():
    ja_dir, en_dir, ja_csv, en_csv = _latest_dirs()
    assert ja_dir.exists(), f"Missing directory: {ja_dir}"
    assert en_dir.exists(), f"Missing directory: {en_dir}"
    assert ja_csv.exists(), f"Missing CSV: {ja_csv}"
    assert en_csv.exists(), f"Missing CSV: {en_csv}"


def test_read_eng_csv_sjis_latest():
    _, en_dir, _, _ = _latest_dirs()

    # Your function signature is (directory, filename)
    _, eng_df = read_eng_csv_sjis(en_dir, "EdinetcodeDlInfo.csv")

    assert len(eng_df) > 1000
    assert len(eng_df.columns) > 5


def test_read_jpn_csv_sjis_latest():
    ja_dir, _, _, _ = _latest_dirs()

    _, jpn_df = read_jpn_csv_sjis(ja_dir, "EdinetcodeDlInfo.csv")

    assert len(jpn_df) > 1000
    assert len(jpn_df.columns) > 5


def test_filter_eng_listedcompanies_latest():
    _, en_dir, _, _ = _latest_dirs()
    _, eng_df = read_eng_csv_sjis(en_dir, "EdinetcodeDlInfo.csv")

    listed = filter_eng_listedcompanies(eng_df)

    assert 0 < len(listed) < len(eng_df)


def test_filter_jpn_listedcompanies_latest():
    ja_dir, _, _, _ = _latest_dirs()
    _, jpn_df = read_jpn_csv_sjis(ja_dir, "EdinetcodeDlInfo.csv")

    listed = filter_jpn_listedcompanies(jpn_df)

    assert 0 < len(listed) < len(jpn_df)
