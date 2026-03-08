from pathlib import Path
import pytest

from utils.edinet.paths import edinet_ref_dir, edinet_codelist_dir, edinet_codelist_csv


def test_lang_validation():
    repo_root = Path(__file__).resolve().parents[3]
    with pytest.raises(ValueError):
        edinet_codelist_dir(repo_root, "jp")


def test_edinet_ref_dir_shape():
    repo_root = Path(__file__).resolve().parents[3]
    p = edinet_ref_dir(repo_root)
    assert p.as_posix().endswith("Data/reference/edinet")


def test_edinet_codelist_ja_latest_exists():
    repo_root = Path(__file__).resolve().parents[3]
    d = edinet_codelist_dir(repo_root, "ja")
    assert d.exists(), (
        f"Missing directory: {d}\n"
        "Run utils/edinet/download_edinet_codelists.py --outdir Data/reference/edinet "
        "(defaults create Edinetcode_ja_latest/)."
    )


def test_edinet_codelist_ja_latest_csv_exists():
    repo_root = Path(__file__).resolve().parents[3]
    p = edinet_codelist_csv(repo_root, "ja")
    assert p.exists(), (
        f"Missing CSV: {p}\n"
        "Expected EdinetcodeDlInfo.csv inside Edinetcode_ja_latest/. "
        "Re-run the downloader with unzip enabled."
    )
