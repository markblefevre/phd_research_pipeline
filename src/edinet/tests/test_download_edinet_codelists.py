import zipfile
from pathlib import Path
import pytest

# Adjust import to match your file name/module path
from utils.edinet.download_edinet_codelists import build_filename, safe_unzip


def make_zip(zip_path: Path, files: dict[str, bytes]) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)


def test_build_filename_date_stamp():
    assert build_filename("Edinetcode_ja", "20260105", ".zip") == "Edinetcode_ja_20260105.zip"
    assert build_filename("Edinetcode_en", "", ".zip") == "Edinetcode_en.zip"


def test_safe_unzip_extracts_files(tmp_path: Path):
    z = tmp_path / "sample.zip"
    make_zip(z, {"foo.csv": b"a,b\n1,2\n", "bar/baz.txt": b"hello"})

    out_dir = tmp_path / "out"
    extracted = safe_unzip(z, out_dir, overwrite=False)

    assert "foo.csv" in extracted
    assert "bar/baz.txt" in extracted
    assert (out_dir / "foo.csv").read_bytes() == b"a,b\n1,2\n"
    assert (out_dir / "bar" / "baz.txt").read_bytes() == b"hello"


def test_safe_unzip_blocks_zip_slip(tmp_path: Path):
    z = tmp_path / "evil.zip"
    # Attempt path traversal
    make_zip(z, {"../evil.txt": b"nope"})

    out_dir = tmp_path / "out"
    with pytest.raises(RuntimeError, match="Refusing to extract suspicious path"):
        safe_unzip(z, out_dir, overwrite=False)


def test_safe_unzip_no_overwrite(tmp_path: Path):
    z = tmp_path / "sample.zip"
    make_zip(z, {"foo.csv": b"first"})

    out_dir = tmp_path / "out"
    safe_unzip(z, out_dir, overwrite=False)
    assert (out_dir / "foo.csv").read_bytes() == b"first"

    # Now zip contains different content but overwrite=False should error
    make_zip(z, {"foo.csv": b"second"})
    with pytest.raises(RuntimeError, match="Refusing to overwrite existing file"):
        safe_unzip(z, out_dir, overwrite=False)
