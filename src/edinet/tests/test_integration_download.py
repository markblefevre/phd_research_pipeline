import os
from pathlib import Path
import subprocess
import sys
import pytest

pytestmark = pytest.mark.integration

def test_real_download_and_unzip(tmp_path: Path):
    script = Path(__file__).resolve().parents[1] / "download_edinet_codelists.py"

    cmd = [
        sys.executable, str(script),
        "--outdir", str(tmp_path),
        "--unzip",  # default true, but explicit is nice
        "--latest-alias",
        "--overwrite",
    ]
    subprocess.check_call(cmd)

    # Verify outputs exist
    assert (tmp_path / "Edinetcode_ja_latest.zip").exists()
    assert (tmp_path / "Edinetcode_en_latest.zip").exists()

    # Unzipped dirs (default names in your script)
    ja_dir = tmp_path / "Edinetcode_ja_latest"
    en_dir = tmp_path / "Edinetcode_en_latest"
    assert ja_dir.exists()
    assert en_dir.exists()

    # At least one file extracted
    assert any(p.is_file() for p in ja_dir.rglob("*"))
    assert any(p.is_file() for p in en_dir.rglob("*"))
