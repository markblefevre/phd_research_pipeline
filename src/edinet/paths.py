from __future__ import annotations
from pathlib import Path


def repo_root_from_file(file: Path, levels_up: int = 3) -> Path:
    file = Path(file).resolve()
    for _ in range(levels_up):
        file = file.parent
    return file


def edinet_ref_dir(repo_root: Path) -> Path:
    """
    Return <repo_root>/(data|Data)/reference/edinet.

    We support both 'data' and 'Data' because this repo has been used on
    case-insensitive and case-sensitive filesystems (incl. NAS shares).
    """
    repo_root = Path(repo_root)

    candidates = [
        repo_root / "data" / "reference" / "edinet",
        repo_root / "Data" / "reference" / "edinet",
    ]

    for c in candidates:
        if c.exists():
            return c

    # If neither exists, return the preferred path (and let callers fail loudly)
    return candidates[0]


def edinet_codelist_dir(repo_root: Path, lang: str = "ja") -> Path:
    if lang not in {"ja", "en"}:
        raise ValueError(f"lang must be 'ja' or 'en', got: {lang!r}")
    return edinet_ref_dir(repo_root) / f"Edinetcode_{lang}_latest"


def edinet_codelist_csv(repo_root: Path, lang: str = "ja") -> Path:
    return edinet_codelist_dir(repo_root, lang) / "EdinetcodeDlInfo.csv"
