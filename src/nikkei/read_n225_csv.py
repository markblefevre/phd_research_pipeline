# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd

def read_n225_csv(path: Path = Path("nikkei225_all.csv")) -> pd.DataFrame:
    """
    Read the newer Nikkei 225 CSV and return a tidy DataFrame.

    Parameters
    ----------
    path : Path
        Path to the CSV file (works on macOS, Windows, NAS, etc.)

    Expected CSV columns:
      - code (int or str)
      - symbol (str, e.g., '4151.T')
      - name_ja (optional)
      - company_ja (optional)
      - sector_sub (optional)
      - asof_date (YYYY-MM-DD)

    Returns
    -------
    pandas.DataFrame
        Columns: ['code','symbol','company_ja','name_ja','sector_sub','asof_date']
    """
    path = Path(path).expanduser().resolve()

    if not path.exists():
        raise FileNotFoundError(f"Nikkei 225 CSV not found: {path}")

    df = pd.read_csv(path, dtype={"code": str})
    df["code"] = df["code"].str.zfill(4)

    # Use symbol if present, else create Yahoo-style symbol
    if "symbol" not in df.columns or df["symbol"].isna().any():
        df["symbol"] = df["code"] + ".T"

    # Parse date if available
    if "asof_date" in df.columns:
        df["asof_date"] = pd.to_datetime(df["asof_date"], errors="coerce").dt.date
    else:
        df["asof_date"] = pd.NaT

    # Ensure missing columns exist
    for col in ["company_ja", "name_ja", "sector_sub"]:
        if col not in df.columns:
            df[col] = pd.NA

    return df[["code", "symbol", "company_ja", "name_ja", "sector_sub", "asof_date"]]


if __name__ == "__main__":
    # Example: read from a NAS or local path
    example_path = Path(r'\\NAS\nas1\nikkei\nikkei225_all.csv')  # adjust to your setup
    df = read_n225_csv(example_path)
    print(f"Loaded {len(df)} rows from {example_path}")
    print(df.head().to_string(index=False))
