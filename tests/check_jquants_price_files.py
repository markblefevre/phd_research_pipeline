from pathlib import Path
import re
import pandas as pd

PRICE_DIR = Path(
    r"Z:\Documents\Education\2021 EDHEC Exec PhD\5 Pipeline\data\raw\paper2\prices"
)

PATTERN = re.compile(r"^equities_bars_daily_(\d{6})\.csv\.gz$")

found = {}

for path in PRICE_DIR.iterdir():
    m = PATTERN.match(path.name)
    if m:
        yyyymm = m.group(1)
        found[yyyymm] = path

if not found:
    raise SystemExit(f"No matching price files found in:\n{PRICE_DIR}")

months_found = sorted(found)

first = pd.Period(months_found[0], freq="M")
last = pd.Period(months_found[-1], freq="M")

expected = pd.period_range(first, last, freq="M")
expected_str = [p.strftime("%Y%m") for p in expected]

missing = [m for m in expected_str if m not in found]

print(f"Directory:    {PRICE_DIR}")
print(f"Files found:  {len(found)}")
print(f"First month:  {months_found[0]}")
print(f"Last month:   {months_found[-1]}")
print(f"Expected:     {len(expected_str)} months")
print(f"Missing:      {len(missing)} months")

if missing:
    print("\nMissing files:")
    for month in missing:
        print(f"  equities_bars_daily_{month}.csv.gz")
else:
    print("\nNo gaps between first and last downloaded month.")