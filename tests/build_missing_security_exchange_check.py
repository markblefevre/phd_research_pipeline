from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile, BadZipFile
import re
import xml.etree.ElementTree as ET

import pandas as pd


ROOT = Path(r"Z:\Documents\Education\2021 EDHEC Exec PhD\5 Pipeline")

MISSING_CSV = (
    ROOT
    / "data/interim/paper2/market_reaction/missing_security_events.csv"
)

PAIRS_CSV = (
    ROOT
    / "data/interim/paper2/longitudinal/research_eligible_pairs.csv"
)

EDINET_ROOT = ROOT / "data/raw/paper2/edinet"

OUT_DIR = ROOT / "data/interim/paper2/market_reaction"
OUT_DIR.mkdir(parents=True, exist_ok=True)

EVENT_OUT = OUT_DIR / "missing_security_exchange_events.csv"
SUMMARY_OUT = OUT_DIR / "missing_security_exchange_check.csv"


# ---------------------------------------------------------------------
# EDINET XBRL concept
# ---------------------------------------------------------------------

EXCHANGE_CONCEPT = (
    "NameOfFinancialInstrumentsExchangeOnWhichSecuritiesAreListed"
    "OrAuthorizedFinancialInstrumentsBusinessAssociationToWhich"
    "SecuritiesAreRegistered"
)


def local_name(tag: str) -> str:
    """
    Convert:
        {namespace}SomeConcept
    or:
        jpcrp_cor:SomeConcept
    to:
        SomeConcept
    """
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    if ":" in tag:
        return tag.rsplit(":", 1)[-1]
    return tag


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None

    value = re.sub(r"\s+", " ", value).strip()

    if not value:
        return None

    return value


def classify_exchange(values: list[str]) -> str:
    """
    Convert raw EDINET exchange descriptions into broad exchange categories.

    Keep TOKYO_PRO_MARKET separate from ordinary TSE because J-Quants
    stock-price coverage may differ.
    """
    if not values:
        return "UNKNOWN"

    text = " | ".join(values)
    text_upper = text.upper()

    categories = set()

    # Check PRO Market before generic Tokyo.
    if (
        "TOKYO PRO MARKET" in text_upper
        or "TOKYO PRO" in text_upper
        or "プロマーケット" in text
    ):
        categories.add("TOKYO_PRO_MARKET")

    # Regional exchanges
    if "札幌" in text or "SAPPORO" in text_upper:
        categories.add("SAPPORO")

    if "名古屋" in text or "NAGOYA" in text_upper:
        categories.add("NAGOYA")

    if "福岡" in text or "FUKUOKA" in text_upper:
        categories.add("FUKUOKA")

    # Tokyo Stock Exchange
    if (
        "東京証券取引所" in text
        or "東京証券" in text
        or "TOKYO STOCK EXCHANGE" in text_upper
    ):
        # Don't automatically erase PRO Market distinction.
        if "TOKYO_PRO_MARKET" not in categories:
            categories.add("TSE")

    if not categories:
        return "OTHER_UNKNOWN"

    if len(categories) == 1:
        return next(iter(categories))

    return "MULTIPLE:" + "+".join(sorted(categories))


def extract_exchange_from_zip(zip_path: Path) -> tuple[list[str], str]:
    """
    Search all XBRL/XML files in one EDINET filing ZIP for the standardized
    exchange-name concept.

    Returns:
        (unique_values, extraction_status)
    """

    if not zip_path.exists():
        return [], "ZIP_MISSING"

    try:
        with ZipFile(zip_path, "r") as zf:

            candidates = [
                name
                for name in zf.namelist()
                if name.lower().endswith((".xbrl", ".xml"))
            ]

            if not candidates:
                return [], "NO_XBRL_XML"

            values = []

            for name in candidates:
                try:
                    raw = zf.read(name)
                    root = ET.fromstring(raw)
                except Exception:
                    continue

                for elem in root.iter():
                    if local_name(elem.tag) != EXCHANGE_CONCEPT:
                        continue

                    value = clean_text("".join(elem.itertext()))

                    if value:
                        values.append(value)

            # Deduplicate but preserve order.
            values = list(dict.fromkeys(values))

            if values:
                return values, "OK"

            return [], "FACT_NOT_FOUND"

    except BadZipFile:
        return [], "BAD_ZIP"
    except Exception as exc:
        return [], f"ERROR:{type(exc).__name__}"


# ---------------------------------------------------------------------
# Load missing events
# ---------------------------------------------------------------------

missing = pd.read_csv(
    MISSING_CSV,
    dtype={
        "secCode": "string",
        "edinetCode": "string",
        "curr_docID": "string",
    },
    low_memory=False,
)

pairs = pd.read_csv(
    PAIRS_CSV,
    dtype={
        "secCode": "string",
        "edinetCode": "string",
        "curr_docID": "string",
    },
    low_memory=False,
)


# ---------------------------------------------------------------------
# Attach filer metadata if not already present
# ---------------------------------------------------------------------

meta_cols = [
    "edinetCode",
    "secCode",
    "curr_docID",
    "filerName",
    "curr_submitDateTime",
    "curr_periodEnd",
]

meta_cols = [c for c in meta_cols if c in pairs.columns]

meta = (
    pairs[meta_cols]
    .drop_duplicates(
        subset=["edinetCode", "secCode", "curr_docID"]
    )
)

join_keys = [
    c
    for c in ["edinetCode", "secCode", "curr_docID"]
    if c in missing.columns and c in meta.columns
]

df = missing.merge(
    meta,
    on=join_keys,
    how="left",
    suffixes=("", "_pair"),
)


# Prefer metadata values already present in missing-security file.
for col in ["filerName", "curr_submitDateTime", "curr_periodEnd"]:
    pair_col = f"{col}_pair"

    if pair_col in df.columns:
        if col not in df.columns:
            df[col] = df[pair_col]
        else:
            df[col] = df[col].fillna(df[pair_col])


# ---------------------------------------------------------------------
# Extract exchange from every missing event filing
# ---------------------------------------------------------------------

results = []

cache = {}

print("==========================================")
print("Stage 7A.1 EDINET Exchange Classification")
print("==========================================")
print(f"Missing events: {len(df):,}")

for i, row in enumerate(df.itertuples(index=False), 1):

    edinet_code = str(row.edinetCode)
    doc_id = str(row.curr_docID)

    zip_path = (
        EDINET_ROOT
        / edinet_code
        / doc_id
        / f"{doc_id}.zip"
    )

    # Cache in case an event/file is encountered more than once.
    key = str(zip_path)

    if key not in cache:
        cache[key] = extract_exchange_from_zip(zip_path)

    exchange_values, status = cache[key]

    result = row._asdict()

    result["edinetZip"] = str(zip_path)
    result["exchangeExtractionStatus"] = status

    result["exchangeRaw"] = (
        " | ".join(exchange_values)
        if exchange_values
        else pd.NA
    )

    result["exchangeClassification"] = classify_exchange(
        exchange_values
    )

    results.append(result)

    if i % 100 == 0 or i == len(df):
        print(f"  processed {i:4d}/{len(df):4d}")


events = pd.DataFrame(results)

events.to_csv(
    EVENT_OUT,
    index=False,
    encoding="utf-8-sig",
)


# ---------------------------------------------------------------------
# Summarize by security
# ---------------------------------------------------------------------

events["eventDate"] = pd.to_datetime(
    events.get("submitDateTime", events.get("curr_submitDateTime")),
    errors="coerce",
)

summary = (
    events
    .groupby(
        ["secCode", "edinetCode", "filerName"],
        dropna=False,
    )
    .agg(
        eventCount=("curr_docID", "count"),
        firstEventDate=("eventDate", "min"),
        lastEventDate=("eventDate", "max"),

        exchangeClassifications=(
            "exchangeClassification",
            lambda x: " | ".join(
                sorted(
                    set(
                        str(v)
                        for v in x.dropna()
                    )
                )
            ),
        ),

        exchangeRawValues=(
            "exchangeRaw",
            lambda x: " | ".join(
                sorted(
                    set(
                        str(v)
                        for v in x.dropna()
                    )
                )
            ),
        ),

        successfulExtractions=(
            "exchangeExtractionStatus",
            lambda x: int((x == "OK").sum()),
        ),

        failedExtractions=(
            "exchangeExtractionStatus",
            lambda x: int((x != "OK").sum()),
        ),
    )
    .reset_index()
    .sort_values(
        ["eventCount", "secCode"],
        ascending=[False, True],
    )
)

summary.to_csv(
    SUMMARY_OUT,
    index=False,
    encoding="utf-8-sig",
)


# ---------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------

print("\nExtraction status:")
print(
    events["exchangeExtractionStatus"]
    .value_counts(dropna=False)
    .to_string()
)

print("\nExchange classification by event:")
print(
    events["exchangeClassification"]
    .value_counts(dropna=False)
    .to_string()
)

print("\nUnique securities by classification:")

security_class = (
    events[
        [
            "secCode",
            "edinetCode",
            "exchangeClassification",
        ]
    ]
    .drop_duplicates()
)

print(
    security_class["exchangeClassification"]
    .value_counts(dropna=False)
    .to_string()
)

print("\nTop 30 missing securities:")
print(
    summary[
        [
            "secCode",
            "edinetCode",
            "filerName",
            "eventCount",
            "exchangeClassifications",
        ]
    ]
    .head(30)
    .to_string(index=False)
)

print("\nOutputs:")
print(EVENT_OUT)
print(SUMMARY_OUT)