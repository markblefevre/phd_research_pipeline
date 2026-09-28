#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import zipfile
from pathlib import Path

import pandas as pd


DEFAULT_FAILURE_AUDIT = (
    "data/interim/paper2/market_reaction/diagnostics/"
    "stage7d_market_model_failure_audit.csv"
)
DEFAULT_EDINET_ROOT = "data/raw/paper2/edinet"
DEFAULT_OUTPUT = (
    "data/interim/paper2/market_reaction/diagnostics/"
    "stage7d_failed_event_venue_audit.csv"
)


# Conservative venue patterns.  We intentionally distinguish TOKYO PRO
# from ordinary TSE markets because the event-study baseline uses the
# ordinary TSE/TOPIX equity universe.
PATTERNS = {
    "TOKYO_PRO": [
        r"TOKYO\s+PRO\s+Market",
        r"ＴＯＫＹＯ\s*ＰＲＯ\s*Ｍａｒｋｅｔ",
        r"東京プロマーケット",
        r"東京証券取引所\s*TOKYO\s+PRO",
    ],
    "TSE_PRIME": [
        r"東京証券取引所.{0,20}プライム",
        r"Tokyo Stock Exchange.{0,20}Prime",
        r"TSE.{0,10}Prime",
    ],
    "TSE_STANDARD": [
        r"東京証券取引所.{0,20}スタンダード",
        r"Tokyo Stock Exchange.{0,20}Standard",
        r"TSE.{0,10}Standard",
    ],
    "TSE_GROWTH": [
        r"東京証券取引所.{0,20}グロース",
        r"Tokyo Stock Exchange.{0,20}Growth",
        r"TSE.{0,10}Growth",
    ],
    "TSE_FIRST": [
        r"東京証券取引所.{0,20}市場第一部",
        r"東京証券取引所.{0,20}第一部",
        r"Tokyo Stock Exchange.{0,20}First Section",
    ],
    "TSE_SECOND": [
        r"東京証券取引所.{0,20}市場第二部",
        r"東京証券取引所.{0,20}第二部",
        r"Tokyo Stock Exchange.{0,20}Second Section",
    ],
    "TSE_MOTHERS": [
        r"東京証券取引所.{0,20}マザーズ",
        r"Tokyo Stock Exchange.{0,20}Mothers",
    ],
    "TSE_JASDAQ": [
        r"東京証券取引所.{0,30}JASDAQ",
        r"東京証券取引所.{0,30}ジャスダック",
        r"Tokyo Stock Exchange.{0,30}JASDAQ",
    ],
    "NAGOYA": [
        r"名古屋証券取引所",
        r"Nagoya Stock Exchange",
    ],
    "FUKUOKA": [
        r"福岡証券取引所",
        r"Fukuoka Stock Exchange",
    ],
    "SAPPORO": [
        r"札幌証券取引所",
        r"Sapporo Securities Exchange",
        r"Sapporo Stock Exchange",
    ],
}

ORDINARY_TSE = {
    "TSE_PRIME",
    "TSE_STANDARD",
    "TSE_GROWTH",
    "TSE_FIRST",
    "TSE_SECOND",
    "TSE_MOTHERS",
    "TSE_JASDAQ",
}

REGIONAL = {"NAGOYA", "FUKUOKA", "SAPPORO"}


def repo_root() -> Path:
    # tests/audit_stage7d_failed_event_venues.py -> repo root
    return Path(__file__).resolve().parents[1]


def norm_code(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip()
    return out.str.replace(r"\.0$", "", regex=True)


def read_failure_events(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        dtype={
            "edinetCode": "string",
            "curr_docID": "string",
            "secCode": "string",
        },
        low_memory=False,
    )

    required = {
        "edinetCode",
        "curr_docID",
        "secCode",
        "eventTradingDate",
        "marketModelStatus",
        "productionEstimationObs",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{path} missing required columns: {sorted(missing)}"
        )

    df["secCode"] = norm_code(df["secCode"])
    df["eventTradingDate"] = pd.to_datetime(
        df["eventTradingDate"], errors="raise"
    ).dt.normalize()

    # The failure audit already contains only failed events, but keep this
    # defensive in case the source file is later expanded.
    if "marketModelStatus" in df.columns:
        df = df.loc[
            df["marketModelStatus"] != "estimated"
        ].copy()

    return df


def candidate_zip_paths(
    edinet_root: Path,
    edinet_code: str,
    doc_id: str,
) -> list[Path]:
    return [
        edinet_root / edinet_code / doc_id / f"{doc_id}.zip",
        edinet_root / edinet_code / doc_id / f"{doc_id}_csv.zip",
    ]


def decode_bytes(data: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            pass
    return data.decode("utf-8", errors="ignore")


def extract_search_text(zip_path: Path) -> tuple[str, int, list[str]]:
    """
    Concatenate textual XBRL/iXBRL/XML/HTML content from the EDINET ZIP.
    Returns text, files_scanned, filenames_scanned.
    """
    wanted_suffixes = (
        ".xbrl",
        ".xml",
        ".htm",
        ".html",
        ".xhtml",
    )

    chunks: list[str] = []
    names: list[str] = []

    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            low = name.lower()
            if not low.endswith(wanted_suffixes):
                continue

            # Focus on filing content rather than images/resources.
            try:
                data = zf.read(name)
            except KeyError:
                continue

            text = decode_bytes(data)
            chunks.append(text)
            names.append(name)

    return "\n".join(chunks), len(names), names


def find_hits(text: str) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}

    for venue, patterns in PATTERNS.items():
        venue_hits: list[str] = []
        for pattern in patterns:
            for m in re.finditer(
                pattern,
                text,
                flags=re.IGNORECASE | re.DOTALL,
            ):
                start = max(0, m.start() - 80)
                end = min(len(text), m.end() + 120)
                snippet = re.sub(
                    r"\s+",
                    " ",
                    text[start:end],
                ).strip()
                venue_hits.append(snippet)

                # Enough evidence for diagnostic classification; avoid
                # bloating output with repeated boilerplate.
                if len(venue_hits) >= 3:
                    break
            if len(venue_hits) >= 3:
                break

        if venue_hits:
            hits[venue] = venue_hits

    return hits


def classify_hits(hits: dict[str, list[str]]) -> tuple[str, str]:
    found = set(hits)

    has_ordinary_tse = bool(found & ORDINARY_TSE)
    has_tokyo_pro = "TOKYO_PRO" in found
    has_regional = bool(found & REGIONAL)

    if has_ordinary_tse:
        if has_tokyo_pro or has_regional:
            return (
                "ORDINARY_TSE_WITH_OTHER_VENUE_MENTIONS",
                ",".join(sorted(found)),
            )
        return (
            "ORDINARY_TSE",
            ",".join(sorted(found & ORDINARY_TSE)),
        )

    if has_tokyo_pro:
        if has_regional:
            return (
                "TOKYO_PRO_WITH_REGIONAL_MENTIONS",
                ",".join(sorted(found)),
            )
        return ("TOKYO_PRO", "TOKYO_PRO")

    regional_found = found & REGIONAL
    if regional_found:
        if len(regional_found) == 1:
            venue = next(iter(regional_found))
            return ("REGIONAL_ONLY", venue)
        return (
            "MULTIPLE_REGIONAL",
            ",".join(sorted(regional_found)),
        )

    # Generic TSE phrase can appear without a market segment in older
    # disclosures.  Keep it unresolved rather than assuming eligibility.
    generic_tse = bool(
        re.search(
            r"東京証券取引所|Tokyo Stock Exchange",
            " ".join(
                snippet
                for snippets in hits.values()
                for snippet in snippets
            ),
            flags=re.IGNORECASE,
        )
    )
    if generic_tse:
        return ("TSE_UNSPECIFIED", "")

    return ("UNRESOLVED", "")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit historical listing venue for Stage 7D failed events "
            "using each event's EDINET filing ZIP."
        )
    )
    parser.add_argument(
        "--failure-audit",
        default=DEFAULT_FAILURE_AUDIT,
    )
    parser.add_argument(
        "--edinet-root",
        default=DEFAULT_EDINET_ROOT,
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
    )
    args = parser.parse_args()

    root = repo_root()

    failure_path = root / args.failure_audit
    edinet_root = root / args.edinet_root
    output_path = root / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    events = read_failure_events(failure_path)

    print("=== Stage 7D failed-event venue audit ===")
    print(f"Failed events:      {len(events):,}")
    print(f"Unique securities: {events['secCode'].nunique():,}")
    print()

    rows: list[dict] = []

    for i, row in enumerate(
        events.itertuples(index=False),
        start=1,
    ):
        edinet_code = str(row.edinetCode)
        doc_id = str(row.curr_docID)
        sec_code = str(row.secCode)

        zip_path = next(
            (
                p
                for p in candidate_zip_paths(
                    edinet_root,
                    edinet_code,
                    doc_id,
                )
                if p.exists()
            ),
            None,
        )

        if zip_path is None:
            rows.append(
                {
                    "edinetCode": edinet_code,
                    "curr_docID": doc_id,
                    "secCode": sec_code,
                    "eventTradingDate": row.eventTradingDate,
                    "marketModelStatus": row.marketModelStatus,
                    "productionEstimationObs": (
                        row.productionEstimationObs
                    ),
                    "zipFound": False,
                    "zipPath": "",
                    "filesScanned": 0,
                    "venueClass": "ZIP_NOT_FOUND",
                    "venueDetail": "",
                    "matchedVenueLabels": "",
                    "evidenceSnippet1": "",
                    "evidenceSnippet2": "",
                    "evidenceSnippet3": "",
                }
            )
            continue

        try:
            text, files_scanned, _ = extract_search_text(
                zip_path
            )
            hits = find_hits(text)
            venue_class, venue_detail = classify_hits(hits)

            evidence: list[str] = []
            for label in sorted(hits):
                for snippet in hits[label]:
                    evidence.append(
                        f"[{label}] {snippet}"
                    )
                    if len(evidence) >= 3:
                        break
                if len(evidence) >= 3:
                    break

            rows.append(
                {
                    "edinetCode": edinet_code,
                    "curr_docID": doc_id,
                    "secCode": sec_code,
                    "eventTradingDate": row.eventTradingDate,
                    "marketModelStatus": row.marketModelStatus,
                    "productionEstimationObs": (
                        row.productionEstimationObs
                    ),
                    "zipFound": True,
                    "zipPath": str(zip_path),
                    "filesScanned": files_scanned,
                    "venueClass": venue_class,
                    "venueDetail": venue_detail,
                    "matchedVenueLabels": ",".join(
                        sorted(hits)
                    ),
                    "evidenceSnippet1": (
                        evidence[0]
                        if len(evidence) > 0
                        else ""
                    ),
                    "evidenceSnippet2": (
                        evidence[1]
                        if len(evidence) > 1
                        else ""
                    ),
                    "evidenceSnippet3": (
                        evidence[2]
                        if len(evidence) > 2
                        else ""
                    ),
                }
            )

        except Exception as exc:
            rows.append(
                {
                    "edinetCode": edinet_code,
                    "curr_docID": doc_id,
                    "secCode": sec_code,
                    "eventTradingDate": row.eventTradingDate,
                    "marketModelStatus": row.marketModelStatus,
                    "productionEstimationObs": (
                        row.productionEstimationObs
                    ),
                    "zipFound": True,
                    "zipPath": str(zip_path),
                    "filesScanned": 0,
                    "venueClass": "READ_ERROR",
                    "venueDetail": type(exc).__name__,
                    "matchedVenueLabels": "",
                    "evidenceSnippet1": str(exc),
                    "evidenceSnippet2": "",
                    "evidenceSnippet3": "",
                }
            )

        if i % 50 == 0:
            print(f"Processed {i:,}/{len(events):,} events")

    out = pd.DataFrame(rows)

    # Join back useful failure-QC fields when present.
    extra_cols = [
        "edinetCode",
        "curr_docID",
        "secCode",
        "usablePairedReturnsInWindow",
        "rawSecurityRows",
        "validCloseRows",
        "rawRowsInWindow",
        "validCloseRowsInWindow",
    ]
    available = [
        c for c in extra_cols
        if c in events.columns
    ]
    if len(available) > 3:
        extra = events[available].copy()
        out = out.merge(
            extra,
            on=["edinetCode", "curr_docID", "secCode"],
            how="left",
            validate="one_to_one",
        )

    out.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
    )

    print()
    print("Venue-class counts:")
    print(
        out["venueClass"]
        .value_counts(dropna=False)
        .to_string()
    )
    print()

    by_security = (
        out.groupby("secCode", observed=True)
        .agg(
            failedEvents=("secCode", "size"),
            venueClasses=(
                "venueClass",
                lambda s: ",".join(
                    sorted(set(s.astype(str)))
                ),
            ),
            minObs=(
                "productionEstimationObs",
                "min",
            ),
            maxObs=(
                "productionEstimationObs",
                "max",
            ),
        )
        .sort_values(
            ["failedEvents", "secCode"],
            ascending=[False, True],
        )
    )

    print("Security-level venue summary (first 40):")
    print(by_security.head(40).to_string())
    print()

    suspect = out.loc[
        out["venueClass"].isin(
            {
                "TOKYO_PRO",
                "TOKYO_PRO_WITH_REGIONAL_MENTIONS",
                "REGIONAL_ONLY",
                "MULTIPLE_REGIONAL",
            }
        )
    ]

    print(
        "Events clearly outside ordinary TSE universe: "
        f"{len(suspect):,}"
    )
    print(
        "Unique securities clearly outside ordinary TSE universe: "
        f"{suspect['secCode'].nunique():,}"
    )
    print()

    if not suspect.empty:
        print(
            suspect[
                [
                    "secCode",
                    "eventTradingDate",
                    "productionEstimationObs",
                    "venueClass",
                    "venueDetail",
                ]
            ]
            .sort_values(
                ["secCode", "eventTradingDate"]
            )
            .head(100)
            .to_string(index=False)
        )
        print()

    unresolved = out.loc[
        out["venueClass"].isin(
            {
                "UNRESOLVED",
                "TSE_UNSPECIFIED",
                "ZIP_NOT_FOUND",
                "READ_ERROR",
            }
        )
    ]

    print(f"Unresolved events: {len(unresolved):,}")
    print(
        f"Unresolved securities: "
        f"{unresolved['secCode'].nunique():,}"
    )
    print()
    print(f"Audit written: {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
