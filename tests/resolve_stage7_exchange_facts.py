#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd


DEFAULT_AUDIT = (
    "data/interim/paper2/market_reaction/diagnostics/"
    "stage7_full_sample_venue_audit.csv"
)
DEFAULT_OUTPUT = (
    "data/interim/paper2/market_reaction/diagnostics/"
    "stage7_targeted_exchange_fact_resolution.csv"
)

TARGET_CLASSES = {
    "REGIONAL_ONLY",
    "MULTIPLE_REGIONAL",
    "TSE_UNSPECIFIED",
}

VENUE_PATTERNS = {
    "TOKYO_PRO": [
        r"TOKYO\s+PRO\s+Market",
        r"ＴＯＫＹＯ\s*ＰＲＯ\s*Ｍａｒｋｅｔ",
        r"東京プロマーケット",
    ],
    "TSE_PRIME": [
        r"東京証券取引所.{0,30}プライム",
        r"東証.{0,15}プライム",
        r"Tokyo Stock Exchange.{0,30}Prime",
    ],
    "TSE_STANDARD": [
        r"東京証券取引所.{0,30}スタンダード",
        r"東証.{0,15}スタンダード",
        r"Tokyo Stock Exchange.{0,30}Standard",
    ],
    "TSE_GROWTH": [
        r"東京証券取引所.{0,30}グロース",
        r"東証.{0,15}グロース",
        r"Tokyo Stock Exchange.{0,30}Growth",
    ],
    "TSE_FIRST": [
        r"東京証券取引所.{0,30}(?:市場)?第一部",
        r"東証.{0,15}(?:市場)?第一部",
        r"Tokyo Stock Exchange.{0,30}First Section",
    ],
    "TSE_SECOND": [
        r"東京証券取引所.{0,30}(?:市場)?第二部",
        r"東証.{0,15}(?:市場)?第二部",
        r"Tokyo Stock Exchange.{0,30}Second Section",
    ],
    "TSE_MOTHERS": [
        r"東京証券取引所.{0,30}マザーズ",
        r"東証.{0,15}マザーズ",
        r"Tokyo Stock Exchange.{0,30}Mothers",
    ],
    "TSE_JASDAQ": [
        r"東京証券取引所.{0,40}(?:JASDAQ|ジャスダック)",
        r"東証.{0,20}(?:JASDAQ|ジャスダック)",
        r"Tokyo Stock Exchange.{0,40}JASDAQ",
    ],
    "TSE_GENERIC": [
        r"東京証券取引所",
        r"Tokyo Stock Exchange",
        r"東証",
    ],
    "NAGOYA": [
        r"名古屋証券取引所",
        r"名証",
        r"Nagoya Stock Exchange",
    ],
    "FUKUOKA": [
        r"福岡証券取引所",
        r"福証",
        r"Fukuoka Stock Exchange",
    ],
    "SAPPORO": [
        r"札幌証券取引所",
        r"札証",
        r"Sapporo (?:Securities|Stock) Exchange",
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
    "TSE_GENERIC",
}
REGIONAL = {"NAGOYA", "FUKUOKA", "SAPPORO"}

# EDINET taxonomy concepts vary by vintage. Rather than hard-coding one exact
# local name, target concept names that clearly concern listing/exchange venue.
CONCEPT_HINTS = (
    "financialinstrumentsexchange",
    "financialinstrumentexchange",
    "stockexchange",
    "securitiesexchange",
    "exchangeonwhich",
    "exchangewher",
    "listedexchange",
    "listingexchange",
    "marketwher",
)

# Useful words for a fallback concept-name score.
CONCEPT_WORDS = (
    "exchange",
    "listed",
    "listing",
    "market",
    "financialinstrument",
    "securities",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_path(root: Path, value: str) -> Path:
    p = Path(value).expanduser()
    return p if p.is_absolute() else root / p


def norm_code(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip()
    return out.str.replace(r"\.0$", "", regex=True)


def local_name(tag: str) -> str:
    if "}" in tag:
        tag = tag.rsplit("}", 1)[1]
    if ":" in tag:
        tag = tag.rsplit(":", 1)[1]
    return tag


def clean_text(value: str) -> str:
    value = html.unescape(value or "")
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def concept_relevance(name: str) -> int:
    n = re.sub(r"[^a-z0-9]", "", name.lower())
    if any(h in n for h in CONCEPT_HINTS):
        return 100

    score = sum(1 for w in CONCEPT_WORDS if w in n)
    if "exchange" in n and ("listed" in n or "listing" in n):
        score += 5
    if "financialinstrument" in n and "exchange" in n:
        score += 5
    return score


def classify_value(value: str) -> tuple[str, str]:
    labels: set[str] = set()
    for label, patterns in VENUE_PATTERNS.items():
        if any(re.search(p, value, flags=re.IGNORECASE | re.DOTALL) for p in patterns):
            labels.add(label)

    # Specific TSE segment supersedes generic TSE for display purposes.
    specific_tse = labels & (ORDINARY_TSE - {"TSE_GENERIC"})
    if specific_tse:
        labels.discard("TSE_GENERIC")

    has_tse = bool(labels & ORDINARY_TSE)
    has_pro = "TOKYO_PRO" in labels
    regional = labels & REGIONAL

    if has_tse:
        if has_pro:
            cls = "ORDINARY_TSE_AND_TOKYO_PRO"
        elif regional:
            cls = "ORDINARY_TSE_AND_REGIONAL"
        else:
            cls = "ORDINARY_TSE"
    elif has_pro:
        cls = "TOKYO_PRO_WITH_REGIONAL" if regional else "TOKYO_PRO"
    elif regional:
        cls = "REGIONAL_ONLY" if len(regional) == 1 else "MULTIPLE_REGIONAL"
    else:
        cls = "UNRESOLVED"

    return cls, ",".join(sorted(labels))


def candidate_zip_paths(
    edinet_root: Path,
    edinet_code: str,
    doc_id: str,
) -> list[Path]:
    return [
        edinet_root / edinet_code / doc_id / f"{doc_id}.zip",
        edinet_root / edinet_code / doc_id / f"{doc_id}_csv.zip",
    ]


def public_doc_members(zf: zipfile.ZipFile) -> list[str]:
    suffixes = (".xbrl", ".xml", ".htm", ".html", ".xhtml")
    names = [
        n for n in zf.namelist()
        if "XBRL/PublicDoc/" in n.replace("\\", "/")
        and n.lower().endswith(suffixes)
    ]
    if names:
        return names
    return [n for n in zf.namelist() if n.lower().endswith(suffixes)]


def decode_bytes(data: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            pass
    return data.decode("utf-8", errors="ignore")


def extract_xml_facts(text: str, filename: str) -> list[dict]:
    """
    Extract potentially relevant XBRL/iXBRL facts.

    Handles:
      - ordinary XML/XBRL elements, where the tag itself is the taxonomy concept
      - inline XBRL nonNumeric/nonFraction facts, where @name is the concept
    """
    facts: list[dict] = []

    # XML parser first. EDINET XBRL is normally well formed.
    try:
        root = ET.fromstring(text)
        for elem in root.iter():
            tag_local = local_name(str(elem.tag))
            attrs = {local_name(str(k)): str(v) for k, v in elem.attrib.items()}

            concept = attrs.get("name", tag_local)
            relevance = concept_relevance(concept)
            if relevance <= 0:
                continue

            value = clean_text(" ".join(elem.itertext()))
            if not value:
                continue

            facts.append({
                "sourceFile": filename,
                "conceptName": concept,
                "conceptLocalName": local_name(concept),
                "contextRef": attrs.get("contextRef", ""),
                "relevanceScore": relevance,
                "factValue": value[:4000],
                "extractionMethod": "xml_element",
            })
    except ET.ParseError:
        pass

    # Regex fallback for inline XBRL. Some HTML/iXBRL files are not accepted by
    # ElementTree because of HTML quirks. Capture ix:nonNumeric @name and body.
    ix_pat = re.compile(
        r"<ix:(?:nonNumeric|nonFraction)\b([^>]*)>(.*?)</ix:(?:nonNumeric|nonFraction)>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    name_pat = re.compile(
        r"\bname\s*=\s*[\"']([^\"']+)[\"']",
        flags=re.IGNORECASE,
    )
    context_pat = re.compile(
        r"\bcontextRef\s*=\s*[\"']([^\"']+)[\"']",
        flags=re.IGNORECASE,
    )

    for m in ix_pat.finditer(text):
        attrs = m.group(1)
        body = m.group(2)
        nm = name_pat.search(attrs)
        if not nm:
            continue

        concept = nm.group(1)
        relevance = concept_relevance(concept)
        if relevance <= 0:
            continue

        value = clean_text(body)
        if not value:
            continue

        cm = context_pat.search(attrs)
        facts.append({
            "sourceFile": filename,
            "conceptName": concept,
            "conceptLocalName": local_name(concept),
            "contextRef": cm.group(1) if cm else "",
            "relevanceScore": relevance,
            "factValue": value[:4000],
            "extractionMethod": "ix_regex",
        })

    # Deduplicate parser/regex duplicates.
    unique: dict[tuple, dict] = {}
    for f in facts:
        k = (
            f["sourceFile"],
            f["conceptName"],
            f["contextRef"],
            f["factValue"],
        )
        prev = unique.get(k)
        if prev is None or f["relevanceScore"] > prev["relevanceScore"]:
            unique[k] = f

    return list(unique.values())


def structured_facts_from_zip(zip_path: Path) -> list[dict]:
    all_facts: list[dict] = []

    with zipfile.ZipFile(zip_path) as zf:
        for name in public_doc_members(zf):
            text = decode_bytes(zf.read(name))
            all_facts.extend(extract_xml_facts(text, name))

    return all_facts


def choose_best_fact(facts: list[dict]) -> dict | None:
    if not facts:
        return None

    enriched = []
    for f in facts:
        cls, labels = classify_value(f["factValue"])
        ff = dict(f)
        ff["factVenueClass"] = cls
        ff["factVenueLabels"] = labels

        # Prefer exchange/listing concepts, then facts that actually contain
        # recognizable venue text, then shorter/non-boilerplate values.
        recognized = int(cls != "UNRESOLVED")
        ff["_rank"] = (
            int(f["relevanceScore"]),
            recognized,
            -min(len(f["factValue"]), 4000),
        )
        enriched.append(ff)

    enriched.sort(key=lambda x: x["_rank"], reverse=True)
    return enriched[0]


def fallback_listing_context(zip_path: Path) -> tuple[str, str]:
    """
    Conservative fallback: find compact contexts around likely listing/exchange
    table labels inside PublicDoc. This is diagnostic only.
    """
    anchor_patterns = [
        r"上場金融商品取引所",
        r"上場している金融商品取引所",
        r"金融商品取引所名",
        r"金融商品取引所",
        r"Name of financial instruments exchange",
        r"financial instruments exchange on which",
    ]

    contexts: list[str] = []

    with zipfile.ZipFile(zip_path) as zf:
        for name in public_doc_members(zf):
            text = decode_bytes(zf.read(name))
            plain = clean_text(text)

            for pat in anchor_patterns:
                for m in re.finditer(pat, plain, flags=re.IGNORECASE):
                    start = max(0, m.start() - 250)
                    end = min(len(plain), m.end() + 700)
                    contexts.append(plain[start:end])
                    if len(contexts) >= 5:
                        break
                if len(contexts) >= 5:
                    break
            if len(contexts) >= 5:
                break

    joined = " | ".join(contexts)
    cls, labels = classify_value(joined)
    return cls, joined[:6000]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Targeted second-pass Stage 7 venue resolver using structured "
            "EDINET XBRL/iXBRL exchange/listing facts."
        )
    )
    parser.add_argument("--audit", default=DEFAULT_AUDIT)
    parser.add_argument("--edinet-root", required=True)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    root = repo_root()
    audit_path = resolve_path(root, args.audit)
    edinet_root = resolve_path(root, args.edinet_root)
    output_path = resolve_path(root, args.output)

    if not audit_path.exists():
        raise FileNotFoundError(audit_path)
    if not edinet_root.exists():
        raise FileNotFoundError(edinet_root)

    audit = pd.read_csv(
        audit_path,
        dtype={
            "edinetCode": "string",
            "curr_docID": "string",
            "secCode": "string",
        },
        low_memory=False,
    )
    audit["secCode"] = norm_code(audit["secCode"])

    review = audit.loc[
        audit["venueClass"].isin(TARGET_CLASSES)
    ].copy()

    print("=== Stage 7 targeted structured exchange-fact resolver ===")
    print(f"Input full-audit rows: {len(audit):,}")
    print(f"Target review rows:    {len(review):,}")
    print()
    print(review["venueClass"].value_counts().to_string())
    print()

    rows: list[dict] = []

    for i, row in enumerate(review.itertuples(index=False), start=1):
        edinet_code = str(row.edinetCode)
        doc_id = str(row.curr_docID)
        sec_code = str(row.secCode)

        zip_path = next(
            (
                p for p in candidate_zip_paths(
                    edinet_root,
                    edinet_code,
                    doc_id,
                )
                if p.exists()
            ),
            None,
        )

        base = {
            "edinetCode": edinet_code,
            "curr_docID": doc_id,
            "secCode": sec_code,
            "eventTradingDate": row.eventTradingDate,
            "firstPassVenueClass": row.venueClass,
            "firstPassVenueDetail": getattr(row, "venueDetail", ""),
        }

        if zip_path is None:
            rows.append({
                **base,
                "zipFound": False,
                "structuredFactFound": False,
                "conceptName": "",
                "contextRef": "",
                "factValue": "",
                "structuredVenueClass": "ZIP_NOT_FOUND",
                "structuredVenueLabels": "",
                "fallbackVenueClass": "",
                "fallbackContext": "",
                "finalResolution": "ZIP_NOT_FOUND",
            })
            continue

        try:
            facts = structured_facts_from_zip(zip_path)
            best = choose_best_fact(facts)

            fallback_cls = ""
            fallback_context = ""

            if best is None or best["factVenueClass"] == "UNRESOLVED":
                fallback_cls, fallback_context = fallback_listing_context(zip_path)

            if best is not None and best["factVenueClass"] != "UNRESOLVED":
                final_resolution = best["factVenueClass"]
            elif fallback_cls and fallback_cls != "UNRESOLVED":
                final_resolution = fallback_cls
            else:
                final_resolution = "UNRESOLVED"

            rows.append({
                **base,
                "zipFound": True,
                "structuredFactFound": best is not None,
                "conceptName": best["conceptName"] if best else "",
                "conceptLocalName": best["conceptLocalName"] if best else "",
                "contextRef": best["contextRef"] if best else "",
                "relevanceScore": best["relevanceScore"] if best else 0,
                "factValue": best["factValue"] if best else "",
                "structuredVenueClass": (
                    best["factVenueClass"] if best else "UNRESOLVED"
                ),
                "structuredVenueLabels": (
                    best["factVenueLabels"] if best else ""
                ),
                "fallbackVenueClass": fallback_cls,
                "fallbackContext": fallback_context,
                "finalResolution": final_resolution,
            })

        except Exception as exc:
            rows.append({
                **base,
                "zipFound": True,
                "structuredFactFound": False,
                "conceptName": "",
                "contextRef": "",
                "factValue": "",
                "structuredVenueClass": "READ_ERROR",
                "structuredVenueLabels": "",
                "fallbackVenueClass": "",
                "fallbackContext": str(exc),
                "finalResolution": "READ_ERROR",
            })

        if i % 20 == 0:
            print(f"Processed {i:,}/{len(review):,}")

    out = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False, encoding="utf-8")

    print()
    print("Final resolution counts:")
    print(out["finalResolution"].value_counts(dropna=False).to_string())
    print()

    print("By first-pass class -> final resolution:")
    print(
        pd.crosstab(
            out["firstPassVenueClass"],
            out["finalResolution"],
            dropna=False,
        ).to_string()
    )
    print()

    unresolved = out.loc[
        out["finalResolution"].isin({"UNRESOLVED", "READ_ERROR", "ZIP_NOT_FOUND"})
    ]
    print(
        f"Still unresolved: {len(unresolved):,} events / "
        f"{unresolved['secCode'].nunique():,} securities"
    )

    if not unresolved.empty:
        print()
        print(
            unresolved[
                [
                    "secCode",
                    "edinetCode",
                    "curr_docID",
                    "eventTradingDate",
                    "firstPassVenueClass",
                    "conceptName",
                    "factValue",
                    "fallbackVenueClass",
                ]
            ].to_string(index=False)
        )

    print()
    print(f"Output: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
