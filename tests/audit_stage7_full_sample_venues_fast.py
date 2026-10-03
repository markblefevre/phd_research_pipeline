#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd


DEFAULT_EVENTS = (
    "data/interim/paper2/market_reaction/stage7_event_dates.csv"
)
DEFAULT_OUTPUT = (
    "data/interim/paper2/market_reaction/diagnostics/"
    "stage7_full_sample_venue_audit.csv"
)
DEFAULT_SUSPECT_OUTPUT = (
    "data/interim/paper2/market_reaction/diagnostics/"
    "stage7_full_sample_venue_suspects.csv"
)
DEFAULT_CHECKPOINT = (
    "data/interim/paper2/market_reaction/diagnostics/"
    "stage7_full_sample_venue_checkpoint.csv"
)
DEFAULT_SUMMARY = (
    "data/interim/paper2/market_reaction/diagnostics/"
    "stage7_full_sample_venue_summary.json"
)

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

SUSPECT_CLASSES = {
    "TOKYO_PRO",
    "TOKYO_PRO_WITH_REGIONAL_MENTIONS",
    "REGIONAL_ONLY",
    "MULTIPLE_REGIONAL",
}
REVIEW_CLASSES = SUSPECT_CLASSES | {
    "UNRESOLVED",
    "TSE_UNSPECIFIED",
    "ZIP_NOT_FOUND",
    "READ_ERROR",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_path(root: Path, value: str) -> Path:
    p = Path(value).expanduser()
    return p if p.is_absolute() else root / p


def norm_code(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip()
    return out.str.replace(r"\.0$", "", regex=True)


def read_events(path: Path) -> pd.DataFrame:
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
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{path} missing required columns: {sorted(missing)}"
        )

    df["edinetCode"] = df["edinetCode"].astype("string").str.strip()
    df["curr_docID"] = df["curr_docID"].astype("string").str.strip()
    df["secCode"] = norm_code(df["secCode"])
    df["eventTradingDate"] = pd.to_datetime(
        df["eventTradingDate"], errors="raise"
    ).dt.normalize()

    key = ["edinetCode", "curr_docID", "secCode"]
    dupes = int(df.duplicated(key).sum())
    if dupes:
        raise ValueError(f"Stage 7B contains {dupes:,} duplicate event keys")

    return df


def candidate_zip_paths(
    edinet_root: str,
    edinet_code: str,
    doc_id: str,
) -> list[Path]:
    root = Path(edinet_root)
    return [
        root / edinet_code / doc_id / f"{doc_id}.zip",
        root / edinet_code / doc_id / f"{doc_id}_csv.zip",
    ]


def decode_bytes(data: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            pass
    return data.decode("utf-8", errors="ignore")


def preferred_members(names: list[str]) -> list[str]:
    """
    Prefer the primary filing content under XBRL/PublicDoc.

    Avoid scanning attachments, audit reports, images, and ancillary files
    unless there is no usable PublicDoc textual filing content.
    """
    text_suffixes = (".xbrl", ".xml", ".htm", ".html", ".xhtml")

    public_doc = [
        n for n in names
        if "XBRL/PublicDoc/" in n.replace("\\", "/")
        and n.lower().endswith(text_suffixes)
    ]
    if public_doc:
        return public_doc

    return [
        n for n in names
        if n.lower().endswith(text_suffixes)
    ]


def find_hits_in_text(text: str) -> dict[str, list[str]]:
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

                if len(venue_hits) >= 2:
                    break
            if len(venue_hits) >= 2:
                break

        if venue_hits:
            hits[venue] = venue_hits

    return hits


def merge_hits(
    target: dict[str, list[str]],
    source: dict[str, list[str]],
) -> None:
    for label, snippets in source.items():
        existing = target.setdefault(label, [])
        for snippet in snippets:
            if snippet not in existing:
                existing.append(snippet)
            if len(existing) >= 2:
                break


def enough_for_early_stop(hits: dict[str, list[str]]) -> bool:
    """
    We can stop once the venue classification cannot change materially.

    Ordinary-TSE-only can stop once an ordinary segment is found, unless
    TOKYO PRO or regional evidence has already appeared.
    Suspect combinations can also stop once both relevant classes exist.
    """
    found = set(hits)
    has_ordinary = bool(found & ORDINARY_TSE)
    has_tokyo_pro = "TOKYO_PRO" in found
    has_regional = bool(found & REGIONAL)

    if has_ordinary and not has_tokyo_pro and not has_regional:
        return True
    if has_tokyo_pro and has_regional and not has_ordinary:
        return True
    if has_ordinary and (has_tokyo_pro or has_regional):
        return True

    return False


def classify_hits(
    generic_tse_found: bool,
    hits: dict[str, list[str]],
) -> tuple[str, str]:
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
            return ("REGIONAL_ONLY", next(iter(regional_found)))
        return (
            "MULTIPLE_REGIONAL",
            ",".join(sorted(regional_found)),
        )

    if generic_tse_found:
        return ("TSE_UNSPECIFIED", "")

    return ("UNRESOLVED", "")


def collect_evidence(
    hits: dict[str, list[str]],
    limit: int = 3,
) -> list[str]:
    evidence: list[str] = []
    for label in sorted(hits):
        for snippet in hits[label]:
            evidence.append(f"[{label}] {snippet}")
            if len(evidence) >= limit:
                return evidence
    return evidence


def scan_one_event(task: tuple[str, str, str, str, str]) -> dict:
    edinet_code, doc_id, sec_code, event_date, edinet_root = task

    base = {
        "edinetCode": edinet_code,
        "curr_docID": doc_id,
        "secCode": sec_code,
        "eventTradingDate": event_date,
    }

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
        return {
            **base,
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

    try:
        all_hits: dict[str, list[str]] = {}
        generic_tse_found = False
        files_scanned = 0

        with zipfile.ZipFile(zip_path) as zf:
            members = preferred_members(zf.namelist())

            for name in members:
                try:
                    data = zf.read(name)
                except KeyError:
                    continue

                text = decode_bytes(data)
                files_scanned += 1

                if not generic_tse_found:
                    generic_tse_found = bool(
                        re.search(
                            r"東京証券取引所|Tokyo Stock Exchange",
                            text,
                            flags=re.IGNORECASE,
                        )
                    )

                hits = find_hits_in_text(text)
                merge_hits(all_hits, hits)

                if enough_for_early_stop(all_hits):
                    break

        venue_class, venue_detail = classify_hits(
            generic_tse_found,
            all_hits,
        )
        evidence = collect_evidence(all_hits)

        return {
            **base,
            "zipFound": True,
            "zipPath": str(zip_path),
            "filesScanned": files_scanned,
            "venueClass": venue_class,
            "venueDetail": venue_detail,
            "matchedVenueLabels": ",".join(sorted(all_hits)),
            "evidenceSnippet1": evidence[0] if len(evidence) > 0 else "",
            "evidenceSnippet2": evidence[1] if len(evidence) > 1 else "",
            "evidenceSnippet3": evidence[2] if len(evidence) > 2 else "",
        }

    except Exception as exc:
        return {
            **base,
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


def save_checkpoint(rows: list[dict], path: Path) -> None:
    if not rows:
        return

    out = pd.DataFrame(rows)
    out = (
        out.drop_duplicates(
            ["edinetCode", "curr_docID", "secCode"],
            keep="last",
        )
        .sort_values(
            ["edinetCode", "curr_docID", "secCode"],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False, encoding="utf-8")


def load_checkpoint(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(
        path,
        dtype={
            "edinetCode": "string",
            "curr_docID": "string",
            "secCode": "string",
        },
        low_memory=False,
    )
    df["edinetCode"] = df["edinetCode"].astype("string").str.strip()
    df["curr_docID"] = df["curr_docID"].astype("string").str.strip()
    df["secCode"] = norm_code(df["secCode"])
    return df


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fast parallel full-sample historical venue audit for Stage 7B."
        )
    )
    parser.add_argument("--events", default=DEFAULT_EVENTS)
    parser.add_argument("--edinet-root", required=True)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--suspect-output", default=DEFAULT_SUSPECT_OUTPUT)
    parser.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT)
    parser.add_argument("--summary", default=DEFAULT_SUMMARY)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--checkpoint-every", type=int, default=500)
    parser.add_argument("--progress-every", type=int, default=250)
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore/delete any existing checkpoint and start over.",
    )
    args = parser.parse_args()

    if args.workers < 1:
        raise ValueError("--workers must be >= 1")

    root = repo_root()
    events_path = resolve_path(root, args.events)
    edinet_root = resolve_path(root, args.edinet_root)
    output_path = resolve_path(root, args.output)
    suspect_output_path = resolve_path(root, args.suspect_output)
    checkpoint_path = resolve_path(root, args.checkpoint)
    summary_path = resolve_path(root, args.summary)

    if not events_path.exists():
        raise FileNotFoundError(events_path)
    if not edinet_root.exists():
        raise FileNotFoundError(edinet_root)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    suspect_output_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    if args.fresh and checkpoint_path.exists():
        checkpoint_path.unlink()

    events = read_events(events_path)
    checkpoint = load_checkpoint(checkpoint_path)

    key_cols = ["edinetCode", "curr_docID", "secCode"]

    completed_keys: set[tuple[str, str, str]] = set()
    rows: list[dict] = []

    if not checkpoint.empty:
        completed_keys = {
            (str(r.edinetCode), str(r.curr_docID), str(r.secCode))
            for r in checkpoint[key_cols].itertuples(index=False)
        }
        rows.extend(checkpoint.to_dict("records"))

    pending = events.loc[
        ~events.apply(
            lambda r: (
                str(r["edinetCode"]),
                str(r["curr_docID"]),
                str(r["secCode"]),
            ) in completed_keys,
            axis=1,
        )
    ].copy()

    tasks = [
        (
            str(r.edinetCode),
            str(r.curr_docID),
            str(r.secCode),
            str(pd.Timestamp(r.eventTradingDate).date()),
            str(edinet_root),
        )
        for r in pending.itertuples(index=False)
    ]

    print("=== Fast Stage 7 full-sample historical venue audit ===")
    print(f"Total events:       {len(events):,}")
    print(f"Checkpointed:       {len(completed_keys):,}")
    print(f"Pending:            {len(tasks):,}")
    print(f"Workers:            {args.workers}")
    print(f"EDINET root:        {edinet_root}")
    print()

    processed_this_run = 0

    if tasks:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            future_to_task = {
                pool.submit(scan_one_event, task): task
                for task in tasks
            }

            try:
                for future in as_completed(future_to_task):
                    row = future.result()
                    rows.append(row)
                    processed_this_run += 1

                    if (
                        args.progress_every > 0
                        and processed_this_run % args.progress_every == 0
                    ):
                        done_total = len(completed_keys) + processed_this_run
                        print(
                            f"Processed {done_total:,}/{len(events):,} events"
                        )

                    if (
                        args.checkpoint_every > 0
                        and processed_this_run % args.checkpoint_every == 0
                    ):
                        save_checkpoint(rows, checkpoint_path)

            except KeyboardInterrupt:
                print()
                print("Interrupted. Writing checkpoint before exit...")
                save_checkpoint(rows, checkpoint_path)
                raise

    save_checkpoint(rows, checkpoint_path)

    out = pd.DataFrame(rows)
    out = (
        out.drop_duplicates(
            key_cols,
            keep="last",
        )
        .merge(
            events[key_cols + ["eventTradingDate"]],
            on=key_cols,
            how="right",
            suffixes=("", "_canonical"),
            validate="one_to_one",
        )
    )

    if "eventTradingDate_canonical" in out.columns:
        out["eventTradingDate"] = out["eventTradingDate_canonical"]
        out = out.drop(columns="eventTradingDate_canonical")

    out = out.sort_values(
        ["edinetCode", "curr_docID", "secCode"],
        kind="mergesort",
    ).reset_index(drop=True)

    out.to_csv(output_path, index=False, encoding="utf-8")

    review = out.loc[out["venueClass"].isin(REVIEW_CLASSES)].copy()
    review.to_csv(
        suspect_output_path,
        index=False,
        encoding="utf-8",
    )

    venue_counts = {
        str(k): int(v)
        for k, v in out["venueClass"]
        .value_counts(dropna=False)
        .to_dict()
        .items()
    }

    suspect = out.loc[out["venueClass"].isin(SUSPECT_CLASSES)].copy()
    unresolved = out.loc[
        out["venueClass"].isin(
            {
                "UNRESOLVED",
                "TSE_UNSPECIFIED",
                "ZIP_NOT_FOUND",
                "READ_ERROR",
            }
        )
    ].copy()

    summary = {
        "events": int(len(out)),
        "uniqueSecurities": int(out["secCode"].nunique()),
        "venueClassCounts": venue_counts,
        "clearlyOutsideOrdinaryTSEEvents": int(len(suspect)),
        "clearlyOutsideOrdinaryTSESecurities": int(
            suspect["secCode"].nunique()
        ),
        "needsReviewEvents": int(len(unresolved)),
        "needsReviewSecurities": int(unresolved["secCode"].nunique()),
        "workers": int(args.workers),
        "edinetRoot": str(edinet_root),
    }

    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print()
    print("Venue-class counts:")
    print(out["venueClass"].value_counts(dropna=False).to_string())
    print()
    print(
        "Clearly outside ordinary-TSE universe by text classifier: "
        f"{len(suspect):,} events / "
        f"{suspect['secCode'].nunique():,} securities"
    )

    if not suspect.empty:
        print()
        print(
            suspect[
                [
                    "secCode",
                    "edinetCode",
                    "curr_docID",
                    "eventTradingDate",
                    "venueClass",
                    "venueDetail",
                ]
            ]
            .sort_values(["secCode", "eventTradingDate"])
            .to_string(index=False)
        )

    print()
    print(
        f"Needs review/unresolved: {len(unresolved):,} events / "
        f"{unresolved['secCode'].nunique():,} securities"
    )
    print()
    print(f"Full audit:    {output_path}")
    print(f"Review subset: {suspect_output_path}")
    print(f"Checkpoint:    {checkpoint_path}")
    print(f"Summary:       {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
