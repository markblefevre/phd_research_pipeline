from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DOC_KEY = ["edinetCode", "docID"]
VALID_MANIFEST_STATUSES = {"success", "skipped_existing"}


def load_lmmd_sets(dict_csv: str | Path, *, token_col: str = "GPT_JA") -> tuple[set[str], set[str]]:
    """Load the Paper 1 translated LMMD positive/negative token sets unchanged."""
    path = Path(dict_csv).expanduser().resolve()
    lmmd = pd.read_csv(path, low_memory=False)
    required = {token_col, "Positive", "Negative"}
    missing = required - set(lmmd.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {sorted(missing)}")

    translated = lmmd[token_col].notna() & lmmd[token_col].astype(str).str.strip().ne("")
    pos_set = set(lmmd.loc[translated & (lmmd["Positive"] > 0), token_col].astype(str).str.strip())
    neg_set = set(lmmd.loc[translated & (lmmd["Negative"] > 0), token_col].astype(str).str.strip())
    return pos_set, neg_set


def _score_token_file(path: Path, pos_set: set[str], neg_set: set[str]) -> tuple[int, int, int]:
    token_count = positive_count = negative_count = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            token = line.rstrip("\r\n")
            if not token:
                continue
            token_count += 1
            positive_count += token in pos_set
            negative_count += token in neg_set
    return token_count, positive_count, negative_count


def score_lmmd_corpus(
    *,
    manifest_csv: str | Path,
    token_root: str | Path,
    lmmd_dict_csv: str | Path,
    output_csv: str | Path,
    metadata_json: str | Path | None = None,
    token_col: str = "GPT_JA",
    variant: str = "sudachi_c_raw",
) -> pd.DataFrame:
    """Score the frozen Stage 4 token corpus with the Paper 1 LMMD methodology."""
    manifest_csv = Path(manifest_csv).expanduser().resolve()
    token_root = Path(token_root).expanduser().resolve()
    lmmd_dict_csv = Path(lmmd_dict_csv).expanduser().resolve()
    output_csv = Path(output_csv).expanduser().resolve()
    metadata_json = (
        Path(metadata_json).expanduser().resolve()
        if metadata_json is not None
        else output_csv.with_suffix(".metadata.json")
    )

    manifest = pd.read_csv(
        manifest_csv,
        dtype={"edinetCode": "string", "docID": "string", "status": "string", "variant": "string"},
        low_memory=False,
    )
    required = {*DOC_KEY, "status", "variant", "tokenCount"}
    missing = required - set(manifest.columns)
    if missing:
        raise ValueError(f"{manifest_csv} missing columns: {sorted(missing)}")

    manifest = manifest.loc[manifest["variant"].astype(str).eq(variant)].copy()
    manifest = manifest.loc[manifest["status"].astype(str).str.lower().isin(VALID_MANIFEST_STATUSES)].copy()
    if manifest.empty:
        raise ValueError(f"No valid {variant} rows found in {manifest_csv}")
    if manifest.duplicated(DOC_KEY).any():
        raise ValueError(f"Manifest contains {int(manifest.duplicated(DOC_KEY).sum()):,} duplicate document key(s)")
    if manifest[DOC_KEY].isna().any().any():
        raise ValueError("Manifest contains missing edinetCode/docID values")

    pos_set, neg_set = load_lmmd_sets(lmmd_dict_csv, token_col=token_col)
    rows: list[dict[str, object]] = []
    missing_files: list[str] = []

    for r in manifest.itertuples(index=False):
        edinet_code = str(r.edinetCode)
        doc_id = str(r.docID)
        token_path = token_root / edinet_code / f"{doc_id}.tokens.txt"
        if not token_path.exists():
            missing_files.append(str(token_path))
            continue

        token_count, pos_count, neg_count = _score_token_file(token_path, pos_set, neg_set)
        if token_count <= 0:
            raise ValueError(f"Zero-token document: {token_path}")
        expected_count = int(r.tokenCount)
        if token_count != expected_count:
            raise ValueError(
                f"Token-count mismatch for {edinet_code}/{doc_id}: "
                f"manifest={expected_count:,}, observed={token_count:,}"
            )

        pos_rate = pos_count / token_count
        neg_rate = neg_count / token_count
        rows.append({
            "edinetCode": edinet_code,
            "docID": doc_id,
            "tokenCount": token_count,
            "lmmdPositiveCount": pos_count,
            "lmmdNegativeCount": neg_count,
            "lmmdPositiveRate": pos_rate,
            "lmmdNegativeRate": neg_rate,
            "lmmdNet": pos_rate - neg_rate,
        })

    if missing_files:
        sample = missing_files[:10]
        raise FileNotFoundError(
            f"Missing {len(missing_files):,} token file(s) under {token_root}; sample={sample}"
        )

    out = pd.DataFrame(rows)
    if len(out) != len(manifest):
        raise ValueError(f"Output row count {len(out):,} != manifest row count {len(manifest):,}")
    if out.duplicated(DOC_KEY).any():
        raise ValueError("LMMD output contains duplicate document keys")
    if not ((out["lmmdPositiveCount"] <= out["tokenCount"]) & (out["lmmdNegativeCount"] <= out["tokenCount"])).all():
        raise ValueError("LMMD count exceeds token count")

    manifest_token_total = int(pd.to_numeric(manifest["tokenCount"], errors="raise").sum())
    observed_token_total = int(out["tokenCount"].sum())
    if observed_token_total != manifest_token_total:
        raise ValueError(
            f"Corpus token total mismatch: manifest={manifest_token_total:,}, observed={observed_token_total:,}"
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False, encoding="utf-8")

    metadata = {
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "stage": "lmmd_sentiment",
        "variant": variant,
        "tokenColumn": token_col,
        "documentCount": int(len(out)),
        "totalTokens": observed_token_total,
        "positiveDictionaryTerms": int(len(pos_set)),
        "negativeDictionaryTerms": int(len(neg_set)),
        "positiveNegativeOverlapCount": int(len(pos_set & neg_set)),
        "inputs": {
            "manifestCsv": str(manifest_csv),
            "tokenRoot": str(token_root),
            "lmmdDictionaryCsv": str(lmmd_dict_csv),
        },
        "outputCsv": str(output_csv),
        "summary": {
            "positiveHits": int(out["lmmdPositiveCount"].sum()),
            "negativeHits": int(out["lmmdNegativeCount"].sum()),
            "medianSentimentHitsPerDocument": float((out["lmmdPositiveCount"] + out["lmmdNegativeCount"]).median()),
            "meanSentimentHitsPerDocument": float((out["lmmdPositiveCount"] + out["lmmdNegativeCount"]).mean()),
            "meanPositiveRate": float(out["lmmdPositiveRate"].mean()),
            "medianPositiveRate": float(out["lmmdPositiveRate"].median()),
            "meanNegativeRate": float(out["lmmdNegativeRate"].mean()),
            "medianNegativeRate": float(out["lmmdNegativeRate"].median()),
            "lmmdNetDistribution": {
                "mean": float(out["lmmdNet"].mean()),
                "std": float(out["lmmdNet"].std()),
                "min": float(out["lmmdNet"].min()),
                "p01": float(out["lmmdNet"].quantile(0.01)),
                "p05": float(out["lmmdNet"].quantile(0.05)),
                "p10": float(out["lmmdNet"].quantile(0.10)),
                "p25": float(out["lmmdNet"].quantile(0.25)),
                "median": float(out["lmmdNet"].median()),
                "p75": float(out["lmmdNet"].quantile(0.75)),
                "p90": float(out["lmmdNet"].quantile(0.90)),
                "p95": float(out["lmmdNet"].quantile(0.95)),
                "p99": float(out["lmmdNet"].quantile(0.99)),
                "max": float(out["lmmdNet"].max()),
            },
        },
    }
    metadata_json.parent.mkdir(parents=True, exist_ok=True)
    metadata_json.write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return out
