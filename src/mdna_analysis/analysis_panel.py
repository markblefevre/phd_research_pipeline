from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PAIR_KEY = ["edinetCode", "prev_docID", "curr_docID"]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")
    return pd.read_csv(
        path,
        dtype={
            "edinetCode": "string",
            "prev_docID": "string",
            "curr_docID": "string",
            "secCode": "string",
        },
        low_memory=False,
    )


def _validate_unique(df: pd.DataFrame, name: str) -> None:
    missing = set(PAIR_KEY) - set(df.columns)
    if missing:
        raise ValueError(f"{name} missing pair-key columns: {sorted(missing)}")
    dup = int(df.duplicated(PAIR_KEY).sum())
    if dup:
        raise ValueError(f"{name} contains {dup:,} duplicate pair row(s)")


def _load_novelty(novelty_root: Path, variant: str, output_name: str) -> pd.DataFrame:
    path = novelty_root / variant / "pair_novelty.csv"
    df = _read_csv(path)
    _validate_unique(df, f"novelty[{variant}]")
    required = {"variant", "cosineSimilarity", "novelty"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {sorted(missing)}")
    values = set(df["variant"].dropna().astype(str))
    if values != {variant}:
        raise ValueError(f"Variant mismatch in {path}: expected {variant!r}, found {sorted(values)}")
    return df[PAIR_KEY + ["cosineSimilarity", "novelty"]].rename(
        columns={
            "cosineSimilarity": f"{output_name}CosineSimilarity",
            "novelty": output_name,
        }
    )


def build_analysis_panel(
    *,
    pairs_csv: str | Path,
    novelty_root: str | Path,
    output_csv: str | Path,
    baseline_variant: str = "sudachi_c_num",
    raw_variant: str = "sudachi_c_raw",
    diagnostics_csv: str | Path | None = None,
    metadata_json: str | Path | None = None,
) -> pd.DataFrame:
    """Build the frozen Stage 6A pair-level analysis-panel foundation."""
    pairs_csv = Path(pairs_csv).expanduser().resolve()
    novelty_root = Path(novelty_root).expanduser().resolve()
    output_csv = Path(output_csv).expanduser().resolve()
    diagnostics_csv = (
        Path(diagnostics_csv).expanduser().resolve()
        if diagnostics_csv is not None
        else novelty_root / "pair_diagnostics.csv"
    )
    metadata_json = (
        Path(metadata_json).expanduser().resolve()
        if metadata_json is not None
        else output_csv.with_suffix(".metadata.json")
    )

    pairs = _read_csv(pairs_csv)
    _validate_unique(pairs, "research_eligible_pairs")

    if "is_research_eligible" in pairs.columns:
        eligible = pairs["is_research_eligible"].astype(str).str.lower().isin(["true", "1"])
        if not eligible.all():
            raise ValueError(
                f"research_eligible_pairs contains {(~eligible).sum():,} row(s) not marked eligible"
            )

    baseline = _load_novelty(novelty_root, baseline_variant, "noveltyCNum")
    raw = _load_novelty(novelty_root, raw_variant, "noveltyCRaw")

    diagnostics = _read_csv(diagnostics_csv)
    _validate_unique(diagnostics, "pair_diagnostics")
    diag_cols = [
        "prevMdnaLength",
        "currMdnaLength",
        "lengthRatio",
        "logLengthChange",
        "absLogLengthChange",
    ]
    missing_diag = set(diag_cols) - set(diagnostics.columns)
    if missing_diag:
        raise ValueError(f"{diagnostics_csv} missing columns: {sorted(missing_diag)}")
    diagnostics = diagnostics[PAIR_KEY + diag_cols]

    expected_rows = len(pairs)
    panel = pairs.merge(baseline, on=PAIR_KEY, how="left", validate="one_to_one")
    panel = panel.merge(raw, on=PAIR_KEY, how="left", validate="one_to_one")
    panel = panel.merge(diagnostics, on=PAIR_KEY, how="left", validate="one_to_one")

    if len(panel) != expected_rows:
        raise ValueError(f"Row-count changed during joins: expected {expected_rows:,}, got {len(panel):,}")

    added_cols = [
        "noveltyCNumCosineSimilarity",
        "noveltyCNum",
        "noveltyCRawCosineSimilarity",
        "noveltyCRaw",
        *diag_cols,
    ]
    missing_counts = {col: int(panel[col].isna().sum()) for col in added_cols}
    missing_nonzero = {k: v for k, v in missing_counts.items() if v}
    if missing_nonzero:
        raise ValueError(f"Stage 6A joins produced missing values: {missing_nonzero}")

    if "curr_periodEnd" in panel.columns:
        curr_period_end = pd.to_datetime(panel["curr_periodEnd"], errors="coerce")
        if curr_period_end.isna().any():
            raise ValueError(
                f"Invalid curr_periodEnd values: {int(curr_period_end.isna().sum()):,}"
            )
        panel["fiscalYear"] = curr_period_end.dt.year.astype("int64")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(output_csv, index=False, encoding="utf-8")

    metadata = {
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "stage": "analysis_panel",
        "rowCount": int(len(panel)),
        "columnCount": int(len(panel.columns)),
        "pairKey": PAIR_KEY,
        "baselineNoveltyVariant": baseline_variant,
        "rawNoveltyVariant": raw_variant,
        "inputs": {
            "pairsCsv": str(pairs_csv),
            "noveltyRoot": str(novelty_root),
            "diagnosticsCsv": str(diagnostics_csv),
        },
        "outputCsv": str(output_csv),
        "joinMissingCounts": missing_counts,
        "noveltySummary": {
            "baselineMean": float(panel["noveltyCNum"].mean()),
            "baselineMedian": float(panel["noveltyCNum"].median()),
            "rawMean": float(panel["noveltyCRaw"].mean()),
            "rawMedian": float(panel["noveltyCRaw"].median()),
        },
    }
    metadata_json.parent.mkdir(parents=True, exist_ok=True)
    metadata_json.write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    return panel
