#!/usr/bin/env python3
"""Reproducible descriptive tables for EDHEC Paper 2, Stage 9.

Inputs (read-only):
  data/interim/paper2/regressions/stage9/regression_dataset.csv
  data/interim/paper2/regressions/stage9/model_summary.csv
  data/interim/paper2/regressions/stage9/provenance.json

Outputs:
  data/interim/paper2/regressions/stage9/tables/table1_descriptives.{csv,tex}
  data/interim/paper2/regressions/stage9/tables/component_correlations.{csv,tex}
  data/interim/paper2/regressions/stage9/tables/sample_flow.{csv,tex}
  data/interim/paper2/regressions/stage9/tables/table_provenance.json

This is a reporting-only script. It does not fit regressions, alter the
Stage 9 estimation sample, or modify Stage 7 / 8 / 9 model outputs.

Run from the repository root:
  python scripts/paper2/build_stage9_descriptive_tables.py

Requires NumPy; otherwise uses only Python's standard library.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

VARS = [
    ("unchangedSentiment", "Unchanged sentiment"),
    ("revisedSentiment", "Revised sentiment"),
    ("newSentiment", "New sentiment"),
    ("car_pp_m1_1", "CAR [-1,+1], percentage points"),
    ("noveltyCNum", "Whole-document novelty"),
    ("logLengthChange", "Log MD&A length change"),
    ("ln_mktcap", "Log market capitalization"),
    ("book_to_market", "Book-to-market ratio"),
    ("roe_ttm", "Return on equity"),
    ("volatility_60d", "Pre-filing return volatility"),
    ("log_avg_trading_value_60d", "Log average trading value"),
]
CORR_KEYS = [
    "unchangedSentiment", "revisedSentiment", "newSentiment",
    "car_pp_m1_1", "noveltyCNum",
]
LABELS = dict(VARS)
REQ = set(LABELS) | {
    "edinetCode", "fiscalYear", "baseline_eligible", "eventEligible",
    "carStatus_m1_1", "curr_docID",
}
STAGES = [
    ("Eligible consecutive annual-report pairs", "all"),
    ("After market/listing eligibility", "market"),
    ("After event-specific market model estimation", "model"),
    ("Valid three-day CAR [-1,+1]", "car"),
    ("All three sentiment components present", "components"),
    ("Complete primary regression sample", "baseline"),
]


def yes(v: str) -> bool:
    return v.strip().lower() in {"true", "1", "yes"}


def valid_float(x: str) -> bool:
    try:
        return bool(np.isfinite(float(x)))
    except (TypeError, ValueError):
        return False


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fmt_num(x: float, places: int = 4) -> str:
    return f"{x:,.{places}f}"


def write_csv(path: Path, header, data):
    with path.open("w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(header)
        w.writerows(data)


def write_tex(path: Path, caption: str, label: str, colspec: str,
              headers, rows, notes: str):
    lines = [
        "\\begin{table}[htbp]",
        "\\centering",
        f"\\caption{{{caption}}}",
        f"\\label{{{label}}}",
        "\\small",
        f"\\begin{{tabular}}{{{colspec}}}",
        "\\hline\\hline",
        " & ".join(headers) + r" \\",
        "\\hline",
    ]
    lines.extend(" & ".join(map(lambda x: str(x).replace("&", r"\&"), row)) + r" \\" for row in rows)
    lines.extend([
        "\\hline\\hline", "\\end{tabular}",
        "\\par\\vspace{2pt}",
        "\\begin{minipage}{0.97\\linewidth}",
        "\\footnotesize \\textit{Notes:} " + notes,
        "\\end{minipage}", "\\end{table}", "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input-dir", type=Path,
                   default=Path("data/interim/paper2/regressions/stage9"))
    p.add_argument("--output-dir", type=Path, default=None)
    args = p.parse_args()
    indir = args.input_dir
    outdir = args.output_dir or (indir / "tables")
    source = indir / "regression_dataset.csv"
    summary_path = indir / "model_summary.csv"
    provenance_path = indir / "provenance.json"
    for path in (source, summary_path, provenance_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    with summary_path.open(newline="", encoding="utf-8") as f:
        matches = [r for r in csv.DictReader(f) if r["model"] == "A_baseline"]
    if len(matches) != 1:
        raise ValueError("Expected exactly one A_baseline record in model_summary.csv")
    expected = matches[0]
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))

    counts = {name: 0 for _, name in STAGES}
    status_counts: dict[str, int] = {}
    numeric = {name: [] for name in LABELS}
    firms: set[str] = set()
    years: set[str] = set()
    ids: set[str] = set()
    rows = 0
    with source.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing_columns = REQ.difference(reader.fieldnames or ())
        if missing_columns:
            raise ValueError(f"Missing expected columns: {sorted(missing_columns)}")
        for r in reader:
            rows += 1
            current_id = r["curr_docID"]
            if current_id in ids:
                raise ValueError(f"Repeated filing ID in Stage 9 dataset: {current_id}")
            ids.add(current_id)
            eligible = yes(r["eventEligible"])
            status = r["carStatus_m1_1"]
            status_counts[status] = status_counts.get(status, 0) + 1
            if eligible:
                counts["market"] += 1
            if status in {"ok", "missing_stock_return"}:
                counts["model"] += 1
            if status == "ok":
                counts["car"] += 1
                if all(valid_float(r[k]) for k in CORR_KEYS[:3]):
                    counts["components"] += 1
            if yes(r["baseline_eligible"]):
                if status != "ok":
                    raise ValueError(f"Baseline row without a valid CAR: {current_id}")
                bad = [k for k in LABELS if not valid_float(r[k])]
                if bad:
                    raise ValueError(f"Baseline row {current_id} has missing values: {bad}")
                counts["baseline"] += 1
                firms.add(r["edinetCode"])
                years.add(r["fiscalYear"])
                for name in LABELS:
                    numeric[name].append(float(r[name]))
    counts["all"] = rows
    # The ordered, mutually exclusive event-study statuses explain 33,046 -> 30,926.
    required_status = {"ok", "stage7a_ineligible", "market_model_not_estimated", "missing_stock_return"}
    if set(status_counts) != required_status:
        raise ValueError(f"Unexpected Stage 7 status vocabulary: {status_counts}")
    if counts["all"] != counts["market"] + status_counts["stage7a_ineligible"]:
        raise ValueError("Market eligibility does not reconcile")
    if counts["market"] != counts["model"] + status_counts["market_model_not_estimated"]:
        raise ValueError("Market model count does not reconcile")
    if counts["model"] != counts["car"] + status_counts["missing_stock_return"]:
        raise ValueError("CAR availability does not reconcile")
    if counts["baseline"] != int(expected["N"]) or len(firms) != int(expected["firms"]):
        raise ValueError("Primary sample disagrees with frozen model_summary.csv")
    if len(years) != int(expected["years"]):
        raise ValueError("Year coverage disagrees with frozen model_summary.csv")
    for local, prov in [("all", "panel"), ("market", "matched_controls"),
                        ("car", "eligible_car"), ("components", "all_components_present"),
                        ("baseline", "baseline_eligible")]:
        if counts[local] != int(provenance["counts"][prov]):
            raise ValueError(f"{local} count disagrees with provenance.json")

    outdir.mkdir(parents=True, exist_ok=True)

    # One common N is crucial: every statistic is from A_baseline, not the full corpus.
    summary_rows = []
    for key, label in VARS:
        x = np.asarray(numeric[key], dtype=float)
        q25, median, q75 = np.quantile(x, [0.25, 0.5, 0.75])
        summary_rows.append((label, len(x), fmt_num(x.mean()),
                             fmt_num(x.std(ddof=1)), fmt_num(q25),
                             fmt_num(median), fmt_num(q75)))
    write_csv(outdir / "table1_descriptives.csv",
              ["Variable", "N", "Mean", "SD", "P25", "Median", "P75"],
              summary_rows)
    write_tex(outdir / "table1_descriptives.tex",
              "Descriptive statistics for the primary regression sample",
              "tab:stage9_descriptives", "lrrrrrr",
              ["Variable", "$N$", "Mean", "SD", "P25", "Median", "P75"],
              [tuple(row[:1]) + (f"{row[1]:,}",) + tuple(row[2:]) for row in summary_rows],
              "All variables are measured on the identical Stage~9 model A sample "
              f"($N={counts['baseline']:,}$ filings, {len(firms):,} firms). "
              "CARs are in percentage points; component sentiment is in [-1,1].")

    matrix = np.column_stack([numeric[k] for k in CORR_KEYS])
    cc = np.corrcoef(matrix, rowvar=False)
    corr_rows = [(LABELS[k],) + tuple(f"{v:.3f}" for v in cc[i])
                 for i, k in enumerate(CORR_KEYS)]
    write_csv(outdir / "component_correlations.csv",
              ["Variable"] + [LABELS[k] for k in CORR_KEYS], corr_rows)
    write_tex(outdir / "component_correlations.tex",
              "Pearson correlations: disclosure sentiment and market variables",
              "tab:stage9_correlations", "lrrrrr",
              ["Variable"] + [str(i) for i in range(1, 6)], corr_rows,
              "The variables, in column order, are (1) unchanged sentiment, "
              "(2) revised sentiment, (3) new sentiment, "
              "(4) three-day CAR, and (5) whole-document novelty. "
              f"All correlations use the same {counts['baseline']:,} observations.")

    flow = []
    previous = None
    for label, key in STAGES:
        current = counts[key]
        lost = "" if previous is None else f"{previous - current:,}"
        flow.append((label, f"{current:,}", lost))
        previous = current
    write_csv(outdir / "sample_flow.csv", ["Stage", "Retained", "Excluded at step"], flow)
    write_tex(outdir / "sample_flow.tex", "Sample construction and attrition",
              "tab:stage9_sampleflow", "lrr",
              ["Stage", "Retained", "Excluded"], flow,
              "Market/listing ineligibility removes 920 filings; 428 further events "
              "lack a fitted market model with sufficient valid paired returns; "
              "772 have incomplete three-day stock returns. "
              "The remaining losses reflect absent sentiment components or incomplete controls.")

    metadata = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "source_dataset_sha256": sha256_file(source),
        "model_summary_sha256": sha256_file(summary_path),
        "stage9_provenance_sha256": sha256_file(provenance_path),
        "primary_model": "A_baseline",
        "sample_counts": counts,
        "stage7_car_status": status_counts,
        "primary_sample_firms": len(firms),
        "primary_sample_years": len(years),
        "correlation_keys": CORR_KEYS,
        "no_estimator_rerun": True,
    }
    (outdir / "table_provenance.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Validated: {counts['all']:,} filings -> {counts['car']:,} CARs -> "
          f"{counts['baseline']:,} baseline, {len(firms):,} firms")
    print("Component correlations:")
    for i, k in enumerate(CORR_KEYS[:3]):
        print(f"  {k}: {cc[i, :3].round(3)}")
    print(f"Generated 7 files under: {outdir}")


if __name__ == "__main__":
    main()
