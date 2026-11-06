#!/usr/bin/env python3
"""Build Stage 9 manuscript tables and secondary event-time regressions.

Consolidated reporting entry point for:
  * Sample flow, descriptive statistics, correlations
  * Primary Model A coefficients and direct contrasts (Table 3)
  * Event-day decomposition (Table 4)
  * Filing-time heterogeneity (Table 5)
  * Stage 9 robustness, binary taxonomy, and momentum sensitivity (Table 6 and Appendix)

From the Paper 2 repository root:
    python scripts/paper2/build_stage9_tables.py
    python scripts/paper2/build_stage9_tables.py --skip-event-day
    python scripts/paper2/build_stage9_tables.py --skip-filing-time
    python scripts/paper2/build_stage9_tables.py --only filing-time
    python scripts/paper2/build_stage9_tables.py --only robustness

All outputs are written to stage9/tables/. Source Stage 9 files are read-only;
no LLM calls, market-data downloads, or upstream pipeline stages are run.
The event-day and filing-time portions refit secondary FE regressions using
exactly the frozen Stage 9 Model A sample, controls, and clustering.
Requires numpy, pandas, scipy.
"""
from __future__ import annotations
import argparse
from pathlib import Path

def _run_descriptive_tables(arglist: list[str]) -> None:
    #!/usr/bin/env python3
    """Reproducible descriptive tables for EDHEC Paper 2, Stage 9.

    Inputs (read-only):
      data/interim/paper2/regressions/stage9/regression_dataset.csv
      data/interim/paper2/regressions/stage9/model_summary.csv
      data/interim/paper2/regressions/stage9/provenance.json

    Outputs:
      data/interim/paper2/regressions/stage9/tables/table1_descriptives.{csv,tex}
      data/interim/paper2/regressions/stage9/tables/tableA1_component_correlations.{csv,tex}
      data/interim/paper2/regressions/stage9/tables/sample_flow.{csv,tex}
      data/interim/paper2/regressions/stage9/tables/table_provenance.json

    This is a reporting-only script. It does not fit regressions, alter the
    Stage 9 estimation sample, or modify Stage 7 / 8 / 9 model outputs.

    Run from the repository root:
      python scripts/paper2/build_stage9_descriptive_tables.py

    Requires NumPy; otherwise uses only Python's standard library.
    """


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
        args = p.parse_args(arglist)
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
                  "All variables are measured on the primary regression sample "
                  f"($N={counts['baseline']:,}$ filings, {len(firms):,} firms). "
                  "CARs are in percentage points; component sentiment is in [-1,1].")

        matrix = np.column_stack([numeric[k] for k in CORR_KEYS])
        cc = np.corrcoef(matrix, rowvar=False)
        corr_rows = [(LABELS[k],) + tuple(f"{v:.3f}" for v in cc[i])
                     for i, k in enumerate(CORR_KEYS)]
        write_csv(outdir / "tableA1_component_correlations.csv",
                  ["Variable"] + [LABELS[k] for k in CORR_KEYS], corr_rows)
        write_tex(outdir / "tableA1_component_correlations.tex",
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
    main()

def _run_primary_regression_table(arglist: list[str]) -> None:
    #!/usr/bin/env python3
    """Publication table for Paper 2 primary Stage 9 regression (read-only inputs).

    Run from the repository root:
        python scripts/paper2/build_stage9_primary_regression_table.py

    Reads the existing Stage 9 coefficients, contrasts, model summary, and
    provenance. Writes only into stage9/tables. No regression is re-estimated.
    Requires Python standard library only.
    """


    import argparse
    import csv
    import hashlib
    import json
    from datetime import datetime, timezone
    from pathlib import Path

    MODEL = 'A_baseline'
    STAGE9_DEFAULT = Path('data/interim/paper2/regressions/stage9')
    MAIN = [
        ('unchangedSentiment', r'Unchanged sentiment'),
        ('revisedSentiment', r'Revised sentiment'),
        ('newSentiment', r'New sentiment'),
    ]
    CONTROLS = [
        ('noveltyCNum', r'Whole-document novelty'),
        ('logLengthChange', r'Log MD\&A length change'),
        ('ln_mktcap', r'Log market capitalization'),
        ('book_to_market', r'Book-to-market ratio'),
        ('roe_ttm', r'Return on equity'),
        ('volatility_60d', r'Pre-filing return volatility'),
        ('log_avg_trading_value_60d', r'Log average trading value'),
    ]
    CONTRASTS = [
        ('newSentiment - unchangedSentiment', r'New $-$ unchanged'),
        ('newSentiment - revisedSentiment', r'New $-$ revised'),
        ('revisedSentiment - unchangedSentiment', r'Revised $-$ unchanged'),
    ]


    def read_rows(path):
        with path.open(newline='', encoding='utf-8-sig') as f:
            return list(csv.DictReader(f))


    def sha256(path):
        h = hashlib.sha256()
        with path.open('rb') as f:
            for block in iter(lambda: f.read(1 << 20), b''):
                h.update(block)
        return h.hexdigest()


    def fmt(x):
        return f'{float(x):.4f}'


    def ci(row):
        return f"[{fmt(row['ci95_low'])}, {fmt(row['ci95_high'])}]"


    def main():
        ap = argparse.ArgumentParser(description=__doc__)
        ap.add_argument('--stage9-dir', type=Path, default=STAGE9_DEFAULT)
        ap.add_argument('--output-dir', type=Path, default=None)
        args = ap.parse_args(arglist)
        base = args.stage9_dir
        out = args.output_dir or base / 'tables'
        cf, ctrf, sf, pf = [base / name for name in (
            'coefficients.csv', 'coefficient_contrasts.csv',
            'model_summary.csv', 'provenance.json')]
        for p in (cf, ctrf, sf, pf):
            if not p.is_file():
                raise FileNotFoundError(f'Missing required Stage 9 input: {p}')

        coefficients = {r['term']:r for r in read_rows(cf) if r['model'] == MODEL}
        contrasts = {r['contrast']:r for r in read_rows(ctrf) if r['model'] == MODEL}
        summaries = [r for r in read_rows(sf) if r['model'] == MODEL]
        if len(summaries) != 1:
            raise ValueError('Expected exactly one primary model in model_summary.csv')
        sm = summaries[0]
        prov = json.loads(pf.read_text(encoding='utf-8'))
        if int(sm['N']) != int(prov['counts']['baseline_eligible']):
            raise ValueError('Stage 9 sample size disagrees with provenance')
        if int(sm['N']) <= 0 or int(sm['firms']) <= 1:
            raise ValueError('Implausible model sample/firm counts')
        if set(coefficients) != {x[0] for x in MAIN + CONTROLS}:
            raise ValueError('Unexpected set of primary model regressors; audit config before building table')
        if set(contrasts) != {x[0] for x in CONTRASTS}:
            raise ValueError('Unexpected set of primary contrasts')
        # The 3 pairwise contrasts should reproduce the differences of model coefficients.
        for key, _ in CONTRASTS:
            left, right = key.split(' - ')
            d = float(coefficients[left]['coef'])-float(coefficients[right]['coef'])
            if abs(d-float(contrasts[key]['difference'])) > 1e-8:
                raise ValueError(f'Contrast mismatch: {key}')

        out.mkdir(parents=True, exist_ok=True)
        tex = [
            r'\begin{table}[htbp]',
            r'\centering',
            r'\caption{Disclosure-component sentiment and three-day abnormal returns}',
            r'\label{tab:stage9_primary}',
            r'\small',
            r'\setlength{\tabcolsep}{4pt}',
            r'\begin{tabular}{lrrrl}',
            r'\hline\hline',
            r'Variable & Coefficient & Clustered SE & $p$-value & 95\% CI \\',
            r'\hline',
            r'\multicolumn{5}{l}{\textit{Panel A: Joint three-component regression}} \\',
        ]
        csvrows = []
        for key, label in MAIN + CONTROLS:
            r = coefficients[key]
            tex.append(f"{label} & {fmt(r['coef'])} & {fmt(r['se'])} & {fmt(r['p'])} & {ci(r)} " + r'\\')
            csvrows.append(['A: coefficients', key, r['coef'], r['se'], r['p'], r['ci95_low'], r['ci95_high']])
        tex.extend([
            r'\hline',
            r'\multicolumn{5}{l}{\textit{Panel B: Direct comparisons of component coefficients}} \\',
        ])
        for key, label in CONTRASTS:
            r = contrasts[key]
            tex.append(f"{label} & {fmt(r['difference'])} & {fmt(r['se'])} & {fmt(r['p'])} & {ci(r)} " + r'\\')
            csvrows.append(['B: contrasts', key, r['difference'], r['se'], r['p'], r['ci95_low'], r['ci95_high']])
        tex.extend([
            r'\hline',
            f"Observations & \\multicolumn{{4}}{{r}}{{{int(sm['N']):,}}} " + r'\\',
            f"Firms & \\multicolumn{{4}}{{r}}{{{int(sm['firms']):,}}} " + r'\\',
            f"Years & \\multicolumn{{4}}{{r}}{{{int(sm['years'])}}} " + r'\\',
            f"Within-$R^2$ & \\multicolumn{{4}}{{r}}{{{float(sm['within_r2']):.4f}}} " + r'\\',
            f"Joint Wald $F(2,{int(sm['joint_wald_df_den']):,})$ & \\multicolumn{{4}}{{r}}{{{float(sm['joint_wald_F']):.4f}}} " + r'\\',
            f"Joint equality $p$-value & \\multicolumn{{4}}{{r}}{{{fmt(sm['joint_wald_p'])}}} " + r'\\',
            r'Firm fixed effects & \multicolumn{4}{r}{Yes} \\',
            r'Year fixed effects & \multicolumn{4}{r}{Yes} \\',
            r'Firm-clustered standard errors & \multicolumn{4}{r}{Yes} \\',
            r'Historical momentum control & \multicolumn{4}{r}{No} \\',
            r'\hline\hline',
            r'\end{tabular}',
            r'\par\vspace{2pt}',
            r'\begin{minipage}{0.99\linewidth}',
            r'\footnotesize\textit{Notes:} Dependent variable: CAR $[-1,+1]$ in percentage points. ',
            r'Sentiment measures range from $-1$ to $+1$. Coefficients and confidence intervals ',
            r'are reported in CAR percentage points per unit change in each regressor. ',
            r'All measures are evaluated on the primary complete-case sample. ',
            r'The reported tests use the firm-clustered covariance matrix. ',
            r'The joint Wald test evaluates $\beta_U=\beta_R=\beta_N$. ',
            r'Historical momentum is excluded from the primary model and evaluated in sensitivity analyses.',
            r'\end{minipage}',
            r'\end{table}',
            '',
        ])
        tablepath = out / 'table3_primary_stage9.tex'
        tablepath.write_text('\n'.join(tex), encoding='utf-8')
        with (out / 'table3_primary_stage9.csv').open('w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(['panel','term','estimate','clustered_se','p','ci95_low','ci95_high'])
            w.writerows(csvrows)
        meta = {
            'generated_utc':datetime.now(timezone.utc).isoformat(),
            'model':MODEL,
            'N':int(sm['N']),
            'firms':int(sm['firms']),
            'model_sample_sha256':sm['sample_sha256'],
            'source_sha256':{p.name:sha256(p) for p in (cf,ctrf,sf,pf)},
            'estimator_rerun':False,
        }
        (out / 'table3_primary_stage9_provenance.json').write_text(json.dumps(meta, indent=2)+'\n', encoding='utf-8')
        print(f'Generated {tablepath} ({len(csvrows)} rows, N={int(sm["N"]):,})')
    main()

def _run_event_day_table(arglist: list[str]) -> None:
    #!/usr/bin/env python3
    """Stage 9 event-day sensitivity table, on exactly the frozen Model A sample.

    Run from the Paper 2 repository root:
        python scripts/paper2/build_stage9_event_day_table.py

    Inputs are the existing Stage 9 regression_dataset.csv, coefficients.csv and
    model_summary.csv. This script refits *only* event-day outcomes using the same
    three sentiments, seven controls, firm/year FE and firm-CR1 inference as Model A.
    It never modifies Stage 9 estimator outputs or any prior pipeline stage.

    Requirements: numpy, pandas, scipy.
    """

    import argparse
    import hashlib
    import json
    from datetime import datetime, timezone
    from pathlib import Path

    import numpy as np
    import pandas as pd
    from scipy.stats import t as student_t

    TERMS = [
        'unchangedSentiment', 'revisedSentiment', 'newSentiment',
        'noveltyCNum', 'logLengthChange', 'ln_mktcap', 'book_to_market',
        'roe_ttm', 'volatility_60d', 'log_avg_trading_value_60d',
    ]
    SENTIMENT = TERMS[:3]
    LABELS = {'unchangedSentiment':'Unchanged sentiment',
              'revisedSentiment':'Revised sentiment',
              'newSentiment':'New sentiment'}
    OUTCOMES = ['Day -1', 'Day 0', 'Day +1', 'CAR [-1,+1]', 'Day +1 minus day -1']


    def digest(path: Path) -> str:
        h=hashlib.sha256()
        with path.open('rb') as f:
            for c in iter(lambda:f.read(1024*1024),b''):
                h.update(c)
        return h.hexdigest()


    def fe_residualize(z: np.ndarray, firm_ix: np.ndarray, year_ix: np.ndarray,
                       groups: int, years: int, tol=1e-12, max_iterations=300):
        z=z.copy()
        firm_count=np.bincount(firm_ix,minlength=groups)
        year_count=np.bincount(year_ix,minlength=years)
        for iteration in range(1,max_iterations+1):
            prior=z.copy()
            for ids, counts, num in ((firm_ix,firm_count,groups),(year_ix,year_count,years)):
                for col in range(z.shape[1]):
                    z[:,col]-=np.bincount(ids,weights=z[:,col],minlength=num)[ids]/counts[ids]
            if np.max(np.abs(z-prior))<tol:
                return z,iteration
        raise RuntimeError('Firm/year FE absorption failed to converge')


    def four(x):
        return f'{x:+.4f}' if x>=0 else f'{x:.4f}'


    def latex_table(results: pd.DataFrame, n:int, firms:int, daytest:pd.Series) -> str:
        # The table uses no external LaTeX packages and fits a standard article column.
        rows=[]
        for term in SENTIMENT:
            subset=results[(results.term==term)].set_index('outcome')
            estimates=[]; errors=[]
            for outcome in OUTCOMES[:4]:
                rec=subset.loc[outcome]
                stars='^{**}' if rec.p<.05 else ('^{*}' if rec.p<.10 else '')
                estimates.append(f'${four(float(rec.coef))}{stars}$')
                errors.append(f'({float(rec.se):.4f})')
            rows.extend([
                LABELS[term]+' & '+' & '.join(estimates)+r' \\',
                ' & '+' & '.join(errors)+r' \\',
            ])
        return '\n'.join([
            r'\begin{table}[htbp]',
            r'\centering',
            r'\caption{Event-day decomposition of disclosure-component sentiment associations}',
            r'\label{tab:stage9_event_days}',
            r'\small',
            r'\setlength{\tabcolsep}{5pt}',
            r'\begin{tabular}{lrrrr}',
            r'\hline\hline',
            r' & Day $-1$ & Day $0$ & Day $+1$ & CAR $[-1,+1]$ \\',
            r'\hline',
            *rows,
            r'\hline',
            f'Observations & {n:,} & {n:,} & {n:,} & {n:,} '+r'\\',
            f'Firms & {firms:,} & {firms:,} & {firms:,} & {firms:,} '+r'\\',
            r'Firm and year fixed effects & Yes & Yes & Yes & Yes \\',
            r'Primary controls & Yes & Yes & Yes & Yes \\',
            r'\hline\hline',
            r'\end{tabular}',
            r'\par\vspace{2pt}',
            r'\begin{minipage}{0.99\linewidth}',
            (r'\footnotesize\textit{Notes:} Coefficients are abnormal-return percentage points per '
             r'sentiment unit. Firm-clustered CR1 standard errors appear in parentheses. '
             r'$^{**}p<0.05$, $^{*}p<0.10$ (unadjusted, two-sided). '
             r'Day 0 denotes the assigned event trading day; days $-1$ and $+1$ are adjacent '
             r'TSE sessions. All models use the identical primary estimation sample and regressors. '
             r'CAR $[-1,+1]$ is the sum of the three daily abnormal returns. '
             f'The direct new-sentiment day $+1$ minus day $-1$ slope difference is '
             f'${four(float(daytest.coef))}$ ($p={float(daytest.p):.3f}$). '
             r'Day-level tests are exploratory and not multiplicity-adjusted.'),
            r'\end{minipage}',
            r'\end{table}',
            ''
        ])


    def main():
        ap=argparse.ArgumentParser(description=__doc__)
        ap.add_argument('--stage9-dir',type=Path,default=Path('data/interim/paper2/regressions/stage9'))
        args=ap.parse_args(arglist)
        root=args.stage9_dir
        paths={Path(k).stem:root/k for k in ('regression_dataset.csv','model_summary.csv','coefficients.csv')}
        for p in paths.values():
            if not p.is_file():raise FileNotFoundError(f'Missing Stage 9 file: {p}')
        base=pd.read_csv(paths['model_summary'])
        model=base.loc[base['model'].eq('A_baseline')].iloc[0]
        primary=pd.read_csv(paths['coefficients'])
        primary=primary.loc[primary['model'].eq('A_baseline')].set_index('term')
        required=['baseline_eligible','edinetCode','fiscalYear','car_pp_m1_1','car_pp_0_0',
                  'car_pp_0_1','abnormalReturn_m1','abnormalReturn_0','abnormalReturn_1']+TERMS
        d=pd.read_csv(paths['regression_dataset'],usecols=required,low_memory=False)
        d=d.loc[d['baseline_eligible'].eq(True)].reset_index(drop=True)
        n=len(d)
        firms,firm_ix=np.unique(d.edinetCode.astype(str).to_numpy(),return_inverse=True)
        years,year_ix=np.unique(d.fiscalYear.to_numpy(),return_inverse=True)
        g=len(firms);ny=len(years);k=len(TERMS)
        if (n,g,ny)!=(int(model.N),int(model.firms),int(model.years)):
            raise ValueError('The current baseline sample does not match frozen Stage 9 Model A')
        rankfe=g+ny-1
        if int(model.fe_rank)!=rankfe:
            raise ValueError('Unexpected FE rank; verify component connectivity before estimating')
        x=d[TERMS].to_numpy(dtype=float)
        car=d['car_pp_m1_1'].to_numpy(dtype=float)
        car00=d['car_pp_0_0'].to_numpy(dtype=float)
        car01=d['car_pp_0_1'].to_numpy(dtype=float)
        # Exactly Stage 8F's day definitions, now on the final corrected Stage 9 data.
        ym1=car-car01
        y0=car00
        yp1=car01-car00
        y=np.column_stack([ym1,y0,yp1,car,yp1-ym1])
        if not np.isfinite(np.column_stack([x,y])).all():
            raise ValueError('Nonfinite values in baseline regressor or event-day outcome')
        for col,daily in [('abnormalReturn_m1',ym1),('abnormalReturn_0',y0),('abnormalReturn_1',yp1)]:
            if not np.allclose(d[col].to_numpy(dtype=float)*100,daily,rtol=1e-9,atol=1e-8):
                raise ValueError(f'Event-day identity fails: {col}')
        if not np.allclose(ym1+y0+yp1,car,rtol=0,atol=1e-9):
            raise ValueError('CAR decomposition identity fails')
        z,iterations=fe_residualize(np.column_stack([x,y]),firm_ix,year_ix,g,ny)
        xw=z[:,:k];yw=z[:,k:]
        invxx=np.linalg.inv(xw.T@xw)
        beta=invxx@(xw.T@yw)
        resid=yw-xw@beta
        fac=(g/(g-1))*((n-1)/(n-rankfe-k))
        if not np.isclose(fac,float(model.cr1_factor),rtol=1e-6):
            raise ValueError('CR1 correction differs from canonical Stage 9 model')
        rows=[]
        max_b=max_se=0.
        for j,outcome in enumerate(OUTCOMES):
            s=np.zeros((g,k))
            np.add.at(s,firm_ix,xw*resid[:,[j]])
            cov=fac*invxx@(s.T@s)@invxx
            se=np.sqrt(np.diag(cov))
            p=2*student_t.sf(np.abs(beta[:,j]/se),df=g-1)
            tcrit=student_t.ppf(0.975,df=g-1)
            for i,term in enumerate(TERMS):
                rows.append({'outcome':outcome,'term':term,'coef':float(beta[i,j]),
                             'se':float(se[i]),'p':float(p[i]),
                             'ci95_low':float(beta[i,j]-tcrit*se[i]),
                             'ci95_high':float(beta[i,j]+tcrit*se[i]),'N':n,'firms':g})
                if outcome=='CAR [-1,+1]':
                    max_b=max(max_b,abs(float(beta[i,j])-float(primary.loc[term,'coef'])))
                    max_se=max(max_se,abs(float(se[i])-float(primary.loc[term,'se'])))
        if max_b>1e-7 or max_se>1e-7:
            raise ValueError(f'Independent Model A replication FAILED: beta diff {max_b}, SE diff {max_se}')
        df=pd.DataFrame(rows)
        daytest=df.loc[df['outcome'].eq('Day +1 minus day -1') & df['term'].eq('newSentiment')].iloc[0]
        out=root/'tables'
        out.mkdir(parents=True,exist_ok=True)
        csv_path=out/'table4_event_day_decomposition.csv'
        tex_path=out/'table4_event_day_decomposition.tex'
        provenance_path=out/'table4_event_day_decomposition_provenance.json'
        df.to_csv(csv_path,index=False)
        tex_path.write_text(latex_table(df,n,g,daytest),encoding='utf-8')
        meta={'generated_utc':datetime.now(timezone.utc).isoformat(),
              'stage9_model':'A_baseline','N':n,'firms':g,'years':ny,
              'baseline_sample_sha256_from_model_summary':str(model.sample_sha256),
              'sources_sha256':{k:digest(v) for k,v in paths.items()},
              'method':'Independent FE-within OLS and firm-clustered CR1; day AR via CAR arithmetic',
              'regressors':TERMS,'outcomes':OUTCOMES,'absorb_iterations':iterations,
              'cr1_factor':fac,'max_primary_beta_difference':max_b,
              'max_primary_se_difference':max_se,'daily_sum_identity':True,
              'event_day_difference_new_sentiment':{'coef':float(daytest.coef),'p':float(daytest.p)},
              'new_llm_calls':False,'new_market_data_calculations':False,
              'new_sensitivity_regressions':True}
        provenance_path.write_text(json.dumps(meta,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
        print(f'Generated {tex_path} (N={n:,}, firms={g:,}); maximum Model A beta and SE differences: {max_b:.2e}, {max_se:.2e}')
        for outcome in OUTCOMES:
            r=df[(df.outcome==outcome)&(df.term=='newSentiment')].iloc[0]
            print(f'  {outcome:25s} beta={r.coef:+.6f}, SE={r.se:.6f}, p={r.p:.6f}')
    main()

def _run_filing_time_table(arglist: list[str]) -> None:
    #!/usr/bin/env python3
    """Stage 9 filing-time heterogeneity: same-sample firm/year FE interaction analysis.

    Run from repository root:
        python scripts/paper2/build_stage9_filing_time.py --stage9-dir data/interim/paper2/regressions/stage9

    Uses frozen Stage 9 Model A sample and inputs, estimates only OLS robustness
    regressions; no LLM calls or market data rebuild. Outputs go under tables/.
    Requires numpy, pandas and scipy.
    """
    import argparse, hashlib, json
    from pathlib import Path
    from datetime import datetime, timezone
    import numpy as np
    import pandas as pd
    from scipy.stats import t as tdist, f as fdist

    TERMS=['unchangedSentiment','revisedSentiment','newSentiment','noveltyCNum','logLengthChange',
           'ln_mktcap','book_to_market','roe_ttm','volatility_60d','log_avg_trading_value_60d']
    TONE=TERMS[:3]
    INTERACTIONS=[s+'_x_afterClose' for s in TONE]
    SPEC=TERMS+['afterCloseInt']+INTERACTIONS
    OUTCOMES=['Day -1','Day 0','Day +1','CAR [-1,+1]']

    def sha256(path):
        h=hashlib.sha256()
        with open(path,'rb') as f:
            for block in iter(lambda:f.read(1<<20),b''):h.update(block)
        return h.hexdigest()

    def within(z,fi,yi,g,ny,tol=1e-12,max_iter=350):
        z=z.copy()
        counts=[np.bincount(fi,minlength=g),np.bincount(yi,minlength=ny)]
        for iteration in range(max_iter):
            z0=z.copy()
            for ix,ct,sz in [(fi,counts[0],g),(yi,counts[1],ny)]:
                for c in range(z.shape[1]):
                    z[:,c]-=np.bincount(ix,weights=z[:,c],minlength=sz)[ix]/ct[ix]
            if np.max(np.abs(z-z0))<tol:return z,iteration+1
        raise RuntimeError('Fixed effect absorption did not converge')

    def fit(x,y,fi,yi,g,ny):
        n,k=x.shape
        z,iterations=within(np.column_stack([x,y]),fi,yi,g,ny)
        xw=z[:,:k];yw=z[:,k:]
        if np.linalg.matrix_rank(xw)!=k:raise RuntimeError('Regressors not full rank after FE absorption')
        invxx=np.linalg.inv(xw.T@xw)
        beta=invxx@(xw.T@yw)
        resid=yw-xw@beta
        rankfe=g+ny-1
        cr1=(g/(g-1))*((n-1)/(n-rankfe-k))
        covs=[]
        for j in range(y.shape[1]):
            score=np.zeros((g,k))
            np.add.at(score,fi,xw*resid[:,[j]])
            covs.append(cr1*invxx@(score.T@score)@invxx)
        return beta,covs,cr1,iterations

    def summary(coef,se,df):
        p=2*tdist.sf(abs(coef/se),df)
        critical=tdist.ppf(.975,df)
        return dict(coef=float(coef),se=float(se),p=float(p),ci95_low=float(coef-critical*se),ci95_high=float(coef+critical*se))

    def latex(rows,n,g,groups,joint_tests):
        lookup={(r['outcome'],r['term'],r['contrast']):r for r in rows}
        def fmt(r):
            if r is None:return '---',''
            stars='^{**}' if r['p']<.05 else ('^{*}' if r['p']<.10 else '')
            return f"${r['coef']:+.4f}{stars}$",f"({r['se']:.4f})"
        lines=[]
        labels=[('unchangedSentiment','Unchanged sentiment'),('revisedSentiment','Revised sentiment'),('newSentiment','New sentiment')]
        for term,name in labels:
            for contrast,label in [('at_or_before','At/before close'),('after_close','After close'),('difference','After minus at/before')]:
                c=[fmt(lookup.get((oc,term,contrast))) for oc in OUTCOMES]
                lines.append(('\\quad ' if contrast!='at_or_before' else '')+ (name+' --- '+label if contrast=='at_or_before' else label)+' & '+' & '.join(x[0] for x in c)+r' \\')
                lines.append(' & '+' & '.join(x[1] for x in c)+r' \\')
            lines.append(r'\addlinespace[2pt]')
        return '\n'.join([
            r'\begin{table}[htbp]',r'\centering',
            r'\caption{Filing-time heterogeneity in disclosure-component sentiment associations}',
            r'\label{tab:stage9_filing_time}',r'\scriptsize',r'\setlength{\tabcolsep}{3pt}',
            r'\begin{tabular}{lrrrr}',r'\hline\hline',
            r' & Day $-1$ & Day $0$ & Day $+1$ & CAR $[-1,+1]$ \\',r'\hline',
            *[ln for ln in lines if ln != r'\addlinespace[2pt]'],
            r'\hline',
            'Joint interactions, $p$ & '+' & '.join(f"{joint_tests[o]['p']:.3f}" for o in OUTCOMES)+r' \\',
            f'Observations & {n:,} & {n:,} & {n:,} & {n:,} '+r'\\',
            f'Firms & {g:,} & {g:,} & {g:,} & {g:,} '+r'\\',
            r'Firm and year fixed effects & Yes & Yes & Yes & Yes \\',
            r'Primary controls & Yes & Yes & Yes & Yes \\',
            r'\hline\hline',r'\end{tabular}',r'\par\vspace{3pt}',
            r'\begin{minipage}{0.99\linewidth}',
            (r'\footnotesize\textit{Notes:} All columns use the primary complete-case sample. '
             f'Of {n:,} filings, {groups["at_or_before"]:,} were submitted at/before the TSE market close and '
             f'{groups["after_close"]:,} afterward. '
             r'Coefficients are abnormal-return percentage points per unit of sentiment. '
             r'The pooled specification includes all three sentiment scores, the seven primary controls, '
             r'an after-close indicator, interactions of after-close with each sentiment score, firm and '
             r'year fixed effects, and firm-clustered CR1 standard errors (parentheses). '
             r'At/before close is the reference group; after-close slopes equal reference slopes plus '
             r'interaction coefficients; differences are directly estimated interaction terms. '
             r'$^{**}p<0.05$, $^{*}p<0.10$ (two-sided, unadjusted). '
             r'Event trading day is shifted to the next TSE session after an after-close filing: '
             r'day $-1$ can therefore include the submission date. '
             r'All tests exploratory; do not infer group differences from subgroup significance alone.'),
            r'\end{minipage}',r'\end{table}',''])

    def main():
        ap=argparse.ArgumentParser(description=__doc__)
        ap.add_argument('--stage9-dir',type=Path,default=Path('data/interim/paper2/regressions/stage9'))
        args=ap.parse_args(arglist);p=args.stage9_dir
        sources={n:p/n for n in ['regression_dataset.csv','model_summary.csv','coefficients.csv']}
        for path in sources.values():
            if not path.is_file():raise FileNotFoundError(path)
        md=pd.read_csv(sources['model_summary.csv']).set_index('model').loc['A_baseline']
        canon=pd.read_csv(sources['coefficients.csv']);canon=canon[canon.model=='A_baseline'].set_index('term')
        req=['baseline_eligible','edinetCode','fiscalYear','afterClose','eventDateRule','submissionOnTradingDay','car_pp_m1_1','car_pp_0_1','car_pp_0_0']+TERMS
        d=pd.read_csv(sources['regression_dataset.csv'],usecols=req,low_memory=False)
        d=d[d.baseline_eligible.eq(True)].reset_index(drop=True)
        n=len(d);firms,fi=np.unique(d.edinetCode.astype(str),return_inverse=True)
        years,yi=np.unique(d.fiscalYear,return_inverse=True);g=len(firms);ny=len(years)
        if (n,g,ny)!=(int(md.N),int(md.firms),int(md.years)):
            raise ValueError('Model A sample mismatch')
        if len(d.afterClose.dropna())!=n:raise ValueError('Missing afterClose classification')
        d['afterClose']=d['afterClose'].map({True:True,False:False,'True':True,'False':False})
        if d['afterClose'].isna().any():raise ValueError('Unrecognized afterClose values')
        d['afterClose']=d['afterClose'].astype(bool)
        if not d.submissionOnTradingDay.eq(True).all():raise ValueError('Unexpected nontrading-day filing')
        if not (d.loc[d.afterClose,'eventDateRule']=='next_trading_day_after_close').all():raise ValueError('afterClose rule mismatch')
        if not (d.loc[~d.afterClose,'eventDateRule']=='same_day_at_or_before_close').all():raise ValueError('reference event-date rule mismatch')
        after=d.afterClose.astype(int).to_numpy()
        group_counts={'at_or_before':int((after==0).sum()),'after_close':int((after==1).sum())}
        x0=d[TERMS].to_numpy(float)
        d0=d.car_pp_0_0.to_numpy(float);d01=d.car_pp_0_1.to_numpy(float);car=d.car_pp_m1_1.to_numpy(float)
        y=np.column_stack([car-d01,d0,d01-d0,car])
        if not np.allclose(y[:,:3].sum(axis=1),y[:,3],atol=1e-9):raise ValueError('AR/CAR identity fails')
        if not np.isfinite(np.column_stack([x0,y])).all():raise ValueError('Nonfinite inputs')
        b0,c0,fac0,it0=fit(x0,y,fi,yi,g,ny)
        maxb=max(abs(b0[i,3]-float(canon.loc[t,'coef'])) for i,t in enumerate(TERMS))
        maxse=max(abs(np.sqrt(c0[3][i,i])-float(canon.loc[t,'se'])) for i,t in enumerate(TERMS))
        if maxb>1e-7 or maxse>1e-7 or abs(fac0-float(md.cr1_factor))>1e-7:
            raise ValueError(f'Baseline reproduction fails: beta={maxb}, se={maxse}, factor={fac0}')
        x=np.column_stack([x0,after]+[x0[:,i]*after for i in range(3)])
        b,covs,fac,it=fit(x,y,fi,yi,g,ny)
        joint_tests={}
        for j,outcome in enumerate(OUTCOMES):
            inds=[len(TERMS)+1+i for i in range(3)]
            delta=b[inds,j]
            cov=covs[j][np.ix_(inds,inds)]
            fstat=float((delta @ np.linalg.solve(cov,delta))/3)
            joint_tests[outcome]={'F':fstat,'df_num':3,'df_denom':g-1,
                                  'p':float(fdist.sf(fstat,3,g-1))}
        rows=[]
        for j,outcome in enumerate(OUTCOMES):
            for i,term in enumerate(TONE):
                for label,weights in [('at_or_before',[(i,1.)]),('after_close',[(i,1.),(len(TERMS)+1+i,1.)]),('difference',[(len(TERMS)+1+i,1.)])]:
                    a=np.zeros(len(SPEC))
                    for k,val in weights:a[k]=val
                    coef=float(a@b[:,j]);se=float(np.sqrt(a@covs[j]@a))
                    rows.append(dict(outcome=outcome,term=term,contrast=label,**summary(coef,se,g-1),N=n,firms=g,
                                     n_at_or_before=group_counts['at_or_before'],n_after_close=group_counts['after_close']))
        # Every CAR slope must be the sum of daily slopes for the same contrast.
        res=pd.DataFrame(rows)
        for term in TONE:
            for contrast in ('at_or_before','after_close','difference'):
                v=res[(res.term==term)&(res.contrast==contrast)].set_index('outcome').coef
                if abs(sum(v[k] for k in OUTCOMES[:3])-v[OUTCOMES[3]])>1e-8:raise ValueError('CAR slopes do not add')
        out=p/'tables';out.mkdir(parents=True,exist_ok=True)
        csv=out/'table5_filing_time_heterogeneity.csv'
        tex=out/'table5_filing_time_heterogeneity.tex'
        provenance=out/'table5_filing_time_heterogeneity_provenance.json'
        res.to_csv(csv,index=False)
        tex.write_text(latex(rows,n,g,group_counts,joint_tests),encoding='utf-8')
        metadata={'generated_utc':datetime.now(timezone.utc).isoformat(),'method':'Pooled 3-component after-close interactions; firm/year FE; CR1 firm clustered',
                  'source_sha256':{key:sha256(value) for key,value in sources.items()},'n':n,'firms':g,
                  'years':ny,'groups':group_counts,'joint_interactions_wald':joint_tests,'terms':SPEC,'outcomes':OUTCOMES,'cr1_factor':fac,
                  'primary_replication_max_beta_diff':float(maxb),'primary_replication_max_se_diff':float(maxse),
                  'primary_cr1_factor':fac0,'iterations_baseline':it0,'iterations_heterogeneity':it,
                  'note':'Day -1 for after-close filings can be the original filing session; p-values exploratory and unadjusted.'}
        provenance.write_text(json.dumps(metadata,indent=2)+'\n')
        print(f'Stage 9 filing-time pooled interactions: N={n:,}, firms={g:,}, groups={group_counts}')
        print(f'Primary replication max beta diff={maxb:.2e}, SE diff={maxse:.2e}')
        for term in TONE:
            print('\n'+term)
            for outcome in OUTCOMES:
                entries=res[(res.term==term)&(res.outcome==outcome)].set_index('contrast')
                print(f' {outcome:12s} ref {entries.loc["at_or_before","coef"]:+.4f} (p={entries.loc["at_or_before","p"]:.3f}); after {entries.loc["after_close","coef"]:+.4f} (p={entries.loc["after_close","p"]:.3f}); diff {entries.loc["difference","coef"]:+.4f} (p={entries.loc["difference","p"]:.3f})')
        print('Joint three-interaction Wald tests:')
        for oc,stats in joint_tests.items(): print(f"  {oc}: F(3,{g-1})={stats['F']:.4f}, p={stats['p']:.5f}")
        print('Generated:',csv,tex,provenance,sep='\n  ')

    main()

def _run_robustness_tables(stage9: Path, tables: Path) -> None:
    # One reporting entry point. Estimation implementation is a reusable src module.
    import sys
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from src.pipeline.stages.stage9_robustness import run
    run(stage9, tables)

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stage9-dir", type=Path,
                   default=Path("data/interim/paper2/regressions/stage9"))
    p.add_argument("--skip-event-day", action="store_true",
                   help="Skip secondary event-day regressions (Table 4)")
    p.add_argument("--skip-filing-time", action="store_true",
                   help="Skip filing-time heterogeneity regressions (Table 5)")
    p.add_argument("--skip-robustness", action="store_true",
                   help="Skip robustness regressions and appendix sensitivity tables (Table 6)")
    p.add_argument("--only", choices=["descriptive", "primary", "event-day", "filing-time", "robustness"],
                   help="Generate only the selected table family; keeps other outputs untouched")
    args = p.parse_args()
    stage9 = args.stage9_dir
    tables = stage9 / "tables"
    # Check all required inputs first, before writing any outputs.
    required = ["regression_dataset.csv", "model_summary.csv", "provenance.json",
                "coefficients.csv", "coefficient_contrasts.csv"]
    missing = [str(stage9 / f) for f in required if not (stage9 / f).is_file()]
    if missing:
        p.error("Missing Stage 9 source files: " + ", ".join(missing))
    if args.only and (args.skip_event_day or args.skip_filing_time or args.skip_robustness):
        p.error("--only cannot be combined with --skip-event-day or --skip-filing-time")
    steps = [
        ("descriptive", "Descriptive statistics, sample flow, correlations",
         lambda: _run_descriptive_tables(["--input-dir", str(stage9), "--output-dir", str(tables)])),
        ("primary", "Model A primary regression table",
         lambda: _run_primary_regression_table(["--stage9-dir", str(stage9), "--output-dir", str(tables)])),
        ("event-day", "Event-day decomposition and independent Model A check",
         lambda: _run_event_day_table(["--stage9-dir", str(stage9)])),
        ("filing-time", "Filing-time heterogeneity and independent Model A check",
         lambda: _run_filing_time_table(["--stage9-dir", str(stage9)])),
        ("robustness", "Stage 9 robustness and independent binary taxonomy",
         lambda: _run_robustness_tables(stage9, tables)),
    ]
    selected = [item for item in steps if
                (args.only is None or item[0] == args.only) and
                (item[0] != "event-day" or not args.skip_event_day) and
                (item[0] != "filing-time" or not args.skip_filing_time) and
                (item[0] != "robustness" or not args.skip_robustness)]
    for index, (_name, description, runner) in enumerate(selected, start=1):
        print(f"[{index}/{len(selected)}] {description}", flush=True)
        runner()
    print("All requested tables generated in:", tables, flush=True)


if __name__ == "__main__":
    main()
