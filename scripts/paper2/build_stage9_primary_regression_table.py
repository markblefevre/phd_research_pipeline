#!/usr/bin/env python3
"""Publication table for Paper 2 primary Stage 9 regression (read-only inputs).

Run from the repository root:
    python scripts/paper2/build_stage9_primary_regression_table.py

Reads the existing Stage 9 coefficients, contrasts, model summary, and
provenance. Writes only into stage9/tables. No regression is re-estimated.
Requires Python standard library only.
"""
from __future__ import annotations

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
    args = ap.parse_args()
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
        r'All measures are evaluated on the same Stage~9 primary complete-case sample. ',
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

if __name__ == '__main__':
    main()
