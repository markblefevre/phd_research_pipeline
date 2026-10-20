#!/usr/bin/env python3
"""Exploratory Stage 8D five-way *composition* regressions with pre-filing controls.

Imports the original five-way reader/validation code; does not modify frozen inputs.
Five-way counts describe composition, NOT five separate sentiment measures.
All five specifications within each CAR window use identical complete-case rows.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

from run_5way_composition_regressions import (
    DEFAULT_CLASSIFICATION, DEFAULT_PANEL, WINDOWS,
    is_true, read_classification, read_panel,
)

DEFAULT_CONTROLS = Path('data/interim/paper2/controls/stage8d_market_industry_event_controls.csv')
DEFAULT_OUT = Path('data/interim/paper2/regressions/stage8d_5way_composition_firstlook')
BASE = ['revisedShare', 'newShare']
DOC = ['noveltyCNum', 'logLengthChange']
VAL = ['ln_mktcap', 'book_to_market', 'roe_ttm']
MARKET = ['volatility_60d', 'momentum_20d', 'log_avg_trading_value_60d']
NUMERIC = [*DOC, *VAL, *MARKET, 'revisedShare', 'newShare', 'changedShare']


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--classification', type=Path, default=DEFAULT_CLASSIFICATION)
    p.add_argument('--panel', type=Path, default=DEFAULT_PANEL)
    p.add_argument('--controls', type=Path, default=DEFAULT_CONTROLS)
    p.add_argument('--output-dir', type=Path, default=DEFAULT_OUT)
    args = p.parse_args()

    for name in ('classification', 'panel', 'controls'):
        path = getattr(args, name).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f'{name} missing: {path}; supply --{name} PATH')
        setattr(args, name, path)
    if args.output_dir.exists():
        raise FileExistsError(f'Refusing to overwrite existing output directory: {args.output_dir}')

    classification = read_classification(args.classification)
    panel = read_panel(args.panel)
    control_cols = ['curr_docID', 'industry_s33', *VAL, *MARKET]
    ctrl = pd.read_csv(args.controls, usecols=control_cols,
                       dtype={'curr_docID': 'string', 'industry_s33': 'string'}, low_memory=False)
    ctrl['curr_docID'] = ctrl['curr_docID'].str.strip()
    if ctrl['curr_docID'].isna().any() or ctrl['curr_docID'].duplicated().any():
        raise ValueError('Controls contain missing or duplicate curr_docID')

    joined = panel.merge(classification, on='curr_docID', how='left', validate='one_to_one', indicator='class_match')
    matched_class = int(joined['class_match'].eq('both').sum())
    joined = joined.merge(ctrl, on='curr_docID', how='left', validate='one_to_one', indicator='control_match')
    matched_ctrl = int(joined['control_match'].eq('both').sum())
    print(f'Panel: {len(panel):,}; five-way matched: {matched_class:,}; controls matched: {matched_ctrl:,}', flush=True)
    if matched_class < 25000 or matched_ctrl < 30000:
        raise ValueError('Unexpectedly low join coverage; inspect inputs')

    for col in NUMERIC:
        joined[col] = pd.to_numeric(joined[col], errors='coerce').replace([np.inf, -np.inf], np.nan)
    joined['fiscalYear'] = pd.to_numeric(joined['fiscalYear'], errors='coerce')
    joined['industry_s33'] = joined['industry_s33'].str.strip().replace('', pd.NA)
    joined['edinetCode'] = joined['edinetCode'].astype('string').str.strip().replace('', pd.NA)

    specs = {
        'shares_year_fe': (BASE, False),
        'shares_doc_controls_year_fe': (BASE + DOC, False),
        'shares_plus_valuation': (BASE + DOC + VAL, False),
        'shares_full_industry_market': (BASE + DOC + VAL + MARKET, True),
        'pooled_changed_full_industry_market': (['changedShare'] + DOC + VAL + MARKET, True),
    }
    comparison = []
    coefficients = []
    summaries = []
    window_stats = {}
    for window, (label, sample_flag, pp, raw) in WINDOWS.items():
        y = f'y_{pp}'
        frame = joined.loc[is_true(joined[sample_flag])].copy() if sample_flag in joined else joined.copy()
        for col in [y, *NUMERIC]:
            if col == y:
                frame[col] = pd.to_numeric(frame[col], errors='coerce').replace([np.inf, -np.inf], np.nan)
        required = [y, 'edinetCode', 'fiscalYear', 'industry_s33', *NUMERIC]
        sample = frame[required].dropna().copy()
        if len(sample) < 1000 or sample['edinetCode'].nunique() < 100:
            raise ValueError(f'Insufficient complete cases for {label}: {len(sample):,}')
        sample['fiscalYear'] = sample['fiscalYear'].astype(int).astype(str)
        sample['industry_s33'] = sample['industry_s33'].astype(str)
        window_stats[window] = {
            'eligible': len(frame), 'complete_case': len(sample),
            'firms': int(sample['edinetCode'].nunique()),
            'industry_groups': int(sample['industry_s33'].nunique()),
        }
        for name, (terms, industry_fe) in specs.items():
            rhs = ' + '.join(terms) + ' + C(fiscalYear)'
            if industry_fe:
                rhs += ' + C(industry_s33)'
            model = smf.ols(f'{y} ~ {rhs}', data=sample).fit(
                cov_type='cluster', cov_kwds={'groups': sample['edinetCode'], 'use_correction': True}
            )
            p_equal = (float(np.asarray(model.t_test('revisedShare - newShare = 0').pvalue).item())
                       if 'revisedShare' in terms and 'newShare' in terms else None)
            row = {'window': window, 'label': label, 'spec': name, 'n': int(model.nobs),
                   'firms': int(sample['edinetCode'].nunique()), 'r2': float(model.rsquared),
                   'p_revised_equals_new': p_equal}
            for term in ('revisedShare', 'newShare', 'changedShare'):
                row[f'beta_{term}'] = float(model.params[term]) if term in terms else None
                row[f'p_{term}'] = float(model.pvalues[term]) if term in terms else None
                row[f'effect_per_10pp_{term}'] = float(model.params[term] * .1) if term in terms else None
            comparison.append(row)
            for term in terms:
                coefficients.append({'window': window, 'spec': name, 'term': term,
                                     'coef': float(model.params[term]),
                                     'se_firm_clustered': float(model.bse[term]),
                                     'pvalue': float(model.pvalues[term]),
                                     'n': int(model.nobs)})
            summaries.append(f'=== {label} / {name} ===\n{model.summary().as_text()}\n')
            def show(term):
                return (f'{term}={model.params[term]*.1:+.4f} pp per +10pp '
                        f'(p={model.pvalues[term]:.4g})') if term in terms else ''
            print(f'{label:8s} {name:36s} N={len(sample):6,d} '
                  f'{show("revisedShare") or show("changedShare")} '
                  f'{show("newShare")} '
                  f'p(rev=new)={p_equal:.4g}' if p_equal is not None else
                  f'{label:8s} {name:36s} N={len(sample):6,d} {show("changedShare")}', flush=True)

    args.output_dir.mkdir(parents=True, exist_ok=False)
    pd.DataFrame(comparison).to_csv(args.output_dir / 'model_comparison.csv', index=False)
    pd.DataFrame(coefficients).to_csv(args.output_dir / 'coefficients.csv', index=False)
    (args.output_dir / 'full_summaries.txt').write_text('\n'.join(summaries), encoding='utf-8')
    audit = {
        'classification': str(args.classification), 'classification_sha256': sha256(args.classification),
        'panel': str(args.panel), 'panel_sha256': sha256(args.panel),
        'controls': str(args.controls), 'controls_sha256': sha256(args.controls),
        'panel_rows': len(panel), 'matched_five_way': matched_class, 'matched_controls': matched_ctrl,
        'window_samples': window_stats,
        'specifications': {name: {'terms': terms, 'industry_fe': industry_fe}
                           for name, (terms, industry_fe) in specs.items()},
        'same_complete_case_sample_within_window': True,
        'dependent_variable_units': 'CAR percentage points',
        'composition_coefficients': '10 percentage point increase in sentence share; unchangedShare is reference category for revised/new share models',
        'standard_errors': 'clustered by firm (edinetCode)',
        'fixed_effects': 'fiscal year; S33 industry in full models',
        'market_controls_caveat': 'raw-close-derived controls; preliminary until adjusted-return replacement',
        'interpretation': 'exploratory disclosure composition associations, not five-way sentiment effects or causal estimates',
    }
    (args.output_dir / 'run_metadata.json').write_text(json.dumps(audit, indent=2) + '\n', encoding='utf-8')
    print('\nOutputs:', args.output_dir, flush=True)


if __name__ == '__main__':
    main()
