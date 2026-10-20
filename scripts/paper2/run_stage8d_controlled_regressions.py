#!/usr/bin/env python3
"""Stage 8D: exploratory novel vs persistent sentiment regressions with firm controls.

Joins the pre-existing Stage 8 component-sentiment filing panel to Stage 8D
pre-filing controls on unique curr_docID. Fits three CAR windows, comparing
three specifications on the SAME complete-case sample per window:
  (1) sentiment + novelty + length change + fiscal-year FE;
  (2) add valuation controls;
  (3) add pre-filing market controls + historical industry FE.
Firm-clustered SE. Never overwrites frozen Stage 8A-C outputs.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

DEFAULT_PANEL = Path('data/interim/paper2/analysis/stage8_panel/stage8_filing_panel.csv')
DEFAULT_CONTROLS = Path('data/interim/paper2/controls/stage8d_market_industry_event_controls.csv')
DEFAULT_OUT = Path('data/interim/paper2/regressions/stage8d_controlled_firstlook')
WINDOWS = {'0_1': 'car_pp_0_1', '0_0': 'car_pp_0_0', 'm1_1': 'car_pp_m1_1'}
CORE = ['persistentSentiment', 'novelSentiment', 'noveltyCNum', 'logLengthChange']
VAL = ['ln_mktcap', 'book_to_market', 'roe_ttm']
MARKET = ['volatility_60d', 'momentum_20d', 'log_avg_trading_value_60d']
ALL = CORE + VAL + MARKET


def sha256(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--panel', type=Path, default=DEFAULT_PANEL)
    ap.add_argument('--controls', type=Path, default=DEFAULT_CONTROLS)
    ap.add_argument('--output-dir', type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    if args.output_dir.exists():
        raise FileExistsError(f'Output directory exists: {args.output_dir}. Supply a fresh --output-dir')
    panel_cols = set(pd.read_csv(args.panel, nrows=0).columns)
    required_panel = {'curr_docID', 'edinetCode', 'fiscalYear', *CORE, *WINDOWS.values(),
                      *(f'eventStudySample_{w}' for w in WINDOWS)}
    if not required_panel.issubset(panel_cols):
        raise ValueError(f'Panel missing: {sorted(required_panel-panel_cols)}')
    panel = pd.read_csv(args.panel, usecols=list(required_panel),
                        dtype={'curr_docID': str, 'edinetCode': str}, low_memory=False)
    if panel['curr_docID'].isna().any() or panel['curr_docID'].duplicated().any():
        raise ValueError('Panel curr_docID missing or duplicated')
    controls_cols = set(pd.read_csv(args.controls, nrows=0).columns)
    required_ctrl = {'curr_docID', 'industry_s33', *VAL, *MARKET}
    if not required_ctrl.issubset(controls_cols):
        raise ValueError(f'Stage 8D controls missing: {sorted(required_ctrl-controls_cols)}')
    controls = pd.read_csv(args.controls, usecols=list(required_ctrl),
                           dtype={'curr_docID': str, 'industry_s33': str}, low_memory=False)
    if controls['curr_docID'].isna().any() or controls['curr_docID'].duplicated().any():
        raise ValueError('Controls curr_docID missing or duplicated')
    panel = panel.merge(controls, on='curr_docID', how='left', validate='one_to_one', indicator=True)
    matched = int(panel['_merge'].eq('both').sum())
    print(f'Panel: {len(panel):,}; matched controls: {matched:,}; unmatched: {len(panel)-matched:,}', flush=True)
    if matched < 30000:
        raise ValueError('Too few Stage 8D control matches; inspect join keys')
    for col in [*ALL, *WINDOWS.values()]:
        panel[col] = pd.to_numeric(panel[col], errors='coerce').replace([np.inf, -np.inf], np.nan)
    panel['fiscalYear'] = pd.to_numeric(panel['fiscalYear'], errors='coerce')
    panel['industry_s33'] = panel['industry_s33'].str.strip().replace('', np.nan)
    for col in ['persistentSentiment', 'novelSentiment']:
        invalid = panel[col].notna() & ~panel[col].between(-1.000001, 1.000001)
        if invalid.any():
            raise ValueError(f'{col} outside [-1,1]: {invalid.sum()} rows')
    specs = {
        'baseline_same_sample': (' + '.join(CORE) + ' + C(fiscalYear)', CORE),
        'plus_valuation': (' + '.join(CORE+VAL) + ' + C(fiscalYear)', CORE+VAL),
        'full_industry_market': (' + '.join(ALL) + ' + C(fiscalYear) + C(industry_s33)', ALL),
    }
    summary_rows = []
    coefficient_rows = []
    full_summaries = []
    sample_stats = {}
    for window, outcome in WINDOWS.items():
        status = panel[f'eventStudySample_{window}'].astype('string').str.strip().str.lower()
        if not status.dropna().isin(['true','false','1','0']).all():
            raise ValueError(f'Invalid eventStudySample_{window} values')
        eligible = status.isin(['true','1'])
        columns = [outcome, 'edinetCode', 'fiscalYear', 'industry_s33', *ALL]
        sample = panel.loc[eligible, columns].dropna().copy()
        sample['fiscalYear'] = sample['fiscalYear'].astype(int).astype(str)
        if len(sample) < 1000 or sample['edinetCode'].nunique() < 100:
            raise ValueError(f'Insufficient full-control sample for {window}: {len(sample)} rows')
        sample_stats[window] = {
            'eligible_event_window_rows': int(eligible.sum()),
            'complete_case_rows': len(sample),
            'complete_case_firms': int(sample['edinetCode'].nunique()),
            'industry_groups': int(sample['industry_s33'].nunique()),
        }
        for name, (rhs, terms) in specs.items():
            formula = f'{outcome} ~ {rhs}'
            fit = smf.ols(formula, data=sample).fit(
                cov_type='cluster', cov_kwds={'groups': sample['edinetCode'], 'use_correction': True}
            )
            test = fit.t_test('novelSentiment - persistentSentiment = 0')
            row = {
                'window': window, 'spec': name, 'n': int(fit.nobs),
                'firms': int(sample['edinetCode'].nunique()), 'r_squared': float(fit.rsquared),
                'beta_persistent': float(fit.params['persistentSentiment']),
                'p_persistent': float(fit.pvalues['persistentSentiment']),
                'beta_novel': float(fit.params['novelSentiment']),
                'p_novel': float(fit.pvalues['novelSentiment']),
                'beta_novel_minus_persistent': float(fit.params['novelSentiment'] - fit.params['persistentSentiment']),
                'p_novel_equals_persistent': float(np.asarray(test.pvalue).item()),
            }
            summary_rows.append(row)
            for term in terms:
                coefficient_rows.append({
                    'window': window, 'spec': name, 'term': term,
                    'coef': float(fit.params[term]), 'se_cluster_firm': float(fit.bse[term]),
                    't': float(fit.tvalues[term]), 'p': float(fit.pvalues[term]),
                    'n': int(fit.nobs),
                })
            full_summaries.append(f'=== {window} / {name} ===\n{fit.summary().as_text()}\n')
            print(f"{window:5s} {name:21s} N={row['n']:6,d} "
                  f"betaP={row['beta_persistent']:+.5f} (p={row['p_persistent']:.4g}) "
                  f"betaN={row['beta_novel']:+.5f} (p={row['p_novel']:.4g}) "
                  f"p(N=P)={row['p_novel_equals_persistent']:.4g}", flush=True)
    args.output_dir.mkdir(parents=True)
    pd.DataFrame(summary_rows).to_csv(args.output_dir/'model_comparison.csv', index=False)
    pd.DataFrame(coefficient_rows).to_csv(args.output_dir/'coefficients.csv', index=False)
    (args.output_dir/'full_summaries.txt').write_text('\n'.join(full_summaries))
    metadata = {
        'panel': str(args.panel), 'panel_sha256': sha256(args.panel),
        'controls': str(args.controls), 'controls_sha256': sha256(args.controls),
        'panel_rows': len(panel), 'matched_controls': matched,
        'window_samples': sample_stats,
        'models': {k: v[0] for k,v in specs.items()},
        'units': 'CAR in percentage points; sentiment on [-1,1]',
        'errors': 'firm-clustered by edinetCode',
        'comparison': 'all three specifications use identical complete-case rows within each CAR window',
        'interpretation': 'exploratory associations, not causal identification',
        'market_caveat': 'raw close-based returns with split sessions excluded; replace with adjusted returns for final results',
    }
    (args.output_dir/'run_metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')
    print('\nOutputs:', args.output_dir, flush=True)

if __name__ == '__main__':
    main()
