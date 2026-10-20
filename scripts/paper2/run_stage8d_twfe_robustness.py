#!/usr/bin/env python3
"""Stage 8D within-firm/year robustness: sentiment and five-way *composition*.

Uses the existing Stage 8 TWFE alternating-projection implementation; new outputs only.
Does NOT estimate five-way component-specific sentiment (not yet scored).
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import t as student_t

from run_stage8_twfe import absorb, fe_rank
from run_5way_composition_regressions import read_classification, WINDOWS, is_true

BASE = Path('data/interim/paper2')
PANEL = Path('~/paper2_local/data/interim/paper2/analysis/stage8_panel/stage8_filing_panel.csv').expanduser()
CLASSIFICATION = BASE / 'alignment/disclosure_change_5way_v1/collected_repaired_prompt_v2/disclosure_change_2group_documents.csv'
CONTROLS = BASE / 'controls/stage8d_market_industry_event_controls.csv'
OUTPUT = BASE / 'regressions/stage8d_twfe_robustness_firstlook'
DOC = ['noveltyCNum', 'logLengthChange']
VAL = ['ln_mktcap', 'book_to_market', 'roe_ttm']
MARKET = ['volatility_60d', 'momentum_20d', 'log_avg_trading_value_60d']
COMMON = DOC + VAL + MARKET
SENTIMENT = ['persistentSentiment', 'novelSentiment']
SHARES = ['revisedShare', 'newShare']


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def fit(sample, y_col, terms, mode, comparison, window):
    y = sample[y_col].to_numpy(dtype=float)
    x = sample[terms].to_numpy(dtype=float)
    firms, firm_ids = np.unique(sample['edinetCode'].to_numpy(), return_inverse=True)
    years, year_ids = np.unique(sample['fiscalYear'].to_numpy(), return_inverse=True)
    groups = [firm_ids, year_ids] if mode == 'firm_year_fe' else [year_ids]
    z, iterations, error = absorb(np.column_stack([y, x]), groups)
    yw, xw = z[:, 0], z[:, 1:]
    n, k = xw.shape
    rank = int(np.linalg.matrix_rank(xw))
    if rank != k:
        raise ValueError(f'{comparison}/{window}/{mode}: rank deficient {rank}/{k}; inspect covariates')
    fe_dof, components = fe_rank(firm_ids, year_ids, mode == 'firm_year_fe')
    df_resid = n - fe_dof - k
    if df_resid <= 0:
        raise ValueError('Insufficient degrees of freedom')
    beta = np.linalg.lstsq(xw, yw, rcond=None)[0]
    residual = yw - xw @ beta
    inv = np.linalg.inv(xw.T @ xw)
    scores = xw * residual[:, None]
    cluster_scores = np.zeros((len(firms), k))
    np.add.at(cluster_scores, firm_ids, scores)
    g = len(firms)
    factor = (g / (g - 1)) * ((n - 1) / df_resid)
    vcov = (inv @ (cluster_scores.T @ cluster_scores) @ inv) * factor
    vcov = (vcov + vcov.T) / 2
    se = np.sqrt(np.diag(vcov))
    if not np.isfinite(se).all() or (se <= 0).any():
        raise ValueError('Invalid clustered standard errors')
    p = 2 * student_t.sf(np.abs(beta / se), df=g-1)
    diff_terms = SHARES if comparison == 'composition' else SENTIMENT
    a, b = diff_terms
    vec = np.zeros(k)
    vec[terms.index(b)] = 1
    vec[terms.index(a)] = -1
    delta = float(vec @ beta)
    delta_se = float(np.sqrt(vec @ vcov @ vec))
    delta_p = float(2 * student_t.sf(abs(delta / delta_se), df=g-1))
    within_r2 = float(1 - (residual @ residual) / (yw @ yw)) if (yw @ yw) > 0 else None
    result = {'analysis': comparison, 'window': window, 'spec': mode, 'N': n,
              'firms': g, 'years': len(years), 'within_r2': within_r2,
              'fe_rank': fe_dof, 'fe_components': components, 'absorb_iterations': iterations,
              'absorb_error': error, 'cr1_factor': factor,
              'difference': f'{b} - {a}', 'difference_coef': delta,
              'difference_se': delta_se, 'difference_p': delta_p}
    coef_rows = []
    for j, term in enumerate(terms):
        coef_rows.append({'analysis': comparison, 'window': window, 'spec': mode,
                          'term': term, 'coef': float(beta[j]), 'se': float(se[j]),
                          'p': float(p[j]), 'N': n, 'firms': g,
                          'effect_per_10pp': float(beta[j] * .1) if term.endswith('Share') else None})
    lookup = {row['term']: row for row in coef_rows}
    for term in diff_terms:
        result[f'beta_{term}'] = lookup[term]['coef']
        result[f'p_{term}'] = lookup[term]['p']
    print(f'{comparison:11s} {window:5s} {mode:13s} N={n:,} '
          f'{a}={lookup[a]["coef"]*(.1 if comparison == "composition" else 1):+.5f} '
          f'(p={lookup[a]["p"]:.4g}) '
          f'{b}={lookup[b]["coef"]*(.1 if comparison == "composition" else 1):+.5f} '
          f'(p={lookup[b]["p"]:.4g}) p(equal)={delta_p:.4g}', flush=True)
    return result, coef_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--panel', type=Path, default=PANEL)
    parser.add_argument('--classification', type=Path, default=CLASSIFICATION)
    parser.add_argument('--controls', type=Path, default=CONTROLS)
    parser.add_argument('--output-dir', type=Path, default=OUTPUT)
    args = parser.parse_args()
    for name in ('panel', 'classification', 'controls'):
        path = getattr(args, name).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f'{name}: {path}')
        setattr(args, name, path)
    if args.output_dir.exists():
        raise FileExistsError(f'Refusing to overwrite {args.output_dir}')

    panel_cols = ['curr_docID', 'edinetCode', 'fiscalYear', *SENTIMENT, *DOC]
    for _, sample_flag, pp, _ in WINDOWS.values():
        panel_cols.extend([sample_flag, pp])
    panel = pd.read_csv(args.panel, usecols=panel_cols,
                        dtype={'curr_docID': 'string', 'edinetCode': 'string'}, low_memory=False)
    controls = pd.read_csv(args.controls, usecols=['curr_docID', *VAL, *MARKET],
                           dtype={'curr_docID': 'string'}, low_memory=False)
    classification = read_classification(args.classification)
    for label, df in [('panel', panel), ('controls', controls), ('classification', classification)]:
        df['curr_docID'] = df['curr_docID'].str.strip()
        if df['curr_docID'].isna().any() or df['curr_docID'].duplicated().any():
            raise ValueError(f'{label}: duplicate or missing curr_docID')
    merged = panel.merge(controls, on='curr_docID', how='left', validate='one_to_one', indicator='ctrl_join')
    matched_controls = int(merged['ctrl_join'].eq('both').sum())
    merged = merged.merge(classification, on='curr_docID', how='left', validate='one_to_one', indicator='class_join')
    matched_class = int(merged['class_join'].eq('both').sum())
    print(f'Panel={len(panel):,}, controls matched={matched_controls:,}, five-way matched={matched_class:,}', flush=True)
    if matched_controls < 30000 or matched_class < 30000:
        raise ValueError('Unexpectedly low match coverage')
    numeric = list(dict.fromkeys([*SENTIMENT, *COMMON, *SHARES, 'changedShare', 'fiscalYear',
                                  *(pp for _, _, pp, _ in WINDOWS.values())]))
    for col in numeric:
        merged[col] = pd.to_numeric(merged[col], errors='coerce').replace([np.inf, -np.inf], np.nan)
    merged['edinetCode'] = merged['edinetCode'].str.strip().replace('', pd.NA)

    all_models, all_coefs, samples = [], [], []
    for window, (label, flag, pp, _) in WINDOWS.items():
        eligible = merged.loc[is_true(merged[flag])].copy()
        for analysis, primary in [('sentiment', SENTIMENT), ('composition', SHARES)]:
            terms = primary + COMMON
            needed = [pp, 'edinetCode', 'fiscalYear', *terms]
            sample = eligible[needed].dropna().copy()
            if len(sample) < 1000 or sample['edinetCode'].nunique() < 100:
                raise ValueError(f'{analysis}/{window}: too few observations')
            if not np.all(sample['fiscalYear'] % 1 == 0):
                raise ValueError('Non-integer fiscal year')
            sample['fiscalYear'] = sample['fiscalYear'].astype(int).astype(str)
            sample['edinetCode'] = sample['edinetCode'].astype(str)
            samples.append({'window': window, 'analysis': analysis, 'eligible': len(eligible),
                            'complete_case': len(sample), 'firms': sample['edinetCode'].nunique(),
                            'single_observation_firms': int((sample.groupby('edinetCode').size() == 1).sum())})
            for mode in ['year_fe', 'firm_year_fe']:
                result, coefficients = fit(sample, pp, terms, mode, analysis, window)
                all_models.append(result)
                all_coefs.extend(coefficients)

    args.output_dir.mkdir(parents=True, exist_ok=False)
    pd.DataFrame(all_models).to_csv(args.output_dir / 'model_comparison.csv', index=False)
    pd.DataFrame(all_coefs).to_csv(args.output_dir / 'coefficients.csv', index=False)
    metadata = {'panel': str(args.panel), 'panel_sha256': digest(args.panel),
                'classification': str(args.classification), 'classification_sha256': digest(args.classification),
                'controls': str(args.controls), 'controls_sha256': digest(args.controls),
                'panel_rows': len(panel), 'matched_controls': matched_controls,
                'matched_five_way': matched_class, 'samples': samples,
                'models': 'full Stage 8D controls, year FE versus firm+year FE, identical sample within analysis/window',
                'standard_errors': 'firm-clustered CR1, effective absorbed-FE degrees of freedom, t(firms-1)',
                'outcome_units': 'CAR percentage points',
                'composition_units': 'effects printed per 10 percentage points of substantive sentence share',
                'composition_reference': 'unchangedShare omitted; new/revised are substitutions for unchanged',
                'sentiment_units': 'persistent/novel sentiment in [-1,1]',
                'note': 'composition is NOT category-specific sentiment; market controls provisional raw close based'}
    (args.output_dir / 'run_metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print('Outputs:', args.output_dir, flush=True)


if __name__ == '__main__':
    main()
