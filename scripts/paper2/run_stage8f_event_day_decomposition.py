#!/usr/bin/env python3
"""Stage 8F: decompose three-component sentiment CAR association by trading day.

Run from repo root after copying alongside the Stage 8E regression script:
 python -u scripts/paper2/run_stage8f_event_day_decomposition.py

No API calls; reads immutable 8E scores, Stage 8 panel, and Stage 8D controls.
All outcomes share EXACTLY the same rows within each missingness analysis.
AR(-1) = CAR[-1,+1] - CAR[0,+1]
AR( 0) = CAR[0,0]
AR(+1) = CAR[0,+1] - CAR[0,0]
CAR is in percentage points. These are trading days relative to Stage 7 event date.
Important: AR(-1) is prior to EVENT day (which can differ from submission date
for after-close filings). This analysis by itself does not prove information leakage.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from run_stage8e_validate_regressions import (
    BASE, DEFAULT_RUN, DEFAULT_PANEL, DEFAULT_CONTROLS, SENTIMENT,
    CONTROLS, WINDOWS, event_flag, validate_scores, load_panel, estimate,
)

DEFAULT_OUT = BASE / 'regressions/stage8f_event_day_decomposition'
DAY_COLS = ['ar_pp_m1', 'ar_pp_0', 'ar_pp_p1']
OUTCOMES = [*DAY_COLS, 'car_pp_0_1', 'car_pp_m1_1']
DAY_CONTRASTS = {
    'm1_minus_0': ('ar_pp_m1', 'ar_pp_0'),
    'm1_minus_p1': ('ar_pp_m1', 'ar_pp_p1'),
    'p1_minus_0': ('ar_pp_p1', 'ar_pp_0'),
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def shared_sample(joined: pd.DataFrame, analysis: str):
    valid = pd.Series(True, index=joined.index)
    for flag, outcome in WINDOWS.values():
        valid &= event_flag(joined[flag]) & joined[outcome].notna()
    eligible = joined.loc[valid].copy()
    if eligible.empty:
        raise ValueError('No observations eligible for all three CAR windows')
    # Fail rather than silently using inconsistent event-window data.
    for _, (flag, outcome) in WINDOWS.items():
        if not event_flag(eligible[flag]).all() or eligible[outcome].isna().any():
            raise ValueError(f'Incomplete CAR on joint sample: {outcome}')
    eligible['ar_pp_m1'] = eligible['car_pp_m1_1'] - eligible['car_pp_0_1']
    eligible['ar_pp_0'] = eligible['car_pp_0_0']
    eligible['ar_pp_p1'] = eligible['car_pp_0_1'] - eligible['car_pp_0_0']
    # Identities must hold row-for-row, with no cumulative-return rounding in this code.
    if not np.allclose(eligible[DAY_COLS].sum(axis=1), eligible['car_pp_m1_1'], rtol=1e-12, atol=1e-10):
        raise ValueError('Event-day decomposition failed [-1,+1] identity')
    if not np.allclose(eligible[['ar_pp_0', 'ar_pp_p1']].sum(axis=1), eligible['car_pp_0_1'], rtol=1e-12, atol=1e-10):
        raise ValueError('Event-day decomposition failed [0,+1] identity')

    if analysis == 'presence_indicators':
        for col in SENTIMENT:
            eligible[f'{col}Present'] = eligible[col].notna().astype(float)
            eligible[col] = eligible[col].fillna(0.0)
        terms = SENTIMENT + [f'{col}Present' for col in SENTIMENT] + CONTROLS
    elif analysis == 'complete_case':
        terms = SENTIMENT + CONTROLS
    else:
        raise ValueError(f'Unknown analysis: {analysis}')
    needed = ['curr_docID', 'edinetCode', 'fiscalYear', *OUTCOMES, *terms]
    sample = eligible[needed].dropna().copy()
    if len(sample) < 1000 or sample['edinetCode'].nunique() < 100:
        raise ValueError(f'{analysis}: too few observations: {len(sample)}')
    if not np.all(sample['fiscalYear'] % 1 == 0):
        raise ValueError('Non-integer fiscal year')
    sample['fiscalYear'] = sample['fiscalYear'].astype(int).astype(str)
    sample['edinetCode'] = sample['edinetCode'].astype(str)
    if sample['curr_docID'].duplicated().any() or sample['curr_docID'].isna().any():
        raise ValueError('Duplicate/missing docID in shared sample')
    diagnostic = {
        'analysis': analysis, 'all_window_eligible_before_covariates': len(eligible),
        'shared_sample_n': len(sample), 'firms': int(sample['edinetCode'].nunique()),
        'years': int(sample['fiscalYear'].nunique()),
        'dropped_missing_covariates': len(eligible) - len(sample),
        'single_observation_firms': int((sample.groupby('edinetCode').size() == 1).sum()),
    }
    return sample, terms, diagnostic


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--run-dir', type=Path, default=DEFAULT_RUN)
    ap.add_argument('--panel', type=Path, default=DEFAULT_PANEL)
    ap.add_argument('--controls', type=Path, default=DEFAULT_CONTROLS)
    ap.add_argument('--output-dir', type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    for k in ('run_dir', 'panel', 'controls', 'output_dir'):
        setattr(args, k, getattr(args, k).expanduser())
    if args.output_dir.exists():
        raise FileExistsError(f'Refusing to overwrite existing output directory: {args.output_dir}')
    for path in (args.panel, args.controls, args.run_dir / 'three_component_sentiment.csv'):
        if not path.is_file():
            raise FileNotFoundError(path)

    scores, validation = validate_scores(args.run_dir)
    joined, coverage = load_panel(args.panel, args.controls, scores)
    models, coefficients, pairwise, day_tests, sample_diagnostics = [], [], [], [], []
    for analysis in ('complete_case', 'presence_indicators'):
        sample, terms, diagnostics = shared_sample(joined, analysis)
        sample_diagnostics.append(diagnostics)
        print(f"\n{analysis}: ONE identical sample for all event-day outcomes: "
              f"N={len(sample):,}, firms={diagnostics['firms']:,}", flush=True)
        # Each difference-in-day-sentiment-coefficients hypothesis is estimated
        # as a regression on the DIFFERENCE in daily abnormal returns. Since X,
        # FE and sample are identical, its t test incorporates cross-day covariance.
        for label, (a, b) in DAY_CONTRASTS.items():
            sample[f'ar_diff_{label}'] = sample[a] - sample[b]
        fit_outcomes = [*OUTCOMES, *(f'ar_diff_{x}' for x in DAY_CONTRASTS)]
        for mode in ('year_fe', 'firm_year_fe'):
            per_outcome = {}
            for outcome in fit_outcomes:
                m, c, p = estimate(sample, outcome, terms.copy(), mode, analysis, outcome)
                kind = 'day_difference_test' if outcome.startswith('ar_diff_') else 'outcome'
                m.update({'outcome': outcome, 'kind': kind})
                models.append(m)
                for row in c:
                    row.update({'outcome': outcome, 'kind': kind})
                    coefficients.append(row)
                for row in p:
                    row.update({'outcome': outcome, 'kind': kind})
                    pairwise.append(row)
                per_outcome[outcome] = {r['term']: r['coef'] for r in c}
                if kind == 'day_difference_test':
                    for r in c:
                        if r['term'] in SENTIMENT:
                            day_tests.append({'analysis': analysis, 'spec': mode,
                                              'day_contrast': outcome.replace('ar_diff_', ''),
                                              'sentiment': r['term'], 'difference': r['coef'],
                                              'se_cluster_firm': r['se_cluster_firm'],
                                              'p': r['p'], 'N': r['N'], 'firms': r['firms']})
            for term in SENTIMENT:
                b = per_outcome
                if not np.isclose(b['ar_pp_m1'][term] + b['ar_pp_0'][term] + b['ar_pp_p1'][term],
                                  b['car_pp_m1_1'][term], rtol=1e-8, atol=1e-8):
                    raise ValueError(f'Coefficient decomposition mismatch for {analysis}/{mode}/{term}')
                if not np.isclose(b['ar_pp_0'][term] + b['ar_pp_p1'][term],
                                  b['car_pp_0_1'][term], rtol=1e-8, atol=1e-8):
                    raise ValueError(f'[0,+1] coefficient decomposition mismatch for {term}')
            print(f'Coefficient sum identities passed for {analysis} / {mode}', flush=True)

    args.output_dir.mkdir(parents=True, exist_ok=False)
    pd.DataFrame(models).to_csv(args.output_dir / 'model_comparison.csv', index=False)
    pd.DataFrame(coefficients).to_csv(args.output_dir / 'coefficients.csv', index=False)
    pd.DataFrame(pairwise).to_csv(args.output_dir / 'component_pairwise_tests.csv', index=False)
    pd.DataFrame(day_tests).to_csv(args.output_dir / 'day_coefficient_difference_tests.csv', index=False)
    pd.DataFrame(sample_diagnostics).to_csv(args.output_dir / 'sample_diagnostics.csv', index=False)
    metadata = {
        'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'analysis': 'Stage 8F decomposition on identical observations across trading-day outcomes',
        'source_stage8e': str(args.run_dir),
        'sentiment_csv_sha256': sha256(args.run_dir / 'three_component_sentiment.csv'),
        'panel': str(args.panel), 'panel_sha256': sha256(args.panel),
        'controls': str(args.controls), 'controls_sha256': sha256(args.controls),
        'join_coverage': coverage, 'samples': sample_diagnostics,
        'day_definitions': {
            'ar_pp_m1': 'car_pp_m1_1 - car_pp_0_1',
            'ar_pp_0': 'car_pp_0_0',
            'ar_pp_p1': 'car_pp_0_1 - car_pp_0_0',
        },
        'time_reference': 'Stage 7 event trading day; not necessarily EDINET submission calendar day',
        'controls_terms': CONTROLS,
        'missing_components': 'complete-case primary; zero*x+presence-indicator robustness',
        'inference': 'firm-clustered CR1 and t with firms-1 degrees of freedom',
        'coefficient_day_tests': 'OLS of day AR difference on identical regressors/sample/FE; cross-day covariance included',
        'data_units': 'CAR and AR in percentage points; sentiment range [-1,1]',
        'checks': 'all three event-window flags, exact CAR/day addition, regression coefficient-sum identities',
        'limitations': 'exploratory; multiple tests; provisional raw-close Stage 8D market controls',
    }
    (args.output_dir / 'run_metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print(f'\nStage 8F outputs: {args.output_dir}', flush=True)


if __name__ == '__main__':
    main()
