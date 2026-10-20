#!/usr/bin/env python3
"""Paper 2 Stage 8I: read-only pre-filing 20-day momentum sensitivity.

Compares (1) frozen Stage 8H specification on its current sample,
(2) removing momentum on that SAME sample, and (3) removing momentum
and admitting observations with otherwise complete controls. Both
complete-case and presence-indicator designs are reported. Uses the
existing Stage 8E regression estimator, firm+fiscal-year fixed effects,
and firm-clustered CR1 inference. No changes to frozen inputs.

Run from repo root:
  python -u scripts/paper2/analyze_stage8i_momentum.py

Default output location is new, never overwritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(2 * 1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    from run_stage8e_validate_regressions import (
        BASE, DEFAULT_RUN, DEFAULT_PANEL, SENTIMENT, CONTROLS, WINDOWS,
        validate_scores, load_panel, event_flag, estimate,
    )
    from run_stage8f_event_day_decomposition import shared_sample

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--panel', type=Path, default=DEFAULT_PANEL)
    p.add_argument('--run-dir', type=Path, default=DEFAULT_RUN)
    p.add_argument('--controls', type=Path, default=BASE/'controls/stage8h_adjusted_market_industry_event_controls.csv')
    p.add_argument('--output-dir', type=Path, default=BASE/'regressions/stage8i_momentum_sensitivity')
    args = p.parse_args()
    for key in ('panel', 'run_dir', 'controls', 'output_dir'):
        setattr(args, key, getattr(args, key).expanduser())
    required = [args.panel, args.controls, args.run_dir/'three_component_sentiment.csv',
                args.run_dir/'manifest.json']
    for path in required:
        if not path.is_file():
            p.error(f'Missing frozen input: {path}')
    if args.output_dir.exists():
        p.error(f'Output exists: {args.output_dir}; use a new --output-dir. Never overwrite frozen results.')
    if CONTROLS.count('momentum_20d') != 1:
        raise AssertionError('Expected exactly one momentum_20d in frozen controls')
    less_controls = [x for x in CONTROLS if x != 'momentum_20d']
    if len(less_controls) != 7:
        raise AssertionError(f'Unexpected number of remaining controls: {len(less_controls)}')

    scores, validated = validate_scores(args.run_dir)
    joined, coverage = load_panel(args.panel, args.controls, scores)
    valid = pd.Series(True, index=joined.index)
    for flag, outcome in WINDOWS.values():
        valid &= event_flag(joined[flag]) & joined[outcome].notna()
    eligible = joined.loc[valid].copy()
    eligible['ar_pp_p1'] = eligible['car_pp_0_1'] - eligible['car_pp_0_0']
    if len(eligible) != 30926:
        raise AssertionError(f'Frozen event-window sample changed: {len(eligible)}')

    model_rows, coef_rows, difference_rows, size_rows = [], [], [], []
    recoverable = []
    missing_diag = []

    for analysis in ('complete_case', 'presence_indicators'):
        original, original_terms, _ = shared_sample(joined, analysis)
        baseline_ids = set(original.curr_docID)

        # Build no-momentum candidates with the EXACT same joint 3-window
        # outcome requirement as Stage 8F, and its same presence treatment.
        e = eligible.copy()
        if analysis == 'presence_indicators':
            for col in SENTIMENT:
                e[col+'Present'] = e[col].notna().astype(float)
                e[col] = e[col].fillna(0.0)
            terms = SENTIMENT + [col+'Present' for col in SENTIMENT] + less_controls
        else:
            terms = SENTIMENT + less_controls
        needed = ['curr_docID', 'edinetCode', 'fiscalYear',
                  'car_pp_m1_1', 'ar_pp_p1', *terms]
        expanded = e[needed].dropna().copy()
        if expanded.curr_docID.duplicated().any():
            raise AssertionError('Duplicate document IDs in no-momentum sample')
        expanded['fiscalYear'] = expanded['fiscalYear'].astype(int).astype(str)
        expanded['edinetCode'] = expanded['edinetCode'].astype(str)
        expanded_ids = set(expanded.curr_docID)
        if not baseline_ids <= expanded_ids:
            raise AssertionError('Dropping momentum unexpectedly removed existing observations')
        if len(original) != (21540 if analysis=='complete_case' else 29227):
            raise AssertionError(f'Frozen {analysis} N changed: {len(original)}')
        new_ids = expanded_ids - baseline_ids
        regained = eligible[eligible.curr_docID.isin(new_ids)].copy()
        if not regained.momentum_20d.isna().all():
            raise AssertionError('Regained documents must have had missing momentum')
        if len(expanded) != len(original) + len(new_ids):
            raise AssertionError('Expanded sample does not reconcile')
        size_rows.append({'analysis':analysis,'original_n':len(original),
                          'no_momentum_n':len(expanded), 'regained_n':len(new_ids),
                          'original_firms':original.edinetCode.nunique(),
                          'no_momentum_firms':expanded.edinetCode.nunique()})
        for docid in sorted(new_ids):
            recoverable.append({'analysis':analysis, 'curr_docID':docid})

        # Same-sample no-momentum regression isolates the effect of dropping
        # that regressor; expanded-sample regressions mix coefficient and N changes.
        cases = [
            ('original_8_controls_original_sample', original, original_terms),
            ('remove_momentum_same_sample', original, terms),
            ('remove_momentum_expanded_sample', expanded, terms),
        ]
        for case, sample, use_terms in cases:
            for outcome in ('car_pp_m1_1', 'ar_pp_p1'):
                label = f'{case}:{outcome}'
                print(f'\n-- {analysis} / {label} --',flush=True)
                model, coefs, contrasts = estimate(sample, outcome, use_terms.copy(),
                                                   'firm_year_fe', analysis, label)
                model_rows.append({'scenario':case,'outcome':outcome, **model})
                coef_rows.extend({'scenario':case,'outcome':outcome,**row} for row in coefs)
                difference_rows.extend({'scenario':case,'outcome':outcome,**row} for row in contrasts)

    # Inspection for missing momentum: these columns may be present in the
    # complete stage8h controls output but are not required for sensitivity.
    ctrl_header = pd.read_csv(args.controls, nrows=0).columns.tolist()
    optional = [x for x in ('market_date','market_chunk_history_sessions',
                            'market_valid_returns_60d', 'volatility_60d',
                            'momentum_60_to_5d','jquants_code') if x in ctrl_header]
    if optional:
        tmp = pd.read_csv(args.controls,
                          usecols=['curr_docID','momentum_20d',*optional],
                          dtype={'curr_docID':'string'}, low_memory=False)
        tmp['curr_docID']=tmp.curr_docID.str.strip()
        subset = tmp[tmp.curr_docID.isin(eligible.curr_docID) &
                     pd.to_numeric(tmp.momentum_20d,errors='coerce').isna()].copy()
        for x in optional:
            if x not in ('market_date','jquants_code'):
                subset[x]=pd.to_numeric(subset[x],errors='coerce')
        missing_diag = subset

    args.output_dir.mkdir(parents=True, exist_ok=False)
    for name, rows in [
        ('sample_sizes.csv', size_rows),
        ('model_summary.csv', model_rows),
        ('coefficients.csv', coef_rows),
        ('coefficient_contrasts.csv', difference_rows),
        ('regained_filing_ids.csv', recoverable),
    ]:
        pd.DataFrame(rows).to_csv(args.output_dir/name,index=False)
    if isinstance(missing_diag,pd.DataFrame):
        missing_diag.to_csv(args.output_dir/'missing_momentum_event_snapshot.csv',index=False)
    manifest = {
        'created_utc':datetime.now(timezone.utc).isoformat(),
        'purpose':'momentum deletion sensitivity; NOT a replacement Stage 8I main specification',
        'inputs':{str(path):sha256(path) for path in required},
        'stage8e_validated_documents':validated.get('documents'),
        'coverage':coverage, 'original_controls':CONTROLS,
        'removed_control':'momentum_20d',
        'outcomes':['car_pp_m1_1','ar_pp_p1'],
        'statistical_inference':'frozen Stage 8E firm+year FE and firm-clustered CR1',
        'model_scenarios':['original_8_controls_original_sample',
                           'remove_momentum_same_sample',
                           'remove_momentum_expanded_sample'],
        'sample_sizes':size_rows,
        'interpretation':['Compare original versus remove_momentum_same_sample for conditional contribution of momentum.',
                          'Compare same_sample versus expanded_sample for sample-composition effects.',
                          'A larger sample does not by itself justify deleting a theoretically useful covariate.',
                          'This is exploratory sensitivity, not a preregistered primary model selection.',
                          'Missing momentum is not imputed or silently bridged.'],
    }
    (args.output_dir/'run_metadata.json').write_text(json.dumps(manifest,indent=2,default=str)+'\n',encoding='utf-8')
    print('\n=== SAMPLE RECOVERY WITHOUT MOMENTUM ===')
    print(pd.DataFrame(size_rows).to_string(index=False))
    print('\n=== PRIMARY CAR[-1,+1] SENSITIVITY ===')
    summaries = pd.DataFrame(coef_rows)
    summaries = summaries[(summaries.outcome=='car_pp_m1_1') &
                          (summaries.term.isin(['momentum_20d','newSentiment','revisedSentiment','unchangedSentiment']))]
    print(summaries[['analysis','scenario','term','N','coef','p']].to_string(index=False))
    print('\n=== CAR[-1,+1] DIRECT NEW-MINUS-OTHER CONTRASTS ===')
    dif = pd.DataFrame(difference_rows)
    dif = dif[(dif.outcome=='car_pp_m1_1') &
              (dif.contrast.str.startswith('newSentiment'))]
    print(dif[['analysis','scenario','contrast','N','difference','p']].to_string(index=False))
    print(f'\nOutputs: {args.output_dir}')


if __name__ == '__main__':
    main()
