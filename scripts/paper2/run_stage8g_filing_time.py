#!/usr/bin/env python3
"""Stage 8G: filing-time heterogeneity of Stage 8F daily sentiment/CAR associations.

Run from repo root with prior scripts installed in scripts/paper2:
    python -u scripts/paper2/run_stage8g_filing_time.py

Uses Stage 8D's *original EDINET submitDateTime* (JST naive) and Stage 7B's
historical TSE close: 15:00 through 2024-11-01; 15:30 from 2024-11-05.
Only actual trading-date alignment can distinguish an immediately tradeable
reaction from one delayed beyond the first eligible trading session; these
regressions alone do not establish causality or economic information novelty.

Outputs new files only. Firm-clustered CR1; exactly same observations across
all daily outcomes within a given missing-component analysis / timing stratum.
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
    BASE, DEFAULT_RUN, DEFAULT_PANEL, DEFAULT_CONTROLS,
    SENTIMENT, CONTROLS, validate_scores, load_panel, estimate,
)
from run_stage8f_event_day_decomposition import shared_sample

DEFAULT_OUT = BASE / 'regressions/stage8g_filing_time_firstlook'
DAILY = ('ar_pp_m1', 'ar_pp_0', 'ar_pp_p1')
OUTCOMES = (*DAILY, 'car_pp_0_1', 'car_pp_m1_1')
TIMING_COLS = ('curr_docID', 'submitDateTime')


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for b in iter(lambda: fh.read(1048576), b''):
            h.update(b)
    return h.hexdigest()


def filing_timing(controls_file):
    header = set(pd.read_csv(controls_file, nrows=0).columns)
    if not set(TIMING_COLS).issubset(header):
        raise ValueError('Stage 8D controls lack submitDateTime; need original EDINET filing timestamps. '
                         f'Columns available: {sorted(header)}')
    a = pd.read_csv(controls_file, usecols=list(TIMING_COLS),
                    dtype={'curr_docID': 'string', 'submitDateTime': 'string'}, low_memory=False)
    a['curr_docID'] = a['curr_docID'].str.strip()
    if a['curr_docID'].isna().any() or a['curr_docID'].duplicated().any():
        raise ValueError('Stage 8D filing-time keys missing or duplicated')
    raw = a['submitDateTime'].astype('string').str.strip()
    # EDINET timestamps are Japan-local; do not silently reinterpret as UTC.
    if raw.str.contains(r'(?:Z|[+-]\d\d:\d\d)$', case=False, regex=True, na=False).any():
        raise ValueError('Offset-aware EDINET timestamps encountered; verify timezone explicitly')
    d = pd.to_datetime(raw, errors='coerce')
    if d.isna().any():
        raise ValueError(f'{int(d.isna().sum())} missing/invalid submitDateTime values')
    a['submission_date'] = d.dt.normalize()
    a['submission_clock'] = d.dt.strftime('%H:%M:%S')
    # Close change at first full session after Nov 1, 2024; Nov 2-4 no TSE trading.
    a['close_clock'] = np.where(d.dt.date >= pd.Timestamp('2024-11-05').date(),
                                 '15:30:00', '15:00:00')
    if (d.dt.weekday >= 5).any():
        print(f'NOTE: {(d.dt.weekday >= 5).sum():,} weekend filings in Stage 8D; '
              'review against Stage 7B event-date assignment', flush=True)
    a['after_close'] = (a['submission_clock'] > a['close_clock']).astype(int)
    a['filing_timing'] = np.where(a['after_close'].eq(1), 'after_close', 'at_or_before_close')
    a['session_subtype'] = np.select([a['after_close'].eq(1),
                                     a['submission_clock'].lt('09:00:00')],
                                    ['after_close', 'before_open'], default='09:00_to_close')
    # Match the frozen Stage 7B eligible-event/timing totals; fail closed.
    if len(a) != 32126 or int(a['after_close'].sum()) != 10149:
        raise ValueError(
            f'Timing disagrees with Stage 7B: rows={len(a):,}, '
            f'after_close={int(a["after_close"].sum()):,}; '
            'expected 32,126 and 10,149. Check timestamp timezone and close rule.')
    return a


def subgroup_fit(sample, terms, analysis, spec, group):
    rows, coefs, contrasts = [], [], []
    for outcome in OUTCOMES:
        model, c, p = estimate(sample, outcome, terms.copy(), spec, analysis, outcome)
        model['timing'] = group
        rows.append(model)
        for item in c:
            item['timing'] = group
            coefs.append(item)
        for item in p:
            item['timing'] = group
            contrasts.append(item)
    return rows, coefs, contrasts


def pooled_interaction(sample, terms, analysis, spec):
    """Pooled within-firm timing interaction; all sentiment slopes interact.

    Controls have common slopes between timing groups. Interactions estimate
    conditional after-close minus pre-close differences with firm CR1 SE.
    """
    d = sample.copy()
    d['afterClose'] = d['after_close'].astype(float)
    # Group membership may be a constant for some firms; firm FE will absorb
    # its main effect where appropriate, as handled by estimate() rank check.
    # Use a centered timing indicator with within-firm variation, but drop
    # the timing main effect if it is not identified after FE absorption.
    interaction_terms = []
    for term in SENTIMENT:
        col = term + '_x_afterClose'
        d[col] = d[term] * d['afterClose']
        interaction_terms.append(col)
    model_terms = [*terms, *interaction_terms, 'afterClose']
    # A main-effect collinear with FE in a subgroup can be detected and
    # removed based on the rank of the absorbed regressor matrix.
    from run_stage8_twfe import absorb
    firms, fi = np.unique(d['edinetCode'].to_numpy(), return_inverse=True)
    _, yi = np.unique(d['fiscalYear'].to_numpy(), return_inverse=True)
    groups = [fi, yi] if spec == 'firm_year_fe' else [yi]
    x = d[model_terms].to_numpy(dtype=float)
    residualized, _, _ = absorb(x, groups)
    if np.linalg.matrix_rank(residualized) < len(model_terms):
        model_terms.remove('afterClose')
        if np.linalg.matrix_rank(residualized[:, :-1]) != len(model_terms):
            raise ValueError(f'Pooled timing interactions rank-deficient: {analysis}/{spec}')
        print(f'{analysis}/{spec}: afterClose main effect absorbed; omitted', flush=True)
    all_models, all_coefs = [], []
    for outcome in OUTCOMES:
        model, coefs, _ = estimate(d, outcome, model_terms.copy(), spec,
                                   analysis, outcome)
        model['timing'] = 'pooled_interaction'
        all_models.append(model)
        for c in coefs:
            c['timing'] = 'pooled_interaction'
            all_coefs.append(c)
        lookup = {c['term']: c for c in coefs}
        print(f'{analysis:18} {spec:12} {outcome:12} '
              f'N={len(d):,}  after-close Δβ_new='
              f'{lookup["newSentiment_x_afterClose"]["coef"]:+.4f} '
              f'(p={lookup["newSentiment_x_afterClose"]["p"]:.4g})', flush=True)
    return all_models, all_coefs


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-dir', type=Path, default=DEFAULT_RUN)
    p.add_argument('--panel', type=Path, default=DEFAULT_PANEL)
    p.add_argument('--controls', type=Path, default=DEFAULT_CONTROLS)
    p.add_argument('--output-dir', type=Path, default=DEFAULT_OUT)
    p.add_argument('--no-pooled-interaction', action='store_true')
    args = p.parse_args()
    for k in ('run_dir', 'panel', 'controls', 'output_dir'):
        setattr(args, k, getattr(args, k).expanduser())
    if args.output_dir.exists():
        raise FileExistsError(f'Refusing to overwrite existing output dir: {args.output_dir}')
    scores, validation = validate_scores(args.run_dir)
    joined, coverage = load_panel(args.panel, args.controls, scores)
    timing = filing_timing(args.controls)
    joined = joined.merge(timing, on='curr_docID', how='left', validate='one_to_one', indicator='timing_join')
    print(f'Timing matched: {joined.timing_join.eq("both").sum():,}/{len(joined):,}', flush=True)
    print('Raw Stage 8D timestamps by close:', timing['filing_timing'].value_counts().to_dict(), flush=True)
    print('Session subtypes:', timing['session_subtype'].value_counts().to_dict(), flush=True)
    models, coefficients, pairwise, sample_stats = [], [], [], []
    for analysis in ('complete_case', 'presence_indicators'):
        sample, terms, diagnostics = shared_sample(joined, analysis)
        if sample['curr_docID'].duplicated().any():
            raise ValueError('Unexpected duplicate sample keys')
        sample = sample.merge(timing[['curr_docID', 'after_close', 'filing_timing', 'session_subtype']],
                              on='curr_docID', how='left', validate='one_to_one')
        if sample['after_close'].isna().any():
            raise ValueError(f'Missing submission timestamps in {analysis} regression sample')
        print(f'\n{analysis}: common daily sample N={len(sample):,}; by timing: '
              f'{sample.filing_timing.value_counts().to_dict()}', flush=True)
        for group in ('at_or_before_close', 'after_close'):
            d = sample.loc[sample.filing_timing.eq(group)].copy()
            if len(d) < 1000 or d.edinetCode.nunique() < 100:
                raise ValueError(f'Timing subgroup too small: {analysis}/{group}: {len(d)}')
            sample_stats.append({'analysis':analysis, 'group':group, 'N':len(d),
                                 'firms':int(d.edinetCode.nunique()),
                                 'before_open_n':int(d.session_subtype.eq('before_open').sum()),
                                 'intraday_n':int(d.session_subtype.eq('09:00_to_close').sum()),
                                 'after_close_n':int(d.session_subtype.eq('after_close').sum())})
            for spec in ('year_fe', 'firm_year_fe'):
                m,c,pairs = subgroup_fit(d, terms, analysis, spec, group)
                models.extend(m); coefficients.extend(c); pairwise.extend(pairs)
        if not args.no_pooled_interaction:
            for spec in ('year_fe', 'firm_year_fe'):
                m,c = pooled_interaction(sample, terms, analysis, spec)
                models.extend(m); coefficients.extend(c)

    args.output_dir.mkdir(parents=True, exist_ok=False)
    pd.DataFrame(models).to_csv(args.output_dir/'model_comparison.csv',index=False)
    pd.DataFrame(coefficients).to_csv(args.output_dir/'coefficients.csv',index=False)
    pd.DataFrame(pairwise).to_csv(args.output_dir/'component_pairwise_tests.csv',index=False)
    pd.DataFrame(sample_stats).to_csv(args.output_dir/'timing_sample_counts.csv',index=False)
    meta = {'created_utc': datetime.now(timezone.utc).isoformat(),
            'sentiment_run':str(args.run_dir), 'sentiment_sha256':digest(args.run_dir/'three_component_sentiment.csv'),
            'controls_sha256':digest(args.controls),'panel_sha256':digest(args.panel),
            'timing_source':'Stage 8D original submitDateTime, Japan local wall clock',
            'close_rule':'15:00 through 2024-11-01, 15:30 since 2024-11-05',
            'grouping':'at-or-before-close versus after-close (includes preopen filings in first group)',
            'interpretation':'TSE close stratification, not proof of first tradable session or new economic information',
            'outcomes':'Stage 8F identical-sample AR days -1,0,+1 and CAR[0,+1],[-1,+1]',
            'inference':'firm-clustered CR1 with firm-count t degrees of freedom',
            'pooled_interactions':'afterClose * each sentiment, other control slopes held common; pooled firm/year FE',
            'covariates':CONTROLS, 'samples':sample_stats,'join':coverage,
            'limitations':'multiple testing, subgroup selection, first-trade timing and provisional raw-close market controls'}
    (args.output_dir/'run_metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
    print(f'\nStage 8G outputs: {args.output_dir}',flush=True)

if __name__=='__main__':
    main()
