#!/usr/bin/env python3
"""Read-only Paper 2 Stage 8I attrition audit.

Reuses the FROZEN Stage 8E score validation and Stage 8F shared-sample
construction rather than creating a new regression sample definition.
Run from repository root:

    python scripts/paper2/audit_stage8i_sample_attrition.py

No LLM/API calls. Writes only a NEW dedicated diagnostic output directory;
never overwrites Stage 6-8I data or frozen results.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import pandas as pd

BASE = Path('data/interim/paper2')
DEFAULT_RUN = BASE / 'sentiment/three_component_stage8e/runs/20261019T021943726283Z_4f2fbd28'
DEFAULT_PANEL = Path('~/paper2_local/data/interim/paper2/analysis/stage8_panel/stage8_filing_panel.csv').expanduser()
DEFAULT_H = BASE / 'controls/stage8h_adjusted_market_industry_event_controls.csv'
DEFAULT_D = BASE / 'controls/stage8d_market_industry_event_controls.csv'
DEFAULT_OUT = BASE / 'regressions/stage8i_sample_attrition_audit'
EXPECTED = {'panel': 33046, 'joint_window': 30926, 'presence': 29227, 'complete': 21540}


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(2 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def assert_same_set(a, b, label):
    sa, sb = set(a), set(b)
    if sa != sb:
        raise AssertionError(f'{label}: {len(sa-sb)} unexpected / {len(sb-sa)} missing document IDs')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--panel', type=Path, default=DEFAULT_PANEL)
    p.add_argument('--run-dir', type=Path, default=DEFAULT_RUN)
    p.add_argument('--stage8h-controls', type=Path, default=DEFAULT_H)
    p.add_argument('--stage8d-controls', type=Path, default=DEFAULT_D)
    p.add_argument('--output-dir', type=Path, default=DEFAULT_OUT)
    p.add_argument('--allow-count-drift', action='store_true', help='Do not assert the frozen Oct 20 Stage 8I counts')
    args = p.parse_args()
    for field in ('panel','run_dir','stage8h_controls','stage8d_controls','output_dir'):
        setattr(args, field, getattr(args, field).expanduser())
    if args.output_dir.exists():
        p.error(f'Output exists: {args.output_dir}. Supply a NEW --output-dir (no overwrite).')
    for path in (args.panel, args.stage8h_controls, args.run_dir/'three_component_sentiment.csv', args.run_dir/'manifest.json'):
        if not path.is_file():
            p.error(f'Missing frozen input: {path}')

    # These modules are already in scripts/paper2 in the validated repository.
    from run_stage8e_validate_regressions import (
        validate_scores, load_panel, WINDOWS, event_flag, CONTROLS, SENTIMENT)
    from run_stage8f_event_day_decomposition import shared_sample

    scores, validation = validate_scores(args.run_dir)
    joined_h, coverage_h = load_panel(args.panel, args.stage8h_controls, scores)
    complete, _, _ = shared_sample(joined_h, 'complete_case')
    presence, _, _ = shared_sample(joined_h, 'presence_indicators')
    full_ids = set(joined_h.curr_docID)
    complete_ids = set(complete.curr_docID)
    presence_ids = set(presence.curr_docID)

    windows = pd.Series(True, index=joined_h.index)
    for flag, outcome in WINDOWS.values():
        windows &= event_flag(joined_h[flag]) & joined_h[outcome].notna()
    idcols = ['edinetCode', 'fiscalYear']
    ctrl_ok = joined_h[idcols + CONTROLS].notna().all(axis=1)
    all_scores = joined_h[SENTIMENT].notna().all(axis=1)
    eligible_ids = set(joined_h.loc[windows, 'curr_docID'])
    expected_presence_ids = set(joined_h.loc[windows & ctrl_ok, 'curr_docID'])
    expected_complete_ids = set(joined_h.loc[windows & ctrl_ok & all_scores, 'curr_docID'])
    assert_same_set(expected_presence_ids, presence_ids, 'Presence-indicator sample vs no-missing-controls rule')
    assert_same_set(expected_complete_ids, complete_ids, 'Complete-case sample vs three-scores rule')
    assert complete_ids <= presence_ids

    n = {'panel': len(full_ids), 'joint_window': len(eligible_ids),
         'presence': len(presence_ids), 'complete': len(complete_ids)}
    if not args.allow_count_drift and n != EXPECTED:
        raise AssertionError(f'Frozen October 20 counts differ: actual={n}, expected={EXPECTED}')
    if len(full_ids) != len(joined_h):
        raise AssertionError('Duplicate filing IDs in joined dataset')

    d = joined_h[['curr_docID', 'edinetCode', 'fiscalYear', 'car_pp_m1_1']].copy()
    d['joint_event_window_valid'] = windows
    d['control_complete'] = ctrl_ok
    for s in SENTIMENT:
        d[s+'Present'] = joined_h[s].notna()
    d['all_three_present'] = all_scores
    d['in_presence_sample'] = d.curr_docID.isin(presence_ids)
    d['in_complete_case_sample'] = d.curr_docID.isin(complete_ids)
    d['missing_control_count'] = joined_h[CONTROLS].isna().sum(axis=1)
    d['exclusion_reason'] = 'included_complete_case'
    d.loc[windows & ctrl_ok & ~all_scores,'exclusion_reason'] = 'one_or_more_sentiment_components_absent'
    d.loc[windows & ~ctrl_ok,'exclusion_reason'] = 'control_or_firm_year_missing'
    d.loc[~windows,'exclusion_reason'] = 'joint_event_window_unavailable'

    # All four reason classes are exclusive and exhaustive across 33,046 pairs.
    groups = d.exclusion_reason.value_counts().to_dict()
    assert sum(groups.values()) == len(d)
    assert groups.get('included_complete_case',0) == n['complete']
    assert groups.get('one_or_more_sentiment_components_absent',0) == n['presence'] - n['complete']
    assert groups.get('control_or_firm_year_missing',0) == n['joint_window'] - n['presence']
    assert groups.get('joint_event_window_unavailable',0) == n['panel'] - n['joint_window']

    audited = []
    for name,count,baseline in [
        ('Stage 1: annual reports',37807,37807),
        ('Stage 2: extracted MD&A',37757,37807),
        ('Stage 3: standard annual disclosure pairs',33046,37757),
        ('Stage 7A: market eligible',32126,33046),
        ('Stage 7E/8I: joint event window valid',n['joint_window'],32126),
        ('Stage 8I: event window + controls complete',n['presence'],n['joint_window']),
        ('Stage 8I: all three scores present',n['complete'],n['presence']),
    ]:
        audited.append({'step':name,'n':count,'loss_from_prior':baseline-count,
                        'pct_of_initial_37807':round(100*count/37807,3)})

    eligible = joined_h.loc[windows].copy()
    excluded_ctrl = joined_h.loc[windows & ~ctrl_ok].copy()
    miss_ctrl = []
    for col in idcols + CONTROLS:
        miss_ctrl.append({'variable':col,
              'missing_in_all_eligible':int(eligible[col].isna().sum()),
              'missing_among_1699_excluded':int(excluded_ctrl[col].isna().sum()),
              'control_complete_required':True})
    missing_masks = joined_h.loc[windows & ~ctrl_ok, idcols+CONTROLS].isna()
    control_patterns = (missing_masks.apply(lambda row: ', '.join(row.index[row.values]), axis=1)
                        .value_counts().rename_axis('missing_variables').reset_index(name='n'))

    has_ctrl = joined_h.loc[windows & ctrl_ok].copy()
    code = has_ctrl[SENTIMENT].notna().apply(lambda row: ''.join('1' if x else '0' for x in row.values), axis=1)
    presence_patterns = (code.value_counts().rename_axis('unchanged_revised_new_presence')
                         .reset_index(name='n'))
    presence_patterns['pct_control_complete'] = (presence_patterns.n / len(has_ctrl) * 100).round(3)

    # Year distribution allows researchers to assess temporal selection.
    d['year'] = pd.to_numeric(d.fiscalYear, errors='coerce').astype('Int64')
    years = (d.groupby(['year','exclusion_reason'], dropna=False).size().unstack(fill_value=0)
               .reset_index())
    years['total_research_pairs'] = years.drop(columns='year').sum(axis=1)
    ycol = [x for x in years if x not in ('year','total_research_pairs')]
    years = years[['year','total_research_pairs',*ycol]]

    selection_profile = (d.groupby('exclusion_reason',dropna=False)
                         .agg(documents=('curr_docID','count'),firms=('edinetCode','nunique'),
                              mean_car_pp_m1_1=('car_pp_m1_1','mean'))
                         .reset_index())

    # Optional previous-version comparison: do not equate net N change with IDs lost.
    comparisons=[]
    if args.stage8d_controls.is_file():
        joined_d,_ = load_panel(args.panel,args.stage8d_controls,scores)
        for analysis in ('complete_case','presence_indicators'):
            old,_,_ = shared_sample(joined_d,analysis)
            old_ids = set(old.curr_docID)
            new_ids = complete_ids if analysis=='complete_case' else presence_ids
            comparisons.append({'analysis':analysis,'stage8d_n':len(old_ids),'stage8h_n':len(new_ids),
                 'lost_on_8d_to_8h':len(old_ids-new_ids),
                 'gained_on_8d_to_8h':len(new_ids-old_ids),
                 'retained_in_both':len(old_ids & new_ids),
                 'net_change':len(new_ids)-len(old_ids)})

    # Audit outputs: leave all canonical inputs untouched.
    args.output_dir.mkdir(parents=True,exist_ok=False)
    for filename,frame in [
        ('attrition_funnel.csv',pd.DataFrame(audited)),
        ('filing_level_attrition.csv',d),
        ('missing_controls.csv',pd.DataFrame(miss_ctrl)),
        ('missing_control_patterns.csv',control_patterns),
        ('component_presence_patterns.csv',presence_patterns),
        ('attrition_by_fiscal_year.csv',years),
        ('selection_profile.csv',selection_profile),
        ('stage8d_to_8h_sample_migration.csv',pd.DataFrame(comparisons)),
    ]:
        frame.to_csv(args.output_dir/filename,index=False)
    input_paths = [args.panel, args.stage8h_controls,
                   args.run_dir/'three_component_sentiment.csv',args.run_dir/'manifest.json']
    if args.stage8d_controls.is_file():
        input_paths.append(args.stage8d_controls)
    provenance = {
        'purpose':'descriptive sample attrition, no regression refit, no model selection',
        'inputs':{str(path):digest(path) for path in input_paths},
        'counts':n,'exclusion_reasons':groups,'stage8h_coverage':coverage_h,
        'frozen_stage8e_validation':validation,
        'limitations':['Original Stage 1-3 and Stage 7A counts are carried from the frozen documentation, not recomputed here.',
                       'Exclusion categories are SEQUENTIAL, and individual variable missingness overlaps.',
                       'Comparing Stage 8D and 8H samples requires filing-ID overlap, not just subtracting N.']}
    (args.output_dir/'audit_manifest.json').write_text(json.dumps(provenance,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
    print('\n=== STAGE 8I ATTRITION FUNNEL ===')
    print(pd.DataFrame(audited).to_string(index=False))
    print('\n=== WHY JOINT EVENT-WINDOW ROWS DO NOT ENTER COMPLETE CASES ===')
    print(f"Missing regression controls or firm/year: {groups['control_or_firm_year_missing']:,}")
    print(f"Controls available, component(s) absent: {groups['one_or_more_sentiment_components_absent']:,}")
    print(f"All regression information available:    {groups['included_complete_case']:,}")
    print('\n=== MISSING CONTROL VARIABLES ===')
    print(pd.DataFrame(miss_ctrl).to_string(index=False))
    print('\n=== COMPONENT PRESENCE PATTERNS (U R N) ===')
    print(presence_patterns.to_string(index=False))
    if comparisons:
        print('\n=== STAGE 8D TO STAGE 8H FILING-ID MIGRATION ===')
        print(pd.DataFrame(comparisons).to_string(index=False))
    print(f'\nAudit written to: {args.output_dir}')


if __name__ == '__main__':
    main()
