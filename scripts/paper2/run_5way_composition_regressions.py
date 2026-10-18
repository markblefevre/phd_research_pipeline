#!/usr/bin/env python3
"""Exploratory Paper 2 disclosure-composition regressions (no API calls).

Reads the five-way collected document summary and the existing Stage 8 filing panel.
Creates new, isolated regression outputs; does not change either input.

Requires: pandas, numpy, statsmodels.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

ROOT = Path('data/interim/paper2')
DEFAULT_CLASSIFICATION = ROOT / 'alignment/disclosure_change_5way_v1/collected_repaired_prompt_v2/disclosure_change_2group_documents.csv'
DEFAULT_PANEL = ROOT / 'analysis/stage8_panel/stage8_filing_panel.csv'
DEFAULT_OUTPUT = ROOT / 'alignment/disclosure_change_5way_v1/exploratory_composition_v1'
WINDOWS = {
    '0_0': ('[0,0]', 'eventStudySample_0_0', 'car_pp_0_0', 'car_0_0'),
    '0_1': ('[0,+1]', 'eventStudySample_0_1', 'car_pp_0_1', 'car_0_1'),
    'm1_1': ('[-1,+1]', 'eventStudySample_m1_1', 'car_pp_m1_1', 'car_m1_1'),
}
COUNT_COLS = ['unchangedSentenceCount', 'revisedSentenceCount', 'newCandidateSentenceCount',
              'nonSubstantiveSentenceCount', 'uncertainSentenceCount', 'substantiveSentenceCount']


def read_classification(path: Path) -> pd.DataFrame:
    cols = pd.read_csv(path, nrows=0).columns.tolist()
    required = ['curr_docID', *COUNT_COLS]
    missing = sorted(set(required) - set(cols))
    if missing:
        raise ValueError(f'Five-way document summary missing columns {missing}. Available: {cols}')
    df = pd.read_csv(path, usecols=required, dtype={'curr_docID': 'string'}, low_memory=False)
    df['curr_docID'] = df['curr_docID'].str.strip()
    if df['curr_docID'].isna().any() or df['curr_docID'].duplicated().any():
        raise ValueError('Five-way document summary has null or duplicate curr_docID values.')
    for c in COUNT_COLS:
        df[c] = pd.to_numeric(df[c], errors='raise')
    if (df[COUNT_COLS] < 0).any().any():
        raise ValueError('Negative sentence counts in five-way document summary.')
    expected = df['unchangedSentenceCount'] + df['revisedSentenceCount'] + df['newCandidateSentenceCount']
    if not (expected == df['substantiveSentenceCount']).all():
        raise ValueError('Substantive sentence counts do not equal unchanged + revised + new.')
    df['unchangedShare'] = np.where(expected > 0, df['unchangedSentenceCount'] / expected, np.nan)
    df['revisedShare'] = np.where(expected > 0, df['revisedSentenceCount'] / expected, np.nan)
    df['newShare'] = np.where(expected > 0, df['newCandidateSentenceCount'] / expected, np.nan)
    df['changedShare'] = df['revisedShare'] + df['newShare']
    return df[['curr_docID', 'substantiveSentenceCount', 'unchangedShare', 'revisedShare', 'newShare', 'changedShare']]


def read_panel(path: Path) -> pd.DataFrame:
    cols = set(pd.read_csv(path, nrows=0).columns)
    required = ['curr_docID', 'edinetCode', 'fiscalYear', 'noveltyCNum', 'logLengthChange']
    missing = sorted(set(required) - cols)
    if missing:
        raise ValueError(f'Stage 8 panel missing required columns: {missing}')
    wanted = set(required)
    for _, sample, pp, raw in WINDOWS.values():
        if pp not in cols and raw not in cols:
            raise ValueError(f'Neither {pp} nor {raw} is present in Stage 8 panel')
        wanted.update(c for c in (sample, pp, raw) if c in cols)
    df = pd.read_csv(path, usecols=list(wanted), dtype={'curr_docID': 'string', 'edinetCode': 'string'}, low_memory=False)
    df['curr_docID'] = df['curr_docID'].str.strip()
    df['edinetCode'] = df['edinetCode'].str.strip()
    if df['curr_docID'].isna().any() or df['curr_docID'].duplicated().any():
        raise ValueError('Stage 8 panel has null or duplicate curr_docID values; inspect before merging.')
    for c in ['fiscalYear', 'noveltyCNum', 'logLengthChange']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    for _, _, pp, raw in WINDOWS.values():
        from_pp = pd.to_numeric(df[pp], errors='coerce') if pp in df else pd.Series(np.nan, index=df.index)
        from_raw = pd.to_numeric(df[raw], errors='coerce') * 100 if raw in df else pd.Series(np.nan, index=df.index)
        df[f'y_{pp}'] = from_pp.fillna(from_raw)
    return df


def is_true(series: pd.Series) -> pd.Series:
    return series.astype('string').str.strip().str.lower().isin(['true', '1', 'yes'])


def fit_one(frame: pd.DataFrame, label: str, spec: str, y_col: str, formula_rhs: str,
            required_vars: list[str]):
    cols = ['edinetCode', 'fiscalYear', y_col, *required_vars]
    data = frame[cols].copy().replace([np.inf, -np.inf], np.nan).dropna()
    data = data[data['edinetCode'].astype(str).str.len() > 0].copy()
    data['fiscalYear'] = data['fiscalYear'].astype(int).astype(str)
    if len(data) < 100 or data['edinetCode'].nunique() < 20:
        raise ValueError(f'Too few observations/clusters for {label}/{spec}: {len(data)} rows')
    model = smf.ols(f'{y_col} ~ {formula_rhs} + C(fiscalYear)', data=data).fit(
        cov_type='cluster', cov_kwds={'groups': data['edinetCode'], 'use_correction': True})
    equality_p = None
    if 'revisedShare' in required_vars and 'newShare' in required_vars:
        equality_p = float(model.t_test('revisedShare = newShare').pvalue)
    rows = []
    for var in required_vars:
        rows.append({
            'window': label, 'spec': spec, 'variable': var,
            'N': int(model.nobs), 'firms': int(data['edinetCode'].nunique()),
            'R2': float(model.rsquared),
            'coef_per_full_share': float(model.params[var]),
            'coef_per_10pp_share': float(model.params[var] * 0.1) if var.endswith('Share') else np.nan,
            'se_per_10pp_share': float(model.bse[var] * 0.1) if var.endswith('Share') else np.nan,
            'pvalue': float(model.pvalues[var]),
            'p_revised_equals_new': equality_p,
        })
    return rows, model, len(data)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--classification', type=Path, default=DEFAULT_CLASSIFICATION)
    p.add_argument('--panel', type=Path, default=DEFAULT_PANEL)
    p.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT)
    args = p.parse_args()
    for path in [args.classification, args.panel]:
        if not path.is_file():
            raise FileNotFoundError(f'Input missing: {path}')
    if args.output_dir.exists():
        raise FileExistsError(f'Output directory already exists (refusing overwrite): {args.output_dir}')

    classification = read_classification(args.classification)
    panel = read_panel(args.panel)
    joined = panel.merge(classification, on='curr_docID', how='left', validate='one_to_one', indicator=True)
    n_matched = int((joined['_merge'] == 'both').sum())
    if n_matched < 25000:
        raise ValueError(f'Only {n_matched:,} filing pairs matched. Check curr_docID keys before proceeding.')
    diagnostics = {
        'classification_input': str(args.classification),
        'panel_input': str(args.panel),
        'five_way_pairs': int(len(classification)),
        'stage8_panel_rows': int(len(panel)),
        'matched_filing_pairs': n_matched,
        'unmatched_panel_rows': int((joined['_merge'] == 'left_only').sum()),
        'matched_pairs_without_substantive_sentences': int(((joined['_merge'] == 'both') & (joined['substantiveSentenceCount'] == 0)).sum()),
        'car_units': 'percentage points; uses car_pp if available, otherwise 100 * car',
        'share_denominator': 'unchanged + revised + new_candidate sentence counts',
        'standard_errors': 'clustered by edinetCode',
        'fixed_effects': 'fiscal year',
    }
    specs = [
        ('shares_year_fe', 'revisedShare + newShare', ['revisedShare', 'newShare']),
        ('shares_controls_year_fe', 'revisedShare + newShare + noveltyCNum + logLengthChange',
         ['revisedShare', 'newShare', 'noveltyCNum', 'logLengthChange']),
        ('pooled_changed_controls_year_fe', 'changedShare + noveltyCNum + logLengthChange',
         ['changedShare', 'noveltyCNum', 'logLengthChange']),
    ]
    args.output_dir.mkdir(parents=True, exist_ok=False)
    all_rows = []
    for _, (label, sample_flag, pp, _) in WINDOWS.items():
        frame = joined.copy()
        if sample_flag in frame:
            frame = frame[is_true(frame[sample_flag])].copy()
        diagnostics[f'eligible_rows_{label}'] = int(len(frame))
        y = f'y_{pp}'
        for spec, rhs, vars_ in specs:
            rows, result, n = fit_one(frame, label, spec, y, rhs, vars_)
            all_rows.extend(rows)
            (args.output_dir / f'regression_{label.replace("[", "").replace("]", "").replace(",", "_").replace("+", "p").replace("-", "m")}_{spec}.txt').write_text(
                result.summary().as_text() + '\n', encoding='utf-8')
            print(f'{label:<8} {spec:<34} N={n:>6,}  R2={result.rsquared:.4f}')
            for row in rows:
                if row['variable'].endswith('Share'):
                    print(f'    {row["variable"]:<15} effect per +10pp={row["coef_per_10pp_share"]:+.4f} CAR pp, p={row["pvalue"]:.4g}')
            if rows[0]['p_revised_equals_new'] is not None:
                print(f'    p(revised = new) = {rows[0]["p_revised_equals_new"]:.4g}')
    results = pd.DataFrame(all_rows)
    results.to_csv(args.output_dir / 'regression_coefficients.csv', index=False)
    (args.output_dir / 'diagnostics.json').write_text(json.dumps(diagnostics, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print('\nMatching:', f'{n_matched:,}/{len(panel):,} Stage 8 rows matched')
    print('Saved:', args.output_dir)
    print('Note: exploratory association, not sentiment effects or causal inference.')


if __name__ == '__main__':
    main()
