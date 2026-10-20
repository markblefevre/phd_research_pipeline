#!/usr/bin/env python3
"""Paper 2 Stage 8E: validate three-component Luna sentiment, then run first-look CAR regressions.

Run from the repository root after copying into scripts/paper2/:
  python -u scripts/paper2/run_stage8e_validate_regressions.py --validate-only
  python -u scripts/paper2/run_stage8e_validate_regressions.py

Read-only inputs. Never changes Stage 8A-D or sentiment artifacts. New output directory only.
Firm-clustered CR1 inference and alternating-projection FE implementation match Stage 8D.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import t as student_t

# This is the existing, previously validated Stage 8 FE implementation.
from run_stage8_twfe import absorb, fe_rank

BASE = Path('data/interim/paper2')
DEFAULT_RUN = BASE / 'sentiment/three_component_stage8e/runs/20261019T021943726283Z_4f2fbd28'
DEFAULT_PANEL = Path('~/paper2_local/data/interim/paper2/analysis/stage8_panel/stage8_filing_panel.csv').expanduser()
DEFAULT_CONTROLS = BASE / 'controls/stage8d_market_industry_event_controls.csv'
DEFAULT_OUT = BASE / 'regressions/stage8e_three_component_firstlook'
PROMPT_HASH = '326e4698aed9575f16e26ed33e097fc0c3d4f68be1e6c774672fcd013e4801ff'
COMPONENTS = ('unchanged', 'revised', 'new')
SENTIMENT = [f'{x}Sentiment' for x in COMPONENTS]
DOC = ['noveltyCNum', 'logLengthChange']
VALUATION = ['ln_mktcap', 'book_to_market', 'roe_ttm']
MARKET = ['volatility_60d', 'momentum_20d', 'log_avg_trading_value_60d']
CONTROLS = DOC + VALUATION + MARKET
WINDOWS = {
    '0_1': ('eventStudySample_0_1', 'car_pp_0_1'),
    '0_0': ('eventStudySample_0_0', 'car_pp_0_0'),
    'm1_1': ('eventStudySample_m1_1', 'car_pp_m1_1'),
}
PAIRS = [('newSentiment', 'unchangedSentiment'),
         ('newSentiment', 'revisedSentiment'),
         ('revisedSentiment', 'unchangedSentiment')]


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def validate_scores(run_dir: Path):
    csv_path, manifest_path = run_dir / 'three_component_sentiment.csv', run_dir / 'manifest.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    if manifest.get('documents') != 33046 or manifest.get('dry_run') is not False:
        raise ValueError('Unexpected manifest size or dry-run setting')
    if manifest.get('model') != 'gpt-6-luna' or manifest.get('prompt_sha256') != PROMPT_HASH:
        raise ValueError('Manifest model/prompt differs from Stage 6F-B production')
    if manifest.get('reasoning_effort') != 'none' or manifest.get('target_unit_chars') != 1050 or manifest.get('max_unit_chars') != 1400:
        raise ValueError('Unexpected reasoning or unit-segmentation settings')
    wanted = {'curr_docID', 'prev_docID', 'model', 'promptSha256'}
    for c in COMPONENTS:
        wanted.update((f'{c}Sentiment', f'{c}Status', f'{c}Chars',
                       f'{c}NewInputTokens', f'{c}NewOutputTokens'))
    available = set(pd.read_csv(csv_path, nrows=0).columns)
    if not wanted <= available:
        raise ValueError(f'Sentiment CSV missing columns: {sorted(wanted - available)}')
    df = pd.read_csv(csv_path, usecols=sorted(wanted),
                     dtype={'curr_docID': 'string', 'prev_docID': 'string'}, low_memory=False)
    if len(df) != manifest['documents']:
        raise ValueError(f'CSV rows {len(df)} != manifest documents {manifest["documents"]}')
    if df['curr_docID'].isna().any() or df['curr_docID'].duplicated().any():
        raise ValueError('Missing/duplicate current filing IDs')
    if df['prev_docID'].isna().any():
        raise ValueError('Missing prior filing IDs')
    if not df['model'].eq('gpt-6-luna').all() or not df['promptSha256'].eq(PROMPT_HASH).all():
        raise ValueError('CSV model or prompt mismatch')

    report = {'documents': len(df), 'model': manifest['model'], 'prompt_sha256': PROMPT_HASH,
              'run_id': manifest['run_id'], 'components': {}}
    input_tokens = output_tokens = 0
    for c in COMPONENTS:
        status_col, tone_col = f'{c}Status', f'{c}Sentiment'
        status = df[status_col].astype('string').str.strip()
        counts = dict(Counter(status.dropna().tolist()))
        if counts != manifest['status_counts'][c]:
            raise ValueError(f'{c}: CSV status counts do not match manifest: {counts}')
        if not status.isin(['empty', 'scored', 'resumed']).all():
            raise ValueError(f'{c}: unexpected or missing status')
        tone = pd.to_numeric(df[tone_col], errors='coerce')
        present = status.isin(['scored', 'resumed'])
        if tone[present].isna().any() or tone[~present].notna().any():
            raise ValueError(f'{c}: scored/empty sentiment missingness mismatch')
        if not np.isfinite(tone[present].to_numpy(dtype=float)).all() or not tone[present].between(-1.000001, 1.000001).all():
            raise ValueError(f'{c}: nonfinite or out-of-range sentiment')
        chars = pd.to_numeric(df[f'{c}Chars'], errors='coerce')
        if chars.isna().any() or (chars < 0).any() or (chars[present] <= 0).any():
            raise ValueError(f'{c}: invalid character counts')
        for suffix in ('NewInputTokens', 'NewOutputTokens'):
            v = pd.to_numeric(df[f'{c}{suffix}'], errors='coerce')
            if v.isna().any() or (v < 0).any():
                raise ValueError(f'{c}: invalid {suffix}')
            if suffix == 'NewInputTokens':
                input_tokens += int(v.sum())
            else:
                output_tokens += int(v.sum())
        report['components'][c] = {
            'status_counts': counts, 'present': int(present.sum()),
            'missing': int((~present).sum()),
            'mean': float(tone[present].mean()), 'std': float(tone[present].std()),
            'min': float(tone[present].min()), 'max': float(tone[present].max()),
        }
        df[tone_col] = tone
    if input_tokens != manifest['new_input_tokens'] or output_tokens != manifest['new_output_tokens']:
        raise ValueError('CSV new token totals do not match manifest')
    report['new_input_tokens'] = input_tokens
    report['new_output_tokens'] = output_tokens
    report['all_three_present'] = int(df[SENTIMENT].notna().all(axis=1).sum())
    report['none_present'] = int(df[SENTIMENT].isna().all(axis=1).sum())
    report['nonempty_components'] = sum(v['present'] for v in report['components'].values())
    if report['nonempty_components'] != 89946 or report['all_three_present'] != 24105:
        raise ValueError('Sentiment coverage differs from Stage 8E inventory')
    print('STAGE 8E VALIDATION PASSED', flush=True)
    print(f"Documents={len(df):,} nonempty_components={report['nonempty_components']:,} "
          f"all_three={report['all_three_present']:,} none={report['none_present']:,}", flush=True)
    for c, s in report['components'].items():
        print(f"{c:10} scored/resumed={s['present']:6,d} empty={s['missing']:5,d} "
              f"mean={s['mean']:+.5f} std={s['std']:.5f} "
              f"range=[{s['min']:+.3f},{s['max']:+.3f}]", flush=True)
    print(f'New tokens: input={input_tokens:,} output={output_tokens:,}', flush=True)
    return df[['curr_docID', *SENTIMENT]].copy(), report


def event_flag(series: pd.Series) -> pd.Series:
    v = series.astype('string').str.strip().str.lower()
    if not v.dropna().isin(['true', 'false', '1', '0']).all():
        raise ValueError('Unexpected eventStudySample flag value')
    return v.isin(['true', '1'])


def load_panel(panel_path: Path, controls_path: Path, scores: pd.DataFrame):
    panel_cols = ['curr_docID', 'edinetCode', 'fiscalYear', *DOC]
    for flag, outcome in WINDOWS.values():
        panel_cols.extend((flag, outcome))
    panel = pd.read_csv(panel_path, usecols=panel_cols,
                        dtype={'curr_docID': 'string', 'edinetCode': 'string'}, low_memory=False)
    ctrl = pd.read_csv(controls_path, usecols=['curr_docID', *VALUATION, *MARKET],
                       dtype={'curr_docID': 'string'}, low_memory=False)
    for name, frame in [('panel', panel), ('controls', ctrl), ('scores', scores)]:
        frame['curr_docID'] = frame['curr_docID'].str.strip()
        if frame['curr_docID'].isna().any() or frame['curr_docID'].duplicated().any():
            raise ValueError(f'{name}: missing/duplicate filing IDs')
    if len(panel) != 33046:
        raise ValueError(f'Unexpected Stage 8 panel size: {len(panel)}')
    joined = panel.merge(scores, on='curr_docID', how='left', validate='one_to_one', indicator='sent_join')
    matched_sent = int(joined['sent_join'].eq('both').sum())
    if matched_sent != len(panel) or len(scores) != len(panel):
        raise ValueError(f'Sentiment join incomplete: {matched_sent}/{len(panel)}')
    joined = joined.merge(ctrl, on='curr_docID', how='left', validate='one_to_one', indicator='ctrl_join')
    matched_ctrl = int(joined['ctrl_join'].eq('both').sum())
    if matched_ctrl < 30000:
        raise ValueError(f'Too few Stage 8D controls matched: {matched_ctrl}')
    for col in [*SENTIMENT, *CONTROLS, 'fiscalYear', *(y for _, y in WINDOWS.values())]:
        joined[col] = pd.to_numeric(joined[col], errors='coerce').replace([np.inf, -np.inf], np.nan)
    joined['edinetCode'] = joined['edinetCode'].str.strip().replace('', pd.NA)
    print(f'Panel={len(panel):,} matched sentiment={matched_sent:,} matched controls={matched_ctrl:,}', flush=True)
    return joined, {'panel_rows': len(panel), 'sentiment_matched': matched_sent, 'controls_matched': matched_ctrl}


def estimate(sample: pd.DataFrame, outcome: str, terms: list[str], mode: str,
             analysis: str, window: str):
    y = sample[outcome].to_numpy(dtype=float)
    x = sample[terms].to_numpy(dtype=float)
    firms, firm_ids = np.unique(sample['edinetCode'].to_numpy(), return_inverse=True)
    years, year_ids = np.unique(sample['fiscalYear'].to_numpy(), return_inverse=True)
    groups = [firm_ids, year_ids] if mode == 'firm_year_fe' else [year_ids]
    residualized, iterations, error = absorb(np.column_stack((y, x)), groups)
    yw, xw = residualized[:, 0], residualized[:, 1:]
    n, k = xw.shape
    rank = int(np.linalg.matrix_rank(xw))
    dropped_terms = []
    if rank != k and analysis == 'presence_indicators':
        # A rare component may be present in every eligible observation, or
        # its presence flag may be absorbed by FE. Retain all tone and control
        # regressors; keep only identified presence indicators.
        mandatory = [j for j, term in enumerate(terms) if not term.endswith('Present')]
        if np.linalg.matrix_rank(xw[:, mandatory]) != len(mandatory):
            raise ValueError(f'{analysis}/{window}/{mode}: base regressors rank deficient')
        keep = mandatory[:]
        current_rank = len(keep)
        for j, term in enumerate(terms):
            if j in mandatory:
                continue
            candidate_rank = int(np.linalg.matrix_rank(xw[:, keep + [j]]))
            if candidate_rank > current_rank:
                keep.append(j)
                current_rank = candidate_rank
            else:
                dropped_terms.append(term)
        keep.sort()
        xw = xw[:, keep]
        terms = [terms[j] for j in keep]
        k = len(terms)
        rank = int(np.linalg.matrix_rank(xw))
        if dropped_terms:
            print(f'{analysis}/{window}/{mode}: unidentified presence flags omitted: '
                  f'{dropped_terms}', flush=True)
    if rank != k:
        raise ValueError(f'{analysis}/{window}/{mode}: rank deficiency {rank}/{k}')
    fe_dof, components = fe_rank(firm_ids, year_ids, mode == 'firm_year_fe')
    df_resid = n - fe_dof - k
    if df_resid <= 0 or len(firms) < 100:
        raise ValueError('Insufficient degrees of freedom or firms')
    beta = np.linalg.lstsq(xw, yw, rcond=None)[0]
    residual = yw - xw @ beta
    inv = np.linalg.inv(xw.T @ xw)
    scores = xw * residual[:, None]
    cluster_scores = np.zeros((len(firms), k))
    np.add.at(cluster_scores, firm_ids, scores)
    g = len(firms)
    factor = (g / (g - 1)) * ((n - 1) / df_resid)
    vcov = inv @ (cluster_scores.T @ cluster_scores) @ inv * factor
    vcov = (vcov + vcov.T) / 2
    se = np.sqrt(np.diag(vcov))
    if not np.isfinite(se).all() or (se <= 0).any():
        raise ValueError(f'Invalid standard errors: {analysis}/{window}/{mode}')
    p = 2 * student_t.sf(np.abs(beta / se), df=g-1)
    within_r2 = float(1 - (residual @ residual) / (yw @ yw)) if (yw @ yw) > 0 else None
    model = {'analysis': analysis, 'window': window, 'spec': mode, 'N': n,
             'firms': g, 'years': len(years), 'within_r2': within_r2,
             'fe_rank': fe_dof, 'fe_components': components,
             'absorb_iterations': iterations, 'absorb_error': error, 'cr1_factor': factor,
             'dropped_presence_flags': ','.join(dropped_terms)}
    coefficients = [{'analysis': analysis, 'window': window, 'spec': mode,
                     'term': term, 'coef': float(beta[j]), 'se_cluster_firm': float(se[j]),
                     'p': float(p[j]), 'N': n, 'firms': g}
                    for j, term in enumerate(terms)]
    pairwise = []
    for a, b in PAIRS:
        # Both primary and presence-indicator robustness use the same named sentiment columns.
        v = np.zeros(k)
        v[terms.index(a)] = 1
        v[terms.index(b)] = -1
        difference = float(v @ beta)
        difference_se = float(np.sqrt(v @ vcov @ v))
        if not np.isfinite(difference_se) or difference_se <= 0:
            raise ValueError(f'Invalid difference SE: {a}-{b}')
        difference_p = float(2 * student_t.sf(abs(difference / difference_se), df=g-1))
        pairwise.append({'analysis': analysis, 'window': window, 'spec': mode,
                         'contrast': f'{a} - {b}', 'difference': difference,
                         'se_cluster_firm': difference_se, 'p': difference_p,
                         'ci95_low': difference - student_t.ppf(.975, df=g-1)*difference_se,
                         'ci95_high': difference + student_t.ppf(.975, df=g-1)*difference_se,
                         'N': n, 'firms': g})
    lookup = {v['term']: v for v in coefficients}
    print(f'{analysis:18} {window:5} {mode:12} N={n:6,d} firms={g:4,d} '
          f'U={lookup["unchangedSentiment"]["coef"]:+.4f} (p={lookup["unchangedSentiment"]["p"]:.3g}) '
          f'R={lookup["revisedSentiment"]["coef"]:+.4f} (p={lookup["revisedSentiment"]["p"]:.3g}) '
          f'N={lookup["newSentiment"]["coef"]:+.4f} (p={lookup["newSentiment"]["p"]:.3g}) '
          f'p(N-R)={pairwise[1]["p"]:.3g}', flush=True)
    return model, coefficients, pairwise


def run_regressions(joined: pd.DataFrame):
    models, coefficients, pairwise, samples = [], [], [], []
    for window, (flag, outcome) in WINDOWS.items():
        eligible = joined.loc[event_flag(joined[flag])].copy()
        for analysis in ('complete_case', 'presence_indicators'):
            data = eligible.copy()
            if analysis == 'presence_indicators':
                for col in SENTIMENT:
                    data[f'{col}Present'] = data[col].notna().astype(float)
                    data[col] = data[col].fillna(0.0)
                # These zeros are never interpreted as neutral tone: presence dummies
                # identify absence, and the sentiment regressor is S * 1[present].
                terms = SENTIMENT + [f'{c}Present' for c in SENTIMENT] + CONTROLS
            else:
                terms = SENTIMENT + CONTROLS
            needed = [outcome, 'edinetCode', 'fiscalYear', *terms]
            sample = data[needed].dropna().copy()
            if len(sample) < 1000 or sample['edinetCode'].nunique() < 100:
                raise ValueError(f'{analysis}/{window}: too few observations')
            if not np.all(sample['fiscalYear'] % 1 == 0):
                raise ValueError('Noninteger fiscalYear')
            sample['fiscalYear'] = sample['fiscalYear'].astype(int).astype(str)
            sample['edinetCode'] = sample['edinetCode'].astype(str)
            samples.append({'analysis': analysis, 'window': window, 'eligible': len(eligible),
                            'complete_case': len(sample), 'firms': sample['edinetCode'].nunique(),
                            'single_observation_firms': int((sample.groupby('edinetCode').size() == 1).sum())})
            for mode in ('year_fe', 'firm_year_fe'):
                m, c, p = estimate(sample, outcome, terms, mode, analysis, window)
                models.append(m)
                coefficients.extend(c)
                pairwise.extend(p)
    return models, coefficients, pairwise, samples


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--run-dir', type=Path, default=DEFAULT_RUN)
    ap.add_argument('--panel', type=Path, default=DEFAULT_PANEL)
    ap.add_argument('--controls', type=Path, default=DEFAULT_CONTROLS)
    ap.add_argument('--output-dir', type=Path, default=DEFAULT_OUT)
    ap.add_argument('--validate-only', action='store_true')
    args = ap.parse_args()
    for key in ('run_dir', 'panel', 'controls', 'output_dir'):
        setattr(args, key, getattr(args, key).expanduser())
    if not args.validate_only and args.output_dir.exists():
        raise FileExistsError(f'Refusing to overwrite existing output: {args.output_dir}')
    scores, report = validate_scores(args.run_dir)
    if args.validate_only:
        return
    for p in (args.panel, args.controls):
        if not p.is_file():
            raise FileNotFoundError(p)
    joined, coverage = load_panel(args.panel, args.controls, scores)
    models, coefs, pairs, samples = run_regressions(joined)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    pd.DataFrame(models).to_csv(args.output_dir / 'model_comparison.csv', index=False)
    pd.DataFrame(coefs).to_csv(args.output_dir / 'coefficients.csv', index=False)
    pd.DataFrame(pairs).to_csv(args.output_dir / 'pairwise_tests.csv', index=False)
    pd.DataFrame(samples).to_csv(args.output_dir / 'sample_diagnostics.csv', index=False)
    (args.output_dir / 'validation_report.json').write_text(json.dumps(report, indent=2) + '\n')
    metadata = {
        'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'sentiment_run_dir': str(args.run_dir),
        'sentiment_csv_sha256': digest(args.run_dir / 'three_component_sentiment.csv'),
        'sentiment_manifest_sha256': digest(args.run_dir / 'manifest.json'),
        'panel': str(args.panel), 'panel_sha256': digest(args.panel),
        'controls': str(args.controls), 'controls_sha256': digest(args.controls),
        'join_coverage': coverage, 'samples': samples,
        'outcome': 'CAR in percentage points',
        'sentiment': 'Luna v2; three components each in [-1,1]',
        'main_analysis': 'all three nonempty components, firm+fiscal-year FE',
        'robustness': 'absent sentiments encoded as zero times presence, with separate presence indicators',
        'control_terms': CONTROLS,
        'inference': 'firm-clustered CR1; cluster t(df=number of firms minus 1)',
        'limitations': 'exploratory association, not causal; Stage 8D raw-close market controls provisional; compare [-1,0,+1] leakage',
    }
    (args.output_dir / 'run_metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print(f'\nRegression outputs: {args.output_dir}', flush=True)


if __name__ == '__main__':
    main()
