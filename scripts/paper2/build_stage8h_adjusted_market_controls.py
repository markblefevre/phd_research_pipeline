#!/usr/bin/env python3
"""Stage 8H: adjusted-return robustness version of Stage 8D market controls.

Inputs:
  - Stage 8D valuation event controls (one row per eligible filing)
  - J-Quants monthly historical security-master archives
  - Existing Stage 7 monthly daily-price archives

For each filing, use only daily observations with Date < calendar date of
submitDateTime. This intentionally excludes the filing day even if the filing
was after market close. No financial-summary accounting data are needed.

Outputs: NEW Stage 8H controls CSV and QC, never overwriting frozen Stage 8D.
Adjusted-return formula matches validated Stage 7, but Stage 8H
rolling-history availability is checked separately.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_VAL = Path('data/interim/paper2/controls/stage8d_valuation_event_controls.csv')
DEFAULT_MASTER = Path('data/raw/paper2/controls/jquants/equities/master/historical')
DEFAULT_PRICES = Path('data/raw/paper2/prices')
DEFAULT_VAL_ROOT = Path('data/raw/paper2/controls/jquants/equities/valuation/historical')
DEFAULT_OUT = Path('data/interim/paper2/controls/stage8h_adjusted_market_industry_event_controls.csv')
DEFAULT_OLD = Path('data/interim/paper2/controls/stage8d_market_industry_event_controls.csv')


def normalize_code(value):
    if pd.isna(value):
        return None
    s = str(value).strip().upper()
    if s.endswith('.0') and s[:-2].isalnum():
        s = s[:-2]
    if re.fullmatch(r'[0-9A-Z]{4}', s):
        return s + '0'
    if re.fullmatch(r'[0-9A-Z]{5}', s):
        return s
    return None


def price_paths(root, last_month):
    files = sorted(root.glob('equities_bars_daily_*.csv.gz'))
    files = [p for p in files if p.name.rsplit('_', 1)[-1][:6] <= last_month]
    if not files:
        raise FileNotFoundError(f'No price files under {root}')
    return files


def master_paths(root, last_month):
    files = sorted(root.glob('*/equities_master_*.csv.gz'))
    files = [p for p in files if p.name.rsplit('_', 1)[-1][:6] <= last_month]
    if not files:
        raise FileNotFoundError(f'No security-master files under {root}')
    return files


def asof_join(events, history, event_date_col, history_date_col, fields):
    """Stable per-code backward asof; require strictly prior history date."""
    e = events[['stage8d_event_row', 'jquants_code', event_date_col]].copy()
    e[event_date_col] = pd.to_datetime(e[event_date_col])
    h = history[['jquants_code', history_date_col, *fields]].copy()
    h[history_date_col] = pd.to_datetime(h[history_date_col])
    e_valid = e.dropna(subset=['jquants_code', event_date_col]).sort_values(event_date_col)
    h_valid = h.dropna(subset=['jquants_code', history_date_col]).sort_values(history_date_col)
    if h_valid.duplicated(['jquants_code', history_date_col]).any():
        raise ValueError(f'Duplicate history keys in {history_date_col}')
    if h_valid.empty:
        return pd.DataFrame({'stage8d_event_row': events['stage8d_event_row']})
    joined = pd.merge_asof(
        e_valid, h_valid,
        left_on=event_date_col, right_on=history_date_col,
        by='jquants_code', direction='backward', allow_exact_matches=False,
    )
    return joined.drop(columns=['jquants_code', event_date_col])


def load_master(root, codes, last_month):
    parts = []
    files = master_paths(root, last_month)
    fields = ['S17', 'S17Nm', 'S33', 'S33Nm', 'Mkt', 'MktNm', 'ProdCat']
    for i, path in enumerate(files, 1):
        m = pd.read_csv(path, dtype=str, usecols=['Date', 'Code', *fields], low_memory=False)
        m = m[m['Code'].isin(codes)].copy()
        if m.empty:
            continue
        m = m.sort_values(['Code', 'Date'], kind='stable')
        # Keep first observation per code/month and any within-month changes.
        # Keeping a first monthly snapshot is harmless and avoids needing to
        # maintain cross-month classification state.
        filled = m[fields].fillna('<NA>')
        previous = filled.groupby(m['Code'], sort=False).shift(1)
        keep = filled.ne(previous).any(axis=1) | m.groupby('Code', sort=False).cumcount().eq(0)
        changes = m.loc[keep, ['Date', 'Code', *fields]].copy()
        if not changes.empty:
            parts.append(changes)
        if i % 24 == 0 or i == len(files):
            print(f'  master: {i}/{len(files)} monthly files; retained {sum(map(len, parts)):,} snapshots', flush=True)
    if not parts:
        raise ValueError('No master records matched eligible securities')
    master = pd.concat(parts, ignore_index=True)
    master = master.rename(columns={'Date': 'master_date', 'Code': 'jquants_code',
                                    **{k: f'industry_{k.lower()}' for k in fields}})
    return master, len(files)


def rolling_features(current, tail):
    """Stage 8H: adjusted returns with strict consecutive TSE session continuity.

    Keep the same 60/20/60-to-5 feature definitions as Stage 8D. A missing
    price bar, suspension or invalid adjustment cannot create a bridged return.
    Momentum is the compounded *adjusted daily returns*, not an unadjusted
    endpoint price ratio. Tail state carries 75 historical observed bars.
    """
    cols = ['Date', 'Code', 'C', 'Va', 'AdjFactor', '_session_idx']
    a = pd.concat([tail, current[cols]], ignore_index=True) if not tail.empty else current[cols].copy()
    a = a.sort_values(['Code', 'Date'], kind='stable').reset_index(drop=True)
    g = a.groupby('Code', sort=False)
    close = a['C'].where(a['C'] > 0)
    previous_close = g['C'].shift(1).where(lambda x: x > 0)
    factor = a['AdjFactor'].fillna(1.0)
    consecutive = (a['_session_idx'] - g['_session_idx'].shift(1)).eq(1)
    # Mirror the validated Stage 7 adjusted simple return convention.
    ret = (close / (previous_close * factor) - 1).where(
        consecutive & factor.gt(0)
    )
    ret = ret.where(np.isfinite(ret))
    a['_adjusted_return'] = ret
    a['_adjusted_log1p'] = np.log1p(ret.where(ret > -1))
    a['volatility_60d'] = a.groupby('Code')['_adjusted_return'].transform(
        lambda x: x.rolling(60, min_periods=40).std()
    )
    a['market_valid_returns_60d'] = ret.notna().astype('int8').groupby(a['Code']).transform(
        lambda x: x.rolling(60, min_periods=1).sum()
    )
    # 20 consecutive returns ending at t; five-day gap for the long horizon.
    # A moving window containing an invalid/gapped return becomes NaN.
    a['momentum_20d'] = a.groupby('Code')['_adjusted_log1p'].transform(
        lambda x: np.expm1(x.rolling(20, min_periods=20).sum())
    )
    a['momentum_60_to_5d'] = a.groupby('Code')['_adjusted_log1p'].transform(
        lambda x: np.expm1(x.shift(5).rolling(60, min_periods=60).sum())
    )
    avg_value = a.groupby('Code')['Va'].transform(
        lambda x: x.rolling(60, min_periods=40).mean()
    )
    a['log_avg_trading_value_60d'] = np.log1p(avg_value.where(avg_value >= 0))
    a['market_chunk_history_sessions'] = a.groupby('Code').cumcount() + 1
    keep_cols = ['Date', 'Code', 'volatility_60d', 'market_valid_returns_60d',
                 'momentum_20d', 'momentum_60_to_5d', 'log_avg_trading_value_60d',
                 'market_chunk_history_sessions']
    next_tail = a[cols].groupby(a['Code'], sort=False).tail(75).copy()
    return a.loc[a['Date'].isin(current['Date'].unique()), keep_cols], next_tail


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--valuation-events', type=Path, default=DEFAULT_VAL)
    parser.add_argument('--master-root', type=Path, default=DEFAULT_MASTER)
    parser.add_argument('--prices-root', type=Path, default=DEFAULT_PRICES)
    parser.add_argument('--valuation-root', type=Path, default=DEFAULT_VAL_ROOT)
    parser.add_argument('--output', type=Path, default=DEFAULT_OUT)
    parser.add_argument('--baseline-controls', type=Path, default=DEFAULT_OLD)
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()
    report_path = args.output.with_name(args.output.stem + '_coverage.json')
    if not args.overwrite and (args.output.exists() or report_path.exists()):
        raise FileExistsError('Stage 8D output exists; use --overwrite explicitly')

    e = pd.read_csv(args.valuation_events, dtype={'jquants_code': str, 'secCode': str,
                                                  'curr_docID': str, 'edinetCode': str},
                    low_memory=False)
    required = {'stage8d_event_row', 'jquants_code', 'submitDateTime', 'curr_docID'}
    if not required.issubset(e.columns):
        raise ValueError(f'Valuation event file missing {sorted(required - set(e.columns))}')
    if e['stage8d_event_row'].duplicated().any() or e['curr_docID'].duplicated().any():
        raise ValueError('Duplicate event row ID or curr_docID in valuation controls')
    e['jquants_code'] = e['jquants_code'].map(normalize_code)
    e['filing_date'] = pd.to_datetime(e['submitDateTime'], errors='coerce').dt.normalize()
    if e['filing_date'].isna().any():
        raise ValueError('Some submitDateTime values are invalid')
    if e['jquants_code'].isna().any():
        raise ValueError('Some J-Quants security codes are invalid')
    codes = set(e['jquants_code'])
    last_month = e['filing_date'].max().strftime('%Y%m')
    print(f'Events: {len(e):,}; unique securities: {len(codes):,}', flush=True)

    print('Loading historical industry changes...', flush=True)
    master, master_count = load_master(args.master_root, codes, last_month)
    industry_fields = [c for c in master.columns if c.startswith('industry_')]
    industry = asof_join(e, master, 'filing_date', 'master_date', industry_fields)
    e = e.merge(industry, on='stage8d_event_row', how='left', validate='one_to_one')
    print('Industry matched:', int(e['industry_s33'].notna().sum()), '/', len(e), flush=True)

    paths = price_paths(args.prices_root, last_month)
    print(f'Scanning price calendar across {len(paths)} files...', flush=True)
    calendar = set()
    for i, path in enumerate(paths, 1):
        dates = pd.read_csv(path, usecols=['Date'], dtype={'Date': str})['Date'].dropna().unique()
        calendar.update(dates)
        if i % 24 == 0 or i == len(paths):
            print(f'  calendar: {i}/{len(paths)} files', flush=True)
    trading_dates = pd.DatetimeIndex(pd.to_datetime(sorted(calendar)))
    if not trading_dates.is_unique:
        raise ValueError('Trading calendar contains duplicate dates')
    pos = trading_dates.searchsorted(e['filing_date'], side='left') - 1
    e['market_date'] = pd.NaT
    ok = pos >= 0
    e.loc[ok, 'market_date'] = trading_dates.take(pos[ok])
    e['market_date'] = pd.to_datetime(e['market_date'])
    if not (e['market_date'].dropna() < e.loc[e['market_date'].notna(), 'filing_date']).all():
        raise AssertionError('Market-date look-ahead detected')
    target_keys = pd.MultiIndex.from_frame(
        e.loc[e['market_date'].notna(), ['market_date', 'jquants_code']]
        .rename(columns={'market_date': 'Date', 'jquants_code': 'Code'})
        .assign(Date=lambda d: d['Date'].dt.strftime('%Y-%m-%d'))
    )
    target_dates = set(e['market_date'].dropna().dt.strftime('%Y-%m-%d'))
    # Recompute valuation controls at the same strictly pre-filing market date.
    # The previous Stage 8D valuation file used eventTradingDate as cutoff,
    # which can be AFTER the filing date for after-close submissions.
    print('Recomputing valuation controls at the pre-filing cutoff...', flush=True)
    val_paths = sorted(args.valuation_root.glob('*/equities_valuation_*.csv.gz'))
    val_paths = [p for p in val_paths if p.name.rsplit('_', 1)[-1][:6] <= last_month]
    if not val_paths:
        raise FileNotFoundError(f'No historical valuation archives under {args.valuation_root}')
    val_parts = []
    for i, path in enumerate(val_paths, 1):
        v = pd.read_csv(path, usecols=['Date', 'Code', 'MktCap', 'PBR', 'ROE'],
                        dtype={'Date': str, 'Code': str}, low_memory=False)
        v = v[v['Date'].isin(target_dates) & v['Code'].isin(codes)]
        if not v.empty:
            ix = pd.MultiIndex.from_frame(v[['Date', 'Code']])
            v = v.loc[ix.isin(target_keys)]
            if not v.empty:
                val_parts.append(v)
        if i % 24 == 0 or i == len(val_paths):
            print(f'  valuation: {i}/{len(val_paths)} files', flush=True)
    if not val_parts:
        raise ValueError('No pre-filing valuation observations matched')
    val = pd.concat(val_parts, ignore_index=True)
    val['Date'] = pd.to_datetime(val['Date'])
    val = val.rename(columns={'Date': 'market_date', 'Code': 'jquants_code',
                              'MktCap': '_new_mktcap', 'PBR': '_new_pbr', 'ROE': '_new_roe'})
    if val.duplicated(['market_date', 'jquants_code']).any():
        raise ValueError('Duplicate pre-filing valuation keys')
    old_valuation_date = pd.to_datetime(e['valuation_date'], errors='coerce')
    lookahead_risk = int((old_valuation_date >= e['filing_date']).sum())
    e = e.rename(columns={c: f'{c}_event_trading_cutoff' for c in (
        'valuation_date', 'valuation_lag_sessions', 'valuation_market_cap_jpy_millions',
        'valuation_pbr', 'valuation_roe', 'ln_mktcap', 'book_to_market', 'roe_ttm'
    ) if c in e.columns})
    e = e.merge(val, on=['market_date', 'jquants_code'], how='left', validate='many_to_one')
    e['valuation_date'] = e['market_date'].where(e['_new_mktcap'].notna() |
                                                   e['_new_pbr'].notna() | e['_new_roe'].notna())
    for col in ['_new_mktcap', '_new_pbr', '_new_roe']:
        e[col] = pd.to_numeric(e[col], errors='coerce')
    e['valuation_market_cap_jpy_millions'] = e['_new_mktcap']
    e['valuation_pbr'] = e['_new_pbr']
    e['valuation_roe'] = e['_new_roe']
    e['ln_mktcap'] = np.log(e['_new_mktcap'].where(e['_new_mktcap'] > 0))
    e['book_to_market'] = 1 / e['_new_pbr'].where(e['_new_pbr'] > 0)
    e['roe_ttm'] = e['_new_roe']
    e = e.drop(columns=['_new_mktcap', '_new_pbr', '_new_roe'])
    print(f'  Original valuation observations dated on/after filing date: {lookahead_risk:,}', flush=True)
    print(f'  Pre-filing valuation market-cap coverage: {e.ln_mktcap.notna().sum():,}/{len(e):,}', flush=True)

    print('Computing pre-filing rolling market controls...', flush=True)
    tail = pd.DataFrame(columns=['Date', 'Code', 'C', 'Va', 'AdjFactor', '_session_idx'])
    matches = []
    for i, path in enumerate(paths, 1):
        current = pd.read_csv(path, usecols=['Date', 'Code', 'C', 'Va', 'AdjFactor'],
                              dtype={'Date': str, 'Code': str}, low_memory=False)
        current = current[current['Code'].isin(codes)].copy()
        if current.empty:
            continue
        if current.duplicated(['Date', 'Code']).any():
            raise ValueError(f'Duplicate daily price keys in {path}')
        for c in ['C', 'Va', 'AdjFactor']:
            current[c] = pd.to_numeric(current[c], errors='coerce')
        current['_session_idx'] = trading_dates.get_indexer(pd.to_datetime(current['Date']))
        if (current['_session_idx'] < 0).any():
            raise AssertionError('Price row absent from global trading calendar')
        feats, tail = rolling_features(current, tail)
        if target_dates.intersection(current['Date'].unique()):
            selected = feats[feats['Date'].isin(target_dates)]
            ix = pd.MultiIndex.from_frame(selected[['Date', 'Code']])
            selected = selected.loc[ix.isin(target_keys)]
            if not selected.empty:
                matches.append(selected)
        if i % 12 == 0 or i == len(paths):
            print(f'  prices: {i}/{len(paths)} files; selected rows {sum(map(len, matches)):,}', flush=True)
    if not matches:
        raise ValueError('No market observations matched eligible events')
    market = pd.concat(matches, ignore_index=True)
    market['Date'] = pd.to_datetime(market['Date'])
    market = market.rename(columns={'Date': 'market_date', 'Code': 'jquants_code'})
    if market.duplicated(['market_date', 'jquants_code']).any():
        raise ValueError('Duplicate selected market observations')
    e = e.merge(market, on=['market_date', 'jquants_code'], how='left', validate='many_to_one')
    e['market_lag_calendar_days'] = (e['filing_date'] - e['market_date']).dt.days
    if (e['market_lag_calendar_days'].dropna() < 1).any():
        raise AssertionError('Pre-filing cutoff violated')
    if e['master_date'].notna().any():
        if not (pd.to_datetime(e.loc[e.master_date.notna(), 'master_date']) <
                e.loc[e.master_date.notna(), 'filing_date']).all():
            raise AssertionError('Industry point-in-time cutoff violated')

    fields = ['ln_mktcap', 'book_to_market', 'roe_ttm', 'industry_s33', 'industry_s17', 'volatility_60d', 'momentum_20d',
              'momentum_60_to_5d', 'log_avg_trading_value_60d']
    report = {
        'events': len(e), 'unique_firms': e['jquants_code'].nunique(),
        'master_files': master_count, 'price_files': len(paths), 'valuation_files': len(val_paths),
        'original_valuation_on_or_after_filing_date': lookahead_risk,
        'valuation_policy': 'latest available valuation on the last market session strictly before filing calendar date',
        'market_cutoff': 'strictly before calendar date of EDINET submitDateTime',
        'industry_cutoff': 'strictly before calendar date of EDINET submitDateTime',
        'volatility': '60 observed-bars std of Stage 7-formula adjusted returns, min 40; invalid or nonconsecutive sessions missing',
        'momentum': 'compounded Stage 7-formula adjusted returns, 20 or 60-to-5 valid consecutive daily observations required',
        'adjusted_return_formula': 'C_t/(C_(t-1)*AdjFactor_t)-1, only consecutive global trading sessions',
        'liquidity': 'log(1 + average daily traded value in yen over 60 sessions), min 40',
        'matched_market_rows': int(e['market_chunk_history_sessions'].notna().sum()),
        'coverage': {c: {'n': int(e[c].notna().sum()), 'pct': round(100*e[c].notna().mean(), 2)} for c in fields},
        'all_market_and_industry': int(e[fields].notna().all(axis=1).sum()),
        'filing_day_excluded': bool((e['market_date'].dropna() < e.loc[e.market_date.notna(), 'filing_date']).all()),
    }
    if not (e['valuation_date'].dropna() < e.loc[e.valuation_date.notna(), 'filing_date']).all():
        raise AssertionError('Pre-filing valuation cutoff violated')
    e = e.drop(columns=['filing_date'])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Compare with original Stage 8D on matched document keys before writing.
    if not args.baseline_controls.exists():
        raise FileNotFoundError(f'Baseline Stage 8D controls not found: {args.baseline_controls}')
    old = pd.read_csv(args.baseline_controls, usecols=['curr_docID', 'volatility_60d',
                              'momentum_20d', 'momentum_60_to_5d'], dtype={'curr_docID': str})
    if old['curr_docID'].duplicated().any():
        raise ValueError('Baseline Stage 8D has duplicate curr_docID')
    merged = e[['curr_docID', 'volatility_60d', 'momentum_20d', 'momentum_60_to_5d']].merge(
        old, on='curr_docID', how='left', validate='one_to_one', suffixes=('_adjusted', '_raw'))
    comparisons = {}
    for col in ['volatility_60d', 'momentum_20d', 'momentum_60_to_5d']:
        new_vals = pd.to_numeric(merged[f'{col}_adjusted'], errors='coerce')
        old_vals = pd.to_numeric(merged[f'{col}_raw'], errors='coerce')
        paired = new_vals.notna() & old_vals.notna()
        delta = new_vals[paired] - old_vals[paired]
        comparisons[col] = {
            'old_nonmissing': int(old_vals.notna().sum()),
            'new_nonmissing': int(new_vals.notna().sum()),
            'paired': int(paired.sum()),
            'new_only': int((new_vals.notna() & old_vals.isna()).sum()),
            'old_only': int((new_vals.isna() & old_vals.notna()).sum()),
            'mean_delta_new_minus_old': float(delta.mean()) if len(delta) else None,
            'median_abs_delta': float(delta.abs().median()) if len(delta) else None,
            'p99_abs_delta': float(delta.abs().quantile(.99)) if len(delta) else None,
        }
    report['stage8h_comparison_to_stage8d'] = comparisons
    report['baseline_controls_path'] = str(args.baseline_controls)
    e.to_csv(args.output, index=False)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    print('\n=== STAGE 8H ADJUSTED-CONTROL COVERAGE ===', flush=True)
    print(json.dumps(report, indent=2, ensure_ascii=False), flush=True)
    print('Output:', args.output, flush=True)
    print('Coverage:', report_path, flush=True)
    print('Old versus adjusted-control comparison:', json.dumps(comparisons, indent=2), flush=True)


if __name__ == '__main__':
    main()
