#!/usr/bin/env python3
"""Stage 8D: point-in-time valuation controls for eligible Stage 7 events.

Read-only inputs: Stage 7 event dates and J-Quants monthly historical valuation archives.
Writes a new, event-level CSV and coverage report; never edits frozen stages.

Policy: for each event, find the latest *existing valuation record* strictly before
its eventTradingDate, looking back no more than N global valuation sessions.
Missing fields in that record remain missing; we never silently substitute older
financial metrics. Market cap is in J-Quants' published units (JPY millions).
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_EVENTS = Path('data/interim/paper2/market_reaction/stage7_event_dates.csv')
DEFAULT_ROOT = Path('data/raw/paper2/controls/jquants/equities/valuation/historical')
DEFAULT_OUT = Path('data/interim/paper2/controls/stage8d_valuation_event_controls.csv')


def normalized_code(value: object) -> str | None:
    if pd.isna(value):
        return None
    s = str(value).strip().upper()
    if s.endswith('.0') and s[:-2].isdigit():
        s = s[:-2]
    if re.fullmatch(r'[0-9A-Z]{4}', s):
        return s + '0'
    if re.fullmatch(r'[0-9A-Z]{5}', s):
        return s
    return None


def monthly_files(root: Path, end_month: str) -> list[Path]:
    files = sorted(root.glob('*/equities_valuation_*.csv.gz'))
    files = [p for p in files if p.stem.split('_')[-1].split('.')[0] <= end_month]
    if not files:
        raise FileNotFoundError(f'No monthly valuation archives found under {root}')
    return files


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--events', type=Path, default=DEFAULT_EVENTS)
    ap.add_argument('--valuation-root', type=Path, default=DEFAULT_ROOT)
    ap.add_argument('--output', type=Path, default=DEFAULT_OUT)
    ap.add_argument('--max-lag-sessions', type=int, default=5)
    ap.add_argument('--overwrite', action='store_true')
    args = ap.parse_args()
    if not 1 <= args.max_lag_sessions <= 30:
        ap.error('--max-lag-sessions must be between 1 and 30')
    report_path = args.output.with_name(args.output.stem + '_coverage.json')
    if not args.overwrite:
        for target in (args.output, report_path):
            if target.exists():
                raise FileExistsError(f'Output exists: {target}. Use --overwrite explicitly.')

    events = pd.read_csv(args.events, dtype=str, low_memory=False)
    required = {'secCode', 'eventTradingDate', 'eventEligible'}
    if not required.issubset(events.columns):
        raise ValueError(f'Event file missing columns: {sorted(required - set(events.columns))}')
    eligible = events['eventEligible'].fillna('').str.strip().str.lower().isin(('true', '1', 'yes'))
    e = events.loc[eligible].copy().reset_index(drop=True)
    e.insert(0, 'stage8d_event_row', np.arange(len(e), dtype=np.int64))
    e['jquants_code'] = e['secCode'].map(normalized_code)
    e['event_date'] = pd.to_datetime(e['eventTradingDate'], errors='coerce').dt.normalize()
    print(f'Stage 7 rows: {len(events):,}; eligible: {len(e):,}; '
          f'invalid codes: {e.jquants_code.isna().sum():,}; invalid dates: {e.event_date.isna().sum():,}', flush=True)

    valid_dates = e['event_date'].dropna()
    if valid_dates.empty:
        raise ValueError('No eligible events with a valid eventTradingDate')
    end_month = valid_dates.max().strftime('%Y%m')
    files = monthly_files(args.valuation_root, end_month)
    print(f'Valuation monthly files: {len(files)}', flush=True)

    # Calendar is built from actual historical valuation dates, not event dates.
    dates = set()
    for i, path in enumerate(files, 1):
        frame = pd.read_csv(path, usecols=['Date'], dtype={'Date': str})
        dates.update(frame['Date'].dropna().unique())
        if i % 24 == 0 or i == len(files):
            print(f'  calendar scan: {i}/{len(files)} files', flush=True)
    calendar = pd.DatetimeIndex(pd.to_datetime(sorted(dates), errors='raise'))
    if not calendar.is_unique:
        raise ValueError('Unexpected duplicate calendar dates')
    print(f'Historical valuation sessions: {len(calendar):,}; '
          f'{calendar.min().date()} through {calendar.max().date()}', flush=True)

    # Build at most N prior-session candidate keys for each event.
    valid = e[e.jquants_code.notna() & e.event_date.notna()].copy()
    event_positions = calendar.searchsorted(pd.DatetimeIndex(valid.event_date), side='left')
    candidate_parts = []
    for lag in range(1, args.max_lag_sessions + 1):
        positions = event_positions - lag
        mask = positions >= 0
        if not mask.any():
            continue
        subset = valid.loc[mask, ['stage8d_event_row', 'jquants_code']].copy()
        subset['valuation_date'] = calendar.take(positions[mask]).strftime('%Y-%m-%d')
        subset['valuation_lag_sessions'] = lag
        candidate_parts.append(subset)
    if not candidate_parts:
        raise ValueError('No pre-event valuation dates available')
    candidates = pd.concat(candidate_parts, ignore_index=True)
    candidates['month'] = candidates.valuation_date.str[:7].str.replace('-', '', regex=False)
    candidates_by_month = {k: v.drop(columns='month') for k, v in candidates.groupby('month', sort=False)}

    found = []
    for i, path in enumerate(files, 1):
        month = path.name.removeprefix('equities_valuation_')[:6]
        wanted = candidates_by_month.get(month)
        if wanted is None:
            continue
        data = pd.read_csv(path, dtype={'Code': str}, usecols=['Date', 'Code', 'MktCap', 'PBR', 'ROE'], low_memory=False)
        if data.duplicated(['Date', 'Code']).any():
            raise ValueError(f'Duplicate (Date, Code) in {path}')
        joined = wanted.merge(
            data, left_on=['valuation_date', 'jquants_code'],
            right_on=['Date', 'Code'], how='inner', validate='many_to_one'
        )
        if not joined.empty:
            found.append(joined[['stage8d_event_row', 'valuation_date', 'valuation_lag_sessions', 'MktCap', 'PBR', 'ROE']])
        if i % 24 == 0 or i == len(files):
            print(f'  matching: {i}/{len(files)} files', flush=True)

    if found:
        matches = pd.concat(found, ignore_index=True)
        matches = matches.sort_values(['stage8d_event_row', 'valuation_lag_sessions'])
        matches = matches.drop_duplicates('stage8d_event_row', keep='first')
    else:
        matches = pd.DataFrame(columns=['stage8d_event_row', 'valuation_date', 'valuation_lag_sessions', 'MktCap', 'PBR', 'ROE'])

    e = e.merge(matches, on='stage8d_event_row', how='left', validate='one_to_one')
    for field in ('MktCap', 'PBR', 'ROE'):
        e[field] = pd.to_numeric(e[field], errors='coerce')
    e['ln_mktcap'] = np.log(e['MktCap'].where(e['MktCap'] > 0))
    e['book_to_market'] = (1 / e['PBR']).where(e['PBR'] > 0)
    e['roe_ttm'] = e['ROE']
    e['valuation_market_cap_jpy_millions'] = e['MktCap']
    e['valuation_pbr'] = e['PBR']
    e['valuation_roe'] = e['ROE']
    e['valuation_date'] = pd.to_datetime(e['valuation_date'], errors='coerce')
    # Fail loudly on any accidental look-ahead.
    if (e['valuation_date'] >= e['event_date']).fillna(False).any():
        raise AssertionError('LOOK-AHEAD: valuation_date >= eventTradingDate')

    keep = ['stage8d_event_row'] + [c for c in
            ('edinetCode', 'secCode', 'curr_docID', 'submitDateTime', 'eventTradingDate') if c in e.columns]
    keep += ['jquants_code', 'valuation_date', 'valuation_lag_sessions',
             'valuation_market_cap_jpy_millions', 'ln_mktcap',
             'valuation_pbr', 'book_to_market', 'valuation_roe', 'roe_ttm']
    result = e[keep].copy()
    result['valuation_date'] = result['valuation_date'].dt.strftime('%Y-%m-%d')

    report = {
        'source_events': str(args.events),
        'source_valuation_root': str(args.valuation_root),
        'event_date_policy': 'latest valuation row strictly before eventTradingDate',
        'max_lag_sessions': args.max_lag_sessions,
        'eligible_events': int(len(result)),
        'matched_valuation_rows': int(result.valuation_date.notna().sum()),
        'missing_valuation_rows': int(result.valuation_date.isna().sum()),
        'positive_mktcap': int(result.ln_mktcap.notna().sum()),
        'positive_pbr': int(result.book_to_market.notna().sum()),
        'nonmissing_roe': int(result.roe_ttm.notna().sum()),
        'complete_three_controls': int(result[['ln_mktcap', 'book_to_market', 'roe_ttm']].notna().all(axis=1).sum()),
        'valuation_lag_sessions': {str(k): int(v) for k, v in
                                  result.valuation_lag_sessions.value_counts(dropna=False).items()},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.output, index=False)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    print('\n=== STAGE 8D VALUATION COVERAGE ===')
    print(json.dumps(report, indent=2))
    print(f'Output: {args.output}')
    print(f'Coverage report: {report_path}')


if __name__ == '__main__':
    main()
