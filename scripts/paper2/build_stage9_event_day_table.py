#!/usr/bin/env python3
"""Stage 9 event-day sensitivity table, on exactly the frozen Model A sample.

Run from the Paper 2 repository root:
    python scripts/paper2/build_stage9_event_day_table.py

Inputs are the existing Stage 9 regression_dataset.csv, coefficients.csv and
model_summary.csv. This script refits *only* event-day outcomes using the same
three sentiments, seven controls, firm/year FE and firm-CR1 inference as Model A.
It never modifies Stage 9 estimator outputs or any prior pipeline stage.

Requirements: numpy, pandas, scipy.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import t as student_t

TERMS = [
    'unchangedSentiment', 'revisedSentiment', 'newSentiment',
    'noveltyCNum', 'logLengthChange', 'ln_mktcap', 'book_to_market',
    'roe_ttm', 'volatility_60d', 'log_avg_trading_value_60d',
]
SENTIMENT = TERMS[:3]
LABELS = {'unchangedSentiment':'Unchanged sentiment',
          'revisedSentiment':'Revised sentiment',
          'newSentiment':'New sentiment'}
OUTCOMES = ['Day -1', 'Day 0', 'Day +1', 'CAR [-1,+1]', 'Day +1 minus day -1']


def digest(path: Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for c in iter(lambda:f.read(1024*1024),b''):
            h.update(c)
    return h.hexdigest()


def fe_residualize(z: np.ndarray, firm_ix: np.ndarray, year_ix: np.ndarray,
                   groups: int, years: int, tol=1e-12, max_iterations=300):
    z=z.copy()
    firm_count=np.bincount(firm_ix,minlength=groups)
    year_count=np.bincount(year_ix,minlength=years)
    for iteration in range(1,max_iterations+1):
        prior=z.copy()
        for ids, counts, num in ((firm_ix,firm_count,groups),(year_ix,year_count,years)):
            for col in range(z.shape[1]):
                z[:,col]-=np.bincount(ids,weights=z[:,col],minlength=num)[ids]/counts[ids]
        if np.max(np.abs(z-prior))<tol:
            return z,iteration
    raise RuntimeError('Firm/year FE absorption failed to converge')


def four(x):
    return f'{x:+.4f}' if x>=0 else f'{x:.4f}'


def latex_table(results: pd.DataFrame, n:int, firms:int, daytest:pd.Series) -> str:
    # The table uses no external LaTeX packages and fits a standard article column.
    rows=[]
    for term in SENTIMENT:
        subset=results[(results.term==term)].set_index('outcome')
        estimates=[]; errors=[]
        for outcome in OUTCOMES[:4]:
            rec=subset.loc[outcome]
            stars='^{**}' if rec.p<.05 else ('^{*}' if rec.p<.10 else '')
            estimates.append(f'${four(float(rec.coef))}{stars}$')
            errors.append(f'({float(rec.se):.4f})')
        rows.extend([
            LABELS[term]+' & '+' & '.join(estimates)+r' \\',
            ' & '+' & '.join(errors)+r' \\',
        ])
    return '\n'.join([
        r'\begin{table}[htbp]',
        r'\centering',
        r'\caption{Event-day decomposition of disclosure-component sentiment associations}',
        r'\label{tab:stage9_event_days}',
        r'\small',
        r'\setlength{\tabcolsep}{5pt}',
        r'\begin{tabular}{lrrrr}',
        r'\hline\hline',
        r' & Day $-1$ & Day $0$ & Day $+1$ & CAR $[-1,+1]$ \\',
        r'\hline',
        *rows,
        r'\hline',
        f'Observations & {n:,} & {n:,} & {n:,} & {n:,} '+r'\\',
        f'Firms & {firms:,} & {firms:,} & {firms:,} & {firms:,} '+r'\\',
        r'Firm and year fixed effects & Yes & Yes & Yes & Yes \\',
        r'Primary controls & Yes & Yes & Yes & Yes \\',
        r'\hline\hline',
        r'\end{tabular}',
        r'\par\vspace{2pt}',
        r'\begin{minipage}{0.99\linewidth}',
        (r'\footnotesize\textit{Notes:} Coefficients are abnormal-return percentage points per '
         r'sentiment unit. Firm-clustered CR1 standard errors appear in parentheses. '
         r'$^{**}p<0.05$, $^{*}p<0.10$ (unadjusted, two-sided). '
         r'Day 0 denotes the assigned event trading day; days $-1$ and $+1$ are adjacent '
         r'TSE sessions. All models use the identical Stage 9 Model A sample and regressors. '
         r'CAR $[-1,+1]$ is the sum of the three daily abnormal returns. '
         f'The direct new-sentiment day $+1$ minus day $-1$ slope difference is '
         f'${four(float(daytest.coef))}$ ($p={float(daytest.p):.3f}$). '
         r'Day-level tests are exploratory and not multiplicity-adjusted.'),
        r'\end{minipage}',
        r'\end{table}',
        ''
    ])


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--stage9-dir',type=Path,default=Path('data/interim/paper2/regressions/stage9'))
    args=ap.parse_args()
    root=args.stage9_dir
    paths={Path(k).stem:root/k for k in ('regression_dataset.csv','model_summary.csv','coefficients.csv')}
    for p in paths.values():
        if not p.is_file():raise FileNotFoundError(f'Missing Stage 9 file: {p}')
    base=pd.read_csv(paths['model_summary'])
    model=base.loc[base['model'].eq('A_baseline')].iloc[0]
    primary=pd.read_csv(paths['coefficients'])
    primary=primary.loc[primary['model'].eq('A_baseline')].set_index('term')
    required=['baseline_eligible','edinetCode','fiscalYear','car_pp_m1_1','car_pp_0_0',
              'car_pp_0_1','abnormalReturn_m1','abnormalReturn_0','abnormalReturn_1']+TERMS
    d=pd.read_csv(paths['regression_dataset'],usecols=required,low_memory=False)
    d=d.loc[d['baseline_eligible'].eq(True)].reset_index(drop=True)
    n=len(d)
    firms,firm_ix=np.unique(d.edinetCode.astype(str).to_numpy(),return_inverse=True)
    years,year_ix=np.unique(d.fiscalYear.to_numpy(),return_inverse=True)
    g=len(firms);ny=len(years);k=len(TERMS)
    if (n,g,ny)!=(int(model.N),int(model.firms),int(model.years)):
        raise ValueError('The current baseline sample does not match frozen Stage 9 Model A')
    rankfe=g+ny-1
    if int(model.fe_rank)!=rankfe:
        raise ValueError('Unexpected FE rank; verify component connectivity before estimating')
    x=d[TERMS].to_numpy(dtype=float)
    car=d['car_pp_m1_1'].to_numpy(dtype=float)
    car00=d['car_pp_0_0'].to_numpy(dtype=float)
    car01=d['car_pp_0_1'].to_numpy(dtype=float)
    # Exactly Stage 8F's day definitions, now on the final corrected Stage 9 data.
    ym1=car-car01
    y0=car00
    yp1=car01-car00
    y=np.column_stack([ym1,y0,yp1,car,yp1-ym1])
    if not np.isfinite(np.column_stack([x,y])).all():
        raise ValueError('Nonfinite values in baseline regressor or event-day outcome')
    for col,daily in [('abnormalReturn_m1',ym1),('abnormalReturn_0',y0),('abnormalReturn_1',yp1)]:
        if not np.allclose(d[col].to_numpy(dtype=float)*100,daily,rtol=1e-9,atol=1e-8):
            raise ValueError(f'Event-day identity fails: {col}')
    if not np.allclose(ym1+y0+yp1,car,rtol=0,atol=1e-9):
        raise ValueError('CAR decomposition identity fails')
    z,iterations=fe_residualize(np.column_stack([x,y]),firm_ix,year_ix,g,ny)
    xw=z[:,:k];yw=z[:,k:]
    invxx=np.linalg.inv(xw.T@xw)
    beta=invxx@(xw.T@yw)
    resid=yw-xw@beta
    fac=(g/(g-1))*((n-1)/(n-rankfe-k))
    if not np.isclose(fac,float(model.cr1_factor),rtol=1e-6):
        raise ValueError('CR1 correction differs from canonical Stage 9 model')
    rows=[]
    max_b=max_se=0.
    for j,outcome in enumerate(OUTCOMES):
        s=np.zeros((g,k))
        np.add.at(s,firm_ix,xw*resid[:,[j]])
        cov=fac*invxx@(s.T@s)@invxx
        se=np.sqrt(np.diag(cov))
        p=2*student_t.sf(np.abs(beta[:,j]/se),df=g-1)
        tcrit=student_t.ppf(0.975,df=g-1)
        for i,term in enumerate(TERMS):
            rows.append({'outcome':outcome,'term':term,'coef':float(beta[i,j]),
                         'se':float(se[i]),'p':float(p[i]),
                         'ci95_low':float(beta[i,j]-tcrit*se[i]),
                         'ci95_high':float(beta[i,j]+tcrit*se[i]),'N':n,'firms':g})
            if outcome=='CAR [-1,+1]':
                max_b=max(max_b,abs(float(beta[i,j])-float(primary.loc[term,'coef'])))
                max_se=max(max_se,abs(float(se[i])-float(primary.loc[term,'se'])))
    if max_b>1e-7 or max_se>1e-7:
        raise ValueError(f'Independent Model A replication FAILED: beta diff {max_b}, SE diff {max_se}')
    df=pd.DataFrame(rows)
    daytest=df.loc[df['outcome'].eq('Day +1 minus day -1') & df['term'].eq('newSentiment')].iloc[0]
    out=root/'tables'
    out.mkdir(parents=True,exist_ok=True)
    csv_path=out/'table4_event_day_decomposition.csv'
    tex_path=out/'table4_event_day_decomposition.tex'
    provenance_path=out/'table4_event_day_decomposition_provenance.json'
    df.to_csv(csv_path,index=False)
    tex_path.write_text(latex_table(df,n,g,daytest),encoding='utf-8')
    meta={'generated_utc':datetime.now(timezone.utc).isoformat(),
          'stage9_model':'A_baseline','N':n,'firms':g,'years':ny,
          'baseline_sample_sha256_from_model_summary':str(model.sample_sha256),
          'sources_sha256':{k:digest(v) for k,v in paths.items()},
          'method':'Independent FE-within OLS and firm-clustered CR1; day AR via CAR arithmetic',
          'regressors':TERMS,'outcomes':OUTCOMES,'absorb_iterations':iterations,
          'cr1_factor':fac,'max_primary_beta_difference':max_b,
          'max_primary_se_difference':max_se,'daily_sum_identity':True,
          'event_day_difference_new_sentiment':{'coef':float(daytest.coef),'p':float(daytest.p)},
          'new_llm_calls':False,'new_market_data_calculations':False,
          'new_sensitivity_regressions':True}
    provenance_path.write_text(json.dumps(meta,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print(f'Generated {tex_path} (N={n:,}, firms={g:,}); maximum Model A beta and SE differences: {max_b:.2e}, {max_se:.2e}')
    for outcome in OUTCOMES:
        r=df[(df.outcome==outcome)&(df.term=='newSentiment')].iloc[0]
        print(f'  {outcome:25s} beta={r.coef:+.6f}, SE={r.se:.6f}, p={r.p:.6f}')

if __name__=='__main__':
    main()
