#!/usr/bin/env python3
"""Read-only Paper 2 Stage 9 CAR influence diagnostic.

Reproduces the canonical two-way firm/fiscal-year FE estimator with firm-clustered
CR1 errors and Student-t p-values. First-order deletion scores SCREEN all filings;
selected observations/firms are then re-estimated exactly on the reduced data.

This is a diagnostic, not an alternative model-selection procedure. Never use
ex-post influence rankings to redefine the primary estimation sample.

Usage from repository root:
    python scripts/paper2/diagnose_stage9_car_influence.py \
        --input data/interim/paper2/regressions/stage9/regression_dataset.csv \
        --output data/interim/paper2/diagnostics/extreme_car
"""
from __future__ import annotations
import argparse
from pathlib import Path
import time
import numpy as np
import pandas as pd
from scipy.stats import t as student_t

SENTIMENT=['unchangedSentiment','revisedSentiment','newSentiment']
CONTROLS=['noveltyCNum','logLengthChange','ln_mktcap','book_to_market',
          'roe_ttm','volatility_60d','log_avg_trading_value_60d']
TERMS=SENTIMENT+CONTROLS
CAR='car_pp_m1_1'
NEED=TERMS + [CAR, 'baseline_eligible','edinetCode','fiscalYear',
              'curr_docID','filerName','secCode','curr_submitDateTime',
              'eventTradingDate','afterClose']


def absorb(z, dims, sizes, tol=1e-12, max_iters=400):
    z=np.array(z,dtype=float,copy=True,order='C')
    counts=[np.bincount(idx,minlength=n) for idx,n in zip(dims,sizes)]
    for iteration in range(1,max_iters+1):
        prev=z.copy()
        for idx,c,n in zip(dims,counts,sizes):
            for j in range(z.shape[1]):
                z[:,j] -= np.bincount(idx,weights=z[:,j],minlength=n)[idx]/c[idx]
        if np.max(np.abs(z-prev)) < tol:
            return z,iteration
    raise RuntimeError('Two-way FE absorption did not converge')


def fit(frame, outcome=None, with_matrices=False):
    y=(frame[CAR].to_numpy(float) if outcome is None else np.asarray(outcome,float))
    x=frame[TERMS].to_numpy(float)
    n,k=x.shape
    firms,fi=np.unique(frame['edinetCode'].astype(str),return_inverse=True)
    years,yi=np.unique(frame['fiscalYear'],return_inverse=True)
    g=len(firms)
    z,iteration=absorb(np.column_stack([x,y]),[fi,yi],[g,len(years)])
    xw,yw=z[:,:k],z[:,k]
    xx_inv=np.linalg.inv(xw.T@xw)
    b=xx_inv@(xw.T@yw)
    residual=yw-xw@b

    # Rank of FE matrix for possibly disconnected firm/year incidence graph.
    parent=np.arange(g+len(years))
    def root(a):
        while parent[a]!=a:
            parent[a]=parent[parent[a]]
            a=parent[a]
        return a
    for a0,b0 in zip(fi,yi):
        parent[root(int(a0))]=root(g+int(b0))
    components=len({root(i) for i in range(len(parent))})
    rank_fe=g+len(years)-components
    cr1=(g/(g-1))*((n-1)/(n-rank_fe-k))
    scores=np.zeros((g,k))
    np.add.at(scores,fi,xw*residual[:,None])
    cov=cr1*xx_inv@(scores.T@scores)@xx_inv
    cov=(cov+cov.T)/2
    se=np.sqrt(np.maximum(np.diag(cov),0))
    p=2*student_t.sf(np.abs(b/se),df=g-1)
    delta=b[2]-b[0]
    diff_se=np.sqrt(cov[2,2]+cov[0,0]-2*cov[2,0])
    diff_p=float(2*student_t.sf(abs(delta/diff_se),df=g-1))
    out=dict(n=n,firms=g,coef=float(b[2]),se=float(se[2]),p=float(p[2]),
             diff=float(delta),diff_se=float(diff_se),diff_p=diff_p,
             iterations=iteration)
    if with_matrices:
        out.update(xw=xw,yw=yw,resid=residual,xx_inv=xx_inv,fi=fi,
                   firms_names=firms,b=b)
    return out


def row_info(d, ix):
    r=d.iloc[int(ix)]
    return dict(index=int(ix),edinetCode=str(r['edinetCode']),
                filerName=str(r['filerName']),secCode=str(r['secCode']),
                curr_docID=str(r['curr_docID']),
                filing_time=str(r['curr_submitDateTime']),
                event_date=str(r['eventTradingDate']),
                car_pp=float(r[CAR]),new_sentiment=float(r['newSentiment']),
                unchanged_sentiment=float(r['unchangedSentiment']),
                revised_sentiment=float(r['revisedSentiment']),
                extreme_car=bool(abs(r[CAR])>20))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--input',type=Path,default=Path('data/interim/paper2/regressions/stage9/regression_dataset.csv'))
    parser.add_argument('--output',type=Path,default=Path('data/interim/paper2/diagnostics/extreme_car'))
    parser.add_argument('--screen-top',type=int,default=150)
    parser.add_argument('--firm-top',type=int,default=40)
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    t0=time.perf_counter()
    raw=pd.read_csv(args.input,usecols=NEED,low_memory=False)
    main=raw.loc[raw.baseline_eligible.eq(True)].copy().reset_index(drop=True)
    assert len(main)==22266 and main.edinetCode.nunique()==4103, 'Wrong Stage 9 sample'
    base=fit(main,with_matrices=True)
    # Guard against accidentally analyzing a different regression specification.
    assert abs(base['coef']-0.24527446)<1e-5 and abs(base['se']-0.11328345)<1e-5
    assert abs(base['diff']-0.26757171)<1e-4
    screen_approx=(base['xw']@base['xx_inv'][:,2])*base['resid']
    approx_abs_order=np.argsort(-np.abs(screen_approx))
    tail_ix=np.flatnonzero(main[CAR].abs().to_numpy()>20)
    assert len(tail_ix)==96
    selected=sorted(set(approx_abs_order[:args.screen_top].tolist()+tail_ix.tolist()))
    print(f'Baseline replicated. Exactly refitting {len(selected)} selected rows; '
          f'96 extreme CAR events + first-order top {args.screen_top}... ',flush=True)
    exact=[]
    for j,ix in enumerate(selected):
        f=fit(main.drop(index=ix))
        rec=row_info(main,ix)
        rec.update(screen_delta_beta=float(screen_approx[ix]),
                   exact_delta_beta=base['coef']-f['coef'],
                   leave_one_out_beta=f['coef'],
                   leave_one_out_se=f['se'],leave_one_out_p=f['p'],
                   leave_one_out_contrast=f['diff'],
                   leave_one_out_contrast_p=f['diff_p'])
        exact.append(rec)
        if j%50==49:
            print(f'  completed {j+1}/{len(selected)} rows',flush=True)
    influence=pd.DataFrame(exact).sort_values('exact_delta_beta',key=lambda s: s.abs(),ascending=False)
    influence.to_csv(args.output/'influence_observations.csv',index=False,encoding='utf-8-sig')
    top= influence.sort_values('exact_delta_beta',key=lambda s:s.abs(),ascending=False)
    top_positive=influence.sort_values('exact_delta_beta',ascending=False)

    # Cluster deletion screening, then exact firm leave-one-cluster-out refits.
    approx_firm=np.bincount(base['fi'],weights=screen_approx,
                           minlength=len(base['firms_names']))
    top_firm_idx=np.argsort(-np.abs(approx_firm))[:args.firm_top]
    firm_rows=[]
    for idx in top_firm_idx:
        firm=base['firms_names'][idx]
        mask=main.edinetCode.astype(str).ne(firm)
        sub=main.loc[mask]
        f=fit(sub)
        df=main.loc[~mask]
        firm_rows.append(dict(edinetCode=str(firm),filerName=str(df.iloc[-1].filerName),
                              filings_removed=len(df),max_abs_car_pp=float(df[CAR].abs().max()),
                              screen_delta_beta=float(approx_firm[idx]),
                              exact_delta_beta=base['coef']-f['coef'],
                              leave_one_firm_out_beta=f['coef'],
                              leave_one_firm_out_se=f['se'],
                              leave_one_firm_out_p=f['p'],
                              leave_one_firm_out_contrast_p=f['diff_p']))
    firms=pd.DataFrame(firm_rows).sort_values('exact_delta_beta',key=lambda s:s.abs(),ascending=False)
    firms.to_csv(args.output/'influence_firms.csv',index=False,encoding='utf-8-sig')

    # Scenarios: diagnostic deletions are selected *using the observed outcome*;
    # NEVER present as a prespecified statistical alternative.
    scenarios=[]
    def collect(label,kept,notes='',y_override=None):
        f=fit(kept,outcome=y_override)
        scenarios.append(dict(scenario=label,N=f['n'],firms=f['firms'],
                              new_beta=f['coef'],new_se=f['se'],new_p=f['p'],
                              new_minus_unchanged=f['diff'],contrast_p=f['diff_p'],
                              note=notes))
    collect('Original baseline',main,'Canonical model A')
    collect('Exclude all |CAR|>20pp',main.loc[main[CAR].abs().le(20)],
            'Original pre-existing extreme CAR robustness')
    qlo,qhi=main[CAR].quantile([.01,.99]).tolist()
    collect('Winsorize CAR 1/99',main,
            f'Original pre-existing robustness; cap [{qlo:.5f},{qhi:.5f}] pp',
            y_override=main[CAR].clip(qlo,qhi).to_numpy(float))
    highest_car_ix=int(main[CAR].abs().idxmax())
    collect('Delete largest absolute CAR',main.drop(index=highest_car_ix),
            f'Original pre-existing single-event robustness (index {highest_car_ix})')
    # Individually confirmed most influential observations (all are part of exact refit set).
    abs_ids=top['index'].to_numpy(int)
    pos_ids=top_positive.loc[top_positive.exact_delta_beta.gt(0),'index'].to_numpy(int)
    for k in [1,5,10,20]:
        collect(f'Delete top {k} absolute coefficient-influence',
                main.drop(index=abs_ids[:k]),
                'EX POST coefficient-influence diagnostic; not confirmatory')
        collect(f'Delete top {k} positive coefficient-influence',
                main.drop(index=pos_ids[:k]),
                'EX POST outcome-selected diagnostic; not confirmatory')
    for k in [1,5,10]:
        ids=firms.head(k).edinetCode.tolist()
        collect(f'Delete top {k} absolute firm-influence',
                main.loc[~main.edinetCode.astype(str).isin(ids)],
                'EX POST cluster-deletion diagnostic; not confirmatory')
    summary=pd.DataFrame(scenarios)
    summary.to_csv(args.output/'influence_scenarios.csv',index=False,encoding='utf-8-sig')

    # Compact reproducible text summary; all counts/data derived from dataset and refits.
    top20=top.head(20)
    top20_tail=int(top20.extreme_car.sum())
    top50_tail=int(top.head(50).extreme_car.sum())
    top100_tail=int(top.head(100).extreme_car.sum())
    pos=int((influence['exact_delta_beta']>0).sum())
    tail=influence.loc[influence.extreme_car]
    largest=influence.loc[influence['index'].eq(highest_car_ix)].iloc[0]
    lines=['# Paper 2: Stage 9 influence diagnostic', '',
      'This analysis is computed solely from the frozen Stage 9 regression dataset.',
      'All model refits use identical two-way firm and fiscal-year fixed effects,',
      'seven controls, firm-clustered CR1 standard errors, and Student-t p-values',
      'with number of firm clusters minus one degrees of freedom.', '',
      '## Replication gates',
      f"- Baseline N={base['n']:,}, firms={base['firms']:,}.",
      f"- New-sentiment coefficient={base['coef']:.8f}, SE={base['se']:.8f}, p={base['p']:.8f}.",
      f"- New-minus-unchanged contrast={base['diff']:.8f}, p={base['diff_p']:.8f}.",
      f'- Extreme-return group: {len(tail_ix)} observations with |CAR|>20 pp.',
      f'- Exact leave-one-out refits: {len(selected)} observations, all 96 extreme-return filings',
      f'  and the {args.screen_top} largest absolute first-order coefficient-deletion scores.',
      f'- Exact leave-one-firm-out refits: {len(firms)} firms screened using firm-grouped first-order scores.',
      '', '## Findings',
      f'- Among largest 20 exactly evaluated absolute coefficient-influences, {top20_tail} have |CAR|>20 pp.',
      f'- Among largest 50 exactly evaluated absolute coefficient-influences, {top50_tail} have |CAR|>20 pp.',
      f'- Among largest 100 exactly evaluated absolute coefficient-influences, {top100_tail} have |CAR|>20 pp.',
      f"- The filing with the largest absolute CAR changes beta by {largest.exact_delta_beta:+.8f} when retained versus removed.",
      f'- Of the 96 extreme-CAR filings, {int((tail.exact_delta_beta>0).sum())} individually increase beta',
      f'  and {int((tail.exact_delta_beta<0).sum())} individually decrease beta on net (case deletion).',
      '', '## Scenario table', '',
      '| Diagnostic | N | beta new | p new | contrast p |',
      '|---|---:|---:|---:|---:|']
    for row in summary.itertuples(index=False):
        lines.append(f'| {row.scenario} | {row.N:,} | {row.new_beta:+.4f} | {row.new_p:.4f} | {row.contrast_p:.4f} |')
    lines += ['', '## Interpretation and limitations',
      '- The first-order scores are a screening approximation; exact results are based on',
      '  re-estimation with the selected observation or firm removed, including re-absorption',
      '  of fixed effects and recalculation of the clustered covariance matrix.',
      '- The selection of highly influential observations is based on realized data and is',
      '  **not a prespecified exclusion criterion**. Do not use these ex-post exclusions to',
      '  claim or refute statistical significance in a preferred paper specification.',
      '- Removing multiple observations need not equal the sum of their individual effects;',
      '  all listed joint-deletion scenarios are separately re-estimated.',
      '- These results assess coefficient stability, not the economic reason for price changes',
      '  and not the validity of stock-price or adjustment-factor data.',
      '- No LLM classification, sentiment scoring or original Stage 9 outputs were changed.',
      '', f'Elapsed processing time: {time.perf_counter()-t0:.1f}s.']
    (args.output/'influence_notes.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print('\nTOP 15 ABS COEFFICIENT INFLUENCES')
    cols=['filerName','event_date','car_pp','new_sentiment','exact_delta_beta',
          'leave_one_out_beta','leave_one_out_p','extreme_car']
    print(top[cols].head(15).to_string(index=False))
    print('\nTOP 12 FIRM INFLUENCES')
    print(firms.head(12).to_string(index=False))
    print('\nSCENARIOS')
    print(summary[['scenario','N','new_beta','new_p','contrast_p']].to_string(index=False))
    print(f'\nElapsed {time.perf_counter()-t0:.1f}s. Wrote 4 diagnostic files to {args.output}')


if __name__=='__main__':main()
