"""Paper 2 Stage 9: read-only input, versioned output econometrics."""
from __future__ import annotations
import hashlib, json, math
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import t as student_t, f as f_dist
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

SENTIMENT = ['unchangedSentiment','revisedSentiment','newSentiment']
CONTRASTS = [('newSentiment','unchangedSentiment'),('newSentiment','revisedSentiment'),('revisedSentiment','unchangedSentiment')]

def sha256(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
    return h.hexdigest()

def check_keys(df,name):
    if 'curr_docID' not in df: raise ValueError(f'{name}: no curr_docID')
    df['curr_docID']=df['curr_docID'].astype('string').str.strip()
    if df.curr_docID.isna().any() or df.curr_docID.eq('').any() or df.curr_docID.duplicated().any():
        raise ValueError(f'{name}: missing/duplicate filing IDs')
    return df

def is_true(s):
    q=s.astype('string').str.lower().str.strip()
    if not q.dropna().isin(['true','false','1','0']).all(): raise ValueError('Invalid eventStudySample flag')
    return q.isin(['true','1'])

def build_dataset(cfg,root):
    inp=cfg['stage9_dataset']; paths={k:root/v for k,v in [('panel_csv',inp['panel_csv']),('controls_csv',inp['controls_csv']),('sentiment_csv',inp['sentiment_csv'])]}
    for k,p in paths.items():
        if not p.is_file(): raise FileNotFoundError(f'{k}: {p}')
    p=check_keys(pd.read_csv(paths['panel_csv'],dtype={'curr_docID':'string','edinetCode':'string'},low_memory=False),'panel')
    c=check_keys(pd.read_csv(paths['controls_csv'],dtype={'curr_docID':'string'},low_memory=False),'controls')
    s=check_keys(pd.read_csv(paths['sentiment_csv'],dtype={'curr_docID':'string'},low_memory=False),'sentiment')
    # Prevent silently replacing five-way-derived sentiment with separate binary taxonomy.
    for name, cols in [('panel',['curr_docID','edinetCode','fiscalYear','eventStudySample_m1_1','car_pp_m1_1','noveltyCNum','logLengthChange']),('controls',['curr_docID','ln_mktcap','book_to_market','roe_ttm','volatility_60d','log_avg_trading_value_60d']),('sentiment',['curr_docID',*SENTIMENT])]:
        data={'panel':p,'controls':c,'sentiment':s}[name]
        if set(cols)-set(data): raise ValueError(f'{name}: missing columns {sorted(set(cols)-set(data))}')
    if not {'unchangedStatus','revisedStatus','newStatus'} <= set(s):
        raise ValueError('Stage 8E statuses missing; required to verify structural absence')
    if 'prev_docID' in p and 'prev_docID' in s:
        test=p[['curr_docID','prev_docID']].merge(s[['curr_docID','prev_docID']],on='curr_docID',how='inner',suffixes=('_panel','_sent'))
        if not test.prev_docID_panel.astype(str).equals(test.prev_docID_sent.astype(str)):
            raise ValueError('Previous-document ID mismatch between panel and scored corpus')
    if len(p)!=int(inp.get('expected_panel_rows',33046)) or len(s)!=len(p): raise ValueError('Unexpected panel or sentiment row count')
    merged=p.merge(s[['curr_docID',*SENTIMENT,'unchangedStatus','revisedStatus','newStatus']],on='curr_docID',validate='one_to_one',how='left',indicator='_score_join')
    if not merged._score_join.eq('both').all(): raise ValueError('Unmatched scores')
    merged=merged.drop(columns=['_score_join']).merge(c,on='curr_docID',validate='one_to_one',how='left',indicator='_control_join',suffixes=('','_control'))
    if int(merged._control_join.eq('both').sum()) != int(inp.get('expected_controls_rows',32126)):
        raise ValueError('Controls join count inconsistent with expected eligible events')
    merged['event_eligible']=is_true(merged['eventStudySample_m1_1'])
    fields=set(SENTIMENT+cfg['stage9_regressions']['controls']+cfg['stage9_robustness']['momentum_columns']+['car_pp_m1_1','fiscalYear'])
    for field in fields:
        if field not in merged: raise ValueError(f'Missing regression field: {field}')
        merged[field]=pd.to_numeric(merged[field],errors='coerce').replace([np.inf,-np.inf],np.nan)
    for name in SENTIMENT:
        status=merged[name.replace('Sentiment','Status')].astype('string')
        if not status.isin(['empty','scored','resumed']).all(): raise ValueError(f'{name}: invalid status')
        if (status.eq('empty') != merged[name].isna()).any(): raise ValueError(f'{name}: structural absence mismatch')
        if not merged[name].dropna().between(-1.000001,1.000001).all(): raise ValueError(f'{name}: out of bounds')
    if merged['edinetCode'].isna().any() or merged['curr_docID'].duplicated().any(): raise ValueError('Invalid panel keys')
    if (merged.event_eligible & merged.car_pp_m1_1.isna()).any(): raise ValueError('Eligible CAR missing')
    if ((~merged.event_eligible)&merged.car_pp_m1_1.notna()).any(): raise ValueError('CAR outside eligible sample')
    controls=cfg['stage9_regressions']['controls']; moms=cfg['stage9_robustness']['momentum_columns']
    eligible=merged.event_eligible
    core=['edinetCode','fiscalYear','car_pp_m1_1',*SENTIMENT,*controls]
    merged['baseline_eligible']=eligible & merged[core].notna().all(axis=1)
    for mom in moms: merged[f'eligible_{mom}']=merged.baseline_eligible & merged[mom].notna()
    counts={'panel':len(p),'matched_scores':len(s),'matched_controls':int(merged._control_join.eq('both').sum()),'eligible_car':int(eligible.sum()),'all_components_present':int((eligible&merged[SENTIMENT].notna().all(axis=1)).sum()),'baseline_eligible':int(merged.baseline_eligible.sum())}
    counts.update({f'eligible_{m}':int(merged[f'eligible_{m}'].sum()) for m in moms})
    missing=pd.DataFrame([{'variable':v,'missing_among_event_eligible':int(merged.loc[eligible,v].isna().sum()),'missing_among_baseline':int(merged.loc[merged.baseline_eligible,v].isna().sum())} for v in [*SENTIMENT,*controls,*moms]])
    provenance={'input_files':{k:{'path':str(v),'sha256':sha256(v)} for k,v in paths.items()},'counts':counts,'notes':['Scores are missing for empty components (not set to neutral).','Stage 8H inputs remain read-only.','No momentum in baseline eligibility.']}
    return merged,missing,provenance

def absorb(a,groups,tol=1e-11,iterations=10000):
    z=np.asarray(a,float).copy()
    for it in range(iterations):
        old=z.copy()
        for g in groups:
            sums=np.zeros((int(g.max())+1,z.shape[1]));np.add.at(sums,g,z)
            means=sums/np.bincount(g)[:,None]
            z-=means[g]
        if np.max(np.abs(z-old))<tol: return z,it+1,float(np.max(np.abs(z-old)))
    raise ValueError('Fixed-effect absorption failed to converge')

def fit(df, terms, tag):
    if len(df)<25: raise ValueError(f'{tag}: too few observations')
    firm,fi=np.unique(df.edinetCode.astype(str).values,return_inverse=True)
    year,yi=np.unique(df.fiscalYear.astype(int).values,return_inverse=True)
    z,nit,err=absorb(df[['car_pp_m1_1',*terms]].to_numpy(dtype=float),[fi,yi])
    y,x=z[:,0],z[:,1:];n,k=x.shape;g=len(firm)
    if np.linalg.matrix_rank(x)!=k: raise ValueError(f'{tag}: rank deficiency after FE absorption')
    # Rank of firm and year dummies: # firms + # years - # connected components.
    bipartite=coo_matrix((np.r_[np.ones(n),np.ones(n)],(np.r_[fi,g+yi],np.r_[g+yi,fi])),shape=(g+len(year),g+len(year))).tocsr()
    components,_=connected_components(bipartite,directed=False)
    fe_rank=g+len(year)-components; df_resid=n-fe_rank-k
    if df_resid<=0 or g<3: raise ValueError(f'{tag}: insufficient residual df/clusters')
    beta=np.linalg.lstsq(x,y,rcond=None)[0];u=y-x@beta;inv=np.linalg.inv(x.T@x)
    scores=np.zeros((g,k));np.add.at(scores,fi,x*u[:,None]);factor=g/(g-1)*(n-1)/df_resid
    cov=inv@(scores.T@scores)@inv*factor;cov=(cov+cov.T)/2
    se=np.sqrt(np.diag(cov)); tcrit=student_t.ppf(.975,g-1)
    if not np.isfinite(se).all() or (se<=0).any(): raise ValueError(f'{tag}: invalid SE')
    stats={'model':tag,'N':n,'firms':g,'years':len(year),'within_r2':float(1-u@u/(y@y)),'fe_rank':fe_rank,'cr1_factor':factor,'absorb_iterations':nit,'absorb_error':err}
    coef=[{'model':tag,'term':v,'coef':float(beta[j]),'se':float(se[j]),'p':float(2*student_t.sf(abs(beta[j]/se[j]),g-1)),'ci95_low':float(beta[j]-tcrit*se[j]),'ci95_high':float(beta[j]+tcrit*se[j]),'sample_sd':float(df[v].std()),'effect_per_sd':float(beta[j]*df[v].std())} for j,v in enumerate(terms)]
    contrasts=[]
    for a,b in CONTRASTS:
        vec=np.zeros(k);vec[terms.index(a)]=1;vec[terms.index(b)]=-1
        d=float(vec@beta);s=math.sqrt(float(vec@cov@vec))
        contrasts.append({'model':tag,'contrast':f'{a} - {b}','difference':d,'se':s,'p':float(2*student_t.sf(abs(d/s),g-1)),'ci95_low':float(d-tcrit*s),'ci95_high':float(d+tcrit*s)})
    r=np.zeros((2,k));r[0,terms.index('newSentiment')]=1;r[0,terms.index('unchangedSentiment')]=-1;r[1,terms.index('revisedSentiment')]=1;r[1,terms.index('unchangedSentiment')]=-1
    q=r@beta;vc=r@cov@r.T;f=float(q@np.linalg.solve(vc,q)/2)
    stats.update({'joint_wald_F':f,'joint_wald_df_num':2,'joint_wald_df_den':g-1,'joint_wald_p':float(f_dist.sf(f,2,g-1))})
    return stats,coef,contrasts

def run_models(dataset,cfg):
    base_terms=SENTIMENT+cfg['stage9_regressions']['controls']; moms=cfg['stage9_robustness']['momentum_columns']; rows=[];coeffs=[];tests=[]
    for tag,mask,terms in [('A_baseline',dataset.baseline_eligible,base_terms)]+[(f'B_restricted_{m}',dataset[f'eligible_{m}'],base_terms) for m in moms]+[(f'C_with_{m}',dataset[f'eligible_{m}'],base_terms+[m]) for m in moms]:
        data=dataset.loc[mask].copy();stat,co,ct=fit(data,terms,tag)
        # sample fingerprints make B/C identical membership verifiable
        stat['sample_sha256']=hashlib.sha256('\n'.join(sorted(data.curr_docID.astype(str))).encode()).hexdigest()
        rows.append(stat);coeffs.extend(co);tests.extend(ct)
    for m in moms:
        b=next(x for x in rows if x['model']==f'B_restricted_{m}');c=next(x for x in rows if x['model']==f'C_with_{m}')
        if (b['N'],b['sample_sha256'])!=(c['N'],c['sample_sha256']): raise ValueError('B/C common sample invariant failed')
    return pd.DataFrame(rows),pd.DataFrame(coeffs),pd.DataFrame(tests)
