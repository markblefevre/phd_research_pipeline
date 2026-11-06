"""Paper 2 Stage 9 sensitivity estimation library.

Called by the single scripts/paper2/build_stage9_tables.py entry point.
Works exclusively from frozen Stage 9 tables/data and never edits their inputs.
The full regression inventory is fixed in run() before inspecting estimates.
"""
from __future__ import annotations

import hashlib
import json
import sys
import scipy
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import t as student_t, f as fisher_f

PRIMARY = ['unchangedSentiment', 'revisedSentiment', 'newSentiment']
CONTROL = ['noveltyCNum', 'logLengthChange', 'ln_mktcap', 'book_to_market',
           'roe_ttm', 'volatility_60d', 'log_avg_trading_value_60d']
BASE_TERMS = PRIMARY + CONTROL
BINARY = ['persistentSentiment', 'novelSentiment']
MODEL = 'A_baseline'
CAR = 'car_pp_m1_1'
SOURCE_NAMES = ['regression_dataset.csv', 'model_summary.csv', 'coefficients.csv',
                'coefficient_contrasts.csv', 'provenance.json']


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for part in iter(lambda: f.read(1 << 20), b''):
            h.update(part)
    return h.hexdigest()


def within(z: np.ndarray, dimensions: list[np.ndarray], sizes: list[int],
           *, tol: float = 1e-12, max_iters: int = 400):
    """Alternating projections: absorb one or two categorical FE dimensions."""
    z = np.array(z, dtype=float, copy=True, order='C')
    counts = [np.bincount(ix, minlength=n) for ix, n in zip(dimensions, sizes)]
    for iteration in range(1, max_iters + 1):
        prev = z.copy()
        for ix, count, n in zip(dimensions, counts, sizes):
            for j in range(z.shape[1]):
                z[:, j] -= np.bincount(ix, weights=z[:, j], minlength=n)[ix] / count[ix]
        if np.max(np.abs(z - prev)) < tol:
            return z, iteration
    raise RuntimeError('FE absorption did not converge')


def fit(d: pd.DataFrame, terms: list[str], *, outcome: str = CAR,
        fe: str = 'firm_year', y_override=None, model_name: str = '') -> dict:
    """OLS FE within estimator, CR1 firm clustering, Student-t and Wald tests."""
    if d.empty:
        raise ValueError(f'Empty regression sample: {model_name}')
    if not d[['edinetCode', 'fiscalYear'] + terms].notna().all().all():
        raise ValueError(f'Missing regressor or identifier in {model_name}')
    y = (d[outcome].to_numpy(float) if y_override is None
         else np.asarray(y_override, dtype=float))
    x = d[terms].to_numpy(float)
    if len(y) != len(d) or not np.isfinite(x).all() or not np.isfinite(y).all():
        raise ValueError(f'Non-finite inputs: {model_name}')
    n, k = x.shape
    firm_names, firm_ix = np.unique(d['edinetCode'].astype(str), return_inverse=True)
    year_names, year_ix = np.unique(d['fiscalYear'], return_inverse=True)
    g = len(firm_names)
    groups = {'firm_year': [firm_ix, year_ix], 'year': [year_ix]}
    levels = {'firm_year': [g, len(year_names)], 'year': [len(year_names)]}
    if fe == 'industry_year':
        if d['industry_s17'].isna().any():
            raise ValueError('Industry FE missing')
        industry_names, industry_ix = np.unique(d['industry_s17'].astype(str), return_inverse=True)
        dimensions = [industry_ix, year_ix]
        sizes = [len(industry_names), len(year_names)]
    else:
        dimensions = groups[fe]
        sizes = levels[fe]
    z, iteration = within(np.column_stack([x, y]), dimensions, sizes)
    xw, yw = z[:, :k], z[:, k]
    if np.linalg.matrix_rank(xw) != k:
        raise ValueError(f'Rank-deficient regressors: {model_name}')
    # For two-way FE the full column rank is d1+d2−1 when the
    # two-way category incidence graph is connected. Check this condition.
    if len(dimensions) == 2:
        parent = np.arange(sizes[0] + sizes[1])
        def root(a):
            while parent[a] != a:
                parent[a] = parent[parent[a]]
                a = parent[a]
            return a
        for left, right in zip(dimensions[0], dimensions[1]):
            a = root(int(left)); b = root(sizes[0] + int(right))
            parent[a] = b
        components = len({root(i) for i in range(len(parent))})
        rank_fe = sum(sizes) - components
    else:
        components = 1
        rank_fe = sizes[0]
    if n <= rank_fe + k:
        raise ValueError('Nonpositive residual degrees of freedom')
    invxx = np.linalg.inv(xw.T @ xw)
    beta = invxx @ (xw.T @ yw)
    resid = yw - xw @ beta
    cr1 = (g / (g - 1)) * ((n - 1) / (n - rank_fe - k))
    scores = np.zeros((g, k))
    np.add.at(scores, firm_ix, xw * resid[:, None])
    cov = cr1 * invxx @ (scores.T @ scores) @ invxx
    cov = (cov + cov.T) / 2
    se = np.sqrt(np.maximum(np.diag(cov), 0))
    tcrit = student_t.ppf(0.975, df=g - 1)
    rows = []
    for i, term in enumerate(terms):
        p = 2 * student_t.sf(abs(beta[i] / se[i]), df=g - 1)
        rows.append(dict(model=model_name, term=term, coef=float(beta[i]), se=float(se[i]),
                         p=float(p), ci95_low=float(beta[i] - tcrit * se[i]),
                         ci95_high=float(beta[i] + tcrit * se[i]), N=n, firms=g))
    return dict(model=model_name, beta=beta, cov=cov, se=se, terms=terms,
                n=n, firms=g, years=len(year_names), fe=fe, fe_rank=rank_fe,
                fe_components=components, cr1_factor=cr1, iterations=iteration, rows=rows)


def contrast(fit_result: dict, left: str, right: str) -> dict:
    terms = fit_result['terms']
    a = np.zeros(len(terms))
    a[terms.index(left)] = 1
    a[terms.index(right)] = -1
    coef = float(a @ fit_result['beta'])
    se = float(np.sqrt(a @ fit_result['cov'] @ a))
    p = float(2 * student_t.sf(abs(coef / se), df=fit_result['firms'] - 1))
    tcrit = student_t.ppf(.975, fit_result['firms'] - 1)
    return dict(difference=coef, se=se, p=p,
                ci95_low=float(coef-tcrit*se), ci95_high=float(coef+tcrit*se))


def joint_equality(fit_result: dict) -> dict:
    if not all(t in fit_result['terms'] for t in PRIMARY):
        return dict(F=None, p=None)
    terms = fit_result['terms']
    R = np.zeros((2, len(terms)))
    for j, (a, b) in enumerate([(PRIMARY[2],PRIMARY[0]), (PRIMARY[1],PRIMARY[0])]):
        R[j,terms.index(a)] = 1
        R[j,terms.index(b)] = -1
    w = R @ fit_result['beta']
    V = R @ fit_result['cov'] @ R.T
    F = float(w @ np.linalg.solve(V,w) / 2)
    return {'F': F, 'p': float(fisher_f.sf(F,2,fit_result['firms']-1))}


def summary_row(res: dict, label: str, family: str, note: str = '') -> dict:
    lookup = {r['term']:r for r in res['rows']}
    c = contrast(res, PRIMARY[2], PRIMARY[0])
    joint = joint_equality(res)
    return dict(model=res['model'], label=label, family=family, N=res['n'], firms=res['firms'],
                b_unchanged=lookup['unchangedSentiment']['coef'],
                se_unchanged=lookup['unchangedSentiment']['se'],
                p_unchanged=lookup['unchangedSentiment']['p'],
                b_revised=lookup['revisedSentiment']['coef'],
                se_revised=lookup['revisedSentiment']['se'],
                p_revised=lookup['revisedSentiment']['p'],
                b_new=lookup['newSentiment']['coef'],
                se_new=lookup['newSentiment']['se'],
                p_new=lookup['newSentiment']['p'],
                diff_new_unchanged=c['difference'], diff_se=c['se'],
                diff_p=c['p'], diff_ci_low=c['ci95_low'], diff_ci_high=c['ci95_high'],
                joint_equality_F=joint['F'], joint_equality_p=joint['p'],
                fe=res['fe'], cr1_factor=res['cr1_factor'], note=note)


def latex_escape(s):
    return s.replace('&',r'\&').replace('%',r'\%').replace('_',r'\_')


def tex_table6(results: pd.DataFrame) -> str:
    """Main-text compact table: all three slopes and primary contrast."""
    def val(x, p, se):
        stars = '^{**}' if p < .05 else ('^{*}' if p < .10 else '')
        return f'${x:+.3f}{stars}$',f'({se:.3f})'
    row_lines=[]
    current_family=None
    for row in results.itertuples(index=False):
        if row.family != current_family:
            if current_family is not None:row_lines.append(r'\hline')
            current_family=row.family
            row_lines.append(r'\multicolumn{7}{l}{\emph{'+latex_escape(current_family)+r'}} \\')
        cols=[val(row.b_unchanged,row.p_unchanged,row.se_unchanged),
              val(row.b_revised,row.p_revised,row.se_revised),
              val(row.b_new,row.p_new,row.se_new),
              val(row.diff_new_unchanged,row.diff_p,row.diff_se)]
        row_lines.append(latex_escape(row.label)+' & '+ f'{row.N:,} & '+f'{row.firms:,} & '+
                         ' & '.join(z[0] for z in cols)+r' \\')
        row_lines.append(' & & & '+' & '.join(z[1] for z in cols)+r' \\')
    return '\n'.join([
        r'\begin{table}[htbp]',r'\centering',
        r'\caption{Robustness of three-component sentiment associations with abnormal returns}',
        r'\label{tab:stage9_robustness}',r'\footnotesize',r'\setlength{\tabcolsep}{3pt}',
        r'\begin{tabular}{lrrrrrr}',r'\hline\hline',
        r'Specification & $N$ & Firms & Unchanged & Revised & New & New $-$ unchanged \\',
        r'\hline',*row_lines,r'\hline\hline',r'\end{tabular}',r'\par\vspace{3pt}',
        r'\begin{minipage}{0.99\linewidth}',
        (r'\footnotesize\textit{Notes:} Coefficients and firm-clustered CR1 standard errors '
         r'(parentheses) are in abnormal-return percentage points per sentiment unit. '
         r'All specifications include the seven baseline controls unless the indicated '
         r'control is replaced; firm and fiscal-year fixed effects are used unless otherwise '
         r'noted. All models are estimated independently on the indicated sample. '
         r'The complete-case sample includes all three substantive components. '
         r'The presence-indicator model zero-codes absent components and includes separate '
         r'component-presence indicators; its slopes apply conditional on component presence. '
         r'The winsorized outcome is capped at the 1st and 99th percentiles of the original '
         r'primary-sample three-day CAR. '
         r'$^{**}p<0.05$, $^{*}p<0.10$ (two-sided, unadjusted). '
         r'The principal equality test is new minus unchanged, not the new-sentiment '
         r'coefficient alone. These are sensitivity and exploratory tests, not a new '
         r'confirmatory hypothesis family.'),
        r'\end{minipage}',r'\end{table}',''
    ])


def tex_binary(results: pd.DataFrame) -> str:
    lines=[r'\begin{table}[htbp]',r'\centering',
           r'\caption{Independent binary persistent/novel sentiment classification}',
           r'\label{tab:stage9_binary_robustness}',r'\small',r'\setlength{\tabcolsep}{5pt}',
           r'\begin{tabular}{lrr}',r'\hline\hline',
           r' & Three-way complete-case sample & Expanded binary sample \\',r'\hline']
    for c,label in [('persistentSentiment','Persistent sentiment'),('novelSentiment','Novel sentiment')]:
        values=[]
        for rr in results.itertuples(index=False):
            v=getattr(rr,f'b_{c}'); s=getattr(rr,f'se_{c}'); p=getattr(rr,f'p_{c}')
            stars='^{**}' if p<.05 else ('^{*}' if p<.1 else '')
            values.append((f'${v:+.4f}{stars}$',f'({s:.4f})'))
        lines += [label+' & '+' & '.join(v[0] for v in values)+r' \\',
                  ' & '+' & '.join(v[1] for v in values)+r' \\']
    for name,label,fmt in [('diff','Novel $-$ persistent','+.4f'),('p_diff','Difference $p$-value','.3f'),
                           ('N','Observations',',d'),('firms','Firms',',d')]:
        vv=[]
        for rr in results.itertuples(index=False):
            x=getattr(rr,name)
            vv.append(f'{x:{fmt}}')
        lines.append(label+' & '+' & '.join(vv)+r' \\')
    return '\n'.join(lines+[r'\hline\hline',r'\end{tabular}',r'\par\vspace{3pt}',
        r'\begin{minipage}{0.99\linewidth}',
        (r'\footnotesize\textit{Notes:} This binary persistent/novel classification '
         r'and sentiment scoring are independently constructed from the five-category '
         r'unchanged/revised/new design. The first column holds the primary three-category '
         r'complete-case sample fixed; the second expands to all CAR-valid filings with '
         r'both binary sentiment scores and complete seven-control information. '
         r'CAR is $[-1,+1]$ in percentage points. Both models include seven primary controls, '
         r'firm and year fixed effects, and firm-clustered CR1 standard errors. '
         r'Coefficient differences test novel minus persistent within each binary model. '
         r'The models should not be interpreted as a mechanical aggregation of three-category labels.'),
        r'\end{minipage}',r'\end{table}',''])


def tex_momentum(results: pd.DataFrame) -> str:
    lines=[r'\begin{table}[htbp]',r'\centering',
           r'\caption{Pre-filing momentum robustness: common-sample comparisons}',
           r'\label{tab:stage9_momentum_robustness}',r'\small',
           r'\begin{tabular}{lrrrr}',r'\hline\hline',
           r'Model & $N$ & New coefficient & New $-$ unchanged & Contrast $p$ \\',r'\hline']
    for rr in results.itertuples(index=False):
        lines.append(latex_escape(rr.label)+f' & {rr.N:,} & {rr.b_new:+.4f} & '
                     f'{rr.diff_new_unchanged:+.4f} & {rr.diff_p:.3f} '+r'\\')
    return '\n'.join(lines+[r'\hline\hline',r'\end{tabular}',r'\par\vspace{3pt}',
        r'\begin{minipage}{0.99\linewidth}',
        (r'\footnotesize\textit{Notes:} A is the full primary Model A without momentum. '
         r'B restricts the sample to that with observed momentum but does not include momentum. '
         r'C uses exactly the same restricted sample as B, adding the corresponding '
         r'pre-filing momentum control. CAR units are percentage points; firm/year FE and '
         r'firm-clustered standard errors throughout. Differences in p-values across '
         r'different samples are not independent tests of a change in coefficients.'),
        r'\end{minipage}',r'\end{table}',''])



def holm_adjusted(pvalues: list[float]) -> list[float]:
    """Holm step-down adjusted p-values, no dependency on statsmodels."""
    p = np.asarray(pvalues, dtype=float)
    order = np.argsort(p)
    ordered = p[order]
    adjusted = np.maximum.accumulate((len(p) - np.arange(len(p))) * ordered)
    adjusted = np.clip(adjusted, 0, 1)
    result = np.empty(len(p))
    result[order] = adjusted
    return result.tolist()


def tex_multiplicity(results: pd.DataFrame) -> str:
    lines=[r'\begin{table}[htbp]',r'\centering',
           r'\caption{Sensitivity of primary regression inference to multiplicity}',
           r'\label{tab:stage9_multiplicity}',r'\small',
           r'\begin{tabular}{llrr}',r'\hline\hline',
           r'Family & Test & Raw $p$ & Holm-adjusted $p$ \\',r'\hline']
    for r in results.itertuples(index=False):
        lines.append(latex_escape(r.family)+' & '+latex_escape(r.test)+
                     f' & {r.p_raw:.3f} & {r.p_holm:.3f} '+r'\\')
    return '\n'.join(lines+[r'\hline\hline',r'\end{tabular}',r'\par\vspace{3pt}',
        r'\begin{minipage}{0.99\linewidth}',
        (r'\footnotesize\textit{Notes:} The three individual component slopes are '
         r'one illustrative family and the three pairwise coefficient contrasts a second '
         r'family. Adjustments are computed separately within families, using Holm '
         r'step-down control of the family-wise error rate. The pre-designated '
         r'principal hypothesis is new minus unchanged (raw $p=0.157$); it does not '
         r'become significant under multiplicity correction. The component slopes '
         r'are auxiliary associations, not the primary test of unequal component '
         r'return relations. Filing-time and robustness subgroup tests are exploratory.'),
        r'\end{minipage}',r'\end{table}',''])

def run(stage9_dir: Path, output_dir: Path | None = None) -> dict:
    root = Path(stage9_dir)
    out = output_dir or (root / 'tables')
    sources = {name:root/name for name in SOURCE_NAMES}
    for p in sources.values():
        if not p.is_file():
            raise FileNotFoundError(f'Missing source: {p}')
    model_summary = pd.read_csv(sources['model_summary.csv']).set_index('model')
    canonical = model_summary.loc[MODEL]
    coef = pd.read_csv(sources['coefficients.csv'])
    coef = coef[coef.model.eq(MODEL)].set_index('term')
    cont = pd.read_csv(sources['coefficient_contrasts.csv'])
    cont = cont[cont.model.eq(MODEL)].set_index('contrast')
    required = sorted(set(BASE_TERMS+BINARY + ['edinetCode','fiscalYear','industry_s17',
        'baseline_eligible','carStatus_m1_1','noveltyCRaw','absLogLengthChange',
        'momentum_20d','momentum_60_to_5d','car_pp_m1_1','car_pp_0_0','car_pp_0_1',
        'unchangedStatus','revisedStatus','newStatus']))
    d = pd.read_csv(sources['regression_dataset.csv'], usecols=required, low_memory=False)
    main = d[d.baseline_eligible.eq(True)].copy().reset_index(drop=True)
    if (len(main),main.edinetCode.nunique(),main.fiscalYear.nunique()) != (
            int(canonical.N), int(canonical.firms), int(canonical.years)):
        raise ValueError('Primary sample differs from Model A provenance')
    all_car = d[d.carStatus_m1_1.eq('ok')].copy()
    base_control_complete = all_car[CONTROL].notna().all(axis=1)
    full_control = all_car[base_control_complete].copy()
    if not main[BASE_TERMS+[CAR]].notna().all().all():
        raise ValueError('Baseline incomplete')
    # Independent baseline reconstruction is a mandatory failure gate.
    baseline = fit(main,BASE_TERMS,model_name='baseline')
    max_beta = max(abs(baseline['beta'][i]-float(coef.loc[t,'coef'])) for i,t in enumerate(BASE_TERMS))
    max_se = max(abs(baseline['se'][i]-float(coef.loc[t,'se'])) for i,t in enumerate(BASE_TERMS))
    if (max_beta>1e-7 or max_se>1e-7
        or abs(baseline['cr1_factor']-float(canonical.cr1_factor))>1e-7):
        raise ValueError(f'Failed to replicate canonical Model A: beta {max_beta}; se {max_se}')
    for lab, left,right in [('newSentiment - unchangedSentiment',PRIMARY[2],PRIMARY[0]),
                           ('newSentiment - revisedSentiment',PRIMARY[2],PRIMARY[1]),
                           ('revisedSentiment - unchangedSentiment',PRIMARY[1],PRIMARY[0])]:
        test=contrast(baseline,left,right)
        if abs(test['difference']-float(cont.loc[lab,'difference']))>1e-7 or abs(test['se']-float(cont.loc[lab,'se']))>1e-7:
            raise ValueError('Canonical contrast replication failed: '+lab)
    rr = []
    full = []
    def collect(fit_result,label,family,note=''):
        rr.append(summary_row(fit_result,label,family,note))
        full.extend(fit_result['rows'])
    collect(baseline,r'Primary $[-1,+1]$','Panel A: Timing and sample', 'Original unmodified main model')
    collect(fit(main,BASE_TERMS,outcome='car_pp_0_1',model_name='CAR[0,+1]'),
            'CAR [0,+1]','Panel A: Timing and sample')
    collect(fit(main,BASE_TERMS,outcome='car_pp_0_0',model_name='CAR[0,0]'),
            'CAR [0,0]','Panel A: Timing and sample')
    no18=main[main.fiscalYear.ne(2018)].copy()
    collect(fit(no18,BASE_TERMS,model_name='exclude_fy2018'),
            'Exclude FY2018','Panel A: Timing and sample', 'Reporting-period end year 2018 removed')
    tail20=main[main[CAR].abs().le(20)].copy()
    collect(fit(tail20,BASE_TERMS,model_name='abs_car_le_20pp'),
            r'Exclude $|CAR|>20$ pp','Panel B: Return tails', 'Rule-based absolute CAR exclusion')
    q01,q99=main[CAR].quantile([.01,.99]).tolist()
    cap=main[CAR].clip(lower=q01,upper=q99).to_numpy(float)
    collect(fit(main,BASE_TERMS,y_override=cap,model_name='winsor_1_99'),
            'Winsorize CAR 1/99','Panel B: Return tails', f'CAR percentiles: {q01:.6f} to {q99:.6f} pp')
    # Influence diagnostic: remove the single largest absolute-CAR observation.
    idx_max=main[CAR].abs().idxmax()
    collect(fit(main.drop(index=idx_max),BASE_TERMS,model_name='drop_max_abs_car'),
            'Omit largest $|CAR|$','Panel B: Return tails',
            f'Excluded event {main.loc[idx_max,"edinetCode"]} (CAR={main.loc[idx_max,CAR]:.6f} pp)')
    alternate=BASE_TERMS.copy();alternate[alternate.index('noveltyCNum')]='noveltyCRaw'
    collect(fit(main,alternate,model_name='raw_number_novelty'),
            'Raw-number novelty','Panel C: Specification', 'Replace only normalized TF-IDF novelty')
    alternate2=BASE_TERMS.copy();alternate2[alternate2.index('logLengthChange')]='absLogLengthChange'
    collect(fit(main,alternate2,model_name='abs_length_change'),
            'Absolute length change','Panel C: Specification', 'Replace signed log length change')
    collect(fit(main,BASE_TERMS,fe='year',model_name='year_fe'),
            'Year FE only','Panel C: Specification', 'Omit firm FE, retain firm clustered SE')
    collect(fit(main,BASE_TERMS,fe='industry_year',model_name='industry_year_fe'),
            'Industry + year FE','Panel C: Specification', 'Use 17-sector industry group FE, not firm FE')
    # Extending complete cases with explicit category-presence controls.
    presence=full_control.copy().reset_index(drop=True)
    for name in PRIMARY:
        flag=name+'_present'
        presence[flag]=presence[name].notna().astype(float)
        presence[name]=presence[name].fillna(0.)
    flags=[t+'_present' for t in PRIMARY]
    absent_count={t:int((presence[t+'_present']==0).sum()) for t in PRIMARY}
    presfit=fit(presence,BASE_TERMS+flags,model_name='presence_indicators')
    collect(presfit,'Presence indicators','Panel D: Sample expansion',
            'Missing components zero-coded only with presence indicators')
    results=pd.DataFrame(rr)
    # The primary direct contrast must match canonical.
    if abs(results.iloc[0]['diff_new_unchanged']-float(cont.loc['newSentiment - unchangedSentiment','difference']))>1e-7:
        raise ValueError('Primary contrast mismatch in summary table')
    # Binary taxonomy: common sample and expanded binary-eligible sample.
    bin_full=full_control[full_control[BINARY].notna().all(axis=1)].copy()
    bin_rows=[]
    for name,label,frame in [('binary_matched','Matched three-way sample',main),
                              ('binary_full','Expanded binary sample',bin_full)]:
        if not frame[BINARY].notna().all().all():
            raise ValueError(f'Unexpected missing binary measure in {name}')
        bf=fit(frame,BINARY+CONTROL,model_name=name)
        full.extend(bf['rows'])
        vals={r['term']:r for r in bf['rows']}
        c=contrast(bf,'novelSentiment','persistentSentiment')
        bin_rows.append(dict(model=name,label=label,N=bf['n'],firms=bf['firms'],
                             b_persistentSentiment=vals['persistentSentiment']['coef'],
                             se_persistentSentiment=vals['persistentSentiment']['se'],
                             p_persistentSentiment=vals['persistentSentiment']['p'],
                             b_novelSentiment=vals['novelSentiment']['coef'],
                             se_novelSentiment=vals['novelSentiment']['se'],
                             p_novelSentiment=vals['novelSentiment']['p'],
                             diff=c['difference'],se_diff=c['se'],p_diff=c['p'],
                             ci_low=c['ci95_low'],ci_high=c['ci95_high']))
    binary=pd.DataFrame(bin_rows)
    # Momentum A/B/C results are canonical read-only Stage 9 outputs.
    ms=pd.read_csv(sources['model_summary.csv']).set_index('model')
    cs=pd.read_csv(sources['coefficients.csv'])
    ct=pd.read_csv(sources['coefficient_contrasts.csv'])
    momentum_rows=[]
    models=[('A_baseline','A: no momentum (full)'),
            ('B_restricted_momentum_20d','B: 20-day eligible'),
            ('C_with_momentum_20d','C: add 20-day momentum'),
            ('B_restricted_momentum_60_to_5d','B: 60-to-5 eligible'),
            ('C_with_momentum_60_to_5d','C: add 60-to-5 momentum')]
    for mod,label in models:
        c=cs[(cs.model==mod)&cs.term.eq('newSentiment')].iloc[0]
        z=ct[(ct.model==mod)&ct.contrast.eq('newSentiment - unchangedSentiment')].iloc[0]
        momentum_rows.append(dict(model=mod,label=label,N=int(ms.loc[mod,'N']),
                                  firms=int(ms.loc[mod,'firms']),b_new=float(c.coef),
                                  se_new=float(c.se),p_new=float(c.p),
                                  diff_new_unchanged=float(z.difference),
                                  se_diff=float(z.se),diff_p=float(z.p)))
    momentum=pd.DataFrame(momentum_rows)
    for mom in ['momentum_20d','momentum_60_to_5d']:
        expected=int(ms.loc['B_restricted_'+mom,'N'])
        actual=int(main[mom].notna().sum())
        if actual!=expected:
            raise ValueError(f'Momentum sample size mismatch: {mom} {actual} != {expected}')
    # Illustrative multiplicity checks for two transparent hypothesis families.
    coeff_labels=[('unchangedSentiment','Unchanged slope'),
                  ('revisedSentiment','Revised slope'),('newSentiment','New slope')]
    cont_labels=[('newSentiment - unchangedSentiment','New - unchanged'),
                 ('newSentiment - revisedSentiment','New - revised'),
                 ('revisedSentiment - unchangedSentiment','Revised - unchanged')]
    multiplicity_rows=[]
    for family,pairs,source in [('Three sentiment slopes',coeff_labels,coef),
                                ('Three pairwise contrasts',cont_labels,cont)]:
        ps=[float(source.loc[key,'p']) for key,_ in pairs]
        for (key,label),p,adjusted in zip(pairs,ps,holm_adjusted(ps)):
            multiplicity_rows.append(dict(family=family,test=label,term=key,
                                          p_raw=p,p_holm=adjusted))
    multiplicity=pd.DataFrame(multiplicity_rows)
    # All computations validated; write after tests pass. Outputs are replaceable generated artifacts.
    out.mkdir(parents=True,exist_ok=True)
    results.to_csv(out/'table6_stage9_robustness.csv',index=False)
    (out/'table6_stage9_robustness.tex').write_text(tex_table6(results),encoding='utf-8')
    binary.to_csv(out/'tableA2_binary_taxonomy.csv',index=False)
    (out/'tableA2_binary_taxonomy.tex').write_text(tex_binary(binary),encoding='utf-8')
    momentum.to_csv(out/'tableA3_momentum.csv',index=False)
    multiplicity.to_csv(out/'tableA4_multiplicity.csv',index=False)
    (out/'tableA4_multiplicity.tex').write_text(tex_multiplicity(multiplicity),encoding='utf-8')
    (out/'tableA3_momentum.tex').write_text(tex_momentum(momentum),encoding='utf-8')
    pd.DataFrame(full).to_csv(out/'stage9_robustness_all_coefficients.csv',index=False)
    diagnostics={'generated_utc':datetime.now(timezone.utc).isoformat(),
                 'source_sha256':{n:sha256(p) for n,p in sources.items()},
                 'software_versions':{'python':sys.version.split()[0], 'numpy':np.__version__,
                                      'pandas':pd.__version__, 'scipy':scipy.__version__},
                 'method':'OLS with absorbed firm/year or selected FE; firm-clustered CR1; Student-t G-1',
                 'baseline_replication_max_beta_diff':float(max_beta),
                 'baseline_replication_max_se_diff':float(max_se),
                 'baseline_n':len(main),'baseline_firms':int(main.edinetCode.nunique()),
                 'car_valid_n':len(all_car),'controls_complete_car_n':len(full_control),
                 'presence_n':len(presence),'presence_firms':int(presence.edinetCode.nunique()),
                 'absent_component_counts_in_presence_sample':absent_count,
                 'binary_matched_n':len(main),'binary_expanded_n':len(bin_full),
                 'fy2018_excluded_n':len(main)-len(no18),
                 'abs_car_gt_20_excluded_n':len(main)-len(tail20),
                 'winsor_quantiles_car_pp':[float(q01),float(q99)],
                 'largest_abs_car_excluded':{'edinetCode':str(main.loc[idx_max,'edinetCode']),
                                            'car_pp':float(main.loc[idx_max,CAR])},
                 'multiplicity_families':multiplicity.to_dict(orient='records'),
                 'primary_direct_contrasts':{lab:contrast(baseline,l,r)
                    for lab,l,r in [('new_minus_unchanged',PRIMARY[2],PRIMARY[0]),
                                    ('new_minus_revised',PRIMARY[2],PRIMARY[1]),
                                    ('revised_minus_unchanged',PRIMARY[1],PRIMARY[0])]},
                 'regression_inventory':[dict(name=r['model'],N=r['N'],firms=r['firms'],fe=r['fe'],note=r['note'])
                                          for r in rr],
                 'limitations':['No human-labeled five-category validation corpus supplied',
                                'No timestamp-matched preceding earnings announcements supplied',
                                'Frozen Stage 8A-C LMMD/BERT/Sol model-level scores not supplied',
                                'Three-category component length columns absent from Stage 9 dataset',
                                'Sensitivity tests are exploratory, not pre-registered']}
    (out/'table6_stage9_robustness_provenance.json').write_text(
        json.dumps(diagnostics,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print(f'Stage 9 robustness: Model A matched, max beta {max_beta:.3e}, SE {max_se:.3e}')
    print(f'  CAR-valid={len(all_car):,}, controls-complete={len(full_control):,}, '
          f'presence N={len(presence):,}, binary-expanded N={len(bin_full):,}')
    print(results[['model','N','b_new','p_new','diff_new_unchanged','diff_p','joint_equality_p']].to_string(index=False,
          float_format=lambda x:f'{x:.5f}'))
    print('Binary:',binary[['model','N','b_novelSentiment','p_novelSentiment','diff','p_diff']].to_string(index=False,
          float_format=lambda x:f'{x:.5f}'))
    print('Written to:',out)
    return {'main':results,'binary':binary,'momentum':momentum,'diagnostics':diagnostics}
