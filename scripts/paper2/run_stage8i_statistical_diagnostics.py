#!/usr/bin/env python3
"""Paper 2 — Stage 8I: statistical identification and measurement diagnostics.

Read-only inputs, new output directory, no LLM/API calls. Run at repo root:

 python -u scripts/paper2/run_stage8i_statistical_diagnostics.py

Core: Stage 8E validated three-way scores, Stage 8F same three-day event sample,
Stage 8H strictly pre-filing adjusted market controls, firm/year FE, firm-clustered
CR1 small-sample correction. Endpoints: three-day CAR as the retrospectively
chosen main research contrast; AR(+1) exploratory temporal endpoint.

Computes actual coefficient covariance, omnibus equality tests, pairwise contrasts,
Holm adjustments, raw and FE-adjusted predictor correlations, conditional VIFs,
within-firm variation, detectable-difference diagnostics, component text length,
missingness, and *separately labeled* length-share exposure and frozen binary
persistent/novel sensitivity models. No sequential model-selection decisions.

Do not treat "retrospectively chosen primary" as preregistration.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import f as f_dist, nct, spearmanr, t as t_dist

from run_stage8_twfe import absorb, fe_rank
from run_stage8e_validate_regressions import (
    BASE, DEFAULT_RUN, DEFAULT_PANEL, CONTROLS, SENTIMENT, WINDOWS,
    validate_scores, load_panel, event_flag, estimate as legacy_estimate,
)
from run_stage8f_event_day_decomposition import shared_sample

DEFAULT_ADJUSTED = BASE / 'controls/stage8h_adjusted_market_industry_event_controls.csv'
DEFAULT_OUT = BASE / 'regressions/stage8i_identification_diagnostics'
CATEGORIES = ('unchanged', 'revised', 'new')
CHARS = [f'{x}Chars' for x in CATEGORIES]
BINARY = ['persistentSentiment', 'novelSentiment']
ENDPOINTS = ('car_pp_m1_1', 'ar_pp_p1')
PAIR_COMPARISONS = (
    ('newSentiment', 'unchangedSentiment'),
    ('newSentiment', 'revisedSentiment'),
    ('revisedSentiment', 'unchangedSentiment'),
)


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as src:
        for block in iter(lambda: src.read(2 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def json_safe(x):
    if isinstance(x, Path):
        return str(x)
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        return float(x) if np.isfinite(x) else None
    if isinstance(x, (np.bool_,)):
        return bool(x)
    if isinstance(x, dict):
        return {str(k): json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [json_safe(v) for v in x]
    return x


def require(condition, message):
    if not condition:
        raise ValueError(message)


def standardize_ids(df, label):
    df['curr_docID'] = df['curr_docID'].astype('string').str.strip()
    require(df['curr_docID'].notna().all() and df['curr_docID'].ne('').all(),
            f'{label}: missing filing IDs')
    require(df['curr_docID'].is_unique, f'{label}: duplicate filing IDs')
    return df


def prepare_binary(joined, panel_path):
    required = {'curr_docID', *BINARY}
    header = set(pd.read_csv(panel_path, nrows=0).columns)
    require(required <= header, 'Frozen Stage 6F-B binary sentiment missing from Stage 8 panel: '
            + str(sorted(required - header)))
    vals = standardize_ids(pd.read_csv(panel_path, usecols=sorted(required),
                          dtype={'curr_docID': 'string'}, low_memory=False), 'binary panel')
    require(len(vals) == 33046, 'Binary panel length unexpected')
    for name in BINARY:
        vals[name] = pd.to_numeric(vals[name], errors='coerce')
        require(vals[name].dropna().between(-1.000001, 1.000001).all(),
                f'Binary {name} out of allowed range')
    return joined.merge(vals, on='curr_docID', how='left', validate='one_to_one')


def prepare_lengths(joined, run_dir):
    fields = ['curr_docID', *CHARS, *(f'{c}Status' for c in CATEGORIES)]
    path = run_dir / 'three_component_sentiment.csv'
    lengths = standardize_ids(pd.read_csv(path, usecols=fields,
                    dtype={'curr_docID': 'string'}, low_memory=False), 'lengths')
    for c in CHARS:
        lengths[c] = pd.to_numeric(lengths[c], errors='coerce')
        require(lengths[c].notna().all() and (lengths[c] >= 0).all(), f'Invalid {c}')
    for c in CATEGORIES:
        require(lengths[f'{c}Status'].isin(['empty','scored','resumed']).all(),
                f'{c}: invalid status')
    return joined.merge(lengths, on='curr_docID', how='left', validate='one_to_one')


def extract_eligible(joined):
    valid = pd.Series(True, index=joined.index)
    for flag, y in WINDOWS.values():
        valid &= event_flag(joined[flag]) & joined[y].notna()
    e = joined.loc[valid].copy()
    e['ar_pp_p1'] = e['car_pp_0_1'] - e['car_pp_0_0']
    e['ar_pp_m1'] = e['car_pp_m1_1'] - e['car_pp_0_1']
    require(np.allclose(e['ar_pp_m1'] + e['car_pp_0_0'] + e['ar_pp_p1'],
                        e['car_pp_m1_1'], rtol=1e-11, atol=1e-10),
            'Stage 8F event decomposition differs')
    return e


def sample_from_eligible(e, terms):
    use = ['curr_docID', 'edinetCode', 'fiscalYear', *ENDPOINTS, *terms]
    require(set(use) <= set(e.columns), 'Required columns absent from eligible data')
    df = e[use].dropna().copy()
    require(len(df) > 1000 and df.edinetCode.nunique() > 100, 'Too few records in regression')
    require(df.curr_docID.is_unique, 'Duplicate IDs in analytic sample')
    require(np.all(np.mod(df.fiscalYear.to_numpy(dtype=float), 1) == 0), 'Noninteger years')
    df['fiscalYear'] = df['fiscalYear'].astype(int).astype(str)
    df['edinetCode'] = df['edinetCode'].astype(str)
    return df


def build_exposure(joined):
    """Separate estimand: category-average sentiment × category share OF 3 categories.

    The denominator excludes non-substantive/uncertain text by construction.
    Include 2 independent share covariates to avoid confusing tone exposure with
    amount of text, and presence indicators for the optional presence specification.
    """
    out = joined.copy()
    total = out[CHARS].sum(axis=1)
    positive = total.gt(0)
    require(positive.sum() >= max(1, math.floor(.9 * len(out))),
            'Unexpected number of zero-length filings')
    for c in CATEGORIES:
        size = out[f'{c}Chars'].astype(float)
        share = (size / total.where(positive)).fillna(0.0)
        out[f'{c}Share3'] = share
        raw = out[f'{c}Sentiment']
        # Zero-category exposure is structurally zero. Positive text without a
        # score is NOT silently zero-imputed and must not be used.
        bad = raw.isna() & size.gt(0)
        # An unscorable nonempty category is MISSING, not zero sentiment.
        # Exclude those filings from exposure regressions while retaining
        # them for score-presence descriptive diagnostics.
        out[f'{c}Exposure'] = (raw.fillna(0.0) * share).mask(bad)
        out[f'{c}Present'] = raw.notna().astype(float)
    summation = out[[f'{c}Share3' for c in CATEGORIES]].sum(axis=1)
    require(np.allclose(summation[positive].to_numpy(), 1.0, atol=1e-11),
            'Category shares do not sum to 1')
    return out


def adjusted_vif(xw, terms):
    """Conditional VIF after FE absorption, with other covariates still in X."""
    x = np.asarray(xw, dtype=float)
    gram = x.T @ x
    iv = np.linalg.inv(gram)
    scale = np.diag(gram)
    values = np.maximum(1.0, np.diag(iv) * scale)
    return {term: float(v) for term, v in zip(terms, values)}


def fit_clustered(sample: pd.DataFrame, outcome: str, terms: list[str],
                  design: str, analysis: str, model_family: str):
    """Faithful extension of validated Stage 8E CR1 estimator to expose vcov."""
    y = sample[outcome].to_numpy(dtype=float)
    x = sample[terms].to_numpy(dtype=float)
    require(np.isfinite(y).all() and np.isfinite(x).all(), 'Nonfinite regression data')
    firms, fi = np.unique(sample['edinetCode'].to_numpy(), return_inverse=True)
    years, yi = np.unique(sample['fiscalYear'].to_numpy(), return_inverse=True)
    groups = [fi, yi] if design == 'firm_year_fe' else [yi]
    transformed, iterations, error = absorb(np.column_stack((y, x)), groups)
    yw, xw = transformed[:, 0], transformed[:, 1:]
    current_terms = terms.copy()
    n = len(y)
    k = len(current_terms)
    dropped = []
    rank = int(np.linalg.matrix_rank(xw))
    if rank != k:
        # Exact match to Stage 8E's rare-unidentified-presence treatment.
        mandatory = [i for i, term in enumerate(current_terms) if not term.endswith('Present')]
        if np.linalg.matrix_rank(xw[:, mandatory]) != len(mandatory):
            raise ValueError(f'{model_family}/{analysis}: rank deficient mandatory regressors')
        keep = mandatory.copy()
        current_rank = len(keep)
        for j, term in enumerate(current_terms):
            if j in mandatory:
                continue
            r = np.linalg.matrix_rank(xw[:, keep + [j]])
            if r > current_rank:
                keep.append(j)
                current_rank = r
            else:
                dropped.append(term)
        keep.sort()
        xw = xw[:, keep]
        current_terms = [current_terms[i] for i in keep]
        k = len(current_terms)
        rank = int(np.linalg.matrix_rank(xw))
        require(rank == k, f'{model_family}/{analysis}: remaining rank deficiency')
    fe_dof, components = fe_rank(fi, yi, design == 'firm_year_fe')
    df_resid = n - fe_dof - k
    require(df_resid > 0 and len(firms) >= 100, 'Too few clusters/residual df')
    beta = np.linalg.lstsq(xw, yw, rcond=None)[0]
    u = yw - xw @ beta
    inv = np.linalg.inv(xw.T @ xw)
    scores = xw * u[:, None]
    cluster_scores = np.zeros((len(firms), k))
    np.add.at(cluster_scores, fi, scores)
    g = len(firms)
    factor = (g / (g - 1)) * ((n - 1) / df_resid)
    v = inv @ (cluster_scores.T @ cluster_scores) @ inv * factor
    v = (v + v.T) / 2
    se = np.sqrt(np.diag(v))
    require(np.isfinite(se).all() and np.all(se > 0), 'Invalid CR1 SE')
    p = 2 * t_dist.sf(np.abs(beta / se), df=g - 1)
    row = {
        'family': model_family, 'analysis': analysis, 'outcome': outcome,
        'spec': design, 'N': n, 'firms': g, 'years': len(years),
        'within_r2': float(1-u@u/(yw@yw)) if yw@yw > 0 else np.nan,
        'fe_rank': fe_dof, 'fe_components': components,
        'absorb_iterations': iterations, 'absorb_error': error,
        'cr1_factor': factor, 'dropped_presence_flags': ','.join(dropped),
    }
    fit = {
        'model': row, 'beta': beta, 'cov': v, 'se': se, 'p': p,
        'terms': current_terms, 'vif': adjusted_vif(xw, current_terms),
        'xw': xw, 'clusters': fi, 'g': g,
        'transformed_std': np.std(xw, axis=0, ddof=1),
        'raw_std': np.std(sample[current_terms].to_numpy(dtype=float),axis=0,ddof=1),
    }
    return fit


def contrast(fit, left, right, *, confidence=.95):
    names = fit['terms']
    require(left in names and right in names, f'Contrast missing {left}/{right}')
    a = np.zeros(len(names))
    a[names.index(left)] = 1.0
    a[names.index(right)] = -1.0
    diff = float(a @ fit['beta'])
    var = float(a @ fit['cov'] @ a)
    require(var > 0, f'Invalid variance of {left}-{right}')
    se = math.sqrt(var)
    g = fit['g']
    crit = t_dist.ppf((1 + confidence) / 2, df=g - 1)
    pv = float(2 * t_dist.sf(abs(diff / se), df=g - 1))
    return {'contrast': f'{left} - {right}', 'difference': diff, 'se_cluster_firm': se,
            'p': pv, 'ci95_low': diff - crit * se,
            'ci95_high': diff + crit * se, 'firms': g}


def joint_equality(fit, variables):
    require(len(variables) >= 3, 'Joint test requires >=3 quantities')
    names = fit['terms']
    require(all(z in names for z in variables), 'Joint-test regressors missing')
    base = variables[0]
    q = len(variables) - 1
    r = np.zeros((q, len(names)))
    for i, other in enumerate(variables[1:]):
        r[i, names.index(base)] = 1
        r[i, names.index(other)] = -1
    d = r @ fit['beta']
    rv = r @ fit['cov'] @ r.T
    wald = float(d @ np.linalg.solve(rv, d))
    f_value = wald / q
    pv = float(f_dist.sf(f_value, q, fit['g'] - 1))
    require(np.isfinite(pv) and 0 <= pv <= 1, 'Invalid joint test p-value')
    return {'null': ' = '.join(variables), 'restrictions': q, 'wald_chi2_approx': wald,
            'F_cluster_approx': f_value, 'df_num': q, 'df_den': fit['g'] - 1,
            'p': pv, 'inference_note': 'approx F(q,G-1), CR1 firm-cluster covariance'}


def minimum_detectable(se, g, power=.8, alpha=.05):
    require(se > 0 and g > 1, 'Bad SE/clusters for MDE')
    df = g - 1
    cutoff = t_dist.ppf(1-alpha/2, df)
    def achieved(ncp):
        return float(nct.sf(cutoff, df, ncp) + nct.cdf(-cutoff, df, ncp))
    root = brentq(lambda d: achieved(d)-power, 0, 6)
    return float(root * se)


def holm_adjust(values):
    p = np.asarray(values, dtype=float)
    require(np.isfinite(p).all() and ((p>=0)&(p<=1)).all(), 'Invalid p for Holm')
    order = np.argsort(p, kind='stable')
    adjusted = np.empty(len(p))
    running = 0.0
    for i, j in enumerate(order):
        running = max(running, (len(p)-i) * p[j])
        adjusted[j] = min(1.0, running)
    return adjusted


def record_fit(fit, model_rows, coefficient_rows, test_rows, wald_rows, cov_rows,
               vif_rows, mde_rows, compare=None):
    base = fit['model']
    model_rows.append(base.copy())
    cols = fit['terms']
    for i, name in enumerate(cols):
        coefficient_rows.append({**base, 'term': name, 'coef': float(fit['beta'][i]),
                'se_cluster_firm': float(fit['se'][i]), 'p': float(fit['p'][i]),
                'within_fe_sd': float(fit['transformed_std'][i]),
                'raw_predictor_sd': float(fit['raw_std'][i]),
                'estimated_effect_per_1sd_predictor':float(fit['beta'][i]*fit['raw_std'][i])})
        vif_rows.append({**{key:base[key] for key in ('family','analysis','outcome','N')},
                         'term':name,'conditional_vif':fit['vif'][name]})
    if compare:
        for left, right in compare:
            c = contrast(fit, left, right)
            test_rows.append({**{key:base[key] for key in ('family','analysis','outcome','spec','N','firms')}, **c})
            mde_rows.append({**{key:base[key] for key in ('family','analysis','outcome','N','firms')},
                             'contrast':c['contrast'], 'se':c['se_cluster_firm'],
                             'mde_80_power':minimum_detectable(c['se_cluster_firm'],fit['g'],.8),
                             'mde_90_power':minimum_detectable(c['se_cluster_firm'],fit['g'],.9),
                             'mde_80_power_bonferroni_3':minimum_detectable(c['se_cluster_firm'],fit['g'],.8,alpha=.05/3),
                             'confidence_95_width':c['ci95_high']-c['ci95_low'],
                             'units':'CAR/AR percentage points per 1 sentiment unit; ex-post SE fixed'})
        if len(compare)>=2 and len(cols)>=3:
            vars_ = list(dict.fromkeys(z for ab in compare for z in ab))
            if len(vars_) == 3:
                wald_rows.append({**{key:base[key] for key in ('family','analysis','outcome','N','firms')},
                                  **joint_equality(fit, vars_)})
        variables = list(dict.fromkeys(z for ab in compare for z in ab))
        for a in variables:
            for b in variables:
                ia, ib = cols.index(a), cols.index(b)
                cov = fit['cov'][ia, ib]
                corr = cov/(fit['se'][ia]*fit['se'][ib])
                cov_rows.append({**{key:base[key] for key in ('family','analysis','outcome')},
                                 'term_a':a, 'term_b':b,
                                 'covariance':float(cov),'coefficient_corr':float(corr)})


def raw_and_fe_correlations(sample, analysis, corr_rows, variation_rows):
    pre = sample[SENTIMENT].copy()
    fi = pd.factorize(sample.edinetCode, sort=True)[0]
    yi = pd.factorize(sample.fiscalYear, sort=True)[0]
    z, _, _ = absorb(pre.to_numpy(dtype=float), [fi, yi])
    for tag, arr in [('raw', pre.to_numpy(dtype=float)), ('firm_year_fe_residual', z)]:
        pearson = np.corrcoef(arr.T)
        for i,a in enumerate(SENTIMENT):
            for j,b in enumerate(SENTIMENT):
                if j <= i:
                    continue
                spear = float(spearmanr(arr[:,i], arr[:,j]).statistic)
                corr_rows.append({'analysis':analysis,'transformation':tag,
                    'term_a':a,'term_b':b, 'pearson':float(pearson[i,j]),
                    'spearman':spear,'N':len(sample)})
    for idx, name in enumerate(SENTIMENT):
        s = sample[name].astype(float)
        sd = sample.groupby('edinetCode', sort=False)[name].agg(['count','std'])
        eligible = sd['count'].ge(2)
        between = sd['std'].fillna(0)
        variation_rows.append({'analysis':analysis,'term':name,'N':len(sample),
           'raw_mean':float(s.mean()),'raw_sd':float(s.std()),
           'firm_year_FE_sd':float(np.std(z[:,idx],ddof=1)),
           'firms':int(len(sd)),'firms_two_plus':int(eligible.sum()),
           'firms_two_plus_nonzero_within_sd':int((eligible & between.gt(1e-10)).sum()),
           'median_firm_within_sd':float(between[eligible].median()) if eligible.any() else np.nan,
           'p10_firm_within_sd':float(between[eligible].quantile(.1)) if eligible.any() else np.nan})


def length_diagnostics(joined, analytic_ids, length_rows, bins_rows, pattern_rows):
    e = joined.loc[joined.curr_docID.isin(analytic_ids)].copy()
    require(len(e) == len(analytic_ids), 'Length diagnostic join mismatch')
    total = e[CHARS].sum(axis=1)
    for c in CATEGORIES:
        name, chars, status = f'{c}Sentiment', f'{c}Chars', f'{c}Status'
        present = e[name].notna()
        share = e[chars].div(total.where(total>0))
        length_rows.append({'component':c,'sample':'joint_eligible_with_controls',
            'N':len(e),'scored':int(present.sum()), 'empty':int((~present).sum()),
            'empty_with_positive_chars':int(((~present)&e[chars].gt(0)).sum()),
            'pct_scored':float(present.mean()*100),
            'median_chars_scored':float(e.loc[present,chars].median()),
            'p10_chars_scored':float(e.loc[present,chars].quantile(.1)),
            'p90_chars_scored':float(e.loc[present,chars].quantile(.9)),
            'pct_scored_lt_100_chars':float((e.loc[present,chars]<100).mean()*100),
            'pct_scored_lt_500_chars':float((e.loc[present,chars]<500).mean()*100),
            'median_share_of_3classified_chars':float(share.median()),
            'corr_abs_sentiment_log1p_chars':float(np.corrcoef(e.loc[present,name].abs(),
                     np.log1p(e.loc[present,chars]))[0,1]) if present.sum()>5 else np.nan})
        frame = e.loc[present,[chars,name]].copy()
        frame['length_bin'] = pd.cut(frame[chars],[-np.inf,99,499,1999,np.inf],
                                labels=['<100','100-499','500-1999','2000+'])
        for bucket, group in frame.groupby('length_bin',observed=False):
            vals = group[name]
            bins_rows.append({'component':c,'length_bin':str(bucket),'N':len(group),
             'mean_score':float(vals.mean()) if len(group) else np.nan,
             'sd_score':float(vals.std()) if len(group)>1 else np.nan,
             'fraction_abs_ge_0_75':float(vals.abs().ge(.75).mean()) if len(group) else np.nan,
             'median_chars':float(group[chars].median()) if len(group) else np.nan})
    patt = e[[f'{c}Status' for c in CATEGORIES]].apply(
                lambda row: '|'.join('1' if v in ('scored','resumed') else '0'
                                      for v in row), axis=1)
    e['_pat'] = patt
    for pat, group in e.groupby('_pat',sort=True):
        pattern_rows.append({'pattern_unchanged_revised_new':pat,
           'N':len(group), 'pct':len(group)/len(e)*100,
           'mean_car_m1_1_pp':float(group.car_pp_m1_1.mean()),
           'mean_ar_p1_pp':float((group.car_pp_0_1-group.car_pp_0_0).mean())})


def append_adjustments(contrasts, coeff, wald):
    """Transparent exploratory inference families. No post-hoc filtering."""
    families = []
    c = pd.DataFrame(contrasts)
    filt_three = c.family.eq('three_intensity')
    # Primary 3 contrast family. One outcome / complete case.
    primary = filt_three & c.analysis.eq('complete_case') & c.outcome.eq('car_pp_m1_1')
    families.append(('primary_3_pairwise_car_complete', c.loc[primary]))
    # All 2 samples × 2 outcomes × 3 contrasts, intentionally stringent.
    families.append(('all_12_three_component_pairwise', c.loc[filt_three]))
    f_expo = c.family.eq('share_weighted_exposure')
    families.append(('exploratory_12_exposure_pairwise', c.loc[f_expo]))
    # The frozen binary estimates answer a separate question and are not pooled
    # with the primary family; all its actual tests are a separate family.
    f_binary = c.family.str.startswith('binary_')
    families.append(('supplementary_binary_all_contrasts',c.loc[f_binary]))
    adjusted=[]
    for family,subset in families:
        if subset.empty:
            continue
        vals=holm_adjust(subset.p.to_numpy(dtype=float))
        for (idx,row),hp in zip(subset.iterrows(),vals):
            adjusted.append({'family':family, 'test_type':'coefficient_difference',
                'model_family':row['family'],'analysis':row['analysis'],
                'outcome':row['outcome'],'hypothesis':row['contrast'],
                'p_raw':row['p'],'p_holm':float(hp),'family_size':len(subset)})
    k = pd.DataFrame(coeff)
    singles=k[(k.family.eq('three_intensity')) & (k.term.eq('newSentiment'))]
    if not singles.empty:
        vals=holm_adjust(singles.p.to_numpy(dtype=float))
        for (_,row),hp in zip(singles.iterrows(),vals):
            adjusted.append({'family':'all_4_three_component_new_slope',
                'test_type':'individual_beta','model_family':row['family'],
                'analysis':row['analysis'],'outcome':row['outcome'],
                'hypothesis':row['term'],'p_raw':row['p'],
                'p_holm':float(hp),'family_size':len(singles)})
    w=pd.DataFrame(wald)
    for name, subset in [('all_4_three_component_joint_wald',
                          w[w.family.eq('three_intensity')]),
                         ('all_4_exposure_joint_wald',
                          w[w.family.eq('share_weighted_exposure')])]:
        if subset.empty:
            continue
        vals=holm_adjust(subset.p.to_numpy(dtype=float))
        for (_,row),hp in zip(subset.iterrows(),vals):
            adjusted.append({'family':name,'test_type':'joint_wald',
                'model_family':row['family'],'analysis':row['analysis'],
                'outcome':row['outcome'],'hypothesis':row['null'],
                'p_raw':row['p'],'p_holm':float(hp),'family_size':len(subset)})
    return adjusted


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-dir',type=Path,default=DEFAULT_RUN)
    p.add_argument('--panel',type=Path,default=DEFAULT_PANEL)
    p.add_argument('--controls',type=Path,default=DEFAULT_ADJUSTED)
    p.add_argument('--output-dir',type=Path,default=DEFAULT_OUT)
    p.add_argument('--skip-legacy-crosscheck',action='store_true',
                   help='Skip independent agreement check against frozen Stage 8E estimator')
    args=p.parse_args()
    for key in ('run_dir','panel','controls','output_dir'):
        setattr(args,key,getattr(args,key).expanduser())
    require(not args.output_dir.exists(),f'Refusing to overwrite {args.output_dir}; choose a new --output-dir')
    for path in (args.panel,args.controls,args.run_dir/'three_component_sentiment.csv',
                 args.run_dir/'manifest.json'):
        require(path.is_file(),f'Missing required input: {path}')

    scores,validated=validate_scores(args.run_dir)
    joined,coverage=load_panel(args.panel,args.controls,scores)
    joined=prepare_lengths(joined,args.run_dir)
    joined=prepare_binary(joined,args.panel)
    # An explicit common *event-window* sample, but each analysis differs by
    # component presence and observable controls, as in frozen Stage 8F.
    eligible=extract_eligible(joined)
    eligible_count=len(eligible)
    print(f'Joint event-window eligible: {eligible_count:,}',flush=True)
    names=['models','coefficients','contrasts','joint_wald','covariance','variance_decomposition','vif',
           'mde','correlations','variation','length_summary','length_bins',
           'presence_patterns','samples']
    results={name:[] for name in names}
    full_samples={}
    fits={}

    for analysis in ('complete_case','presence_indicators'):
        # Reuse exact frozen Stage 8F shared-sample logic, then join back
        # for secondary length and binary regression designs.
        sample,terms,stats=shared_sample(joined,analysis)
        full_samples[analysis]=sample.copy()
        results['samples'].append({'family':'three_intensity','analysis':analysis,**stats})
        print(f'\nThree-component {analysis}: N={len(sample):,} firms={stats["firms"]:,}',flush=True)
        raw_and_fe_correlations(sample,analysis,results['correlations'],results['variation'])
        for outcome in ENDPOINTS:
            fit=fit_clustered(sample,outcome,terms,'firm_year_fe',analysis,'three_intensity')
            record_fit(fit,*[results[z] for z in ('models','coefficients','contrasts','joint_wald',
                       'covariance','vif','mde')],compare=PAIR_COMPARISONS)
            fits[(analysis,outcome)]=fit
            new_p=fit['p'][fit['terms'].index('newSentiment')]
            print(f'  {outcome}: N={len(sample):,}, beta_new={fit["beta"][fit["terms"].index("newSentiment")]:+.4f} p={new_p:.4g}',flush=True)
            if not args.skip_legacy_crosscheck and outcome=='car_pp_m1_1':
                # Refit with original estimator and fail on any unexpected
                # coefficient/CR1 covariance disagreement.
                _,legacy_coefs,legacy_pairs=legacy_estimate(sample,outcome,terms.copy(),
                    'firm_year_fe',analysis,'8i_crosscheck')
                for rec in legacy_coefs:
                    k=fit['terms'].index(rec['term'])
                    require(np.isclose(fit['beta'][k],rec['coef'],rtol=1e-7,atol=1e-9),
                            'Stage 8I beta does not match Stage 8E estimator')
                    require(np.isclose(fit['se'][k],rec['se_cluster_firm'],rtol=1e-7,atol=1e-9),
                            'Stage 8I SE does not match Stage 8E estimator')
                for rec in legacy_pairs:
                    a,b=rec['contrast'].split(' - ')
                    assert np.isclose(contrast(fit,a,b)['p'],rec['p'],rtol=1e-7,atol=1e-9)
                print(f'  Validated equivalence to frozen 8E CR1 inference: {analysis}',flush=True)

    # Length and presence descriptions computed on all event-window-eligible
    # observations that ALSO have core controls (without requiring sentiments).
    diagnostic_common=eligible.dropna(subset=CONTROLS)
    length_diagnostics(diagnostic_common,set(diagnostic_common.curr_docID),
          results['length_summary'],results['length_bins'],results['presence_patterns'])

    # Separate exposure estimand: tone * category fraction, controlling
    # independent revised/new category shares. No changes to original outcome.
    # All three categories need not exist; empty category exposure=0.
    exposure_e=build_exposure(eligible)
    exposure_terms=[f'{c}Exposure' for c in CATEGORIES]
    share_controls=['revisedShare3','newShare3']
    for analysis in ('complete_case','presence_indicators'):
        terms=exposure_terms+share_controls+CONTROLS
        if analysis=='presence_indicators':
            terms += [f'{c}Present' for c in CATEGORIES]
        if analysis=='complete_case':
            allowed=set(full_samples[analysis].curr_docID)
            pool=exposure_e.loc[exposure_e.curr_docID.isin(allowed)].copy()
        else:
            pool=exposure_e.copy()
        sample=sample_from_eligible(pool,terms)
        print(f'\nExploratory share-weighted exposure {analysis}: N={len(sample):,}',flush=True)
        results['samples'].append({'family':'share_weighted_exposure','analysis':analysis,
                 'eligible_before_covariate_selection':len(pool),
                 'shared_sample_n':len(sample),'firms':sample.edinetCode.nunique()})
        exposure_pairs=[('newExposure','unchangedExposure'),('newExposure','revisedExposure'),
                        ('revisedExposure','unchangedExposure')]
        for outcome in ENDPOINTS:
            fit=fit_clustered(sample,outcome,terms,'firm_year_fe',analysis,'share_weighted_exposure')
            record_fit(fit,*[results[z] for z in ('models','coefficients','contrasts','joint_wald',
                       'covariance','vif','mde')],compare=exposure_pairs)

    # Frozen Stage 6F-B binary persistence/novelty, separate estimand.
    # Fit on full binary-available event sample, and separately fit on the
    # same three-way complete-case and presence-indicator filing sets.
    for analysis in ('binary_full','binary_on_three_complete_case',
                     'binary_on_three_presence_indicators'):
        if analysis=='binary_full':
            pool=eligible.copy()
        else:
            target='complete_case' if 'complete_case' in analysis else 'presence_indicators'
            allowed=set(full_samples[target].curr_docID)
            pool=eligible.loc[eligible.curr_docID.isin(allowed)].copy()
        binary_terms=BINARY+CONTROLS
        sample=sample_from_eligible(pool,binary_terms)
        results['samples'].append({'family':'binary_persistent_novel','analysis':analysis,
              'shared_sample_n':len(sample),'firms':sample.edinetCode.nunique()})
        print(f'\nBinary {analysis}: N={len(sample):,}',flush=True)
        for outcome in ENDPOINTS:
            fit=fit_clustered(sample,outcome,binary_terms,'firm_year_fe',analysis,
                              'binary_persistent_novel')
            record_fit(fit,*[results[z] for z in ('models','coefficients','contrasts','joint_wald',
                       'covariance','vif','mde')],compare=[('novelSentiment','persistentSentiment')])

    # Var(b_a-b_b) = Var(b_a) + Var(b_b) - 2*Cov(b_a,b_b).
    # This table directly diagnoses precision, beyond comparing individual p-values.
    for key,fit in fits.items():
        analysis,outcome=key
        for a,b in PAIR_COMPARISONS:
            ia,ib=fit['terms'].index(a),fit['terms'].index(b)
            va=float(fit['cov'][ia,ia]);vb=float(fit['cov'][ib,ib]);cov=float(fit['cov'][ia,ib])
            total=va+vb-2*cov
            require(np.isclose(total,contrast(fit,a,b)['se_cluster_firm']**2,rtol=1e-9,atol=1e-12),
                    'Contrast variance decomposition failed')
            results['variance_decomposition'].append({'analysis':analysis,'outcome':outcome,
               'contrast':f'{a} - {b}','variance_left':va,'variance_right':vb,
               'minus_twice_covariance':-2*cov,'difference_variance':total,
               'difference_se':math.sqrt(total)})

    adjustments=append_adjustments(results['contrasts'],results['coefficients'],results['joint_wald'])
    results['multiple_testing']=adjustments
    args.output_dir.mkdir(parents=True,exist_ok=False)
    output_names={
      'models':'model_summary.csv','coefficients':'coefficients.csv',
      'contrasts':'coefficient_contrasts.csv','joint_wald':'joint_equality_tests.csv',
      'covariance':'sentiment_coefficient_covariance.csv',
      'variance_decomposition':'contrast_variance_decomposition.csv',
      'vif':'conditional_vif.csv','mde':'minimum_detectable_differences.csv',
      'correlations':'predictor_correlations.csv','variation':'within_firm_variation.csv',
      'length_summary':'component_length_summary.csv','length_bins':'component_length_bins.csv',
      'presence_patterns':'component_presence_patterns.csv',
      'samples':'sample_diagnostics.csv','multiple_testing':'multiple_testing_adjusted.csv',
    }
    for key,name in output_names.items():
        pd.DataFrame(results[key]).to_csv(args.output_dir/name,index=False)
    try:
        git_sha=subprocess.run(['git','rev-parse','HEAD'],check=True,capture_output=True,
                text=True,timeout=5).stdout.strip()
    except (OSError,subprocess.SubprocessError):
        git_sha=None
    meta={
      'created_at_utc':datetime.now(timezone.utc).isoformat(),
      'script':'scripts/paper2/run_stage8i_statistical_diagnostics.py',
      'git_commit_at_execution':git_sha,
      'score_validation':validated, 'join_coverage':coverage,
      'input_files':{key:{'path':str(path),'sha256':checksum(path)} for key,path in {
       'three_component_sentiment':args.run_dir/'three_component_sentiment.csv',
       'three_component_manifest':args.run_dir/'manifest.json',
       'panel':args.panel,'stage8h_controls':args.controls}.items()},
      'event_window_eligible':eligible_count,
      'primary_outcome_retrospectively_designated':'car_pp_m1_1',
      'exploratory_outcome':'ar_pp_p1',
      'primary_inference':'three_intensity, complete_case, firm_year_fe, firm-clustered CR1',
      'interpretation':'Associations not causal; textual novelty != first public information arrival.',
      'multiple_testing_families':[name for name in pd.DataFrame(adjustments).family.unique()],
      'length_shares':'Component character counts / sum of three category character counts, NOT entire filing',
      'exposure_warning':'Distinct estimand: share-weighted sentiment, with revised/new share controls; exploratory only',
      'binary_warning':'Stage 6E persistent/novel and five-way unchanged/revised/new are distinct classifications and separate estimands',
      'power_warning':'Ex-post MDE holds observed CR1 SE fixed and uses noncentral-t approximation; not post-study power',
      'joint_wald_note':'F approximation q,G-1 using clustered CR1 covariance; small-sample exactness not claimed',
      'pre_registration':'No hypotheses or outcomes here were prospectively preregistered',
      'original_frozen_artifacts_unchanged':True,
    }
    (args.output_dir/'run_metadata.json').write_text(json.dumps(json_safe(meta),indent=2)+'\n',encoding='utf-8')
    main=results['contrasts']
    print('\n=== MAIN THREE-COMPONENT CONTRASTS (complete-case, CAR[-1,+1]) ===',flush=True)
    for item in main:
        if item['family']=='three_intensity' and item['analysis']=='complete_case' and item['outcome']=='car_pp_m1_1':
            print(f"  {item['contrast']}: delta={item['difference']:+.5f} "
                  f"95%CI=[{item['ci95_low']:+.5f},{item['ci95_high']:+.5f}] "
                  f"p={item['p']:.4g}",flush=True)
    for item in results['joint_wald']:
        if item['family']=='three_intensity' and item['analysis']=='complete_case' and item['outcome']=='car_pp_m1_1':
            print(f"  Joint equal slopes: F({item['df_num']},{item['df_den']})="
                  f"{item['F_cluster_approx']:.4f}, p={item['p']:.4g}",flush=True)
    print('\n=== MAIN THREE-COMPONENT CONTRASTS: Holm across 3 ===',flush=True)
    for item in adjustments:
        if item['family']=='primary_3_pairwise_car_complete':
            print(f"  {item['hypothesis']}: p={item['p_raw']:.4g}, "
                  f"Holm p={item['p_holm']:.4g}",flush=True)
    print('\n=== MAIN PRECISION AND COLLINEARITY ===',flush=True)
    for item in results['mde']:
        if item['family']=='three_intensity' and item['analysis']=='complete_case' and item['outcome']=='car_pp_m1_1':
            print(f"  {item['contrast']}: SE={item['se']:.4f}, "
                  f"MDE80={item['mde_80_power']:.4f}, "
                  f"MDE80(Bonf/3)={item['mde_80_power_bonferroni_3']:.4f}",flush=True)
    for item in results['vif']:
        if item['family']=='three_intensity' and item['analysis']=='complete_case' and item['outcome']=='car_pp_m1_1' and item['term'] in SENTIMENT:
            print(f"  Conditional VIF {item['term']}: {item['conditional_vif']:.3f}",flush=True)
    print('\n=== SUPPLEMENTARY FROZEN BINARY PERSISTENT / NOVEL (NOT SAME ESTIMAND) ===',flush=True)
    for item in results['contrasts']:
        if item['family']=='binary_persistent_novel' and item['outcome']=='car_pp_m1_1':
            print(f"  {item['analysis']} N={item['N']:,}: "
                  f"novel-persistent={item['difference']:+.4f}, "
                  f"p={item['p']:.4g}",flush=True)
    print(f'\nStage 8I completed. Outputs: {args.output_dir}',flush=True)
    print('Review coefficient_contrasts.csv, joint_equality_tests.csv, '
          'multiple_testing_adjusted.csv, and sample_diagnostics.csv first.',flush=True)


if __name__=='__main__':
    main()
