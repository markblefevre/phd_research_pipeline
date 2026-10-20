#!/usr/bin/env python3
"""Synthetic data tests for Stage 8I; no project data, API or network required."""
from __future__ import annotations
import unittest
import numpy as np
import pandas as pd
from run_stage8i_statistical_diagnostics import (
    SENTIMENT, CONTROLS, PAIR_COMPARISONS,
    fit_clustered, contrast, joint_equality,
    holm_adjust, minimum_detectable, build_exposure,
    record_fit, append_adjustments,
)
from run_stage8e_validate_regressions import estimate


class Stage8ITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rng=np.random.default_rng(20261020)
        firms=160
        years=6
        n=firms*years
        f=np.repeat(np.arange(firms),years)
        t=np.tile(np.arange(years),firms)
        shared=rng.normal(size=n)
        u=0.4*shared+rng.normal(size=n)
        r=0.3*shared+rng.normal(size=n)
        new=0.35*shared+rng.normal(size=n)
        d=pd.DataFrame({'curr_docID':[f'SYNI{i:05d}' for i in range(n)],
           'edinetCode':[f'E{i:05d}' for i in f],
           'fiscalYear':[str(2017+x) for x in t],
           'unchangedSentiment':u,'revisedSentiment':r,'newSentiment':new})
        for v in CONTROLS:
            d[v]=rng.normal(size=n)
        y=0.11*u+0.07*r+0.30*new
        for v in CONTROLS:
            y+=0.01*d[v].to_numpy()
        y+=rng.normal(size=firms)[f]+rng.normal(size=years)[t]+rng.normal(scale=0.55,size=n)
        d['car_pp_m1_1']=y
        d['ar_pp_p1']=0.45*y+rng.normal(scale=.2,size=n)
        cls.d=d

    def test_covariance_reproduces_frozen_estimator(self):
        d=self.d
        terms=SENTIMENT+CONTROLS
        x=fit_clustered(d,'car_pp_m1_1',terms,'firm_year_fe','complete_case','three_intensity')
        _,rows,pairs=estimate(d,'car_pp_m1_1',terms,'firm_year_fe','complete_case','test')
        for z in rows:
            i=x['terms'].index(z['term'])
            self.assertAlmostEqual(x['beta'][i],z['coef'],places=10)
            self.assertAlmostEqual(x['se'][i],z['se_cluster_firm'],places=10)
            self.assertAlmostEqual(x['p'][i],z['p'],places=10)
        for z in pairs:
            a,b=z['contrast'].split(' - ')
            d=contrast(x,a,b)
            self.assertAlmostEqual(d['difference'],z['difference'],places=10)
            self.assertAlmostEqual(d['se_cluster_firm'],z['se_cluster_firm'],places=10)
            self.assertAlmostEqual(d['p'],z['p'],places=10)
            self.assertLessEqual(d['ci95_low'],d['difference'])
            self.assertGreaterEqual(d['ci95_high'],d['difference'])
        joint=joint_equality(x,SENTIMENT)
        self.assertEqual(joint['df_num'],2)
        self.assertTrue(0<joint['p']<=1)
        self.assertTrue(np.isfinite(x['cov']).all())
        self.assertTrue(all(v>=1 for v in x['vif'].values()))

    def test_holm_and_mde(self):
        p=[0.015,0.003,0.8,0.02]
        adj=holm_adjust(p)
        self.assertTrue(np.all(np.asarray(adj)>=np.asarray(p)-1e-14))
        self.assertAlmostEqual(adj[1],0.012)
        self.assertAlmostEqual(adj[0],0.045)
        self.assertGreater(minimum_detectable(.2,150,power=.9),
                           minimum_detectable(.2,150,power=.8))
        self.assertGreater(minimum_detectable(.2,150),0)

    def test_exposure_shares_and_zero_category(self):
        a=pd.DataFrame({
          'unchangedChars':[100,50], 'revisedChars':[50,50], 'newChars':[0,100],
          'unchangedSentiment':[.1,.2], 'revisedSentiment':[.3,-.4],
          'newSentiment':[np.nan,.5]})
        x=build_exposure(a)
        self.assertEqual(float(x.loc[0,'newExposure']),0)
        self.assertEqual(float(x.loc[0,'newShare3']),0)
        self.assertAlmostEqual(float(x.loc[1,'newShare3']),.5)
        self.assertAlmostEqual(float(x.loc[1,'newExposure']),.25)
        b=a.copy()
        b.loc[1,'newSentiment']=np.nan
        x2=build_exposure(b)
        self.assertTrue(np.isnan(x2.loc[1,'newExposure']))

    def test_pres_indicators_identification(self):
        d=self.d.copy()
        rng=np.random.default_rng(2)
        for name in SENTIMENT:
            pres=rng.uniform(size=len(d))>0.1
            d[f'{name}Present']=pres.astype(float)
            d.loc[~pres,name]=0.0
        terms=SENTIMENT+[f'{v}Present' for v in SENTIMENT]+CONTROLS
        fit=fit_clustered(d,'ar_pp_p1',terms,'firm_year_fe',
                          'presence_indicators','three_intensity')
        self.assertTrue(all(v in fit['terms'] for v in SENTIMENT))
        self.assertEqual(fit['g'],160)
        models,coefs,contrasts,wald,cov,vif,mde=[],[],[],[],[],[],[]
        record_fit(fit,models,coefs,contrasts,wald,cov,vif,mde,
                   compare=PAIR_COMPARISONS)
        adj=append_adjustments(contrasts,coefs,wald)
        self.assertTrue(any(z['family']=='all_12_three_component_pairwise' for z in adj))
        self.assertTrue(any(z['test_type']=='joint_wald' for z in adj))


if __name__=='__main__':
    unittest.main(verbosity=2)
