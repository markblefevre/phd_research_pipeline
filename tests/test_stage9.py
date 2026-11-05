import sys, unittest
from pathlib import Path
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from src.mdna_analysis.stage9 import fit,run_models,absorb

class TestStage9(unittest.TestCase):
    def test_absorption(self):
        v=np.arange(48,dtype=float).reshape(12,4)
        a,_,_=absorb(v,[np.repeat(np.arange(4),3),np.tile(np.arange(3),4)])
        self.assertTrue(np.allclose(a.mean(axis=0),0,atol=1e-10))
    def test_common_sample(self):
        rng=np.random.default_rng(481)
        nfirm=80;nyear=7;n=nfirm*nyear
        firms=np.repeat(np.arange(nfirm),nyear);years=np.tile(np.arange(nyear),nfirm)
        d=pd.DataFrame({'edinetCode':firms.astype(str),'fiscalYear':years+2018,'curr_docID':[f'D{i}' for i in range(n)]})
        cols=['unchangedSentiment','revisedSentiment','newSentiment','noveltyCNum','logLengthChange','ln_mktcap','book_to_market','roe_ttm','volatility_60d','log_avg_trading_value_60d','momentum_20d','momentum_60_to_5d']
        for c in cols: d[c]=rng.normal(size=n)
        d['car_pp_m1_1']=0.15*d.newSentiment+0.22*d.revisedSentiment+0.04*d.unchangedSentiment+rng.normal(size=n)
        d['baseline_eligible']=True
        d.loc[::11,'momentum_20d']=np.nan
        d.loc[::13,'momentum_60_to_5d']=np.nan
        for c in ['momentum_20d','momentum_60_to_5d']:
            d[f'eligible_{c}']=d.baseline_eligible & d[c].notna()
        cfg={'stage9_regressions':{'controls':cols[3:10]},'stage9_robustness':{'momentum_columns':cols[10:]}}
        summaries,coefficients,contrasts=run_models(d,cfg)
        self.assertEqual(len(summaries),5)
        self.assertEqual(len(contrasts),15)
        for m in cols[10:]:
            a=summaries.set_index('model')
            self.assertEqual(a.loc[f'B_restricted_{m}','sample_sha256'],a.loc[f'C_with_{m}','sample_sha256'])
        self.assertLess(float(summaries.loc[0,'joint_wald_p']),1.0)

if __name__=='__main__':unittest.main()
