#!/usr/bin/env python3
"""Stage 9 runner; run at repo root: python scripts/paper2/run_stage9.py --config configs/paper2/stage9.toml"""
import argparse, json, sys, tomllib
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from src.mdna_analysis.stage9 import build_dataset,run_models,sha256

STAGES=('stage9_dataset','stage9_regressions','stage9_robustness','stage9_reporting')

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--config',default='configs/paper2/stage9.toml')
    ap.add_argument('--root',type=Path,default=ROOT)
    args=ap.parse_args(); root=args.root.expanduser().resolve();cfg_path=(root/args.config).resolve()
    cfg=tomllib.loads(cfg_path.read_text())
    if cfg.get('run',{}).get('paper')!='paper2': raise ValueError('Expected paper2 config')
    flags=cfg['stages']; unknown=set(flags)-set(STAGES)
    if unknown: raise ValueError(f'Unknown Stage 9 toggle(s): {unknown}')
    enabled=[x for x in STAGES if flags.get(x,False)]
    # First version requires full, sequenced execution; no hidden reliance on prior artifact state.
    if enabled!=list(STAGES): raise ValueError('Stage 9 v1 requires all four stages enabled (dataset -> regression -> robustness -> reporting)')
    out=root/cfg['stage9_reporting']['output_dir']
    if out.exists(): raise FileExistsError(f'Refusing to overwrite Stage 9 output: {out}')
    dataset,missing,provenance=build_dataset(cfg,root)
    summary,coef,contrasts=run_models(dataset,cfg)
    out.mkdir(parents=True,exist_ok=False)
    dataset.to_csv(out/'regression_dataset.csv',index=False)
    missing.to_csv(out/'missingness.csv',index=False)
    summary.to_csv(out/'model_summary.csv',index=False)
    coef.to_csv(out/'coefficients.csv',index=False)
    contrasts.to_csv(out/'coefficient_contrasts.csv',index=False)
    (out/'provenance.json').write_text(json.dumps({'created_utc':datetime.now(timezone.utc).isoformat(),'config':str(cfg_path),'config_sha256':sha256(cfg_path),**provenance},indent=2)+'\n')
    print(summary[['model','N','firms','joint_wald_p']].to_string(index=False))
    print(f'Output: {out}')

if __name__=='__main__': main()
# After this, run the table generation
# From repo root
# python scripts/paper2/build_stage9_descriptive_tables.py
# Files are ccreated here data/interim/paper2/regressions/stage9/tables

