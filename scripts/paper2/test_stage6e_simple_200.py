#!/usr/bin/env python3
"""Isolated, resumable 200-row Stage 6E alternative-definition pilot.

Default is READ-ONLY preflight. --run makes synchronous OpenAI calls and
writes ONLY into a new validation/simplified_200_v1 directory. --finalize
produces comparisons only after all 200 have valid responses.

The original Stage 6E production CSV, prompt, and all outputs remain untouched.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path('data/interim/paper2/alignment/persistent_novel/validation')
SOURCE = ROOT / 'stage6e_validation_sample_200_seed20261014.csv'
PROMPT = Path('configs/paper2/prompts/persistent_novel_simple_200_v1.md')
OUTPUT = ROOT / 'simplified_200_v1'
MODEL = 'gpt-6-luna'
LABELS = {'unchanged', 'revised', 'new_candidate', 'non_substantive', 'uncertain'}
CONFIDENCE = {'high', 'mid', 'low'}
KEYS = ('curr_docID', 'current_sentence_index')


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def text(value) -> str:
    return '' if pd.isna(value) else str(value)


def load_sample(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={'curr_docID': 'string', 'prev_docID': 'string'})
    required = {'sample_group', 'current_sentence', 'label', *KEYS,
                'top1_prior_index', 'top1_prior_sentence',
                'top2_prior_index', 'top2_prior_sentence',
                'top3_prior_index', 'top3_prior_sentence'}
    if missing := required - set(df.columns):
        raise ValueError(f'Missing sample columns: {sorted(missing)}')
    if len(df) != 200 or df.duplicated(list(KEYS)).any():
        raise ValueError('Expected exactly 200 unique (curr_docID, sentence_index) observations')
    if df['sample_group'].value_counts().to_dict() != {
        'representative': 100,
        'high_similarity_novel': 25,
        'low_similarity_persistent': 25,
        'lower_confidence_novel': 25,
        'lower_confidence_persistent': 25,
    }:
        raise ValueError('Sample composition differs from the frozen 200-row design')
    return df


def row_id(row) -> str:
    return f'{row["curr_docID"]}::C{int(row["current_sentence_index"])}'


def input_for(row) -> str:
    # No original labels, reasons, confidence, or cosine scores are sent.
    candidates = []
    seen = set()
    for i in (1, 2, 3):
        ix = row[f'top{i}_prior_index']
        sentence = text(row[f'top{i}_prior_sentence']).strip()
        if pd.isna(ix) or not sentence:
            continue
        ix = int(ix)
        if ix in seen:
            continue
        seen.add(ix)
        candidates.append(f'P{ix}: {sentence}')
    return ('## Current-year sentence\n'
            f'C{int(row["current_sentence_index"])}: {text(row["current_sentence"])}\n\n'
            '## Retrieved prior-year candidates (not the entire report)\n'
            + ('\n'.join(candidates) if candidates else '(none)'))


def parse_response(raw: str, valid_prior_indices: set[int]) -> dict:
    s = raw.strip()
    if s.startswith('```'):
        s = s.split('\n', 1)[1].rsplit('```', 1)[0].strip()
    obj = json.loads(s)
    if not isinstance(obj, dict):
        raise ValueError('Model response must be a JSON object')
    label = obj.get('label')
    conf = obj.get('confidence')
    prior = obj.get('prior_index_used')
    if label not in LABELS or conf not in CONFIDENCE:
        raise ValueError(f'Invalid label/confidence: {label!r}, {conf!r}')
    if prior is not None:
        if isinstance(prior, bool):
            raise ValueError('prior_index_used cannot be a boolean')
        prior = int(prior)
        if prior not in valid_prior_indices:
            raise ValueError(f'Prior index {prior} not in retrieved candidates')
    reason = obj.get('reason')
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError('Missing explanation')
    return {'label': label, 'confidence': conf, 'prior_index_used': prior,
            'reason': reason.strip()}


def prior_indices(row) -> set[int]:
    return {int(row[f'top{i}_prior_index']) for i in (1, 2, 3)
            if not pd.isna(row[f'top{i}_prior_index'])
            and text(row[f'top{i}_prior_sentence']).strip()}


def read_checkpoint(path: Path, expected_ids: set[str]) -> dict[str, dict]:
    completed = {}
    if not path.exists():
        return completed
    with path.open(encoding='utf-8') as f:
        for line_no, line in enumerate(f, 1):
            if not line.strip():
                continue
            item = json.loads(line)
            rid = item['row_id']
            if rid not in expected_ids or rid in completed:
                raise ValueError(f'Unexpected or duplicate checkpoint row {rid} at line {line_no}')
            completed[rid] = item
    return completed


def extract_text(response) -> str:
    if getattr(response, 'output_text', None):
        return str(response.output_text)
    return '\n'.join(str(c.text) for item in getattr(response, 'output', [])
                     for c in getattr(item, 'content', [])
                     if getattr(c, 'text', None))


def call_one(client, model, prompt, message, indices, max_output_tokens, retries):
    for attempt in range(1, retries + 1):
        try:
            response = client.responses.create(
                model=model, instructions=prompt, input=message,
                max_output_tokens=max_output_tokens, store=False)
            result = parse_response(extract_text(response), indices)
            usage = getattr(response, 'usage', None)
            return result, {
                'response_id': getattr(response, 'id', None),
                'response_model': getattr(response, 'model', None),
                'input_tokens': getattr(usage, 'input_tokens', None),
                'output_tokens': getattr(usage, 'output_tokens', None),
            }
        except Exception as e:
            if attempt == retries:
                raise
            print(f'  Attempt {attempt}/{retries} failed: {e}', file=sys.stderr)
            time.sleep(min(2 ** attempt, 15))


def create_or_check_manifest(out: Path, meta: dict):
    manifest = out / 'experiment_manifest.json'
    if manifest.exists():
        old = json.loads(manifest.read_text(encoding='utf-8'))
        for key in ('source_sha256', 'prompt_sha256', 'model', 'n_rows'):
            if old.get(key) != meta.get(key):
                raise RuntimeError(f'Experiment already exists with a different {key}. '
                                   'Choose a new --output directory; do not mix runs.')
    else:
        if any(out.iterdir()):
            raise RuntimeError(f'{out} is nonempty without a manifest; refusing to write')
        with manifest.open('x', encoding='utf-8') as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)


def finalize(df, checkpoint, out):
    if len(checkpoint) != len(df):
        raise RuntimeError(f'Cannot finalize: {len(checkpoint)}/{len(df)} complete')
    rows = []
    for row in df.to_dict('records'):
        rid = row_id(row)
        rec = checkpoint[rid]
        rows.append({
            'row_id': rid, 'sample_group': row['sample_group'],
            'prev_docID': row['prev_docID'], 'curr_docID': row['curr_docID'],
            'current_sentence_index': int(row['current_sentence_index']),
            'current_sentence': row['current_sentence'],
            'original_stage6e_label': row['label'],
            'simple_label': rec['classification']['label'],
            'simple_confidence': rec['classification']['confidence'],
            'simple_prior_index_used': rec['classification']['prior_index_used'],
            'simple_reason': rec['classification']['reason'],
            'top1_prior_sentence': row['top1_prior_sentence'],
            'top2_prior_sentence': row['top2_prior_sentence'],
            'top3_prior_sentence': row['top3_prior_sentence'],
        })
    result = pd.DataFrame(rows)
    # No overwriting, even within the experimental directory.
    compare = out / 'stage6e_simple_200_comparison.csv'
    with compare.open('x', encoding='utf-8', newline='') as f:
        result.to_csv(f, index=False)
    summary = {
        'n_total': len(result),
        'n_representative': 100,
        'n_targeted': 100,
        'note': 'AI reclassification, NOT human validation. Targeted sample is enriched; do not pool for prevalence.',
        'representative_original_by_new': pd.crosstab(
            result.loc[result.sample_group.eq('representative'), 'original_stage6e_label'],
            result.loc[result.sample_group.eq('representative'), 'simple_label']).to_dict(),
        'targeted_new_labels': dict(Counter(result.loc[~result.sample_group.eq('representative'), 'simple_label'])),
        'all_new_labels': dict(Counter(result.simple_label)),
    }
    with (out / 'stage6e_simple_200_summary.json').open('x', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f'Comparison: {compare}\nSummary: {out / "stage6e_simple_200_summary.json"}')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--source', type=Path, default=SOURCE)
    ap.add_argument('--prompt', type=Path, default=PROMPT)
    ap.add_argument('--output', type=Path, default=OUTPUT)
    ap.add_argument('--model', default=MODEL)
    action = ap.add_mutually_exclusive_group()
    action.add_argument('--run', action='store_true', help='Make paid API calls; checkpoint each successful row')
    action.add_argument('--finalize', action='store_true', help='Write comparison only when all 200 rows completed')
    ap.add_argument('--limit', type=int, default=200, help='Max TOTAL rows processed in --run (10 for smoke test)')
    ap.add_argument('--max-output-tokens', type=int, default=1200)
    ap.add_argument('--retries', type=int, default=3)
    args = ap.parse_args()
    if not args.source.is_file() or not args.prompt.is_file():
        ap.error(f'Source or prompt missing: {args.source}, {args.prompt}')
    if not (1 <= args.limit <= 200):
        ap.error('--limit must be between 1 and 200')
    df = load_sample(args.source)
    ptxt = args.prompt.read_text(encoding='utf-8')
    ids = {row_id(r) for r in df.to_dict('records')}
    meta = {
        'purpose': 'isolated_200_sentence_alternative_definition_pilot',
        'source': str(args.source), 'source_sha256': sha256(args.source),
        'prompt': str(args.prompt), 'prompt_sha256': sha256(args.prompt),
        'model': args.model, 'n_rows': len(df),
        'prior_context': 'top_3_retrieved_only',
        'original_labels_sent_to_model': False,
        'production_outputs_modified': False,
    }
    ck_path = args.output / 'responses_checkpoint.jsonl'
    completed = read_checkpoint(ck_path, ids)
    if not args.run and not args.finalize:
        print(f'DRY RUN: {len(df)} frozen rows; {len(completed)} already checkpointed.')
        print(f'Input: {args.source}\nPrompt: {args.prompt}\nOutput: {args.output}')
        print('No files written, no API calls. Use --run --limit 10 for a smoke test.')
        return
    if args.finalize:
        if not args.output.is_dir():
            raise RuntimeError('Experiment directory does not exist')
        old = json.loads((args.output / 'experiment_manifest.json').read_text(encoding='utf-8'))
        for k in ('source_sha256', 'prompt_sha256', 'model', 'n_rows'):
            if old.get(k) != meta.get(k):
                raise RuntimeError(f'Cannot finalize; {k} differs from experiment manifest')
        finalize(df, completed, args.output)
        return
    if not os.environ.get('OPENAI_API_KEY'):
        raise RuntimeError('OPENAI_API_KEY not set; refusing API run')
    from openai import OpenAI
    args.output.mkdir(parents=True, exist_ok=True)
    create_or_check_manifest(args.output, meta)
    client = OpenAI()
    for pos, row in enumerate(df.to_dict('records')[:args.limit], 1):
        rid = row_id(row)
        if rid in completed:
            continue
        print(f'[{pos}/{args.limit}] {rid}', flush=True)
        cls, usage = call_one(client, args.model, ptxt, input_for(row),
                              prior_indices(row), args.max_output_tokens, args.retries)
        record = {
            'row_id': rid, 'timestamp_utc': datetime.now(timezone.utc).isoformat(),
            'input_sha256': hashlib.sha256(input_for(row).encode('utf-8')).hexdigest(),
            'classification': cls, 'usage': usage,
        }
        with ck_path.open('a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
            f.flush()
            os.fsync(f.fileno())
        completed[rid] = record
    print(f'Completed {len(completed)}/200. Original data unchanged.')
    if len(completed) == 200:
        print('All 200 complete. Run with --finalize to create the comparison.')


if __name__ == '__main__':
    main()
