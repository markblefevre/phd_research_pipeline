#!/usr/bin/env python3
"""Plan, run, and assemble isolated five-way classification repairs.

Original batch requests/responses and Stage 6E/6F files are never modified.
Run from paper2_local; requires openai only for 'run'.
"""
import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

PROMPT_HASH = 'dda8bc260b7c2054ccf4e5822e78e7509d43379dd3d7d964edaa8ff08adc319f'
LABELS = {'unchanged', 'revised', 'new_candidate', 'non_substantive', 'uncertain'}
CONF = {'high', 'mid', 'low'}


def dumps(x):
    return json.dumps(x, ensure_ascii=False, separators=(',', ':'))


def digest(s):
    return hashlib.sha256(s.encode('utf-8')).hexdigest()


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def write_new(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def records(directory, names, wanted):
    found = {}
    for path in sorted(directory.glob(names)):
        with path.open(encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                rec = json.loads(line)
                cid = rec.get('custom_id', '')
                if not cid.startswith('dc5::'):
                    continue
                doc = cid[5:]
                if doc in wanted:
                    if doc in found:
                        raise ValueError(f'Duplicate {doc} in {directory}')
                    found[doc] = rec
    if set(found) != wanted:
        raise ValueError(f'Missing documents in {directory}: {wanted - set(found)}')
    return found


def response_text(body):
    if isinstance(body.get('output_text'), str):
        return body['output_text']
    return '\n'.join(str(c['text']) for item in body.get('output', []) or []
                     for c in item.get('content', []) or []
                     if isinstance(c, dict) and isinstance(c.get('text'), str))


def parse(raw):
    raw = raw.strip()
    if raw.startswith('```'):
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
    obj = json.loads(raw)
    if not isinstance(obj, dict) or not isinstance(obj.get('classifications'), list):
        raise ValueError('Missing classifications list')
    return obj['classifications']


def valid_entries(entries, expected, n_prior):
    counts = Counter(e['current_index'] for e in entries if isinstance(e, dict)
                     and type(e.get('current_index')) is int)
    good = {}
    for e in entries:
        if not isinstance(e, dict) or type(e.get('current_index')) is not int:
            continue
        i = e['current_index']
        if i not in expected or counts[i] != 1:
            continue
        label = str(e.get('label', '')).strip().lower()
        confidence = str(e.get('confidence', '')).strip().lower()
        p = e.get('prior_index_used')
        reason = e.get('reason')
        if label not in LABELS or confidence not in CONF:
            continue
        if p is not None and (type(p) is not int or not 0 <= p < n_prior):
            continue
        if not isinstance(reason, str) or not reason.strip():
            continue
        good[i] = e
    return good


def split_input(s):
    headings = ['## PRIOR-YEAR MD&A', '## CURRENT-YEAR MD&A', '## RETRIEVAL MAP']
    if any(s.count(h) != 1 for h in headings):
        raise ValueError('Unexpected input headings')
    head, s = s.split(headings[0], 1)
    prior, s = s.split(headings[1], 1)
    current, mapping = s.split(headings[2], 1)
    return head, prior, current, mapping


def indexed_lines(section, pattern, kind):
    intro = []
    indexed = {}
    started = False
    for line in section.splitlines(keepends=True):
        m = re.match(pattern, line)
        if m:
            started = True
            i = int(m.group(1))
            if i in indexed:
                raise ValueError(f'Duplicate {kind} index {i}')
            indexed[i] = line
        elif not started:
            intro.append(line)
        elif line.strip():
            raise ValueError(f'Unexpected {kind} continuation: {line[:80]}')
    return ''.join(intro), indexed


def input_parts(original):
    head, prior, current, mapping = split_input(original)
    _, p = indexed_lines(prior, r'^P(\d+)\t', 'P')
    ci, c = indexed_lines(current, r'^C(\d+)\t', 'C')
    mi, m = indexed_lines(mapping, r'^C(\d+)\s*->', 'map')
    if set(p) != set(range(len(p))) or not set(c) <= set(m):
        raise ValueError('Noncontiguous prior indices or incomplete retrieval map')
    return head, prior, ci, c, mi, m, len(p)


def chunk_input(parts, indices):
    head, prior, ci, c, mi, m, _ = parts
    if not set(indices) <= set(c):
        raise ValueError('Chunk includes unknown C index')
    return (head + '## PRIOR-YEAR MD&A' + prior +
            '## CURRENT-YEAR MD&A' + ci + ''.join(c[i] for i in indices) +
            '## RETRIEVAL MAP' + mi + ''.join(m[i] for i in indices) +
            '\n## RECOVERY SCOPE\nClassify exactly the C-index sentences shown above, '
            'using their original indices. Do not classify omitted C-indices. '
            'Use the full prior-year MD&A and the corresponding retrieval map '
            'as evidence; follow the unchanged frozen classification instructions.\n')


def plan(args):
    root = args.root
    with (root / 'collected_full_prompt_v2/classification_failures.csv').open(
            encoding='utf-8', newline='') as f:
        failures = {r['curr_docID']: r['error'] for r in csv.DictReader(f)}
    audit = {}
    with (root / 'repair_audit_prompt_v2/failed_pair_audit.jsonl').open(encoding='utf-8') as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                audit[r['curr_docID']] = r
    if len(failures) != 145 or set(audit) != set(failures):
        raise ValueError('Audit/failures mismatch; expected 145 pairs')
    reqdir = root / 'batch_requests_full_prompt_v2'
    batchdir = root / 'batch_outputs_full_prompt_v2'
    if read(reqdir / 'batch_build_manifest.json')['prompt_sha256'] != PROMPT_HASH:
        raise ValueError('Frozen prompt hash mismatch')
    requests = records(reqdir, '*.jsonl', set(failures))
    responses = records(batchdir, '*_output.jsonl', set(failures))
    if args.work_dir.exists():
        raise ValueError(f'Refusing to reuse work directory: {args.work_dir}')
    args.work_dir.mkdir(parents=True)
    (args.work_dir / 'tasks').mkdir()
    (args.work_dir / 'checkpoints').mkdir()
    (args.work_dir / 'attempts').mkdir()
    docs = {}
    task_count = 0
    preserved = unresolved_total = 0
    for doc in sorted(failures):
        body = requests[doc]['body']
        if digest(body['instructions']) != PROMPT_HASH or not isinstance(body['input'], str):
            raise ValueError(f'{doc}: prompt or input mismatch')
        parts = input_parts(body['input'])
        _, _, _, current, _, _, n_prior = parts
        expected = set(current)
        a = audit[doc]
        if len(expected) != a['expected_sentences'] or n_prior != a['prior_sentences']:
            raise ValueError(f'{doc}: retrieval audit disagrees with request')
        original = responses[doc]
        raw = response_text((original.get('response') or {}).get('body') or {})
        try:
            entries = parse(raw)
        except (ValueError, TypeError):
            entries = []  # Malformed JSON: no speculative salvage.
        good = valid_entries(entries, expected, n_prior)
        unresolved = sorted(expected - set(good))
        if unresolved != a['unresolved_indices'] or len(good) != a['valid_entries_preservable']:
            raise ValueError(f'{doc}: re-audit mismatch, refusing to plan')
        names = []
        for n, start in enumerate(range(0, len(unresolved), args.chunk_size), 1):
            indices = unresolved[start:start + args.chunk_size]
            task_id = f'{doc}__{n:03d}'
            write_new(args.work_dir / 'tasks' / f'{task_id}.json', {
                'task_id': task_id, 'doc': doc, 'model': body['model'],
                'instructions': body['instructions'],
                'input': chunk_input(parts, indices),
                'indices': indices, 'n_prior': n_prior,
            })
            names.append(task_id)
            task_count += 1
        docs[doc] = {
            'expected': sorted(expected), 'n_prior': n_prior,
            'preserved': [good[i] for i in sorted(good)],
            'tasks': names, 'original_sha256': digest(dumps(original)),
            'failure': failures[doc],
        }
        preserved += len(good)
        unresolved_total += len(unresolved)
    write_new(args.work_dir / 'plan.json', {
        'prompt_sha256': PROMPT_HASH, 'batch_dir': str(batchdir),
        'docs': docs, 'task_count': task_count,
        'preserved': preserved, 'unresolved': unresolved_total,
        'chunk_size': args.chunk_size,
    })
    print(f'PLAN: {len(docs)} pairs, {task_count} tasks; '
          f'{preserved:,} preserved and {unresolved_total:,} unresolved sentences.')
    print(f'Files: {args.work_dir} (no API calls; originals untouched)')


def strict_recovery(raw, indices, n_prior):
    entries = parse(raw)
    good = valid_entries(entries, set(indices), n_prior)
    if set(good) != set(indices) or len(entries) != len(indices):
        raise ValueError(f'Incomplete/invalid recovery: expected={len(indices)} '
                         f'entries={len(entries)} valid={len(good)} '
                         f'missing={sorted(set(indices) - set(good))[:12]}')
    return [good[i] for i in sorted(good)]


def run(args):
    from openai import OpenAI
    p = read(args.work_dir / 'plan.json')
    if p['prompt_sha256'] != PROMPT_HASH:
        raise ValueError('Prompt hash mismatch')
    tasks = sorted((args.work_dir / 'tasks').glob('*.json'))
    if len(tasks) != p['task_count']:
        raise ValueError('Task count mismatch')
    client = OpenAI(timeout=args.timeout, max_retries=2)
    finished = new = 0
    for path in tasks:
        task = read(path)
        if digest(task['instructions']) != PROMPT_HASH:
            raise ValueError(f"{task['task_id']}: task instructions changed")
        checkpoint = args.work_dir / 'checkpoints' / path.name
        if checkpoint.exists():
            saved = read(checkpoint)
            if saved['task_id'] != task['task_id']:
                raise ValueError(f'Checkpoint mismatch {checkpoint}')
            strict_recovery(dumps({'classifications': saved['classifications']}),
                            task['indices'], task['n_prior'])
            finished += 1
            continue
        if args.limit is not None and new >= args.limit:
            break
        print(f"Calling {task['task_id']} ({len(task['indices'])} sentences)", flush=True)
        response = client.responses.create(
            model=task['model'], instructions=task['instructions'],
            input=task['input'], store=False,
            max_output_tokens=args.max_output_tokens,
        )
        raw = response_text(response.model_dump())
        # Save each paid response before validation, including failures.
        # Never overwrite an attempt; an invalid response is not a valid checkpoint.
        attempt = args.work_dir / 'attempts' / path.name
        if attempt.exists():
            raise ValueError(f'Existing attempt needs review before retry: {attempt}')
        write_new(attempt, {
            'task_id': task['task_id'], 'response_id': response.id,
            'status': response.status,
            'usage': response.usage.model_dump() if response.usage else None,
            'output_text': raw,
        })
        classifications = strict_recovery(raw, task['indices'], task['n_prior'])
        write_new(checkpoint, {
            'task_id': task['task_id'], 'response_id': response.id,
            'status': response.status,
            'usage': response.usage.model_dump() if response.usage else None,
            'classifications': classifications, 'raw_sha256': digest(raw),
        })
        finished += 1
        new += 1
        print(f'  Valid checkpoint {finished}/{len(tasks)}', flush=True)
    print(f'Valid checkpoints: {finished}/{len(tasks)}. Resume by running the same command.')


def assemble(args):
    p = read(args.work_dir / 'plan.json')
    overlays = {}
    for doc, info in p['docs'].items():
        merged = {e['current_index']: e for e in info['preserved']}
        for task_id in info['tasks']:
            task = read(args.work_dir / 'tasks' / f'{task_id}.json')
            checkpoint = args.work_dir / 'checkpoints' / f'{task_id}.json'
            if not checkpoint.exists():
                raise ValueError(f'Missing checkpoint {task_id}')
            saved = read(checkpoint)
            if saved['task_id'] != task_id:
                raise ValueError(f'Wrong checkpoint {task_id}')
            recovered = strict_recovery(dumps({'classifications': saved['classifications']}),
                                        task['indices'], task['n_prior'])
            for e in recovered:
                i = e['current_index']
                if i in merged:
                    raise ValueError(f'{doc}: duplicate merged index {i}')
                merged[i] = e
        entries = strict_recovery(dumps({'classifications': list(merged.values())}),
                                  info['expected'], info['n_prior'])
        overlays[doc] = entries
    if args.merged_dir.exists():
        raise ValueError(f'Refusing to overwrite {args.merged_dir}')
    args.merged_dir.mkdir(parents=True)
    replaced = set()
    seen = set()
    for src in sorted(Path(p['batch_dir']).glob('*_output.jsonl')):
        with src.open(encoding='utf-8') as f, (args.merged_dir / src.name).open('x', encoding='utf-8') as out:
            for line in f:
                if not line.strip():
                    continue
                rec = json.loads(line)
                cid = rec.get('custom_id', '')
                if cid.startswith('dc5::'):
                    doc = cid[5:]
                    if doc in seen:
                        raise ValueError(f'Duplicate response {doc}')
                    seen.add(doc)
                    if doc in overlays:
                        if digest(dumps(rec)) != p['docs'][doc]['original_sha256']:
                            raise ValueError(f'{doc}: original response changed')
                        rec = {
                            'custom_id': cid, 'response': {
                                'status_code': 200,
                                'request_id': (rec.get('response') or {}).get('request_id'),
                                'body': {'output_text': dumps({'classifications': overlays[doc]})},
                            }, 'error': None,
                        }
                        replaced.add(doc)
                out.write(dumps(rec) + '\n')
    if replaced != set(overlays) or len(seen) != 33046:
        raise ValueError(f'Output count mismatch: replaced {len(replaced)} total {len(seen)}')
    write_new(args.merged_dir / 'repair_provenance.json', {
        'prompt_sha256': PROMPT_HASH, 'original_batch_dir': p['batch_dir'],
        'replaced_pairs': len(replaced), 'preserved': p['preserved'],
        'recovered': p['unresolved'], 'recovery_tasks': p['task_count'],
        'plan': str(args.work_dir / 'plan.json'),
        'note': 'Batch-compatible synthetic overlay; original responses untouched',
    })
    print(f'Assembled {len(seen):,} responses, repaired {len(replaced)} pairs: {args.merged_dir}')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('mode', choices=['plan', 'run', 'assemble'])
    ap.add_argument('--root', type=Path, default=Path(
        'data/interim/paper2/alignment/disclosure_change_5way_v1'))
    ap.add_argument('--work-dir', type=Path)
    ap.add_argument('--merged-dir', type=Path)
    ap.add_argument('--chunk-size', type=int, default=40)
    ap.add_argument('--max-output-tokens', type=int, default=12000)
    ap.add_argument('--timeout', type=int, default=1200)
    ap.add_argument('--limit', type=int)
    a = ap.parse_args()
    a.work_dir = a.work_dir or a.root / 'recovery_prompt_v2'
    a.merged_dir = a.merged_dir or a.root / 'batch_outputs_repaired_prompt_v2'
    if not 1 <= a.chunk_size <= 100:
        ap.error('chunk-size must be 1..100')
    {'plan': plan, 'run': run, 'assemble': assemble}[a.mode](a)


if __name__ == '__main__':
    main()
