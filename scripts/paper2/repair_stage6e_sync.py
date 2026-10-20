#!/usr/bin/env python3
"""Targeted synchronous repair for Paper 2 Stage 6E persistent/novel classification."""
from __future__ import annotations
import argparse, copy, hashlib, json, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from openai import OpenAI

PRODUCTION_PROMPT_SHA256="0f1824dcc9477f684d3a9749880b094b2df3e83ba772204b4adcacd8605fb1b0"
DEFAULT_PROMPT=Path("configs/paper2/prompts/persistent_novel_pair_classifier_v1.md")
DEFAULT_MODEL="gpt-6-luna"

def clean(x): return "" if x is None else str(x).strip()
def sha256_text(s): return hashlib.sha256(s.encode("utf-8")).hexdigest()
def normalize_sentence(text): return re.sub(r"\s+"," ",str(text).replace("\u3000"," ")).strip()
def split_japanese_sentences(text,min_chars=12):
    text=text.replace("\r\n","\n").replace("\r","\n")
    return [x for x in (normalize_sentence(c) for c in re.split(r"(?<=[。！？!?])|\n+",text)) if len(x)>=min_chars]

def build_mdna_index(root):
    print(f"Indexing MD&A files under {root} ...",flush=True); idx={}
    for p in root.rglob("*.txt"):
        if p.stem in idx: raise RuntimeError(f"Duplicate MD&A doc ID: {p.stem}")
        idx[p.stem]=p
    print(f"Indexed {len(idx):,} MD&A files.",flush=True); return idx

def read_mdna(idx,doc_id):
    if doc_id not in idx: raise FileNotFoundError(f"Could not find {doc_id} in MD&A index")
    return idx[doc_id].read_text(encoding="utf-8-sig",errors="replace")

def build_pair_input(pair,prior):
    current=sorted(pair["sentences"],key=lambda x:int(x["current_sentence_index"]))
    prior_block="\n".join(f"P{i}\t{s}" for i,s in enumerate(prior))
    current_block="\n".join(f"C{int(x['current_sentence_index'])}\t{x['current_sentence']}" for x in current)
    retrieval_block="\n".join("C{} -> {}".format(int(x["current_sentence_index"]),",".join(f"P{int(c['prior_index'])}" for c in x["reranked_candidates"][:3])) for x in current)
    return f"""## FIRM-YEAR PAIR
Prior document: {pair['prev_docID']}
Current document: {pair['curr_docID']}

## PRIOR-YEAR MD&A
Each sentence appears exactly once and is identified by P-index.

{prior_block}

## CURRENT-YEAR MD&A
{current_block}

## RETRIEVAL MAP
For each current sentence, these are the three prior-year sentences selected
by the frozen Ruri + IDF reranking system. Use them as the primary comparison
evidence. The complete indexed documents above are available for surrounding
context and disambiguation.

{retrieval_block}
"""

def build_repair_input(pair,prior,missing):
    missing_csv=", ".join(f"C{i}" for i in missing)
    return build_pair_input(pair,prior)+f"""
## TARGETED COMPLETION REPAIR

This is a completion repair of an existing production response.
The existing response already contains valid classifications for all current
sentences except these missing C-indices:

{missing_csv}

Using the SAME classification rules and the complete pair context above,
return classifications ONLY for those missing C-indices.
Do not return any other C-index. Do not renumber indices.
Every requested C-index must appear exactly once.
Return the same production JSON structure with a top-level \"classifications\"
array and fields current_index, label, confidence, and reason.
"""

def extract_body_text(body):
    if body.get("output_text"): return str(body["output_text"])
    pieces=[]
    for item in body.get("output",[]) or []:
        for c in item.get("content",[]) or []:
            if isinstance(c,dict) and c.get("text"): pieces.append(str(c["text"]))
    return "\n".join(pieces)

def extract_sdk_text(resp):
    if getattr(resp,"output_text",None): return str(resp.output_text)
    pieces=[]
    for item in getattr(resp,"output",[]) or []:
        for c in getattr(item,"content",[]) or []:
            if getattr(c,"text",None): pieces.append(str(c.text))
    return "\n".join(pieces)

def parse_obj(text):
    s=text.strip()
    if s.startswith("```"):
        s=re.sub(r"^```(?:json)?\s*","",s); s=re.sub(r"\s*```$","",s)
    try: obj=json.loads(s)
    except json.JSONDecodeError:
        m=re.search(r"\{.*\}",s,re.S)
        if not m: raise
        obj=json.loads(m.group(0))
    if not isinstance(obj,dict): raise ValueError("model output is not a JSON object")
    return obj

def parse_rows(obj,allowed=None,require_exact=None):
    rows=obj.get("classifications")
    if not isinstance(rows,list): raise ValueError("missing classifications array")
    ans={}
    for r in rows:
        i=int(r["current_index"]); lab=clean(r.get("label")).lower(); conf=clean(r.get("confidence")).lower()
        if i in ans: raise ValueError(f"duplicate C{i}")
        if allowed is not None and i not in allowed: raise ValueError(f"unexpected C{i}")
        if lab not in {"persistent","novel"}: raise ValueError(f"bad label C{i}: {lab}")
        if conf not in {"high","mid","low"}: raise ValueError(f"bad confidence C{i}: {conf}")
        ans[i]={"label":lab,"confidence":conf,"reason":clean(r.get("reason"))}
    if require_exact is not None and set(ans)!=require_exact:
        raise ValueError(f"coverage mismatch missing={sorted(require_exact-set(ans))[:10]} extra={sorted(set(ans)-require_exact)[:10]}")
    return ans

def parse_partial(body): return parse_rows(parse_obj(extract_body_text(body)))
def validate_full(ans,expected):
    if set(ans)!=expected: raise ValueError(f"coverage mismatch missing={sorted(expected-set(ans))[:10]} extra={sorted(set(ans)-expected)[:10]}")
def rows_obj(ans): return {"classifications":[{"current_index":i,**ans[i]} for i in sorted(ans)]}

def load_source(path):
    records=[]; by={}
    with path.open("r",encoding="utf-8") as f:
        for n,line in enumerate(f,1):
            if not line.strip(): continue
            x=json.loads(line); cid=str(x.get("custom_id","")); curr=cid.split("pn::",1)[1] if cid.startswith("pn::") else cid
            if not curr: raise ValueError(f"{path}:{n}: empty custom_id")
            if curr in by: raise ValueError(f"duplicate source response for {curr}")
            resp=x.get("response") or {}
            if resp.get("status_code")!=200: raise ValueError(f"{curr}: source status_code={resp.get('status_code')}")
            records.append(x); by[curr]=x
    return records,by

def load_pairs(path,wanted):
    out={}
    with path.open("r",encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            p=json.loads(line); curr=str(p["curr_docID"])
            if curr in wanted:
                if curr in out: raise ValueError(f"duplicate retrieval pair for {curr}")
                out[curr]=p
    miss=wanted-set(out)
    if miss: raise ValueError(f"{len(miss)} wanted docs absent from retrieval JSONL; sample={sorted(miss)[:10]}")
    return out

def find_missing(responses,pairs):
    existing={}; missing={}
    for curr,pair in pairs.items():
        body=(responses[curr].get("response") or {}).get("body") or {}; got=parse_partial(body)
        expected={int(x["current_sentence_index"]) for x in pair["sentences"]}
        extra=set(got)-expected
        if extra: raise ValueError(f"{curr}: existing unexpected indices {sorted(extra)[:10]}")
        absent=sorted(expected-set(got))
        if absent: existing[curr]=got; missing[curr]=absent
    return existing,missing

def append_jsonl(path,obj):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("a",encoding="utf-8") as f: f.write(json.dumps(obj,ensure_ascii=False)+"\n"); f.flush()

def load_checkpoint(path):
    done={}
    if not path.exists(): return done
    with path.open("r",encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            x=json.loads(line)
            if x.get("status")=="ok": done[str(x["curr_docID"])]=x
    return done

def usage_dict(resp):
    u=getattr(resp,"usage",None)
    return {k:getattr(u,k,None) if u is not None else None for k in ("input_tokens","output_tokens","total_tokens")}

def call_repair(client,model,instructions,user_input,requested,max_output_tokens,max_retries,retry_base):
    last=None
    for attempt in range(1,max_retries+1):
        try:
            resp=client.responses.create(model=model,instructions=instructions,input=user_input,store=False,max_output_tokens=max_output_tokens)
            text=extract_sdk_text(resp)
            if not text.strip(): raise ValueError("empty model response text")
            repaired=parse_rows(parse_obj(text),allowed=requested,require_exact=requested)
            return repaired,{"response_id":getattr(resp,"id",None),"response_model":getattr(resp,"model",model),"usage":usage_dict(resp),"attempt":attempt}
        except Exception as e:
            last=e
            if attempt>=max_retries: break
            wait=retry_base*(2**(attempt-1)); print(f"    attempt {attempt}/{max_retries} failed: {e}; retrying in {wait:.1f}s",file=sys.stderr,flush=True); time.sleep(wait)
    raise last

def patch_record(record,merged,meta):
    x=copy.deepcopy(record); body=x.setdefault("response",{}).setdefault("body",{})
    body["output_text"]=json.dumps(rows_obj(merged),ensure_ascii=False,separators=(",",":"))
    x["stage6e_sync_repair"]=meta
    return x

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--retrieval-jsonl",type=Path,default=Path("~/paper2_stage4/stage6e_work/retrieval_pairs.jsonl"))
    ap.add_argument("--source-jsonl",type=Path,default=Path("data/interim/paper2/alignment/persistent_novel/final_batch_outputs/stage6e_luna_final_output.jsonl"))
    ap.add_argument("--prompt",type=Path,default=DEFAULT_PROMPT)
    ap.add_argument("--mdna-root",type=Path,default=Path("~/paper2_stage4/mdna"))
    ap.add_argument("--output-jsonl",type=Path,default=Path("data/interim/paper2/alignment/persistent_novel/sync_repair/stage6e_luna_final_repaired_output.jsonl"))
    ap.add_argument("--checkpoint-jsonl",type=Path,default=Path("data/interim/paper2/alignment/persistent_novel/sync_repair/stage6e_sync_repair_audit.jsonl"))
    ap.add_argument("--model",default=DEFAULT_MODEL)
    ap.add_argument("--max-output-tokens",type=int,default=4096)
    ap.add_argument("--max-retries",type=int,default=3)
    ap.add_argument("--retry-base-seconds",type=float,default=2.0)
    ap.add_argument("--max-pairs",type=int,default=None,help="Smoke-test limit; successful repairs checkpointed, no final file until all repaired.")
    ap.add_argument("--only-doc",action="append",default=[])
    ap.add_argument("--dry-run",action="store_true")
    ap.add_argument("--overwrite-output",action="store_true")
    a=ap.parse_args()
    for k in ("retrieval_jsonl","source_jsonl","prompt","mdna_root","output_jsonl","checkpoint_jsonl"): setattr(a,k,getattr(a,k).expanduser())

    instructions=a.prompt.read_text(encoding="utf-8-sig"); prompt_sha=sha256_text(instructions)
    if prompt_sha!=PRODUCTION_PROMPT_SHA256: raise RuntimeError(f"REFUSING TO RUN: prompt SHA mismatch\nexpected {PRODUCTION_PROMPT_SHA256}\nactual   {prompt_sha}\n{a.prompt}")
    print(f"Production prompt SHA-256: {prompt_sha}"); print(f"Model: {a.model}")
    records,responses=load_source(a.source_jsonl); print(f"Loaded source responses: {len(records):,}")
    if len(records)!=33046: raise RuntimeError(f"Expected 33,046 source responses, found {len(records):,}")

    wanted=set(responses)
    if a.only_doc:
        wanted=set(a.only_doc); unknown=wanted-set(responses)
        if unknown: raise ValueError(f"Unknown --only-doc: {sorted(unknown)}")
    pairs=load_pairs(a.retrieval_jsonl,wanted); existing,missing=find_missing(responses,pairs)
    total_missing=sum(map(len,missing.values()))
    print(f"Pairs needing repair: {len(missing):,}; missing classifications: {total_missing:,}")
    if not missing: print("Nothing to repair."); return
    docs=[d for d in responses if d in missing]
    if a.max_pairs is not None: docs=docs[:a.max_pairs]
    print(f"Selected this run: {len(docs):,} pairs / {sum(len(missing[d]) for d in docs):,} classifications")
    for d in docs[:10]: print(f"  {d}: {', '.join('C'+str(i) for i in missing[d])}")
    if len(docs)>10: print(f"  ... {len(docs)-10:,} more")
    if a.dry_run: print("\nDRY RUN PASSED. No API calls or files written."); return
    if a.output_jsonl.exists() and not a.overwrite_output: raise FileExistsError(f"{a.output_jsonl} exists; use --overwrite-output only to intentionally rebuild")

    cp=load_checkpoint(a.checkpoint_jsonl); idx=build_mdna_index(a.mdna_root); client=OpenAI(); repaired={}
    for d in docs:
        x=cp.get(d)
        if x and x.get("prompt_sha256")==prompt_sha and x.get("model")==a.model and set(map(int,x.get("requested_indices",[])))==set(missing[d]):
            rr={int(r["current_index"]):{"label":clean(r["label"]).lower(),"confidence":clean(r["confidence"]).lower(),"reason":clean(r.get("reason"))} for r in x["repair_classifications"]}
            parse_rows(rows_obj(rr),allowed=set(missing[d]),require_exact=set(missing[d])); repaired[d]=rr
    if repaired: print(f"Resuming from checkpoint: {len(repaired):,} successful pair repairs.")

    for pos,d in enumerate(docs,1):
        if d in repaired: print(f"[{pos:,}/{len(docs):,}] {d}: checkpoint OK",flush=True); continue
        pair=pairs[d]; requested=set(missing[d]); prior=split_japanese_sentences(read_mdna(idx,str(pair["prev_docID"])),int(pair.get("min_sentence_chars",12)))
        exp_prior=int(pair.get("prior_sentence_count",len(prior)))
        if len(prior)!=exp_prior: raise ValueError(f"{d}: prior reconstruction {len(prior)} != expected {exp_prior}")
        user_input=build_repair_input(pair,prior,sorted(requested)); print(f"[{pos:,}/{len(docs):,}] {d}: repairing {', '.join('C'+str(i) for i in sorted(requested))}",flush=True)
        ts=datetime.now(timezone.utc).isoformat()
        try:
            rr,meta=call_repair(client,a.model,instructions,user_input,requested,a.max_output_tokens,a.max_retries,a.retry_base_seconds)
            overlap=set(existing[d])&set(rr)
            if overlap: raise RuntimeError(f"{d}: attempted overwrite {sorted(overlap)}")
            merged=dict(existing[d]); merged.update(rr); expected={int(x["current_sentence_index"]) for x in pair["sentences"]}; validate_full(merged,expected)
            audit={"status":"ok","timestamp_utc":ts,"curr_docID":d,"prev_docID":str(pair["prev_docID"]),"model":a.model,"prompt_path":str(a.prompt),"prompt_sha256":prompt_sha,"requested_indices":sorted(requested),"repair_classifications":[{"current_index":i,**rr[i]} for i in sorted(rr)],"response_id":meta["response_id"],"response_model":meta["response_model"],"usage":meta["usage"],"attempt":meta["attempt"],"post_merge_coverage":"pass"}
            append_jsonl(a.checkpoint_jsonl,audit); repaired[d]=rr
        except Exception as e:
            append_jsonl(a.checkpoint_jsonl,{"status":"error","timestamp_utc":ts,"curr_docID":d,"prev_docID":str(pair["prev_docID"]),"model":a.model,"prompt_path":str(a.prompt),"prompt_sha256":prompt_sha,"requested_indices":sorted(requested),"error":repr(e)}); raise

    unresolved=set(missing)-set(repaired)
    if unresolved:
        print(f"\nSelected repairs complete; {len(unresolved):,} pairs remain. Checkpoint saved. Re-run without --max-pairs/--only-doc to resume."); return
    if a.only_doc:
        print("\nSelected documents repaired and validated. No global output written with --only-doc."); return

    # Global all-pair validation: complete pairs stay untouched; repaired pairs are merged.
    all_pairs=load_pairs(a.retrieval_jsonl,set(responses)); merged_by={}
    for d,pair in all_pairs.items():
        got=parse_partial((responses[d].get("response") or {}).get("body") or {})
        if d in repaired:
            overlap=set(got)&set(repaired[d])
            if overlap: raise RuntimeError(f"{d}: repaired overlap {sorted(overlap)}")
            got.update(repaired[d])
        expected={int(x["current_sentence_index"]) for x in pair["sentences"]}; validate_full(got,expected); merged_by[d]=got
    if len(merged_by)!=33046: raise RuntimeError(f"Expected 33,046 validated pairs, got {len(merged_by):,}")

    a.output_jsonl.parent.mkdir(parents=True,exist_ok=True); patched=0
    with a.output_jsonl.open("w",encoding="utf-8") as fout:
        for rec in records:
            cid=str(rec.get("custom_id","")); d=cid.split("pn::",1)[1] if cid.startswith("pn::") else cid
            if d in repaired:
                rec=patch_record(rec,merged_by[d],{"method":"targeted_sync_completion","model":a.model,"prompt_sha256":prompt_sha,"repaired_indices":sorted(repaired[d]),"repaired_count":len(repaired[d]),"source_jsonl":str(a.source_jsonl),"audit_jsonl":str(a.checkpoint_jsonl)}); patched+=1
            fout.write(json.dumps(rec,ensure_ascii=False)+"\n")
    print("\nFINAL REPAIR VALIDATION PASSED."); print(f"Source responses         : {len(records):,}"); print(f"Repaired pairs           : {patched:,}"); print(f"Repaired classifications : {total_missing:,}"); print(f"Fully covered pairs      : {len(merged_by):,}"); print(f"Output                   : {a.output_jsonl}"); print(f"Audit/checkpoint          : {a.checkpoint_jsonl}")

if __name__=="__main__": main()
