#!/usr/bin/env python3
"""Validate Stage 6E Batch outputs and assemble canonical sentence/document files."""

from __future__ import annotations
import argparse,csv,json,re
from pathlib import Path

def clean(x): return "" if x is None else str(x).strip()
def extract_response_text(body:dict)->str:
    if body.get("output_text"): return body["output_text"]
    pieces=[]
    for item in body.get("output",[]) or []:
        for c in item.get("content",[]) or []:
            if isinstance(c,dict) and c.get("text"): pieces.append(c["text"])
    return "\n".join(pieces)

def parse_obj(text):
    s=text.strip()
    if s.startswith("```"):
        s=re.sub(r"^```(?:json)?\s*","",s); s=re.sub(r"\s*```$","",s)
    try:return json.loads(s)
    except json.JSONDecodeError:
        m=re.search(r"\{.*\}",s,re.S)
        if not m: raise
        return json.loads(m.group(0))

def parse_classifications(body,expected):
    obj=parse_obj(extract_response_text(body))
    rows=obj.get("classifications")
    if not isinstance(rows,list): raise ValueError("missing classifications array")
    ans={}; 
    for r in rows:
        i=int(r["current_index"]); lab=clean(r.get("label")).lower(); conf=clean(r.get("confidence")).lower()
        if i in ans: raise ValueError(f"duplicate C{i}")
        if lab not in {"persistent","novel"}: raise ValueError(f"bad label C{i}: {lab}")
        if conf not in {"high","mid","low"}: raise ValueError(f"bad confidence C{i}: {conf}")
        ans[i]={"label":lab,"confidence":conf,"reason":clean(r.get("reason"))}
    if set(ans)!=expected:
        raise ValueError(f"coverage mismatch missing={sorted(expected-set(ans))[:10]} extra={sorted(set(ans)-expected)[:10]}")
    return ans

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--retrieval-jsonl",type=Path,required=True)
    ap.add_argument("--batch-output-dir",type=Path,required=True)
    ap.add_argument("--output-dir",type=Path,default=Path("data/interim/paper2/alignment/persistent_novel"))
    args=ap.parse_args(); args.retrieval_jsonl=args.retrieval_jsonl.expanduser()
    args.output_dir.mkdir(parents=True,exist_ok=True)

    raw={}
    failures=[]
    for p in sorted(args.batch_output_dir.glob("*_output.jsonl")):
        with p.open("r",encoding="utf-8") as f:
            for line in f:
                if not line.strip():continue
                x=json.loads(line); cid=x.get("custom_id","")
                curr=cid.split("pn::",1)[1] if cid.startswith("pn::") else cid
                resp=x.get("response") or {}
                if resp.get("status_code")!=200:
                    failures.append({"curr_docID":curr,"source":str(p),"error":json.dumps(x.get("error") or resp,ensure_ascii=False)})
                    continue
                if curr in raw: raise ValueError(f"duplicate batch output for {curr}")
                raw[curr]=resp.get("body") or {}
    print(f"Loaded successful batch responses: {len(raw):,}")

    sent_path=args.output_dir/"persistent_novel_sentences.csv"
    doc_path=args.output_dir/"persistent_novel_documents.csv"
    class_path=args.output_dir/"classified_pairs.jsonl"
    fail_path=args.output_dir/"persistent_novel_failures.csv"

    sent_fields=["prev_docID","curr_docID","current_sentence_index","current_sentence",
                 "label","confidence","reason","top1_prior_index","top1_cosine",
                 "top1_rerank_score","top2_prior_index","top2_cosine","top2_rerank_score",
                 "top3_prior_index","top3_cosine","top3_rerank_score"]
    doc_fields=["prev_docID","curr_docID","currentSentenceCount","persistentSentenceCount",
                "novelSentenceCount","persistentChars","novelChars","persistentShareChars",
                "novelShareChars","persistentText","novelText"]

    n_pairs=n_sent=0
    with sent_path.open("w",newline="",encoding="utf-8") as sf, \
         doc_path.open("w",newline="",encoding="utf-8") as df, \
         class_path.open("w",encoding="utf-8") as cf:
        sw=csv.DictWriter(sf,fieldnames=sent_fields); sw.writeheader()
        dw=csv.DictWriter(df,fieldnames=doc_fields); dw.writeheader()
        with args.retrieval_jsonl.open("r",encoding="utf-8") as rf:
            for line in rf:
                if not line.strip():continue
                pair=json.loads(line); curr=str(pair["curr_docID"])
                if curr not in raw:
                    failures.append({"curr_docID":curr,"source":"merge","error":"missing successful batch response"})
                    continue
                items=sorted(pair["sentences"],key=lambda x:int(x["current_sentence_index"]))
                expected={int(x["current_sentence_index"]) for x in items}
                try: ans=parse_classifications(raw[curr],expected)
                except Exception as e:
                    failures.append({"curr_docID":curr,"source":"validation","error":str(e)}); continue

                persistent=[]; novel=[]; classified=[]
                for x in items:
                    i=int(x["current_sentence_index"]); a=ans[i]; text=str(x["current_sentence"])
                    (persistent if a["label"]=="persistent" else novel).append(text)
                    cands=x.get("reranked_candidates",[])[:3]
                    row={"prev_docID":pair["prev_docID"],"curr_docID":curr,
                         "current_sentence_index":i,"current_sentence":text,**a}
                    for rank in range(3):
                        c=cands[rank] if rank<len(cands) else {}
                        row[f"top{rank+1}_prior_index"]=c.get("prior_index")
                        row[f"top{rank+1}_cosine"]=c.get("cosine")
                        row[f"top{rank+1}_rerank_score"]=c.get("rerank_score")
                    sw.writerow(row); classified.append({"current_index":i,**a}); n_sent+=1
                ptext="\n".join(persistent); ntext="\n".join(novel)
                total_chars=len(ptext)+len(ntext)
                dw.writerow({
                    "prev_docID":pair["prev_docID"],"curr_docID":curr,
                    "currentSentenceCount":len(items),
                    "persistentSentenceCount":len(persistent),"novelSentenceCount":len(novel),
                    "persistentChars":len(ptext),"novelChars":len(ntext),
                    "persistentShareChars":len(ptext)/total_chars if total_chars else 0,
                    "novelShareChars":len(ntext)/total_chars if total_chars else 0,
                    "persistentText":ptext,"novelText":ntext
                })
                cf.write(json.dumps({"prev_docID":pair["prev_docID"],"curr_docID":curr,
                                     "classifications":classified},ensure_ascii=False)+"\n")
                n_pairs+=1
                if n_pairs%500==0: print(f"assembled pairs={n_pairs:,} sentences={n_sent:,}",flush=True)

    # De-duplicate failure messages by tuple for readable QC.
    seen=set(); uniq=[]
    for x in failures:
        k=(x["curr_docID"],x["source"],x["error"])
        if k not in seen: seen.add(k); uniq.append(x)
    with fail_path.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["curr_docID","source","error"]); w.writeheader(); w.writerows(uniq)
    summary={"pairs_assembled":n_pairs,"sentences_assembled":n_sent,"failure_rows":len(uniq),
             "successful_batch_responses_loaded":len(raw)}
    (args.output_dir/"pair_level_classification_summary.json").write_text(
        json.dumps(summary,indent=2),encoding="utf-8")
    print(json.dumps(summary,indent=2))
    if uniq:
        print(f"WARNING: {len(uniq)} failure rows; inspect {fail_path} and do not freeze Stage 6E yet.")
    else:
        print("All retrieved pairs assembled with no classification failures.")

if __name__=="__main__": main()
