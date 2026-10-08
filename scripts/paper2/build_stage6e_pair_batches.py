#!/usr/bin/env python3
"""Build Stage 6E pair-level GPT-6 Luna Batch API request files.

No API calls are made. The frozen retrieval JSONL is streamed once. Each
firm-year pair becomes one /v1/responses request containing complete indexed
prior/current MD&A plus the frozen top-3 retrieval map.

Files are rotated before --max-batch-bytes (default 180 MB), safely below the
OpenAI Batch API 200 MB/file limit. Request count is also capped below 50,000.
"""

from __future__ import annotations
import argparse, hashlib, json, re
from pathlib import Path

def normalize_sentence(text: str) -> str:
    text = str(text).replace("\u3000", " ")
    return re.sub(r"\s+", " ", text).strip()

def split_japanese_sentences(text: str, min_chars: int = 12) -> list[str]:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    chunks = re.split(r"(?<=[。！？!?])|\n+", text)
    return [x for x in (normalize_sentence(y) for y in chunks) if len(x) >= min_chars]

def build_mdna_index(root: Path) -> dict[str, Path]:
    """Scan the MD&A corpus once and map docID -> file path."""
    print(f"Indexing MD&A files under {root} ...", flush=True)

    index: dict[str, Path] = {}

    for path in root.rglob("*.txt"):
        doc_id = path.stem
        if doc_id in index:
            raise RuntimeError(
                f"Multiple files found for {doc_id}: "
                f"{index[doc_id]} and {path}"
            )
        index[doc_id] = path

    print(f"Indexed {len(index):,} MD&A files.", flush=True)
    return index


def read_mdna(index: dict[str, Path], doc_id: str) -> str:
    try:
        path = index[doc_id]
    except KeyError:
        raise FileNotFoundError(f"Could not find {doc_id} in MD&A index")
    return path.read_text(encoding="utf-8-sig", errors="replace")

def build_pair_input(pair: dict, prior: list[str]) -> str:
    current = sorted(pair["sentences"], key=lambda x: int(x["current_sentence_index"]))
    prior_block = "\n".join(f"P{i}\t{s}" for i,s in enumerate(prior))
    current_block = "\n".join(
        f"C{int(x['current_sentence_index'])}\t{x['current_sentence']}" for x in current
    )
    retrieval_block = "\n".join(
        "C{} -> {}".format(
            int(x["current_sentence_index"]),
            ",".join(f"P{int(c['prior_index'])}" for c in x["reranked_candidates"][:3])
        ) for x in current
    )
    return f"""## FIRM-YEAR PAIR
Prior document: {pair['prev_docID']}
Current document: {pair['curr_docID']}

## PRIOR-YEAR MD&A
Each sentence appears exactly once and is identified by P-index.

{prior_block}

## CURRENT-YEAR MD&A
Classify every C-index below.

{current_block}

## RETRIEVAL MAP
For each current sentence, these are the three prior-year sentences selected
by the frozen Ruri + IDF reranking system. Use them as the primary comparison
evidence. The complete indexed documents above are available for surrounding
context and disambiguation.

{retrieval_block}
"""

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--retrieval-jsonl", type=Path, required=True)
    ap.add_argument("--mdna-root", type=Path, default=Path("~/paper2_stage4/mdna"))
    ap.add_argument("--prompt", type=Path, default=Path("configs/paper2/prompts/persistent_novel_pair_classifier_v1.md"))
    ap.add_argument("--output-dir", type=Path, default=Path("data/interim/paper2/alignment/persistent_novel/batch_requests"))
    ap.add_argument("--model", default="gpt-6-luna")
    ap.add_argument("--max-batch-bytes", type=int, default=180_000_000)
    ap.add_argument("--max-requests-per-batch", type=int, default=45_000)
    ap.add_argument("--max-pairs", type=int, default=None, help="Smoke-test limit; omit for production.")
    args=ap.parse_args()
    args.mdna_root=args.mdna_root.expanduser()
    args.retrieval_jsonl=args.retrieval_jsonl.expanduser()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    mdna_index=build_mdna_index(args.mdna_root)
    instructions=args.prompt.read_text(encoding="utf-8-sig")
    prompt_sha=hashlib.sha256(instructions.encode("utf-8")).hexdigest()

    part=0; fh=None; part_bytes=0; part_n=0; total_n=0; total_bytes=0
    parts=[]
    def open_part():
        nonlocal part,fh,part_bytes,part_n
        part += 1
        p=args.output_dir/f"stage6e_luna_batch_{part:03d}.jsonl"
        fh=p.open("wb"); part_bytes=0; part_n=0
        parts.append({"part":part,"path":str(p),"requests":0,"bytes":0})
    open_part()

    with args.retrieval_jsonl.open("r",encoding="utf-8") as src:
        for line_no,line in enumerate(src,1):
            if not line.strip(): continue
            pair=json.loads(line)
            if pair.get("status") not in (None,"ok","success","retrieved"):
                # Retrieval production records normally have usable sentences even
                # when status naming differs. Only skip explicit failures.
                if str(pair.get("status","")).lower() in {"error","failed","failure"}:
                    continue
            prior_text=read_mdna(mdna_index,str(pair["prev_docID"]))
            min_chars=int(pair.get("min_sentence_chars",12))
            prior=split_japanese_sentences(prior_text,min_chars)
            expected=int(pair.get("prior_sentence_count",len(prior)))
            if len(prior)!=expected:
                raise ValueError(f"{pair['curr_docID']}: prior reconstruction {len(prior)} != expected {expected}")
            user_input=build_pair_input(pair,prior)
            req={
                "custom_id":f"pn::{pair['curr_docID']}",
                "method":"POST",
                "url":"/v1/responses",
                "body":{
                    "model":args.model,
                    "instructions":instructions,
                    "input":user_input,
                    "store":False
                }
            }
            b=(json.dumps(req,ensure_ascii=False,separators=(",",":"))+"\n").encode("utf-8")
            if part_n and (part_bytes+len(b)>args.max_batch_bytes or part_n>=args.max_requests_per_batch):
                fh.close()
                parts[-1]["requests"]=part_n; parts[-1]["bytes"]=part_bytes
                open_part()
            fh.write(b); part_bytes+=len(b); part_n+=1
            total_n+=1; total_bytes+=len(b)
            if total_n%250==0:
                print(f"pairs={total_n:,} part={part:03d} part_MB={part_bytes/1e6:.1f}",flush=True)
            if args.max_pairs is not None and total_n>=args.max_pairs: break
    if fh:
        fh.close(); parts[-1]["requests"]=part_n; parts[-1]["bytes"]=part_bytes

    manifest={
        "model":args.model,"prompt":str(args.prompt),"prompt_sha256":prompt_sha,
        "retrieval_jsonl":str(args.retrieval_jsonl),"mdna_root":str(args.mdna_root),
        "total_requests":total_n,"total_bytes":total_bytes,"parts":parts
    }
    (args.output_dir/"batch_build_manifest.json").write_text(
        json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8"
    )
    print(f"\nBuilt {total_n:,} requests in {len(parts)} file(s), {total_bytes/1e9:.3f} GB total.")
    for x in parts: print(f"  {Path(x['path']).name}: {x['requests']:,} requests, {x['bytes']/1e6:.1f} MB")
    print(f"Manifest: {args.output_dir/'batch_build_manifest.json'}")

if __name__=="__main__": main()
