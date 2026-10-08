#!/usr/bin/env python3
"""Submit, inspect, and download Stage 6E OpenAI Batch jobs."""

from __future__ import annotations
import argparse, json
from pathlib import Path
from openai import OpenAI

def load(path): return json.loads(path.read_text(encoding="utf-8"))
def save(path,obj): path.write_text(json.dumps(obj,indent=2,ensure_ascii=False,default=str),encoding="utf-8")

def main():
    ap=argparse.ArgumentParser()
    sub=ap.add_subparsers(dest="cmd",required=True)
    p=sub.add_parser("submit")
    p.add_argument("--build-manifest",type=Path,required=True)
    p.add_argument("--state",type=Path,default=None)
    p.add_argument("--max-batches",type=int,default=None,help="Use 1 for a live smoke test.")
    p=sub.add_parser("status")
    p.add_argument("--state",type=Path,required=True)
    p=sub.add_parser("download")
    p.add_argument("--state",type=Path,required=True)
    p.add_argument("--output-dir",type=Path,required=True)
    args=ap.parse_args(); client=OpenAI()

    if args.cmd=="submit":
        build=load(args.build_manifest)
        state_path=args.state or args.build_manifest.parent/"batch_state.json"
        state={"build_manifest":str(args.build_manifest),"batches":[]}
        limit=args.max_batches or len(build["parts"])
        for x in build["parts"][:limit]:
            path=Path(x["path"])
            print(f"Uploading {path.name} ({x['requests']:,} requests)...",flush=True)
            with path.open("rb") as f:
                uploaded=client.files.create(file=f,purpose="batch")
            batch=client.batches.create(
                input_file_id=uploaded.id,endpoint="/v1/responses",completion_window="24h",
                metadata={"stage":"paper2_6e","part":str(x["part"])}
            )
            state["batches"].append({
                "part":x["part"],"local_input":str(path),"input_file_id":uploaded.id,
                "batch_id":batch.id,"status":batch.status
            })
            save(state_path,state)
            print(f"  {batch.id} status={batch.status}",flush=True)
        print(f"State: {state_path}")
        return

    state=load(args.state)
    if args.cmd=="status":
        for x in state["batches"]:
            b=client.batches.retrieve(x["batch_id"])
            x.update({
                "status":b.status,
                "output_file_id":getattr(b,"output_file_id",None),
                "error_file_id":getattr(b,"error_file_id",None),
                "request_counts":(
                    b.request_counts.model_dump() if getattr(b,"request_counts",None)
                    and hasattr(b.request_counts,"model_dump") else str(getattr(b,"request_counts",None))
                )
            })
            print(f"part={x['part']:03d} {b.id} status={b.status} counts={x['request_counts']}")
        save(args.state,state)
        return

    args.output_dir.mkdir(parents=True,exist_ok=True)
    for x in state["batches"]:
        b=client.batches.retrieve(x["batch_id"])
        x["status"]=b.status
        x["output_file_id"]=getattr(b,"output_file_id",None)
        x["error_file_id"]=getattr(b,"error_file_id",None)
        if b.output_file_id:
            dest=args.output_dir/f"stage6e_luna_batch_{int(x['part']):03d}_output.jsonl"
            dest.write_bytes(client.files.content(b.output_file_id).content)
            x["local_output"]=str(dest)
            print(f"Downloaded {dest}")
        if b.error_file_id:
            dest=args.output_dir/f"stage6e_luna_batch_{int(x['part']):03d}_errors.jsonl"
            dest.write_bytes(client.files.content(b.error_file_id).content)
            x["local_errors"]=str(dest)
            print(f"Downloaded {dest}")
    save(args.state,state)

if __name__=="__main__": main()
