"""D–G full run through the Gemini Batch API (half the price of live calls; results within 24 h).

  python3.11 defg_batch.py build [--ids-file F]   # -> batch/req_NN.jsonl (all entries by default; skips parts already OK in defg_full.jsonl)
  GOOGLE_API_KEY=... python3.11 defg_batch.py submit [--shards 0,1]   # upload + create jobs -> batch/jobs.tsv
  GOOGLE_API_KEY=... python3.11 defg_batch.py status
  GOOGLE_API_KEY=... python3.11 defg_batch.py collect   # finished jobs -> defg_full.jsonl (same record format as defg_job.py)
Then `python3.11 defg_job.py stats defg_full.jsonl`. Re-run build + submit to retry failed parts.
"""
import argparse, csv, json, os, sys
from pathlib import Path
HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
import defg_job as dj

BATCH = HERE / "batch"
JOBS = BATCH / "jobs.tsv"
OUT = HERE / "defg_full.jsonl"
SHARD = 1500  # requests per batch job


def client():
    key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not key:
        sys.exit("set GOOGLE_API_KEY or GEMINI_API_KEY")
    from google import genai
    return genai.Client(api_key=key)


def done_ids():
    ok = set()
    if OUT.exists():
        for line in OUT.open(encoding="utf-8"):
            r = json.loads(line)
            if "error" not in r:
                ok.add(r["id"])
    return ok


def build(a):
    ents = dj.entries()
    ids = ([l.strip() for l in Path(a.ids_file).read_text(encoding="utf-8").splitlines() if l.strip()]
           if a.ids_file else sorted(ents))
    its = [it for it in dj.items(ids, ents, a.part_chars) if it["id"] not in done_ids()]
    prompt = dj.PROMPT.read_text(encoding="utf-8")
    BATCH.mkdir(exist_ok=True)
    for f in BATCH.glob("req_*.jsonl"):
        f.unlink()
    meta = {}
    for k in range(0, len(its), SHARD):
        with (BATCH / f"req_{k // SHARD:02d}.jsonl").open("w", encoding="utf-8") as fh:
            for it in its[k:k + SHARD]:
                meta[it["id"]] = {"entry_id": it["entry_id"], "part": it["part"], "chars": len(it["text"])}
                fh.write(json.dumps({"key": it["id"], "request": {
                    "contents": [{"role": "user", "parts": [{"text": json.dumps(it, ensure_ascii=False)}]}],
                    "system_instruction": {"parts": [{"text": prompt}]},
                    "generation_config": {"temperature": 0.0, "max_output_tokens": 65536,
                                          "response_mime_type": "application/json",
                                          "thinking_config": {"thinking_budget": a.thinking}}}},
                    ensure_ascii=False) + "\n")
    (BATCH / "meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    ch = sum(m["chars"] for m in meta.values())
    print(f"{len(its)} requests in {(len(its) + SHARD - 1) // SHARD} shards, {ch:,} chars")


def submit(a):
    from google.genai import types
    c = client()
    shards = sorted(BATCH.glob("req_*.jsonl"))
    if a.shards:
        want = {int(x) for x in a.shards.split(",")}
        shards = [s for s in shards if int(s.stem.split("_")[1]) in want]
    new = not JOBS.exists()
    with JOBS.open("a", encoding="utf-8") as fh:
        if new:
            fh.write("shard\tjob\tfile\n")
        for s in shards:
            up = c.files.upload(file=str(s), config=types.UploadFileConfig(display_name=s.name, mime_type="jsonl"))
            job = c.batches.create(model=dj.MODEL, src=up.name, config={"display_name": f"defg-{s.stem}"})
            fh.write(f"{s.stem}\t{job.name}\t{up.name}\n"); fh.flush()
            print("submitted", s.name, job.name)


def jobs():
    return list(csv.DictReader(JOBS.open(encoding="utf-8"), delimiter="\t")) if JOBS.exists() else []


def status(a):
    c = client()
    for j in jobs():
        b = c.batches.get(name=j["job"])
        print(j["shard"], b.state.name, getattr(b, "batch_stats", "") or "")


def collect(a):
    c = client()
    meta = json.loads((BATCH / "meta.json").read_text(encoding="utf-8"))
    seen = done_ids()
    tot = {"in": 0, "out": 0, "think": 0, "ok": 0, "err": 0}
    with OUT.open("a", encoding="utf-8") as out:
        for j in jobs():
            b = c.batches.get(name=j["job"])
            if b.state.name not in ("JOB_STATE_SUCCEEDED", "JOB_STATE_PARTIALLY_SUCCEEDED"):
                print(j["shard"], b.state.name, "- not collected"); continue
            raw = c.files.download(file=b.dest.file_name).decode("utf-8")
            for line in raw.splitlines():
                r = json.loads(line); key = r.get("key"); m = meta.get(key, {})
                if key in seen:
                    continue
                rec = {"id": key, **m, "model": dj.MODEL}
                try:
                    resp = r["response"]
                    txt = "".join(p.get("text", "") for p in resp["candidates"][0]["content"]["parts"])
                    um = resp.get("usageMetadata", {})
                    rec.update(tok_in=um.get("promptTokenCount", 0), tok_out=um.get("candidatesTokenCount", 0),
                               tok_think=um.get("thoughtsTokenCount", 0), result=json.loads(txt))
                    tot["in"] += rec["tok_in"]; tot["out"] += rec["tok_out"]; tot["think"] += rec["tok_think"]; tot["ok"] += 1
                except Exception as e:
                    rec = {"id": key, "error": f"{type(e).__name__}: {e}"[:300]}; tot["err"] += 1
                out.write(json.dumps(rec, ensure_ascii=False) + "\n"); seen.add(key)
            print(j["shard"], "collected")
    cost = (tot["in"] / 1e6 * dj.PRICE_IN + (tot["out"] + tot["think"]) / 1e6 * dj.PRICE_OUT) / 2
    print(f"{tot} ≈ ${cost:.2f} at batch price (half of ${dj.PRICE_IN}/${dj.PRICE_OUT} per M)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "submit", "status", "collect"])
    ap.add_argument("--ids-file", default="")
    ap.add_argument("--shards", default="")
    ap.add_argument("--part-chars", type=int, default=dj.PART_CHARS)
    ap.add_argument("--thinking", type=int, default=0)
    a = ap.parse_args()
    {"build": build, "submit": submit, "status": status, "collect": collect}[a.cmd](a)
