"""D–G full-text pass (stations, events, performance facts, sources): SCHEMA_DRAFT §2–§7.

One entry (or one part of a long entry) per call; output is append-only JSONL keyed by
`id` = entry_id#part, so a plain re-run resumes. Token use is stored on every record.

  python3.11 defg_job.py pilot-ids                 # -> qa/defg_pilot_ids.txt (Vol 3 sample)
  GOOGLE_API_KEY=... python3.11 defg_job.py run --ids-file qa/defg_pilot_ids.txt --out defg_pilot.jsonl [--limit N]
  python3.11 defg_job.py dump --ids-file ... --dir <dir>   # same inputs as files, for a Claude reference run
  python3.11 defg_job.py stats defg_pilot.jsonl            # tokens per entry / per 1k chars, cost
"""
import argparse, csv, json, os, random, re, sys, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).parent
ENTRY_TEXTS = HERE.parent / "people" / "entry_texts.tsv"
PROMPT = HERE / "prompts" / "defg.txt"
MODEL = "gemini-3-flash-preview"
PRICE_IN, PRICE_OUT = 0.50, 3.00  # $ per M tokens assumed for gemini-3-flash-preview (as in 8f)
PART_CHARS = 24000
csv.field_size_limit(10**9)


def entries():
    return {r["person_id"]: r for r in csv.DictReader(ENTRY_TEXTS.open(encoding="utf-8"), delimiter="\t")}


def split_text(t, n=PART_CHARS):
    """Cut at a sentence end (". ") near every n chars; never mid-word."""
    parts, i = [], 0
    while len(t) - i > n * 1.25:
        j = t.rfind(". ", i + int(n * 0.6), i + n)
        j = (j + 1) if j > 0 else (t.find(" ", i + n) if t.find(" ", i + n) > 0 else len(t))
        parts.append(t[i:j].strip()); i = j
    parts.append(t[i:].strip())
    return [p for p in parts if p]


def items(ids, ents):
    out = []
    for pid in ids:
        e = ents[pid]
        ps = split_text(e["entry_text"])
        for k, p in enumerate(ps, 1):
            out.append({"id": f"{pid}#{k}", "entry_id": pid, "heading": e["heading"],
                        "volume": e["volume"], "part": k, "parts": len(ps), "text": p})
    return out


def itinerary_ids():
    ids = set()
    for f in (HERE.parent / "plays").rglob("*.jsonl"):
        if not any(k in str(f) for k in ("itiner", "dossier", "france", "dybbuk")):
            continue
        for line in f.open(encoding="utf-8"):
            try:
                d = json.loads(line)
            except Exception:
                continue
            pid = d.get("person_id") or d.get("id")
            if pid and "error" not in d and d.get("stations") is not None:
                ids.add(pid)
    return ids


def pilot_ids(ents, seed=1115):
    """24 Volume-3 entries: 8 with an old itinerary (QA, decision 3) + 16 by length quartile,
    including the 2 longest under 100k chars (tests the part split)."""
    rnd = random.Random(seed)
    v3 = sorted((e for e in ents.values() if e["volume"] == "3" and len(e["entry_text"]) > 300),
                key=lambda e: len(e["entry_text"]))
    it = itinerary_ids()
    with_it = [e for e in v3 if e["person_id"] in it]
    rest = [e for e in v3 if e["person_id"] not in it and len(e["entry_text"]) < 100000]
    pick = rnd.sample(with_it, 8)
    long2 = rest[-2:]
    q = len(rest[:-2]) // 4
    for k in range(4):
        pick += rnd.sample(rest[k * q:(k + 1) * q], 3 if k < 2 else 4)
    pick += long2
    return [e["person_id"] for e in pick]


def run(its, out_path, workers=8, thinking=0, model=MODEL):
    key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not key:
        sys.exit("set GOOGLE_API_KEY or GEMINI_API_KEY")
    os.environ["GOOGLE_API_KEY"] = key
    from google import genai
    from google.genai import types
    have = set()
    if out_path.exists():
        for line in out_path.open(encoding="utf-8"):
            r = json.loads(line)
            if "error" not in r:
                have.add(r["id"])
    todo = [it for it in its if it["id"] not in have]
    print(f"{out_path.name}: {len(its)} parts, {len(have)} done, {len(todo)} to do", flush=True)
    client = genai.Client(http_options=types.HttpOptions(timeout=600_000))
    prompt = PROMPT.read_text(encoding="utf-8")
    lock, tot = threading.Lock(), {"in": 0, "out": 0, "think": 0, "ok": 0, "err": 0}
    out = out_path.open("a", encoding="utf-8")

    def call(it):
        last = None
        for attempt in range(3):
            try:
                resp = client.models.generate_content(
                    model=model, contents=json.dumps(it, ensure_ascii=False),
                    config=types.GenerateContentConfig(
                        system_instruction=prompt, temperature=0.0, max_output_tokens=65536,
                        response_mime_type="application/json",
                        thinking_config=types.ThinkingConfig(thinking_budget=thinking)))
                um = resp.usage_metadata
                return (json.loads(resp.text), um.prompt_token_count or 0,
                        um.candidates_token_count or 0, getattr(um, "thoughts_token_count", 0) or 0)
            except Exception as ex:
                last = ex
                time.sleep(3 * 2 ** attempt)
        raise last

    with ThreadPoolExecutor(workers) as ex:
        futs = {ex.submit(call, it): it for it in todo}
        for n, f in enumerate(as_completed(futs), 1):
            it = futs[f]
            with lock:
                try:
                    data, ti, to, th = f.result()
                    rec = {"id": it["id"], "entry_id": it["entry_id"], "part": it["part"],
                           "chars": len(it["text"]), "model": model,
                           "tok_in": ti, "tok_out": to, "tok_think": th, "result": data}
                    tot["in"] += ti; tot["out"] += to; tot["think"] += th; tot["ok"] += 1
                except Exception as e:
                    rec = {"id": it["id"], "error": f"{type(e).__name__}: {e}"[:300]}
                    tot["err"] += 1
                out.write(json.dumps(rec, ensure_ascii=False) + "\n"); out.flush()
                print(f"  {n}/{len(todo)} {it['id']} ok={tot['ok']} err={tot['err']} "
                      f"in={tot['in']:,} out={tot['out']:,} think={tot['think']:,}", flush=True)
    out.close()
    cost = tot["in"] / 1e6 * PRICE_IN + (tot["out"] + tot["think"]) / 1e6 * PRICE_OUT
    print(f"DONE ok={tot['ok']} err={tot['err']} in={tot['in']:,} out={tot['out']:,} "
          f"think={tot['think']:,} ≈ ${cost:.2f} at ${PRICE_IN}/${PRICE_OUT} per M")


def stats(path, ents):
    recs = [json.loads(l) for l in Path(path).open(encoding="utf-8")]
    ok = [r for r in recs if "error" not in r]
    ch = sum(r["chars"] for r in ok)
    ti = sum(r["tok_in"] for r in ok); to = sum(r["tok_out"] + r.get("tok_think", 0) for r in ok)
    print(f"{len(ok)} parts ok, {len(recs) - len(ok)} errors; {ch:,} chars; in {ti:,} out {to:,}")
    print(f"per 1k chars: in {ti / ch * 1000:.0f}, out {to / ch * 1000:.0f}")
    allc = sum(len(e["entry_text"]) for e in ents.values())
    est = allc / ch * (ti / 1e6 * PRICE_IN + to / 1e6 * PRICE_OUT)
    print(f"pilot ≈ ${ti / 1e6 * PRICE_IN + to / 1e6 * PRICE_OUT:.2f}; scaled to all {len(ents)} entries "
          f"({allc:,} chars) ≈ ${est:.2f} at ${PRICE_IN}/${PRICE_OUT} per M")
    lay = {k: 0 for k in ("stations", "events", "performance_facts", "source_uses", "schema_issues")}
    for r in ok:
        for k in lay:
            lay[k] += len(r["result"].get(k, []) or [])
    print(lay)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["pilot-ids", "run", "dump", "stats"])
    ap.add_argument("path", nargs="?")
    ap.add_argument("--ids-file", default=str(HERE / "qa" / "defg_pilot_ids.txt"))
    ap.add_argument("--out", default="defg_pilot.jsonl")
    ap.add_argument("--dir", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--thinking", type=int, default=0)
    a = ap.parse_args()
    ents = entries()
    if a.cmd == "pilot-ids":
        ids = pilot_ids(ents)
        Path(a.ids_file).write_text("\n".join(ids) + "\n", encoding="utf-8")
        for pid in ids:
            print(pid, len(ents[pid]["entry_text"]), ents[pid]["heading"][:40])
        return
    if a.cmd == "stats":
        return stats(a.path, ents)
    ids = [l.strip() for l in Path(a.ids_file).read_text(encoding="utf-8").splitlines() if l.strip()]
    if a.limit:
        ids = ids[: a.limit]
    its = items(ids, ents)
    if a.cmd == "dump":
        d = Path(a.dir); d.mkdir(parents=True, exist_ok=True)
        for it in its:
            (d / (it["id"].replace("#", "__") + ".json")).write_text(
                json.dumps(it, ensure_ascii=False), encoding="utf-8")
        print(f"{len(its)} parts -> {d}")
        return
    run(its, HERE / a.out, workers=a.workers, thinking=a.thinking)


if __name__ == "__main__":
    main()
