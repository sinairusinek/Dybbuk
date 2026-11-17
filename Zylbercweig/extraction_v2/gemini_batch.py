"""Shared Gemini runner for the v2 delta jobs (A residue, B, C).

Sends items in batches of N as a JSON array; the model returns a JSON array of
results keyed by `id`. Output is append-only JSONL, so a plain re-run resumes and
retries failed batches. Token usage is summed into the log for cost accounting.

    GOOGLE_API_KEY=... python3.11 <job>.py [--limit N] [--workers 12]
"""
import json, os, sys, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

MODEL = "gemini-3-flash-preview"


def done_ids(out_path: Path) -> set:
    ids = set()
    if out_path.exists():
        for line in out_path.open(encoding="utf-8"):
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if "error" not in rec:
                ids.add(rec["id"])
    return ids


def run(items, system_prompt, out_path: Path, batch_size=25, workers=12, model=MODEL):
    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        sys.exit("set GOOGLE_API_KEY or GEMINI_API_KEY")
    os.environ["GOOGLE_API_KEY"] = api_key
    from google import genai
    from google.genai import types

    have = done_ids(out_path)
    todo = [it for it in items if it["id"] not in have]
    print(f"{out_path.name}: {len(items)} items, {len(have)} done, {len(todo)} to do")
    if not todo:
        return
    batches = [todo[i:i + batch_size] for i in range(0, len(todo), batch_size)]
    client = genai.Client(http_options=types.HttpOptions(timeout=180_000))
    lock = threading.Lock()
    usage = {"in": 0, "out": 0, "err": 0, "ok": 0}
    out = out_path.open("a", encoding="utf-8")

    def call(batch):
        payload = json.dumps(batch, ensure_ascii=False)
        last = None
        for attempt in range(4):
            try:
                resp = client.models.generate_content(
                    model=model, contents=payload,
                    config=types.GenerateContentConfig(
                        system_instruction=system_prompt, temperature=0.0,
                        max_output_tokens=32768,
                        response_mime_type="application/json",
                        thinking_config=types.ThinkingConfig(thinking_budget=0)))
                data = json.loads(resp.text)
                if isinstance(data, dict):
                    data = next((v for v in data.values() if isinstance(v, list)), [])
                um = resp.usage_metadata
                return data, (um.prompt_token_count or 0), (um.candidates_token_count or 0)
            except Exception as ex:
                last = ex
                time.sleep(2 ** attempt * 3)
        raise last

    t0 = time.time()
    with ThreadPoolExecutor(workers) as ex:
        futs = {ex.submit(call, b): b for b in batches}
        for n, f in enumerate(as_completed(futs), 1):
            b = futs[f]
            want = {it["id"] for it in b}
            with lock:
                try:
                    data, ti, to = f.result()
                    usage["in"] += ti; usage["out"] += to
                    got = set()
                    for rec in data:
                        if isinstance(rec, dict) and rec.get("id") in want and rec["id"] not in got:
                            rec["model"] = model
                            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                            got.add(rec["id"])
                    for mid in want - got:
                        out.write(json.dumps({"id": mid, "error": "missing_in_response"}) + "\n")
                    usage["ok"] += len(got); usage["err"] += len(want - got)
                except Exception as e:
                    for mid in want:
                        out.write(json.dumps({"id": mid, "error": f"{type(e).__name__}: {e}"[:300]}) + "\n")
                    usage["err"] += len(want)
                out.flush()
                if n % 20 == 0 or n == len(batches):
                    print(f"  {n}/{len(batches)} batches  ok={usage['ok']} err={usage['err']} "
                          f"tok_in={usage['in']:,} tok_out={usage['out']:,} {time.time()-t0:.0f}s", flush=True)
    out.close()
    print(f"DONE {out_path.name}: ok={usage['ok']} err={usage['err']} "
          f"tokens in={usage['in']:,} out={usage['out']:,}")
