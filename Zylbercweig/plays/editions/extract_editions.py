#!/usr/bin/env python3.11
"""Step 2b/3: extract publication events from Lexicon windows with Gemini.

Input is a windows TSV (candidate_windows.tsv, or a pilot file). Each entry is
sent as one request -- long windows are cut into parts of <= PART_CHARS on
"…" / sentence boundaries. Raw replies are appended to a JSONL cache keyed by
(xml_id, part), so the run is resumable and re-running never re-pays; the
flattened TSV is rebuilt from the cache every time.

    python3.11 extract_editions.py --in pilot_windows.tsv --tag pilot
    python3.11 extract_editions.py --in pilot_recall.tsv  --tag recall
    python3.11 extract_editions.py --in candidate_windows.tsv --tag full --dry-run

Outputs: raw_<tag>.jsonl (cache), editions_<tag>.tsv (one row per record).
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import json
import pathlib
import re
import sys
import threading
import time

HERE = pathlib.Path(__file__).resolve().parent
csv.field_size_limit(10**9)

MODEL = "gemini-3.8-flash"
REQUEST_TIMEOUT_MS = 180_000
PART_CHARS = 12_000
# Thinking tokens count against this budget: at 16k a 60-item list (Tunkeler)
# came back cut off mid-JSON on four tries.
MAX_OUTPUT_TOKENS = 64_000

SYSTEM_PROMPT = """\
You read passages from Zalmen Zylbercweig's Lexicon of the Yiddish Theatre
(Yiddish, Hebrew script). Each passage comes from the entry on one person, the
SUBJECT, named in the heading. Initials such as "ל.'ס" or "ב." usually refer to
the subject.

Find every PUBLICATION EVENT the passage reports: a specific, named work
appearing in print -- as a book or booklet, inside an anthology or collected
works, or in a newspaper or journal (including serialisation). The work may be
by the subject or by anyone else (e.g. a play by another author whose printing
is mentioned in passing).

One record per event. The same work printed twice (first in a newspaper, later
as a book; a second edition) is two records. A list of printed works gives one
record per item. A collection that is itself named gets its own record, and
each named work said to be in it gets a record with pub_kind "collection".

A work that is clearly identified but not named in the passage ("the play",
"his first drama") is still recorded: title "" and say what it is in
genre_phrase.

DO NOT record:
- generic activity with no identifiable work ("published poems and articles in the
  press", "wrote many one-act plays");
- works that were written or performed but explicitly NOT printed, or exist
  only in manuscript;
- bibliographic citations of sources ABOUT the subject (articles, lexicons,
  memoirs cited as references);
- a periodical merely being founded or edited, without a named work in it.

Copy all Yiddish/Hebrew strings VERBATIM from the passage: same spelling, same
vowel points, no added or removed diacritics, no transliteration. Leave a field
"" when the passage does not say it. Never guess a year or a place.

WHICH WORK IS IT. The Lexicon often gives one work under several names: staged
"א"נ" (under the name) another title, printed "א"ט" (under the title) another,
"the same play ... as", translated from an original, or a double title "X אָדער
Y". Whenever the passage STATES that two or more titles are the same work --
whether or not it was printed -- add a "same_work" group. Do not group titles
merely because they look alike; only what the passage says.

Return JSON: {"records": [ ... ], "same_work": [ ... ]}.
Each record:
{
 "title": "work title as printed, without surrounding quotes",
 "genre_phrase": "descriptor printed with it, e.g. דראַמע אין 4 אַקטן",
 "work_kind": "play" | "play_translation" | "play_adaptation" | "libretto"
              | "song" | "poetry" | "prose" | "nonfiction" | "other",
 "author": "author as named in the passage, or 'SUBJECT'",
 "subject_role": "author" | "co_author" | "translator" | "adapter" | "editor"
                 | "publisher" | "none",
 "original_title": "for translations/adaptations: title of the source work",
 "original_author": "for translations/adaptations: its author",
 "alt_titles": ["other titles the passage gives for this same work"],
 "pub_kind": "book" | "periodical" | "collection" | "unknown",
 "container": "newspaper/journal/anthology/collected-works title",
 "publisher": "publishing house as printed",
 "place": "", "year": "", "date_detail": "issue numbers, day/month, pages",
 "edition_note": "e.g. 2nd edition, unauthorised, incomplete",
 "evidence": "verbatim span (<= 250 chars) that states the publication",
 "confidence": "high" | "medium" | "low"
}
Each same_work group:
{
 "titles": ["every title the passage gives for the work, verbatim"],
 "author": "author as named in the passage, or 'SUBJECT'",
 "how": "e.g. staged under another name / translation of / double title",
 "evidence": "verbatim span (<= 250 chars) that states the identity"
}
If there is nothing, return {"records": [], "same_work": []}.
"""

FIELDS = ["xml_id", "part", "person_id", "volume", "heading", "rec_no",
          "title", "genre_phrase", "work_kind", "author", "subject_role",
          "original_title", "original_author", "alt_titles", "pub_kind",
          "container",
          "publisher", "place", "year", "date_detail", "edition_note",
          "evidence", "confidence"]


SW_FIELDS = ["xml_id", "part", "person_id", "heading", "group_no", "titles",
             "author", "how", "evidence"]

_S = {"type": "string"}
_ARR = {"type": "array", "items": _S}
# Enforced output schema. Free-form JSON mode let ~2% of replies through with
# unescaped quotes from Yiddish abbreviations (ד"ר, תר"פּ, א"נ) -- every one a
# lost request.
RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "records": {"type": "array", "items": {
            "type": "object",
            "properties": {k: (_ARR if k == "alt_titles" else _S) for k in [
                "title", "genre_phrase", "work_kind", "author", "subject_role",
                "original_title", "original_author", "alt_titles", "pub_kind",
                "container", "publisher", "place", "year", "date_detail",
                "edition_note", "evidence", "confidence"]},
            "required": ["title", "work_kind", "pub_kind", "evidence"]}},
        "same_work": {"type": "array", "items": {
            "type": "object",
            "properties": {"titles": _ARR, "author": _S, "how": _S,
                           "evidence": _S},
            "required": ["titles", "evidence"]}},
    },
    "required": ["records", "same_work"],
}


def read_tsv(p):
    with open(p, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def split_parts(text: str) -> list[str]:
    if len(text) <= PART_CHARS:
        return [text]
    pieces = re.split(r"(?<=[.!?…])\s+", text)
    parts, cur = [], ""
    for p in pieces:
        if cur and len(cur) + len(p) + 1 > PART_CHARS:
            parts.append(cur)
            cur = p
        else:
            cur = f"{cur} {p}" if cur else p
    if cur:
        parts.append(cur)
    return parts


def parse_json_loose(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, re.S)
        return json.loads(m.group(0)) if m else {}


def make_client():
    from google import genai
    return genai.Client()


def ask(client, heading: str, passage: str) -> tuple[dict, str]:
    from google.genai import types
    resp = client.models.generate_content(
        model=MODEL,
        contents=f"SUBJECT (entry heading): {heading}\n\nPASSAGE:\n{passage}",
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            temperature=0.0,
            response_mime_type="application/json",
            response_json_schema=RESPONSE_SCHEMA,
            # Without a client-side deadline a hung socket stalls the run
            # forever (eponym judge, 2026-10-14).
            http_options=types.HttpOptions(timeout=REQUEST_TIMEOUT_MS),
        ),
    )
    text = (resp.text or "").strip()
    try:
        finish = str(resp.candidates[0].finish_reason)
    except (AttributeError, IndexError, TypeError):
        finish = "?"
    try:
        return parse_json_loose(text), text
    except json.JSONDecodeError as e:
        raise ValueError(f"bad JSON (finish={finish}, {len(text)} chars): {e}")


def preflight(client) -> None:
    from google.genai import types
    try:
        r = client.models.generate_content(
            model=MODEL, contents="Reply with the single word: ok",
            config=types.GenerateContentConfig(max_output_tokens=20,
                                               temperature=0.0))
    except Exception as e:  # noqa: BLE001
        sys.exit(f"preflight failed for {MODEL}: {type(e).__name__}: "
                 f"{str(e)[:300]}\n(nothing was extracted)")
    print(f"preflight ok: {MODEL} -> {(r.text or '').strip()[:20]!r}")


def load_cache(path: pathlib.Path) -> dict:
    done = {}
    if path.exists():
        for line in path.open(encoding="utf-8"):
            rec = json.loads(line)
            if rec.get("ok"):
                done[(rec["xml_id"], rec["part"])] = rec
    return done


def flatten(cache: dict, meta: dict, out: pathlib.Path) -> int:
    rows = []
    for (xml_id, part), rec in sorted(cache.items()):
        m = meta.get(xml_id, {})
        for i, r in enumerate(rec["result"].get("records", []) or []):
            row = {k: (r.get(k) or "") for k in FIELDS}
            if isinstance(row["alt_titles"], list):
                row["alt_titles"] = " | ".join(row["alt_titles"])
            row.update(xml_id=xml_id, part=part, rec_no=i,
                       person_id=m.get("person_id", ""),
                       volume=m.get("volume", ""), heading=m.get("heading", ""))
            for k, v in row.items():
                if isinstance(v, str):
                    row[k] = v.replace("\t", " ").replace("\n", " ")
            rows.append(row)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    groups = []
    for (xml_id, part), rec in sorted(cache.items()):
        m = meta.get(xml_id, {})
        for i, g in enumerate(rec["result"].get("same_work", []) or []):
            groups.append({
                "xml_id": xml_id, "part": part, "group_no": i,
                "person_id": m.get("person_id", ""),
                "heading": m.get("heading", ""),
                "titles": " | ".join(g.get("titles") or []),
                "author": g.get("author") or "", "how": g.get("how") or "",
                "evidence": (g.get("evidence") or "").replace("\t", " ")
                .replace("\n", " ")})
    sw = out.with_name(out.name.replace("editions_", "same_work_"))
    with sw.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SW_FIELDS, delimiter="\t",
                           lineterminator="\n")
        w.writeheader()
        w.writerows(groups)
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    rows = read_tsv(HERE / a.inp)
    meta = {r["xml_id"]: r for r in rows}
    cache_path = HERE / f"raw_{a.tag}.jsonl"
    cache = load_cache(cache_path)
    work = []
    for r in rows:
        for i, p in enumerate(split_parts(r["window_text"])):
            if (r["xml_id"], i) not in cache:
                work.append((r["xml_id"], i, r["heading"], p))
    if a.limit:
        work = work[:a.limit]
    chars = sum(len(w[3]) for w in work)
    print(f"{len(rows)} entries, {len(work)} parts to do, {chars:,} chars, "
          f"{len(cache)} cached  model={MODEL}")
    if a.dry_run or not work:
        n = flatten(cache, meta, HERE / f"editions_{a.tag}.tsv")
        print(f"flattened {n} records from cache -> editions_{a.tag}.tsv")
        return

    client = make_client()
    preflight(client)
    lock = threading.Lock()

    def run(item):
        xml_id, part, heading, passage = item
        err = ""
        for attempt in range(4):
            try:
                result, raw = ask(client, heading, passage)
                return {"xml_id": xml_id, "part": part, "ok": True,
                        "result": result, "raw": raw}
            except Exception as e:  # noqa: BLE001
                err = f"{type(e).__name__}: {str(e)[:200]}"
                print(f"  retry {attempt + 1} {xml_id}/{part}: {err}")
                time.sleep(5 * (attempt + 1))
        return {"xml_id": xml_id, "part": part, "ok": False, "error": err}

    n_ok = n_err = 0
    with cf.ThreadPoolExecutor(a.workers) as pool, \
            cache_path.open("a", encoding="utf-8") as fout:
        futs = [pool.submit(run, w) for w in work]
        for k, fut in enumerate(cf.as_completed(futs), 1):
            rec = fut.result()
            with lock:
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                fout.flush()
            if rec["ok"]:
                n_ok += 1
                cache[(rec["xml_id"], rec["part"])] = rec
            else:
                n_err += 1
            if k % 25 == 0:
                print(f"  {k}/{len(work)}  ok={n_ok} err={n_err}")
    n = flatten(cache, meta, HERE / f"editions_{a.tag}.tsv")
    print(f"done ok={n_ok} err={n_err}; {n} records -> editions_{a.tag}.tsv")


if __name__ == "__main__":
    main()
