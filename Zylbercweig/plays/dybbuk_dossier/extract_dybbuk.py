#!/usr/bin/env python3.11
"""Dybbuk dossier: extract every Dybbuk-play fact from the Zylbercweig entries that
mention a dybbuk (dybbuk_ids.txt), with Gemini.

There is more than one play called „דער דיבוק": Ansky's (1914-20), Lateiner's comedy
(1880s, a.k.a. „די באָבע חוה"), Shomer's Goldfadn pastiche, plus parodies, operas,
films, translations and adaptations. Each mention is classified to a work first;
facts (creation / adaptation / production ...) hang off the mention.

    GOOGLE_API_KEY=... python3.11 extract_dybbuk.py [--limit N] [--only id,id]
Output: dybbuk_drafts.jsonl (resumable; error rows are retried on re-run).
"""
import argparse, csv, json, os, sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENTRY_TEXTS = HERE.parent.parent / "people" / "entry_texts.tsv"
IDS = HERE / "dybbuk_ids.txt"
OUT = HERE / "dybbuk_drafts.jsonl"
MODEL = "gemini-3-flash-preview"
csv.field_size_limit(10**9)

SYSTEM = """You read entries of the Yiddish "Leksikon fun yidishn teater" (Zylbercweig) and
extract EVERYTHING the entry says about plays/works called "The Dybbuk" (דער דיבוק / דבוק /
הדיבוק / Dybuk), including works known under another title.

Known works (classify each mention to one "work" code):
- "ansky": S. An-ski's „דער דיבוק" / „צווישן צוויי וועלטן" (Between Two Worlds), written
  1914-17 (Russian, then Yiddish), Vilna Troupe premiere 1920, Habima Hebrew version
  (Bialik's translation, Vakhtangov, 1922) — and all later productions/translations of it,
  incl. Gershenzon's or others' adaptations of Ansky's text.
- "lateiner": Joseph Lateiner's comedy „דער דיבוק, אָדער די באָבע חוה" (1880s); characters
  like Bobe Khave, Falik, Yokel, Amalie, Leon.
- "shomer": Shomer (N.M. Shaykevitsh)'s „דער דיבוק", a pastiche of Goldfadn material.
- "parody": a parody/travesty/revue number on the Dybbuk (e.g. „דבוקיאַדע").
- "opera_music": an opera, oratorio, ballet or musical score called/based on the Dybbuk
  (if it is the music FOR a stage production of Ansky's play, use "ansky" with a composer).
- "film": a film of the Dybbuk (e.g. the 1937 Polish Yiddish film).
- "other": another play titled Dybbuk by a different author, or an unidentifiable work.
- "figurative": the word is used figuratively, not about a work (still report it, no facts).
Decide from the entry's context (author named, date, characters, troupe). If unsure between
ansky and another, say so in "work_note" and pick the more likely.

Return ONLY JSON: {"mentions": [ ... ]}, one object per distinct mention-context:
{
  "work": "<code>",
  "work_title": "<title VERBATIM as in text>",
  "work_author": "<author as named in text, verbatim, or ''>",
  "alt_title": "<other title the text gives this work, verbatim, or ''>",
  "work_note": "<English, why you classified it so; ambiguity>",
  "evidence_quote": "<VERBATIM Yiddish snippet, <=300 chars>",
  "facts": [ {
     "fact_type": "writing" | "translation" | "adaptation" | "revision" | "composition"
                | "design" | "rehearsal" | "premiere" | "production" | "performance"
                | "tour" | "reading" | "publication" | "film" | "reception" | "other",
     "language": "<language of the version, English, or ''>",
     "place": "<place VERBATIM in Yiddish, or ''>",
     "place_en": "<English name of the place, or ''>",
     "country": "<modern country in English, or ''>",
     "venue": "<theatre/hall verbatim or ''>",
     "troupe": "<troupe/company/studio verbatim or ''>",
     "date_start": "YYYY|YYYY-MM|YYYY-MM-DD|''", "date_end": "same|''",
     "date_certainty": "explicit" | "approximate" | "none",
     "people": [ { "name": "<VERBATIM; if it is the entry's subject, use the entry heading>",
                   "name_en": "<romanized name>",
                   "is_entry_subject": true|false,
                   "role": "author" | "translator" | "adaptor" | "director" | "composer"
                         | "set_designer" | "costume_designer" | "choreographer"
                         | "actor" | "producer" | "manager" | "conductor" | "prompter"
                         | "critic" | "reader" | "other",
                   "role_detail": "<verbatim role words or ''>",
                   "character": "<character played, VERBATIM, or ''>" } ],
     "description": "<one English sentence>",
     "evidence_quote": "<VERBATIM Yiddish snippet, <=300 chars>"
  } ]
}
Rules:
- Creation/adaptation steps BEFORE production count as facts too (where Ansky wrote it, who
  translated it where and when, who read it to whom, censorship, rehearsals).
- A performer who "played in the Dybbuk" is an actor fact; give the character if named.
- "played the Dybbuk 300 times" / "toured with the Dybbuk over Poland" = performance/tour
  with the place/region named.
- Dates only as stated or clearly implied; never invent. Keep names/places verbatim.
- If the entry merely lists the Dybbuk in a repertoire, still emit a production fact with
  whatever troupe/place/date the context gives, and the entry subject as a person if the
  context says they acted/directed in it.
"""


def load_entries():
    with open(ENTRY_TEXTS, encoding="utf-8") as f:
        return {r["person_id"]: r for r in csv.DictReader(f, delimiter="\t")}


def call(client, types, e, pid):
    rec = {"person_id": pid, "heading": e["heading"], "volume": e["volume"], "model": MODEL,
           "drafted_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    msg = (f"Entry heading: {e['heading']}\nEntry id: {pid} (volume {e['volume']})\n\n"
           f"Entry text:\n{e['entry_text']}")
    try:
        r = client.models.generate_content(
            model=MODEL, contents=msg,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM, max_output_tokens=65536, temperature=0.0,
                response_mime_type="application/json",
                thinking_config=types.ThinkingConfig(thinking_budget=0)))
        data = json.loads((r.text or "").strip())
        if isinstance(data, list):
            data = {"mentions": data}
        rec["mentions"] = data.get("mentions", [])
    except Exception as ex:
        rec["error"] = f"{type(ex).__name__}: {ex}"
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only")
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not key:
        sys.exit("set GOOGLE_API_KEY")
    from google import genai
    from google.genai import types
    ids = [l.strip() for l in IDS.read_text().splitlines() if l.strip()]
    if a.only:
        ids = [i for i in ids if i in set(a.only.split(","))]
    done = set()
    if OUT.exists():
        for l in OUT.open(encoding="utf-8"):
            r = json.loads(l)
            if "error" not in r:
                done.add(r["person_id"])
    work = [i for i in ids if i not in done]
    if a.limit:
        work = work[:a.limit]
    print(f"in scope {len(ids)} done {len(done)} to do {len(work)}")
    entries = load_entries()
    client = genai.Client(http_options=types.HttpOptions(timeout=240_000))
    with OUT.open("a", encoding="utf-8") as out, ThreadPoolExecutor(a.workers) as ex:
        futs = [ex.submit(call, client, types, entries[p], p) for p in work]
        for i, f in enumerate(futs, 1):
            rec = f.result()
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out.flush()
            print(f"{i}/{len(work)} {rec['person_id']} mentions={len(rec.get('mentions', []))}"
                  f" {rec.get('error', '')[:80]}", flush=True)


if __name__ == "__main__":
    main()
