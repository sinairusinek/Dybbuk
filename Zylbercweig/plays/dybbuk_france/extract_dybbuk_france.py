#!/usr/bin/env python3.11
"""Dybbuk-in-France dossier: extract every fact that ties a "Dybbuk" work to France
(performances, tours, productions, translations, French-language versions, reception,
screenings, people) from the entries that mention both (candidate_ids.txt), with Gemini.

Big entries are cut to the paragraphs that mention a Dybbuk or a French place (±1 paragraph).

    GOOGLE_API_KEY=... python3.11 extract_dybbuk_france.py [--limit N] [--only id,id]
Output: drafts.jsonl (resumable; error rows are retried on re-run).
"""
import argparse, csv, json, os, re, sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENTRY_TEXTS = HERE.parent.parent / "people" / "entry_texts.tsv"
IDS = HERE / "candidate_ids.txt"
OUT = HERE / "drafts.jsonl"
MODEL = "gemini-3-flash-preview"
MAX_FULL = 30000
csv.field_size_limit(10**9)
POINTS = re.compile(r"[֑-ׇ]")
STEM = re.compile(r"דיבוק|דבוק|דיבעק|דבעק|צווישן צוויי וועלטן|צווישען צוויי וועלטען|באבע חוה")
FR_RX = re.compile(r"פאריז|פראנקרייך|(?<!\S)מעץ|עלזאס|לאטרינגען|דראנסי|ווישי|פיטיוויע|גורס(?!\S)|"
                   r"(?<!\S)ניצא(?!\S)|מארסעל(?!א|\S)|מארסיי|ליאן|ראלאנד|וויטעל|באניע|פראנצויז|"
                   r"פראנצעז|שטראסבורג|נאנסי|באטי(?!\S)|באטיס")

SYSTEM = """You read entries of the Yiddish "Leksikon fun yidishn teater" (Zylbercweig) for a
dossier on THE DYBBUK IN FRANCE. Extract every fact that connects a work called "The Dybbuk"
(דער דיבוק / דבוק / הדיבוק / Le Dibbouk) with France or the French language:

A. Events IN FRANCE (Paris, Metz, Strasbourg, Nancy, Alsace-Lorraine, Lyon, Nice, Marseille,
   anywhere in France): performances, productions, premieres, guest tours (Vilna Troupe,
   Habima, others), rehearsals, readings, lectures, concerts of Dybbuk music, film screenings,
   parodies, amateur productions, reviews/reception by French or Paris critics.
B. FRENCH-LANGUAGE Dybbuks anywhere: French translations, French productions
   (e.g. Gaston Baty's „Le Dibbouk"), French actors/directors, publications in French.
C. A person's Dybbuk activity that the text explicitly ties to their time in France
   (e.g. "in Paris he directed the Dybbuk", "came to Paris with the Dybbuk").
Do NOT report Dybbuk events elsewhere that have no French link, and do NOT report France
events that are not about a Dybbuk work.

Work codes: "ansky" (S. An-ski's play, incl. Habima/Vilna/translations/adaptations),
"lateiner" (Joseph Lateiner's 1880s comedy „דער דיבוק, אָדער די באָבע חוה"), "shomer",
"parody", "opera_music" (opera/ballet/score called or based on the Dybbuk), "film" (e.g. the
1937 film), "other". If unsure, say so in "note".

Return ONLY JSON: {"facts": [ {
  "work": "<code>",
  "work_title": "<title VERBATIM as in text>",
  "link": "in_france" | "french_language" | "person_in_france",
  "fact_type": "performance" | "production" | "premiere" | "tour" | "rehearsal" | "reading"
             | "lecture" | "concert" | "screening" | "translation" | "publication" | "reception"
             | "other",
  "language": "<language of the version, English (Yiddish/Hebrew/French/...) or ''>",
  "place": "<place VERBATIM in Yiddish, or ''>",
  "place_en": "<English name of the town/city (one place only; '' if only the country)>",
  "venue": "<theatre/hall verbatim or ''>",
  "venue_en": "<romanized/French name of the venue or ''>",
  "troupe": "<troupe/company verbatim or ''>",
  "troupe_en": "<romanized/English name of the troupe or ''>",
  "date_start": "YYYY|YYYY-MM|YYYY-MM-DD|''", "date_end": "same|''",
  "date_certainty": "explicit" | "approximate" | "none",
  "people": [ { "name": "<VERBATIM; for the entry subject use the entry heading>",
                "name_en": "<romanized name, surname first: 'Surname, Given'>",
                "is_entry_subject": true|false,
                "role": "author" | "translator" | "adaptor" | "director" | "composer"
                      | "set_designer" | "costume_designer" | "choreographer" | "actor"
                      | "producer" | "manager" | "conductor" | "critic" | "reader"
                      | "audience" | "other",
                "character": "<character played, VERBATIM, or ''>" } ],
  "description": "<one English sentence>",
  "evidence_quote": "<VERBATIM Yiddish snippet copied from the text, <=300 chars>",
  "note": "<English: ambiguity, inference, or ''>"
} ] }
Rules: one fact per distinct event (a tour through several cities: one fact per French city if
the text names them, otherwise one). Dates only as stated or clearly implied; never invent.
Keep names/places verbatim. Return {"facts": []} if the entry has no such fact.
"""


def load_entries():
    with open(ENTRY_TEXTS, encoding="utf-8") as f:
        return {r["person_id"]: r for r in csv.DictReader(f, delimiter="\t")}


def windowed(text):
    if len(text) <= MAX_FULL:
        return text, False
    paras = [p for p in re.split(r"\n+", text) if p.strip()]
    if len(paras) < 3:  # no paragraphing: cut into ~1500-char chunks on sentence ends
        paras = re.findall(r".{1,1500}(?:[.!?]\s|$)", text, flags=re.S)
    keep = set()
    for i, p in enumerate(paras):
        u = POINTS.sub("", p)
        if STEM.search(u) or FR_RX.search(u):
            keep.update({i - 1, i, i + 1})
    out = []
    for i in sorted(k for k in keep if 0 <= k < len(paras)):
        if out and out[-1][0] != i - 1:
            out.append((None, "[...]"))
        out.append((i, paras[i]))
    return "\n".join(p for _, p in out), True


def call(client, types, e, pid):
    text, cut = windowed(e["entry_text"])
    rec = {"person_id": pid, "heading": e["heading"], "volume": e["volume"], "model": MODEL,
           "windowed": cut, "sent_chars": len(text),
           "drafted_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    msg = (f"Entry heading: {e['heading']}\nEntry id: {pid} (volume {e['volume']})\n"
           + ("(Long entry: only the passages about a Dybbuk or France are given.)\n" if cut else "")
           + f"\nEntry text:\n{text}")
    try:
        r = client.models.generate_content(
            model=MODEL, contents=msg,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM, max_output_tokens=65536, temperature=0.0,
                response_mime_type="application/json",
                thinking_config=types.ThinkingConfig(thinking_budget=0)))
        data = json.loads((r.text or "").strip())
        if isinstance(data, list):
            data = {"facts": data}
        rec["facts"] = data.get("facts", [])
    except Exception as ex:
        rec["error"] = f"{type(ex).__name__}: {ex}"
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only")
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    if not (os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")):
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
    client = genai.Client(http_options=types.HttpOptions(timeout=180_000))
    with OUT.open("a", encoding="utf-8") as out, ThreadPoolExecutor(a.workers) as ex:
        futs = [ex.submit(call, client, types, entries[p], p) for p in work]
        for i, f in enumerate(futs, 1):
            rec = f.result()
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out.flush()
            print(f"{i}/{len(work)} {rec['person_id']} facts={len(rec.get('facts', []))}"
                  f" {rec.get('error', '')[:80]}", flush=True)


if __name__ == "__main__":
    main()
