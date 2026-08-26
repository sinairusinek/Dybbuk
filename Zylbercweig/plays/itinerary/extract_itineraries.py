#!/usr/bin/env python3.11
"""Istanbul itinerary pilot: extract full station/event itineraries from the
79 Zylbercweig entries that mention Istanbul.

Model contract (station = spine, events attach to stations):
  station: subject x place x interval, in narrative order (seq).
  event:   point occurrence inside a station (performance, founding, ...).

Reusable beyond the pilot: --ids-file / --out / --prompt-extra retarget it at any
entry set (the person-dossier track uses this). Defaults preserve pilot behaviour.

Usage:
    GOOGLE_API_KEY=... python3.11 extract_itineraries.py --limit 5   # calibration
    GOOGLE_API_KEY=... python3.11 extract_itineraries.py            # all 79
    GOOGLE_API_KEY=... python3.11 extract_itineraries.py \
        --ids-file ../person_dossier/rumshinsky_ids.txt \
        --out ../person_dossier/rumshinsky_stations.jsonl \
        --prompt-extra "The subject is a composer ..."

Output: itinerary_drafts.jsonl (one JSON object per entry; resumable), or --out.
"""
import argparse, csv, json, os, re, sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENTRY_TEXTS = HERE.parent.parent / "people" / "entry_texts.tsv"
IDS_FILE = HERE / "istanbul_entry_ids.txt"
OUT = HERE / "itinerary_drafts.jsonl"
DEFAULT_MODEL = "gemini-3-flash-preview"

csv.field_size_limit(10**8)

SYSTEM_PROMPT = """You extract biographical itineraries from entries of the Yiddish-language
"Leksikon fun yidishn teater" (Zylbercweig). The entry text is unpunctuated-ish OCR'd Yiddish;
entries narrate a theater person's life roughly chronologically.

Return ONLY a JSON object: {"stations": [...]}.

A STATION is one stay/presence of a subject at a place: an element of an itinerary.
Emit stations in NARRATIVE ORDER. For each station:

{
  "seq": <1-based narrative order>,
  "subject": "entry" OR the name of another person/org when the text narrates someone
             else's whereabouts (e.g. a father's, or testimony about a colleague),
  "subject_type": "person" | "org",
  "place": "<place name VERBATIM from the text, in Yiddish>",
  "place_kind": "settlement" | "region" | "country" | "venue_only" | "unknown",
  "org": "<troupe/theatre/organization the subject is with at this station, verbatim, or ''>",
  "role": "<subject's role there: performer/director/prompter/student/businessman/... or ''>",
  "verb_class": "born" | "settled" | "stay" | "pass_through" | "tour" | "flight"
              | "emigration" | "return" | "visit" | "death" | "unknown",
  "date_start": "YYYY" | "YYYY-MM" | "YYYY-MM-DD" | "",
  "date_end":   same forms | "",
  "date_certainty": "explicit" | "approximate" | "none",
  "events": [ { "event_type": "performance" | "premiere" | "debut" | "season"
                            | "founding" | "marriage" | "death" | "burial"
                            | "business" | "conflict" | "other",
                "play": "<play title if any, verbatim>",
                "venue": "<theatre/venue name if any, verbatim>",
                "date": "YYYY[-MM[-DD]]" | "",
                "description": "<one short clause, in English>" } ],
  "evidence_quote": "<short VERBATIM Yiddish snippet supporting the station>"
}

Rules:
- Stations are the spine; every event goes INSIDE the station where it happened.
  Never emit an event twice. A performance mention implies a station at that place.
- "toured over Romania/Galicia" = ONE station with verb_class "tour" and the region as place.
- A list like "played in X, Y, Z" = separate consecutive stations (verb_class "stay"),
  unless it is a summary tour (then one "tour" station, place = the listed region/route,
  place_kind "region").
- Dates: only what the text states (date_certainty "explicit"), or clearly implies
  ("approximate"). NEVER interpolate dates yourself; leave "" with certainty "none".
- Third-person testimony about others' itineraries: subject = that person's name.
- Birth/death are stations (verb_class born/death) with a matching event only if
  ceremonial detail is given (e.g. burial).
- Do NOT extract: mere origins of relatives with no movement narrative, play titles
  containing place names (e.g. "di royz fun Stambul"), publication venues of texts.
- Keep place names exactly as written (OCR errors included).
"""


def load_entries():
    rows = {}
    with open(ENTRY_TEXTS, encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            rows[row["person_id"]] = row
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only", help="comma-separated person_ids")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--redo", action="store_true", help="re-run even if already in output")
    ap.add_argument("--ids-file", default=str(IDS_FILE),
                    help="file of entry person_ids, one per line "
                         "(default: the Istanbul pilot list)")
    ap.add_argument("--out", default=str(OUT),
                    help="output .jsonl (default: itinerary_drafts.jsonl)")
    ap.add_argument("--prompt-extra", default="",
                    help="extra guidance appended to the system prompt")
    args = ap.parse_args()

    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        sys.exit("set GOOGLE_API_KEY or GEMINI_API_KEY in env")
    os.environ["GOOGLE_API_KEY"] = api_key
    from google import genai
    from google.genai import types

    ids_file, out_path = Path(args.ids_file), Path(args.out)
    system_prompt = SYSTEM_PROMPT
    if args.prompt_extra:
        system_prompt += "\n\nAdditional guidance for this run:\n" + args.prompt_extra

    ids = [l.strip() for l in ids_file.read_text(encoding="utf-8").splitlines()
           if l.strip() and not l.startswith("#")]
    if args.only:
        ids = [i for i in ids if i in set(args.only.split(","))]
    entries = load_entries()

    missing = [i for i in ids if i not in entries]
    if missing:
        sys.exit(f"person_ids not found in {ENTRY_TEXTS.name}: {missing[:5]}")

    done = set()
    if out_path.exists() and not args.redo:
        with out_path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    done.add(json.loads(line)["person_id"])
                except Exception:
                    pass
    work = [i for i in ids if i not in done or args.redo]
    if args.limit:
        work = work[:args.limit]
    print(f"entries in scope: {len(ids)}  done: {len(done)}  to do: {len(work)}")
    if not work:
        return

    client = genai.Client()
    mode = "a" if out_path.exists() and not args.redo else "w"
    n_st = n_ev = errors = 0
    with out_path.open(mode, encoding="utf-8") as out:
        for i, pid in enumerate(work, 1):
            e = entries[pid]
            user_msg = (f"Entry heading: {e['heading']}\nEntry id: {pid} "
                        f"(volume {e['volume']})\n\nEntry text:\n{e['entry_text']}")
            rec = {"person_id": pid, "heading": e["heading"], "volume": e["volume"],
                   "model": args.model,
                   "drafted_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            try:
                resp = client.models.generate_content(
                    model=args.model,
                    contents=user_msg,
                    config=types.GenerateContentConfig(
                        system_instruction=system_prompt,
                        max_output_tokens=65536,
                        temperature=0.0,
                        response_mime_type="application/json",
                        thinking_config=types.ThinkingConfig(thinking_budget=0),
                    ),
                )
                try:
                    fr = str(resp.candidates[0].finish_reason)
                except Exception:
                    fr = ""
                if fr and "STOP" not in fr:
                    print(f"  WARNING {pid}: finish_reason={fr} "
                          f"(entry {len(e['entry_text']):,} chars) — output may be truncated")
                text = (resp.text or "").strip()
                data = json.loads(text)
                if isinstance(data, list):
                    # bare list of stations, or a list wrapping {"stations": [...]}
                    if len(data) == 1 and isinstance(data[0], dict) and "stations" in data[0]:
                        stations = data[0]["stations"]
                    else:
                        stations = data
                else:
                    stations = data.get("stations", [])
                rec["stations"] = stations
                n_st += len(stations)
                n_ev += sum(len(s.get("events") or []) for s in stations)
            except Exception as ex:
                rec["error"] = f"{type(ex).__name__}: {ex}"
                errors += 1
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out.flush()
            print(f"  {i}/{len(work)} {pid} "
                  f"stations={len(rec.get('stations', []))} "
                  f"{'ERROR ' + rec['error'][:80] if 'error' in rec else ''}")
    print(f"\ntotal stations={n_st} events={n_ev} errors={errors} -> {out_path.name}")


if __name__ == "__main__":
    main()
