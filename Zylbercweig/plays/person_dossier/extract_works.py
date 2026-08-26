#!/usr/bin/env python3.11
"""Works & genre pass for a person dossier (optional module: runs only when the
config sets has_works).

One Gemini call over the subject's entry, then three deterministic checks:
  (a) hallucination guard — a title is kept only if it occurs verbatim in the
      entry text (points-stripped); we also record whether it sits inside a
      „..." quoted span, which is how the Leksikon marks titles;
  (b) KG join — against the ego's composed_music edges and the music facts in
      kg_facts_linked.tsv, flagging both directions of mismatch;
  (c) a nikud-tolerant genre regex sanity pass over the evidence quote.

Deliberately EXCLUDED: eval/eval_reference_works.tsv's genre column, which is
held-out evaluation data — joining against it here would contaminate the eval.

Usage: GOOGLE_API_KEY=... python3.11 extract_works.py --config rumshinsky.json
"""
import argparse, csv, json, os, re, sys, unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENTRY_TEXTS = HERE.parent.parent / "people" / "entry_texts.tsv"
KG = HERE.parent / "kg"
FACTS = HERE.parent / "kg_facts_linked.tsv"
DEFAULT_MODEL = "gemini-3-flash-preview"

csv.field_size_limit(10**8)
POINTS = re.compile(r"[֑-ׇ]")
QUOTES = "„“”\"'»«"

# genre vocabulary of the Leksikon, unvocalized bases
GENRE_BASES = {
    "אפערעטע": "operetta", "אפערע": "opera", "קאמעדיע": "comedy",
    "דראמע": "drama", "לעבנסבילד": "scene from life", "מעלאדראמע": "melodrama",
    "פארס": "farce", "טראגעדיע": "tragedy", "מוזיקאלישע קאמעדיע": "musical comedy",
    "אפערעטע-קאמעדיע": "operetta-comedy", "פאלקסאפערעטע": "folk operetta",
    "ראמאן": "romance", "בילד": "tableau", "סקיצע": "sketch", "רעוויו": "revue",
    "מיוזיקל": "musical", "ליד": "song", "קאנטאטע": "cantata", "סימפאניע": "symphony",
    "מארש": "march", "וואלס": "waltz", "ניגון": "nign (melody)", "פאנטאזיע": "fantasia",
}

SYSTEM_PROMPT = """You read one entry of the Yiddish "Leksikon fun yidishn teater"
(Zylbercweig) about a COMPOSER and extract the WORKS the entry credits to him,
with their genre as the entry itself names it.

Return ONLY: {"works": [...]}. For each work:

{
  "title": "<the work's title EXACTLY as written in the entry, in Yiddish,
            WITHOUT the surrounding quotation marks>",
  "genre_raw": "<the Yiddish genre word the entry uses for it (אָפּערעטע,
                קאָמעדיע, לעבנסבילד, אָפּערע, דראַמע, ליד ...), verbatim; '' if
                the entry gives none>",
  "genre_norm": "<a short English normalization of genre_raw: operetta, opera,
                 comedy, drama, melodrama, farce, scene from life, song,
                 musical number set, revue, cantata, other; '' if unknown>",
  "year": "YYYY" | "",
  "role": "<what he did: music | some numbers | libretto | conductor | other>",
  "collaborators": ["<other people named for this work, verbatim Yiddish>"],
  "venue": "<theatre where it was staged, verbatim, or ''>",
  "evidence_quote": "<short VERBATIM Yiddish snippet from the entry that names
                     this title>"
}

Rules:
- ONLY works actually named in the entry. Never infer a title from general
  knowledge. If you are not certain a string is a title, leave it out.
- The title must be copied character-for-character from the entry.
- Include works he wrote music for even when someone else wrote the text.
- Do NOT include: theatres, organizations, newspapers, books ABOUT him,
  or the names of people.
- The evidence_quote must be a literal substring of the entry.
"""


def sp(s):
    return POINTS.sub("", unicodedata.normalize("NFC", s or ""))


# Hebrew presentation ligatures: KG labels and entry text disagree on these
LIGATURES = {"\u05f0": "\u05d5\u05d5", "\u05f1": "\u05d5\u05d9", "\u05f2": "\u05d9\u05d9",
             "\ufb1f": "\u05d9\u05d9", "\ufb4f": "\u05d0\u05dc"}


def loose(s):
    """points-stripped, quote/whitespace-normalized, for substring checks"""
    s = sp(s)
    for q in QUOTES:
        s = s.replace(q, "")
    for lig, exp in LIGATURES.items():
        s = s.replace(lig, exp)
    s = s.replace("־", "-").replace("–", "-")
    return re.sub(r"\s+", " ", s).strip()


def key(s):
    """loose() plus hyphen/space equivalence — for title identity only."""
    return re.sub(r"[\s-]+", " ", loose(s)).strip()


def nikud_tolerant(base):
    return "".join(re.escape(c) + r"[֑-ׇ]*" for c in base)


def load_entry(pid):
    with open(ENTRY_TEXTS, encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            if row["person_id"] == pid:
                return row
    sys.exit(f"entry {pid} not found")


def kg_titles(cfg):
    """(title -> {source}) for the ego's composed_music edges and music facts."""
    ego, pid = cfg["node_id"], cfg["entry_person_id"]
    nodes = {r["node_id"]: r for r in
             csv.DictReader(open(KG / "nodes.tsv", encoding="utf-8"), delimiter="\t")}
    out = {}
    for e in csv.DictReader(open(KG / "edges.tsv", encoding="utf-8"), delimiter="\t"):
        if e["source_id"] == ego and e["edge_type"] == "composed_music":
            n = nodes.get(e["target_id"], {})
            t = key(n.get("label_yiddish") or n.get("label_english") or "")
            out.setdefault(t, {"title": n.get("label_yiddish", ""), "srcs": set(),
                               "ids": set(), "year": e.get("date_start", ""), "ev": set()})
            out[t]["srcs"].add("composed_music edge")
            out[t]["ids"].add(e["target_id"])
            if e.get("evidence_sentence"):
                out[t]["ev"].add(loose(e["evidence_sentence"]))
    for r in csv.DictReader(open(FACTS, encoding="utf-8"), delimiter="\t"):
        if r["person_id"] != pid:
            continue
        if r["fact_type"] not in ("music", "mention_only") or r["person_surface"] != "[HOST]":
            continue
        t = key(r["play_title_surface"])
        if not t:
            continue
        out.setdefault(t, {"title": r["play_title_surface"], "srcs": set(),
                           "ids": set(), "year": r.get("date_start", ""), "ev": set()})
        out[t]["srcs"].add(f"kg_facts_linked ({r['fact_type']})")
        if r.get("play_link"):
            out[t]["ids"].add(r["play_link"])
        if r.get("evidence_quote"):
            out[t]["ev"].add(loose(r["evidence_quote"]))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--redo", action="store_true")
    args = ap.parse_args()
    cfg = json.loads((HERE / args.config).read_text(encoding="utf-8"))
    if not cfg.get("has_works"):
        print(f"{cfg['slug']}: has_works is false — works module skipped")
        return
    out_path = HERE / f"{cfg['slug']}_works.json"

    entry = load_entry(cfg["entry_person_id"])
    text = entry["entry_text"]
    hay = loose(text)

    if out_path.exists() and not args.redo:
        works = json.loads(out_path.read_text(encoding="utf-8"))["works_raw"]
        print(f"reusing {len(works)} drafted works from {out_path.name} (--redo to re-call)")
    else:
        api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
        if not api_key:
            sys.exit("set GOOGLE_API_KEY or GEMINI_API_KEY in env")
        os.environ["GOOGLE_API_KEY"] = api_key
        from google import genai
        from google.genai import types
        client = genai.Client()
        resp = client.models.generate_content(
            model=args.model,
            contents=(f"Entry heading: {entry['heading']}\n"
                      f"Entry id: {cfg['entry_person_id']} (volume {entry['volume']})\n\n"
                      f"Entry text:\n{text}"),
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT, max_output_tokens=65536,
                temperature=0.0, response_mime_type="application/json",
                thinking_config=types.ThinkingConfig(thinking_budget=0)))
        try:
            fr = str(resp.candidates[0].finish_reason)
        except Exception:
            fr = ""
        if fr and "STOP" not in fr:
            print(f"WARNING finish_reason={fr} — output may be truncated")
        data = json.loads((resp.text or "").strip())
        works = data.get("works", data if isinstance(data, list) else [])
        print(f"model returned {len(works)} works")

    # ---- (a) hallucination guard ----
    kept, rejected = [], []
    quoted_spans = set()
    for m in re.finditer(r"[„\"']([^„“”\"']{2,80})[“”\"']", text):
        quoted_spans.add(loose(m.group(1)))
    for w in works:
        t = loose(w.get("title", ""))
        ev = loose(w.get("evidence_quote", ""))
        if not t:
            continue
        w["_key"] = key(w.get("title", ""))
        w["in_text"] = t in hay
        w["in_quotes"] = t in quoted_spans
        w["evidence_in_text"] = bool(ev) and ev in hay
        (kept if w["in_text"] else rejected).append(w)

    # ---- (c) genre regex sanity pass ----
    grx = [(re.compile(nikud_tolerant(b)), b, en) for b, en in GENRE_BASES.items()]
    for w in kept:
        blob = sp(w.get("evidence_quote", "") + " " + w.get("genre_raw", ""))
        found = [en for rx, b, en in grx if rx.search(blob)]
        w["genre_regex"] = found
        gr = loose(w.get("genre_raw", ""))
        w["genre_raw_attested"] = bool(gr) and gr in hay
        w["genre_agrees"] = (not w.get("genre_norm")) or (not found) or \
            any(w["genre_norm"].lower() in f or f in w["genre_norm"].lower() for f in found)

    # ---- (b) KG join ----
    kg = kg_titles(cfg)
    for w in kept:
        w["kg_sources"], w["kg_ids"] = [], []
        w["kg_match_via"], w["kg_title"], w["kg_status"] = "", "", "works_only"

    def attach(w, hit, how):
        w["kg_sources"] = sorted(hit["srcs"])
        w["kg_ids"] = sorted(hit["ids"])
        w["kg_match_via"], w["kg_title"], w["kg_status"] = how, hit["title"], "in_kg"
        hit["seen"] = True

    # pass 1 — title identity (exact, then containment either way)
    for w in kept:
        hit = kg.get(w["_key"]) or next(
            (v for k, v in kg.items() if k and (k in w["_key"] or w["_key"] in k)), None)
        if hit:
            attach(w, hit, "title")

    # pass 2 — the KG often carries a play's CANONICAL title where the entry
    # names an alternate one ("קלוגע פרויען" for חכמת נשים). Fall back to the
    # evidence sentence, which is the same text in both layers — but claim at
    # most ONE work per KG fact, since one sentence can name several titles.
    for kt, hit in kg.items():
        if hit.get("seen"):
            continue
        cands = [w for w in kept
                 if w["kg_status"] == "works_only"
                 and w["_key"] and any(w["_key"] in e for e in hit["ev"])]
        if not cands:
            continue
        year = re.match(r"(\d{4})", hit.get("year") or "")
        cands.sort(key=lambda w: (
            -(bool(year) and w.get("year", "")[:4] == year.group(1)),
            -len(w["_key"])))
        attach(cands[0], hit, "evidence")

    kg_only = [{"title": v["title"], "sources": sorted(v["srcs"]),
                "ids": sorted(v["ids"]), "year": v.get("year", "")}
               for v in kg.values() if not v.get("seen")]

    out = {
        "slug": cfg["slug"], "person_id": cfg["entry_person_id"],
        "model": args.model,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "works_raw": works, "works": kept, "rejected": rejected, "kg_only": kg_only,
        "genre_vocab": GENRE_BASES,
    }
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"kept {len(kept)} / rejected {len(rejected)} (title not found verbatim in entry)")
    for w in rejected:
        print(f"   REJECT  {w.get('title','')}")
    print("in quoted span:", sum(1 for w in kept if w["in_quotes"]), "/", len(kept))
    print("evidence quote verbatim:", sum(1 for w in kept if w["evidence_in_text"]), "/", len(kept))
    print("genres:", dict(Counter(w.get("genre_norm") or "—" for w in kept).most_common()))
    dis = [w["title"] for w in kept if not w["genre_agrees"]]
    print("genre_norm disagreeing with the regex pass:", dis or "none")
    via = Counter(w["kg_match_via"] for w in kept if w["kg_status"] == "in_kg")
    print("KG match route:", dict(via))
    print(f"KG join: in_kg {sum(1 for w in kept if w['kg_status']=='in_kg')}, "
          f"works_only {sum(1 for w in kept if w['kg_status']=='works_only')}, "
          f"kg_only {len(kg_only)}")
    for k in kg_only:
        print(f"   KG-ONLY  {k['title']}  {k['sources']}")
    print("wrote", out_path.name)


if __name__ == "__main__":
    main()
