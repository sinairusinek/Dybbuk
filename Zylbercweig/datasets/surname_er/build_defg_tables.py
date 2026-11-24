"""D–G relation tables for the surname-ER pack (2026-11-24).

From the full-text extraction of every Lexicon entry (extraction_v2/defg/*.tsv), keyed so they join
to the pack: `entry_id` -> `entry_hub_id` (the person whose entry it is), person keys (`mkey`)
-> `defg_people.tsv`, and each ER mention -> the D–G person mentions in its host entry that carry
its surname (`er_mention_defg_links.tsv`). Places get `place_en` from place_linker (same as
defg_places_en). Facts stated only inside quotations are dropped. No LLM.

  python3.11 build_defg_tables.py   (after build_er_context.py)
"""
from __future__ import annotations

import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ZYL = HERE.parent.parent
D = ZYL / "extraction_v2" / "defg"
OUT = HERE / "defg"
csv.field_size_limit(10**9)
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ZYL / "people"))
from place_linker import Linker  # noqa: E402
from people_similarity import normalize_person_name  # noqa: E402


def tsv(p):
    return csv.DictReader(open(p, encoding="utf-8"), delimiter="\t")


def write(name, rows, cols):
    OUT.mkdir(exist_ok=True)
    with open(OUT / name, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, cols, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"{name}: {len(rows):,} rows")


def main():
    ent2hub = {}
    for r in tsv(HERE / "er_candidate_profiles.tsv"):
        for e in r["lexicon_entries"].split("|"):
            if e:
                ent2hub[e] = r["hub_id"]
    base = lambda e: e.split("~")[0]
    hub_of = lambda e: ent2hub.get(e, "") if "~" not in e else ""  # ~aN/~gN parts are other subjects
    L = Linker()
    cache = {}

    def place_en(place, evidence, entry):
        if not place:
            return ""
        k = (place, entry)
        if k not in cache:
            cache[k] = L.link(place, evidence, None, base(entry))[0] or "?"
        return cache[k]

    # ---- people
    people, by_entry = [], defaultdict(list)
    for r in tsv(D / "mentions.tsv"):
        if r["kind"] != "person":
            continue
        subj = r["is_subject"] == "1" and r["mkey"].endswith(":p0")
        row = {"mkey": r["mkey"], "entry_id": r["entry_id"], "entry_hub_id": hub_of(r["entry_id"]),
               "surface": r["surface"], "full_form_in_entry": r["full_form"],
               "is_entry_subject": "1" if subj else "",
               "hub_id": hub_of(r["entry_id"]) if subj else "", "flags": r["flags"]}
        people.append(row)
        by_entry[base(r["entry_id"])].append(row)
    write("defg_people.tsv", people, list(people[0]))

    # ---- events + participants
    ev = []
    for r in tsv(D / "events.tsv"):
        if r["in_quote"]:
            continue
        ev.append({**{k: r[k] for k in ("ekey", "entry_id", "event_type", "event_text", "status", "date_start",
                                         "date_end", "date_certainty", "place", "venue", "work", "language",
                                         "evidence", "flags")},
                   "entry_hub_id": hub_of(r["entry_id"]),
                   "place_en": place_en(r["place"], r["evidence"], r["entry_id"])})
    write("defg_events.tsv", ev, ["ekey", "entry_id", "entry_hub_id", "event_type", "event_text", "status",
                                  "date_start", "date_end", "date_certainty", "place", "place_en", "venue",
                                  "work", "language", "evidence", "flags"])
    keep = {e["ekey"] for e in ev}
    parts = [{**r, "entry_hub_id": hub_of(r["ekey"].split("#")[0])}
             for r in tsv(D / "event_participants.tsv") if r["ekey"] in keep]
    write("defg_event_participants.tsv", parts, ["ekey", "entry_hub_id", "person_key", "person", "role",
                                                 "character", "flags"])

    # ---- performance facts
    pf = []
    for r in tsv(D / "performance_facts.tsv"):
        if r["in_quote"]:
            continue
        pf.append({**r, "entry_hub_id": hub_of(r["entry_id"]),
                   "place_en": place_en(r["place"], r["evidence"], r["entry_id"])})
    write("defg_performance_facts.tsv", pf, ["part_id", "entry_id", "entry_hub_id", "person_key", "person",
                                             "function", "function_text", "character", "work", "org", "venue",
                                             "place", "place_en", "date_start", "date_end", "language", "status",
                                             "event_key", "evidence", "flags"])

    # ---- work relations
    wr = [{**r, "entry_hub_id": hub_of(r["entry_id"])} for r in tsv(D / "work_relations.tsv")]
    write("defg_work_relations.tsv", wr, ["part_id", "entry_id", "entry_hub_id", "from_key", "from", "to_key",
                                          "to", "type", "evidence", "flags"])

    # ---- ER mention -> D–G person mentions in the host entry with the same surname
    hub_entries = defaultdict(list)
    for e, h in ent2hub.items():
        hub_entries[h].append(e)
    links, n_m, n_hit = [], 0, 0
    for m in tsv(HERE / "er_mention_context.tsv"):
        n_m += 1
        key = m["surname_key"]
        if not key:
            continue
        hits = []
        for e in hub_entries.get(m["host_hub_id"], []):
            for p in by_entry.get(e, []):
                if p["is_entry_subject"]:
                    continue
                toks = set(normalize_person_name(p["surface"]).split()) | set(
                    normalize_person_name(p["full_form_in_entry"]).split())
                if key in toks or any(t.rstrip("ן").rstrip("ס") == key for t in toks):
                    hits.append(p)
        if hits:
            n_hit += 1
        for p in hits:
            links.append({"mention_id": m["mention_id"], "mkey": p["mkey"], "surface": p["surface"],
                          "full_form_in_entry": p["full_form_in_entry"]})
    write("er_mention_defg_links.tsv", links, ["mention_id", "mkey", "surface", "full_form_in_entry"])
    print(f"ER mentions with >=1 D–G person mention: {n_hit:,} / {n_m:,}")


if __name__ == "__main__":
    main()
