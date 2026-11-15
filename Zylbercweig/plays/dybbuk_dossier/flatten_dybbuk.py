#!/usr/bin/env python3.11
"""dybbuk_drafts.jsonl -> dybbuk_facts.tsv (one row per fact; review table)
and dybbuk_data.json (feed for build_dybbuk_pages.py).

Places resolve with the itinerary resolver (toponyms gazetteer + KG place nodes);
each fact gets its Lexicon volume/columns + archive.org scan link via lexicon_ref."""
import csv, json, re, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "itinerary"))
sys.path.insert(0, str(HERE.parent))
from build_itinerary_data import load_gazetteer, resolve, norm  # noqa: E402
from lexicon_ref import ref  # noqa: E402

POINTS = re.compile(r"[֑-ׇ]")
# work codes → display order / names
WORKS = ["ansky", "lateiner", "shomer", "parody", "opera_music", "film", "other", "figurative"]


def main():
    lut = load_gazetteer()
    rows, entries = [], {}
    for line in open(HERE / "dybbuk_drafts.jsonl", encoding="utf-8"):
        r = json.loads(line)
        if "error" in r:
            print("ERROR row", r["person_id"], r["error"][:100])
            continue
        entries[r["person_id"]] = {"heading": POINTS.sub("", r["heading"]), "volume": r["volume"],
                                   "n_mentions": len(r["mentions"])}
        for mi, m in enumerate(r["mentions"]):
            facts = m.get("facts") or [{}]
            for fi, f in enumerate(facts):
                st, qid, en, lat, lon = resolve(f.get("place", ""), lut)
                if st in ("unresolved", "empty") and f.get("place_en"):
                    st2 = resolve(f["place_en"], lut)
                    if st2[0] not in ("unresolved", "empty"):
                        st, qid, en, lat, lon = st2[0] + "_en", *st2[1:]
                # tour strings list several cities: map every resolvable part
                pts = []
                if st.endswith("first_of_list") or st == "settlement_partial" or re.search(r"[,;(]| און ", f.get("place", "")):
                    for part in re.split(r"[,;()]| און |\.", f.get("place", "")):
                        p_ = resolve(part, lut)
                        if p_[0] in ("settlement", "region") and p_[1] not in {x[0] for x in pts}:
                            pts.append(p_[1:])
                if not pts and lat is not None:
                    pts = [(qid, en, lat, lon)]
                q = f.get("evidence_quote") or m.get("evidence_quote", "")
                try:
                    lr = ref(r["person_id"], q) if q else {}
                except Exception:
                    lr = {}
                rows.append({
                    "person_id": r["person_id"], "heading": POINTS.sub("", r["heading"]),
                    "volume": r["volume"], "mention": mi, "fact": fi,
                    "work": m.get("work", ""), "work_title": m.get("work_title", ""),
                    "work_author": m.get("work_author", ""), "alt_title": m.get("alt_title", ""),
                    "work_note": m.get("work_note", ""),
                    "fact_type": f.get("fact_type", ""), "language": f.get("language", ""),
                    "place": f.get("place", ""), "place_en": f.get("place_en", ""),
                    "country": f.get("country", ""), "venue": f.get("venue", ""),
                    "troupe": f.get("troupe", ""), "date_start": f.get("date_start", ""),
                    "date_end": f.get("date_end", ""), "certainty": f.get("date_certainty", ""),
                    "people": f.get("people") or [], "description": f.get("description", ""),
                    "evidence": q, "res_status": st, "qid": qid, "label_en": en,
                    "lat": lat, "lon": lon, "points": pts, "cols": lr.get("cols", ""), "scan": lr.get("url", ""),
                })
    with open(HERE / "dybbuk_facts.tsv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), delimiter="\t")
        w.writeheader()
        for x in rows:
            w.writerow({**x, "people": json.dumps(x["people"], ensure_ascii=False)})
    json.dump({"entries": entries, "facts": rows}, open(HERE / "dybbuk_data.json", "w",
              encoding="utf-8"), ensure_ascii=False)
    from collections import Counter
    print(len(entries), "entries", len(rows), "fact rows")
    print(Counter(x["work"] for x in rows))
    print(Counter(x["res_status"] for x in rows if x["place"] or x["place_en"]))
    un = Counter(norm(x["place"]) + " / " + x["place_en"] for x in rows
                 if x["res_status"] in ("unresolved",) and (x["place"] or x["place_en"]))
    print("unresolved:", un.most_common(40))


if __name__ == "__main__":
    main()
