"""Step 8e: D–G stations vs the old itineraries (Istanbul, Danzig, France, Rumshinsky) for the
same entries (decision 3: re-run them, compare as QA). No LLM.

Places on both sides go through the itinerary resolver (build_itinerary_data.resolve: toponym
gazetteer + confirmed Kima rows + plays-KG places), so a match is by QID where both resolve,
else by letter skeleton. Per entry: old places found again, new-only places, year agreement.

  python3.11 qa/defg_itinerary_compare.py   -> qa/qa_itinerary_compare.tsv, qa/qa_itinerary_old_only.tsv
"""
import csv, re, sys, collections, unicodedata
from pathlib import Path

HERE = Path(__file__).parent
V2 = HERE.parent
Z = V2.parent
sys.path.insert(0, str(Z / "plays" / "itinerary"))
import build_itinerary_data as bid  # noqa: E402
csv.field_size_limit(10**9)

OLD = [("istanbul", Z / "plays/itinerary/stations_resolved.tsv"),
       ("danzig", Z / "plays/itinerary/danzig_stations_resolved.tsv"),
       ("france", Z / "plays/france_dossier/france_stations_resolved.tsv"),
       ("rumshinsky", Z / "plays/person_dossier/rumshinsky_stations_resolved.tsv")]


def skel(t):
    t = unicodedata.normalize("NFD", t or "")
    t = re.sub(r"[֑-ׇ]", "", t)
    t = t.split(",")[0].split("(")[0]
    return re.sub(r"[^\w]|_", "", t)


def year(s):
    m = re.match(r"(1[6-9]\d\d)", str(s or ""))
    return int(m.group(1)) if m else None


def main():
    lut = bid.load_gazetteer()
    res_cache = {}

    def key(place):
        if place not in res_cache:
            st, q, *_ = bid.resolve(place, lut)
            res_cache[place] = q if st.startswith("settlement") and str(q).startswith("Q") else ("S:" + skel(place))
        return res_cache[place]

    old = collections.defaultdict(list)
    src = {}
    for name, p in OLD:
        if not p.exists():
            continue
        for r in csv.DictReader(p.open(encoding="utf-8"), delimiter="\t"):
            if r.get("subject", "entry") != "entry" or not r.get("place"):
                continue
            k = r["qid"] if str(r.get("qid", "")).startswith("Q") else key(r["place"])
            old[r["person_id"]].append({"k": k, "place": r["place"], "y": year(r.get("date_start")) or year(r.get("t_start")),
                                        "verb": r.get("verb", ""), "evidence": r.get("evidence", "")})
            src.setdefault(r["person_id"], name)

    new = collections.defaultdict(list)
    for r in csv.DictReader((V2 / "defg/stations.tsv").open(encoding="utf-8"), delimiter="\t"):
        if "~" in r["entry_id"] or not r["subject_key"].endswith(":p0") or not r["place"]:
            continue
        new[r["entry_id"]].append({"k": key(r["place"]), "place": r["place"], "y": year(r["date_start"]),
                                   "verb": r["verb_class"]})

    anyp = collections.defaultdict(set)  # loose: every place the new pass wrote for the entry, any table
    for tab in ("stations", "events", "performance_facts"):
        for r in csv.DictReader((V2 / f"defg/{tab}.tsv").open(encoding="utf-8"), delimiter="\t"):
            if r.get("place") and "~" not in r["entry_id"]:
                anyp[r["entry_id"]].add(key(r["place"]))
            if tab == "stations" and r.get("org") and "~" not in r["entry_id"]:
                anyp[r["entry_id"]].add("O:" + skel(r["org"]))
    rows, old_only, T = [], [], collections.Counter()
    for pid in sorted(old):
        o, n = old[pid], new.get(pid, [])
        ok_, nk = {x["k"] for x in o}, {x["k"] for x in n}
        both = ok_ & nk
        oy = {x["k"]: x["y"] for x in o if x["y"]}
        ny = collections.defaultdict(set)
        for x in n:
            if x["y"]:
                ny[x["k"]].add(x["y"])
        yc = [k for k in both if k in oy and ny.get(k)]
        yagree = sum(1 for k in yc if any(abs(oy[k] - y) <= 1 for y in ny[k]))
        rows.append({"person_id": pid, "source": src[pid], "old_stations": len(o), "new_stations": len(n),
                     "old_places": len(ok_), "found_again": len(both), "new_only_places": len(nk - ok_),
                     "year_compared": len(yc), "year_agree_pm1": yagree})
        T["entries"] += 1; T["old_places"] += len(ok_); T["found"] += len(both); T["new_only"] += len(nk - ok_)
        T["yc"] += len(yc); T["ya"] += yagree
        if not n:
            T["no_new_stations"] += 1
        loose = {x["k"] for x in o if x["k"] in anyp.get(pid, set()) or ("O:" + skel(x["place"])) in anyp.get(pid, set())}
        rows[-1]["found_loose"] = len(loose | both); T["loose"] += len(loose | both)
        for x in o:
            if x["k"] not in nk and x["k"] not in loose:
                old_only.append({"person_id": pid, "source": src[pid], "place": x["place"], "key": x["k"],
                                 "year": x["y"] or "", "verb": x["verb"], "evidence": x["evidence"][:200]})
    out = HERE / "qa_itinerary_compare.tsv"
    with out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), delimiter="\t"); w.writeheader(); w.writerows(rows)
    with (HERE / "qa_itinerary_old_only.tsv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(old_only[0]), delimiter="\t"); w.writeheader(); w.writerows(old_only)
    by = collections.defaultdict(collections.Counter)
    for r in rows:
        by[r["source"]]["n"] += 1; by[r["source"]]["old"] += r["old_places"]; by[r["source"]]["found"] += r["found_again"]
        by[r["source"]]["new_only"] += r["new_only_places"]
    print(f"{T['entries']} entries; old places found again {T['found']}/{T['old_places']} = {T['found'] / T['old_places']:.1%}; "
          f"new-only places {T['new_only']}; year agrees (±1) {T['ya']}/{T['yc']} = {T['ya'] / max(1, T['yc']):.1%}; "
          f"entries with no new station {T['no_new_stations']}")
    print(f"loose (place anywhere in the entry's new stations/events/performance facts, or the old 'place' is a new org): "
          f"{T['loose']}/{T['old_places']} = {T['loose'] / T['old_places']:.1%}")
    for s, c in by.items():
        print(f"  {s:11} {c['n']:3} entries: found {c['found']}/{c['old']} = {c['found'] / max(1, c['old']):.1%}, new-only {c['new_only']}")


if __name__ == "__main__":
    main()
