"""Reconcile the bio layer's died_in / buried_in EDGES against D–G's death /
burial EVENTS, which record the same facts in a richer shape.

READ-ONLY. It writes one report and changes nothing else: the point is to see,
before any migration, how cleanly the two sides line up and where they disagree.
Step 9c cannot be specified until this is known.

The two sides:

  * `Zylbercweig/plays/kg/edges.tsv` — died_in / buried_in, built by
    plays/kg_bio.py out of the 2026-03 vitals. One edge = person → place,
    with a date; no other detail.
  * `Zylbercweig/extraction_v2/defg/events.tsv` — event_type death / burial
    from the D–G full-text pass, each with a date, a place, and participants
    in `event_participants.tsv`.

Join key: the Lexicon entry. The KG edge carries it as `provenance_person_id`,
the event as `entry_id`; both are the same `P-<vol>-facs_<n>_<...>` id.

Per entry and type (death, burial) the pairing falls into:

  BOTH_AGREE     edge and event, same place (and the dates do not contradict)
  BOTH_DIFFER    edge and event, but they disagree on the place or the date
                 — these need a human before anything is retired
  EDGE_ONLY      the vitals know it, D–G did not extract it
  EVENT_ONLY     D–G found it, the vitals did not have it

    python3.11 reconcile_vitals_events.py            # from extraction_v2/
    python3.11 reconcile_vitals_events.py --tsv      # also write the row-level file
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
EDGES = REPO / "Zylbercweig" / "plays" / "kg" / "edges.tsv"
NODES = REPO / "Zylbercweig" / "plays" / "kg" / "nodes.tsv"
EVENTS = HERE / "defg" / "events.tsv"
PARTS = HERE / "defg" / "event_participants.tsv"
OUT = HERE / "qa" / "vitals_events_reconciliation.tsv"

PAIRS = [("died_in", "death"), ("buried_in", "burial")]

QID: dict[str, str] = {}   # Yiddish surface -> QID, filled in main()


def rows(p: Path) -> list[dict]:
    with p.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def year(s: str) -> str | None:
    """The year out of a date, however the two sides spell it."""
    m = re.search(r"\b(1[6-9]\d\d|20[0-2]\d)\b", s or "")
    return m.group(1) if m else None


# Yiddish points (nikud) and the two sides' different spellings make a raw
# string comparison useless: וואַרשע vs ווארשע and ווין vs וויען are the same
# city written two ways, not a disagreement about where someone died.
_POINTS = re.compile(r"[֑-ׇ]")
_FINALS = str.maketrans("ךםןףץ", "כמנפצ")


def norm_place(s: str) -> str:
    """Fold a place string to something comparable across the two sides."""
    s = (s or "").strip().lower()
    s = _POINTS.sub("", s)                    # strip nikud
    s = re.sub(r"\(.*?\)", " ", s)            # drop qualifiers
    s = re.sub(r"[^\w֐-׿]+", " ", s)  # punctuation, keep Hebrew
    s = s.translate(_FINALS)                  # final letters -> medial
    return " ".join(s.split())


# A burial edge often names the CEMETERY where the event names the CITY. That
# is a difference of granularity, not a conflict. Matched AFTER norm_place has
# stripped the nikud and the punctuation, so "בית-עולם" and "בית עולם" both
# reduce to "בית עולם"; the transliterated forms are listed too.
# NB these are matched against a norm_place() result, which has already mapped
# the final letters to their medial forms — so "עולם" must be written "עולמ"
# here, or the pattern can never fire. (It silently didn't, at first.)
_CEMETERY = re.compile(
    "|".join(w.translate(_FINALS) for w in [
        r"בית\s*עולם", r"בית\s*קברות", r"בעיס\s*עולם", "סעמעטער", "צעמעטער",
        r"הר\s*הכרמל", "פרידהאף", "פרידהויף", "cemetery", "friedhof"]))


def place_qids() -> dict[str, str]:
    """Yiddish (or Latin) surface -> QID, from every reviewed source we have.

    The D–G decisions file alone is not enough: it covers only the names that
    run left UNRESOLVED, so common cities (ווין, בוקאַרעסט, בראָדי) are absent
    from it and every spelling variant of them reads as a conflict. The
    zibn-shtern attestations carry the reviewed links for the rest.
    """
    out: dict[str, str] = {}

    att = (REPO / "Zylbercweig" / "zibn-shtern" / "data" / "working"
           / "toponyms_attestations.csv")
    if att.exists():
        with att.open(encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh):          # comma-delimited, not tab
                q = (r.get("qid") or "").strip()
                if not q.startswith("Q"):
                    continue
                for field in ("source_value", "label_yi", "label_en"):
                    k = norm_place(r.get(field, ""))
                    if k:
                        out.setdefault(k, q)

    dec = HERE / "qa" / "kima" / "defg_place_decisions_2026_11_22.tsv"
    if dec.exists():
        for r in rows(dec):                        # the D–G residue decisions
            q = (r.get("qid") or "").strip()
            for field in ("yiddish", "english"):
                k = norm_place(r.get(field, ""))
                if q.startswith("Q") and k:
                    out.setdefault(k, q)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tsv", action="store_true",
                    help="also write the row-level report")
    args = ap.parse_args()

    node = {n["node_id"]: n for n in rows(NODES)}
    global QID
    QID = place_qids()
    edges = [e for e in rows(EDGES)
             if e["edge_type"] in {t for t, _ in PAIRS}]
    events = [v for v in rows(EVENTS)
              if v["event_type"] in {t for _, t in PAIRS}]

    # participants, so the report can say what an event holds that an edge cannot
    parts = defaultdict(list)
    for p in rows(PARTS):
        parts[p["ekey"]].append(p)

    # index both sides by (entry, kind)
    by_edge = defaultdict(list)
    for e in edges:
        key = e["provenance_person_id"].strip()
        if key:
            kind = dict(PAIRS)[e["edge_type"]]
            by_edge[(key, kind)].append(e)

    by_event = defaultdict(list)
    for v in events:
        key = v["entry_id"].strip()
        if key:
            by_event[(key, v["event_type"])].append(v)

    # A disagreement is not one kind of thing. Sorting them means a reviewer
    # can take the easy classes in bulk and spend their attention on the rest.
    _HOSP = re.compile(r"שפּיטאָל|שפיטאל|האָספּיטאַל|האספיטאל|סאַנאַטאָרי|קליניק|hospital")
    _QUAL = re.compile(r"גאַליצי|גאליצי|מערב|מזרח|דאָרף|נעבן|פוילן|רוסלאַנד|אַמעריקע")

    def classify(edge_place: str, event_place: str, why: str) -> str:
        if why == "date":
            return "date only"
        if not event_place.strip():
            return "event has no place"
        if _HOSP.search(edge_place) or _HOSP.search(event_place):
            return "institution vs city"
        if _QUAL.search(event_place) and not _QUAL.search(edge_place):
            return "event adds a qualifier or region"
        return "different place"

    report, verdicts, classes = [], Counter(), Counter()
    for key in sorted(set(by_edge) | set(by_event)):
        entry, kind = key
        es, vs = by_edge.get(key, []), by_event.get(key, [])

        if es and vs:
            # Compare by QID where both sides have one — the strings are
            # spelled differently on each side and matching them directly
            # manufactures conflicts that are not there.
            e_qids = {node.get(e["target_id"], {}).get("ext_ref_id", "")
                      for e in es}
            e_qids = {q for q in e_qids if q.startswith("Q")}
            e_names = {norm_place(node.get(e["target_id"], {})
                                  .get("label_yiddish", "")) or
                       norm_place(node.get(e["target_id"], {})
                                  .get("label_english", ""))
                       for e in es} - {""}
            v_names = {norm_place(v["place"]) for v in vs
                       if v["place"].strip()} - {""}
            v_qids = {QID[n] for n in v_names if n in QID}

            e_years = {year(e["date_start"]) for e in es} - {None}
            v_years = {year(v["date_start"]) for v in vs} - {None}

            # a cemetery named against a city is granularity, not conflict
            cemetery = any(_CEMETERY.search(n) for n in e_names | v_names)

            if e_qids and v_qids:
                place_ok, basis = bool(e_qids & v_qids), "qid"
            elif e_names and v_names:
                place_ok, basis = bool(e_names & v_names), "name"
            else:
                place_ok, basis = True, "one side has no place"
            if not place_ok and cemetery:
                place_ok, basis = True, "cemetery vs city"

            date_ok = (not e_years or not v_years or bool(e_years & v_years))
            verdict = "BOTH_AGREE" if (place_ok and date_ok) else "BOTH_DIFFER"
            why = (basis if verdict == "BOTH_AGREE" else ", ".join(
                x for x in [("place" if not place_ok else ""),
                            ("date" if not date_ok else "")] if x))
        elif es:
            verdict, why = "EDGE_ONLY", "D–G did not extract this event"
        else:
            verdict, why = "EVENT_ONLY", "not in the vitals"

        verdicts[(kind, verdict)] += 1
        ev = vs[0] if vs else {}
        ed = es[0] if es else {}
        ed_place = ((node.get(ed.get("target_id", ""), {}).get("label_yiddish", "")
                     or node.get(ed.get("target_id", ""), {}).get("label_english", ""))
                    if ed else "")
        klass = (classify(ed_place, ev.get("place", ""), why)
                 if verdict == "BOTH_DIFFER" else "")
        if klass:
            classes[klass] += 1
        if args.tsv:
            report.append({
                "entry_id": entry, "kind": kind, "verdict": verdict,
                "disagree_on": why, "class": klass,
                "edge_place": (node.get(ed.get("target_id", ""), {})
                               .get("label_yiddish", "")
                               or node.get(ed.get("target_id", ""), {})
                               .get("label_english", "")),
                "edge_place_id": ed.get("target_id", ""),
                "edge_date": ed.get("date_start", ""),
                "event_place": ev.get("place", ""),
                "event_date": ev.get("date_start", ""),
                "event_participants": len(parts.get(ev.get("ekey", ""), [])),
                "event_evidence": (ev.get("evidence", "") or "")[:200],
                "n_edges": len(es), "n_events": len(vs),
            })

    # ---- summary
    print(f"edges:  died_in {sum(1 for e in edges if e['edge_type']=='died_in')}"
          f"  buried_in {sum(1 for e in edges if e['edge_type']=='buried_in')}")
    print(f"events: death {sum(1 for v in events if v['event_type']=='death')}"
          f"  burial {sum(1 for v in events if v['event_type']=='burial')}")
    print()
    order = ["BOTH_AGREE", "BOTH_DIFFER", "EDGE_ONLY", "EVENT_ONLY"]
    width = max(len(v) for v in order)
    for kind in ("death", "burial"):
        tot = sum(n for (k, _), n in verdicts.items() if k == kind)
        print(f"{kind}  ({tot} entries)")
        for v in order:
            n = verdicts.get((kind, v), 0)
            pct = f"{100*n/tot:4.1f}%" if tot else "   — "
            print(f"   {v:<{width}}  {n:5}  {pct}")
        print()

    differ = sum(n for (_, v), n in verdicts.items() if v == "BOTH_DIFFER")
    edge_only = sum(n for (_, v), n in verdicts.items() if v == "EDGE_ONLY")
    print(f"needs a human before any edge is retired: {differ} disagreements")
    print(f"would be LOST if the edges were simply dropped: {edge_only} "
          f"entries D–G never extracted")
    if classes:
        print("\nwhat the disagreements are:")
        for k, n in classes.most_common():
            print(f"   {n:5}  {k}")

    if args.tsv:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        with OUT.open("w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(report[0]),
                               delimiter="\t", lineterminator="\n")
            w.writeheader()
            w.writerows(report)
        print(f"\nrow-level report: {OUT} ({len(report)} rows)")
    else:
        print("\n(--tsv writes the row-level report)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
