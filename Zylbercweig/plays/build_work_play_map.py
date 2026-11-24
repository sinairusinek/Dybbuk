"""Build `work_play_map.tsv`: the persisted catalogue-work -> plays_db join.

Why this is a file and not a function
-------------------------------------
`kg_crosslink.work_to_play()` used to recompute the join on every call, keyed on
`eid <N>` scraped out of plays_db's free-text `notes` with a normalised-title
fallback. That was fragile twice over:

  * only 51 of 273 plays_db rows carry an `eid` in `notes`, so 220 of the 271
    resolved joins rested on the title;
  * where two catalogue works share one Yiddish title, the fallback picked one
    play and hid the other. Both such pairs are a real Lateiner/Hurwitz
    distinction the catalogue draws and plays_db does not (expr 3868/3960
    "דוד בן ישי", expr 3925/4061 "יהודית און האָלאָפֿערנעס").

So the join is materialised here with the `basis` recorded per row, and the
ambiguous cases are marked `AMBIGUOUS` instead of being resolved silently.
`work_to_play()` reads this file and returns '' for them, so nothing downstream
claims a join that was never made.

Statuses
--------
  OK         one catalogue work <-> one play. `basis` = eid_in_notes | title_author
  AMBIGUOUS  several catalogue works map to this one play (see `basis`)
  UNMAPPED   plays_db has no play for this work

Run:  python3.11 build_work_play_map.py        (from Zylbercweig/plays/)
      python3.11 build_work_play_map.py --check   verify the file is current
"""
from __future__ import annotations

import collections
import csv
import re
import sys

import plays_common as pc

HEADER = ["expression_id", "play_id", "status", "basis"]
OUT_TSV = pc.HERE / "work_play_map.tsv"


def build() -> list[tuple[str, str, str, str]]:
    import kg_crosslink as kc

    plays = {p["play_id"]: p for p in pc.load_plays_db()}
    works = {w["id"]: w for w in kc.load_entity_works()}

    # The legacy matcher, kept HERE only, as the proposer. It is deliberately
    # not used at read time any more.
    by_eid: dict[str, str] = {}
    by_title: dict[str, set[str]] = collections.defaultdict(set)
    for p in plays.values():
        m = re.search(r"\beid (\d+)", p.get("notes", ""))
        if m:
            by_eid[m.group(1)] = p["play_id"]
        by_title[pc.norm_yiddish(p["title_yiddish"])].add(p["play_id"])

    proposal: dict[str, str] = {}
    basis: dict[str, str] = {}
    for wid, w in works.items():
        eid = str(w.get("expression_id") or "")
        pid = by_eid.get(eid, "")
        how = "eid_in_notes" if pid else ""
        if not pid:
            cands = by_title.get(pc.norm_yiddish(w.get("yiddish_title") or ""), set())
            if len(cands) > 1:
                aid = kc.PLAYWRIGHT_DB_ID.get(w.get("playwright", ""))
                cands = {c for c in cands
                         if plays[c]["author_db_id"] == aid} or cands
            if len(cands) == 1:
                pid, how = next(iter(cands)), "title_author"
        proposal[wid] = pid
        basis[wid] = how

    claimed = collections.defaultdict(list)
    for wid, pid in proposal.items():
        if pid:
            claimed[pid].append(wid)

    rows = []
    for wid, w in works.items():
        eid = str(w.get("expression_id") or "")
        pid = proposal[wid]
        if not pid:
            rows.append((eid, "", "UNMAPPED",
                         "no play in plays_db with this title"))
            continue
        peers = [x for x in claimed[pid] if x != wid]
        if peers:
            exprs = "/".join(str(works[x].get("expression_id")) for x in peers)
            wrights = "/".join(str(works[x].get("playwright")) for x in peers)
            rows.append((eid, pid, "AMBIGUOUS",
                         f"same title as expr {exprs} (playwright {wrights}); "
                         f"plays_db has ONE play"))
        else:
            rows.append((eid, pid, "OK", basis[wid]))
    return sorted(rows, key=lambda r: int(r[0]) if r[0].isdigit() else 0)


def main() -> int:
    rows = build()
    counts = collections.Counter(r[2] for r in rows)
    bases = collections.Counter(r[3] for r in rows if r[2] == "OK")

    if "--check" in sys.argv:
        have = [tuple(r[c] for c in HEADER) for r in pc.read_tsv(OUT_TSV)]
        if have != [tuple(r) for r in rows]:
            print("work_play_map.tsv is STALE — rerun without --check")
            return 1
        print(f"work_play_map.tsv current: {dict(counts)}")
        return 0

    with open(OUT_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(HEADER)
        w.writerows(rows)
    print(f"wrote {OUT_TSV.name}: {dict(counts)}")
    print(f"  OK basis: {dict(bases)}")
    for r in rows:
        if r[2] != "OK":
            print(f"  {r[2]:9} expr {r[0]:6} {r[1] or '—':8} {r[3]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
