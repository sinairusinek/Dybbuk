"""Report Ruthie's 8 split requests from the troupe-tags review sheet.

She flagged these as "several troupes, not one". They are NOT applied: a split
mints new db_ids and rewrites linked_cluster_ids, and per
feedback_core_db_link_authority there is no single authority for those links —
so a split gets confirmed by a human first.

Two structurally different kinds, and they need different handling:

TYPE A — catalogue bundles (6): db1044, db1076, db1418, db1419, db1420, db1484
    The NAME is already plural: "די טרופּעס פֿון X, Y און Z". Each of these
    clusters has exactly ONE mention, so there is nothing to partition — the
    row is a Zylbercweig index heading that bundles N impresarios, never an
    organization. Fix = replace 1 row with N rows, each inheriting that single
    mention (the actor did perform with all N).
    This is the same diagnosis auto_stamp_splits.is_namelist() already encodes.

TYPE B — one reused name, several referents (2): db774, db888
    Genuinely one name covering distinct companies. db888 Ruthie enumerated
    into 5 groups and they map 1:1 onto `_ - heading` (the host person entry)
    with zero leftovers and zero ambiguity — 20/20 mentions assigned.
    db774 she left as phases without assigning mentions ("the Astrakhan
    company is the impresario one"), so it stays open.

Usage: python3 report_ruthie_splits.py
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
SPLITS = HERE / "ruthie_troupe_splits.tsv"
CORE_DB = HERE / "core_db.tsv"
CLUSTERED = HERE / "organizations_clustered.tsv"
TAGS = HERE / "troupe_tags.tsv"

COL_HEAD = "_ - heading"
COL_ORG = "clustered organization"
COL_SENT = "_ - organizations - _ - relations - _ - original_sentence"
COL_SETTLE = "_ - organizations - _ - locations - _ - settlement"

# db888: Ruthie's 5 groups, cued on the host person entry (_ - heading).
DB888_GROUPS = {
    1: ("Kaminski 1910 organization / Muranower / Prilutski circle",
        ["קאַמינסק", "מוראַנאָווער", "פרילוצקי", "ליבּערט", "ליפּאָווסקאַ",
         "נאַטאַן", "קרויזע", "סיעראָצקי", "בראַנדעסקאָ", "מיגדאָלסקי",
         "ליטעראַרישע", "קאָן"]),
    2: ("Esther Perlman (2 mentions) + probably Fischer", ["פּערלמאַן", "פֿישער"]),
    3: ("Titelman / Regina Lashkovska (Lodz)", ["טיטעלמאַן", "לאַשקאָווסקאַ"]),
    4: ("Reuven Vandorf — 'Farieynikte trupe in Lite'", ["ווענדאָרף"]),
    5: ("Bertha Kornfeld (Paris)", ["קאָרנפעלד"]),
}


def load(p: Path) -> list[dict]:
    with p.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def main() -> None:
    core = {r["db_id"]: r for r in load(CORE_DB)}
    tags = {r["db_id"]: r for r in load(TAGS)}
    clustered = load(CLUSTERED)
    by_cid: dict[str, list[dict]] = {}
    for r in clustered:
        by_cid.setdefault(r.get("cluster_id", "").strip(), []).append(r)

    print("=" * 78)
    print("TYPE A — catalogue bundles: 1 row -> N orgs, N mentions to inherit = 1")
    print("=" * 78)
    total_new = 0
    for s in load(SPLITS):
        db, cid = s["db_id"], s["cluster_id"]
        parts = s["part_names_yiddish"].split("|")
        assert len(parts) == int(s["n_parts"]), f"db{db} part count mismatch"
        mentions = by_cid.get(cid, [])
        row = core.get(db, {})
        print(f"\ndb{db}  [{cid}]  {len(mentions)} mention(s)  -> {len(parts)} orgs")
        print(f"  now:      {row.get('name_yiddish','')!r}")
        print(f"  org_type: {row.get('org_type','')!r}   tags: {tags.get(db,{}).get('tags','')!r}")
        print(f"  basis:    {s['basis']}")
        print(f"  Ruthie:   {s['ruthie_comment']}")
        for m in mentions:
            print(f"  source:   ({m.get(COL_HEAD,'')}) {m.get(COL_SENT,'')}")
        for i, p in enumerate(parts, 1):
            print(f"    part {i}: {p}")
        total_new += len(parts)

    print("\n" + "=" * 78)
    print("TYPE B — one name, several referents")
    print("=" * 78)

    print("\ndb888  [ORG-C01898]  פֿאַראייניקטע טרופּע  -> 5 entities")
    print("  Ruthie enumerated the groups; they map onto the host person entry.")
    ments = by_cid.get("ORG-C01898", [])
    assigned: dict[int, list[str]] = {g: [] for g in DB888_GROUPS}
    unassigned = []
    for m in ments:
        head = m.get(COL_HEAD, "")
        hits = [g for g, (_, cues) in DB888_GROUPS.items() if any(c in head for c in cues)]
        if len(hits) == 1:
            assigned[hits[0]].append(head)
        else:
            unassigned.append((head, hits))
    for g, (label, _) in DB888_GROUPS.items():
        print(f"    group {g}: {label}")
        for h in assigned[g]:
            print(f"        - {h}")
    n_ok = sum(len(v) for v in assigned.values())
    print(f"  coverage: {n_ok}/{len(ments)} mentions assigned, "
          f"{len(unassigned)} ambiguous")
    if unassigned:
        for h, hits in unassigned:
            print(f"    ?? {h!r} -> groups {hits}")
    print("  CAVEAT: she wrote 'כנראה גם פישער' (probably also Fischer), but the")
    print("  entry text names Herman Fischer as prompter in the KAMINSKI founding")
    print("  troupe (group 1). Her group-2 placement is a judgement call, not a")
    print("  reading of the text — confirm before minting.")

    print("\ndb774  [ORG-C04063 | ORG-C04918 | ORG-C06406]  ברייטמאַןס טרופּע")
    print("  Ruthie: 'not one uniform company; it had several phases with")
    print("  different characteristics. The Astrakhan company is the impresario one.'")
    print("  She named PHASES but did not assign mentions to them, and the 3 linked")
    print("  clusters already carry separate decisions:")
    for cid in ("ORG-C04063", "ORG-C04918", "ORG-C06406"):
        ms = by_cid.get(cid, [])
        for m in ms:
            print(f"    {cid}: {m.get(COL_ORG,'')!r} settle={m.get(COL_SETTLE,'')!r}")
            print(f"       ({m.get(COL_HEAD,'')}) {m.get(COL_SENT,'')[:120]}")
    print("  -> The 3 clusters ALREADY look like her phases: Astrakhan (the")
    print("     impresario one she names), Paul Breitman's brother's Romania")
    print("     tour, and a Warsaw children's troupe. So db774 may not need a")
    print("     split so much as an UNMERGE: C04918/C06406 are arguably not")
    print("     'Breitmans trupe' at all. Confirm with her which phases are")
    print("     separate orgs vs. the same company over time. OPEN.")

    print("\n" + "=" * 78)
    print(f"SUMMARY: Type A = 6 rows -> {total_new} orgs (+{total_new - 6} net)")
    print("         Type B = db888 ready pending Fischer check; db774 open")
    print("Nothing written. Splits need human confirmation "
          "(feedback_core_db_link_authority).")


if __name__ == "__main__":
    main()
