#!/usr/bin/env python3
"""Per-UID consolidation of a Fischer-gazetteer Kimatch run.

The Fischer gazetteer has multiple HebName spellings per place (UID). The Kimatch
engine matches each spelling independently. This script regroups by UID to:

  1. Pick a per-UID *anchor* Kima place — the kima_id supported by the most confident
     evidence among that UID's spellings (grade A_autolink first, then any assigned id).
  2. Check whether all spellings of the UID AGREE with that anchor.
  3. Record, per UID, the set of *alternative* kima_ids any sibling spelling resolved to
     (or listed in _candidates) — the future-ambiguity flag the PI asked for.
  4. Mark sibling spellings that fell to fuzzy/no_match but whose UID has an anchor as
     donatable-variant candidates for the anchor place.

Outputs (next to the input):
  - <stem>.by_uid.tsv        one row per UID: anchor, agreement, alt ids, donatable variants
  - <stem>.uid_conflicts.tsv UIDs whose confident spellings disagree on the place (need review)

Usage:
  python consolidate_fischer_by_uid.py <fischer_matched.csv>
"""
import csv, sys, os
from collections import defaultdict, Counter

GRADE_RANK = {"A_autolink": 0, "B_review": 1, "C_review": 2, "": 3}


def load(path):
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main(path):
    rows = load(path)
    by_uid = defaultdict(list)
    for r in rows:
        by_uid[r.get("UID", "")].append(r)

    stem = os.path.splitext(path)[0]
    uid_out = stem + ".by_uid.tsv"
    conflict_out = stem + ".uid_conflicts.tsv"

    uid_records = []
    conflicts = []
    for uid, group in by_uid.items():
        # confident assignments: rows that actually got a kima_id, ranked by grade
        assigned = [r for r in group if r.get("_kima_id")]
        assigned.sort(key=lambda r: GRADE_RANK.get(r.get("_grade", ""), 9))

        # anchor = most confident assigned kima_id (grade A preferred)
        anchor = assigned[0]["_kima_id"] if assigned else ""
        anchor_grade = assigned[0].get("_grade", "") if assigned else ""
        anchor_rom = assigned[0].get("_kima_name_rom", "") if assigned else ""

        # which distinct kima_ids did *assigned* spellings land on?
        assigned_ids = Counter(r["_kima_id"] for r in assigned)
        agree = len(assigned_ids) <= 1

        # union of every candidate id seen across the UID (assigned + _candidates lists)
        alt_ids = set()
        for r in group:
            if r.get("_kima_id"):
                alt_ids.add(r["_kima_id"])
            for c in (r.get("_candidates", "") or "").split("|"):
                if c.strip():
                    alt_ids.add(c.strip())
        alt_ids.discard(anchor)

        # sibling spellings with no confident match but a UID anchor exists → donatable variants
        donatable = sorted({
            r.get("HebName", "").strip()
            for r in group
            if anchor and not r.get("_kima_id") and r.get("HebName", "").strip()
        })

        spellings = sorted({r.get("HebName", "").strip() for r in group if r.get("HebName", "").strip()})
        any_flag = sorted({r.get("_flags", "") for r in group if r.get("_flags")})

        rec = {
            "UID": uid,
            "EngClean": group[0].get("EngClean", ""),
            "n_spellings": len(spellings),
            "anchor_kima_id": anchor,
            "anchor_grade": anchor_grade,
            "anchor_rom": anchor_rom,
            "all_agree": "Y" if agree else "N",
            "distinct_assigned_ids": "|".join(sorted(assigned_ids)),
            "alt_candidate_ids": "|".join(sorted(alt_ids)),
            "donatable_variants": "|".join(donatable),
            "flags": "|".join(any_flag),
            "spellings": "|".join(spellings),
        }
        uid_records.append(rec)
        if not agree:
            conflicts.append(rec)

    cols = ["UID", "EngClean", "n_spellings", "anchor_kima_id", "anchor_grade",
            "anchor_rom", "all_agree", "distinct_assigned_ids", "alt_candidate_ids",
            "donatable_variants", "flags", "spellings"]
    for out, recs in ((uid_out, uid_records), (conflict_out, conflicts)):
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
            w.writeheader()
            w.writerows(recs)

    n = len(uid_records)
    anchored = sum(1 for r in uid_records if r["anchor_kima_id"])
    print(f"UIDs: {n}  | anchored: {anchored}  | conflicts (disagree): {len(conflicts)}")
    print(f"  donatable-variant UIDs: {sum(1 for r in uid_records if r['donatable_variants'])}")
    print(f"  UIDs with alt candidates (ambiguity flag): {sum(1 for r in uid_records if r['alt_candidate_ids'])}")
    print(f"wrote {uid_out}")
    print(f"wrote {conflict_out}")


if __name__ == "__main__":
    main(sys.argv[1])
