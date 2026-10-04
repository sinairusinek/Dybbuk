#!/usr/bin/env python3
"""Split every remaining SPLIT cluster that can be split from its own record.

Three bases, applied in order; a cluster is split on the first that fits.

  A. PLACE -- 2+ genuinely distinct settlements (spelling variants folded via
     place_norm). One child per town: <cid>_P01, _P02, ...
  B. ENUMERATED NAME -- the name lists its own parts: "ישיבות פֿון סלוצק און
     סלאָבאָדקע", "אוניווערזיטעטן פון ניו יאָרק און שיקאַגאָ". One child per
     named part, the part carried as the child's settlement: <cid>_N01, ...

Everything else is LEFT AS SPLIT and reported, because the split basis is not
in the record:

  C. PLURAL, UNENUMERATED -- "די טרופּעס פֿון זיגלער" (Ziegler's troupeS) says
     there were several companies but not which; "דייטשע טרופּעס" likewise.
  D. SOURCE-LEVEL -- Maaty's partitions that cite specific volume/facsimile
     passages ("two distinct contexts, v5 facs_535... and ..."). Splitting
     these needs the passages read, not the cluster row.

Inventing children for C and D would mint organizations the sources do not
attest, which is the failure mode the whole generic rule exists to prevent.

Children are born undecided, as asked. Parents retire to SPLIT_DONE so they
leave the queue with the audit trail intact. Idempotent.
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from place_norm import cluster_places        # noqa: E402

csv.field_size_limit(10**9)

HERE = pathlib.Path(__file__).resolve().parent
REVIEW = HERE / "org_alignment_review.tsv"
LEFT = HERE / "split_needs_sources_punchlist.tsv"

REVIEWER = "split_2026_10_04"
NOT_A_PLACE = {"פּראָווינץ", "פראווינץ", "אַמעריקע", "אמעריקע"}
PLURAL_MARKERS = ("טרופּעס", "טרופעס", "ישיבות", "אוניווערזיטעטן",
                  "אוניווערסיטעטן", "הייזער", "שולן", "לאָקאל")


def settlements(row: dict) -> list[str]:
    return [p.strip() for p in (row.get("extracted_settlements") or "").split("|") if p.strip()]


def enumerated_parts(name: str) -> list[str]:
    """Parts a name lists for itself: 'yeshives of A and B' -> [A, B]."""
    m = re.search(r"(?:פֿון|פון|אין)\s+(.+)$", name or "")
    if not m:
        return []
    parts = [p.strip() for p in re.split(r"\s+און\s+|\s*,\s*", m.group(1)) if p.strip()]
    return parts if len(parts) >= 2 else []


def child_row(parent: dict, headers: list[str], cid: str, place: str, why: str) -> dict:
    child = {h: "" for h in headers}
    child.update({
        "cluster_id": cid,
        "canonical_yiddish": (parent.get("canonical_yiddish") or "").strip(),
        "org_type": (parent.get("org_type") or "").strip(),
        "name_variants": (parent.get("name_variants") or "").strip(),
        "extracted_settlements": place,
        "candidate_db_ids": (parent.get("candidate_db_ids") or "").strip(),
        "cluster_size": "",
        "decision": "",
        "reviewer_notes": (f"{why} of {parent['cluster_id']} ({place}) — 2026-10-04. "
                           f"Parent note: {(parent.get('reviewer_notes') or '').strip()[:150]}"),
    })
    return child


def main(apply: bool) -> int:
    with open(REVIEW, newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        headers, rows = list(rd.fieldnames or []), list(rd)

    existing = {(r.get("cluster_id") or "").strip() for r in rows}
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    children, left = [], []
    n_place = n_enum = 0

    for row in rows:
        if (row.get("decision") or "").strip() != "SPLIT":
            continue
        cid = (row.get("cluster_id") or "").strip()
        name = (row.get("canonical_yiddish") or "").strip()

        groups = cluster_places(settlements(row))
        places = [p for p in groups if p not in NOT_A_PLACE]
        parts = enumerated_parts(name)

        if len(places) >= 2:
            kind, items, tag, why = "place", sorted(places), "P", "Place-split"
            spellings = {p: " | ".join(groups[p]) for p in items}
        elif parts:
            kind, items, tag, why = "enum", parts, "N", "Name-split"
            spellings = {p: p for p in items}
        else:
            basis = ("plural name, parts not enumerated"
                     if any(p in name for p in PLURAL_MARKERS)
                     else "source-level partition")
            left.append((row, basis))
            continue

        if f"{cid}_{tag}01" in existing:
            continue
        for i, item in enumerate(items, 1):
            children.append(child_row(row, headers, f"{cid}_{tag}{i:02d}",
                                      spellings[item], why))
        if kind == "place":
            n_place += 1
        else:
            n_enum += 1
        if apply:
            row["decision"] = "SPLIT_DONE"
            row["reviewer"] = REVIEWER
            row["reviewed_at"] = stamp
            row["reviewer_notes"] = (
                f"Split into {len(items)} children ({', '.join(items)}) 2026-10-04. "
                f"{(row.get('reviewer_notes') or '').strip()}")[:480]

    print(f"split by place            : {n_place} parents")
    print(f"split by enumerated name  : {n_enum} parents")
    print(f"children created          : {len(children)}")
    print(f"left as SPLIT (needs sources): {len(left)}")
    for _, basis in left[:1]:
        pass
    import collections
    for b, n in collections.Counter(b for _, b in left).most_common():
        print(f"    {n:3}  {b}")

    if not apply:
        print("\n[dry run] nothing written. Re-run with --apply")
        return 0

    rows.extend(children)
    tmp = REVIEW.with_suffix(".tsv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=headers, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(REVIEW)

    cols = ["cluster_id", "canonical_yiddish", "org_type", "cluster_size",
            "extracted_settlements", "reviewer", "basis", "reviewer_notes"]
    with open(LEFT, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for row, basis in left:
            w.writerow({**{c: row.get(c, "") for c in cols}, "basis": basis})
    print(f"\nwrote {len(children)} children; punchlist -> {LEFT.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main(apply="--apply" in sys.argv))
