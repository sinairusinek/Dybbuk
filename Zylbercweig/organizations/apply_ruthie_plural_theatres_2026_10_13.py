"""Ruthie's answer on the two person-named plural טעאַטערס (2026-10-13).

apply_theatre_plural_generic_2026_10_13.py deliberately held these two back:
the decision vocabulary says a name is NOT generic when a person is the
institution's subject rather than its sponsor, and both could be read as "the
theatres where X played". Ruthie ruled they are generic expressions after all —
plural over a person's houses names a kind, not an individuated institution —
so neither needs to be minted as a theatre.

Append-only on the decision columns; never overwrites an existing decision.

Usage:
    python3 Zylbercweig/organizations/apply_ruthie_plural_theatres_2026_10_13.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
REVIEW_TSV = HERE / "org_alignment_review.tsv"
REVIEWER = "Ruthie"
NOTE = (
    "Generic expression (Ruthie 2026-10-13) — plural טעאַטערס over one person's "
    "houses names a kind, not an individuated theatre; not to be minted"
)

TARGETS = {
    "ORG-C01424",      # קאַמינסקאַ"-טעאַטערס
    "ORG-C02046",      # דזשעני גאָלדשטיינס טעאַטערס
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with open(REVIEW_TSV, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fields = list(reader.fieldnames or [])
        rows = list(reader)

    stamp = dt.date.today().isoformat()
    changed = 0
    skipped: list[str] = []
    seen: set[str] = set()

    for r in rows:
        cid = r["cluster_id"].strip()
        if cid not in TARGETS:
            continue
        seen.add(cid)
        cur = (r.get("decision") or "").strip()
        if cur:
            skipped.append(f"{cid} (already '{cur}')")
            continue
        r["decision"] = "GENERIC"
        r["reviewer_notes"] = (
            f"{r.get('reviewer_notes','').strip()}; {NOTE}".lstrip("; ")
        )
        r["reviewer"] = REVIEWER
        r["reviewed_at"] = stamp
        changed += 1
        print(f"  {cid}  -> GENERIC   {r['canonical_yiddish'][:50]}")

    print(f"\ntargets: {len(TARGETS)}  changed: {changed}")
    if skipped:
        print(f"  left alone (already decided): {', '.join(skipped)}")
    for missing in sorted(TARGETS - seen):
        print(f"  WARN cluster not found: {missing}")

    if args.dry_run:
        print("\n--dry-run: org_alignment_review.tsv NOT written")
        return

    with open(REVIEW_TSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator="\r\n")
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote {REVIEW_TSV}")


if __name__ == "__main__":
    main()
