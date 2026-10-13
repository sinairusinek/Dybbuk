"""Plural טעאַטערס -> GENERIC on the cluster list (Sinai's rule, 2026-10-13).

GENERIC is a *decision* in org_alignment_review.tsv, not an org_type, so this
rule lands here rather than on core_db.tsv (which has no plural-טעאַטערס rows).
A plural name — "Yiddish theatres on Second Avenue", "East Side theatres" —
names a KIND of institution, never an individuated one, which is exactly the
generic rule in the decision vocabulary.

Ruthie had already closed 6 such rows GENERIC; this applies the same reading to
the 7 still-undecided Theatre ones. ORG-C03923 is excluded: it is a drama SCHOOL
at the Warsaw government theatres (type Education), an individuated institution
whose name merely contains the plural.

Also held back, for Sinai/Ruthie to decide rather than me:
  ORG-C01424  קאַמינסקאַ"-טעאַטערס        — named for the Kaminska family
  ORG-C02046  דזשעני גאָלדשטיינס טעאַטערס  — named for Jenny Goldstein
Both are plural, so the rule as given would mark them GENERIC, but the decision
vocabulary says a name is NOT generic when a person is the institution's subject
rather than its sponsor. These two are "the theatres where X played/ran" — a
plural over one person's houses, not a kind-noun. Left undecided on purpose.

Append-only on the decision columns; never overwrites an existing decision.

Usage:
    python3 Zylbercweig/organizations/apply_theatre_plural_generic_2026_10_13.py [--dry-run]
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
REVIEWER = "claude (plural-theatres rule by Sinai)"
NOTE = "Plural טעאַטערס — names a kind of theatre, not an individuated one"

TARGETS = {
    "ORG-C00267",      # יידישע טעאַטערס אויף סעקאָנד עוועניו
    "ORG-C01376",      # איסט סייד טעאַטערס
    "ORG-C01938",      # רעגירונגס-טעאַטערס
    "ORG-C06509",      # טעאַטערס טאָליאָ
    "ORG-C06906",      # יידישע טעאַטערס פון אַמעריקע
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
