#!/usr/bin/env python3
"""Mark undecided gymnasium/yeshiva clusters GENERIC when the name is town-only.

Same test as unmint_generic_schools.individuates, applied to the review queue
rather than core_db: a gymnasium or yeshiva named only by its town, or by a
descriptive adjective, names a kind. A personal name, a dedication, a quoted
title or a famous institution keeps it in the queue.
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from unmint_generic_schools import sm, GYM, YESH, individuates   # noqa: E402

csv.field_size_limit(10**9)

HERE = pathlib.Path(__file__).resolve().parent
REVIEW = HERE / "org_alignment_review.tsv"
REVIEWER = "rule_2026_10_04"


def kind_of(name: str) -> str | None:
    b = sm(name)
    if any(sm(p) in b for p in YESH):
        return "yeshiva"
    if any(sm(p) in b for p in GYM):
        return "gymnasium"
    return None


def main(apply: bool) -> int:
    with open(REVIEW, newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        headers, rows = list(rd.fieldnames or []), list(rd)

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    closed, kept = [], []
    for row in rows:
        if (row.get("decision") or "").strip():
            continue
        name = (row.get("canonical_yiddish") or "").strip()
        kind = kind_of(name)
        if not kind:
            continue
        # individuates() reads core_db field names; feed it the cluster name
        probe = {"name": name, "name_yiddish": name, "name_variants": ""}
        if individuates(probe):
            kept.append((name, kind))
            continue
        closed.append((name, kind))
        if apply:
            row["decision"] = "GENERIC"
            row["aligned_db_id"] = ""
            row["reviewer"] = REVIEWER
            row["reviewed_at"] = stamp
            row["reviewer_notes"] = (
                f"Generic rule 2026-10-04 — {kind} named only by its town or a "
                f"descriptive adjective; names a kind, not an institution. "
                + (row.get("reviewer_notes") or ""))[:480]

    print(f"undecided {('yeshiva/gymnasium')} rows : {len(closed) + len(kept)}")
    print(f"  -> GENERIC : {len(closed)}")
    for n, k in closed:
        print(f"       {n[:46]}")
    print(f"  kept       : {len(kept)}")
    for n, k in kept[:10]:
        print(f"       {n[:46]}")

    if not apply:
        print("\n[dry run] nothing written. Re-run with --apply")
        return 0

    tmp = REVIEW.with_suffix(".tsv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=headers, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(REVIEW)
    print("\nwritten.")
    return 0


if __name__ == "__main__":
    sys.exit(main(apply="--apply" in sys.argv))
