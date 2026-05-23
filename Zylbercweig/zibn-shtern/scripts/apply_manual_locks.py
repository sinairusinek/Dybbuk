#!/usr/bin/env python3
"""Re-apply the manual per-attestation QID locks after a clean regeneration.

`auto_reclassify.py` regenerates `places_unified_corrected.csv` from the raw
extraction, re-deriving every QID from scratch. 122 attestations carry
hand-verified / blessed QIDs that live ONLY as data and cannot be re-derived by
the corrections code. A clean regen silently regresses every one of them
(e.g. London Q16→Q92561, Kazan Q159→Q900, Simferopol Q19566→Q43421). 121 were
stamped `correction_applied='True'` by an earlier manual pass; the 122nd is
Kitayhorod, whose raw QID now reconciles to Jatinangor, Indonesia (Q1022907) —
the verified Ukrainian village is Q4222383. See the
`feedback_rebuild_corrected_nonidempotent` memory.

This step restores them from the version-controlled snapshot
`data/reference/manual_qid_locks.csv`, making the rebuild reproducible: a clean
`rebuild_corrected.py` run now diffs against main with 0 unintended QID changes.

Match key: row position (the raw extraction is order-stable, so
places_unified_corrected.csv is positionally aligned across regens). Each lock
also stores its (entry_id, source_value); the apply step ASSERTS these match at
the recorded row_index and aborts loudly on any drift rather than mislocking.

Locks run BEFORE the translit and Kimatch-review apply steps, so newer human
review decisions (Kimatch) still override an old lock when they touch the same
row; the lock only restores the manual baseline that nothing else corrects.

NOTE: NY (→Q60) and Crimea (→Q7835) manual decisions are NOT in this file — they
are reproduced systematically by QID_OVERRIDES / CATEGORY_OVERRIDES in
corrections.py. Run with system python (no zibn_shtern import needed).
"""
import csv
import sys
from pathlib import Path

csv.field_size_limit(10**7)

_ROOT = Path(__file__).resolve().parent.parent
UNIFIED = _ROOT / "data" / "working" / "places_unified_corrected.csv"
LOCKS = _ROOT / "data" / "reference" / "manual_qid_locks.csv"

# Columns the lock snapshot owns and overwrites on the matched row.
SNAP_COLS = [
    "qid", "qid_source", "wikidata_label_en", "wikidata_label_yi",
    "wikidata_type", "resolved_category", "other_type", "source_role",
    "cemetery", "burial_city", "death_site", "settlement", "province",
    "country", "neighborhood", "other", "review_flags", "needs_review",
    "correction_applied",
]


def main() -> None:
    rows = list(csv.DictReader(open(UNIFIED)))
    fields = list(rows[0].keys())
    locks = list(csv.DictReader(open(LOCKS)))

    applied = 0
    for lk in locks:
        i = int(lk["row_index"])
        if i >= len(rows):
            sys.exit(f"✗ manual lock row_index {i} out of range ({len(rows)} rows)")
        row = rows[i]
        # Guard: the lock must land on the same raw attestation it was cut from.
        if row["entry_id"] != lk["entry_id"] or row["source_value"] != lk["source_value"]:
            sys.exit(
                f"✗ manual lock drift at row {i}: expected "
                f"({lk['entry_id']!r}, {lk['source_value']!r}) but found "
                f"({row['entry_id']!r}, {row['source_value']!r}). "
                "Raw extraction order changed — re-cut manual_qid_locks.csv."
            )
        for col in SNAP_COLS:
            if col in row and col in lk:
                row[col] = lk[col]
        applied += 1

    with open(UNIFIED, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"apply_manual_locks: restored {applied} manual QID lock(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
