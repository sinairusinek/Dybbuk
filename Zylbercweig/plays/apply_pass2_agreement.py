"""A4b — Fold pass-2 agreement into the draft confidences.

For every window present in kg_extraction_drafts_pass2.tsv, compare the two
extraction passes fact-by-fact (key: play hint/title + fact_type + person
surface + role). Facts found in both passes get notes+=pass2_agree; facts
only in pass 1 get notes+=pass2_only and their confidence is demoted one
step (high->medium, medium->low). Facts only in pass 2 are NOT added (pass 1
is the canonical set; pass 2 is a stability probe).

Usage:
    python3.11 apply_pass2_agreement.py [--execute]
"""
from __future__ import annotations

import argparse
from collections import defaultdict

import plays_common as pc

PASS2_TSV = pc.HERE / "kg_extraction_drafts_pass2.tsv"
DEMOTE = {"high": "medium", "medium": "low", "low": "low", "": "low"}


def key(r: dict) -> tuple:
    return (r.get("play_id_hint") or "|".join(pc.title_segments(r.get("play_title_surface", ""))),
            r.get("fact_type", ""), r.get("person_surface", ""), r.get("person_role", ""))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    p2 = defaultdict(set)
    p2_windows = set()
    for r in pc.read_tsv(PASS2_TSV):
        if r.get("fact_type") in ("", "none"):
            p2_windows.add(r["window_id"])
            continue
        p2_windows.add(r["window_id"])
        p2[r["window_id"]].add(key(r))

    rows = pc.read_tsv(pc.DRAFTS_TSV)
    n_agree = n_only1 = 0
    for r in rows:
        if r.get("fact_type") in ("", "none") or r["window_id"] not in p2_windows:
            continue
        if key(r) in p2[r["window_id"]]:
            n_agree += 1
            r["notes"] = (r["notes"] + "; " if r["notes"] else "") + "pass2_agree"
        else:
            n_only1 += 1
            r["confidence"] = DEMOTE.get(r.get("confidence", ""), "low")
            r["notes"] = (r["notes"] + "; " if r["notes"] else "") + "pass2_only"
    total = n_agree + n_only1
    print(f"windows probed: {len(p2_windows)}  facts compared: {total}  "
          f"agree: {n_agree} ({100 * n_agree / max(1, total):.0f}%)  demoted: {n_only1}")
    if not args.execute:
        print("dry-run — pass --execute to update kg_extraction_drafts.tsv")
        return
    pc.write_tsv(pc.DRAFTS_TSV, rows, pc.EXTRACTION_FIELDS)
    print(f"updated {pc.DRAFTS_TSV}")


if __name__ == "__main__":
    main()
