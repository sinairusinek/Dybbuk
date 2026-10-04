#!/usr/bin/env python3
"""Split human-reviewed multi-place SPLIT clusters into one child per settlement.

Scope (agreed with Sinai 2026-10-04):
  * Only SPLIT rows recorded by a HUMAN reviewer. The 26 `auto_drafter` SPLITs
    are LLM proposals, not decisions -- they are listed for confirmation, never
    acted on. See the drafter note in project_org_matching_drafter.
  * Only rows whose settlements are genuinely distinct after normalisation.
    Yiddish spelling variants (ווין/ווינער, ניו יאָרק/ניו-יאָרק, לעמבער/לעמבערג)
    collapse to one place and are NOT a reason to split.

The other SPLITs are not place-splits at all: plural NAMES ("the universities of
Konigsberg AND ...", "Ben Banu's troupeS") and source-level partitions. Those
need the underlying passages and are left untouched.

Each parent yields one child per distinct settlement:
    <cluster_id>_P01, _P02, ...
with decision cleared (back to undecided, as asked), the settlement pinned, and
provenance recorded. The parent is retired to decision=SPLIT_DONE so it leaves
the queue without losing the audit trail.

Idempotent: a parent already carrying SPLIT_DONE, or whose children exist, is
skipped.
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from place_norm import cluster_places          # noqa: E402

csv.field_size_limit(10**9)

HERE = pathlib.Path(__file__).resolve().parent
REVIEW = HERE / "org_alignment_review.tsv"
FORM = HERE / "maty_split_approval_2026-10-04.tsv"

REVIEWER = "split_by_place_2026_10_04"
# Tokens that are not settlements and must never become a child on their own.
NOT_A_PLACE = {"פּראָווינץ", "פראווינץ", "פּראָווינצן", "אַמעריקע", "אמעריקע"}


def settlements(row: dict) -> list[str]:
    return [p.strip() for p in (row.get("extracted_settlements") or "").split("|") if p.strip()]


def main(apply: bool) -> int:
    with open(REVIEW, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    existing = {(r.get("cluster_id") or "").strip() for r in rows}
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    plan, skipped_auto, not_multi, children = [], 0, 0, []
    for row in rows:
        if (row.get("decision") or "").strip() != "SPLIT":
            continue
        if (row.get("reviewer") or "").strip() == "auto_drafter":
            skipped_auto += 1
            continue
        groups = cluster_places(settlements(row))
        places = [p for p in groups if p not in NOT_A_PLACE]
        if len(places) < 2:
            not_multi += 1
            continue
        plan.append((row, groups, places))

    for row, groups, places in plan:
        cid = (row.get("cluster_id") or "").strip()
        if f"{cid}_P01" in existing:
            continue
        for i, place in enumerate(sorted(places), 1):
            child = {h: "" for h in headers}
            child.update({
                "cluster_id": f"{cid}_P{i:02d}",
                "canonical_yiddish": (row.get("canonical_yiddish") or "").strip(),
                "org_type": (row.get("org_type") or "").strip(),
                "name_variants": (row.get("name_variants") or "").strip(),
                "extracted_settlements": " | ".join(groups[place]),
                "candidate_db_ids": (row.get("candidate_db_ids") or "").strip(),
                "cluster_size": "",          # recount belongs to the pipeline
                "decision": "",              # back to undecided, per Sinai
                "reviewer_notes": (f"Place-split of {cid} ({place}) — "
                                   f"team rule 2026-10-04. Parent note: "
                                   f"{(row.get('reviewer_notes') or '').strip()[:160]}"),
            })
            children.append(child)
        if apply:
            row["decision"] = "SPLIT_DONE"
            row["reviewer"] = REVIEWER
            row["reviewed_at"] = stamp
            row["reviewer_notes"] = (f"Split by place into {len(places)} children "
                                     f"({', '.join(sorted(places))}). "
                                     f"{(row.get('reviewer_notes') or '').strip()}")[:480]

    print(f"human SPLIT rows that are genuinely multi-place : {len(plan)}")
    print(f"  children to create                            : {len(children)}")
    print(f"skipped, auto_drafter proposals                 : {skipped_auto}")
    print(f"skipped, not multi-place after normalisation    : {not_multi}")

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

    cols = ["q", "parent_cluster_id", "child_cluster_id", "name_yiddish", "org_type",
            "settlement", "parent_size", "candidate_db_ids",
            "decision", "target_db_id", "suggested_name", "note"]
    with open(FORM, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for q, ch in enumerate(children, 1):
            parent = ch["cluster_id"].rsplit("_P", 1)[0]
            src = next(r for r, _, _ in plan if (r.get("cluster_id") or "").strip() == parent)
            w.writerow({
                "q": q,
                "parent_cluster_id": parent,
                "child_cluster_id": ch["cluster_id"],
                "name_yiddish": ch["canonical_yiddish"],
                "org_type": ch["org_type"],
                "settlement": ch["extracted_settlements"],
                "parent_size": src.get("cluster_size", ""),
                "candidate_db_ids": ch["candidate_db_ids"],
                "decision": "", "target_db_id": "", "suggested_name": "", "note": "",
            })
    print(f"\nwrote {len(children)} children to {REVIEW.name}")
    print(f"wrote approval form -> {FORM.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main(apply="--apply" in sys.argv))
