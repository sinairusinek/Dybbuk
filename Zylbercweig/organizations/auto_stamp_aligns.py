"""Auto-stamp decision=ALIGN for high-confidence single-candidate alignments.

For each Undecided cluster whose top DB candidate is an exact name match
(score=1.000, method=exact), stamp decision=ALIGN with reviewer="auto_aligner"
provided the alignment respects two safety rules:

  1. The DB row is unclaimed (linked_cluster_ids empty) OR already linked to
     this exact cluster_id. Avoids hijacking a DB row already mapped elsewhere.

  2. No sibling _Q## sub-cluster shares the same top candidate. After the QID
     exploder, sibling _Q clusters are distinct location-bound entities
     (different cities) — if multiple siblings point to the same DB row, the
     match is suspect (likely the DB row is a generic name and the cluster
     siblings should each become their own NEW entity, not share one).

Rows that pass both gates are stamped ALIGN automatically. Failures are
reported but NOT stamped — they go to the human queue.

Usage:
    .venv/bin/python3 Zylbercweig/organizations/auto_stamp_aligns.py [--apply] [--limit N]

Default is dry-run; pass --apply to write.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
ALIGN = HERE / "org_alignment_review.tsv"
CORE_DB = HERE / "core_db.tsv"

QID_RE = re.compile(r"_Q\d{2}$")


def _base_cluster_id(cid: str) -> str:
    return QID_RE.sub("", cid)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Write changes back to org_alignment_review.tsv")
    ap.add_argument("--limit", type=int, default=25, help="Preview at most N stamped rows in dry-run")
    args = ap.parse_args()

    if not ALIGN.exists():
        raise FileNotFoundError(f"Missing: {ALIGN}")
    if not CORE_DB.exists():
        raise FileNotFoundError(f"Missing: {CORE_DB}")

    with ALIGN.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    with CORE_DB.open(newline="", encoding="utf-8") as f:
        db = {r["db_id"]: r for r in csv.DictReader(f, delimiter="\t")}

    # First pass: collect candidates and pre-compute which DB rows are claimed
    # by multiple _Q siblings.
    sibling_claims: dict[str, set[str]] = defaultdict(set)  # db_id -> set of base cluster_ids
    candidates: list[dict] = []
    for r in rows:
        if r.get("decision", "").strip():
            continue
        cids = [x.strip() for x in (r.get("candidate_db_ids", "") or "").split("|") if x.strip()]
        scores = [s.strip() for s in (r.get("candidate_scores", "") or "").split("|") if s.strip()]
        methods = [m.strip() for m in (r.get("candidate_methods", "") or "").split("|") if m.strip()]
        if not cids or not scores or not methods:
            continue
        try:
            top_score = float(scores[0])
        except ValueError:
            continue
        if methods[0] != "exact" or top_score < 0.999:
            continue
        cid = r["cluster_id"].strip()
        top_db = cids[0]
        sibling_claims[top_db].add(_base_cluster_id(cid))
        candidates.append({"row": r, "cid": cid, "top_db": top_db, "score": top_score})

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stamped: list[tuple[str, str, str, str]] = []
    skipped_claimed: list[tuple[str, str, str]] = []
    skipped_sibling: list[tuple[str, str, str]] = []

    for c in candidates:
        r = c["row"]
        cid = c["cid"]
        top_db = c["top_db"]
        db_row = db.get(top_db, {})
        linked_raw = db_row.get("linked_cluster_ids", "")
        linked = [x.strip() for x in linked_raw.split("|") if x.strip()]

        # Safety 1: DB row must be unclaimed or already linked to this cluster.
        if linked and cid not in linked:
            skipped_claimed.append((cid, top_db, " | ".join(linked)))
            continue

        # Safety 2: no other _Q sibling claiming the same DB row.
        # (For non-_Q clusters this collapses to no-op since base is unique.)
        if QID_RE.search(cid):
            base = _base_cluster_id(cid)
            # Check whether any sibling _Q (with same base, different cid) also
            # has this DB row as its top candidate.
            sibling_pool = [
                cc for cc in candidates
                if cc["cid"] != cid
                and cc["top_db"] == top_db
                and _base_cluster_id(cc["cid"]) == base
            ]
            if sibling_pool:
                others = ", ".join(x["cid"] for x in sibling_pool)
                skipped_sibling.append((cid, top_db, others))
                continue

        # Stamp.
        r["decision"] = "ALIGN"
        r["aligned_db_id"] = top_db
        prev_notes = r.get("reviewer_notes", "").strip()
        r["reviewer_notes"] = (
            f"[auto_aligner] exact name match, score=1.000"
            + (f". Prior notes: {prev_notes}" if prev_notes else "")
        )
        r["reviewer"] = "auto_aligner"
        r["reviewed_at"] = now
        stamped.append((cid, r.get("canonical_yiddish", "").strip(), top_db, db_row.get("name", "")))

    print(f"Auto-stamp candidates (exact, score=1.000): {len(candidates)}")
    print(f"  Stamped: {len(stamped)}")
    print(f"  Skipped (DB row already linked elsewhere): {len(skipped_claimed)}")
    print(f"  Skipped (sibling _Q claims same DB row): {len(skipped_sibling)}")
    print()
    print("Sample stamped:")
    for cid, cname, dbid, dbname in stamped[: args.limit]:
        print(f"  {cid} '{cname}' -> {dbid} '{dbname}'")
    if skipped_claimed:
        print("\nSkipped (claimed):")
        for cid, dbid, linked in skipped_claimed[:10]:
            print(f"  {cid} -> {dbid}  (already linked to: {linked})")
    if skipped_sibling:
        print("\nSkipped (sibling conflict):")
        for cid, dbid, others in skipped_sibling[:10]:
            print(f"  {cid} -> {dbid}  (also claimed by: {others})")

    if not args.apply:
        print("\n(dry-run) re-run with --apply to write.")
        return

    # Also update DB row's linked_cluster_ids to include the newly-stamped cluster.
    db_dirty = False
    for cid, _, top_db, _ in stamped:
        db_row = db.get(top_db)
        if db_row is None:
            continue
        linked = [x.strip() for x in db_row.get("linked_cluster_ids", "").split("|") if x.strip()]
        if cid not in linked:
            linked.append(cid)
            db_row["linked_cluster_ids"] = " | ".join(linked)
            db_dirty = True

    with ALIGN.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nWrote {len(stamped)} ALIGN stamps to {ALIGN}")

    if db_dirty:
        with CORE_DB.open(newline="", encoding="utf-8") as f:
            db_headers = list(csv.DictReader(f, delimiter="\t").fieldnames or [])
        with CORE_DB.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=db_headers, delimiter="\t")
            writer.writeheader()
            writer.writerows(db.values())
        print(f"Updated linked_cluster_ids in {CORE_DB}")


if __name__ == "__main__":
    main()
