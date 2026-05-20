#!/usr/bin/env python3
"""Second-pass cluster exploder: split non-itinerant clusters whose member
rows resolve to ≥2 distinct Wikidata QIDs (settlement → country fallback).

Complements pre_explode_clusters.py (which used string similarity). This pass
catches cases where two place strings refer to the *same* place (handled by
settlement_overlay.resolve_anyplace) and the row-level locations are actually
distinct cities.

Scope filter: only clusters whose decision in org_alignment_review.tsv is
empty or "SPLIT" are eligible. Reviewed clusters (ALIGN/NEW/MERGE/etc.) are
left alone — surfaced separately by audit_decided_multi_place.py.

Rows with no resolvable QID (empty or unresolved location) are merged into
the largest QID sub-cluster, preserving them rather than dropping them.

Dry-run by default; pass --apply to write.
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import shutil
import sys
from collections import Counter, defaultdict

_HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from settlement_overlay import resolve_anyplace  # noqa: E402
from pre_explode_clusters import (  # noqa: E402
    COL_CLUSTER_ID, COL_CLUSTER_SIZE, COL_CANONICAL, COL_AUTO,
    COL_ORG_TYPE, COL_SETTLEMENT, COL_COUNTRY,
    is_itinerant, best_name, canonical_for_group,
)

csv.field_size_limit(sys.maxsize)

SRC = _HERE / "organizations_clustered.tsv"
BACKUP = _HERE / "organizations_clustered_pre_explode_qid_backup.tsv"
LOG = _HERE / "qid_explode_log.tsv"
ALIGN = _HERE / "org_alignment_review.tsv"


def _row_qid(row: dict[str, str]) -> str | None:
    for col in (COL_SETTLEMENT, COL_COUNTRY):
        v = (row.get(col) or "").strip()
        if not v:
            continue
        hit = resolve_anyplace(v)
        if hit:
            return hit.qid
    return None


def _eligible_clusters() -> set[str]:
    """Cluster IDs whose decision is empty or 'SPLIT'."""
    if not ALIGN.exists():
        print(f"WARNING: {ALIGN} missing; treating all clusters as eligible.",
              file=sys.stderr)
        return set()  # empty = no filter
    eligible: set[str] = set()
    with ALIGN.open(newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            dec = (r.get("decision") or "").strip().upper()
            if dec in ("", "SPLIT"):
                cid = (r.get("cluster_id") or "").strip()
                if cid:
                    eligible.add(cid)
    return eligible


def _split_components(cluster_rows: list[dict[str, str]]) -> list[list[int]] | None:
    """Return list of index-lists per sub-cluster, or None if no split needed."""
    qids = [_row_qid(r) for r in cluster_rows]
    distinct = {q for q in qids if q}
    if len(distinct) < 2:
        return None

    by_qid: dict[str, list[int]] = defaultdict(list)
    for i, q in enumerate(qids):
        if q is not None:
            by_qid[q].append(i)

    # Stable order: by member count desc, then by QID for determinism.
    order = sorted(by_qid.keys(), key=lambda q: (-len(by_qid[q]), q))
    largest = order[0]

    # Rows with no resolved QID attach to the largest sub-cluster.
    for i, q in enumerate(qids):
        if q is None:
            by_qid[largest].append(i)

    return [by_qid[q] for q in order]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Write changes to organizations_clustered.tsv "
                         "(otherwise dry-run).")
    ap.add_argument("--limit", type=int, default=10,
                    help="Print at most N split clusters in dry-run preview.")
    args = ap.parse_args()

    if not SRC.exists():
        raise FileNotFoundError(f"Missing file: {SRC}")

    eligible = _eligible_clusters()
    print(f"Eligible (undecided / SPLIT) clusters: {len(eligible)}")

    with SRC.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    by_cluster: dict[str, list[int]] = defaultdict(list)
    for i, row in enumerate(rows):
        cid = (row.get(COL_CLUSTER_ID) or "").strip()
        if cid:
            by_cluster[cid].append(i)

    changed_assignments: list[dict[str, str]] = []
    split_count = 0
    skipped_itinerant = 0
    skipped_decided = 0
    printed = 0

    for cid, idxs in by_cluster.items():
        if len(idxs) < 2:
            continue
        if cid not in eligible:
            skipped_decided += 1
            continue
        cluster_rows = [rows[i] for i in idxs]
        if all(is_itinerant(r.get(COL_ORG_TYPE, "")) for r in cluster_rows):
            skipped_itinerant += 1
            continue

        comps = _split_components(cluster_rows)
        if not comps:
            continue

        split_count += 1
        if printed < args.limit:
            print(f"\n{cid}  ({len(cluster_rows)} rows → {len(comps)} sub-clusters)")
            for k, comp in enumerate(comps, start=1):
                qid_counts = Counter(_row_qid(cluster_rows[j]) for j in comp)
                sample_name = best_name(cluster_rows[comp[0]])
                print(f"  _S{k:02d}  n={len(comp)}  qids={dict(qid_counts)}  {sample_name}")
            printed += 1

        for comp_idx, comp in enumerate(comps, start=1):
            new_cid = f"{cid}_Q{comp_idx:02d}"
            group_rows = [cluster_rows[j] for j in comp]
            canonical = canonical_for_group(group_rows)
            csize = str(len(group_rows))
            for local_j in comp:
                global_i = idxs[local_j]
                old_cid = rows[global_i].get(COL_CLUSTER_ID, "")
                rows[global_i][COL_CLUSTER_ID] = new_cid
                rows[global_i][COL_CLUSTER_SIZE] = csize
                rows[global_i][COL_CANONICAL] = canonical
                rows[global_i][COL_AUTO] = "qid_exploded"
                changed_assignments.append({
                    "old_cluster_id": old_cid,
                    "new_cluster_id": new_cid,
                    "name": best_name(rows[global_i]),
                    "org_type": rows[global_i].get(COL_ORG_TYPE, ""),
                    "settlement": rows[global_i].get(COL_SETTLEMENT, ""),
                    "country": rows[global_i].get(COL_COUNTRY, ""),
                    "qid": _row_qid(rows[global_i]) or "",
                })

    print("\n" + "─" * 70)
    print(f"Clusters split            : {split_count}")
    print(f"Rows reassigned           : {len(changed_assignments)}")
    print(f"Skipped (decided)         : {skipped_decided}")
    print(f"Skipped (all itinerant)   : {skipped_itinerant}")

    if not args.apply:
        print("\n(dry run — re-run with --apply to write changes)")
        return

    shutil.copy2(SRC, BACKUP)
    print(f"Backup → {BACKUP.name}")

    with SRC.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {SRC.name}")

    with LOG.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["old_cluster_id", "new_cluster_id", "name",
                        "org_type", "settlement", "country", "qid"],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(changed_assignments)
    print(f"Log    → {LOG.name}")


if __name__ == "__main__":
    main()
