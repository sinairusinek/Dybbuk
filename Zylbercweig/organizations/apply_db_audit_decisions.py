"""Apply REMOVE decisions from db_audit_decisions.tsv surgically.

For each (db_id, cluster_id) row with decision="REMOVE":
  - blank aligned_db_id on the cluster row in org_alignment_review.tsv
    (and clear decision="NEW" if set by Maaty's mint/merge pipeline)
  - remove the cluster_id from the DB row's linked_cluster_ids field
  - if the DB has zero remaining aligned clusters AFTER this, delete the row
    from core_db.tsv (it's now an empty entity)

KEEP_IN and CHECK decisions are no-ops at the data level (still recorded).

Backups go to *.pre_audit_apply_<TS> for both TSVs before any mutation.
Use --dry-run to preview without writing.

Same surgical pattern as /tmp/revert_maaty_strong_merges.py — predictable and
re-runnable as more decisions accumulate.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import shutil
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
CORE_DB   = HERE / "core_db.tsv"
ALIGN     = HERE / "org_alignment_review.tsv"
DECISIONS = HERE / "db_audit_decisions.tsv"


def _load(p: Path) -> tuple[list[str], list[dict]]:
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd)


def _dump(p: Path, fields: list[str], rows: list[dict]) -> None:
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main(dry_run: bool = False) -> None:
    if not DECISIONS.exists():
        print(f"no decisions file at {DECISIONS} — nothing to apply")
        return

    _, dec_rows = _load(DECISIONS)
    removes_per_db: dict[str, set[str]] = defaultdict(set)
    for r in dec_rows:
        if r.get("decision", "").strip().upper() == "REMOVE":
            db = (r.get("db_id") or "").strip()
            cid = (r.get("cluster_id") or "").strip()
            if db and cid:
                removes_per_db[db].add(cid)
    n_remove = sum(len(v) for v in removes_per_db.values())

    if not removes_per_db:
        print("no REMOVE decisions to apply")
        return

    print(f"to apply: {n_remove} REMOVE decisions across {len(removes_per_db)} DBs")

    core_fields, core_rows = _load(CORE_DB)
    align_fields, align_rows = _load(ALIGN)

    core_by_id = {r["db_id"]: r for r in core_rows if (r.get("db_id") or "").strip()}

    # Count current alignments per DB
    aligned_now: dict[str, set[str]] = defaultdict(set)
    for r in align_rows:
        aid = (r.get("aligned_db_id") or "").strip()
        cid = (r.get("cluster_id") or "").strip()
        if aid and cid:
            aligned_now[aid].add(cid)

    # Plan
    dbs_to_delete: list[str] = []
    cluster_blanks = 0
    for db, removes in removes_per_db.items():
        if db not in core_by_id:
            print(f"  WARN: db {db} not in core_db — skipping")
            continue
        remaining = aligned_now[db] - removes
        cluster_blanks += len(removes & aligned_now[db])
        if not remaining:
            dbs_to_delete.append(db)

    print(f"  cluster alignments to blank: {cluster_blanks}")
    print(f"  DB rows to delete (became empty): {len(dbs_to_delete)}")

    if dry_run:
        print("DRY-RUN — no files written")
        for db in dbs_to_delete:
            print(f"    would delete db {db}: {(core_by_id[db].get('name_yiddish') or '')[:50]}")
        return

    # Backup
    ts = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
    shutil.copy(CORE_DB, CORE_DB.with_suffix(f".tsv.pre_audit_apply_{ts}"))
    shutil.copy(ALIGN, ALIGN.with_suffix(f".tsv.pre_audit_apply_{ts}"))
    print(f"backups: *.pre_audit_apply_{ts}")

    # Mutate alignment
    blanked = 0
    for r in align_rows:
        aid = (r.get("aligned_db_id") or "").strip()
        cid = (r.get("cluster_id") or "").strip()
        if aid in removes_per_db and cid in removes_per_db[aid]:
            r["aligned_db_id"] = ""
            if r.get("decision") == "NEW":
                r["decision"] = ""
            blanked += 1
    _dump(ALIGN, align_fields, align_rows)
    print(f"alignment: blanked {blanked} cluster rows")

    # Mutate core_db: remove cluster_ids from linked_cluster_ids; delete empty DBs
    kept_core: list[dict] = []
    edited = 0
    deleted = 0
    for r in core_rows:
        db = r.get("db_id", "")
        if db in dbs_to_delete:
            deleted += 1
            continue
        if db in removes_per_db:
            linked = (r.get("linked_cluster_ids") or "").strip()
            if linked:
                parts = [p.strip() for p in linked.split("|") if p.strip()
                         and p.strip() not in removes_per_db[db]]
                new_linked = " | ".join(parts)
                if new_linked != linked:
                    r["linked_cluster_ids"] = new_linked
                    edited += 1
        kept_core.append(r)
    _dump(CORE_DB, core_fields, kept_core)
    print(f"core_db: edited linked_cluster_ids on {edited} rows; deleted {deleted} empty rows")
    print("DONE")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="preview without writing")
    args = ap.parse_args()
    main(dry_run=args.dry_run)
