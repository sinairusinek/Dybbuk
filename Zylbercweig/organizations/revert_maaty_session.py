"""Re-run the Maaty 2026-06-20/21 session revert against current TSVs.

This consolidates the two ad-hoc scripts used during the session:

  - Revert every mint_solo action Maaty performed in the settlement_audit view
    (derives the cluster→db_id pairs fresh from activity_log.tsv each run, so
    it picks up any mint_solos that happened after the previous revert).
  - Revert four specific strong-revert merges (db_ids 1102, 1158, 1317, 1320),
    plus any later INTO_EXISTING merges that pointed at them.

Backups go to *.pre_maaty_revert_<TS> before any mutation. Idempotent: re-running
on already-clean TSVs is a no-op.

Usage:
    python Zylbercweig/organizations/revert_maaty_session.py [--dry-run]

NOTE: Only run AFTER any active Zalmen session has been closed — otherwise
the next autosave from that session will re-push the bad data within minutes.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import shutil
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
CORE_DB  = HERE / "core_db.tsv"
ALIGN    = HERE / "org_alignment_review.tsv"
ACTIVITY = HERE / "activity_log.tsv"

REVIEWER = "Maaty"
STRONG_REVERT_DBS = {"1102", "1158", "1317", "1320"}


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


def derive_mint_pairs() -> list[tuple[str, str]]:
    """Parse activity_log for all of Maaty's settlement_audit/mint_solo actions.
    Returns [(cluster_id, db_id), ...].
    """
    if not ACTIVITY.exists():
        return []
    pairs: list[tuple[str, str]] = []
    with ACTIVITY.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r.get("reviewer") != REVIEWER:
                continue
            if r.get("view") != "settlement_audit" or r.get("action") != "mint_solo":
                continue
            cid = (r.get("target_id") or "").strip()
            note = r.get("note", "") or ""
            # note format: "Created DB X from N cluster(s)"
            if "Created DB " not in note:
                continue
            db = note.split("Created DB ")[1].split(" ")[0]
            if cid and db:
                pairs.append((cid, db))
    return pairs


def main(dry_run: bool = False) -> None:
    mint_pairs = derive_mint_pairs()
    mint_dbs = {db for _, db in mint_pairs}
    mint_cids = {c for c, _ in mint_pairs}
    print(f"derived from activity_log: {len(mint_pairs)} Maaty mint_solo actions "
          f"({len(mint_dbs)} unique db_ids, {len(mint_cids)} unique clusters)")
    targets = mint_dbs | STRONG_REVERT_DBS
    print(f"strong-revert merge DBs: {sorted(STRONG_REVERT_DBS)}")
    print(f"total db_ids to delete if present: {len(targets)}")

    core_fields, core_rows = _load(CORE_DB)
    align_fields, align_rows = _load(ALIGN)

    db_present = {r["db_id"] for r in core_rows if (r.get("db_id") or "").strip()}
    to_delete = targets & db_present
    print(f"in current core_db.tsv: {len(to_delete)} of those are present, "
          f"{len(targets) - len(to_delete)} already absent")

    aligned_to_targets = sum(
        1 for r in align_rows if (r.get("aligned_db_id") or "").strip() in targets
    )
    print(f"alignment rows currently pointing at any target db: {aligned_to_targets}")

    if not to_delete and not aligned_to_targets:
        print("nothing to do — already clean")
        return

    if dry_run:
        print("DRY-RUN — no files written")
        for db in sorted(to_delete):
            row = next(r for r in core_rows if r["db_id"] == db)
            print(f"  would delete db {db}: {(row.get('name_yiddish') or '')[:50]}")
        return

    # Backup
    ts = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
    shutil.copy(CORE_DB, CORE_DB.with_suffix(f".tsv.pre_maaty_revert_{ts}"))
    shutil.copy(ALIGN, ALIGN.with_suffix(f".tsv.pre_maaty_revert_{ts}"))
    print(f"backups: *.pre_maaty_revert_{ts}")

    # core_db: drop target rows
    before = len(core_rows)
    core_rows = [r for r in core_rows if r["db_id"] not in targets]
    _dump(CORE_DB, core_fields, core_rows)
    print(f"core_db: {before} -> {len(core_rows)} rows (deleted {before - len(core_rows)})")

    # alignment: blank aligned_db_id (and decision=NEW) on every cluster pointing at a target
    cleared = 0
    for r in align_rows:
        if (r.get("aligned_db_id") or "").strip() in targets:
            r["aligned_db_id"] = ""
            if r.get("decision") == "NEW":
                r["decision"] = ""
            cleared += 1
    _dump(ALIGN, align_fields, align_rows)
    print(f"alignment: cleared aligned_db_id on {cleared} cluster rows")
    print("DONE")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="preview without writing")
    args = ap.parse_args()
    main(dry_run=args.dry_run)
