"""Apply PI-approved empty-type backfill to core_db.tsv.

Reads db_empty_type_review.tsv. For rows with pi_decision == "APPROVE":
- writes pi_type (or suggested_type as fallback) into core_db.org_type for that db_id.

Only fills empty cells (never overwrites). After running, rerun
prepare_alignment.py to refresh block keys + candidate_db_ids.

Usage:
    .venv/bin/python3 Zylbercweig/organizations/apply_empty_type_backfill.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
REVIEW_TSV = HERE / "db_empty_type_review.tsv"
CORE_DB_TSV = HERE / "core_db.tsv"


def _read_tsv(p: Path) -> tuple[list[str], list[dict[str, str]]]:
    with open(p, encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames or []), list(r)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rv_fields, rv_rows = _read_tsv(REVIEW_TSV)
    db_fields, db_rows = _read_tsv(CORE_DB_TSV)
    db_by_id = {r["db_id"].strip(): r for r in db_rows}

    updates = 0
    skipped = 0
    for r in rv_rows:
        if (r.get("pi_decision") or "").strip().upper() != "APPROVE":
            skipped += 1
            continue
        t = (r.get("pi_type") or "").strip() or (r.get("suggested_type") or "").strip()
        if not t:
            skipped += 1
            continue
        did = r["db_id"].strip()
        db_row = db_by_id.get(did)
        if not db_row:
            print(f"  WARN: db_id {did} not in core_db.tsv — skipping")
            continue
        if (db_row.get("org_type") or "").strip():
            skipped += 1  # already set, don't overwrite
            continue
        db_row["org_type"] = t
        updates += 1

    print(f"Approved rows applied: {updates} · skipped: {skipped}")
    if args.dry_run:
        print("(dry-run; no files written)")
        return
    if not updates:
        return

    with open(CORE_DB_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=db_fields, delimiter="\t")
        w.writeheader()
        w.writerows(db_rows)
    print(f"Wrote {CORE_DB_TSV}")
    print("Next: rerun prepare_alignment.py to refresh blocking + candidate_db_ids.")


if __name__ == "__main__":
    main()
