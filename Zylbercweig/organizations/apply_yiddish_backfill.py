"""Apply PI-approved Yiddish-variant backfill to core_db.tsv and org_addresses_review.tsv.

Reads db_yiddish_variant_backfill.tsv. For each row where pi_decision == "APPROVE":
- writes pi_canonical_yiddish (or proposed_canonical_yiddish as fallback) into
  core_db.tsv[name_yiddish] for that db_id
- mirrors the same value into org_addresses_review.tsv[canonical_yiddish] if the
  db_id has a row there (only fills if currently empty, to avoid overwriting
  PI-edited address-card values)

After running, rerun prepare_alignment.py to refresh candidate_db_ids using the
new Yiddish handles.

Usage:
    .venv/bin/python3 Zylbercweig/organizations/apply_yiddish_backfill.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
BACKFILL_TSV = HERE / "db_yiddish_variant_backfill.tsv"
CORE_DB_TSV = HERE / "core_db.tsv"
ADDR_TSV = HERE / "org_addresses_review.tsv"


def _read_tsv(p: Path) -> tuple[list[str], list[dict[str, str]]]:
    with open(p, encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames or []), list(r)


def _write_tsv(p: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    bf_fields, bf_rows = _read_tsv(BACKFILL_TSV)
    db_fields, db_rows = _read_tsv(CORE_DB_TSV)
    if "name_yiddish" not in db_fields:
        sys.exit("core_db.tsv is missing the name_yiddish column.")
    db_by_id = {r["db_id"]: r for r in db_rows}

    addr_fields, addr_rows = _read_tsv(ADDR_TSV)
    addr_by_id = {r["db_id"]: r for r in addr_rows}

    db_updates = 0
    addr_updates = 0
    skipped = 0

    for r in bf_rows:
        if (r.get("pi_decision") or "").strip().upper() != "APPROVE":
            skipped += 1
            continue
        yid = (r.get("pi_canonical_yiddish") or "").strip() or (r.get("proposed_canonical_yiddish") or "").strip()
        if not yid:
            skipped += 1
            continue
        did = r["db_id"].strip()
        db_row = db_by_id.get(did)
        if not db_row:
            print(f"  WARN: db_id {did} not in core_db.tsv — skipping")
            skipped += 1
            continue
        if db_row.get("name_yiddish", "") != yid:
            db_row["name_yiddish"] = yid
            db_updates += 1

        addr_row = addr_by_id.get(did)
        if addr_row and not (addr_row.get("canonical_yiddish") or "").strip():
            addr_row["canonical_yiddish"] = yid
            addr_updates += 1

    print(f"Approved rows applied: db_updates={db_updates} addr_updates={addr_updates} skipped={skipped}")
    if args.dry_run:
        print("(dry-run; no files written)")
        return

    _write_tsv(CORE_DB_TSV, db_fields, db_rows)
    _write_tsv(ADDR_TSV, addr_fields, addr_rows)
    print(f"Wrote {CORE_DB_TSV} and {ADDR_TSV}.")
    print("Next: rerun prepare_alignment.py to refresh candidate_db_ids.")


if __name__ == "__main__":
    main()
