"""Apply Ruthie's non-tag follow-ups from the troupe-tags review sheet.

Her sheet's `comment` column carries dispositions that tagging cannot express:
org_type corrections, OCR name fixes, and split requests. The tags themselves
were ingested by ingest_troupe_tag_sheet.py (2026-09-15, c9d9f61ec); this is
the counterpart for everything she wrote as prose.

Scope here is deliberately the SAFE half — single-field edits to core_db.tsv:
  org_type      SET  (db1577, db1591, db959, db1704)
  name          FIX  (db843 — OCR אַרופּע -> טרופּע)
  name_variants ADD  (db390, db1474 — aliases she supplied; purely additive)

NOT applied here (they need Ruthie or Sinai first):
  - the 8 split requests -> ruthie_troupe_splits.tsv + report_ruthie_splits.py
  - db583 / db1478 name REVIEW -> she flagged an OCR error without giving the
    correct reading; guessing a Yiddish surname is exactly the
    OCR-vs-legitimate-variant trap, so these are reported, never written.
  - db1005 Q56466308 -> that QID is the Teatro Marconi VENUE, not the troupe.
    Belongs on the address/venue record; writing it onto the org would assert
    the troupe *is* the theatre.

Backup to core_db.tsv.pre_ruthie_followups_<TS>. Dry-run by default.

Usage: python3 apply_ruthie_troupe_followups.py [--apply]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import shutil
import sys
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
FOLLOWUPS = HERE / "ruthie_troupe_followups.tsv"

# Canonical org_type spellings per CLASSIFICATION_POLICY.md.
CANON_TYPES = {"Theatre", "Traveling Company", "Military", "Amateur", "Kleinkunst"}


def _load(p: Path) -> tuple[list[str], list[dict]]:
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd)


def _dump(p: Path, fields: list[str], rows: list[dict]) -> None:
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def main(apply: bool) -> None:
    _, fu = _load(FOLLOWUPS)
    fields, rows = _load(CORE_DB)
    by_id = {r["db_id"].strip(): r for r in rows}

    planned: list[tuple[str, str, str, str]] = []   # db_id, field, old, new
    deferred: list[tuple[str, str]] = []

    for f in fu:
        db = f["db_id"].strip()
        kind, action, val = f["kind"].strip(), f["action"].strip(), f["value"].strip()
        row = by_id.get(db)
        if row is None:
            deferred.append((db, "NOT IN core_db — skipped"))
            continue

        if action in {"REVIEW", "KEEP", "NOTE"}:
            deferred.append((db, f"{action} {kind}: {f['rationale']}"))
            continue

        if kind == "org_type" and action == "SET":
            if val not in CANON_TYPES:
                deferred.append((db, f"non-canonical org_type {val!r} — refused"))
                continue
            old = row["org_type"]
            if old.strip() == val:
                deferred.append((db, f"org_type already {val!r} — no-op"))
                continue
            planned.append((db, "org_type", old, val))

        elif kind == "name_variants" and action == "ADD":
            cur = [v.strip() for v in (row.get("name_variants") or "").split("|") if v.strip()]
            if val in cur:
                deferred.append((db, f"variant {val!r} already present — no-op"))
                continue
            planned.append((db, "name_variants", row.get("name_variants", ""),
                             " | ".join(cur + [val])))

        elif kind == "name" and action == "FIX":
            old = row["name_yiddish"]
            if old.strip() == val:
                deferred.append((db, "name already correct — no-op"))
                continue
            planned.append((db, "name_yiddish", old, val))
        else:
            deferred.append((db, f"unhandled {kind}/{action}"))

    print(f"PLANNED core_db edits: {len(planned)}")
    for db, fld, old, new in planned:
        print(f"  db{db:>5}  {fld:<13} {old!r}")
        print(f"  {'':>7}  {'':<13} -> {new!r}")

    print(f"\nDEFERRED / reported only: {len(deferred)}")
    for db, why in deferred:
        print(f"  db{db:>5}  {why}")

    if not apply:
        print("\n(dry-run; pass --apply to write)")
        return
    if not planned:
        print("\nnothing to write")
        return

    ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    bak = CORE_DB.with_suffix(f".tsv.pre_ruthie_followups_{ts}")
    shutil.copy2(CORE_DB, bak)

    for db, fld, _old, new in planned:
        by_id[db][fld] = new
        # name_yiddish is the OCR-corrected form; keep display `name` in sync
        # only when it was byte-identical to the old Yiddish (db843's case).
        if fld == "name_yiddish" and by_id[db].get("name", "").strip() == _old.strip():
            by_id[db]["name"] = new

    _dump(CORE_DB, fields, rows)
    print(f"\nbackup: {bak.name}")
    print(f"wrote {len(planned)} edits to {CORE_DB.name}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(ap.parse_args().apply)
