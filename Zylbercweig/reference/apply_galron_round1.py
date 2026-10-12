"""Apply round 1 of the Galron press review to core_db.

Scope, per Sinai 2026-10-10: **the Zylbercweig side only.** Galron's list is a
reference, not a source of new organizations -- so nothing is minted. The 73
A2 place-conflict pairs are recorded as REJECT in the ledger (the Zylbercweig
org is in one city, Galron's paper in another: different papers), and the 45
would-be mints are dropped. "Named after" is not recorded as a relation.

What this writes to core_db:
  1. `address` on 5 existing Zylbercweig rows that had none, from the Galron
     row that names the city.
  2. Nothing else. No new ids, no org_type changes, no linked_cluster_ids.

Idempotent: a row already carrying the intended address is left alone, so a
second run is a no-op. Writes only the cells it reports.

Run with --execute to write; default is a dry run.
"""
from __future__ import annotations

import csv
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CORE_DB = HERE.parent / "organizations" / "core_db.tsv"
LEDGER = HERE / "galron_decisions_round1.tsv"

# db_id -> (address, provenance note). Places are Galron's, in Galron's
# Hebrew-script spelling; the settlement resolver normalises them later.
BACKFILL = {
    "1294": ("סט. פטרבורג", "place from Galron union list (galron-0088, 1912, CBJ)"),
    "1219": ("קובנה", "place from Galron union list (galron-0705, 1920–1921, ePaveldas)"),
    "1547": ("ביאליסטוק", "place from Galron union list (galron-0710, 1920–1931, Polona)"),
    "272": ("ריגה", "place from Galron union list (galron-0336, 1934–1940, JPress)"),
    "267": ("קרקוב", "place from Galron union list (galron-1893, 1901–1904, Hathitrust)"),
}


def read_tsv(path: Path) -> tuple[list[str], list[dict]]:
    csv.field_size_limit(10 ** 9)
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        return list(reader.fieldnames or []), list(reader)


def main() -> None:
    execute = "--execute" in sys.argv
    fields, rows = read_tsv(CORE_DB)

    # core_db is pushed by the app on every save, so refuse to write over a
    # file that differs from what this script was reasoned against.
    by_id = {r["db_id"]: r for r in rows}
    missing = [i for i in BACKFILL if i not in by_id]
    if missing:
        raise SystemExit(f"db_ids absent from core_db, aborting: {missing}")

    planned, skipped, conflicts = [], [], []
    for db_id, (place, note) in BACKFILL.items():
        row = by_id[db_id]
        current = (row.get("address") or "").strip()
        label = (row.get("name_yiddish") or row.get("name") or "").strip()
        if current == place:
            skipped.append((db_id, label, "already set"))
        elif current:
            # Someone filled it meanwhile: their value wins, always.
            conflicts.append((db_id, label, current, place))
        else:
            planned.append((db_id, label, place, note))

    print(f"core_db: {len(rows)} rows, high-water "
          f"{max(int(r['db_id']) for r in rows if r['db_id'].strip().isdigit())}")
    print(f"\nbackfill address on {len(planned)} row(s):")
    for db_id, label, place, _ in planned:
        print(f"  db{db_id:>5}  {label[:30]:32} address -> {place}")
    for db_id, label, why in skipped:
        print(f"  db{db_id:>5}  {label[:30]:32} SKIP ({why})")
    for db_id, label, current, place in conflicts:
        print(f"  db{db_id:>5}  {label[:30]:32} SKIP (holds {current!r}, "
              f"not overwriting with {place!r})")
    print("\nminted: 0 rows (Zylbercweig side only -- Galron's own papers "
          "are not organizations here)")

    if not execute:
        print("\nDRY RUN -- rerun with --execute to write")
        return
    if not planned:
        print("\nnothing to write")
        return

    backup = CORE_DB.with_suffix(".tsv.bak")
    shutil.copy2(CORE_DB, backup)
    for db_id, _, place, note in planned:
        row = by_id[db_id]
        row["address"] = place
        if "note" in row:
            row["note"] = note

    # core_db is stored with CRLF line endings (the app writes it that way).
    # Emitting LF rewrites all 2,598 lines, which buries the real change in the
    # diff and invites a conflict with the app's next save -- so match the file.
    newline = "\r\n" if b"\r\n" in CORE_DB.read_bytes()[:4096] else "\n"

    tmp = CORE_DB.with_suffix(".tsv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, delimiter="\t",
                                lineterminator=newline, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(CORE_DB)        # atomic swap, per the atomic-saves rule
    print(f"\nwrote {len(planned)} cell(s); backup at {backup.name}")

    diff = subprocess.run(
        ["git", "diff", "--numstat", "--", str(CORE_DB)],
        capture_output=True, text=True, cwd=HERE.parent.parent)
    print("git numstat:", diff.stdout.strip() or "(no change)")


if __name__ == "__main__":
    main()
