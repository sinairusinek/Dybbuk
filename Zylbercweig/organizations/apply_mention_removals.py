#!/usr/bin/env python3
"""Fold reviewer mention-removals into organizations_clustered.tsv.

The Zalmen app records removals as an append-only overlay
(mention_removals.tsv) rather than rewriting the 34 MB clustered table on a
Cloud container — see zalmen/mention_removals.py for why. Readers apply the
overlay at load time, so the app is already correct without this script; run
it locally when you want the removals baked into the table itself, e.g.
before a reclustering pass.

Usage:
    python3 apply_mention_removals.py --dry-run     # report only (default)
    python3 apply_mention_removals.py --write       # rewrite the TSV
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "zalmen"))
csv.field_size_limit(sys.maxsize)

import mention_removals  # noqa: E402
from atomic_io import atomic_write  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
CLUSTER_FILE = HERE / "organizations_clustered.tsv"
_COL_CID = "cluster_id"


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--write", action="store_true", help="rewrite the clustered TSV")
    g.add_argument("--dry-run", action="store_true", help="report only (default)")
    args = ap.parse_args()

    removed = mention_removals.load_removed_keys()
    if not removed:
        print("No REMOVE decisions recorded — nothing to apply.")
        return 0

    with open(CLUSTER_FILE, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        headers, rows = list(r.fieldnames), list(r)

    hits = [
        row for row in rows
        if row.get(_COL_CID, "").strip() and mention_removals.mention_key(row) in removed
    ]
    print(f"{len(removed)} REMOVE decision(s); {len(hits)} matching row(s) in the table.")
    for row in hits[:20]:
        print(f"  {row.get('File','')} / {row.get('_ - xml:id','')} "
              f"[{row.get(_COL_CID,'')}] {row.get('clustered organization','')}")
    if len(hits) > 20:
        print(f"  ... and {len(hits) - 20} more")

    stale = len(removed) - len({mention_removals.mention_key(r) for r in hits})
    if stale > 0:
        print(f"note: {stale} decision(s) matched no row — already applied, or the "
              f"table was regenerated with different content.")

    if not args.write:
        print("\nDry run. Re-run with --write to rewrite the table.")
        return 0

    for row in hits:
        row[_COL_CID] = ""
    with atomic_write(CLUSTER_FILE) as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"\nWrote {CLUSTER_FILE.name}: cleared cluster_id on {len(hits)} row(s).")
    print("Commit the table, and keep mention_removals.tsv — it is the audit trail.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
