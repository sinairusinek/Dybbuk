#!/usr/bin/env python3
"""Append stub card rows to org_addresses_review.tsv for core_db orgs that
have none, so every live DB entity shows up in the Zalmen Organization Cards
view regardless of how it was coined (app or script).

The app's own NEW path (org_review.append_address_row) does this live for
orgs created in the app; script-side coinings (umbrella children, homonym
splits, people-matcher NEW waves) historically skipped it. Run this after
any script that adds core_db rows.

Append-only and idempotent: existing rows are never touched, existing db_ids
are never duplicated. Deprecated and out_of_project core rows are skipped
(the app hides them everywhere). Stubs are enriched from
org_alignment_review.tsv: linked clusters, summed mentions, and the union of
extracted settlements, so the cards sort and filter sensibly.

Usage:
    python3.11 sync_address_cards.py --dry-run   # report only
    python3.11 sync_address_cards.py             # append missing rows
"""

import argparse
import csv
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
CORE_FILE = HERE / "core_db.tsv"
ALIGN_FILE = HERE / "org_alignment_review.tsv"
ADDR_FILE = HERE / "org_addresses_review.tsv"

SEP = " | "


def load(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader.fieldnames or []), list(reader)


def is_hidden(row):
    return (row.get("deprecated", "") or "").strip().lower() == "true" or \
           (row.get("out_of_project", "") or "").strip().lower() == "true"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    _, core_rows = load(CORE_FILE)
    _, align_rows = load(ALIGN_FILE)
    addr_headers, addr_rows = load(ADDR_FILE)
    if "db_id" not in addr_headers:
        sys.exit("org_addresses_review.tsv missing db_id column")

    addr_ids = {r["db_id"].strip() for r in addr_rows}

    # db_id -> aligned clusters (any stamped decision that points at it)
    by_db = {}
    for a in align_rows:
        db_id = (a.get("aligned_db_id", "") or "").strip()
        if db_id:
            by_db.setdefault(db_id, []).append(a)

    stubs = []
    for row in core_rows:
        db_id = row["db_id"].strip()
        if not db_id or db_id in addr_ids or is_hidden(row):
            continue
        clusters = by_db.get(db_id, [])
        settlements = []
        mentions = 0
        for c in clusters:
            mentions += int(c.get("cluster_size", "0") or "0")
            for s in (c.get("extracted_settlements", "") or "").split("|"):
                s = s.strip()
                if s and s not in settlements:
                    settlements.append(s)
        stub = {h: "" for h in addr_headers}
        stub["db_id"] = db_id
        name = (row.get("name", "") or "").strip()
        name_yid = (row.get("name_yiddish", "") or "").strip()
        label = name_yid or name
        # Homonym children often disambiguate only in the Latin name
        # ("Forverts (Chicago)" vs bare "פֿאַרווערטס") — carry the qualifier over
        # so their cards are distinguishable.
        if name_yid and "(" not in name_yid and "(" in name:
            label = f"{name_yid} {name[name.index('('):]}"
        stub["canonical_yiddish"] = label
        stub["org_type"] = (row.get("org_type", "") or "").strip()
        stub["linked_cluster_ids"] = SEP.join(c["cluster_id"] for c in clusters)
        stub["mentions"] = str(mentions)
        stub["n_settlements"] = str(len(settlements))
        stub["extracted_settlements"] = SEP.join(settlements)
        stubs.append(stub)

    print(f"core rows: {len(core_rows)} | existing cards: {len(addr_ids)} | "
          f"stubs to append: {len(stubs)}")
    for s in stubs[:20]:
        print(f"  {s['db_id']}\t{s['canonical_yiddish']}\t{s['org_type']}\t"
              f"mentions={s['mentions']} settlements={s['n_settlements']}")
    if len(stubs) > 20:
        print(f"  ... and {len(stubs) - 20} more")

    if args.dry_run or not stubs:
        return

    # Append-only: leave existing bytes (quoting, CRLF) untouched.
    with open(ADDR_FILE, "rb") as f:
        f.seek(-1, 2)
        ends_nl = f.read(1) == b"\n"
    with open(ADDR_FILE, "a", newline="", encoding="utf-8") as f:
        if not ends_nl:
            f.write("\r\n")
        w = csv.DictWriter(f, fieldnames=addr_headers, delimiter="\t")
        w.writerows(stubs)
    print(f"appended {len(stubs)} rows to {ADDR_FILE.name}")


if __name__ == "__main__":
    main()
