"""Retype Business/Labour by what the organization is (PI, 2026-11-01).

CLASSIFICATION_POLICY.md used to type a brewery by the person's relation to
it — Business for an owner, Labour for a worker — which splits one brewery
into two rows once it is mentioned both ways. The rule is now: production
site -> Labour (factory/workshop); commerce and services -> Business. The
relation stays on the mention and the graph edge.

This applies the rule to the existing Business-typed clusters that are
places of production (and the one core_db row linked to them). Writes
org_alignment_review.tsv and core_db.tsv, logs one row per change, and is
idempotent: a second --apply changes and logs nothing.

Usage: python3 apply_labour_business_rule_2026_11_01.py [--apply --reviewer Sinai]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
LABOUR = "Labour (factory/workshop)"

# cluster_id -> why it is a place of production
CLUSTERS = {
    "ORG-C01876": "Pilsner brewery, Radomyshl",
    "ORG-C03851": "vinegar brewery (עסיג-ברויז), Kiev",
    "ORG-C02906": "Korman's bakery, Warsaw",
    "ORG-C03819": "Pardes Rosatto, an orchard at Hadera",
    "ORG-C02739": "I. K. Poznański, Łódź textile mills",
    "ORG-C04629": "Poznański, Łódź textile mills",
    "ORG-C01235": "Heintzel & Kunitzer, Łódź textile mills",
    "ORG-C06813": "Heintzel & Kunitzer AG, Łódź textile mills",
    "ORG-C01206": "Manischewitz, matzo and food manufacturer",
    "ORG-C07038": "Manischewitz, matzo and food manufacturer",
    "ORG-C04075": "Singer sewing machines, a manufacturer (ORG-C05714 already Labour)",
    "ORG-C02542": "Ford Motor Company, a manufacturer",
}
DB_ROWS = {"1238": "vinegar brewery (עסיג-ברויז), Kiev — linked to ORG-C03851"}


def _rw(p: Path):
    term = "\r\n" if b"\r\n" in p.read_bytes()[:4096] else "\n"
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd), term


def _dump(p: Path, fields, rows, term) -> None:
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator=term)
        w.writeheader()
        w.writerows(rows)


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log = []
    for path, key, targets, view in (
            (HERE / "org_alignment_review.tsv", "cluster_id", CLUSTERS, "org_review"),
            (HERE / "core_db.tsv", "db_id", DB_ROWS, "org_addresses")):
        fields, rows, term = _rw(path)
        n = 0
        for r in rows:
            k = r[key].strip()
            if k in targets and r.get("org_type", "") != LABOUR:
                before = r.get("org_type", "")
                print(f"  {path.name:26} {k:12} {before} -> {LABOUR}  ({targets[k]})")
                r["org_type"] = LABOUR
                n += 1
                log.append([now, reviewer, view, "org_type_change", k, f"org_type={LABOUR}",
                            f"Business/Labour by what the org is (PI 2026-11-01): {targets[k]}",
                            json.dumps({"before": before, "after": LABOUR,
                                        "source": Path(__file__).name}, ensure_ascii=False)])
        if apply and n:
            _dump(path, fields, rows, term)
    print(f"\n{len(log)} change(s)")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME to write")
        return
    al = HERE / "activity_log.tsv"
    b = al.read_bytes()
    with al.open("ab") as f:
        if not b.endswith(b"\n"):
            f.write(b"\r\n")
        for row in log:
            f.write(("\t".join(row) + "\r\n").encode("utf-8"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    main(a.apply, a.reviewer.strip())
