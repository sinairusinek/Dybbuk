"""Recheck of Trade Union / Professional Association rows in core_db.tsv
(Sinai, 2026-10-25): which of them are really theatre-related societies?

All 48 rows typed 'Trade Union / Professional Association' were read against
their mention contexts in organizations_clustered.tsv. Eleven move to
'Theatre-related Society/ Union'. Three are theatre bodies outright; the rest
are cultural / art societies, moved for consistency with how core_db already
types their twins (cited per row).

The 37 that stay are genuine trade or professional bodies. The closest calls
among them: 3490 (metalworkers' club that hosted drama circles), 3497
(workers' union whose concert a singer appeared at), 3013 (gardeners'
society that owned the Bagatela theatre), 3008 (film critics' association).

Duplicates surfaced, NOT merged here (merges belong in the app):
  3000 = 2413 = 2736 = 2737  (actors' union Local 18)
  3001 = 3002 = 3061         (Zachęta)

Idempotent: only rewrites rows still typed Trade Union / Professional
Association.

Usage:
    python3 Zylbercweig/organizations/apply_trade_union_reclass_2026_10_25.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
CORE_DB_TSV = HERE / "core_db.tsv"

OLD_TYPE = "Trade Union / Professional Association"
THEATRE_SOC = "Theatre-related Society/ Union"

# db_id -> (new_type, rule, note)
DECISIONS: dict[str, tuple[str, str, str]] = {
    # ---- theatre bodies outright ------------------------------------------
    "1313": (THEATRE_SOC, "theatre", "Petrograd teater-gezelshaft; Elkin farvalter 1918"),
    "3000": (THEATRE_SOC, "theatre", "aktyorn-yunye 'Local 18' — dup of 2413/2736/2737"),
    "3507": (THEATRE_SOC, "theatre", "Tealig = teater-libhober-gezelshaft, New York"),

    # ---- cultural / art societies, by precedent ---------------------------
    "732":  (THEATRE_SOC, "precedent", "Danzig Perets-fareyn, its libhober troupe (cf. 2726)"),
    "3004": (THEATRE_SOC, "precedent", "Cleveland kultur-gezelshaft, hosted a teater-studye (cf. 2711)"),
    "3009": (THEATRE_SOC, "precedent", "Chicago kultur-gezelshaft (cf. 2711)"),
    "3010": (THEATRE_SOC, "precedent", "YIKUF, Warsaw ghetto (cf. 2711)"),
    "3005": (THEATRE_SOC, "precedent", "Jassy kultur-fareyn 'HaMeitiv' (cf. 2711)"),
    "1148": (THEATRE_SOC, "precedent", "Minsk kunst-gezelshaft, Elkin leader 1907 (cf. 1043/2712)"),
    "3001": (THEATRE_SOC, "precedent", "Zachęta — dup of 3061, which is already this type"),
    "3002": (THEATRE_SOC, "precedent", "Zachęta — dup of 3061, which is already this type"),
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with open(CORE_DB_TSV, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fields = list(reader.fieldnames or [])
        rows = list(reader)

    by_id = {r["db_id"].strip(): r for r in rows}

    changed = 0
    already = 0
    missing: list[str] = []
    moved: list[str] = []
    per_rule: dict[str, int] = {}
    for db_id, (new_type, rule, note) in DECISIONS.items():
        row = by_id.get(db_id)
        if row is None:
            missing.append(db_id)
            continue
        cur = (row.get("org_type") or "").strip()
        if cur != OLD_TYPE:
            if cur == new_type:
                already += 1
            else:
                moved.append(f"{db_id} (is '{cur}', wanted '{new_type}')")
            continue
        row["org_type"] = new_type
        changed += 1
        per_rule[rule] = per_rule.get(rule, 0) + 1
        label = row.get("name") or row.get("name_yiddish") or ""
        print(f"  {db_id:>5}  -> {new_type:<32} [{rule}]  {label[:45]}  — {note}")

    print()
    for rule, n in sorted(per_rule.items(), key=lambda kv: -kv[1]):
        print(f"  {rule:<12} {n}")
    print()
    print(f"decisions: {len(DECISIONS)}  changed: {changed}  already-applied: {already}")
    if moved:
        print(f"  retyped elsewhere since (left alone): {len(moved)}")
        for s in moved:
            print(f"    {s}")
    if missing:
        print(f"  WARN not in core_db: {', '.join(missing)}")

    remaining = sum(1 for r in rows if (r.get("org_type") or "").strip() == OLD_TYPE)
    print(f"still typed {OLD_TYPE} after this pass: {remaining}")

    if args.dry_run:
        print("\n--dry-run: core_db.tsv NOT written")
        return

    with open(CORE_DB_TSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator="\r\n")
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote {CORE_DB_TSV}")


if __name__ == "__main__":
    main()
