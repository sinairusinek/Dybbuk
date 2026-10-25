"""Second pass over Theatre-typed rows in core_db.tsv (Sinai, 2026-10-25).

Follows apply_theatre_reclass_2026_10_13.py. That pass only read rows typed
'Theatre' (capitalised); this one reads every row typed Theatre in any case
(625 on origin/main), against three rules Sinai gave:

  סטודיע                        -> Theatre education
  וואָדעוויל / וואַריעטע          -> Kleinkunst
  כאָר / פֿילהאַרמאָניע / געזאַנג    -> Musical organization

No row carried a כאָר token. Each hit below was read; the decisions are
per-row, so this dict is also the audit trail.

Idempotent: only rewrites rows still typed Theatre.

Usage:
    python3 Zylbercweig/organizations/apply_theatre_reclass_2026_10_25.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
CORE_DB_TSV = HERE / "core_db.tsv"

KLEINKUNST = "Kleinkunst"
MUSIC = "Musical organization"
THEATRE_EDU = "Theatre education"

# db_id -> (new_type, rule, note)
DECISIONS: dict[str, tuple[str, str, str]] = {
    # ---- סטודיע -> Theatre education -------------------------------------
    "1094": (THEATRE_EDU, "studio", "Kiev teater-studye 'Onheyb'"),
    "1121": (THEATRE_EDU, "studio", "Dramstudye Robfak"),
    "1122": (THEATRE_EDU, "studio", "Rafalski's Yiddish studio"),
    "1188": (THEATRE_EDU, "studio", "Yidishe teater-studye"),
    "1190": (THEATRE_EDU, "studio", "Kiev dramstudye"),
    "1203": (THEATRE_EDU, "studio", "Kiev studye"),
    "1285": (THEATRE_EDU, "studio", "Yidishe teater-studye"),
    "1502": (THEATRE_EDU, "studio", "Belorussian-Jewish State Studio (pre-BelGOSET)"),

    # ---- וואָדעוויל / וואַריעטע -> Kleinkunst ---------------------------------
    "1918": (KLEINKUNST, "variety", "Wertheim's variete"),
    "2676": (KLEINKUNST, "vaudeville", "Eldridge vaudeville house"),

    # ---- פֿילהאַרמאָניע / געזאַנג -> Musical organization -----------------------
    "517":  (MUSIC, "philharmonic", "Vilna Philharmonic"),
    "586":  (MUSIC, "philharmonic", "Kishinev Philharmonic"),
    "2804": (MUSIC, "philharmonic", "Sverdlovsk Philharmonic"),
    # 1795 and 1901 are HALLS named for a philharmonic, i.e. venues; the rule
    # was applied as given, but they are the two rows to revisit.
    "1795": (MUSIC, "philharmonic", "Lodz 'Filharmonia' hall — a venue"),
    "1901": (MUSIC, "philharmonic", "Philharmonic Auditorium (LA) — a venue"),
    "1927": (MUSIC, "gesang", "Paterson Yiddish gezangs-farein"),
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
    not_theatre: list[str] = []
    per_rule: dict[str, int] = {}

    for db_id, (new_type, rule, note) in DECISIONS.items():
        row = by_id.get(db_id)
        if row is None:
            missing.append(db_id)
            continue
        cur = (row.get("org_type") or "").strip()
        if cur.lower() != "theatre":
            if cur == new_type:
                already += 1
            else:
                not_theatre.append(f"{db_id} (is '{cur}', wanted '{new_type}')")
            continue
        row["org_type"] = new_type
        changed += 1
        per_rule[rule] = per_rule.get(rule, 0) + 1
        label = row.get("name") or row.get("name_yiddish") or ""
        print(f"  {db_id:>5}  Theatre -> {new_type:<38} [{rule}]  {label[:45]}  — {note}")

    print()
    for rule, n in sorted(per_rule.items(), key=lambda kv: -kv[1]):
        print(f"  {rule:<16} {n}")
    print()
    print(f"decisions: {len(DECISIONS)}  changed: {changed}  already-applied: {already}")
    if not_theatre:
        print(f"  NOT Theatre any more (left alone): {len(not_theatre)}")
        for s in not_theatre:
            print(f"    {s}")
    if missing:
        print(f"  WARN not in core_db: {', '.join(missing)}")

    remaining = sum(1 for r in rows if (r.get("org_type") or "").strip().lower() == "theatre")
    print(f"still typed Theatre after this pass: {remaining}")

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
