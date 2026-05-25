"""Apply Maati's reviewed answers from `pi_punchlist - new punchlist.tsv`.

Maati answered three Hebrew columns per row (ארגון? / אם לא — מה זה? / קשור לתיאטרון יידיש?).
PI (Sinai) reviewed her answers and gave these instructions:
  - טעבענסעס שאַפּ (ORG-C02842, the unknown_tag 'etc' row): Maati was wrong — it IS an
    organization; keep it but set canonical to "OTHER - elaborate!".
  - פּ"צ-פּאָרטיי (ORG-C06481): it is a political body → "Jewish political bodies".
  - אַנטי-פאַשיסטישער באַוועגונג (ORG-C03711) and השכלה-באַוועגונג (ORG-C01134): remove from
    the org list → "Not an organization".
  - ליטווישן נאַציאָנאַל-ראַט (ORG-C02248): Maati glosses it as a Jewish-national body in
    Lithuania → "Jewish political bodies" (was mis-tagged Non-Jewish).
  - All other punchlist rows: accept Maati's judgement (keep current canonical) and mark
    as human-reviewed.

Every touched mapping row gets decided_via stamped (pi_override if canonical changed,
pi_reviewed if confirmed unchanged) and needs_review cleared. Matching is by
(row_id, current_canonical) so duplicate row_ids resolve to the exact reviewed row.
After stamping the mapping TSVs, run apply_canonical_mappings.py to propagate to the
live source TSVs (the app's inputs).
"""
from __future__ import annotations
import csv
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent
PUNCH = HERE / "pi_punchlist - new punchlist.tsv"

NOT_ORG = "Not an organization"
OTHER = "OTHER - elaborate!"
JEW_POL = "Jewish political bodies"

# (row_id, current_canonical_in_punchlist) -> (new_canonical, pi_reason)
OVERRIDES = {
    ("ORG-C02842", "etc"):                          (OTHER,   "pi_decision:tevenses_is_org_keep_other"),
    ("ORG-C03711", "Non-Jewish political bodies"):  (NOT_ORG, "pi_decision:movement_not_org"),
    ("ORG-C01134", "OTHER - elaborate!"):           (NOT_ORG, "pi_decision:haskalah_movement_not_org"),
    ("ORG-C06481", "OTHER - elaborate!"):           (JEW_POL, "pi_decision:poalei_zion_political_body"),
    ("ORG-C02248", "Non-Jewish political bodies"):  (JEW_POL, "pi_decision:litvish_council_jewish_political"),
}

MAP_BY_SOURCE = {
    "organizations_clustered_canonical_mapping.tsv": HERE / "organizations_clustered_canonical_mapping.tsv",
    "org_alignment_review_canonical_mapping.tsv":    HERE / "org_alignment_review_canonical_mapping.tsv",
}


def main() -> None:
    # Read punchlist -> list of (source_file, row_id, current_canonical)
    with PUNCH.open(newline="", encoding="utf-8") as f:
        punch = list(csv.DictReader(f, delimiter="\t"))
    reviewed = {}  # source_file -> set of (row_id, current_canonical)
    for r in punch:
        src = (r.get("source_file") or "").strip()
        rid = (r.get("row_id") or "").strip()
        cur = (r.get("current_canonical") or "").strip()
        if src and rid:
            reviewed.setdefault(src, set()).add((rid, cur))

    totals = {"override": 0, "confirmed": 0, "unmatched": 0}

    for src, keys in reviewed.items():
        path = MAP_BY_SOURCE.get(src)
        if path is None or not path.exists():
            print(f"skip (no mapping for source): {src}")
            continue
        with path.open(newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f, delimiter="\t")
            fields = list(rdr.fieldnames or [])
            rows = list(rdr)

        matched_keys = set()
        for row in rows:
            key = (row.get("row_id", "").strip(), row.get("canonical_type", "").strip())
            if key not in keys:
                continue
            matched_keys.add(key)
            if key in OVERRIDES:
                new_canon, reason = OVERRIDES[key]
                row["canonical_type"] = new_canon
                row["decided_via"] = "pi_override"
                row["review_reason"] = reason
                row["changed"] = "yes"
                totals["override"] += 1
            else:
                row["decided_via"] = "pi_reviewed"
                if not (row.get("review_reason") or "").startswith("pi_"):
                    row["review_reason"] = "pi_reviewed:maati_confirmed"
                totals["confirmed"] += 1
            row["needs_review"] = ""

        for k in keys - matched_keys:
            print(f"  UNMATCHED in {src}: {k}")
            totals["unmatched"] += 1

        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
            w.writeheader()
            w.writerows({c: row.get(c, "") for c in fields} for row in rows)
        print(f"updated {path.name}")

    print("\n=== counts ===")
    for k, v in totals.items():
        print(f"  {v:4d}  {k}")


if __name__ == "__main__":
    main()
