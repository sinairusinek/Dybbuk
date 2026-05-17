"""Apply canonical org_type mappings from canonical_mapping.tsv files to the live source TSVs.

For each source TSV, looks up the row's canonical_type from the corresponding mapping
TSV and writes it to the source's org_type column. For DB rows added since the
mapping was created (db_id > original max), falls back to map_canonical_types_v3 Pass A
tag lookup; rows that still can't be classified are left at their original tag value
and reported.

In-place edit, with .pre_path_c_backup safety copies.
"""
from __future__ import annotations
import csv
import sys
from collections import Counter
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent

# Source TSV  →  (mapping file, source row_id column, source org_type column)
JOBS = [
    ("core_db.tsv",                "core_db_canonical_mapping.tsv",                "db_id",      "org_type"),
    ("org_addresses_review.tsv",   "org_addresses_review_canonical_mapping.tsv",   "db_id",      "org_type"),
    ("org_alignment_review.tsv",   "org_alignment_review_canonical_mapping.tsv",   "cluster_id", "org_type"),
    ("organizations_clustered.tsv","organizations_clustered_canonical_mapping.tsv","cluster_id", "_ - organizations - _ - org_type"),
]

# Pass A tag → canonical lookup, minimal subset covering most cases.
# Mirrors map_canonical_types_v3.TAG_MAP_A for fallback on new rows.
TAG_MAP_A = {
    "theatre": "Theatre", "theater": "Theatre",
    "troupe": "Traveling Company", "amateur troupe": "Amateur",
    "publisher": "Publisher", "printer": "Printer", "printer/publisher": "Printer/Publisher",
    "school": "Education", "university": "Education", "academy": "Education",
    "library": "Library", "museum": "Heritage Institution", "archive": "Heritage Institution",
    "newspaper": "Journals/ Newspapers", "journal": "Journals/ Newspapers", "periodical": "Journals/ Newspapers",
    "newspaper/journal": "Journals/ Newspapers",
    "radio": "Media (Radio/ Film/TV)", "radio_station": "Media (Radio/ Film/TV)",
    "broadcaster": "Media (Radio/ Film/TV)", "film": "Media (Radio/ Film/TV)",
    "film_studio": "Media (Radio/ Film/TV)", "film_company": "Media (Radio/ Film/TV)",
    "tv": "Media (Radio/ Film/TV)", "television": "Media (Radio/ Film/TV)",
    "hospital": "Health institutions", "clinic": "Health institutions", "sanatorium": "Health institutions",
    "factory": "Labour (factory/workshop)", "workshop": "Labour (factory/workshop)",
    "synagogue": "Religious institutions/organizations", "shul": "Religious institutions/organizations",
    "temple": "Religious institutions/organizations", "religious_institution": "Religious institutions/organizations",
    "religious_organization": "Religious institutions/organizations", "house of prayer": "Religious institutions/organizations",
    "synagogue choir": "Musical organization",
    "circus": "Circus",
    "army": "Military", "military": "Military",
    "orchestra": "Musical organization", "choir": "Musical organization",
    "kleinkunst": "Kleinkunst", "amateur": "Amateur",
    "company on tour": "Company on Tour",
    "traveling company": "Traveling Company",
}


def pass_a(tag: str) -> str:
    """Minimal Pass A tag lookup; returns '' if not unambiguous."""
    if not tag:
        return ""
    key = tag.strip().lower()
    return TAG_MAP_A.get(key, "")


def load_mapping(map_file: Path) -> dict[str, str]:
    """row_id -> canonical_type (one per row_id; if duplicates, last wins)."""
    out: dict[str, str] = {}
    with map_file.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            rid = (r.get("row_id") or "").strip()
            ct  = (r.get("canonical_type") or "").strip()
            if rid:
                out[rid] = ct
    return out


def apply_to(source: Path, mapping: dict[str, str], id_col: str, type_col: str) -> dict:
    """Rewrite source TSV's type column from mapping. Returns stats."""
    with source.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        fields = list(rdr.fieldnames or [])
        rows = list(rdr)

    backup = source.with_suffix(source.suffix + ".pre_path_c_backup")
    if not backup.exists():
        with backup.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
            w.writeheader(); w.writerows(rows)

    stats = Counter()
    fallback_used = []
    unmapped = []
    for r in rows:
        rid = (r.get(id_col) or "").strip()
        orig = (r.get(type_col) or "").strip()
        if rid in mapping:
            r[type_col] = mapping[rid]
            stats["from_mapping"] += 1
        else:
            # row didn't exist when mapping was made (e.g. new DB row); try Pass A
            ct = pass_a(orig)
            if ct:
                r[type_col] = ct
                stats["from_pass_a"] += 1
                fallback_used.append((rid, orig, ct))
            else:
                stats["unmapped"] += 1
                unmapped.append((rid, orig))

    with source.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader(); w.writerows(rows)

    stats["fallback_used"] = fallback_used
    stats["unmapped_rows"] = unmapped
    return stats


def main() -> None:
    total_unmapped = []
    for src_name, map_name, id_col, type_col in JOBS:
        src = HERE / src_name
        mp  = HERE / map_name
        if not src.exists() or not mp.exists():
            print(f"SKIP {src_name}: missing source or mapping")
            continue
        mapping = load_mapping(mp)
        stats = apply_to(src, mapping, id_col, type_col)
        print(f"{src_name}: mapped={stats['from_mapping']}  pass_a_fallback={stats['from_pass_a']}  unmapped={stats['unmapped']}")
        for rid, orig, ct in stats["fallback_used"][:5]:
            print(f"    pass_a: {id_col}={rid} '{orig}' -> '{ct}'")
        for rid, orig in stats["unmapped_rows"][:10]:
            print(f"    UNMAPPED: {id_col}={rid} tag='{orig}'")
            total_unmapped.append((src_name, rid, orig))

    if total_unmapped:
        print(f"\nTotal unmapped rows: {len(total_unmapped)}")
        print("Suggestion: review unmapped rows manually or run map_canonical_types_v3 on them.")


if __name__ == "__main__":
    main()
