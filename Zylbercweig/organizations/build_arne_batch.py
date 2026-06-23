"""Build an Arne audit batch for a list of German-archive cities.

Produces four TSVs in UTF-8 (no BOM, no mojibake):
  <batch>_audit_for_RA.tsv          - main worklist
  <batch>_dup_merge_template.tsv    - v2 dup schema (see docs/dup_schema.md)
  <batch>_typology_review_template.tsv
  <batch>_questions_template.tsv

Schema mirrors the Vienna+Berlin handoff, with Yiddish fields inline-annotated
[YIVO_translit] so a non-Yiddish-reading RA can audit.

Filters clusters from org_alignment_review.tsv whose extracted_settlements
contains any Yiddish variant of a target city.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path
from collections import defaultdict

from translit_yiddish_to_latin import annotate

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent

ALIGN = HERE / "org_alignment_review.tsv"
CORE_DB = HERE / "core_db.tsv"
ADDR = HERE / "org_addresses_review.tsv"
OUT_DIR = HERE

# (Latin canonical, list of Yiddish variants found in the corpus).
# Variants discovered by probing extracted_settlements in org_alignment_review.tsv
# on 2026-06-21. Add more spellings as they surface.
CITIES: list[tuple[str, list[str]]] = [
    ("Czernowitz", ["טשערנאָוויץ", "טשערנאוויץ", "טשערנעוויץ"]),
    ("Prag",        ["פּראָג", "פראָג", "פראג"]),
    ("Pressburg",   ["פּרעסבורג", "פרעסבורג", "בראַטיסלאַווע", "בראטיסלאווע"]),
    ("Brünn",       ["ברין", "בריון"]),
    ("Munich",      ["מינכן", "מינכען"]),
    ("Hamburg",     ["האַמבורג", "האמבורג"]),
    ("Breslau",     ["ברעסלוי", "ברעסלאַו", "ברעסלאו"]),
    ("Königsberg",  ["קעניגסבערג", "קעניגסבארג", "קעניגסבעריג"]),
    ("Danzig",      ["דאַנציג", "דאנציג"]),
    ("Leipzig",     ["לייפּציג", "לייפציג", "לייפּסיק"]),
    ("Dresden",     ["דרעזדן", "דרעזדען"]),
]

BATCH_NAME = "arne_batch_2026-06-21"

# Markers that route a comment to questions.tsv (mirrors ingest_arne_audit.py).
QUESTION_MARKERS = ("?", "Maybe", "Probably", "Perhaps", "probably")


def load(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def match_city(extracted: str, variants: list[str]) -> bool:
    if not extracted:
        return False
    return any(v in extracted for v in variants)


def join_annot(value: str, sep: str = "|") -> str:
    """Annotate each `sep`-separated piece with YIVO translit."""
    if not value:
        return ""
    parts = [p.strip() for p in value.split(sep) if p.strip()]
    return sep.join(annotate(p) for p in parts)


def main() -> None:
    align = load(ALIGN)
    core_db = {r["db_id"]: r for r in load(CORE_DB) if r.get("db_id")}
    addr_by_db = {r["db_id"]: r for r in load(ADDR) if r.get("db_id")}

    rows_out: list[dict] = []
    dup_seeds: list[dict] = []
    typology_seeds: list[dict] = []
    question_seeds: list[dict] = []

    # First pass: select clusters per city. A cluster can show up in multiple
    # cities; we still emit one row per (cluster, primary_city) pair.
    selected: list[tuple[str, dict]] = []
    for city_latin, variants in CITIES:
        for r in align:
            if match_city(r.get("extracted_settlements", ""), variants):
                selected.append((city_latin, r))

    # Group by cluster_id to detect heuristic-duplicate candidates (same QID
    # not yet known; flag clusters with identical canonical_yiddish in the
    # same city — Arne refines).
    by_city_name: dict[tuple[str, str], list[str]] = defaultdict(list)
    for city, r in selected:
        key = (city, (r.get("canonical_yiddish") or "").strip())
        if key[1]:
            by_city_name[key].append(r["cluster_id"])

    seen_pairs: set[tuple[str, str]] = set()
    for city, r in selected:
        cid = r["cluster_id"]
        if (city, cid) in seen_pairs:
            continue
        seen_pairs.add((city, cid))

        aligned_db = (r.get("aligned_db_id") or "").strip()
        db = core_db.get(aligned_db) if aligned_db else None
        addr = addr_by_db.get(aligned_db) if aligned_db else None

        canonical_yi = (r.get("canonical_yiddish") or "").strip()
        variants_yi = (r.get("name_variants") or "").strip()
        ext_addr = (r.get("extracted_addresses") or "").strip()
        ext_venue = (r.get("extracted_venues") or "").strip()
        ext_country = (r.get("extracted_countries") or "").strip()
        conf_set = (addr.get("confirmed_settlement", "") if addr else "").strip()
        conf_addr = (addr.get("confirmed_address", "") if addr else "").strip()
        conf_addr_rom = (addr.get("confirmed_address_romanized", "") if addr else "").strip()
        lat = (addr.get("lat", "") if addr else "").strip()
        lon = (addr.get("lon", "") if addr else "").strip()
        parent_db = (addr.get("parent_db_id", "") if addr else "").strip()
        linked_clusters = (addr.get("linked_cluster_ids", "") if addr else "").strip()
        linked_db = aligned_db
        name_latin_seed = (db.get("name", "") if db else "").strip()

        out = {
            "city": city,
            "kind": "cluster",
            "id": cid,
            "org_type": (r.get("org_type") or "").strip(),
            "name_latin": name_latin_seed,
            "name_yiddish_with_translit": annotate(canonical_yi),
            "name_variants_with_translit": join_annot(variants_yi),
            "confirmed_settlement": conf_set,
            "confirmed_address": annotate(conf_addr),
            "confirmed_address_romanized": conf_addr_rom,
            "extracted_addresses_with_translit": join_annot(ext_addr),
            "extracted_venues_with_translit": join_annot(ext_venue),
            "extracted_countries": ext_country,
            "cluster_size": (r.get("cluster_size") or "").strip(),
            "decision": (r.get("decision") or "").strip(),
            "aligned_db_id": aligned_db,
            "linked_cluster_ids": linked_clusters,
            "linked_db_id": linked_db,
            "parent_db_id": parent_db,
            "reviewer_notes": (addr.get("reviewer_notes", "") if addr else "").strip(),
            "lat": lat,
            "lon": lon,
            # Arne-fillable fields:
            "QID": "",
            "historic_address": "",
            "current_address": "",
            "miscls_theatre": "",
            "duplicate": "",
            "comments": "",
        }
        rows_out.append(out)

    # Seed dup_merge_template: name-collision clusters within same city.
    gid_counter = 0
    for (city, name), cids in by_city_name.items():
        if len(cids) < 2:
            continue
        gid_counter += 1
        gid = f"M{gid_counter:03d}"
        # Pre-pick survivor: cluster with highest cluster_size, ties broken by lowest id.
        sizes = {cid: int(next((r.get("cluster_size") or "0") for c, r in selected
                               if c == city and r["cluster_id"] == cid)) for cid in cids}
        survivor = sorted(cids, key=lambda c: (-sizes.get(c, 0), c))[0]
        for cid in sorted(cids):
            dup_seeds.append({
                "merge_group_id": gid,
                "role": "SURVIVOR" if cid == survivor else "MERGE_IN",
                "cluster_id": cid,
                "QID": "",
                "name_latin": "",
                "canonical_address": "",
                "canonical_type": "",
                "evidence_url": "",
                "notes": f"name-collision in {city}: {name}" if cid == survivor else f"dup of {gid}",
            })

    # Seed typology template (empty — Arne fills as he flags rows).
    # Headers only.

    # Seed questions template (empty — Arne fills as needed).
    # Headers only.

    # Worked-example rows (prepended to each TSV; instruction is to delete).
    def example_main():
        return {
            "city": "# EXAMPLE — delete before returning",
            "kind": "cluster", "id": "ORG-C00190_Q05", "org_type": "Theatre",
            "name_latin": "Volksbühne Berlin",
            "name_yiddish_with_translit": "פֿאָלקסבינע בערלין [folksbine berlin]",
            "name_variants_with_translit": "", "confirmed_settlement": "Berlin",
            "confirmed_address": "", "confirmed_address_romanized": "",
            "extracted_addresses_with_translit": "", "extracted_venues_with_translit": "",
            "extracted_countries": "Germany", "cluster_size": "5",
            "decision": "", "aligned_db_id": "", "linked_cluster_ids": "",
            "linked_db_id": "", "parent_db_id": "", "reviewer_notes": "",
            "lat": "", "lon": "",
            "QID": "Q617244", "historic_address": "Bülowplatz",
            "current_address": "Rosa-Luxemburg-Platz, 10178 Berlin",
            "miscls_theatre": "", "duplicate": "yes",
            "comments": "founded 1914; same building today",
        }

    def example_dup():
        return [
            {"merge_group_id": "# EXAMPLE — delete before returning",
             "role": "SURVIVOR", "cluster_id": "ORG-C00190_Q05",
             "QID": "Q617244", "name_latin": "Volksbühne Berlin",
             "canonical_address": "Rosa-Luxemburg-Platz",
             "canonical_type": "Theatre",
             "evidence_url": "https://de.wikipedia.org/wiki/Volksbühne",
             "notes": "founded 1914"},
            {"merge_group_id": "# EXAMPLE — delete before returning",
             "role": "MERGE_IN", "cluster_id": "ORG-C02488",
             "QID": "", "name_latin": "", "canonical_address": "",
             "canonical_type": "", "evidence_url": "",
             "notes": "dup of example group"},
        ]

    def example_typology():
        return {
            "kind": "# EXAMPLE — delete before returning",
            "id": "ORG-C03517", "city": "Berlin",
            "current_org_type": "Theatre",
            "corrected_org_type": "Stagecraft",
            "name_latin": "Theateratelier von Hugo Baruch",
            "comment": "Company for theatre sets, stage design and costumes",
        }

    def example_questions():
        return {
            "kind": "# EXAMPLE — delete before returning",
            "id": "ORG-C0XXXX", "city": "Prag",
            "name_latin": "Yiddish Drama Society", "QID_guess": "",
            "question": "Two clusters under nearly identical names — split or merge?",
        }

    # Write outputs.
    main_fields = [
        "city", "kind", "id", "org_type", "name_latin",
        "name_yiddish_with_translit", "name_variants_with_translit",
        "confirmed_settlement", "confirmed_address", "confirmed_address_romanized",
        "extracted_addresses_with_translit", "extracted_venues_with_translit",
        "extracted_countries", "cluster_size", "decision", "aligned_db_id",
        "linked_cluster_ids", "linked_db_id", "parent_db_id", "reviewer_notes",
        "lat", "lon",
        "QID", "historic_address", "current_address",
        "miscls_theatre", "duplicate", "comments",
    ]
    dup_fields = ["merge_group_id", "role", "cluster_id", "QID", "name_latin",
                  "canonical_address", "canonical_type", "evidence_url", "notes"]
    typology_fields = ["kind", "id", "city", "current_org_type",
                       "corrected_org_type", "name_latin", "comment"]
    question_fields = ["kind", "id", "city", "name_latin", "QID_guess", "question"]

    def dump(name: str, fields: list[str], rows: list[dict], example=None):
        path = OUT_DIR / name
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
            w.writeheader()
            if example is not None:
                ex = example if isinstance(example, list) else [example]
                for e in ex:
                    w.writerow(e)
            for r in rows:
                w.writerow(r)
        print(f"  wrote {len(rows):>4} data rows -> {name}")

    dump(f"{BATCH_NAME}_audit_for_RA.tsv", main_fields, rows_out, example=example_main())
    dump(f"{BATCH_NAME}_dup_merge_template.tsv", dup_fields, dup_seeds, example=example_dup())
    dump(f"{BATCH_NAME}_typology_review_template.tsv", typology_fields, [], example=example_typology())
    dump(f"{BATCH_NAME}_questions_template.tsv", question_fields, [], example=example_questions())

    # Per-city counts
    counts: dict[str, int] = defaultdict(int)
    for r in rows_out:
        counts[r["city"]] += 1
    print("\nrows per city:")
    for c, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {c:14s} {n:4d}")
    print(f"\ntotal: {len(rows_out)} rows | {len(dup_seeds)} dup-seed rows")


if __name__ == "__main__":
    main()
