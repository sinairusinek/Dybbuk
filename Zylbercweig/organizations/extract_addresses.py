#!/usr/bin/env python3
"""
Build org_addresses_review.tsv for the Zalmen A2-Addresses view.

Reads:  core_db.tsv, org_alignment_review.tsv, organizations_clustered.tsv
Output: org_addresses_review.tsv — one row per DB entity, with aggregated
        location data from all aligned biographical mentions + reviewer columns.

All DB entities appear in the output.  Location data is enriched as clusters
are aligned to DB entities via the A1 alignment view.
"""

import csv, sys, pathlib, collections
from typing import Optional, Set

csv.field_size_limit(sys.maxsize)

CORE_DB = pathlib.Path(__file__).with_name("core_db.tsv")
SRC = pathlib.Path(__file__).with_name("organizations_clustered.tsv")
ALIGN = pathlib.Path(__file__).with_name("org_alignment_review.tsv")
OUT = pathlib.Path(__file__).with_name("org_addresses_review.tsv")

COL_CID        = "cluster_id"
COL_ORG_TYPE   = "_ - organizations - _ - org_type"
COL_SETTLEMENT = "_ - organizations - _ - locations - _ - settlement"
COL_ADDRESS    = "_ - organizations - _ - locations - _ - address"
COL_VENUE      = "_ - organizations - _ - locations - _ - Venue"
COL_COUNTRY    = "_ - organizations - _ - locations - _ - country"
COL_PROVINCE   = "_ - organizations - _ - locations - _ - province"

MISSING = {"", "na", "n/a", "null", "none", "-", "--", "_"}

def is_missing(v: str) -> bool:
    return v.strip().lower() in MISSING

def pipe_join(values: set) -> str:
    """Sorted, pipe-separated non-empty distinct values."""
    return " | ".join(sorted(v for v in values if v.strip()))


# ── Read core DB ──────────────────────────────────────────────────────────────

print(f"Reading {CORE_DB.name} ...")
with open(CORE_DB, newline="", encoding="utf-8") as f:
    db_rows = list(csv.DictReader(f, delimiter="\t"))
print(f"  {len(db_rows)} DB entities loaded")

# ── Build db_id → aligned cluster_ids mapping ────────────────────────────────

db_to_clusters: dict[str, set[str]] = collections.defaultdict(set)
if ALIGN.exists():
    with open(ALIGN, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            decision = r.get("decision", "").strip()
            aligned_id = r.get("aligned_db_id", "").strip()
            cid = r.get("cluster_id", "").strip()
            if decision in {"ALIGN", "NEW"} and aligned_id and cid:
                db_to_clusters[aligned_id].add(cid)
    total_links = sum(len(v) for v in db_to_clusters.values())
    print(f"  {len(db_to_clusters)} DB entities with aligned clusters ({total_links} total links)")
else:
    print("  No alignment file found")

# ── Read clustered source data ────────────────────────────────────────────────

print(f"Reading {SRC.name} ...")
with open(SRC, newline="", encoding="utf-8") as f:
    all_src_rows = list(csv.DictReader(f, delimiter="\t"))
print(f"  {len(all_src_rows)} source rows loaded")

# Index source rows by cluster_id for fast lookup
src_by_cid: dict[str, list[dict]] = collections.defaultdict(list)
for r in all_src_rows:
    cid = r.get(COL_CID, "").strip()
    if cid:
        src_by_cid[cid].append(r)

# ── Aggregate per DB entity ──────────────────────────────────────────────────

entities: dict[str, dict] = {}

for db_row in db_rows:
    db_id = db_row.get("db_id", "").strip()
    org_type = db_row.get("org_type", "").strip()

    linked_cids = db_to_clusters.get(db_id, set())

    entity = {
        "name": db_row.get("name", "").strip(),
        "org_type": org_type,
        "linked_cluster_ids": sorted(linked_cids),
        "mentions": 0,
        "settlements": set(),
        "addresses": set(),
        "venues": set(),
        "countries": set(),
        "provinces": set(),
    }

    # Add DB entity's own address if present
    db_address = db_row.get("address", "").strip()
    if db_address and not is_missing(db_address):
        entity["addresses"].add(db_address)

    # Aggregate location data from aligned clusters
    for cid in linked_cids:
        for src_row in src_by_cid.get(cid, []):
            entity["mentions"] += 1
            for col, key in (
                (COL_SETTLEMENT, "settlements"),
                (COL_ADDRESS,    "addresses"),
                (COL_VENUE,      "venues"),
                (COL_COUNTRY,    "countries"),
                (COL_PROVINCE,   "provinces"),
            ):
                v = src_row.get(col, "").strip()
                if not is_missing(v):
                    entity[key].add(v)

    entities[db_id] = entity

print(f"  {len(entities)} DB entities aggregated")

# ── Preserve existing reviewer data ──────────────────────────────────────────

existing: dict[str, dict] = {}
existing_by_name: dict[str, dict] = {}
if OUT.exists():
    with open(OUT, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            did = row.get("db_id", "").strip()
            # Backward compat: old files used cluster_id as key
            if not did:
                did = row.get("cluster_id", "").strip()
            name = row.get("canonical_yiddish", "").strip()
            data  = {
                "confirmed_settlement":         row.get("confirmed_settlement", ""),
                "confirmed_address":            row.get("confirmed_address", ""),
                "confirmed_address_romanized":  row.get("confirmed_address_romanized", ""),
                "confirmed_settlement_yiddish": row.get("confirmed_settlement_yiddish", ""),
                "lat":                          row.get("lat", ""),
                "lon":                          row.get("lon", ""),
                "is_generic":                   row.get("is_generic", ""),
                "is_exploded":                  row.get("is_exploded", ""),
                "parent_db_id":                 row.get("parent_db_id", row.get("parent_cluster_id", "")),
                "reviewer_notes":               row.get("reviewer_notes", ""),
                "confirmed_locations":          row.get("confirmed_locations", ""),
                "reviewer":                     row.get("reviewer", ""),
                "reviewed_at":                  row.get("reviewed_at", ""),
            }
            if did:
                existing[did] = data
            if name and name not in existing_by_name:
                existing_by_name[name] = data
    print(f"  Preserved reviewer data from {len(existing)} existing rows")

# ── Write ─────────────────────────────────────────────────────────────────────

headers = [
    "db_id",
    "canonical_yiddish",
    "org_type",
    "linked_cluster_ids",
    "mentions",
    "n_settlements",                 # count of distinct settlements (high = likely generic)
    "extracted_settlements",
    "extracted_addresses",
    "extracted_venues",
    "extracted_countries",
    "is_generic",                    # reviewer marks TRUE if this is a descriptive label, not one entity
    "is_exploded",                   # TRUE after reviewer explodes this cluster into sub-clusters
    "parent_db_id",                  # set on sub-entries created by explode
    "confirmed_settlement",
    "confirmed_settlement_yiddish",
    "confirmed_address",
    "confirmed_address_romanized",
    "lat",
    "lon",
    "reviewer_notes",
    "confirmed_locations",          # JSON list of {settlement, settlement_yiddish, address, address_romanized, lat, lon}
    "reviewer",
    "reviewed_at",
]

# Sort: by org_type first (theatre first), then by mentions descending, then name
TYPE_ORDER = {"theatre": 0}
sorted_entities = sorted(
    entities.items(),
    key=lambda x: (TYPE_ORDER.get(x[1]["org_type"].lower(), 99), -x[1]["mentions"], x[1]["name"])
)

# Auto-flag entities with many distinct settlements as likely generic.
GENERIC_SETTLE_THRESHOLD = 5

with open(OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
    writer.writeheader()
    for db_id, e in sorted_entities:
        prev = existing.get(db_id) or existing_by_name.get(e["name"], {})
        n_settle = len(e["settlements"])
        prev_generic = prev.get("is_generic", "")
        auto_generic = "TRUE" if (n_settle >= GENERIC_SETTLE_THRESHOLD and not prev_generic) else ""
        writer.writerow({
            "db_id":                        db_id,
            "canonical_yiddish":            e["name"],
            "org_type":                     e["org_type"],
            "linked_cluster_ids":           " | ".join(e["linked_cluster_ids"]),
            "mentions":                     e["mentions"],
            "n_settlements":                n_settle,
            "extracted_settlements":        pipe_join(e["settlements"]),
            "extracted_addresses":          pipe_join(e["addresses"]),
            "extracted_venues":             pipe_join(e["venues"]),
            "extracted_countries":          pipe_join(e["countries"]),
            "is_generic":                   prev_generic or auto_generic,
            "is_exploded":                  prev.get("is_exploded", ""),
            "parent_db_id":                 prev.get("parent_db_id", ""),
            "confirmed_settlement":         prev.get("confirmed_settlement", ""),
            "confirmed_settlement_yiddish": prev.get("confirmed_settlement_yiddish", ""),
            "confirmed_address":            prev.get("confirmed_address", ""),
            "confirmed_address_romanized":  prev.get("confirmed_address_romanized", ""),
            "lat":                          prev.get("lat", ""),
            "lon":                          prev.get("lon", ""),
            "reviewer_notes":               prev.get("reviewer_notes", ""),
            "confirmed_locations":          prev.get("confirmed_locations", ""),
            "reviewer":                     prev.get("reviewer", ""),
            "reviewed_at":                  prev.get("reviewed_at", ""),
        })

print(f"Wrote {len(sorted_entities)} rows → {OUT.name}")
