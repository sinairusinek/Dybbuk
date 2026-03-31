#!/usr/bin/env python3
"""
Build org_addresses_review.tsv for the Zalmen A2-Addresses view.

Reads:  organizations_clustered.tsv
Output: org_addresses_review.tsv — one row per cluster, with aggregated
        location data from all biographical mentions + reviewer columns.

Included org types: everything EXCEPT troupe-like types (troupes travel;
their location data reflects tour stops, not a fixed address).
"""

import csv, sys, pathlib, collections

csv.field_size_limit(sys.maxsize)

SRC = pathlib.Path(__file__).with_name("organizations_clustered.tsv")
OUT = pathlib.Path(__file__).with_name("org_addresses_review.tsv")

# Org types excluded from address review (itinerant by nature)
EXCLUDED_TYPES = {
    "troupe", "טרופּע", "טעאַטער-טרופּע", "travelling company",
    "traveling company", "army", "ארמיי", "אַרמיי", "אַרמעע",
    "military", "expedition",
}

COL_CLUSTER_ID  = "cluster_id"
COL_CANONICAL   = "canonical_yiddish"
COL_ORG_TYPE    = "_ - organizations - _ - org_type"
COL_SETTLEMENT  = "_ - organizations - _ - locations - _ - settlement"
COL_ADDRESS     = "_ - organizations - _ - locations - _ - address"
COL_VENUE       = "_ - organizations - _ - locations - _ - Venue"
COL_COUNTRY     = "_ - organizations - _ - locations - _ - country"
COL_PROVINCE    = "_ - organizations - _ - locations - _ - province"

MISSING = {"", "na", "n/a", "null", "none", "-", "--", "_"}

def is_missing(v: str) -> bool:
    return v.strip().lower() in MISSING

def pipe_join(values: set) -> str:
    """Sorted, pipe-separated non-empty distinct values."""
    return " | ".join(sorted(v for v in values if v.strip()))


print(f"Reading {SRC.name} ...")
with open(SRC, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    all_rows = list(reader)

print(f"  {len(all_rows)} rows loaded")

# ── Aggregate per cluster ─────────────────────────────────────────────────────

ClusterData = collections.namedtuple(
    "ClusterData",
    ["canonical", "org_type", "mentions",
     "settlements", "addresses", "venues", "countries", "provinces"]
)

clusters: dict[str, dict] = {}

for r in all_rows:
    ot = r.get(COL_ORG_TYPE, "").strip().lower()
    if ot in EXCLUDED_TYPES:
        continue

    cid = r.get(COL_CLUSTER_ID, "").strip()
    if not cid:
        continue

    if cid not in clusters:
        clusters[cid] = {
            "canonical": r.get(COL_CANONICAL, "").strip(),
            "org_type": r.get(COL_ORG_TYPE, "").strip(),
            "mentions": 0,
            "settlements": set(),
            "addresses": set(),
            "venues": set(),
            "countries": set(),
            "provinces": set(),
        }

    c = clusters[cid]
    c["mentions"] += 1

    for col, key in (
        (COL_SETTLEMENT, "settlements"),
        (COL_ADDRESS,    "addresses"),
        (COL_VENUE,      "venues"),
        (COL_COUNTRY,    "countries"),
        (COL_PROVINCE,   "provinces"),
    ):
        v = r.get(col, "").strip()
        if not is_missing(v):
            c[key].add(v)

print(f"  {len(clusters)} clusters after excluding itinerant types")

# ── If org_addresses_review.tsv already exists, preserve reviewer columns ─────
# Primary key: cluster_id.  Fallback: canonical_yiddish — used when cluster IDs
# are reassigned after a re-run of cluster_orgs.py (e.g. after the location-
# aware blocking fix splits previously merged clusters).
existing: dict[str, dict] = {}
existing_by_canonical: dict[str, dict] = {}
if OUT.exists():
    with open(OUT, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            cid   = row.get("cluster_id", "")
            canon = row.get("canonical_yiddish", "").strip()
            data  = {
                "confirmed_settlement":         row.get("confirmed_settlement", ""),
                "confirmed_address":            row.get("confirmed_address", ""),
                "confirmed_address_romanized":  row.get("confirmed_address_romanized", ""),
                "lat":                          row.get("lat", ""),
                "lon":                          row.get("lon", ""),
                "is_generic":                   row.get("is_generic", ""),
                "is_exploded":                  row.get("is_exploded", ""),
                "parent_cluster_id":            row.get("parent_cluster_id", ""),
                "reviewer_notes":               row.get("reviewer_notes", ""),
            }
            existing[cid] = data
            # Only register the first occurrence per canonical name as fallback.
            if canon and canon not in existing_by_canonical:
                existing_by_canonical[canon] = data
    print(f"  Preserved reviewer data from {len(existing)} existing rows")

# ── Write ─────────────────────────────────────────────────────────────────────

headers = [
    "cluster_id",
    "canonical_yiddish",
    "org_type",
    "mentions",
    "n_settlements",                 # count of distinct settlements (high = likely generic)
    "extracted_settlements",
    "extracted_addresses",
    "extracted_venues",
    "extracted_countries",
    "is_generic",                    # reviewer marks TRUE if this is a descriptive label, not one entity
    "is_exploded",                   # TRUE after reviewer explodes this cluster into sub-clusters
    "parent_cluster_id",             # set on sub-cluster rows created by explode
    "confirmed_settlement",
    "confirmed_address",
    "confirmed_address_romanized",
    "lat",
    "lon",
    "reviewer_notes",
]

# Sort: by org_type first (theatre first), then by mentions descending
TYPE_ORDER = {"theatre": 0, "טעאַטער": 1}
sorted_clusters = sorted(
    clusters.items(),
    key=lambda x: (TYPE_ORDER.get(x[1]["org_type"].lower(), 99), -x[1]["mentions"])
)

# Auto-flag clusters with many distinct settlements as likely generic.
# Threshold: ≥5 distinct settlements strongly suggests a label, not one entity.
# Reviewer can override in either direction.
GENERIC_SETTLE_THRESHOLD = 5

with open(OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
    writer.writeheader()
    for cid, c in sorted_clusters:
        # Fall back to canonical_yiddish match when cluster_id changed on re-run.
        prev = existing.get(cid) or existing_by_canonical.get(c["canonical"], {})
        n_settle = len(c["settlements"])
        # Auto-flag as generic if ≥ threshold distinct settlements, UNLESS reviewer
        # has already made an explicit decision (preserve it on re-run).
        prev_generic = prev.get("is_generic", "")
        auto_generic = "TRUE" if (n_settle >= GENERIC_SETTLE_THRESHOLD and not prev_generic) else ""
        writer.writerow({
            "cluster_id":                   cid,
            "canonical_yiddish":            c["canonical"],
            "org_type":                     c["org_type"],
            "mentions":                     c["mentions"],
            "n_settlements":                n_settle,
            "extracted_settlements":        pipe_join(c["settlements"]),
            "extracted_addresses":          pipe_join(c["addresses"]),
            "extracted_venues":             pipe_join(c["venues"]),
            "extracted_countries":          pipe_join(c["countries"]),
            "is_generic":                   prev_generic or auto_generic,
            "is_exploded":                  prev.get("is_exploded", ""),
            "parent_cluster_id":            prev.get("parent_cluster_id", ""),
            "confirmed_settlement":         prev.get("confirmed_settlement", ""),
            "confirmed_address":            prev.get("confirmed_address", ""),
            "confirmed_address_romanized":  prev.get("confirmed_address_romanized", ""),
            "lat":                          prev.get("lat", ""),
            "lon":                          prev.get("lon", ""),
            "reviewer_notes":               prev.get("reviewer_notes", ""),
        })

print(f"Wrote {len(sorted_clusters)} rows → {OUT.name}")
