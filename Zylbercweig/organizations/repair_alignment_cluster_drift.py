#!/usr/bin/env python3

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)

BASE = Path(__file__).resolve().parent
CLUSTERED = BASE / "organizations_clustered.tsv"
ALIGNMENT = BASE / "org_alignment_review.tsv"

COL_CLUSTER_ID = "cluster_id"
COL_CANONICAL = "canonical_yiddish"
COL_CLUSTER_SIZE = "cluster_size"
COL_ORG_TYPE = "_ - organizations - _ - org_type"
COL_TITLE = "_ - organizations - _ - title"
COL_CLUSTERED = "clustered organization"
COL_DESC = "_ - organizations - _ - descriptive_name"
COL_SETTLEMENT = "_ - organizations - _ - locations - _ - settlement"
COL_ADDRESS = "_ - organizations - _ - locations - _ - address"
COL_VENUE = "_ - organizations - _ - locations - _ - Venue"
COL_COUNTRY = "_ - organizations - _ - locations - _ - country"


def best_name(row: dict[str, str]) -> str:
    for col in (COL_CLUSTERED, COL_TITLE, COL_DESC):
        value = row.get(col, "").strip()
        if value:
            return value
    return ""


def pipe_join_distinct(values: list[str]) -> str:
    seen: dict[str, None] = {}
    for value in values:
        stripped = value.strip()
        if stripped and stripped not in seen:
            seen[stripped] = None
    return " | ".join(seen.keys())


def semantic_key(canonical_yiddish: str, org_type: str, name_variants: str) -> tuple[str, str, str]:
    return (
        " ".join(canonical_yiddish.split()).strip().lower(),
        org_type.strip().lower(),
        " ".join(name_variants.split()).strip().lower(),
    )


def build_current_cluster_records() -> dict[str, dict[str, str]]:
    with CLUSTERED.open(newline="", encoding="utf-8") as f:
        clustered_rows = list(csv.DictReader(f, delimiter="\t"))

    cluster_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in clustered_rows:
        cid = row.get(COL_CLUSTER_ID, "").strip()
        if cid:
            cluster_groups[cid].append(row)

    out: dict[str, dict[str, str]] = {}
    for cid, rows in cluster_groups.items():
        rep = rows[0]
        name_variants = sorted({best_name(row) for row in rows if best_name(row)})
        settlements = sorted({row.get(COL_SETTLEMENT, "").strip() for row in rows if row.get(COL_SETTLEMENT, "").strip()})
        addresses = sorted({row.get(COL_ADDRESS, "").strip() for row in rows if row.get(COL_ADDRESS, "").strip()})
        venues = sorted({row.get(COL_VENUE, "").strip() for row in rows if row.get(COL_VENUE, "").strip()})
        countries = sorted({row.get(COL_COUNTRY, "").strip() for row in rows if row.get(COL_COUNTRY, "").strip()})
        out[cid] = {
            "cluster_id": cid,
            "canonical_yiddish": rep.get(COL_CANONICAL, "").strip(),
            "org_type": rep.get(COL_ORG_TYPE, "").strip(),
            "cluster_size": rep.get(COL_CLUSTER_SIZE, "").strip() or str(len(rows)),
            "name_variants": pipe_join_distinct(name_variants),
            "extracted_settlements": " | ".join(settlements),
            "extracted_addresses": " | ".join(addresses),
            "extracted_venues": " | ".join(venues),
            "extracted_countries": " | ".join(countries),
        }
    return out


def main() -> None:
    current_by_cluster = build_current_cluster_records()

    with ALIGNMENT.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    repaired = 0
    missing = 0
    unchanged = 0

    for row in rows:
        cid = row.get("cluster_id", "").strip()
        current = current_by_cluster.get(cid)
        if current is None:
            missing += 1
            row["candidate_db_ids"] = ""
            row["candidate_scores"] = ""
            row["candidate_methods"] = ""
            row["decision"] = ""
            row["aligned_db_id"] = ""
            row["reviewer_notes"] = ""
            continue

        row_key = semantic_key(
            row.get("canonical_yiddish", ""),
            row.get("org_type", ""),
            row.get("name_variants", ""),
        )
        current_key = semantic_key(
            current["canonical_yiddish"],
            current["org_type"],
            current["name_variants"],
        )

        for field in (
            "canonical_yiddish",
            "org_type",
            "cluster_size",
            "name_variants",
            "extracted_settlements",
            "extracted_addresses",
            "extracted_venues",
            "extracted_countries",
        ):
            row[field] = current[field]

        if row_key != current_key:
            repaired += 1
            row["candidate_db_ids"] = ""
            row["candidate_scores"] = ""
            row["candidate_methods"] = ""
            row["decision"] = ""
            row["aligned_db_id"] = ""
            row["reviewer_notes"] = ""
        else:
            unchanged += 1

    with ALIGNMENT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Current clusters: {len(current_by_cluster)}")
    print(f"Repaired stale rows: {repaired}")
    print(f"Unchanged rows: {unchanged}")
    print(f"Missing cluster ids cleared: {missing}")


if __name__ == "__main__":
    main()