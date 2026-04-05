#!/usr/bin/env python3
"""Pre-explode organization clusters with conflicting location data.

Rules:
- If one row has location and the other does not -> can stay clustered.
- If rows have same/similar location in the same field -> can stay clustered.
- If same/similar names but significantly different locations -> split,
  unless org type is itinerant (troupe-like), which can stay merged.
"""

from __future__ import annotations

import csv
import pathlib
import shutil
import sys
from collections import Counter, defaultdict, deque
from difflib import SequenceMatcher

csv.field_size_limit(sys.maxsize)

BASE = pathlib.Path(__file__).resolve().parent
SRC = BASE / "organizations_clustered.tsv"
BACKUP = BASE / "organizations_clustered_pre_explode_backup.tsv"
LOG = BASE / "pre_explode_log.tsv"

COL_CLUSTER_ID = "cluster_id"
COL_CLUSTER_SIZE = "cluster_size"
COL_CANONICAL = "canonical_yiddish"
COL_AUTO = "auto_or_review"

COL_ORG_TYPE = "_ - organizations - _ - org_type"
COL_TITLE = "_ - organizations - _ - title"
COL_CLUSTERED = "clustered organization"
COL_DESC = "_ - organizations - _ - descriptive_name"

COL_SETTLEMENT = "_ - organizations - _ - locations - _ - settlement"
COL_ADDRESS = "_ - organizations - _ - locations - _ - address"
COL_VENUE = "_ - organizations - _ - locations - _ - Venue"
COL_COUNTRY = "_ - organizations - _ - locations - _ - country"

LOCATION_COLS = [COL_SETTLEMENT, COL_ADDRESS, COL_VENUE, COL_COUNTRY]

TROUPE_TYPES = {
    "troupe",
    "טרופּע",
    "טעאַטער-טרופּע",
    "travelling company",
    "traveling company",
    "army",
    "ארמיי",
    "אַרמיי",
    "אַרמעע",
    "military",
    "expedition",
}


def is_itinerant(org_type: str) -> bool:
    return org_type.strip().lower() in TROUPE_TYPES


def norm(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def best_name(row: dict[str, str]) -> str:
    for col in (COL_CLUSTERED, COL_TITLE, COL_DESC):
        v = row.get(col, "").strip()
        if v:
            return v
    return ""


def has_location(row: dict[str, str]) -> bool:
    for col in LOCATION_COLS:
        if row.get(col, "").strip():
            return True
    return False


def similar_location(a: dict[str, str], b: dict[str, str], threshold: float = 0.80) -> bool:
    for col in LOCATION_COLS:
        va = norm(a.get(col, ""))
        vb = norm(b.get(col, ""))
        if not va or not vb:
            continue
        if va == vb:
            return True
        if SequenceMatcher(None, va, vb).ratio() >= threshold:
            return True
    return False


def can_cluster(a: dict[str, str], b: dict[str, str]) -> bool:
    # Missing location on one side is explicitly allowed.
    if not has_location(a) or not has_location(b):
        return True
    return similar_location(a, b)


def connected_components(rows: list[dict[str, str]]) -> list[list[int]]:
    n = len(rows)
    adj: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        for j in range(i + 1, n):
            if can_cluster(rows[i], rows[j]):
                adj[i].append(j)
                adj[j].append(i)

    seen: set[int] = set()
    comps: list[list[int]] = []
    for i in range(n):
        if i in seen:
            continue
        q = deque([i])
        seen.add(i)
        comp: list[int] = []
        while q:
            cur = q.popleft()
            comp.append(cur)
            for nxt in adj.get(cur, []):
                if nxt not in seen:
                    seen.add(nxt)
                    q.append(nxt)
        comps.append(comp)
    return comps


def canonical_for_group(group_rows: list[dict[str, str]]) -> str:
    names = [best_name(r) for r in group_rows if best_name(r)]
    if not names:
        return ""
    return Counter(names).most_common(1)[0][0]


def main() -> None:
    if not SRC.exists():
        raise FileNotFoundError(f"Missing file: {SRC}")

    with SRC.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    by_cluster: dict[str, list[int]] = defaultdict(list)
    for i, row in enumerate(rows):
        cid = row.get(COL_CLUSTER_ID, "").strip()
        if cid:
            by_cluster[cid].append(i)

    changed_assignments: list[dict[str, str]] = []
    split_cluster_count = 0

    # Work cluster by cluster.
    for cid, idxs in by_cluster.items():
        if len(idxs) < 2:
            continue

        cluster_rows = [rows[i] for i in idxs]
        # If all rows are itinerant, allow merge across locations.
        if all(is_itinerant(r.get(COL_ORG_TYPE, "")) for r in cluster_rows):
            continue

        comps = connected_components(cluster_rows)
        if len(comps) <= 1:
            continue

        split_cluster_count += 1
        # Reassign cluster IDs with deterministic suffixes.
        for comp_idx, comp in enumerate(comps, start=1):
            new_cid = f"{cid}_S{comp_idx:02d}"
            group_rows = [cluster_rows[j] for j in comp]
            canonical = canonical_for_group(group_rows)
            csize = str(len(group_rows))

            for local_j in comp:
                global_i = idxs[local_j]
                old_cid = rows[global_i].get(COL_CLUSTER_ID, "")
                rows[global_i][COL_CLUSTER_ID] = new_cid
                rows[global_i][COL_CLUSTER_SIZE] = csize
                rows[global_i][COL_CANONICAL] = canonical
                rows[global_i][COL_AUTO] = "pre_exploded"
                changed_assignments.append(
                    {
                        "old_cluster_id": old_cid,
                        "new_cluster_id": new_cid,
                        "name": best_name(rows[global_i]),
                        "org_type": rows[global_i].get(COL_ORG_TYPE, ""),
                        "settlement": rows[global_i].get(COL_SETTLEMENT, ""),
                        "address": rows[global_i].get(COL_ADDRESS, ""),
                        "venue": rows[global_i].get(COL_VENUE, ""),
                        "country": rows[global_i].get(COL_COUNTRY, ""),
                    }
                )

    shutil.copy2(SRC, BACKUP)

    with SRC.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    with LOG.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "old_cluster_id",
                "new_cluster_id",
                "name",
                "org_type",
                "settlement",
                "address",
                "venue",
                "country",
            ],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(changed_assignments)

    print(f"Backed up original file -> {BACKUP.name}")
    print(f"Split clusters: {split_cluster_count}")
    print(f"Reassigned rows: {len(changed_assignments)}")
    print(f"Updated file: {SRC.name}")
    print(f"Log file: {LOG.name}")


if __name__ == "__main__":
    main()
