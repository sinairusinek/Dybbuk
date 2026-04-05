#!/usr/bin/env python3
"""Build core_db.tsv from April DB report + alignment TSV.

Inputs:
- Organisations-Report-20260402-1025-xlsx.tsv
- Organisations-Alignmenet-xlsx.tsv
- organizations_clustered.tsv

Output:
- core_db.tsv
"""

from __future__ import annotations

import csv
import pathlib
import re
import sys
from collections import defaultdict

csv.field_size_limit(sys.maxsize)

BASE = pathlib.Path(__file__).resolve().parent

DB_REPORT = BASE / "Organisations-Report-20260402-1025-xlsx.tsv"
ALIGN_TSV = BASE / "Organisations-Alignmenet-xlsx.tsv"
CLUSTERED = BASE / "organizations_clustered.tsv"

OUT = BASE / "core_db.tsv"
UNRESOLVED_OUT = BASE / "core_db_unresolved_alignment.tsv"

COL_DB_ID = "Id"
COL_DB_NAME = "Name"
COL_DB_TYPE = "Organization Type"
COL_DB_ADDRESS = "Address(es)"

COL_ALIGN_UID = "unique-id"
COL_ALIGN_HEADING = "_ - heading"
COL_ALIGN_WITH_DB = "Align with DB"

COL_XML_ID = "_ - xml:id"
COL_TITLE = "_ - organizations - _ - title"
COL_CLUSTERED_NAME = "clustered organization"
COL_DESC = "_ - organizations - _ - descriptive_name"
COL_CLUSTER_ID = "cluster_id"


def _norm(text: str) -> str:
    if not text:
        return ""
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _best_name(row: dict[str, str]) -> str:
    for col in (COL_CLUSTERED_NAME, COL_TITLE, COL_DESC):
        val = row.get(col, "").strip()
        if val:
            return val
    return ""


def _ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    # A lightweight ratio that avoids external dependencies.
    from difflib import SequenceMatcher

    return SequenceMatcher(None, a, b).ratio()


def _parse_db_id(value: str) -> int | None:
    if not value:
        return None
    first = value.split("|", 1)[0].strip()
    if first.isdigit():
        return int(first)
    return None


def _parse_uid_xml_id(uid: str) -> str:
    # uid format: "1-facs_322_tr_1741260430"
    if not uid:
        return ""
    parts = uid.split("-", 1)
    if len(parts) == 2:
        return parts[1].strip()
    return uid.strip()


def main() -> None:
    for path in (DB_REPORT, ALIGN_TSV, CLUSTERED):
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")

    with DB_REPORT.open(newline="", encoding="utf-8") as f:
        db_rows = list(csv.DictReader(f, delimiter="\t"))

    with ALIGN_TSV.open(newline="", encoding="utf-8") as f:
        align_rows = list(csv.DictReader(f, delimiter="\t"))

    with CLUSTERED.open(newline="", encoding="utf-8") as f:
        clustered_rows = list(csv.DictReader(f, delimiter="\t"))

    # Build lookup from xml_id -> candidate clustered rows.
    by_xml: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in clustered_rows:
        xml_id = row.get(COL_XML_ID, "").strip()
        if xml_id:
            by_xml[xml_id].append(row)

    # Resolve alignment rows to cluster_ids and attach by db_id.
    linked_clusters_by_db_id: dict[int, set[str]] = defaultdict(set)
    unresolved: list[dict[str, str]] = []

    for row in align_rows:
        aligned_val = row.get(COL_ALIGN_WITH_DB, "").strip()
        if not aligned_val:
            continue

        db_id = _parse_db_id(aligned_val)
        if db_id is None:
            unresolved.append(
                {
                    "reason": "invalid_align_with_db",
                    "unique-id": row.get(COL_ALIGN_UID, ""),
                    "heading": row.get(COL_ALIGN_HEADING, ""),
                    "align_with_db": aligned_val,
                }
            )
            continue

        xml_id = _parse_uid_xml_id(row.get(COL_ALIGN_UID, ""))
        heading = _norm(row.get(COL_ALIGN_HEADING, ""))
        cands = by_xml.get(xml_id, [])
        if not cands:
            unresolved.append(
                {
                    "reason": "xml_id_not_found",
                    "unique-id": row.get(COL_ALIGN_UID, ""),
                    "heading": row.get(COL_ALIGN_HEADING, ""),
                    "align_with_db": aligned_val,
                }
            )
            continue

        # Choose best cluster candidate by heading similarity against row names.
        scored: list[tuple[float, str]] = []
        for c in cands:
            cid = c.get(COL_CLUSTER_ID, "").strip()
            if not cid:
                continue
            name_score = _ratio(heading, _norm(_best_name(c)))
            scored.append((name_score, cid))

        if not scored:
            unresolved.append(
                {
                    "reason": "no_cluster_id",
                    "unique-id": row.get(COL_ALIGN_UID, ""),
                    "heading": row.get(COL_ALIGN_HEADING, ""),
                    "align_with_db": aligned_val,
                }
            )
            continue

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_cid = scored[0]

        # Conservative threshold; if low confidence and multiple candidates, keep unresolved.
        if len(scored) > 1 and best_score < 0.60:
            unresolved.append(
                {
                    "reason": "ambiguous_cluster_match",
                    "unique-id": row.get(COL_ALIGN_UID, ""),
                    "heading": row.get(COL_ALIGN_HEADING, ""),
                    "align_with_db": aligned_val,
                }
            )
            continue

        linked_clusters_by_db_id[db_id].add(best_cid)

    # Build core DB rows from report.
    out_rows: list[dict[str, str]] = []
    for row in db_rows:
        db_id_raw = row.get(COL_DB_ID, "").strip()
        if not db_id_raw.isdigit():
            continue
        db_id = int(db_id_raw)
        linked = sorted(linked_clusters_by_db_id.get(db_id, set()))
        out_rows.append(
            {
                "db_id": str(db_id),
                "name": row.get(COL_DB_NAME, "").strip(),
                "org_type": row.get(COL_DB_TYPE, "").strip(),
                "address": row.get(COL_DB_ADDRESS, "").strip(),
                "linked_cluster_ids": " | ".join(linked),
            }
        )

    out_rows.sort(key=lambda r: int(r["db_id"]))

    with OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["db_id", "name", "org_type", "address", "linked_cluster_ids"],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(out_rows)

    with UNRESOLVED_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["reason", "unique-id", "heading", "align_with_db"],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(unresolved)

    print(f"Wrote {len(out_rows)} rows -> {OUT.name}")
    print(f"Resolved linked DB entities: {sum(1 for r in out_rows if r['linked_cluster_ids'])}")
    print(f"Unresolved align rows: {len(unresolved)} -> {UNRESOLVED_OUT.name}")


if __name__ == "__main__":
    main()
