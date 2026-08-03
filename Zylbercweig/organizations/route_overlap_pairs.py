#!/usr/bin/env python3.11
"""Route person-graph overlap shortlist pairs into the normal review flows.

From graph/org_overlap_shortlist.tsv (built by build_person_org_graph.py):
- pairs where both clusters already map to different core_db entities -> append to
  db_dedup_review.tsv with signal=person_graph (surfaces in the DB Audit view's
  dedup tab, filterable by signal), unless already present / decided / PI-distinct
- other pairs -> append to cluster_pairs_review.tsv (RA pair-merge flow) with pair_id
  prefix PG- (person-graph), evidence in the sentence fields, decision left empty
- skipped: DESCRIPTIVE/GENERIC sides, Lexicon-itself pairs, already-reviewed pairs

Dry-run by default; --apply to write.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
SHORTLIST = BASE / "graph" / "org_overlap_shortlist.tsv"
PAIRS = BASE / "cluster_pairs_review.tsv"
DEDUP_REVIEW = BASE / "db_dedup_review.tsv"
DEDUP_DECISIONS = BASE / "db_dedup_decisions.tsv"
DISTINCT = BASE / "confirmed_distinct_pairs.tsv"
CORE_DB = BASE / "core_db.tsv"

SKIP_DECISIONS = {"DESCRIPTIVE", "GENERIC"}

# db 310 = the Lexicon itself (Zylbercweig's publication). It co-occurs with
# everything by construction — pairs against it are milieu noise, never merges.
NOISE_DB_IDS = {"310"}


def read_tsv(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames or []), list(r)


def main() -> None:
    csv.field_size_limit(sys.maxsize)
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    args = ap.parse_args()

    _, shortlist = read_tsv(SHORTLIST)
    pair_headers, pair_rows = read_tsv(PAIRS)
    dedup_headers, dedup_rows = read_tsv(DEDUP_REVIEW)
    _, distinct_rows = read_tsv(DISTINCT)
    _, core_rows = read_tsv(CORE_DB)
    core_by_id = {r["db_id"]: r for r in core_rows}

    existing_cluster_pairs = {
        frozenset((p["cluster_id_i"].strip(), p["cluster_id_j"].strip()))
        for p in pair_rows
    }
    existing_db_pairs = {
        frozenset((p["db_id_a"].strip(), p["db_id_b"].strip())) for p in dedup_rows
    }
    if DEDUP_DECISIONS.exists():
        _, decision_rows = read_tsv(DEDUP_DECISIONS)
        existing_db_pairs |= {
            frozenset((p["db_id_a"].strip(), p["db_id_b"].strip()))
            for p in decision_rows
        }
    distinct_db = {
        frozenset((p["db_a"].strip(), p["db_b"].strip())) for p in distinct_rows
    }

    pg_num = 0
    for p in pair_rows:
        pid = p.get("pair_id", "")
        if pid.startswith("PG-") and pid[3:].isdigit():
            pg_num = max(pg_num, int(pid[3:]))

    new_cluster, new_db, skipped = [], [], []
    for row in shortlist:
        a, b = row["cluster_a"], row["cluster_b"]
        evidence = (f"person-graph: {row['shared_hosts']} shared biographies "
                    f"(idf {row['idf_score']}): {row['shared_host_headings']}")
        if row.get("flag") == "PI_CONFIRMED_DISTINCT":
            skipped.append((a, b, "PI confirmed distinct"))
            continue
        if row.get("decision_a") in SKIP_DECISIONS or row.get("decision_b") in SKIP_DECISIONS:
            skipped.append((a, b, "descriptive/generic side"))
            continue
        db_a, db_b = row.get("db_a", "").strip(), row.get("db_b", "").strip()
        if db_a in NOISE_DB_IDS or db_b in NOISE_DB_IDS:
            skipped.append((a, b, "Lexicon-itself side (milieu noise)"))
            continue
        if db_a and db_b and db_a != db_b:
            key = frozenset((db_a, db_b))
            if key in distinct_db:
                skipped.append((a, b, "db pair PI-distinct"))
            elif key in existing_db_pairs:
                skipped.append((a, b, "db pair already in dedup review/decisions"))
            else:
                existing_db_pairs.add(key)
                core_a, core_b = core_by_id.get(db_a, {}), core_by_id.get(db_b, {})
                dedup = {h: "" for h in dedup_headers}
                dedup.update({
                    "db_id_a": db_a, "db_id_b": db_b,
                    "name_a": core_a.get("name", ""), "name_b": core_b.get("name", ""),
                    "name_yi_a": core_a.get("name_yiddish", ""),
                    "name_yi_b": core_b.get("name_yiddish", ""),
                    "org_type_a": core_a.get("org_type", ""),
                    "org_type_b": core_b.get("org_type", ""),
                    "matched_fields": evidence,
                    "score": row["idf_score"],
                    "signal": "person_graph",
                    "type_match": "Y" if core_a.get("org_type", "").strip().lower()
                                  == core_b.get("org_type", "").strip().lower() else "N",
                    "linked_a": a, "linked_b": b,
                    "suggested_action": "REVIEW",
                })
                new_db.append(dedup)
            continue
        key = frozenset((a, b))
        if key in existing_cluster_pairs:
            skipped.append((a, b, "cluster pair already in review"))
            continue
        existing_cluster_pairs.add(key)
        pg_num += 1
        new_pair = {h: "" for h in pair_headers}
        new_pair.update({
            "pair_id": f"PG-{pg_num:04d}",
            "cluster_id_i": a, "cluster_id_j": b,
            "name_i": row["name_a"], "name_j": row["name_b"],
            "org_type": row["type_a"] if row["type_a"] == row["type_b"]
                        else f"{row['type_a']} / {row['type_b']}",
            "settlement": row["settlements_a"] or row["settlements_b"],
            "similarity": row["jaccard"],
            "sentence_i": evidence, "sentence_j": evidence,
            "heading_i": "person-graph overlap", "heading_j": "person-graph overlap",
        })
        new_cluster.append(new_pair)

    print(f"shortlist rows: {len(shortlist)}")
    print(f"-> cluster_pairs_review: {len(new_cluster)} new rows")
    print(f"-> db_dedup_review (signal=person_graph): {len(new_db)} new rows")
    print(f"skipped: {len(skipped)}")
    for a, b, why in skipped:
        print(f"   {a} <> {b}: {why}")
    for p in new_cluster:
        print(f"  PAIR {p['pair_id']}: {p['name_i']} <> {p['name_j']}")
    for p in new_db:
        print(f"  DB   {p['db_id_a']} <> {p['db_id_b']}: {p['name_yi_a'] or p['name_a']} <> {p['name_yi_b'] or p['name_b']}")

    if not args.apply:
        print("\n(dry-run — pass --apply to write)")
        return

    with PAIRS.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=pair_headers, delimiter="\t")
        w.writeheader()
        w.writerows(pair_rows + new_cluster)
    with DEDUP_REVIEW.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=dedup_headers, delimiter="\t")
        w.writeheader()
        w.writerows(dedup_rows + new_db)
    print("written.")


if __name__ == "__main__":
    main()
