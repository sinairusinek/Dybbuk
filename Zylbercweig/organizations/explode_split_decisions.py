"""Batch-apply the Cards-view 'explode by settlement' action to db_id rows
whose only-or-primary alignment cluster carries a SPLIT decision in
org_alignment_review.tsv.

Mirrors zalmen/views/org_addresses.py: _build_explode_candidates,
_new_subcluster_row, _do_explode. Run from the organizations/ directory.

Today (2026-06-24) this targets the 2 SPLIT clusters that are linked to an
existing db_id row:
    ORG-C01316 -> db_id 263 (Groyse Teater)
    ORG-C03693 -> db_id 584 (Vinitser-Zshitomirer melukhe-teater)

Other SPLIT clusters have no db_id row to explode and were left alone.
"""
from __future__ import annotations

import csv
import pathlib
import sys

csv.field_size_limit(sys.maxsize)

HERE = pathlib.Path(__file__).parent
ADDR_FILE    = HERE / "org_addresses_review.tsv"
CLUSTER_FILE = HERE / "organizations_clustered.tsv"
ALIGN_FILE   = HERE / "org_alignment_review.tsv"

COL_CID     = "cluster_id"
COL_SETTLE  = "_ - organizations - _ - locations - _ - settlement"
COL_ADDR    = "_ - organizations - _ - locations - _ - address"
COL_VENUE   = "_ - organizations - _ - locations - _ - Venue"
COL_COUNTRY = "_ - organizations - _ - locations - _ - country"

MISSING = {"", "na", "n/a", "null", "none", "-", "--", "_"}
def _m(v: str) -> bool: return v.strip().lower() in MISSING


def load_tsv(path: pathlib.Path):
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames), list(r)


def write_tsv(path: pathlib.Path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def build_candidates(linked_cids: list[str], cluster_rows: list[dict]):
    source = [r for r in cluster_rows if r.get(COL_CID, "").strip() in linked_cids]
    groups: dict[str, dict] = {}
    for r in source:
        s = r.get(COL_SETTLE, "").strip() or r.get(COL_COUNTRY, "").strip() or "(unknown)"
        g = groups.setdefault(s, {"mentions": 0, "addresses": set(), "venues": set(), "countries": set()})
        g["mentions"] += 1
        for col, key in ((COL_ADDR, "addresses"), (COL_VENUE, "venues"), (COL_COUNTRY, "countries")):
            v = r.get(col, "").strip()
            if not _m(v):
                g[key].add(v)
    return sorted(groups.items(), key=lambda x: -x[1]["mentions"])


def new_subrow(headers, parent: dict, settle: str, g: dict, idx: int):
    empty = {h: "" for h in headers}
    name = f"{parent.get('canonical_yiddish','')} ({settle})" if settle != "(unknown)" else parent.get("canonical_yiddish","")
    empty.update({
        "db_id":                f"{parent['db_id']}_X{idx:02d}",
        "canonical_yiddish":    name,
        "org_type":             parent.get("org_type", ""),
        "mentions":             str(g["mentions"]),
        "n_settlements":        "1",
        "extracted_settlements": settle,
        "extracted_addresses":  " | ".join(sorted(g["addresses"])),
        "extracted_venues":     " | ".join(sorted(g["venues"])),
        "extracted_countries":  " | ".join(sorted(g["countries"])),
        "is_generic":           "",
        "is_exploded":          "",
        "parent_db_id":         parent["db_id"],
        "confirmed_settlement": settle,
        "confirmed_settlement_yiddish": settle,
    })
    return empty


def split_pipe(s: str) -> list[str]:
    return [v.strip() for v in (s or "").split("|") if v.strip()]


def find_split_db_ids(addr_rows, align_rows) -> list[tuple[str, list[str]]]:
    split_cids = {r["cluster_id"] for r in align_rows
                  if r.get("decision", "").strip() == "SPLIT"}
    out = []
    for r in addr_rows:
        if r.get("is_exploded") == "TRUE":
            continue
        linked = split_pipe(r.get("linked_cluster_ids", ""))
        if any(c in split_cids for c in linked):
            out.append((r["db_id"], linked))
    return out


def main():
    addr_headers, addr_rows = load_tsv(ADDR_FILE)
    _, cluster_rows = load_tsv(CLUSTER_FILE)
    _, align_rows = load_tsv(ALIGN_FILE)

    targets = find_split_db_ids(addr_rows, align_rows)
    if not targets:
        print("No SPLIT-linked db_ids to explode.")
        return

    print(f"Targets: {[d for d, _ in targets]}")

    new_rows = list(addr_rows)
    summary = []
    inserted_total = 0

    for db_id, linked in targets:
        parent_idx = next((i for i, r in enumerate(new_rows) if r.get("db_id") == db_id), None)
        if parent_idx is None:
            print(f"  ! db_id {db_id} not found, skipping")
            continue
        parent = new_rows[parent_idx]
        candidates = build_candidates(linked, cluster_rows)
        if not candidates:
            print(f"  ! db_id {db_id} has no source rows, skipping")
            continue

        subs = [new_subrow(addr_headers, parent, settle, g, i)
                for i, (settle, g) in enumerate(candidates)]
        parent["is_exploded"] = "TRUE"
        parent["is_generic"] = ""

        insert_at = parent_idx + 1
        for sub in reversed(subs):
            new_rows.insert(insert_at, sub)

        inserted_total += len(subs)
        summary.append((db_id, parent.get("canonical_yiddish", ""), [s for s, _ in candidates]))
        print(f"  ✓ {db_id} ({parent.get('canonical_yiddish','')}) -> {len(subs)} sub-clusters: {[s for s, _ in candidates]}")

    if "--apply" not in sys.argv:
        print(f"\nDRY RUN — pass --apply to write. Would insert {inserted_total} sub-clusters.")
        return

    write_tsv(ADDR_FILE, addr_headers, new_rows)
    print(f"\nWrote {ADDR_FILE} ({len(new_rows)} rows, +{inserted_total} sub-clusters).")


if __name__ == "__main__":
    main()
