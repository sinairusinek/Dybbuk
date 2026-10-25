#!/usr/bin/env python3
"""Turn confirmed eponym decisions into edges in graph/person_org_edges.tsv.

Writes rows with edge_source=eponym. Two schema columns are ADDED to the edge
file -- person_hub_id, person_db_id -- empty for all pre-existing rows, because
mention/plays_kg edges identify their person only by host_heading (a display
string) and an eponym edge has no host entry to inherit one from.

An eponym edge asserts ONLY that the org is named after the person. It makes no
role claim: being named for someone is not evidence they founded or ran it.
That is why professional_role_org in people_db.tsv stays empty -- a role is a
different assertion, and nothing here establishes one.

Idempotent: all edge_source=eponym rows are rebuilt from the decision ledger on
every run, so re-running after new decisions never double-writes. Non-eponym
rows are passed through untouched.

  --dry-run   report what would change, write nothing   (default)
  --execute   write
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import pathlib
import shutil

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
EDGES = BASE / "organizations" / "graph" / "person_org_edges.tsv"
DECISIONS = BASE / "organizations" / "eponym_decisions.tsv"
CANDIDATES = BASE / "organizations" / "eponym_candidates.tsv"

NEW_COLS = ["person_hub_id", "person_db_id"]

DECISION_FIELDS = [
    "db_id", "hub_id", "span", "decision", "tier", "pattern",
    "org_name_yiddish", "person_heading", "person_db_ids",
    "reviewer", "reviewed_at", "notes",
]


def load_decisions() -> list[dict]:
    if not DECISIONS.exists():
        return []
    return list(csv.DictReader(DECISIONS.open(encoding="utf-8"), delimiter="\t"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    decisions = load_decisions()
    confirmed = [d for d in decisions if (d.get("decision") or "").strip().upper() == "CONFIRM"]

    rows = list(csv.DictReader(EDGES.open(encoding="utf-8"), delimiter="\t"))
    header = list(rows[0].keys()) if rows else []
    kept = [r for r in rows if (r.get("edge_source") or "") != "eponym"]

    out_header = header + [c for c in NEW_COLS if c not in header]

    # One org can name one person across several spans -- db1335 אַסתר-רחל
    # קאַמינסקי is confirmed on four (אברהם, אסתר, יצחק, רחל), and the ledger
    # records each span so the workbook stops re-asking them. An edge, though,
    # asserts only "this org is named after this person", so the spans collapse
    # to a single edge per (org, person).
    new_edges = []
    seen_pairs: set[tuple] = set()
    for d in confirmed:
        pair = (d["db_id"], d.get("hub_id") or "")
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        new_edges.append({
            "edge_source": "eponym",
            "host_file": "", "host_xmlid": "", "host_heading": "",
            "cluster_id": "",
            "canonical_yiddish": d.get("org_name_yiddish") or "",
            "db_id": d["db_id"],
            "n_mentions": "",
            "rel_categories": "Eponym",
            "roles": "", "dates": "", "settlements": "",
            "person_hub_id": d.get("hub_id") or "",
            "person_db_id": (d.get("person_db_ids") or "").split(",")[0].strip(),
        })

    print(f"edge file:        {EDGES.relative_to(BASE)}")
    print(f"  existing rows:  {len(rows)}")
    print(f"  non-eponym kept:{len(kept)}")
    print(f"  decisions:      {len(decisions)}  (CONFIRM: {len(confirmed)})")
    print(f"  eponym edges:   {len(new_edges)}")
    print(f"  columns:        {len(header)} -> {len(out_header)} (+{', '.join(c for c in NEW_COLS if c not in header) or 'none'})")

    if not args.execute:
        print("\nDRY RUN -- nothing written. Re-run with --execute.")
        for e in new_edges[:8]:
            print(f"   + db{e['db_id']:>5s} hub={e['person_hub_id']:12s} person_db={e['person_db_id']:>6s}")
        return

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = EDGES.with_suffix(f".tsv.pre_eponym_{stamp}")
    shutil.copy2(EDGES, backup)

    tmp = EDGES.with_suffix(".tsv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_header, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for r in kept:
            w.writerow({**{c: "" for c in out_header}, **r})
        for e in new_edges:
            w.writerow({**{c: "" for c in out_header}, **e})
    tmp.replace(EDGES)
    print(f"\nwrote {EDGES.relative_to(BASE)}  (backup: {backup.name})")


if __name__ == "__main__":
    main()
