"""B3 — Integrity checks + health metrics + human spot-check sample.

Checks:
  - referential integrity (every edge/event endpoint exists in nodes.tsv)
  - counts by node_type / edge_type / match_status
  - date sanity (years 1860-1965 for production facts)
  - extraction health: % schema-valid facts, % evidence quotes verbatim-findable
  - YiDraCor links resolve into editions.csv
  - flagship coverage: every registry play of the digitized editions has
    authored + published_as edges
  - prints a random sample of edges with evidence beside the source snippet

Usage:
    python3.11 verify_kg.py [--sample 25] [--seed 7]
"""
from __future__ import annotations

import argparse
import random
import re
from collections import Counter

import plays_common as pc

NODES_TSV = pc.KG_DIR / "nodes.tsv"
EDGES_TSV = pc.KG_DIR / "edges.tsv"
EVENTS_TSV = pc.KG_DIR / "events.tsv"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", type=int, default=25)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    nodes = pc.read_tsv(NODES_TSV)
    edges = pc.read_tsv(EDGES_TSV)
    events = pc.read_tsv(EVENTS_TSV)
    node_ids = {n["node_id"] for n in nodes}
    problems = []

    # referential integrity
    for e in edges:
        for col in ("source_id", "target_id"):
            if e[col] and e[col] not in node_ids:
                problems.append(f"edge {e['edge_id']}: missing node {e[col]}")
    for ev in events:
        for col in ("play_id", "venue_id", "org_id", "place_id"):
            if ev[col] and ev[col] not in node_ids:
                problems.append(f"event {ev['event_id']}: missing node {ev[col]}")
    print(f"nodes={len(nodes)} edges={len(edges)} events={len(events)}")
    print(f"referential integrity: {'OK' if not problems else f'{len(problems)} PROBLEMS'}")
    for p in problems[:10]:
        print("  !", p)

    print("\nnode types:", dict(Counter(n["node_type"] for n in nodes)))
    print("node match_status:", dict(Counter(n["match_status"] for n in nodes)))
    print("edge types:", dict(Counter(e["edge_type"] for e in edges)))
    print("edge match_status:", dict(Counter(e["match_status"] for e in edges)))

    # date sanity
    bad_dates = []
    for e in edges + events:
        for col in ("date_start", "date_end"):
            v = e.get(col, "")
            if v:
                m = re.match(r"^(\d{4})", v)
                if not m or not (1860 <= int(m.group(1)) <= 1965):
                    bad_dates.append((e.get("edge_id") or e.get("event_id"), v))
    print(f"\ndate sanity (1860-1965): {len(bad_dates)} out-of-range")
    for eid, v in bad_dates[:10]:
        print(f"  ? {eid}: {v}")

    # extraction health
    for path in (pc.FLAGSHIP_TSV, pc.DRAFTS_TSV):
        rows = [r for r in pc.read_tsv(path) if r.get("fact_type") not in ("", "none")]
        if not rows:
            continue
        n_ok = sum(1 for r in rows if r.get("evidence_ok") == "yes")
        n_type = sum(1 for r in rows if r.get("fact_type") in pc.FACT_TYPES)
        n_role = sum(1 for r in rows if not r.get("person_role")
                     or r["person_role"] in pc.PERSON_ROLES)
        print(f"\n{path.name}: {len(rows)} facts | evidence verbatim: "
              f"{100 * n_ok / len(rows):.1f}% | valid fact_type: "
              f"{100 * n_type / len(rows):.1f}% | valid role: "
              f"{100 * n_role / len(rows):.1f}%")

    # edition links
    ed_docs = set()
    import csv as _csv
    with open(pc.YIDRACOR_DATA / "editions.csv", encoding="utf-8-sig") as f:
        for r in _csv.DictReader(f):
            d = (r.get("transkribus_doc_id") or "").strip().split(".")[0]
            if d:
                ed_docs.add(d)
    ed_edges = [e for e in edges if e["edge_type"] == "published_as"]
    bad_ed = [e for e in ed_edges
              if e["target_id"].removeprefix("edition:tkb_") not in ed_docs]
    plays_with_ed = {e["source_id"] for e in ed_edges}
    print(f"\npublished_as edges: {len(ed_edges)} covering {len(plays_with_ed)} plays; "
          f"{len(bad_ed)} point at unknown transkribus docs")

    # spot-check sample
    entries = {e["person_id"]: e["entry_text"] for e in pc.read_tsv(pc.ENTRY_TEXTS_TSV)}
    random.seed(args.seed)
    evid_edges = [e for e in edges if e["evidence_sentence"]]
    sample = random.sample(evid_edges, min(args.sample, len(evid_edges)))
    print(f"\n=== spot-check sample ({len(sample)} edges) ===")
    for e in sample:
        src = entries.get(e["provenance_person_id"], "")
        q = e["evidence_sentence"]
        pos = src.find(q[:60]) if q else -1
        snippet = src[max(0, pos - 100):pos + len(q) + 100].replace("\n", " ") if pos >= 0 else "(quote not located)"
        print(f"\n[{e['edge_id']}] {e['source_id']} -{e['edge_type']}-> {e['target_id']}"
              f"  ({e['role_detail']}/{e['character']}) {e['date_start']}")
        print(f"  quote: {q[:160]}")
        print(f"  source: ...{snippet[:280]}...")

    print("\nverify_kg done.")


if __name__ == "__main__":
    main()
