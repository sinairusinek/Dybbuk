"""B1b — Rule-based triage of kg_link_review.tsv.

Closes the mechanical buckets without a human, following the project's
review-triage pattern (auto-resolve mechanical buckets, hand off residual):

  AUTO_CLUSTER  org/venue resolved to a cluster without an entity — this IS
                the designed outcome ("link to clusters that still do not
                have entities"); nothing to review.
  AUTO_NEW      unmatched surface with no candidate at all — becomes a new
                (unlinked) node by design; nothing to decide.
  AUTO_ACCEPT   fuzzy candidate with similarity >= 97 — orthographic variant.
  (queued)      everything else stays undecided for the Gemini drafter /
                a human.

Writes `decision` = AUTO_* with `reviewer_notes` = rule name; never touches
rows that already carry a human decision.

Usage:
    python3.11 auto_triage_links.py [--execute]
"""
from __future__ import annotations

import argparse
import re
from collections import Counter

import plays_common as pc


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    rows = pc.read_tsv(pc.LINK_REVIEW_TSV)
    counts = Counter()
    for r in rows:
        if r.get("decision"):
            counts["already_decided"] += 1
            continue
        method = r.get("auto_method", "")
        status = r.get("auto_status", "")
        slot = r["slot"]
        m = re.search(r"fuzzy_(\d+)", method)
        score = int(m.group(1)) if m else 0

        if "cluster_no_entity" in method:
            r["decision"], r["decided_link"] = "AUTO_CLUSTER", r["auto_link"]
            r["reviewer_notes"] = "triage: cluster-without-entity is the designed target"
        elif status == "unmatched" and not r.get("auto_link") and slot != "play":
            r["decision"] = "AUTO_NEW"
            r["reviewer_notes"] = "triage: no candidate; minted as new unlinked node"
        elif score >= 97:
            r["decision"], r["decided_link"] = "AUTO_ACCEPT", r["auto_link"]
            r["reviewer_notes"] = f"triage: fuzzy {score} orthographic variant"
        else:
            counts[f"queued:{slot}"] += 1
            continue
        counts[r["decision"]] += 1

    print(dict(counts))
    queued = sum(v for k, v in counts.items() if k.startswith("queued"))
    print(f"auto-closed: {sum(v for k, v in counts.items() if k.startswith('AUTO'))}"
          f"  queued for drafter/human: {queued}")
    if not args.execute:
        print("dry-run — pass --execute to write decisions")
        return
    pc.write_tsv(pc.LINK_REVIEW_TSV, rows, list(rows[0].keys()))
    print(f"updated {pc.LINK_REVIEW_TSV}")


if __name__ == "__main__":
    main()
