#!/usr/bin/env python3
"""Surface decided non-itinerant clusters that still span ≥2 distinct QIDs.

These were left untouched by qid_explode_clusters.py because they already
have an RA decision (ALIGN/NEW/MERGE/etc., anything other than '' / SPLIT).
The user reviews them case-by-case to decide whether to split or keep as a
multi-location DB-side entity.

Output: decided_multi_place_audit.tsv
"""
from __future__ import annotations

import csv
import pathlib
import sys

_HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from settlement_overlay import resolve_anyplace  # noqa: E402
from pre_explode_clusters import is_itinerant  # noqa: E402

csv.field_size_limit(sys.maxsize)

ALIGN = _HERE / "org_alignment_review.tsv"
OUT = _HERE / "decided_multi_place_audit.tsv"
SEP = "|"


def _tokens(s: str) -> list[str]:
    return [t.strip() for t in (s or "").split(SEP) if t.strip()]


def main() -> None:
    if not ALIGN.exists():
        sys.exit(f"Missing {ALIGN}")

    rows_out: list[dict[str, str]] = []
    with ALIGN.open(newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            dec = (r.get("decision") or "").strip().upper()
            if dec in ("", "SPLIT"):
                continue
            org_type = r.get("org_type") or ""
            if is_itinerant(org_type):
                continue
            settlements = _tokens(r.get("extracted_settlements") or "")
            if not settlements:
                continue
            qid_to_english: dict[str, str] = {}
            for s in settlements:
                hit = resolve_anyplace(s)
                if hit:
                    qid_to_english.setdefault(hit.qid, hit.english)
            if len(qid_to_english) < 2:
                continue
            rows_out.append({
                "cluster_id": r.get("cluster_id", ""),
                "canonical_yiddish": r.get("canonical_yiddish", ""),
                "org_type": org_type,
                "decision": dec,
                "aligned_db_id": r.get("aligned_db_id", ""),
                "distinct_qids": str(len(qid_to_english)),
                "places_english": " | ".join(qid_to_english.values()),
                "extracted_settlements": r.get("extracted_settlements", ""),
                "reviewer": r.get("reviewer", ""),
            })

    rows_out.sort(key=lambda x: (-int(x["distinct_qids"]), x["cluster_id"]))

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["cluster_id", "canonical_yiddish", "org_type",
                        "decision", "aligned_db_id", "distinct_qids",
                        "places_english", "extracted_settlements", "reviewer"],
            delimiter="\t",
        )
        w.writeheader()
        w.writerows(rows_out)

    print(f"Decided multi-place clusters: {len(rows_out)}")
    print(f"Wrote → {OUT.name}")
    print()
    print("Head (top 20 by distinct_qids):")
    for r in rows_out[:20]:
        print(f"  {r['cluster_id']:<14} {r['decision']:<6} "
              f"q={r['distinct_qids']}  {r['org_type']:<35} "
              f"{r['canonical_yiddish']:<30}  ⇒ {r['places_english']}")


if __name__ == "__main__":
    main()
