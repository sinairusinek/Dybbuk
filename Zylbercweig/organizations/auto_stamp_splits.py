"""Auto-stamp decision=SPLIT for high-confidence drafter SPLIT recommendations.

For each undecided cluster where the drafter proposed SPLIT with confidence=high,
stamp decision=SPLIT into org_alignment_review.tsv with reviewer="auto_drafter",
provided the proposal respects the itinerant-org rule:

  Itinerant org types (Traveling Company, Company on Tour, military, etc., per
  pre_explode_clusters.TROUPE_TYPES) tour by definition — settlement spread is
  NOT evidence of multiple distinct organizations. We only auto-stamp a SPLIT
  on an itinerant type when the cluster name itself enumerates multiple troupes
  (plural form OR serial comma list).

Anything filtered out is reported but NOT stamped — those go to PI as manual
review candidates.

Usage:
    .venv/bin/python3 Zylbercweig/organizations/auto_stamp_splits.py [--apply]

Default is dry-run; pass --apply to write.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from pre_explode_clusters import is_itinerant  # single source of truth

ALIGN_TSV = HERE / "org_alignment_review.tsv"
DRAFTS_TSV = HERE / "org_alignment_drafts.tsv"

# Plural markers that diagnose a multi-troupe enumeration (overrides the
# itinerant-skip rule).
PLURAL_RE = re.compile(
    r"\bטרופּעס\b|\bטראָפּעס\b|\bקאָמפּאַניעס\b|\bטעאַטערס\b|\bטעאַטערן\b|"
    r"\btroupes\b|\bcompanies\b|\bensembles\b|\btheatres\b",
    re.IGNORECASE,
)
COMMA_RE = re.compile(r",")


def is_namelist(name: str) -> bool:
    """Cluster name enumerates multiple distinct entities (plural or serial commas)."""
    return bool(PLURAL_RE.search(name or "") or COMMA_RE.search(name or ""))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Write changes; default is dry-run.")
    args = ap.parse_args()

    with open(ALIGN_TSV, encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        a_fields = list(rdr.fieldnames or [])
        a_rows = list(rdr)
    with open(DRAFTS_TSV, encoding="utf-8") as f:
        drafts = list(csv.DictReader(f, delimiter="\t"))

    for col in ("reviewer", "reviewed_at"):
        if col not in a_fields:
            a_fields.append(col)
            for r in a_rows:
                r.setdefault(col, "")

    idx_by_cid = {r["cluster_id"]: i for i, r in enumerate(a_rows)}

    drafts_by_cid = {d["cluster_id"]: d for d in drafts}

    stamped = 0
    skipped_itinerant = 0
    skipped_not_undecided = 0
    skipped_low_conf = 0
    candidates = []  # itinerant-skipped, surfaced for PI

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for cid, d in drafts_by_cid.items():
        if d.get("draft_decision", "").strip() != "SPLIT":
            continue
        if d.get("confidence", "").strip().lower() != "high":
            skipped_low_conf += 1
            continue
        idx = idx_by_cid.get(cid)
        if idx is None:
            continue
        row = a_rows[idx]
        if row.get("decision", "").strip():
            skipped_not_undecided += 1
            continue
        org_type = row.get("org_type", "").strip()
        name = row.get("canonical_yiddish", "").strip()
        if is_itinerant(org_type) and not is_namelist(name):
            skipped_itinerant += 1
            candidates.append((cid, org_type, name, d.get("rationale", "")))
            continue

        row["decision"] = "SPLIT"
        row["reviewer_notes"] = (d.get("rationale", "") or "").strip()[:500]
        row["reviewer"] = "auto_drafter"
        row["reviewed_at"] = now
        stamped += 1

    print(f"Auto-stamp SPLIT report:")
    print(f"  ✓ would stamp:        {stamped}")
    print(f"  · skipped itinerant:  {skipped_itinerant}  (need manual review)")
    print(f"  · skipped not-undecided: {skipped_not_undecided}")
    print(f"  · skipped low-confidence: {skipped_low_conf}")
    if candidates:
        print(f"\nItinerant SPLIT proposals flagged for PI:")
        for cid, t, name, rat in candidates:
            print(f"  [{cid}] ({t}) {name!r}")
            print(f"      → {rat[:160]}")

    if not args.apply:
        print("\n(dry-run; pass --apply to write)")
        return
    if not stamped:
        return

    with open(ALIGN_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=a_fields, delimiter="\t")
        w.writeheader()
        w.writerows(a_rows)
    print(f"\nWrote {stamped} SPLIT stamps to {ALIGN_TSV}")


if __name__ == "__main__":
    main()
