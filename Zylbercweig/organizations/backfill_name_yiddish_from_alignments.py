"""One-time sweep: backfill core_db.name_yiddish from RA-confirmed ALIGN decisions.

For each cluster in org_alignment_review.tsv with decision=ALIGN:
- if aligned_db_id has empty name_yiddish in core_db AND the cluster's
  canonical_yiddish contains Yiddish-script characters, fill it.

Safe: only fills empty cells; never overwrites. Idempotent: re-running is a no-op
once the gap is filled.

Usage:
    .venv/bin/python3 Zylbercweig/organizations/backfill_name_yiddish_from_alignments.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
ALIGN_TSV = HERE / "org_alignment_review.tsv"
CORE_DB_TSV = HERE / "core_db.tsv"

YID_RE = re.compile(r"[֐-׿יִ-ﭏ]")


def _has_yiddish(s: str) -> bool:
    return bool(YID_RE.search(s or ""))


def _read_tsv(p: Path) -> tuple[list[str], list[dict[str, str]]]:
    with open(p, encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames or []), list(r)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    a_fields, a_rows = _read_tsv(ALIGN_TSV)
    db_fields, db_rows = _read_tsv(CORE_DB_TSV)
    if "name_yiddish" not in db_fields:
        sys.exit("core_db.tsv is missing name_yiddish column. Run schema migration first.")
    db_by_id = {r["db_id"].strip(): r for r in db_rows}

    # Gather candidates: ALIGN rows where cluster is Yiddish-script and DB target is empty.
    proposals: dict[str, list[str]] = defaultdict(list)
    for r in a_rows:
        if r.get("decision", "").strip() != "ALIGN":
            continue
        did = r.get("aligned_db_id", "").strip()
        if not did or did not in db_by_id:
            continue
        if (db_by_id[did].get("name_yiddish") or "").strip():
            continue  # already populated; don't touch
        cy = (r.get("canonical_yiddish") or "").strip()
        if not _has_yiddish(cy):
            continue
        proposals[did].append(cy)

    print(f"DB rows with empty name_yiddish that have ≥1 Yiddish ALIGN: {len(proposals)}")

    updates = 0
    conflicts = 0
    for did, cys in proposals.items():
        uniq = list(dict.fromkeys(cys))  # preserve order, dedupe
        if len(uniq) == 1:
            db_by_id[did]["name_yiddish"] = uniq[0]
            updates += 1
        else:
            # Multiple distinct Yiddish forms aligned to same DB row → ambiguous.
            # Pick the one used most often; report.
            counts: dict[str, int] = {}
            for c in cys:
                counts[c] = counts.get(c, 0) + 1
            best = max(counts.items(), key=lambda x: x[1])
            db_by_id[did]["name_yiddish"] = best[0]
            updates += 1
            conflicts += 1
            print(f"  AMBIGUOUS db_id={did}: {counts}  → picked {best[0]!r}")

    print(f"Will update {updates} core_db rows ({conflicts} ambiguous).")
    if args.dry_run:
        print("(dry-run; no files written)")
        return
    if not updates:
        return

    with open(CORE_DB_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=db_fields, delimiter="\t")
        w.writeheader()
        w.writerows(db_rows)
    print(f"Wrote {CORE_DB_TSV}")
    print("Next: rerun prepare_alignment.py to refresh candidate_db_ids.")


if __name__ == "__main__":
    main()
