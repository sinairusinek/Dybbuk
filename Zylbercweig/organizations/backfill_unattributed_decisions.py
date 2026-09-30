"""Backfill reviewer attribution onto decided rows that never got stamped.

Three shared mutation helpers in `zalmen/views/settlement_audit.py`
(`_align_clusters_to_db`, `_mint_db_from_clusters`, `_consolidate_clusters`) wrote
`decision` + `aligned_db_id` but never `reviewer` / `reviewed_at`. Both
settlement_audit and org_merge_cards (which imports them) route their merges,
mints and aligns through these, so 1322 of 4025 decided rows in
`org_alignment_review.tsv` — 33% — carried a decision with no attribution
(1081 NEW, 232 ALIGN, 6 DISCUSS, 3 UNCLUSTER). The helpers now stamp; this
recovers the historical gap.

`log_action` recorded the reviewer correctly at every one of those call sites, so
`activity_log.tsv` is the recovery source. A log entry can name several clusters,
in `target_id` (sometimes prefixed `cluster:` / `db:`) and in `extra`, so every
ORG-C id in both fields is matched; the LATEST entry per cluster wins.

Stamped as "<Name> (from log)" so a reconstructed attribution is never mistaken
for one recorded at decision time, with the source action in `reviewer_notes`
when that field is empty.

EXCLUDED: entries whose logged decision contradicts the row's — `db_decision`
KEEP_IN/REMOVE and `pair_decision` DISMISS/DEFER against a row reading NEW/ALIGN.
Those log a *different* action on the same cluster and do not attribute the row's
decision; crediting them would name the wrong person. They stay blank.

Idempotent: only fills rows where BOTH reviewer and reviewed_at are empty, and
never rewrites a "(from log)" stamp. Dry-run by default; --execute to write.
"""
from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path

HERE = Path(__file__).parent
ALIGN = HERE / "org_alignment_review.tsv"
LOG = HERE / "activity_log.tsv"

CID_RE = re.compile(r"ORG-C[0-9]+(?:_Q[0-9]+)?")
STAMP_SUFFIX = " (from log)"

# A logged decision attributes the row's decision only if the two are the same
# review act. MINT/MERGE both land as NEW; MERGE can also land as ALIGN.
EQUIVALENT = {
    ("MINT", "NEW"), ("MERGE", "NEW"), ("MERGE", "ALIGN"),
    ("NEW", "NEW"), ("ALIGN", "ALIGN"),
}

csv.field_size_limit(10**9)


def read_tsv(path: Path):
    with open(path, encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return rd.fieldnames, list(rd)


def write_tsv(path: Path, fields, rows):
    """Byte-exact round-trip of the file's dialect: CRLF, minimal quoting."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator="\r\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def attributes_row(logged: str, row_decision: str) -> bool:
    ld, rd = logged.strip().upper(), row_decision.strip().upper()
    return ld == rd or (ld, rd) in EQUIVALENT


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="write the file (default: dry run)")
    args = ap.parse_args()

    fields, rows = read_tsv(ALIGN)
    _, log = read_tsv(LOG)

    unattributed = {
        r["cluster_id"] for r in rows
        if r.get("decision", "").strip()
        and not r.get("reviewer", "").strip()
        and not r.get("reviewed_at", "").strip()
    }
    by_cid = {r["cluster_id"]: r for r in rows}

    # Latest logged entry per unattributed cluster, from any entry that names it.
    best: dict[str, dict] = {}
    for e in log:
        if not e.get("reviewer", "").strip():
            continue
        named = set(CID_RE.findall(f"{e.get('target_id','')} {e.get('extra','')}"))
        for cid in named & unattributed:
            if cid not in best or e.get("ts", "") > best[cid].get("ts", ""):
                best[cid] = e

    filled, skipped_mismatch = [], []
    for cid, e in best.items():
        row = by_cid[cid]
        if not attributes_row(e.get("decision", ""), row.get("decision", "")):
            skipped_mismatch.append((cid, e.get("decision", ""), row.get("decision", ""), e.get("action", "")))
            continue
        row["reviewer"] = e["reviewer"].strip() + STAMP_SUFFIX
        row["reviewed_at"] = e.get("ts", "").strip()
        if not row.get("reviewer_notes", "").strip():
            row["reviewer_notes"] = (
                f"[attribution recovered from activity_log] "
                f"{e.get('view','')}/{e.get('action','')} {e.get('ts','')}"
            )
        filled.append(cid)

    still_blank = len(unattributed) - len(filled)
    print(f"decided rows with no attribution : {len(unattributed)}")
    print(f"  recoverable from activity_log  : {len(filled)}")
    print(f"  skipped, logged act != row act : {len(skipped_mismatch)}")
    print(f"  no log entry at all            : {still_blank - len(skipped_mismatch)}")
    print(f"  still blank after backfill     : {still_blank}")
    print(f"\nby reviewer: {Counter(by_cid[c]['reviewer'] for c in filled).most_common()}")
    if skipped_mismatch:
        print("\nskipped (logged -> row, action):")
        for k, v in Counter((a, b, c) for _, a, b, c in skipped_mismatch).most_common():
            print(f"  {k} -> {v}")

    if not args.execute:
        print("\ndry run — pass --execute to write")
        return 0

    write_tsv(ALIGN, fields, rows)
    print(f"\nwrote {ALIGN}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
