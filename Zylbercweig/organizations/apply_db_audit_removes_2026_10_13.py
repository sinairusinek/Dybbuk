"""Apply the DB-Audit REMOVE decisions that were logged but never written back.

The DB Audit view records a REMOVE as an activity_log `db_decision` row keyed
`<db_id>/<cluster_id>`, meaning "this cluster does not belong to this db entity;
detach it." 16 such decisions exist; only 3 ever took effect. The other 13 were
still sitting as decision=ALIGN pointing at the very db_id they were judged not
to belong to — so the audit's conclusions were invisible to the matcher, to the
app, and to anyone reading the review TSV.

Sinai found the surviving symptom by eye: ORG-C00627 הבימה העברית (a Hebrew
theatre) aligned to db 602 השומר הצעיר (a youth movement). Ruthie had made that
align on 2026-05-27 with no note; Sinai queued its REMOVE on 2026-06-28; the
REMOVE never ran. The other 12 are the same shape — 7 Goldfaden clusters off the
generic-name row db 42, 2 Kaminska off db 297, 2 Eskin off db 596, and Molly
Picon's guest performances off the Kadish-Khash troupe.

This clears decision/aligned_db_id so each cluster returns to the queue, and
re-stamps the reviewer and date from the ORIGINAL log entry — these are their
decisions being carried out late, not new ones of mine. Each detach is logged
back to activity_log.tsv.

None of the 13 appear in core_db.linked_cluster_ids, so the review TSV is the
only file that needs the detach; the script verifies that rather than assuming.

Usage:
    python3 Zylbercweig/organizations/apply_db_audit_removes_2026_10_13.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
REVIEW_TSV = HERE / "org_alignment_review.tsv"
CORE_DB_TSV = HERE / "core_db.tsv"
LOG_TSV = HERE / "activity_log.tsv"

NOTE = "DB Audit REMOVE applied (decision logged {when} by {who}, never written back)"


def read_tsv(p: Path) -> tuple[list[str], list[dict[str, str]]]:
    with open(p, encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames or []), list(r)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # 1. Collect the REMOVE decisions straight from the log — never a
    #    remembered list; the log is the ledger.
    removes: dict[str, tuple[str, str]] = {}
    _, log_rows = read_tsv(LOG_TSV)
    for r in log_rows:
        if r.get("action") == "db_decision" and (r.get("decision") or "").strip() == "REMOVE":
            removes[r["target_id"].strip()] = (r["ts"].strip(), r["reviewer"].strip())

    rv_fields, rv_rows = read_tsv(REVIEW_TSV)
    rv_by_cid = {r["cluster_id"].strip(): r for r in rv_rows}
    _, db_rows = read_tsv(CORE_DB_TSV)
    db_by_id = {r["db_id"].strip(): r for r in db_rows}

    applied: list[tuple[str, str, str, str]] = []
    already: list[str] = []
    linked_warn: list[str] = []

    for target, (ts, who) in sorted(removes.items()):
        if "/" not in target:
            print(f"  skip malformed target_id {target!r}")
            continue
        did, cid = target.split("/", 1)
        row = rv_by_cid.get(cid)
        if row is None:
            print(f"  WARN {cid} not in review TSV — skipping")
            continue

        # core_db.linked_cluster_ids is a separate surface; flag rather than
        # silently half-apply if the link lives there too.
        linked = [
            x.strip()
            for x in (db_by_id.get(did, {}).get("linked_cluster_ids", "") or "").split("|")
            if x.strip()
        ]
        if cid in linked:
            linked_warn.append(f"{did}/{cid}")

        if not (
            row.get("decision", "").strip() == "ALIGN"
            and row.get("aligned_db_id", "").strip() == did
        ):
            already.append(target)
            continue

        row["decision"] = ""
        row["aligned_db_id"] = ""
        note = NOTE.format(when=ts[:10], who=who)
        row["reviewer_notes"] = f"{row.get('reviewer_notes', '').strip()}; {note}".lstrip("; ")
        row["reviewer"] = who          # their decision, carried out late
        row["reviewed_at"] = ts
        applied.append((cid, did, who, ts))
        print(f"  {cid:<16} detached from db {did:<5} ({who}, {ts[:10]})")

    print(f"\nREMOVE decisions in log: {len(removes)}  applied now: {len(applied)}")
    if already:
        print(f"  already not aligned (nothing to do): {len(already)}")
    if linked_warn:
        print(f"  ALSO present in core_db.linked_cluster_ids — handle separately: {linked_warn}")

    if args.dry_run:
        print("\n--dry-run: nothing written")
        return
    if not applied:
        return

    with open(REVIEW_TSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rv_fields, delimiter="\t", lineterminator="\r\n")
        w.writeheader()
        w.writerows(rv_rows)
    print(f"wrote {REVIEW_TSV}")

    # Log each detach. A CLI script off Streamlit does not log on its own.
    log_fields, _ = read_tsv(LOG_TSV)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(LOG_TSV, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=log_fields, delimiter="\t", lineterminator="\n")
        for cid, did, who, ts in applied:
            w.writerow({
                "ts": stamp,
                "reviewer": who,
                "view": "db_audit",
                "action": "alignment",
                "target_id": cid,
                "decision": "",
                "note": NOTE.format(when=ts[:10], who=who),
                "extra": json.dumps({
                    "source": "apply_db_audit_removes_2026_10_13.py",
                    "detached_from_db_id": did,
                    "original_decision_ts": ts,
                }),
            })
    print(f"appended {len(applied)} rows to {LOG_TSV}")


if __name__ == "__main__":
    main()
