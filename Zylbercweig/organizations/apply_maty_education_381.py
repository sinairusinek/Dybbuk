"""Ingest Maty's 381 Education cluster decisions (PR #14) into org_alignment_review.tsv.

PR #14 was computed against a base ~1794 commits behind origin/main, so a git merge
would revert decisions the app has since taken. This applies it as a field-level
ingest instead, preserving every note without losing a downstream decision.

Three classes of cluster, by whether main moved the row after the PR's merge-base:

  1. UNTOUCHED (359) — main never touched the row. Take Maty's decision,
     aligned_db_id, org_type and reviewer_notes wholesale.
  2. AGREED (18) — main already carries the same disposition (she entered them in
     the app the same day). Decision matches, so only fill in reviewer_notes where
     the app left them blank; never overwrite a non-empty note.
  3. SUPERSEDED (4) — main is *more* decided than the PR: C00404 and C03180 both
     went to core 2235, which is exactly the P59 merge Maty's note only defers;
     C03422 -> 2236 and C00793_Q02 -> 2238 likewise. Keep main's decision and
     aligned_db_id, but graft Maty's sourced note on as provenance so the
     reasoning is not lost.

Clusters main changed that Maty never touched are left strictly alone.

reviewer is normalized to "Maaty" — the app's own spelling, and what the 18 agreed
rows already say — rather than the PR's "Codex for Maaty", so one person does not
appear under two names in the same column.

Idempotent: re-running makes no further changes. Dry-run by default; pass --execute
to write.
"""
from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
ALIGN = HERE / "org_alignment_review.tsv"

PR_REF = "pr14"
MERGE_BASE = "8b789be1eda89672e085e07f97b8dbeb40b48f75"
REL = "Zylbercweig/organizations/org_alignment_review.tsv"

REVIEWER = "Maaty"
KEY = "cluster_id"

# Fields Maty's review is authoritative over on an untouched row.
DECISION_FIELDS = ("decision", "aligned_db_id", "org_type")
NOTE_FIELD = "reviewer_notes"

# Prefix marking a note grafted onto a row whose decision main had already settled.
SUPERSEDED_PREFIX = "[main decision stands; Maty's source note] "

csv.field_size_limit(10**9)


def read_tsv(path: Path):
    with open(path, encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return rd.fieldnames, list(rd)


def write_tsv(path: Path, fields, rows):
    """Byte-exact round-trip of the existing file's dialect: CRLF, minimal quoting."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator="\r\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


REVIEW_TAG = "Education 381 review"


def merge_note(existing: str, incoming: str) -> str:
    """Write Maty's note without discarding what the row already said.

    Her PR notes supersede the terser same-day versions she entered through the app
    (same Hebrew reasoning, plus an explicit disposition and a vol-qualified source),
    so those are replaced. Anything else on the row is infrastructure provenance —
    auto_finalize_qid's record of why a cluster was split by settlement, say — and is
    kept, with her note appended after it.
    """
    existing, incoming = existing.strip(), incoming.strip()
    if not existing:
        return incoming
    if not incoming or existing in incoming:
        return incoming or existing
    if REVIEW_TAG in existing:
        return incoming  # her own earlier, terser note — the PR version is fuller
    return f"{existing} | {incoming}"


def read_git_tsv(ref: str):
    """Read the TSV as of a git ref, without touching the working tree."""
    blob = subprocess.run(
        ["git", "show", f"{ref}:{REL}"],
        cwd=HERE, capture_output=True, check=True,
    ).stdout.decode("utf-8")
    rd = csv.DictReader(blob.splitlines(), delimiter="\t")
    return {r[KEY]: r for r in rd}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="write the file (default: dry run)")
    args = ap.parse_args()

    base = read_git_tsv(MERGE_BASE)
    maty = read_git_tsv(PR_REF)
    fields, rows = read_tsv(ALIGN)

    if fields != list(next(iter(maty.values())).keys()):
        print("ABORT: header mismatch between PR and live table", file=sys.stderr)
        return 1

    # Maty's edits, relative to the base she worked from.
    maty_edits = {k for k, v in maty.items() if base.get(k) != v}

    untouched, agreed, superseded, notes_only = [], [], [], []

    for row in rows:
        cid = row[KEY]
        if cid not in maty_edits:
            continue
        mrow = maty[cid]
        brow = base.get(cid, {})

        # Did main move this row after the PR's merge-base? Compare on the fields
        # a review actually writes, so unrelated column churn does not count.
        tracked = DECISION_FIELDS + (NOTE_FIELD, "reviewer", "reviewed_at")
        main_moved = any(row.get(f, "") != brow.get(f, "") for f in tracked)
        same_call = (
            row.get("decision") == mrow.get("decision")
            and row.get("aligned_db_id") == mrow.get("aligned_db_id")
        )

        if not main_moved:
            # Class 1: main never touched it — Maty's review is authoritative.
            for f in DECISION_FIELDS:
                row[f] = mrow.get(f, "")
            row[NOTE_FIELD] = merge_note(row.get(NOTE_FIELD, ""), mrow.get(NOTE_FIELD, ""))
            row["reviewer"] = REVIEWER
            row["reviewed_at"] = mrow.get("reviewed_at", "")
            untouched.append(cid)
        elif same_call:
            # Class 2: main agrees. Fill a blank note only; never clobber one.
            if not row.get(NOTE_FIELD, "").strip():
                row[NOTE_FIELD] = mrow.get(NOTE_FIELD, "")
                notes_only.append(cid)
            if not row.get("reviewer", "").strip():
                row["reviewer"] = REVIEWER
            agreed.append(cid)
        else:
            # Class 3: main is more decided. Keep its call, graft the note as provenance.
            note = mrow.get(NOTE_FIELD, "").strip()
            existing = row.get(NOTE_FIELD, "").strip()
            grafted = SUPERSEDED_PREFIX + note
            if note and SUPERSEDED_PREFIX not in existing:
                row[NOTE_FIELD] = f"{existing} | {grafted}".strip(" |") if existing else grafted
            superseded.append(cid)

    print(f"untouched (took Maty's decision) : {len(untouched)}")
    print(f"agreed with main                 : {len(agreed)} ({len(notes_only)} blank notes filled)")
    print(f"superseded (main's call kept)    : {len(superseded)} {superseded}")
    print(f"total rows in table              : {len(rows)}")

    if not args.execute:
        print("\ndry run — pass --execute to write")
        return 0

    write_tsv(ALIGN, fields, rows)
    print(f"\nwrote {ALIGN}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
