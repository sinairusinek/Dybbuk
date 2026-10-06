#!/usr/bin/env python3
"""Sweep every remaining DEFER row through the 2026-10-04 rules.

  generic kind      -> GENERIC   (closed; not mintable)
  everything else   -> undecided (back in the queue, with suggested alignments
                                  carried in reviewer_notes so whoever works it
                                  sees the candidates without re-deriving them)

Uses the same whitelist classifier as apply_education_generic_rule: a row is
only closed when every token is a recognised institution-kind noun or a generic
adjective. Anything carrying an unrecognised token -- a surname, a town, a
quoted title -- goes back to the queue, because wrongly closing a real
institution loses it silently while a missed generic just waits.

Names are compared with Hebrew combining marks stripped: the same name composes
differently between sources and literal comparison silently under-matches.

Idempotent. Preserves the original deferral reasoning in reviewer_notes.
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import sys
import unicodedata

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from apply_education_generic_rule import classify   # noqa: E402

csv.field_size_limit(10**9)

HERE = pathlib.Path(__file__).resolve().parent
REVIEW = HERE / "org_alignment_review.tsv"
CORE = HERE / "core_db.tsv"

REVIEWER = "rule_2026_10_04"
GENERIC_NOTE = ("Generic rule 2026-10-04: names a kind of institution, not an "
                "individuated one; not mintable.")
REQUEUE_NOTE = ("Returned to the queue 2026-10-04: not generic, needs a "
                "decision.")


def strip_marks(s: str) -> str:
    s = unicodedata.normalize("NFD", (s or "").strip())
    return "".join(c for c in s if not unicodedata.combining(c))


def main(apply: bool) -> int:
    with open(REVIEW, newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        headers, rows = list(rd.fieldnames or []), list(rd)

    db = {}
    with open(CORE, newline="", encoding="utf-8") as fh:
        for c in csv.DictReader(fh, delimiter="\t"):
            db[(c.get("db_id") or "").strip()] = c

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    generic = requeued = suggested = 0

    for row in rows:
        if (row.get("decision") or "").strip() != "DEFER":
            continue
        name = (row.get("canonical_yiddish") or "").strip()
        verdict = classify(name)
        prev = (row.get("reviewer_notes") or "").strip()

        if verdict == "GENERIC":
            generic += 1
            if apply:
                row["decision"] = "GENERIC"
                row["aligned_db_id"] = ""
                row["reviewer"] = REVIEWER
                row["reviewed_at"] = stamp
                row["reviewer_notes"] = f"{GENERIC_NOTE} {prev}"[:480]
            continue

        # back to the queue, carrying the candidates as a readable suggestion
        requeued += 1
        ids = [i.strip() for i in (row.get("candidate_db_ids") or "").split("|") if i.strip()][:3]
        hint = ""
        if ids:
            parts = []
            for i in ids:
                c = db.get(i)
                if not c:
                    continue
                label = (c.get("name_yiddish") or c.get("name") or "").strip()
                addr = (c.get("address") or "").strip()
                parts.append(f"{i}={label}" + (f" ({addr[:20]})" if addr else ""))
            if parts:
                hint = "Suggested: " + " | ".join(parts) + ". "
                suggested += 1
        if apply:
            row["decision"] = ""
            row["aligned_db_id"] = ""
            row["reviewer"] = ""
            row["reviewed_at"] = ""
            row["reviewer_notes"] = f"{REQUEUE_NOTE} {hint}{prev}"[:480]

    print(f"DEFER rows swept       : {generic + requeued}")
    print(f"  -> GENERIC (closed)  : {generic}")
    print(f"  -> back to undecided : {requeued}")
    print(f"     of those, carrying a suggested alignment: {suggested}")

    if not apply:
        print("\n[dry run] nothing written. Re-run with --apply")
        return 0

    tmp = REVIEW.with_suffix(".tsv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=headers, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(REVIEW)
    print("\nwritten.")
    return 0


if __name__ == "__main__":
    sys.exit(main(apply="--apply" in sys.argv))
