#!/usr/bin/env python3
"""Fold DESCRIPTIVE into GENERIC, keeping a small NOT_AN_ORG for artifacts.

The two labels were never distinguished. The reviewers' own notes on
DESCRIPTIVE rows say "Generic high-school term", "Generic educational term",
"a generic term for a state secondary school"; PI_DECISIONS.md says of
גימנאַזיע "generic word; kept DESCRIPTIVE this session". Five names were
classified BOTH ways (ארטיקע גימנאזיע, האי-סקול, האנדלס-שול, היי-סקול,
שולמית), and neither label is defined anywhere in the app. The split was by
author and date, not by meaning: DESCRIPTIVE is almost all human review,
GENERIC almost all the 2026-10-04 sweep.

What IS a distinct category, and is kept as NOT_AN_ORG: rows that are not
organization names at all -- an extraction artifact from broken line order, a
sports event, a city name, a common noun. Those should never return to the
queue, whereas a generic kind might still be minted if a source individuates
it later.

Reversible: the original decision is recorded in reviewer_notes.
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import sys
import unicodedata

csv.field_size_limit(10**9)

HERE = pathlib.Path(__file__).resolve().parent
REVIEW = HERE / "org_alignment_review.tsv"
REVIEWER = "typology_fold_2026_10_04"


def sm(s: str) -> str:
    s = unicodedata.normalize("NFD", (s or "").strip())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.translate(str.maketrans("ךםןףץ", "כמנפצ"))


# Cluster names that do not denote an organization at all.
NOT_AN_ORG = {sm(n) for n in (
    "פֿאָראַרבעטערן",      # extraction artifact, broken line order
    "א'פּ סאָווסקי",        # mis-parse of a personal name
    "מכביאַדע",            # the Maccabiah -- a sports event
    "בערלין",              # a city
    "חבר",                 # "comrade", a common noun
    "ראַדיאָ",             # the medium, not a body
    "השכלה-באַוועגונג",    # a movement, not an organization
)}


def main(apply: bool) -> int:
    with open(REVIEW, newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        headers, rows = list(rd.fieldnames or []), list(rd)

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    folded = artifacts = 0
    art_names = []

    for row in rows:
        if (row.get("decision") or "").strip() != "DESCRIPTIVE":
            continue
        name = (row.get("canonical_yiddish") or "").strip()
        prev = (row.get("reviewer_notes") or "").strip()
        was = (row.get("reviewer") or "").strip()

        if sm(name) in NOT_AN_ORG:
            artifacts += 1
            art_names.append(name)
            verdict = "NOT_AN_ORG"
            why = ("not an organization name -- extraction artifact, event, "
                   "place or common noun; never mintable.")
        else:
            folded += 1
            verdict = "GENERIC"
            why = ("names a kind of organization, not an individuated one. "
                   "Folded from DESCRIPTIVE 2026-10-04: the two labels were "
                   "the same judgement under different names.")

        if apply:
            row["decision"] = verdict
            row["aligned_db_id"] = ""
            row["reviewed_at"] = stamp
            row["reviewer_notes"] = (
                f"{verdict} 2026-10-04 — {why} "
                f"[was DESCRIPTIVE{f', {was}' if was else ''}] {prev}")[:480]

    print(f"DESCRIPTIVE -> GENERIC    : {folded}")
    print(f"DESCRIPTIVE -> NOT_AN_ORG : {artifacts}")
    for n in art_names:
        print(f"    {n}")

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
