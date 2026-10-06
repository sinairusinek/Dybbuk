#!/usr/bin/env python3
"""Close two gaps left by the earlier generic sweeps.

1. KIND + GENERIC ADJECTIVE, NO CITY. The city rule required a place marker
   (an -er adjective or "in <place>"), so יידישע פֿאָלקסשול ("Jewish folk
   school", Detroit) fell through and sat in the queue. An ethnolinguistic or
   descriptive adjective -- Jewish, Russian, Polish, municipal, state, folk,
   modern, new -- does not individuate any more than the city did.

2. A NAMED gymnasium wrongly closed. קאָוונער מאריאנסקי-גימנאַזיע carries a
   proper name (Marianski) beyond the city, so it is individuated and belongs
   back in the queue. The city rule's possessive guard only caught names ending
   in ס; this one is attributive.

Idempotent.
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import re
import sys
import unicodedata

csv.field_size_limit(10**9)

HERE = pathlib.Path(__file__).resolve().parent
REVIEW = HERE / "org_alignment_review.tsv"
REVIEWER = "rule_2026_10_04"

COMMON_KINDS = ("גימנאַזיע", "גימנאזיע", "גימנאָזיע", "פּראָגימנאַזיע",
                "שול", "שולע", "סקול", "חדר", "חיידר", "ישיב", "תלמוד",
                "בית-המדרש", "המדרש", "קלויז", "תורה", "גרונטשול", "הויכשול")
RARE_KINDS = ("קאָנסערוואַטאָרי", "קאָנסערוואַטאָריום", "פּאָליטעכניק",
              "אוניווערזיטעט", "אוניווערסיטעט", "פֿאַקולטעט", "פאקולטעט",
              "אַקאַדעמיע", "אקאדעמיע", "אָקאָדעמיע")
# Adjectives that describe a school without naming one.
GENERIC_ADJ = ("יידיש", "ייִדיש", "העברעאיש", "העברעיש", "רוסיש", "פּויליש",
               "פויליש", "דייטש", "פֿראַנצויזיש", "רומעניש", "ליטוויש",
               "גריכיש", "עוואָנגעליש", "מאָדערן", "נייע", "נייער", "אַלטע",
               "פֿאָלקס", "פאָלקס", "שטאָטיש", "מלוכה", "רעגירונגס", "אָרטיק",
               "ארטיק", "אַלגעמיינ", "העכער", "מיטל", "עלעמענטאַר", "אָנפֿאַנג",
               "אָנפֿאַנגס", "דאָרפֿיש", "פּריוואַט", "קייזערלעך", "קייזערליך",
               "טעכניש", "רעאַל", "אומפּאַרטייאיש")

# Rows closed by the city rule that actually carry a proper name.
REOPEN = ("מאריאנסקי",)


def strip_marks(s: str) -> str:
    s = unicodedata.normalize("NFD", (s or "").strip())
    return "".join(c for c in s if not unicodedata.combining(c))


def is_kind_plus_generic_adj(name: str) -> bool:
    toks = [t for t in re.split(r"[\s\-־,()]+", (name or "").strip()) if t]
    if not toks or len(toks) > 3:
        return False
    if any(c in name for c in '"„”“'):
        return False
    k = strip_marks(name)
    if any(strip_marks(rk) in k for rk in RARE_KINDS):
        return False
    if not any(strip_marks(c) in k for c in COMMON_KINDS):
        return False
    for tok in toks:
        t = strip_marks(tok)
        if any(strip_marks(c) in t for c in COMMON_KINDS):
            continue
        if any(t.startswith(strip_marks(a)[:5]) for a in GENERIC_ADJ):
            continue
        return False
    return True


def main(apply: bool) -> int:
    with open(REVIEW, newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        headers, rows = list(rd.fieldnames or []), list(rd)

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    closed = reopened = 0

    for row in rows:
        name = (row.get("canonical_yiddish") or "").strip()
        decision = (row.get("decision") or "").strip()
        notes = (row.get("reviewer_notes") or "")

        if (decision == "GENERIC" and "city + common institution kind" in notes
                and any(strip_marks(p) in strip_marks(name) for p in REOPEN)):
            reopened += 1
            if apply:
                row["decision"] = ""
                row["aligned_db_id"] = ""
                row["reviewer"] = ""
                row["reviewed_at"] = ""
                row["reviewer_notes"] = (
                    "Returned to the queue 2026-10-04: carries a proper name "
                    "beyond the city, so it is individuated. " + notes)[:480]
            continue

        if decision or not notes.startswith("Returned to the queue"):
            continue
        if not is_kind_plus_generic_adj(name):
            continue
        closed += 1
        if apply:
            row["decision"] = "GENERIC"
            row["aligned_db_id"] = ""
            row["reviewer"] = REVIEWER
            row["reviewed_at"] = stamp
            row["reviewer_notes"] = (
                "Generic rule 2026-10-04 — institution kind with only a "
                "descriptive adjective; names no one institution. " + notes)[:480]

    print(f"kind + generic adjective -> GENERIC : {closed}")
    print(f"named gymnasium reopened            : {reopened}")

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
