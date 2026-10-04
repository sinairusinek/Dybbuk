#!/usr/bin/env python3
"""Apply the 2026-10-04 meeting rule to deferred Education clusters.

RULE (team meeting, 2026-10-04): the education institution kinds raised in the
questions document are generic -- they name a kind of school, not an
individuated institution -- and must not be minted. Universities are exempt.

Sinai's refinement on the grey zone (conservatories, institutes, polytechnics,
academies, colleges, seminaries): exempt where the name is *titular*, i.e.
there was only one institution of that title. A bare kind-noun, with or without
a generic adjective ("the local", "municipal", "state"), is not titular.

So each deferred Education row lands in exactly one bucket:

  GENERIC  - bare kind-noun, no individuating qualifier. Stamped decision=GENERIC.
  EXEMPT   - contains university/faculty. Left untouched (DEFER).
  REVIEW   - grey-zone kind WITH a qualifier. Left untouched (DEFER) and written
             to a punchlist for a human titular check. We do not auto-close a
             possibly-real institution: Wikidata's search API is too unreliable
             to gate on (it returns a *disambiguation page* for "Vienna
             Conservatory" and nothing at all for the Royal College of Music).

Idempotent: re-running stamps nothing new. Writes decision/reviewer/reviewed_at
and prepends the rule to reviewer_notes, preserving existing note text.
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import re
import sys

csv.field_size_limit(10**9)

HERE = pathlib.Path(__file__).resolve().parent
REVIEW = HERE / "org_alignment_review.tsv"
PUNCHLIST = HERE / "education_titular_review_punchlist.tsv"

REVIEWER = "rule_2026_10_04"
NOTE = ("Education generic rule (team meeting 2026-10-04): generic institution "
        "kind, not an individuated institution; not mintable.")

# Kind-nouns named in the questions document, plus the grey-zone tertiary kinds.
UNIVERSITY = ("אוניווערזיטעט", "אוניווערסיטעט", "פֿאַקולטעט", "פאקולטעט")
GREY_KINDS = ("קאָנסערוואַטאָרי", "קאָנסערוואַטאָריום", "פּאָליטעכניק", "אינסטיטוט",
              "אַקאַדעמיע", "אקאדעמיע", "הויך-שול", "סעמינאַר", "סעמינאר",
              "ליציי", "קאָלעדזש")
PLAIN_KINDS = ("שול", "שולע", "גימנאַזיע", "גימנאזיע", "גימנאָזיע", "פּראָגימנאַזיע",
               "תלמוד", "תורה", "חדר", "חיידר", "ישיבֿה", "ישיבה", "בית", "המדרש",
               "ספר", "קורס", "קורסן", "סקול", "העדער", "פֿעלדשער", "קלאַס",
               "פֿאָלקסשול", "פאָלקסשול", "סטודיע", "אָקאָדעמיע", "האָכשול", "האָי",
               "היי", "קינדער", "גאָרטן")
# Adjectives that qualify a kind without individuating it.
GENERIC_ADJ = ("אָרטיקער", "ארטיקער", "שטאָטישע", "שטאָטישער", "מלוכה", "מלוכהש",
               "רעגירונגס", "לערער", "פּעדאַגאָגיש", "פּאָליטעכניש", "יידיש", "ייִדיש",
               "העברעאיש", "העברעיש", "פּויליש", "רוסיש", "דייטש", "פאָלקס", "פֿאָלקס",
               "אַלגעמיינ", "מיטל", "עלעמענטאַר", "האַנטווערקער", "האַנדלס", "מוזיק",
               "טעכניש", "רעאַל", "אָוונט", "טאָג", "העכער", "פרי", "דראַמאַטיש",
               "דראָמאַטיש", "טעאַטראַל", "קונסט", "מעדיצינ", "יורידיש", "קאָמערץ",
               "אינזשיניער", "פּריוואַט", "פּריוואָט", "רומעניש", "ליטוויש",
               "ערשט", "צווייט", "אַרבעטער", "ראָבינער", "תרבות")


def _tokens(name: str) -> list[str]:
    return [t for t in re.split(r"[\s\-־,]+", (name or "").strip()) if t]


def _is_bare_kind(name: str) -> bool:
    """True when the name is only a kind-noun plus generic adjectives."""
    if '"' in name or "„" in name or "”" in name or "“" in name:
        return False  # a quoted proper name individuates
    leftovers = []
    for tok in _tokens(name):
        stripped = tok.lstrip("אבגדהוזחטיכלמנסעפצקרשתְ-ׇ")  # keep it simple
        if any(tok.startswith(k[:6]) for k in GREY_KINDS + PLAIN_KINDS):
            continue
        if any(tok.startswith(a[:5]) for a in GENERIC_ADJ):
            continue
        if tok in {"פֿאַר", "פאר", "און", "דער", "די", "דאָס", "אַ", "א"}:
            continue
        leftovers.append(tok)
    return not leftovers


# A possessive/eponymous or place attachment individuates: "X's school",
# "school of Y", "school at the Z theatre". These must never be auto-closed.
_ATTACH = ("פֿון", "פון", "אין", "ביי", "ביים", 'א"נ', 'א"ד', 'א"פ', "נאָמען")


def _is_attached(name: str) -> bool:
    return any(t in _tokens(name) for t in _ATTACH)


def classify(name: str) -> str:
    """Whitelist, not blacklist.

    Only a name built purely from a kind-noun and generic adjectives is auto-
    closed. Anything carrying a token we do not recognise -- a surname, a place,
    an initialism, a quoted title -- goes to REVIEW for a human. Heuristics are
    good at recognising "the local school"; they are bad at ruling out
    "Dr. Weichert's drama school", and the cost of those two errors is not
    symmetric: wrongly closing a real institution loses it silently.
    """
    if any(u in name for u in UNIVERSITY):
        return "EXEMPT"
    if _is_attached(name):
        return "REVIEW"          # "school of/at X" -> individuated
    if _is_bare_kind(name):
        return "GENERIC"
    return "REVIEW"


def main(apply: bool) -> int:
    with open(REVIEW, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    stamped, review_rows = 0, []
    counts = {"GENERIC": 0, "EXEMPT": 0, "REVIEW": 0}
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for row in rows:
        if (row.get("org_type") or "").strip() != "Education":
            continue
        if (row.get("decision") or "").strip() != "DEFER":
            continue
        name = (row.get("canonical_yiddish") or "").strip()
        verdict = classify(name)
        counts[verdict] += 1
        if verdict == "REVIEW":
            review_rows.append(row)
        if verdict != "GENERIC":
            continue
        if apply:
            prev = (row.get("reviewer_notes") or "").strip()
            row["decision"] = "GENERIC"
            row["reviewer"] = REVIEWER
            row["reviewed_at"] = stamp
            row["reviewer_notes"] = f"{NOTE} | {prev}" if prev else NOTE
        stamped += 1

    print(f"deferred Education rows seen : {sum(counts.values())}")
    print(f"  GENERIC (stamped)          : {counts['GENERIC']}")
    print(f"  EXEMPT  (university)       : {counts['EXEMPT']}")
    print(f"  REVIEW  (titular check)    : {counts['REVIEW']}")

    if not apply:
        print("\n[dry run] nothing written. Re-run with --apply")
        return 0

    tmp = REVIEW.with_suffix(".tsv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers, delimiter="\t",
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(REVIEW)

    cols = ["cluster_id", "canonical_yiddish", "cluster_size",
            "extracted_settlements", "candidate_db_ids", "reviewer_notes"]
    with open(PUNCHLIST, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols + ["titular?", "wikidata_qid"],
                                delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        for row in review_rows:
            writer.writerow({**{c: row.get(c, "") for c in cols},
                             "titular?": "", "wikidata_qid": ""})
    print(f"\nwrote {stamped} GENERIC stamps to {REVIEW.name}")
    print(f"wrote {len(review_rows)} rows to {PUNCHLIST.name} for the titular check")
    return 0


if __name__ == "__main__":
    sys.exit(main(apply="--apply" in sys.argv))
