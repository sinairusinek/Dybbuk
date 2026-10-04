#!/usr/bin/env python3
"""Second pass on the requeued rows, applying Sinai's 2026-10-04 refinements.

Two rules, both from looking at what the attestations actually say:

1. BENEFACTOR NETWORKS are generic. A name that honours a funder or a movement
   names a sponsor, not an institution -- באַראַן הירש-שול is attested in
   Vienna, Sosow, East Broadway (NYC) and Vilna. Like "Rothschild hospital":
   two pupils share a philanthropist, not a school. Same for שלום עליכם and
   אַרבעטער רינג, which ran school NETWORKS by design.

2. CITY + KIND is generic unless the kind is rare in a city. One conservatory,
   polytechnic or university per city is the norm, so "Vienna Conservatory"
   plausibly denotes one place (though see the caveat below); gymnasia,
   schools, kheyders and yeshives ran to dozens per city, so "Warsaw gymnasium"
   denotes no one thing.

   Caveat recorded for the rare kinds: Wikidata lists "Vienna Conservatory" as
   a DISAMBIGUATION page, so even a rare kind is not proof of titularity. The
   rare ones stay in the queue for a human, they are not auto-aligned.

An eponym still individuates when the person is the institution's SUBJECT
rather than its sponsor -- ד"ר ווייכערטס דראַמאַטישער שול is Weichert's own
school. That distinction is why the benefactor list is explicit and short
rather than "any surname".

Idempotent; only touches rows this sweep returned to the queue.
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
REVIEWER = "rule_2026_10_04"

# Funders and movements that ran multi-city school networks.
BENEFACTORS = ("באַראַן הירש", "באראן הירש", "בארון הירש",
               "שלום עליכם", "שלום-עליכם", "שלום־עליכם",
               "אַרבעטער רינג", "אַרבעטער-רינג", "ארבעטער רינג", "ארבעטער-רינג",
               "תרבות", "צישא", "צווישא", "ציש\"א",
               "לעמעל", "באַראָכאָוו", "בארוכוב")

# Short acronyms that are real networks but also live inside ordinary words
# (יקא inside שיקאַגאָ, מוזיקאַלישער) -- matched as whole tokens only.
BENEFACTOR_TOKENS = ("יקא", 'יק"א', "אָרט", "ארט", "ort")

# Kinds a city normally had only one of -> keep for a human titular check.
RARE_KINDS = ("קאָנסערוואַטאָרי", "קאָנסערוואַטאָריום", "פּאָליטעכניק",
              "אוניווערזיטעט", "אוניווערסיטעט", "פֿאַקולטעט", "פאקולטעט",
              "אַקאַדעמיע", "אקאדעמיע", "אָקאָדעמיע")

# Kinds a city had many of -> generic when the only qualifier is the city.
COMMON_KINDS = ("גימנאַזיע", "גימנאזיע", "גימנאָזיע", "פּראָגימנאַזיע",
                "שול", "שולע", "סקול", "חדר", "חיידר", "ישיב", "תלמוד",
                "בית-המדרש", "המדרש", "קלויז", "תורה")

# City adjectives / prepositional place phrases are qualifiers, not names.
PLACE_MARKERS = ("ער ",)          # adjectival, e.g. ווינער, וואַרשעווער


def strip_marks(s: str) -> str:
    s = unicodedata.normalize("NFD", (s or "").strip())
    return "".join(c for c in s if not unicodedata.combining(c))


def tokens(name: str) -> list[str]:
    import re
    return [t for t in re.split(r"[\s\-־,]+", (name or "").strip()) if t]


def is_benefactor(name: str) -> bool:
    k = strip_marks(name)
    if any(strip_marks(b) in k for b in BENEFACTORS):
        return True
    toks = {strip_marks(t) for t in tokens(name)}
    return any(strip_marks(b) in toks for b in BENEFACTOR_TOKENS)


def is_city_plus_common_kind(name: str) -> bool:
    """City adjective (or 'in <place>') + a kind a city had many of."""
    k = strip_marks(name)
    if not any(strip_marks(c) in k for c in COMMON_KINDS):
        return False
    if any(strip_marks(rk) in k for rk in RARE_KINDS):
        return False
    toks = tokens(name)
    if len(toks) > 3:
        return False                      # longer names carry a real title
    if '"' in name or "„" in name or "”" in name:
        return False                      # quoted proper name
    # A possessive surname means the person IS the school (מארגאָליןס
    # דענטיסטישער שול, שווייגערס וואָקאַלער שול) rather than its sponsor --
    # that individuates, so it stays in the queue.
    if any(strip_marks(t).endswith("ס") and len(strip_marks(t)) >= 5
           and not any(strip_marks(c) in strip_marks(t) for c in COMMON_KINDS)
           for t in toks):
        return False
    # a city adjective ends -er, or the name is "<kind> in <place>"
    has_city = any(strip_marks(t).endswith("ער") for t in toks) or " אין " in f" {name} "
    return has_city


def main(apply: bool) -> int:
    with open(REVIEW, newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        headers, rows = list(rd.fieldnames or []), list(rd)

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    net = city = kept = 0
    examples = {"network": [], "city": []}

    for row in rows:
        notes = (row.get("reviewer_notes") or "")
        if not notes.startswith("Returned to the queue"):
            continue
        if (row.get("decision") or "").strip():
            continue                       # a human has since decided it
        name = (row.get("canonical_yiddish") or "").strip()

        if is_benefactor(name):
            why, bucket = ("benefactor/movement network: names a funder, not an "
                           "institution; branches in several cities."), "network"
        elif is_city_plus_common_kind(name):
            why, bucket = ("city + common institution kind: a city had many of "
                           "these, so the name denotes no one institution."), "city"
        else:
            kept += 1
            continue

        if bucket == "network":
            net += 1
        else:
            city += 1
        if len(examples[bucket]) < 6:
            examples[bucket].append(name)

        if apply:
            row["decision"] = "GENERIC"
            row["aligned_db_id"] = ""
            row["reviewer"] = REVIEWER
            row["reviewed_at"] = stamp
            row["reviewer_notes"] = f"Generic rule 2026-10-04 — {why} {notes}"[:480]

    print(f"benefactor/movement networks -> GENERIC : {net}")
    for e in examples["network"]:
        print(f"    {e}")
    print(f"city + common kind           -> GENERIC : {city}")
    for e in examples["city"]:
        print(f"    {e}")
    print(f"left in the queue                       : {kept}")

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
