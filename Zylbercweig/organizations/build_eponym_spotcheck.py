#!/usr/bin/env python3
"""Regenerate eponym_spotcheck_queue.tsv from the decision ledger.

DERIVED, NOT A LEDGER -- this file is rebuilt from eponym_decisions.tsv and
holds no decisions of its own. Re-run it after any ingest; the queue grew from
94 to 153 rows when the 10-21 answers landed, so a stale copy understates the
review backlog.

A CONFIRM lands in the queue when either:
  * `tier` is `low` -- a bare surname the generator could not disambiguate
    (`פֿרידמאַן` matches 15 hubs, `שוואַרץ` 8), or
  * the SPAN does not appear in the chosen person's heading -- the stronger
    signal, because it is the shape of a row slip.

That second test is what caught db1047: `זאָלאָטאַרעווסקיס טרופּע` answered as
`זאַסלאַווסקי`, an answer that had slipped onto a different surname whose hub was
a CONFIRM for a different org. Reading the org name against the person surfaces
these; neither tier nor reviewer identity does.

  python3.11 build_eponym_spotcheck.py            # report
  python3.11 build_eponym_spotcheck.py --write    # rewrite the queue
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import re
import sys
import unicodedata

csv.field_size_limit(10 ** 9)

HERE = pathlib.Path(__file__).resolve().parent
DECISIONS = HERE / "eponym_decisions.tsv"
OUT = HERE / "eponym_spotcheck_queue.tsv"

FIELDS = ["db_id", "org_name", "span", "person_heading", "hub_id",
          "tier", "pattern", "reviewer", "flag", "notes"]


def fold(s: str) -> str:
    """Strip points and fold final letters, so a span matches its heading form.

    Hebrew points must go before any comparison, and finals must fold on BOTH
    sides: the workbook writes spans with final letters normalised away
    (`גאלדפאדנ`) while headings keep them (`גאָלדפֿאַדען`).
    """
    if not s:
        return ""
    nfd = unicodedata.normalize("NFD", s)
    s = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    for a, b in (("ם", "מ"), ("ן", "נ"), ("ך", "כ"), ("ף", "פ"), ("ץ", "צ")):
        s = s.replace(a, b)
    # Keep token boundaries: collapsing punctuation to nothing concatenates a
    # heading's surname and given name (`וואָלערשטיין, סאול` ->
    # `וואלערשטיינסאול`), and a possessive span then matches by accident on the
    # given name's first letter -- `וואלערשטייןס` "found" inside it. Both rows
    # the original queue flagged were lost that way. Fold to SPACES and compare
    # token-wise.
    return re.sub(r"[^\w]+", " ", s).lower().strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args()

    with open(DECISIONS, encoding="utf-8", newline="") as f:
        dec = list(csv.DictReader(f, delimiter="\t"))

    rows = []
    for d in dec:
        if d["decision"].strip() != "CONFIRM":
            continue
        span, heading = d["span"].strip(), d["person_heading"].strip()
        low = d["tier"].strip() == "low"
        # A db-only hub has no heading beyond the minted name; an empty heading
        # cannot be tested for span containment, so do not flag it as a slip.
        # Compare the span EXACTLY as folded, without stripping a possessive
        # suffix. `וואלערשטייןס` is not a substring of `וואָלערשטיין` and
        # `פרידעסע` not of `פֿרידעס`, so both flag -- which is correct and
        # matches the queue's original behaviour: a possessive span is still
        # worth a human glance, and widening the test to ignore the suffix
        # would quietly shrink the net by exactly those rows.
        fs, fh = fold(span), fold(heading)
        # A span counts as present when it is one of the heading's tokens, or a
        # prefix of one (a possessive `וואלערשטיינס` is NOT a prefix of
        # `וואלערשטיין`, so it still flags -- which is what we want).
        present = any(t == fs or t.startswith(fs) for t in fh.split())
        missing = bool(span) and bool(heading) and not present
        if not (low or missing):
            continue
        flag = ("span-not-in-heading+low-tier" if (missing and low)
                else "span-not-in-heading" if missing else "low-tier")
        rows.append({
            "db_id": d["db_id"], "org_name": d.get("org_name_yiddish", ""),
            "span": span, "person_heading": heading, "hub_id": d["hub_id"],
            "tier": d["tier"], "pattern": d["pattern"],
            "reviewer": d["reviewer"], "flag": flag, "notes": "",
        })

    from collections import Counter
    print(f"decisions {len(dec)}  ->  spot-check rows {len(rows)}")
    print("  flag:     ", dict(Counter(r["flag"] for r in rows)))
    print("  reviewer: ", dict(Counter(r["reviewer"] for r in rows)))
    strong = [r for r in rows if "span-not-in-heading" in r["flag"]]
    print(f"  span-not-in-heading (the stronger signal): {len(strong)}")

    if not a.write:
        print("\n(report only -- pass --write to rewrite the queue)")
        return 0

    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(rows)
    print(f"\nwrote {OUT.name} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
