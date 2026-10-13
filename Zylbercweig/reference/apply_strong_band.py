#!/usr/bin/env python3
"""Round 2: decide the 95 never-reviewed STRONG band rows.

Every STRONG row is `skeleton_exact` with phonetic 1.00, no qualifier clash and
no place conflict, matched AFTER both matcher defects were fixed (language is
consulted; a publisher/printer matching a newspaper demotes).

Three groups, per Sinai 2026-10-13:

1. CONFIRM the rows whose titles agree once articles, vocalization/diacritics
   and final-letter forms are folded, and whose dates do not conflict. Same
   precedent as round 1's A1 blanket accept.

2. CONFIRM four variant-spelling pairs that the fold could not reach, each
   verified individually (see VARIANTS).

3. REJECT two demonstrable matcher false positives where the SKELETON collapsed
   distinct titles (see FALSE_POSITIVES), and park the six date-gap rows at
   RESEARCH_FURTHER (see DATE_GAPS) -- the title matches but the years are far
   enough apart that it may be a revival or an earlier run under a reused name,
   which needs a source, not a guess.

Writes `decision`, `reviewer`, `note` in galron_press_match_review.tsv and
appends the same decisions to galron_decisions_round1.tsv so the ledger stays
the authoritative record. Idempotent: a row already decided is left alone.
Line endings preserved per file.

Run with --execute to write; default is a dry run.
"""
from __future__ import annotations

import csv
import difflib
import io
import sys
import unicodedata
from pathlib import Path

HERE = Path(__file__).resolve().parent
REVIEW = HERE / "galron_press_match_review.tsv"
LEDGER = HERE / "galron_decisions_round1.tsv"
REVIEWER = "Sinai"

# Verified one by one: the same paper under a variant spelling or script.
VARIANTS = {
    "ORG-C00065_Q01": "Jidisze Cajtung = די אידישע צייטונג, Landsberg DP camp 1947-48, "
                       "Galron's title is the Latin-script form (Polish orthography). Same "
                       "case as ORG-C07330, closed CONFIRM in round 1.",
    "db1830":         "Jidisze Cajtung = אידישע צייטונג, Landsberg DP camp 1947-48; "
                       "Latin-script form of the same title.",
    "ORG-C04694":     "הצפה / הצופה — the same Kovno 1922 Hebrew paper, defective vs full "
                       "spelling of Ha-Tsofe. Galron's place and year agree.",
    "ORG-C01216":     "דער קיביצער / דער קיבּעצער — the same New York humour weekly "
                       "(Der Kibitzer), differing only in the dagesh and the tsere.",
    "ORG-C01708":     "מאָרגן פרעהייט / מאָרגן פרייהייט — Morgn Frayhayt, New York; "
                       "פרעהייט is the older orthography of פרייהייט.",
    "ORG-C04493":     "גערעכטיגקייט / גערעכטיגקייט (Justice) — the same ILGWU organ; "
                       "Galron appends the English title as a gloss. db 1921-05 sits "
                       "inside Galron's 1919-1932 run (span_check=overlap).",
    "ORG-C05495":     "„השלח“ / השלוח — two spellings of Ha-Shiloah, the Hebrew literary "
                       "monthly; db 1903 sits inside Galron's 1896-1926 run "
                       "(span_check=overlap).",
}

# The skeleton collapsed two genuinely different publications.
FALSE_POSITIVES = {
    "ORG-C01895": "MATCHER FALSE POSITIVE. התקופה and השקפה both skeletonise to 'hskfh' "
                  "(ת and ש reduce alike), but they are different journals: Ha-Tekufah is "
                  "the Warsaw/Berlin literary annual, while Galron's row is Ha-Hashkafah, "
                  "Jerusalem 1896-1908 — which its own place and years confirm.",
    "ORG-C05271": "MATCHER FALSE POSITIVE. באָברויסקער and בריסקער both skeletonise to "
                  "'brskr' (the vowels and the ב/ו of Bobr- reduce away), but these are "
                  "different towns: Bobruisk vs Brisk/Brest. Galron's place is בריסק "
                  "(בלארוס) and its years 1928-1930 against the db's 1912.",
}

# Title matches; the years are far enough apart to need a source.
DATE_GAPS = {
    "ORG-C00305":     ("72y", "db 1938 vs Galron 2010-2022, Birobidzhan — almost certainly "
                              "the post-Soviet revival of the name, not the paper Zylbercweig indexed."),
    "ORG-C01198":     ("36y", "db 1957 vs Galron 1993-1994, Mexico City."),
    "ORG-C02304":     ("14y", "db 1886 vs Galron 1862-1872, Odessa — Kol Mevaser's own run "
                              "ended 1871, so the db date may belong to a different serial."),
    "ORG-C01916":     ("18y", "db 1923-1927 vs Galron 1905, Krakow — Frayhayt is a much-reused name."),
    "ORG-C00335_Q03": ("19y", "db 1903-01 vs Galron 1922, New York — Der Fraynd ran in "
                              "Petersburg from 1903; the New York 1922 paper may be distinct."),
    "ORG-C04139_Q01": ("11y", "db 1909 vs Galron 1920-1931, Bialystok."),
}
# Seven rows carry span_check=apart*; ORG-C05271 is a REJECT above, leaving these
# six. Any further apart* row is added programmatically rather than silently
# confirmed, so the group can never shrink without being noticed.

ART = {"דער", "די", "דאָס", "דאס", "the", "der", "di", "dos"}
FINALS = {"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"}


def read(path: Path):
    csv.field_size_limit(10 ** 9)
    raw = path.read_text(encoding="utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rd = csv.DictReader(io.StringIO(raw), delimiter="\t")
    return list(rd.fieldnames), list(rd), nl


def write(path: Path, headers, rows, nl):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=headers, delimiter="\t",
                       lineterminator=nl, quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    w.writerows(rows)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(buf.getvalue())


def fold(s: str) -> str:
    s = "".join(c for c in unicodedata.normalize("NFD", s) if not unicodedata.combining(c))
    s = s.replace("־", " ").replace("-", " ")
    s = "".join(c for c in s if c.isalnum() or c.isspace())
    toks = [t for t in s.split() if t.lower() not in ART]
    return " ".join("".join(FINALS.get(c, c) for c in t) for t in toks)


def key_of(r: dict) -> str:
    cid = (r.get("cluster_id") or "").strip()
    return cid if cid else f"db{(r.get('db_id') or '').strip()}"


def main() -> int:
    execute = "--execute" in sys.argv
    headers, rows, nl = read(REVIEW)
    lhead, ledger, lnl = read(LEDGER)

    strong = [r for r in rows if r["band"] == "STRONG" and not (r.get("decision") or "").strip()]
    print(f"STRONG rows still open: {len(strong)}")

    gaps = dict(DATE_GAPS)
    for r in strong:
        if (r.get("span_check") or "").startswith("apart") and key_of(r) not in gaps \
                and key_of(r) not in FALSE_POSITIVES:
            gaps[key_of(r)] = ((r["span_check"] or "").replace("apart", "") + "y",
                               f"db {r['db_dates'] or '?'} vs Galron {r['galron_years'] or '?'}")

    plan: list[tuple[dict, str, str]] = []
    for r in strong:
        k = key_of(r)
        if k in FALSE_POSITIVES:
            plan.append((r, "REJECT", FALSE_POSITIVES[k]))
        elif k in gaps:
            span, why = gaps[k]
            plan.append((r, "RESEARCH_FURTHER",
                         f"date gap {span}: {why} Title matches; needs a source before a verdict."))
        elif k in VARIANTS:
            plan.append((r, "CONFIRM", VARIANTS[k]))
        else:
            a = fold(r.get("matched_on") or r.get("db_name") or "")
            b = fold((r.get("galron_title") or "").split("<")[0])
            ratio = difflib.SequenceMatcher(None, a, b).ratio()
            if ratio >= 0.90:
                plan.append((r, "CONFIRM",
                             "round 2 STRONG blanket accept: skeleton_exact + phonetic 1.00, "
                             "titles agree after folding articles/diacritics/final letters, "
                             "no place or date conflict, matched after both matcher fixes."))
            else:
                plan.append((r, "", f"UNRULED (title fold {ratio:.2f}) — not covered by any group"))

    from collections import Counter
    print("plan:", dict(Counter(d for _, d, _ in plan)))
    unruled = [(key_of(r), n) for r, d, n in plan if not d]
    if unruled:
        print("\nERROR: rows not covered by any rule:")
        for k, n in unruled:
            print(f"   {k}: {n}")
        return 2

    new_ledger_rows = []
    for r, dec, note in plan:
        r["decision"] = dec
        r["reviewer"] = REVIEWER
        r["note"] = note
        new_ledger_rows.append({
            "stage": "cluster" if (r.get("cluster_id") or "").strip() else "db",
            "tier": "STRONG",
            "entity": key_of(r),
            "galron_title": r.get("galron_title", ""),
            "decision": dec,
            "reviewer": REVIEWER,
            "note": note,
        })

    print(f"\nreview rows decided after this: "
          f"{sum(1 for r in rows if (r.get('decision') or '').strip())} / {len(rows)}")
    print(f"ledger rows after this: {len(ledger) + len(new_ledger_rows)}")

    if not execute:
        print("\nDRY RUN — nothing written. Re-run with --execute.")
        return 0

    write(REVIEW, headers, rows, nl)
    ledger.extend({k: r.get(k, "") for k in lhead} for r in new_ledger_rows)
    write(LEDGER, lhead, ledger, lnl)
    print(f"\nWROTE {REVIEW.name} and {LEDGER.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
