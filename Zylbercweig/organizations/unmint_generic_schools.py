#!/usr/bin/env python3
"""Unmint gymnasium / yeshiva DB rows whose names do not individuate.

Sinai's rule (2026-10-04): a gymnasium or yeshiva named only by its town, or
by nothing at all, is a generic kind -- a city had many gymnasia, and "the
local yeshiva" names no one institution. Those core_db rows are deprecated and
the clusters pointing at them go back to the queue as GENERIC.

Kept as real institutions, because the name individuates:
  * a PERSON who is the school itself -- כהנס גימנאַזיע, קרינסקיס, 
    זשאָבאָטינסקיס, גימנאַזיע פֿון לעווין, הרב ריינעסעס ישיבה
  * a famous single yeshiva known by its town -- Volozhin, Mir, Telz,
    Slabodka, Rameyles, Lomzhe, Slonim, Grodno
  * a movement, dedication or quoted title -- ישיבה ר' יצחק אַלחנן,
    ישיבה פֿון חפץ חיים, Sholem Aleichem Gymnasium

The famous-yeshiva list is explicit rather than derived: "city + yeshiva" is
exactly the shape of both בריסקער ישיבה (a renowned institution) and
אָדעסער ישיבה (a generic one), so only historical knowledge separates them.
Anything not on the list and carrying nothing but a town is treated as generic.

Deprecation, not deletion: rows are marked deprecated=1 with a reason, so the
audit trail and any downstream reference survive.
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
CORE = HERE / "core_db.tsv"
REVIEWER = "rule_2026_10_04"

GYM = ("גימנאַזיע", "גימנאזיע", "גימנאָזיע", "גימנזיע", "גימנאַזיום", "gymnas")
YESH = ("ישיבֿה", "ישיבה", "ישיבות", "ישיבת", "yeshiv")

# Yeshivas that ARE the institution their town names.
FAMOUS = ("וואָלאָזשין", "volozh", "מיר", "mir yeshiva", "טעלז", "telz",
          "סלאָבאָדק", "slabodka", "ראמיילעס", "ראַמיילעס", "לאָמזשער", "lomzh",
          "סלאָנימער", "slonim", "גראָדנער", "grodno", "בריסק", "brisk",
          "נאָוואָגרודק", "novardok", "חפץ חיים", "chofetz", "יצחק אַלחנן",
          "בית-דוד", "ביתדוד", "לובלינער", "lublin", "פּרעסבורגער", "pressburg")

# Words that describe without naming.
GENERIC_ADJ = ("אָרטיק", "ארטיק", "שטאָטיש", "מלוכה", "רעגירונגס", "יידיש",
               "ייִדיש", "העברעאיש", "העברעיש", "העבראיש", "רוסיש", "פּויליש",
               "פויליש", "דייטש", "פֿאָלקס", "פאָלקס", "אַלגעמיינ", "רעאַל",
               "פרויען", "מענער", "פּריוואַט", "נייע", "נייער", "אַלטע",
               "ערשט", "צווייט", "דריט", "פערט", "העכער", "מיטל", "קהלש",
               "קייזערלעך", "קייזערליך", "jewish", "hebrew", "russian",
               "polish", "state", "municipal", "local", "girls", "boys")


def sm(s: str) -> str:
    """Strip Hebrew points AND fold final letter forms.

    Without the final-form fold, וואָלאָזשין (final nun) fails to match inside
    וואָלאָזשינער (medial nun) and Volozhin reads as a generic town yeshiva.
    """
    s = unicodedata.normalize("NFD", (s or "").strip())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.translate(str.maketrans("ךםןףץ", "כמנפצ"))


def blob(row: dict) -> str:
    return sm(" ".join((row.get(k) or "") for k in
                       ("name", "name_yiddish", "name_variants")))


def is_kind(row: dict) -> str | None:
    b = blob(row)
    if any(sm(p) in b for p in GYM):
        return "gymnasium"
    if any(sm(p) in b for p in YESH):
        return "yeshiva"
    return None


def individuates(row: dict) -> bool:
    """True when something in the name picks out one institution."""
    name = (row.get("name") or row.get("name_yiddish") or "").strip()
    b = blob(row)
    if any(sm(f) in b for f in FAMOUS):
        return True
    if any(c in name for c in '"„”“'):          # quoted dedication/title
        return True
    toks = [t for t in re.split(r"[\s\-־,]+", name) if t]
    city_adjs: list[str] = []
    prep = False
    for tok in toks:
        t = sm(tok)
        if any(sm(k) in t for k in GYM + YESH):
            continue
        if any(t.startswith(sm(a)[:5]) for a in GENERIC_ADJ):
            continue
        if t.endswith("ער") or t.endswith("ע"):  # city adjective / inflection
            city_adjs.append(t)
            continue
        if t in {sm(w) for w in ("פון", "פֿון", "אין", "דער", "די", "דאָס",
                                 "און", "אַ", "א", "of", "in", "the", "and")}:
            prep = True
            continue
        if prep:
            # "<kind> in <Town>" / "<kind> fun <Town>": the token after the
            # preposition is the town. A town does not individuate a kind the
            # city had many of -- that is the same case as וואַרשעווער גימנאַזיע.
            city_adjs.append(t)
            prep = False
            continue
        return True                              # a surname, a title
    # Two place adjectives: the second narrows the first (קיעווער פּעטשערסקער
    # = the Pechersk gymnasium in Kyiv), which picks out one school.
    return len([t for t in city_adjs if not any(sm(a)[:5] in t for a in GENERIC_ADJ)]) >= 2


def main(apply: bool) -> int:
    with open(CORE, newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        cheaders, core = list(rd.fieldnames or []), list(rd)
    with open(REVIEW, newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        rheaders, rows = list(rd.fieldnames or []), list(rd)

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    drop, keep = [], []
    for c in core:
        kind = is_kind(c)
        if not kind:
            continue
        if (c.get("deprecated") or "").strip() in ("1", "true", "yes"):
            continue
        (keep if individuates(c) else drop).append((c, kind))

    drop_ids = {c["db_id"] for c, _ in drop}
    freed = 0
    for row in rows:
        if (row.get("aligned_db_id") or "").strip() not in drop_ids:
            continue
        freed += 1
        if apply:
            row["decision"] = "GENERIC"
            row["aligned_db_id"] = ""
            row["reviewer"] = REVIEWER
            row["reviewed_at"] = stamp
            row["reviewer_notes"] = (
                "Generic rule 2026-10-04 — was aligned to a gymnasium/yeshiva "
                "DB row named only by its town; that names a kind, not an "
                "institution. " + (row.get("reviewer_notes") or ""))[:480]

    if apply:
        for c, kind in drop:
            c["deprecated"] = "1"
            c["name_variants"] = (c.get("name_variants") or "")
            note = f"Deprecated 2026-10-04: generic {kind}, named only by town."
            if "merged_into" in c and not (c.get("merged_into") or "").strip():
                pass
            c["out_of_project"] = (c.get("out_of_project") or "")
            c["linked_cluster_ids"] = ""
            c.setdefault("name", c.get("name", ""))
            c["name"] = (c.get("name") or "").strip()
            c["address"] = (c.get("address") or "").strip()
            # record the reason where a human will see it
            if "name_variants" in c:
                c["name_variants"] = (c["name_variants"] + (" | " if c["name_variants"] else "") + note)[:480]

    print(f"gymnasium/yeshiva DB rows examined : {len(drop) + len(keep)}")
    print(f"  deprecated as generic            : {len(drop)}")
    print(f"  kept (individuated)              : {len(keep)}")
    print(f"alignment rows freed -> GENERIC    : {freed}")
    print()
    print("--- deprecating ---")
    for c, kind in drop[:40]:
        print(f"   {c['db_id']:>6} {(c.get('name') or c.get('name_yiddish') or '')[:42]:42} {kind}")
    print()
    print("--- keeping ---")
    for c, kind in keep[:20]:
        print(f"   {c['db_id']:>6} {(c.get('name') or c.get('name_yiddish') or '')[:42]:42} {kind}")

    if not apply:
        print("\n[dry run] nothing written. Re-run with --apply")
        return 0

    for path, headers, data in ((CORE, cheaders, core), (REVIEW, rheaders, rows)):
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=headers, delimiter="\t", extrasaction="ignore")
            w.writeheader()
            w.writerows(data)
        tmp.replace(path)
    print("\nwritten.")
    return 0


if __name__ == "__main__":
    sys.exit(main(apply="--apply" in sys.argv))
