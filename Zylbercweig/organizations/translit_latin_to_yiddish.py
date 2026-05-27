"""Fill core_db.tsv[name_yiddish_translit] from Latin `name`.

This is a BLOCKING-ONLY auxiliary column. It exists so that Latin-only DB
rows surface as candidates against Yiddish clusters in prepare_alignment.py.
It MUST NOT be used as canonical Yiddish anywhere — name_yiddish stays the
sole source of truth for the canonical Yiddish form, populated only by
RA-confirmed alignments (via backfill_name_yiddish_from_alignments.py).

Idempotent: only fills empty cells; never overwrites.

Usage:
    .venv/bin/python3 Zylbercweig/organizations/translit_latin_to_yiddish.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent
CORE_DB_TSV = HERE / "core_db.tsv"

_HEBREW_RE = re.compile(r"[֐-׿]")

# Latin digraphs → Yiddish (longest first; applied before single-char map).
_DIGRAPHS = [
    ("tsch", "טש"),
    ("dzh", "דזש"),
    ("sch", "ש"),
    ("sh", "ש"),
    # Polish digraphs common in Galician/Polish-sourced names (e.g. Fiszon,
    # Goldfaden's Polish-spelled troupes). Placed before single-char fallback
    # so 'sz' doesn't degrade to ס+ז etc.
    ("szcz", "שטש"),
    ("sz", "ש"),
    ("cz", "טש"),
    ("rz", "זש"),
    ("ch", "טש"),
    ("tz", "צ"),
    ("ts", "צ"),
    ("th", "ט"),
    ("ph", "פֿ"),
    ("ck", "ק"),
    ("kh", "כ"),
    ("gh", "ג"),
    ("zh", "זש"),
    ("dz", "דז"),
    ("ai", "יי"),
    ("ei", "יי"),
    ("ey", "יי"),
    ("ay", "יי"),
    ("au", "אַו"),
    ("ou", "או"),
    ("ow", "אָו"),
    ("oo", "ו"),
    ("ee", "י"),
    ("oi", "וי"),
    ("oy", "וי"),
    ("uy", "וי"),
    ("ie", "י"),
    ("ea", "י"),
    ("io", "יאָ"),
    ("ia", "יאַ"),
    ("ue", "יו"),
    ("eu", "יי"),
    ("qu", "קוו"),
    ("x", "קס"),
    ("j", "דזש"),  # English j; for German-origin words 'j' would be 'י' but we have no language tag
    ("c", "ק"),    # default c → k (Latin words from Anglo/German sources)
]

_SINGLE = {
    "a": "אַ",
    "b": "ב",
    "d": "ד",
    "e": "ע",
    "f": "פֿ",
    "g": "ג",
    "h": "ה",
    "i": "י",
    "k": "ק",
    "l": "ל",
    "m": "מ",
    "n": "נ",
    "o": "אָ",
    "p": "פּ",
    "q": "ק",
    "r": "ר",
    "s": "ס",
    "t": "ט",
    "u": "ו",
    "v": "וו",
    "w": "וו",
    "y": "י",
    "z": "ז",
    "'": "",
    "-": "-",
    " ": " ",
}

# Final-form substitutions for word-final consonants.
_FINALS = {"מ": "ם", "נ": "ן", "כ": "ך", "פ": "ף", "צ": "ץ"}


def _has_hebrew(s: str) -> bool:
    return bool(_HEBREW_RE.search(s or ""))


# Word-level substitutions for high-frequency loanwords whose canonical Yiddish
# spelling deviates from naive char/digraph mapping. Applied before letter-level
# transliteration to dramatically improve blocking quality for theatre records.
_WORD_LEXICON = {
    "theatre": "טעאַטער",
    "theater": "טעאַטער",
    "opera": "אָפּערע",
    "company": "קאָמפּאַניע",
    "troupe": "טרופּע",
    "club": "קלוב",
    "society": "געזעלשאַפֿט",
    "library": "ביבליאָטעק",
    "institute": "אינסטיטוט",
    "union": "יוניאָן",
    "actors": "אַקטיאָרן",
    "jewish": "ייִדיש",
    "hebrew": "העברעיש",
    "yiddish": "ייִדיש",
    "the": "די",
    "of": "פֿון",
    "and": "און",
    "new": "ניו",
    "york": "יאָרק",
    "national": "נאַציאָנאַל",
    "international": "אינטערנאַציאָנאַל",
    "folk": "פֿאָלקס",
    "people's": "פֿאָלקס",
    "peoples": "פֿאָלקס",
    "dramatic": "דראַמאַטיש",
    "drama": "דראַמע",
    "art": "קונסט",
}


def _translit_word(word: str) -> str:
    lw = word.lower()
    if lw in _WORD_LEXICON:
        return _WORD_LEXICON[lw]
    s = lw
    out: list[str] = []
    i = 0
    while i < len(s):
        hit = False
        for dg, rep in _DIGRAPHS:
            if s.startswith(dg, i):
                out.append(rep)
                i += len(dg)
                hit = True
                break
        if hit:
            continue
        ch = s[i]
        out.append(_SINGLE.get(ch, ""))
        i += 1
    result = "".join(out)
    # Apply final-form substitution on the last Yiddish letter, if applicable.
    if result and result[-1] in _FINALS:
        result = result[:-1] + _FINALS[result[-1]]
    return result


def translit_latin_to_yiddish(text: str) -> str:
    """Coarse Latin → Yiddish-letter transliteration for blocking only."""
    if not text:
        return ""
    # Word-by-word so final-form rules apply per token.
    parts = re.split(r"(\s+|[-/])", text)
    out = []
    for p in parts:
        if not p or p.isspace() or p in "-/":
            out.append(p)
            continue
        # Strip non-letters around the token
        m = re.match(r"^([^a-zA-Z']*)([a-zA-Z'-]+)(.*)$", p)
        if not m:
            out.append(p)
            continue
        pre, core, post = m.groups()
        out.append(pre + _translit_word(core) + post)
    return "".join(out).strip()


def _read_tsv(p: Path) -> tuple[list[str], list[dict[str, str]]]:
    with open(p, encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        return list(r.fieldnames or []), list(r)


def _write_tsv(p: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Regenerate name_yiddish_translit even when already populated.",
    )
    args = ap.parse_args()

    fields, rows = _read_tsv(CORE_DB_TSV)
    if "name_yiddish_translit" not in fields:
        # Insert right after name_yiddish (or at the end as fallback).
        if "name_yiddish" in fields:
            idx = fields.index("name_yiddish") + 1
            fields = fields[:idx] + ["name_yiddish_translit"] + fields[idx:]
        else:
            fields.append("name_yiddish_translit")

    filled = 0
    skipped_has_yid = 0
    skipped_has_translit = 0
    skipped_no_latin = 0
    for r in rows:
        if (r.get("name_yiddish_translit") or "").strip() and not args.overwrite:
            skipped_has_translit += 1
            continue
        if (r.get("name_yiddish") or "").strip():
            # Already has a real canonical; no need for a translit.
            skipped_has_yid += 1
            r.setdefault("name_yiddish_translit", "")
            continue
        name = (r.get("name") or "").strip()
        if not name or _has_hebrew(name) or not re.search(r"[A-Za-z]", name):
            skipped_no_latin += 1
            r.setdefault("name_yiddish_translit", "")
            continue
        translit = translit_latin_to_yiddish(name)
        r["name_yiddish_translit"] = translit
        if translit:
            filled += 1

    print(
        f"name_yiddish_translit: filled {filled}, "
        f"skipped {skipped_has_yid} (already has name_yiddish), "
        f"{skipped_has_translit} (already has translit), "
        f"{skipped_no_latin} (no Latin name)."
    )
    if args.dry_run:
        # Show a few examples.
        shown = 0
        for r in rows:
            if (r.get("name_yiddish_translit") or "").strip() and not (r.get("name_yiddish") or "").strip():
                print(f"  {r.get('db_id'):>5}  {r.get('name'):40s}  →  {r['name_yiddish_translit']}")
                shown += 1
                if shown >= 15:
                    break
        return
    _write_tsv(CORE_DB_TSV, fields, rows)
    print(f"Wrote {CORE_DB_TSV}")


if __name__ == "__main__":
    main()
