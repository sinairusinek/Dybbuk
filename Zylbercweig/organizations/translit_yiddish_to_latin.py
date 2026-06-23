"""Coarse Yiddish (Hebrew-script) → YIVO Latin transliteration.

Purpose: produce a readability scaffold for non-Yiddish-reading RAs (e.g. Arne)
working on Zylbercweig org audits. Not authoritative — loshn-koydesh words and
some digraph edge cases will be approximate.

Not for use anywhere a canonical Latin form is required.

Usage:
    from translit_yiddish_to_latin import translit_yiddish_to_latin
    s = translit_yiddish_to_latin("פּראָגער שטאָט-טעאַטער")  # → "proger shtot-teater"
"""
from __future__ import annotations

import re
import unicodedata

_HEBREW_RE = re.compile(r"[֐-׿]")

# Digraphs / composed forms (longest first). Operate on NFC-normalized input.
_DIGRAPHS: list[tuple[str, str]] = [
    ("ייִ", "yi"),
    ("ײַ", "ay"),
    ("יי", "ey"),
    ("ױ", "oy"),
    ("וי", "oy"),
    ("װ", "v"),
    ("וו", "v"),
    ("וּ", "u"),
    ("זש", "zh"),
    ("טש", "tsh"),
    ("דזש", "dzh"),
    ("דז", "dz"),
    ("שטש", "shtsh"),
    ("שׁ", "sh"),
    ("שׂ", "s"),
    ("אַ", "a"),
    ("אָ", "o"),
    ("אֵ", "e"),
    ("אִ", "i"),
    ("בֿ", "v"),
    ("בּ", "b"),
    ("כּ", "k"),
    ("פּ", "p"),
    ("פֿ", "f"),
    ("תּ", "t"),
]

_SINGLE: dict[str, str] = {
    "א": "",       # bare alef: usually silent in NFC-normalized YIVO orthography
    "ב": "b",
    "ג": "g",
    "ד": "d",
    "ה": "h",
    "ו": "u",
    "ז": "z",
    "ח": "kh",
    "ט": "t",
    "י": "i",
    "כ": "kh",
    "ך": "kh",
    "ל": "l",
    "מ": "m",
    "ם": "m",
    "נ": "n",
    "ן": "n",
    "ס": "s",
    "ע": "e",
    "פ": "f",
    "ף": "f",
    "צ": "ts",
    "ץ": "ts",
    "ק": "k",
    "ר": "r",
    "ש": "sh",
    "ת": "s",       # loshn-koydesh fallback; not always right
    "־": "-",       # Hebrew maqaf
}

# Strip stray nikud that survived digraph matching.
_STRAY_NIKUD = re.compile(r"[֑-ֽֿ-ׇ]")


def _translit_word(word: str) -> str:
    s = unicodedata.normalize("NFC", word)
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
        if ch in _SINGLE:
            out.append(_SINGLE[ch])
        elif _STRAY_NIKUD.match(ch):
            pass
        else:
            out.append(ch)
        i += 1
    result = "".join(out)
    # Word-initial yud is consonantal 'y' only when followed by a vowel
    # (YIVO: יאָר → yor, ייִנגל → yingl). Before a consonant it stays 'i'
    # (אינסטיטוט → institut, not ynstitut).
    if result.startswith("i") and len(result) > 1 and result[1] in "aeou":
        result = "y" + result[1:]
    return result


def translit_yiddish_to_latin(text: str) -> str:
    """Coarse Yiddish → YIVO Latin. Returns input unchanged if no Hebrew chars."""
    if not text or not _HEBREW_RE.search(text):
        return text or ""
    parts = re.split(r"(\s+|[-/,;()\"'])", text)
    out = []
    for p in parts:
        if not p or p.isspace() or p in "-/,;()\"'":
            out.append(p)
            continue
        out.append(_translit_word(p))
    return "".join(out).strip()


def annotate(text: str) -> str:
    """Render `<yiddish> [<translit>]` for an RA-readable scaffold.

    Empty/Latin-only input returns unchanged.
    """
    if not text:
        return ""
    if not _HEBREW_RE.search(text):
        return text
    t = translit_yiddish_to_latin(text)
    return f"{text} [{t}]" if t else text


if __name__ == "__main__":
    import sys
    for line in sys.stdin:
        print(annotate(line.rstrip("\n")))
