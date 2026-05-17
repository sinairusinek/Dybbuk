"""Cross-script helpers for Yiddish/Hebrew and English names."""

from __future__ import annotations

import re
import unicodedata

from .distance import phonetic_similarity

_HEBREW_RE = re.compile(r"[\u0590-\u05FF]")
_LATIN_RE = re.compile(r"[A-Za-z]")

# Basic Yiddish/Hebrew orthography to approximate IPA-like Latin symbols.
# This is intentionally lightweight as the first implementation slice.
_REPLACEMENTS = [
    ("דזש", "dʒ"),
    ("טש", "tʃ"),
    ("זש", "ʒ"),
    ("ש", "ʃ"),
    ("צ", "ts"),
    ("יי", "ej"),
    ("וו", "v"),   # must come before ("וי", "oj") — וו is two vavs, וי is vav+yod
    ("וי", "oj"),
    ("אָ", "o"),
    ("אַ", "a"),
    ("ע", "e"),
]

_CHAR_MAP = {
    "א": "ʔ",
    "ב": "b",
    "בֿ": "v",
    "ג": "g",
    "ד": "d",
    "ה": "h",
    "ו": "u",
    "ז": "z",
    "ח": "x",
    "ט": "t",
    "י": "j",
    "כ": "k",
    "ך": "k",
    "ל": "l",
    "מ": "m",
    "ם": "m",
    "נ": "n",
    "ן": "n",
    "ס": "s",
    "ע": "e",
    "פ": "p",
    "ף": "f",
    "פֿ": "f",
    "צ": "ts",
    "ץ": "ts",
    "ק": "k",
    "ר": "r",
    "ש": "ʃ",
    "ת": "s",
    "תּ": "t",
}


def _strip_marks(text: str) -> str:
    nfd = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")


def _detect_script(text: str) -> str:
    if _HEBREW_RE.search(text or ""):
        return "hebrew"
    if _LATIN_RE.search(text or ""):
        return "latin"
    return "other"


def _yiddish_to_ipa(text: str) -> str:
    s = _strip_marks(text or "").strip().lower()
    if not s:
        return ""

    for old, new in _REPLACEMENTS:
        s = s.replace(old, new)

    out = []
    for ch in s:
        out.append(_CHAR_MAP.get(ch, ch))
    ipa = "".join(out)
    ipa = re.sub(r"\s+", " ", ipa).strip()
    return ipa


def _english_to_ipa(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return ""
    try:
        import epitran  # type: ignore

        epi = epitran.Epitran("eng-Latn")
        return epi.transliterate(s)
    except Exception:
        # Fallback: not true IPA, but keeps cross-script channel alive.
        return re.sub(r"[^a-z ]+", "", s.lower())


def ipa_to_dm_input(ipa: str) -> str:
    """Normalize an IPA string to ASCII-friendly form suitable for DM soundex input.

    Converts IPA special characters to Latin digraphs and removes non-phonemic
    markers (glottal stop from silent aleph).
    """
    s = ipa
    s = s.replace("tʃ", "tch").replace("dʒ", "dzh")
    s = s.replace("ʃ", "sh").replace("ʒ", "zh")
    s = s.replace("ʔ", "")   # silent aleph — not a real consonant in this context
    s = s.replace("ej", "ey").replace("oj", "oy")
    return s.strip()


def to_ipa_candidates(text: str) -> list[str]:
    script = _detect_script(text or "")
    if script == "hebrew":
        ipa = _yiddish_to_ipa(text)
        return [ipa] if ipa else []
    if script == "latin":
        ipa = _english_to_ipa(text)
        return [ipa] if ipa else []
    return []


def cross_script_similarity(name_a: str, name_b: str) -> float:
    """Best IPA similarity across generated candidates for two names."""
    cand_a = to_ipa_candidates(name_a)
    cand_b = to_ipa_candidates(name_b)
    if not cand_a or not cand_b:
        return 0.0

    best = 0.0
    for ipa_a in cand_a:
        for ipa_b in cand_b:
            sim = phonetic_similarity(ipa_a, ipa_b)
            if sim > best:
                best = sim
    return best
