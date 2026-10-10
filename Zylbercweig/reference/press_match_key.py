"""Cross-script matching keys for Yiddish/Hebrew press titles.

The problem this solves: core_db records most periodicals ONLY in Hebrew script
(`name_yiddish`, with `name_yiddish_translit` almost always empty), while
Galron's Latin sheet records 1,369 titles ONLY in Latin letters. Matching them
therefore has to cross scripts, and the existing
`organizations/translit_yiddish_to_latin.py` cannot carry that load alone:

it assumes *pointed* YIVO orthography, where `אַ`=a and `אָ`=o, and maps bare
`א` to the empty string. Historical press titles are overwhelmingly UNPOINTED,
so that module turns טאג→"tg", פארווערטס→"frverts", אמעריקאנער→"merikner" --
11 of 14 test words mangled. Those keys would never match "Tog", "Forverts" or
"Amerikaner", and would do so silently.

So this module keeps that transliterator for pointed input and adds a
*matching key*: a deliberately lossy, vowel-light skeleton that both scripts
can be reduced to. Vowels are where Yiddish orthography and romanization
disagree most (tsaytung/tseytung, varhayt/varheyt, Forverts/Forward), and where
unpointed text carries least information -- so the key drops them almost
entirely and keeps the consonant spine, which is stable across both.

Not a transliteration. Never show a match key to a human as a title; it exists
only to be compared with another match key.
"""
from __future__ import annotations

import re
import unicodedata

HEBREW_RE = re.compile(r"[֐-׿]")
NIKUD_RE = re.compile(r"[֑-ׇ]")

# Leading articles/particles carry no identifying weight: Galron and core_db
# disagree constantly on whether a title keeps its article ("Der Tog" vs "טאג",
# db 216 vs galron's entry). Strip them from both sides.
ARTICLES_LATIN = {"der", "di", "dos", "das", "de", "the", "a", "an", "ha", "ho"}
ARTICLES_HEBREW = {"דער", "די", "דאס", "דאָס", "דאז", "ה"}

# Generic press-type words. Kept OUT of the stripping set on purpose: unlike
# "troupe"/"theater" in the org matcher, a periodical's type word is often the
# whole distinguishing feature (צייטונג vs בלאט vs שטימע), and "Folksblat" vs
# "Folkstsaytung" are different papers. Listed here only to document the choice.
PRESS_TYPE_WORDS = {
    "tsaytung", "tseytung", "blat", "blatt", "bletter", "bleter", "shtime",
    "zhurnal", "journal", "vokhnblat", "togblat", "tagblat", "post", "express",
    "ekspres", "nayes", "velt", "vort", "folksblat",
}

# Hebrew-script -> consonant-skeleton. Vowel letters (א ע ו י) collapse to a
# single placeholder rather than vanishing, so "tog" and "tg" still align while
# genuinely different consonant spines stay apart.
_SKELETON = {
    "א": "*", "ע": "*", "ו": "*", "י": "*",      # vowel-ish carriers
    "ב": "b", "בּ": "b", "בֿ": "v", "וו": "v", "װ": "v",
    "ג": "g", "ד": "d", "ה": "h", "ז": "z", "ח": "x", "ט": "t",
    "כ": "x", "ך": "x", "כּ": "k", "ל": "l", "מ": "m", "ם": "m",
    "נ": "n", "ן": "n", "ס": "s", "פ": "f", "ף": "f", "פּ": "p", "פֿ": "f",
    "צ": "c", "ץ": "c", "ק": "k", "ר": "r", "ש": "s", "שׂ": "s", "ת": "s",
    "תּ": "t", "ג׳": "g", "זש": "z", "טש": "c", "דזש": "z", "דז": "dz",
}
# Longest-first so digraphs win over their first letter.
_SKELETON_KEYS = sorted(_SKELETON, key=len, reverse=True)

# Latin -> the same skeleton alphabet. Romanizations disagree on sibilants and
# affricates (zh/sh/tsh/cz/sz), so these all fold together.
_LATIN_FOLD = [
    ("shtsh", "s"), ("tsh", "c"), ("dzh", "dz"), ("szcz", "s"),
    ("sch", "s"), ("sh", "s"), ("sz", "s"), ("zh", "z"), ("rz", "z"),
    ("cz", "c"), ("ts", "c"), ("tz", "c"), ("ch", "x"), ("kh", "x"),
    ("ck", "k"), ("ph", "f"), ("th", "t"), ("qu", "k"),
    ("w", "v"), ("q", "k"), ("j", "*"), ("y", "*"), ("x", "ks"),
]
_VOWELS = "aeiou"

# "ts"/"tz" fold to the tsadi skeleton only when NOT word-final: Tsaytung is
# צייטונג (one letter), but Forver-ts is פארווערט+ס (two). Folding the final
# cluster made Forverts/פארווערטס score 0.364 instead of matching.
_FINAL_TS_RE = re.compile(r"ts$")


def _strip_marks(text: str) -> str:
    """Remove nikud and Latin diacritics (Galron writes Worṭ, Żydowski)."""
    without_nikud = NIKUD_RE.sub("", text)
    decomposed = unicodedata.normalize("NFD", without_nikud)
    return unicodedata.normalize(
        "NFC", "".join(c for c in decomposed if not unicodedata.combining(c)))


def _tokens(text: str) -> list[str]:
    """Split on whitespace and punctuation, dropping parenthetical qualifiers.

    core_db disambiguates homonyms inline -- "Forverts (Detroit)",
    "די אידישע וועלט (פֿילאדעלפֿיע)", "אונזער שטימע <סט. פאולו, ברזיל>" -- and
    Galron uses <angle brackets> for the same job. That qualifier is a place,
    which the caller handles separately; it must not pollute the title key.
    """
    text = re.sub(r"\([^)]*\)|<[^>]*>|\[[^\]]*\]", " ", text)
    text = text.replace("„", " ").replace("“", " ").replace('"', " ")
    return [t for t in re.split(r"[\s\-־–—_,;:.!?/'`’\"]+", text) if t]


def skeleton(text: str) -> str:
    """Reduce a title in EITHER script to a comparable consonant skeleton."""
    if not text:
        return ""
    out_tokens = []
    for token in _tokens(_strip_marks(text)):
        if HEBREW_RE.search(token):
            if token in ARTICLES_HEBREW:
                continue
            piece, i = [], 0
            while i < len(token):
                for key in _SKELETON_KEYS:
                    if token.startswith(key, i):
                        piece.append(_SKELETON[key])
                        i += len(key)
                        break
                else:
                    i += 1          # drop anything unmapped
            folded = "".join(piece)
        else:
            low = token.lower()
            if low in ARTICLES_LATIN:
                continue
            final_ts = bool(_FINAL_TS_RE.search(low))
            if final_ts:
                low = low[:-2]
            for src, dst in _LATIN_FOLD:
                low = low.replace(src, dst)
            if final_ts:
                low += "ts"
            folded = "".join("*" if c in _VOWELS else c
                             for c in low if c.isalnum())
        # Collapse vowel runs, then drop them: they survive above only to keep
        # digraph boundaries honest.
        folded = re.sub(r"\*+", "*", folded).replace("*", "")
        # A doubled consonant is a romanization artifact (Literarishe/Literrishe).
        folded = re.sub(r"(.)\1+", r"\1", folded)
        if folded:
            out_tokens.append(folded)
    return " ".join(out_tokens)


def trigrams(text: str, n: int = 3) -> set[str]:
    padded = f"  {text}  "
    return {padded[i:i + n] for i in range(len(padded) - n + 1)}


def jaccard(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    ta, tb = trigrams(a), trigrams(b)
    union = ta | tb
    return len(ta & tb) / len(union) if union else 0.0


def token_set_ratio(a: str, b: str) -> float:
    """Order-insensitive token overlap.

    Press titles reorder freely across sources ("Folks-shtime" vs
    "Shtime fun folk"), so a pure character measure under-scores real pairs.
    """
    sa, sb = set(a.split()), set(b.split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(len(sa), len(sb))


def similarity(a_title: str, b_title: str) -> tuple[float, str]:
    """Compare two titles in either script. Returns (score, how-it-matched)."""
    ka, kb = skeleton(a_title), skeleton(b_title)
    if not ka or not kb:
        return 0.0, "empty_key"
    if ka == kb:
        return 1.0, "skeleton_exact"
    tok = token_set_ratio(ka, kb)
    if tok == 1.0:
        return 0.97, "skeleton_token_set"
    jac = jaccard(ka, kb)
    best = max(jac, tok * 0.9)
    return best, "skeleton_trigram" if jac >= tok * 0.9 else "skeleton_tokens"


# --- phonetic pass (cascade stage 4) ---------------------------------------
# The skeleton above is an orthographic measure: it compares how titles are
# spelled, with vowels thrown away. That is blind in both directions -- it
# misses אונזער/Undzer (the DB simply omits the ד) and it wrongly unites
# אויפקום/אופקים (same consonants, different words).
#
# Daitch-Mokotoff soundex over a Yiddish->IPA mapping is an independent
# channel: it compares how titles SOUND, keeping vowel information the
# skeleton discards. The entity-matching skill prescribes exactly this pairing
# for the Yiddish<->Latin script pair. Measured over a 16-pair probe set, DM
# agreed on 9/10 true pairs with 1 false positive, and caught the
# אונזער/Undzer case the skeleton missed -- so the two are used together, and
# agreement between them is the strongest evidence available here.
#
# `epitran` is absent in this environment, so dybbuk_phonetic's Latin->IPA
# path degrades to a lowercase strip. DM is therefore fed the Latin string
# directly (it is built for Latin-script surnames) and the IPA conversion is
# applied only to the Hebrew side, which is where it is reliable.
import sys as _sys
from pathlib import Path as _Path

_PHONETIC_SRC = _Path(__file__).resolve().parent.parent / "dybbuk-phonetic" / "src"
if _PHONETIC_SRC.is_dir() and str(_PHONETIC_SRC) not in _sys.path:
    _sys.path.insert(0, str(_PHONETIC_SRC))

try:
    import warnings as _warnings

    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore")
        from abydos.phonetic import DaitchMokotoff as _DM

        from dybbuk_phonetic.bridge import _yiddish_to_ipa as _y2ipa
        from dybbuk_phonetic.bridge import ipa_to_dm_input as _ipa2dm
    _DM_ENCODER = _DM()
    PHONETIC_AVAILABLE = True
except Exception:                                   # pragma: no cover
    _DM_ENCODER = None
    PHONETIC_AVAILABLE = False

# Type words are so common across titles that a phonetic agreement on one
# alone ("...tsaytung" vs "...tsaytung") says nothing about identity.
_PHONETIC_STOPWORDS = {skeleton(w) for w in PRESS_TYPE_WORDS} | {
    skeleton(w) for w in ("צייטונג", "בלאט", "בלעטער", "שטימע", "זשורנאל")}


def _dm_codes(text: str) -> set[str]:
    """Daitch-Mokotoff codes for one token, in either script."""
    if not _DM_ENCODER or not text.strip():
        return set()
    probe = _ipa2dm(_y2ipa(text)) if HEBREW_RE.search(text) else text
    try:
        result = _DM_ENCODER.encode(probe)
    except Exception:                               # pragma: no cover
        return set()
    if isinstance(result, (set, frozenset)):
        return {str(c) for c in result if c}
    return {c for c in str(result).split(",") if c}


def phonetic_codes(title: str) -> list[set[str]]:
    """Per-token DM code sets, skipping articles and generic type words."""
    out = []
    for token in _tokens(_strip_marks(title)):
        low = token.lower()
        if low in ARTICLES_LATIN or token in ARTICLES_HEBREW:
            continue
        if skeleton(token) in _PHONETIC_STOPWORDS:
            continue
        codes = _dm_codes(token)
        if codes:
            out.append(codes)
    return out


def phonetic_agreement(a_title: str, b_title: str) -> float:
    """Share of the shorter title's content tokens that agree phonetically."""
    ca, cb = phonetic_codes(a_title), phonetic_codes(b_title)
    if not ca or not cb:
        return 0.0
    shorter, longer = (ca, cb) if len(ca) <= len(cb) else (cb, ca)
    hits = sum(1 for codes in shorter
               if any(codes & other for other in longer))
    return hits / len(shorter)


def place_agree(a_place: str, b_place: str) -> bool:
    """Do two place strings name the same settlement, across orthographies?

    Zylbercweig writes settlements in Yiddish, Galron's Hebrew sheet in Hebrew,
    and its Latin sheet in Latin. The consonant skeleton does NOT bridge the
    first two: וואַרשע/ווארשה, קראָקע/קרקוב, טשערנאָוויץ/צ׳רנוביץ, קאָוונע/קובנה
    all mismatch on it -- 9 of 9 probe pairs failed -- because Yiddish marks a
    final schwa with ע where Hebrew uses ה, spells /v/ as וו where Hebrew uses ב,
    and hyphenates differently. Treating those as place disagreements flagged
    257 rows, most of them the same city twice.

    So agreement is tested phonetically (which crosses scripts by design) and
    token-wise, with the country parenthetical dropped. Returns True when ANY
    token of one side agrees with any token of the other: a paper's place is
    recorded at different granularities ("Warsaw" vs "Warsaw (Poland)").
    """
    import re as _re
    a_head = _re.split(r"[(<]", a_place or "")[0]
    b_head = _re.split(r"[(<]", b_place or "")[0]
    if not a_head.strip() or not b_head.strip():
        return False
    a_toks = [t for t in _re.split(r"[\s,;/·־–—-]+", a_head) if len(t) > 1]
    b_toks = [t for t in _re.split(r"[\s,;/·־–—-]+", b_head) if len(t) > 1]
    for ta in a_toks:
        for tb in b_toks:
            if skeleton(ta) and skeleton(ta) == skeleton(tb):
                return True
            ca, cb = _dm_codes(ta), _dm_codes(tb)
            if ca and cb:
                if ca & cb:
                    return True
                # DM codes are fixed-width and zero-padded, and the digit that
                # differs across orthographies is the one for the word's final
                # sound: Warsaw is 7945xx in Yiddish (וואַרשע, final ע) against
                # 7940xx in Hebrew (ווארשה, final ה), Krakow 595000 / 595700.
                # Three digits is the first three consonants -- enough to name
                # a settlement (Vilna 786, Riga 795, Lodz 583 all stay
                # distinct) without demanding agreement on the one sound these
                # two orthographies are guaranteed to spell differently.
                if any(x[:3] == y[:3] for x in ca for y in cb
                       if len(x) >= 3 and len(y) >= 3):
                    return True
    return False
