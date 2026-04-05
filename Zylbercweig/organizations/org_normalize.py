#!/usr/bin/env python3
"""Shared Yiddish name normalization utilities.

Extracted from cluster_orgs.py and prepare_alignment.py to avoid duplication.
Used by cluster_orgs.py, prepare_alignment.py, and the Zalmen UI search components.
"""

import re
import unicodedata


def normalize_name(name: str) -> str:
    """NFD Unicode normalisation + strip combining marks + lowercase + collapse whitespace.

    Mirrors the kimatch.core.normalizers.normalize_name function so that the
    two implementations produce identical results even without kimatch installed.
    """
    if not name:
        return ""
    nfd = unicodedata.normalize("NFD", name)
    stripped = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", stripped).strip().lower()


# Yiddish-specific orthographic normalizations applied on top of normalize_name.
_YIDDISH_NORM = [
    (re.compile(r"וו"), "ו"),              # double-vov → single
    (re.compile(r"[\u05DA\u05DB]"), "כ"),  # final kaf / kaf → כ
    (re.compile(r"[\u05DF\u05E0]"), "נ"),  # final nun / nun → נ
    (re.compile(r"[\u05E3\u05E4]"), "פ"),  # final pe / pe → פ
    (re.compile(r"[\u05E5\u05E6]"), "צ"),  # final tsadi / tsadi → צ
    (re.compile(r"[\u05DD\u05DE]"), "מ"),  # final mem / mem → מ
]


def normalize_yiddish(name: str) -> str:
    """NFD strip-diacritics + Yiddish spelling normalizations (final forms, double-vov)."""
    s = normalize_name(name)
    for pat, repl in _YIDDISH_NORM:
        s = pat.sub(repl, s)
    return s


# ── Token sets (built at import time; values are post-normalize_yiddish forms) ─

# Generic organization type tokens that can be stripped to isolate the
# proper-name kernel for comparison.
_GENERIC_ORG_TOKENS = {normalize_yiddish(t) for t in {
    # Troupe / theatre variants (Yiddish + English)
    "טרופע", "טרופה", "טרופעע", "טרופּע",
    "טעאטער", "טעאַטער",
    "טעאטער-טרופע", "טעאַטער-טרופע",
    "troupe", "theatre", "theater",
    # Association / union
    "פאריין", "פֿאַריין",
    # Federation
    "פארבאנד", "פֿאַרבאַנד",
    # Society
    "געזעלשאפט", "געזעלשאַפֿט",
    # Committee
    "קאמיטעט", "קאָמיטעט",
    # School
    "שול",
    # Studio
    "סטודיע",
}}

# Prepositions that introduce a possessor/origin ("of", "from", "von").
_OF_TOKENS = {normalize_yiddish(t) for t in {
    "פון", "פֿון", "fun", "fon", "von", "of", "from",
}}

# Leading articles to strip before comparison.
_ARTICLES = {normalize_yiddish(t) for t in {
    "דער", "די", "דאָס", "דאס", "דעמ", "der", "di", "dos",
}}


def organization_name_aliases(name: str) -> set[str]:
    """Generate comparable aliases for an organization name.

    Handles patterns like:
    - 'X טרופע'            → 'X'   (suffix generic marker)
    - 'טרופע פון X'        → 'X'   (prefix generic marker + of-token)
    - "X'ס טרופע"          → 'X'   (possessive owner)
    - 'דער X'              → 'X'   (leading article)
    """
    base = normalize_yiddish(name)
    if not base:
        return set()

    aliases = {base}
    tokens = [t for t in re.split(r"\s+", base) if t]
    if not tokens:
        return aliases

    # Article stripping: "דער גרויסער טעאַטער" → "גרויסער טעאַטער"
    if len(tokens) >= 2 and tokens[0] in _ARTICLES:
        aliases.add(" ".join(tokens[1:]))

    # Suffix generic marker: "X troupe/theater" → "X"
    if len(tokens) >= 2 and tokens[-1] in _GENERIC_ORG_TOKENS:
        aliases.add(" ".join(tokens[:-1]).strip())

    # Prefix generic marker: "troupe/theater of X" → "X"
    if len(tokens) >= 3 and tokens[0] in _GENERIC_ORG_TOKENS and tokens[1] in _OF_TOKENS:
        aliases.add(" ".join(tokens[2:]).strip())

    # Possessive owner forms: "X'ס" / "X׳ס" → "X"
    expanded = set()
    for a in aliases:
        atoks = [t for t in re.split(r"\s+", a) if t]
        if not atoks:
            continue
        first = atoks[0]
        stripped = re.sub(r"[׳'](?:s|ס)$", "", first)
        if stripped and stripped != first:
            atoks[0] = stripped
            expanded.add(" ".join(atoks).strip())
    aliases |= expanded

    return {a for a in aliases if a}


# ── org_type canonicalization ─────────────────────────────────────────────────

_ORG_TYPE_MAP = {
    # Theatre variants → canonical "theatre"
    "theater":               "theatre",
    "theatre":               "theatre",
    "טעאטער":               "theatre",
    "טעאַטער":              "theatre",
    # Troupe variants → canonical "troupe"
    "troupe":                "troupe",
    "טרופע":                "troupe",
    "טרופּע":               "troupe",
    "טרופה":                "troupe",
    "טרופעע":               "troupe",
    "טעאטער-טרופע":         "troupe",
    "טעאַטער-טרופע":        "troupe",
    "טעאַטער-טרופּע":       "troupe",
}


def normalize_org_type(raw: str) -> str:
    """Map org_type spelling variants to canonical English form.
    Unknown types pass through lowered/stripped."""
    key = raw.strip().lower()
    return _ORG_TYPE_MAP.get(key, key)


# Map from normalized token → canonical org_type.
# Keys are normalize_yiddish() forms so lookup works after normalization.
_TOKEN_TO_TYPE: dict[str, str] = {}
for _tok, _typ in [
    ("טרופע", "troupe"), ("טרופּע", "troupe"), ("טרופה", "troupe"),
    ("טרופעע", "troupe"), ("troupe", "troupe"),
    ("טעאטער", "theatre"), ("טעאַטער", "theatre"),
    ("theatre", "theatre"), ("theater", "theatre"),
    ("טעאטער-טרופע", "troupe"), ("טעאַטער-טרופע", "troupe"),
]:
    _TOKEN_TO_TYPE[normalize_yiddish(_tok)] = _typ


def infer_org_type(name: str) -> str:
    """Guess org_type from name tokens. Returns canonical type or '' if unknown.
    Only used when org_type is empty — never overwrites an existing annotated type."""
    tokens = normalize_yiddish(name).split()
    for t in tokens:
        if t in _TOKEN_TO_TYPE:
            return _TOKEN_TO_TYPE[t]
        # Also check hyphen-joined compounds: "ליבערטי-טעאַטער" → check "טעאטער"
        for part in t.split("-"):
            if part in _TOKEN_TO_TYPE:
                return _TOKEN_TO_TYPE[part]
    return ""


# ── City-adjective metadata extraction ───────────────────────────────────────
# Maps normalized city-adjective prefix → normalized settlement name.
# Used ONLY for metadata enrichment; location adjectives are NOT stripped
# from org names for comparison purposes.
_CITY_ADJECTIVE_MAP = {
    normalize_yiddish("ניו-יאָרקער"):       normalize_yiddish("ניו-יאָרק"),
    normalize_yiddish("ברוקלינער"):         normalize_yiddish("ברוקלין"),
    normalize_yiddish("וואַרשעווער"):       normalize_yiddish("וואַרשע"),
    normalize_yiddish("לאָנדאָנער"):        normalize_yiddish("לאָנדאָן"),
    normalize_yiddish("אָדעסער"):          normalize_yiddish("אָדעס"),
    normalize_yiddish("מאָסקווער"):         normalize_yiddish("מאָסקווע"),
    normalize_yiddish("קיעווער"):          normalize_yiddish("קיעוו"),
    normalize_yiddish("וויענער"):          normalize_yiddish("וויען"),
    normalize_yiddish("בוענאָס-איירעסער"): normalize_yiddish("בוענאָס-איירעס"),
    normalize_yiddish("פּאַריזער"):         normalize_yiddish("פּאַריז"),
    normalize_yiddish("ווילנער"):          normalize_yiddish("ווילנע"),
    normalize_yiddish("מינסקער"):          normalize_yiddish("מינסק"),
    normalize_yiddish("ליטווישער"):        normalize_yiddish("ליטע"),
    normalize_yiddish("באַקוער"):          normalize_yiddish("באַקו"),
}


def extract_location_adjective(name: str) -> str:
    """If the name starts with a known city-adjective, return the settlement name.
    Returns '' if no city-adjective prefix is found.
    Does NOT modify the name — the adjective stays in the name for comparison."""
    tokens = normalize_yiddish(name).split()
    if tokens and tokens[0] in _CITY_ADJECTIVE_MAP:
        return _CITY_ADJECTIVE_MAP[tokens[0]]
    return ""
