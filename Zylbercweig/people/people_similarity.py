"""Similarity layer adapted for Zylbercweig people.

Why this isn't just the org similarity:
  - Person names are surname-first in headings ("אוגער, ישעיה") and given-name-first
    in narrative mentions ("ישעיה אוגער"). We need both orderings to score equally.
  - Initialism is rampant ("ב. גאָרין", "פּ.", "ק."). Bare initial + surname should
    match the corresponding heading but only weakly without context.
  - Pseudonyms are first-class (e.g. שלום-עליכם ↔ שלום ראַבינאָוויטש). They live in
    `subheading` and `names_variants` rather than the heading, so the matcher must
    consult those columns.
  - Multi-script: Hebrew/Yiddish ↔ Latin is handled by dybbuk_phonetic
    cross_script_similarity (already used by the org matcher).

Public API:
  - normalize_person_name(s)      → diacritics stripped, punctuation normalized
  - split_name_tokens(s)          → list of word tokens (after removing brackets)
  - expand_name_variants(heading, names_variants, subheading) → set of canonical
                                    surface forms to compare against
  - person_pair_score(a, b)       → float in [0, 1]  (full all-variants vs all-variants
                                    max, with surname-bonus and order-invariance)

The score is conservative: a bare-surname match scores ~0.55 (so it shows up as a
candidate but never auto-accepts); full name + same initial scores 0.95+; identical
normalized forms score 1.0.
"""
from __future__ import annotations
import re, sys, unicodedata, pathlib
from functools import lru_cache

_DYBBUK_PHONETIC = pathlib.Path(__file__).resolve().parents[1] / "dybbuk-phonetic" / "src"
if str(_DYBBUK_PHONETIC) not in sys.path:
    sys.path.insert(0, str(_DYBBUK_PHONETIC))

try:
    from dybbuk_phonetic.bridge import cross_script_similarity as _xss_raw  # type: ignore
except Exception:
    def _xss_raw(a: str, b: str) -> float:
        return 0.0


@lru_cache(maxsize=500_000)
def cross_script_similarity(a: str, b: str) -> float:
    # canonicalize ordering so (a,b) and (b,a) share a cache slot
    if a > b:
        a, b = b, a
    try:
        return float(_xss_raw(a, b))
    except Exception:
        return 0.0


_BRACKETS = re.compile(r"[\[\(].*?[\]\)]")
_PUNCT = re.compile(r"[״׳`'’\"‎‏‪-‮]+")
_DASH = re.compile(r"[\-–—]+")
_HEB_RANGE = re.compile(r"[֐-׿יִ-ﭏ]")


def is_hebrew(s: str) -> bool:
    return bool(_HEB_RANGE.search(s or ""))


def normalize_person_name(s: str) -> str:
    if not s:
        return ""
    s = _BRACKETS.sub(" ", s)
    nfd = unicodedata.normalize("NFD", s)
    no_marks = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    no_punct = _PUNCT.sub("", no_marks)
    flat = _DASH.sub(" ", no_punct)
    flat = re.sub(r"[,.;:]", " ", flat)
    flat = re.sub(r"\s+", " ", flat).strip()
    return flat.lower()


def split_name_tokens(s: str) -> list[str]:
    n = normalize_person_name(s)
    return [t for t in n.split() if t]


def is_initial(token: str) -> bool:
    # single Hebrew/Latin letter, possibly with trailing dot
    t = token.replace(".", "")
    return len(t) == 1


def heading_to_canonical(heading: str) -> str:
    """Headings are 'surname, given' — flip to 'given surname' so it lines up with
    in-text mentions like 'ישעיה אוגער'. If no comma, returned unchanged."""
    if not heading:
        return ""
    h = _BRACKETS.sub(" ", heading)
    if "," in h:
        parts = [p.strip() for p in h.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            return f"{parts[1]} {parts[0]}"
    return h.strip()


def variant_ok(v: str) -> bool:
    """Phase A fix (rule 1a): reject variants whose normalized form is shorter
    than 3 characters, or that consist of a single initial-like token (one or
    two letters + optional period — periods are stripped by normalization, so
    a length check on the normalized token covers them).

    Examples rejected: 'וו.', 'ג.', 'א. ב' is kept (2 tokens, handled by 1b).
    """
    n = normalize_person_name(v)
    if len(n) < 3:
        return False
    toks = n.split()
    if len(toks) == 1 and len(toks[0]) <= 2:
        return False
    return True


def expand_name_variants(heading: str, names_variants: str = "", subheading: str = "",
                         given_name_counts=None, drop_log: list | None = None) -> set[str]:
    """Build the set of surface forms representing this person.
    Includes: heading, canonical flipped form, every '|'-split name variant,
    and any bracketed alias inside the heading or subheading.

    Phase A fix:
      - rule 1a: forms failing variant_ok (initials / <3 normalized chars) are
        dropped from the set (logged to drop_log as 'initial_or_short').
      - rule 1c: single-token forms from names_variants whose token is a common
        given name (given_name_counts >= 3) are dropped
        (logged as 'common_given_name').
    """
    out: set[str] = set()

    def add(x, check_given: bool = False):
        x = (x or "").strip()
        if not x:
            return
        candidates = [x]
        # also consider the bracket-stripped form
        stripped = _BRACKETS.sub(" ", x).strip()
        if stripped and stripped != x:
            candidates.append(stripped)
        for c in candidates:
            if not variant_ok(c):
                if drop_log is not None:
                    drop_log.append((c, "initial_or_short"))
                continue
            if check_given and given_name_counts:
                toks = split_name_tokens(c)
                if len(toks) == 1 and given_name_counts.get(toks[0], 0) >= _GIVEN_NAME_THRESHOLD:
                    if drop_log is not None:
                        drop_log.append((c, "common_given_name"))
                    continue
            out.add(c)

    add(heading)
    add(heading_to_canonical(heading))
    for v in (names_variants or "").split("|"):
        add(v.strip(), check_given=True)
    # extract bracketed alias content (e.g. "[ביילקע קאָלאַך]" in subheading)
    for src in (heading or "", subheading or ""):
        for m in re.finditer(r"[\[\(]([^\]\)]+)[\]\)]", src):
            piece = m.group(1).strip()
            # skip pure date phrases
            if re.fullmatch(r"[\d\s\-.–—]+", piece):
                continue
            # likely an alias — keep
            add(piece)
    return {v for v in out if v}


@lru_cache(maxsize=200_000)
def _edit_distance(a: str, b: str, transpositions: bool = False) -> int:
    """Levenshtein distance; optionally Damerau (adjacent swap costs 1, not 2)."""
    m, n = len(a), len(b)
    if m < n:
        a, b, m, n = b, a, n, m
    prev2: list[int] = []
    prev = list(range(n + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            d = min(prev[j] + 1, cur[-1] + 1, prev[j - 1] + (ca != cb))
            if (transpositions and i > 1 and j > 1
                    and ca == b[j - 2] and a[i - 2] == cb):
                d = min(d, prev2[j - 2] + 1)
            cur.append(d)
        prev2, prev = prev, cur
    return prev[n]


_MATER_LECTIONIS = frozenset("אעויה")


def _indel_letter(a: str, b: str) -> str:
    """The added/dropped letter of a single-indel pair (assumes edit distance 1)."""
    lo, hi = (a, b) if len(a) < len(b) else (b, a)
    i = 0
    while i < len(lo) and lo[i] == hi[i]:
        i += 1
    return hi[i] if i < len(hi) else hi[-1]


def token_variant_similarity(a: str, b: str, floor_indels: bool = True,
                             transpositions: bool = False) -> float:
    """Character-level similarity for two SINGLE name tokens (e.g. surnames).

    Normalized-Levenshtein, nikkud-stripped. Meant for collapsing OCR/spelling
    variants of the same surname (קאמפאנעעץ↔קאמפאניעץ ≈ 0.89) while keeping
    genuinely different surnames apart (גרינבערג↔גרינבוים ≈ 0.63).

    A single INSERTION/DELETION is floored to 0.9 — BUT only when the added
    letter is a mater lectionis (א/ע/ו/י/ה). That is the real Yiddish spelling
    variance (קאהן↔קאהען, אקסלרוד↔אקסעלרוד). A single CONSONANT indel is not
    floored: it far more often marks a DIFFERENT surname (גארדין/גארין,
    קעסלער/קעלער, גליקמאן/גליקסמאן), so it must clear the ratio on its own merit.
    A single SUBSTITUTION is likewise never floored (פרידמאן/פרישמאן/פריימאן).

    Pass transpositions=True to count an adjacent swap (קאמפאניעעץ↔קאמפאנעיעץ) as
    one edit rather than two — right for OCR/spelling variance, but off by
    default so the resolver's candidate lookup is unaffected.

    Pass floor_indels=False to drop the floor entirely and judge on the raw
    ratio alone. Callers that MERGE review units want this: even a vowel-letter
    difference can be a real distinction (זילבער/זילבערט, סאבסי/סאבסיי) and
    folding those together silently reviews two surnames as one.
    """
    a = normalize_person_name(a)
    b = normalize_person_name(b)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    d = _edit_distance(a, b, transpositions)
    ratio = 1.0 - d / max(len(a), len(b))
    if (floor_indels and d == 1 and len(a) != len(b) and min(len(a), len(b)) >= 4
            and _indel_letter(a, b) in _MATER_LECTIONIS):
        return max(ratio, 0.9)
    return ratio


def _trigram_jaccard(a: str, b: str) -> float:
    a = normalize_person_name(a)
    b = normalize_person_name(b)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if len(a) < 3 or len(b) < 3:
        # fall back to character set jaccard for very short names
        sa, sb = set(a), set(b)
        return len(sa & sb) / max(1, len(sa | sb))
    ta = {a[i:i+3] for i in range(len(a) - 2)}
    tb = {b[i:i+3] for i in range(len(b) - 2)}
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _order_invariant_score(a: str, b: str) -> float:
    """Score that treats 'surname given' and 'given surname' equally."""
    ta = split_name_tokens(a)
    tb = split_name_tokens(b)
    if not ta or not tb:
        return 0.0
    set_a, set_b = set(ta), set(tb)
    if set_a == set_b:
        return 1.0
    # token-set Jaccard (handles reordering for free)
    jac = len(set_a & set_b) / len(set_a | set_b)
    # combine with trigram (catches morphological/spelling variation)
    tri = _trigram_jaccard(a, b)
    return max(jac, tri)


def _cross_script(a: str, b: str) -> float:
    if is_hebrew(a) == is_hebrew(b):
        return 0.0
    try:
        whole = float(cross_script_similarity(a, b))
    except Exception:
        whole = 0.0
    # tokenwise best-pairing — multi-word names degrade IPA quickly, so we also
    # match each token of the shorter side to its best partner on the other side.
    ta = [t for t in split_name_tokens(a) if not is_initial(t)]
    tb = [t for t in split_name_tokens(b) if not is_initial(t)]
    if not ta or not tb:
        return whole
    if len(ta) > len(tb):
        ta, tb = tb, ta
    pair_scores = []
    for x in ta:
        best = 0.0
        for y in tb:
            try:
                s = float(cross_script_similarity(x, y))
            except Exception:
                s = 0.0
            if s > best:
                best = s
        pair_scores.append(best)
    tokenwise = sum(pair_scores) / len(pair_scores) if pair_scores else 0.0
    return max(whole, tokenwise)


def _is_full_form(norm: str) -> bool:
    """Rule 1b qualifier: a normalized form counts as a FULL name form only if
    it has >= 2 tokens, each >= 3 normalized chars. Preserves the pseudonym
    pathway (e.g. שלום-עליכם → 'שלום עליכם', 2 tokens) while stopping bare
    initials ('וו.') and bare surnames from short-circuiting to 1.0."""
    toks = norm.split()
    return len(toks) >= 2 and all(len(t) >= 3 for t in toks)


def variant_pair_score(a: str, b: str) -> float:
    """Score one surface form against another.

    Phase A fix (rule 1b): the exact-equality → 1.0 short-circuit only fires
    when the matched normalized form is a full form (>= 2 tokens, each >= 3
    chars). A single-token exact match (e.g. bare surname) goes through normal
    scoring capped at 0.6.
    """
    if not a or not b:
        return 0.0
    na, nb = normalize_person_name(a), normalize_person_name(b)
    if na == nb:
        if _is_full_form(na):
            return 1.0
        # single-token / short exact match: normal scoring, capped at 0.6
        base = _order_invariant_score(a, b)
        return min(0.6, base)
    base = _order_invariant_score(a, b)
    cross = _cross_script(a, b)
    return max(base, cross)


# Surname gate (Phase A) --------------------------------------------------------
#
# Corpus given-name frequencies, installed by the callers (find_dedup_candidates,
# prepare_alignment) via set_given_name_counts(). Tokens that are common given
# names must not satisfy the surname gate on their own when they appear inside
# comma-less multi-token forms (variants may be given-first OR surname-first).

_GIVEN_NAME_COUNTS: dict[str, int] = {}
_GIVEN_NAME_THRESHOLD = 3


def set_given_name_counts(counts) -> None:
    """Install token -> frequency of appearance in the given-name part of
    headings (the part AFTER the comma). Used by the surname gate."""
    global _GIVEN_NAME_COUNTS
    _GIVEN_NAME_COUNTS = dict(counts)
    _surname_tokens.cache_clear()


@lru_cache(maxsize=200_000)
def _surname_tokens(surface: str) -> frozenset[str]:
    """Return the surname tokens of a surface form (comma-aware; mirrors the
    Shidduch person_name_similarity gate — logic copied, not imported).

      - 'surname, given' → tokens of the part BEFORE the first comma
      - comma-less single token → that token (a surviving single-token alias
        is surname-bearing by construction: common given names are suppressed
        upstream)
      - comma-less multi-token → all tokens EXCEPT common given names
        (variants may be surname-first or given-first, so we keep every
        plausible surname token but never let a shared given name pass)
    """
    h = _BRACKETS.sub(" ", surface or "").strip()
    if "," in h:
        sur_toks = split_name_tokens(h.split(",", 1)[0])
        return frozenset(t for t in sur_toks if len(t) >= 2)
    toks = [t for t in split_name_tokens(h) if len(t) >= 2]
    if not toks:
        return frozenset()
    if len(toks) == 1:
        return frozenset(toks)
    kept = {t for t in toks if _GIVEN_NAME_COUNTS.get(t, 0) < _GIVEN_NAME_THRESHOLD}
    # Natural order ('given surname') dominates comma-less multi-token forms
    # (names_variants, DB hebnames) — the LAST token is the likeliest surname.
    # Always keep it so a surname that also occurs a few times as a given name
    # (e.g. היימאן in 'העלענאַ היימאַן') cannot be annihilated by the
    # common-given filter, which would void the gate for that form.
    kept.add(toks[-1])
    return frozenset(kept)


def has_surname_overlap(a_variants: set[str], b_variants: set[str]) -> bool:
    """True if the two variant sets overlap on the surname part.

    A pair whose only shared token is a given-part token fails this gate.
    Overlap is exact on normalized tokens first; if that fails, we allow a
    fuzzy surname match (trigram jaccard >= 0.30 — catches spelling drift like
    וויגאָדסקי/ווייגאָדסקי or איינהארן/איינהורן; genuinely different surnames
    score well below this) or a cross-script surname match (IPA >= 0.75) so
    Hebrew↔Latin pairs are not gated out. This is a GATE, not a scorer —
    passing it only allows the real similarity scores to count.

    Fail-open when either side yields no surname tokens (initials-only etc.).
    """
    a_sur: set[str] = set()
    for v in a_variants:
        a_sur.update(_surname_tokens(v))
    b_sur: set[str] = set()
    for v in b_variants:
        b_sur.update(_surname_tokens(v))
    if not a_sur or not b_sur:
        return True
    if a_sur & b_sur:
        return True
    for x in a_sur:
        for y in b_sur:
            if _trigram_jaccard(x, y) >= 0.30:
                return True
            if is_hebrew(x) != is_hebrew(y) and cross_script_similarity(x, y) >= 0.75:
                return True
    return False


# backwards-friendly private alias
_has_surname_overlap = has_surname_overlap


def person_pair_score(
    a_variants: set[str], b_variants: set[str], surname_gate: bool = True
) -> tuple[float, str, str]:
    """Best pairwise score across all variant combinations.
    Returns (score, best_a_variant, best_b_variant).

    Phase A: surname gate — if the pair has no overlap on the surname part
    (see has_surname_overlap), return 0.0 immediately: a pair whose only
    shared token is a given-part token scores 0. Pass surname_gate=False to
    get the raw ungated score (used for gate-drop accounting).
    """
    if surname_gate and not has_surname_overlap(a_variants, b_variants):
        return (0.0, "", "")
    best = (0.0, "", "")
    for x in a_variants:
        for y in b_variants:
            s = variant_pair_score(x, y)
            if s > best[0]:
                best = (s, x, y)
                if s >= 0.999:
                    return best
    return best


# Year extraction --------------------------------------------------------------

_YEAR_RE = re.compile(r"(1[6-9]\d{2}|20[0-2]\d)")


def extract_year(date_str: str) -> int | None:
    if not date_str:
        return None
    m = _YEAR_RE.search(date_str)
    return int(m.group(1)) if m else None


def date_overlap_score(a_birth: str, a_death: str, b_birth: str, b_death: str) -> float:
    """Return 1.0 if dates are clearly compatible, 0.5 if partially known and
    consistent, 0.0 if clearly incompatible (>3yr disagreement on either end),
    None-equivalent (0.25) if neither side has any date."""
    ay, by = extract_year(a_birth), extract_year(b_birth)
    ad, bd = extract_year(a_death), extract_year(b_death)
    if any([ay, by, ad, bd]) is False:
        return 0.25
    # check inconsistency
    if ay and by and abs(ay - by) > 3:
        return 0.0
    if ad and bd and abs(ad - bd) > 3:
        return 0.0
    matches = 0
    known = 0
    if ay and by:
        known += 1
        if abs(ay - by) <= 1:
            matches += 1
    if ad and bd:
        known += 1
        if abs(ad - bd) <= 1:
            matches += 1
    if known == 0:
        return 0.5  # nothing to compare but no contradiction
    return matches / known


def place_overlap_score(a_places: list[str], b_places: list[str]) -> float:
    a_norm = {normalize_person_name(p) for p in a_places if p}
    b_norm = {normalize_person_name(p) for p in b_places if p}
    a_norm.discard(""); b_norm.discard("")
    if not a_norm or not b_norm:
        return 0.0
    if a_norm & b_norm:
        return 1.0
    # token overlap as a weaker signal
    a_toks, b_toks = set(), set()
    for p in a_norm: a_toks.update(p.split())
    for p in b_norm: b_toks.update(p.split())
    if a_toks & b_toks:
        return 0.5
    return 0.0


# A composite person-pair score for dedup / DB-alignment use cases.
# Weights are tunable — start conservative and calibrate against the 107
# Duplication-Check ground-truth pairs.

def composite_pair_score(
    name_score: float,
    date_score: float,
    place_score: float,
    same_entry_type: bool,
) -> float:
    # name is the dominant signal
    s = 0.6 * name_score + 0.20 * date_score + 0.15 * place_score
    if same_entry_type:
        s += 0.05
    return min(1.0, s)


if __name__ == "__main__":
    # quick smoke test
    cases = [
        ("אוגער ישעיה", "אוגער, ישעיה"),
        ("אַבּעלמאַן, בעני [ברוך-משה]", "בעני אַבעלמאַן"),
        ("גאָרדין", "גאָרדין, יעקב"),
        ("שלום-עליכם", "שלום ראַבינאָוויטש"),
        ("Yeshaya Uger", "אוגער ישעיה"),
    ]
    for a, b in cases:
        va = expand_name_variants(a)
        vb = expand_name_variants(b)
        s, x, y = person_pair_score(va, vb)
        print(f"{s:.3f}  {a!r}  vs  {b!r}   (best: {x!r} ~ {y!r})")
