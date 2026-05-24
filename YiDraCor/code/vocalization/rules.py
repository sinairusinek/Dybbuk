"""
Yiddish vocalization rules — empirically mined from page 6 of *Yudale der
Blinder* (1908) and applied to bare tokens elsewhere in the play.

Three public entry points:
  - speaker_span(line)        — (start, end) char span of the line's
                                speaker name, or None. Stage A skip.
  - bracket_spans(line)       — list of (start, end) spans inside
                                parentheses / brackets (stage directions).
  - apply_default_rules(token)— add nikkud to a bare token where evidence
                                is categorical or strongly dominant.
  - validate(token)           — list of rule-violation strings on a
                                vocalized token (for QA / Claude post-check).

Rule inventory (see analyze_rules.py and README-design.md for evidence):
  A. speakers & bracketed stage directions: leave bare
  B. word-final ן / ם: never carry nikkud                   (100%)
  C. yod (single or in יי): never carries nikkud            (100%)
  D. cluster-first consonant (esp. ר/ל/נ) → sheva           (~95%)
  E. consonant before ע → segol                             (97%)
  F. consonant before single י → hiriq (rare: holam for /oy/)
  G. consonant before יי → patah (/ay/) or tsere (/ey/),
     lexical choice learned from page 6 (see YII_PRECEDING)
  H. ב carrying a vowel → dagesh (בּ)                       (100%)
  I. ש in a vocalized word → shin-dot (שׁ)                  (90%)
  J. second ו of digraph וו: usually bare                   (strong)

Word-initial positions (i==0) skip rule D (no sheva on syllable nuclei)
but participate in E, F, G — see analyze_rules.py output for examples
like גֶע- prefix and מַיין.
"""

import re

# --- nikkud chars ---
SEGOL  = "ֶ"
PATAH  = "ַ"
KAMATZ = "ָ"
HIRIQ  = "ִ"
TSERE  = "ֵ"
HOLAM  = "ֹ"
SHEVA  = "ְ"
DAGESH = "ּ"
SHIN_DOT = "ׁ"
SIN_DOT  = "ׂ"

NIKKUD_RE  = re.compile(r"[֑-ׇ]")
WORD_RE    = re.compile(r"[א-תְ-ׇ׳״']+")
SPEAKER_RE = re.compile(r"^\s*([א-ת][א-ת֑-ׇ]*)\s*:")

MATRES = set("אעוי")
SONORANT = set("רלנמד")  # cluster-first → ~95% sheva on page 6

# Populated at import-time from page 6 by learn_yii_preferences().
# Maps the consonant immediately before יי to its most common vowel mark.
YII_PRECEDING: dict = {}


def parse_token(tok: str):
    """Return list of [letter, marks_list]."""
    out = []
    for ch in tok:
        if "א" <= ch <= "ת":
            out.append([ch, []])
        elif "֑" <= ch <= "ׇ" and out:
            out[-1][1].append(ch)
        else:
            # apostrophe / geresh — keep as standalone marker letter
            out.append([ch, []])
    return out


def reassemble(letters) -> str:
    return "".join(l + "".join(m) for l, m in letters)


def strip_nikkud(s: str) -> str:
    return NIKKUD_RE.sub("", s)


def is_hebrew_letter(ch: str) -> bool:
    return "א" <= ch <= "ת"


def learn_yii_preferences(tokens) -> dict:
    """
    From an iterable of vocalized tokens, build {consonant -> most-common mark}
    for the consonant immediately preceding יי. Updates module-level
    YII_PRECEDING in place.
    """
    from collections import Counter, defaultdict
    cands = defaultdict(Counter)
    for tok in tokens:
        letters = parse_token(tok)
        for i in range(len(letters) - 2):
            l1, m1 = letters[i]
            if not is_hebrew_letter(l1):
                continue
            if (letters[i+1][0] == "י" and letters[i+2][0] == "י"
                    and l1 not in MATRES):
                vowels = [m for m in m1 if m not in (DAGESH, SHIN_DOT, SIN_DOT)]
                for v in vowels:
                    cands[l1][v] += 1
    YII_PRECEDING.clear()
    for l, c in cands.items():
        YII_PRECEDING[l] = c.most_common(1)[0][0]
    return YII_PRECEDING


# --- Rule A: speaker prefix detection ---

def speaker_span(line: str):
    """Return (start, end) char span of the speaker name in a line, or None."""
    m = SPEAKER_RE.match(line)
    if m:
        return m.span(1)
    return None


# Stage directions in this corpus are wrapped in parentheses (round or square)
# and are conventionally NOT vocalized (occasional pasekh-aleph aside, which
# we tolerate by simply skipping these tokens — we never *add* nikkud here).
_BRACKETS = [("(", ")"), ("[", "]"), ("（", "）"), ("【", "】")]


def bracket_spans(line: str) -> list[tuple[int, int]]:
    """Return list of (start, end) char spans covering text inside any
    paired brackets in the line — stage directions, parenthetical asides, etc.
    Spans cover the content between the brackets (exclusive of the brackets
    themselves) so token offsets fall inside cleanly."""
    spans = []
    for left, right in _BRACKETS:
        i = 0
        while True:
            a = line.find(left, i)
            if a < 0:
                break
            b = line.find(right, a + 1)
            if b < 0:
                break
            spans.append((a + 1, b))
            i = b + 1
    return spans


def in_any_span(start: int, end: int, spans) -> bool:
    return any(s <= start and end <= e for s, e in spans)


# --- Default rule application (for unseen tokens) ---

def apply_default_rules(token: str) -> str:
    """
    Apply categorical/strong rules to a bare Yiddish token. Marks are added
    only where page-6 evidence is unambiguous; ambiguous positions
    (word-initial vowels, /ay/-vs-/ey/ choice, vav vowel quality) stay bare.
    """
    if NIKKUD_RE.search(token):
        # Already has marks — don't overwrite, just validate elsewhere.
        return token

    letters = parse_token(token)
    n = len(letters)
    fired = False  # did any positional rule fire? (gate for ב/ש marking)

    for i, pair in enumerate(letters):
        ltr, marks = pair
        if not is_hebrew_letter(ltr):
            continue
        is_first = (i == 0)
        is_last  = (i == n - 1)
        next_l   = letters[i+1][0] if i+1 < n else None
        next_next = letters[i+2][0] if i+2 < n else None

        # Rule C: yod never marked
        if ltr == "י":
            continue
        # Rule B: word-final ן/ם never marked
        if is_last and ltr in "ןם":
            continue
        # Rule J: second vav of וו stays bare
        if i > 0 and letters[i-1][0] == "ו" and ltr == "ו":
            continue

        # Position-based vowel placement.
        # Rules E, F, G fire word-initial too (e.g. גֶע- prefix, מִיר, מַיין).
        # Rule D (cluster sheva) does NOT fire word-initial.
        if not marks:
            if next_l == "י" and next_next == "י":
                # Rule G: before יי → patah by default; lookup lexical override
                mark = YII_PRECEDING.get(ltr, PATAH)
                marks.append(mark); fired = True
            elif next_l == "י":
                # Rule F: before single י → hiriq
                marks.append(HIRIQ); fired = True
            elif next_l == "ע":
                # Rule E: before ע → segol
                marks.append(SEGOL); fired = True
            elif (not is_first and next_l and is_hebrew_letter(next_l)
                  and next_l not in MATRES and ltr in SONORANT):
                # Rule D: cluster-first → sheva (strong for sonorants, mid-word)
                marks.append(SHEVA); fired = True

    if fired:
        # Rule H/I: only add dagesh/shin-dot if the word is being vocalized
        for ltr, marks in letters:
            if ltr == "ב" and DAGESH not in marks:
                marks.append(DAGESH)
            elif ltr == "ש" and SHIN_DOT not in marks and SIN_DOT not in marks:
                marks.append(SHIN_DOT)

    return reassemble(letters)


# --- Validator ---

def validate(token: str) -> list[str]:
    """Return list of human-readable rule violations on a vocalized token."""
    violations = []
    letters = parse_token(token)
    n = len(letters)
    for i, (ltr, marks) in enumerate(letters):
        is_last = (i == n - 1)
        # Rule B
        if is_last and ltr in "ןם" and marks:
            violations.append(f"final {ltr} carries marks ({''.join(marks)})")
        # Rule C
        if ltr == "י" and marks:
            violations.append(f"yod carries marks ({''.join(marks)})")
        # Rule E (when violated): consonant before ע has wrong vowel
        if i+1 < n and letters[i+1][0] == "ע" and is_hebrew_letter(ltr) and ltr not in MATRES:
            vowels = [m for m in marks if m not in (DAGESH, SHIN_DOT, SIN_DOT)]
            if vowels and SEGOL not in vowels:
                violations.append(
                    f"{ltr} before ע has {vowels} (expected segol)"
                )
    # Rule J relaxed: second vav of וו occasionally carries marks
    # on page 6 (~14%); not flagged.
    # Rule H relaxed: ב without dagesh is legitimate in loshn-koydesh
    # /v/ contexts. Only flag if ב carries its own vowel without dagesh —
    # vocalized but missing the expected dagesh.
    word_has_vowel = any(
        any(c in (SEGOL, PATAH, KAMATZ, HIRIQ, TSERE, HOLAM) for c in m)
        for _, m in letters
    )
    for ltr, marks in letters:
        own_vowel = any(c in (SEGOL, PATAH, KAMATZ, HIRIQ, TSERE, HOLAM) for c in marks)
        if ltr == "ב" and own_vowel and DAGESH not in marks:
            violations.append("ב carries vowel but lacks dagesh")
        if ltr == "ש" and word_has_vowel and SHIN_DOT not in marks and SIN_DOT not in marks:
            violations.append("ש in vocalized word lacks shin/sin dot")
    return violations
