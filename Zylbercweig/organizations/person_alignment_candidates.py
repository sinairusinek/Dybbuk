"""Prototype: org→org alignment candidates derived via embedded person names.

The pipeline:
    cluster name
      ─► extract_person_spans(text)              # Yiddish frame-word parser
      ─► resolve_spans_to_persons(spans, people)  # match person_db
      ─► person_id → candidate org_ids           # surname-pivot bridge
      ─► rank and explain
      ─► append to candidate_db_ids in org_alignment_review.tsv

This is the Dybbuk-side prototype. If results justify it, the span extractor
and person resolver get promoted into Shidduch (the people-matching engine).
The person→org bridge stays in Dybbuk because it's Zylbercweig-specific.

Evaluation harness at the bottom checks recall against Ruthie's recent ALIGNs.
"""
from __future__ import annotations
import csv
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ORG_DIR = REPO / "Zylbercweig" / "organizations"
PEOPLE_DIR = REPO / "Zylbercweig" / "people"

# Shidduch lives in a sibling repo; add to path so we can import its
# phonetic encoder + Hebrew→Latin transliterator.
SHIDDUCH_REPO = REPO.parent / "Shidduch"
if SHIDDUCH_REPO.exists() and str(SHIDDUCH_REPO) not in sys.path:
    sys.path.insert(0, str(SHIDDUCH_REPO))
try:
    import io, contextlib
    from shidduch.core.normalizers import phonetic_encode as _phonetic_encode_raw  # type: ignore
    _PHONETIC_AVAILABLE = True
    NIKUD_PRE = re.compile(r"[֑-ׇ]")
    def phonetic_encode(text):
        # translit_me warns on dagesh and other diacritics; strip + silence stderr
        if not text: return set()
        cleaned = NIKUD_PRE.sub("", text)
        # silence warnings printed to stdout/stderr by translit_me
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            return _phonetic_encode_raw(cleaned)
except ImportError:
    _PHONETIC_AVAILABLE = False
    def phonetic_encode(_): return set()

PEOPLE_DB = PEOPLE_DIR / "people_db.tsv"
CORE_DB = ORG_DIR / "core_db.tsv"
ALIGN = ORG_DIR / "org_alignment_review.tsv"
ACTIVITY = ORG_DIR / "activity_log.tsv"

# ---------------------------------------------------------------------------
# I/O — quote-none safe (Yiddish has stray ")

def load_tsv(path):
    with open(path, encoding="utf-8") as f:
        rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        raw = list(rd)
    if not raw: return []
    h = raw[0]
    return [dict(zip(h, r + [""] * (len(h) - len(r)))) for r in raw[1:]]


# ---------------------------------------------------------------------------
# Normalization (light, prototype-only — will be replaced by matching-core)

NIKUD_RE = re.compile(r"[֑-ׇ]")  # strip Hebrew points/cantillation
def strip_nikud(s: str) -> str:
    return NIKUD_RE.sub("", unicodedata.normalize("NFKC", s))

def strip_possessive(token: str) -> str:
    # Yiddish possessive 'ס or just trailing ס on surnames
    return re.sub(r"['׳]?ס$", "", token) if len(token) > 3 else token

def normalize_token(t: str) -> str:
    t = strip_nikud(t)
    t = strip_possessive(t)
    return t

# ---------------------------------------------------------------------------
# Person-span extraction

HEB_WORD = re.compile(r"[֐-׿יִ-ﭏ]+(?:['׳]?[֐-׿]+)?")
INITIAL  = re.compile(r"^[א-ת]\.?$")

# Frame words that signal a person name follows or precedes them.
FRAME_FOLLOWS = {     # "X follows me" -> person span after these
    "פֿון", "פון", "א'ר", "אנ'", "א'נ", "א\"ר", "א\"נ", "ביי", "אונטער",
}
FRAME_PRECEDES = {    # "X precedes me" -> person span before these
    "ס", "'ס", "׳ס",  # possessive
    "טרופּע", "טרופע", "פֿאַמיליע", "פאמיליע", "פאַמיליע", "משפחה",
    "פאמיליעס", "אַנסאַמבל", "אנסאמבל", "אַטעליע", "קאָמפּאַניע",
}
FRAME_FAMILY = {      # X is a surname after these
    "פֿאַמיליע", "פאמיליע", "פאַמיליע", "משפחה",
    "ברידער", "די-ברידער", "שוועסטער",
}
COORDINATORS = {"און", "מיט"}
STOPWORDS = {
    "די", "דער", "דאָס", "דאס", "דאָרטיקער", "דאָרטיק", "אָרטיק", "אָרטיקער",
    "אַ", "א", "אין", "פֿאַר", "פאר", "ערשטער", "ערשטע", "די", "אַ",
    "ייִדיש", "יידיש", "יידישע", "יידישער", "יידישן", "ייִדישע", "ייִדישער",
    "דייטשער", "דייטשן", "פּוילישער", "פּוילישע", "רוסישער", "רוסישע",
    "מאָסקווער", "וויינער", "וויענער", "ווינער", "אָדעסער", "וואַרשעווער",
    "ווילנער", "ניו-יאָרקער", "אַמעריקאַנער", "פֿאַראייניקטער",
    "וואַנדער", "וואַנדערנדיקע", "וואַנדערנדיקער", "באַוועגלעך",
    "גאַסטראָלן", "גאַסטראָל", "גאַסטראָלירנדיקע", "גאַסטראָלירנדיקער",
    "אייראָפּעישן", "אייראָפּעישער",
    "דראַמאַטיש", "דראַמאַטישע", "דראַמאַטישער", "דראַמאַטישן",
    "דראָמאַטיש", "דראָמאַטישע", "דראָמאַטישן",
    "ליטעראַריש", "ליטעראַרישע",
    "אָפּערעטן", "אָפּערעטע", "מוזיקאַליש", "מוזיקאַלישע",
    "פֿאָלקס", "פאָלקס", "פֿאָלקסטימלעך",
    "טור", "טורניי", "גאַסטראָלן",
}

@dataclass
class PersonSpan:
    text: str             # raw text span (joined surface form)
    tokens: list[str]     # individual tokens
    role: str             # 'full', 'surname', 'family', 'brothers', 'possessive', 'initial+last'
    frame: str = ""       # the frame word that triggered detection

def _is_content(tok: str) -> bool:
    n = normalize_token(tok)
    return len(n) >= 2 and n not in STOPWORDS and not INITIAL.match(n)

def _split_compound_surnames(tokens: list[str]) -> list[list[str]]:
    """Treat hyphen-joined surnames as separate persons: ספּיוואַקאָווסקי-פישזאָן → [['ספּיוואַקאָווסקי'], ['פישזאָן']]."""
    out = []
    for t in tokens:
        if "-" in t:
            parts = [p for p in t.split("-") if _is_content(p)]
            for p in parts: out.append([p])
        else:
            out.append([t])
    return out

STRUCTURE_WORDS = {
    "טרופּע", "טרופע", "טרופּעס", "אַנסאַמבל", "אנסאמבל", "קאָמפּאַניע",
    "פֿאַמיליע", "פאמיליע", "פאַמיליע", "משפחה", "ברידער", "שוועסטער",
    "דירעקציע", "אָפּערעטן-טרופּע", "וואַנדער-טרופּע", "כאָר",
    "אַטעליע", "סטודיע", "קרייז", "קלוב", "פֿאַראיין",
}

# If a cluster name contains any INSTITUTIONAL_WORDS, skip person extraction
# entirely. These are committees/unions/schools/etc. that are not person-named
# even if their tokens happen to phonetically collide with a surname.
INSTITUTIONAL_WORDS = {
    # Governing bodies / organizations
    "ועד", "פֿאַראיין", "פאַראיין", "פאראיין", "פֿעדעראַציע", "פעדעראציע",
    "קאָמיטעט", "קומיטעט", "קאמיטעט", "אינסטיטוט", "אַסאָציאַציע",
    "אַסאסיאַציע", "סאָציעטעט", "געזעלשאַפֿט", "געזעלשאַפט",
    "ייוו\"אָ", "ייווא", "אָרגאַניזאַציע", "אָרגאַניזאציע",
    "מיניסטעריום", "דעפּאַרטמענט", "מאַגיסטראַט", "ראַט",
    "פּאַרטיי", "פארטיי", "ליגע", "בונד", "אַרבעטער-רינג", "אַרבייטער-רינג",
    # Media (newspapers / journals)
    "צייטונג", "טאָגבלאַט", "טאגבלאט", "טאָג-בלאַט", "וואָכנשריפֿט",
    "זשורנאַל", "זשורנאל", "ביכל", "אַלמאַנאַך", "כראָניק",
    "פּרעסע", "פרעסע", "מאָנאַטשריפֿט", "מאָנאַט-שריפֿט",
    # Cultural / educational
    "ביבליאָטעק", "מוזיי", "אַרכיוו", "סינאַגאָגע", "שול",
    "חדר", "ישיבה", "תלמוד-תורה", "בית-מדרש", "קלויז",
    "אַקאַדעמיע", "אוניווערסיטעט", "קאָנסערוואַטאָריע",
    "ספּיטאָל", "האָספּיטאַל", "אַמבולאַטאָריע", "אַפּטייק",
    "סעקציע", "אָפּטיילונג", "ביוראָ",
    "חינוך", "תּלמוד", "תורה",
    # Venues (not person-named when they're place/concept names)
    "הויז", "פּלאַץ", "סקווער", "גאָרטן", "ערד",
    # Cooperative / collective
    "קואָפּעראַטיוו", "קאָאָפּעראַטיוו", "קוך", "קעך",
    # Religious / civic
    "קהילה", "קולטור-ליגע", "פּ.פּ.ס", "פּ\"פּ\"ס",
}

# Surname-suffix patterns typical for Eastern European Jewish surnames.
# Used to gate the bare-name fallback: only treat a single token as a surname
# if it ends in one of these.
SURNAME_SUFFIX_RE = re.compile(
    r"(?:סקי|זקי|וויטש|וויץ|בערג|פעלד|מאַן|מאן|שטיין|בלום|"
    r"אָוו|אָווא|אָווסקי|ערס|ער|אָן|זאָן|זון|"
    r"ענקאָ|אַנא|אַן|ין|ינא|"
    r"אדלער|לעוו|כען|בויעם|"
    r"er|ovich|witz|witsch|berg|feld|man|stein|bloom|"
    r"ov|ova|ovsky|ovski|ovic|enko|ana|son|sohn|kin|in|"
    r")$",
    re.IGNORECASE,
)

def _looks_surname_like(tok: str) -> bool:
    n = normalize_token(tok)
    if not n or len(n) < 3: return False
    if n in STOPWORDS or n in STRUCTURE_WORDS or n in INSTITUTIONAL_WORDS: return False
    return bool(SURNAME_SUFFIX_RE.search(n))

def _is_namelike(tok: str) -> bool:
    n = normalize_token(tok)
    if not n or len(n) < 2: return False
    if n in STOPWORDS or n in STRUCTURE_WORDS: return False
    if n in FRAME_FOLLOWS or n in FRAME_PRECEDES or n in COORDINATORS: return False
    if n in {"פֿון", "פון", "ביי", "אונטער", "מיט", "און"}: return False
    return True

def extract_person_spans(text: str, allow_bare_name: bool = False) -> list[PersonSpan]:
    if not text: return []
    # Skip institutional clusters entirely — too many false-positive surnames otherwise.
    text_normalized = strip_nikud(text)
    if any(w in text_normalized for w in INSTITUTIONAL_WORDS):
        return []
    # Tokenise on whitespace, keep hyphenated tokens intact for the compound check
    tokens = re.findall(r"\S+", text)
    spans: list[PersonSpan] = []
    consumed_idx: set[int] = set()
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        tok_n = strip_nikud(tok)

        # Structure word with name before/after (טרופּע X / X טרופּע)
        if normalize_token(tok) in STRUCTURE_WORDS or any(s in tok_n for s in STRUCTURE_WORDS):
            # Collect run of name-like tokens immediately AFTER
            j = i + 1
            after = []
            while j < len(tokens) and _is_namelike(tokens[j]):
                after.append(tokens[j]); j += 1
                if j < len(tokens) and strip_nikud(tokens[j]) in COORDINATORS:
                    j += 1  # skip "and" / "with"
                    continue
            if after:
                for sub in _split_compound_surnames(after):
                    if sub:
                        role = "full" if len(sub) >= 2 else "surname"
                        spans.append(PersonSpan(" ".join(sub), sub, role, tok))
                        consumed_idx.update(range(i+1, j))
            # Collect single name-like token immediately BEFORE (e.g. נפחלים טרופּע)
            if i > 0 and _is_namelike(tokens[i-1]) and (i-1) not in consumed_idx:
                prev = tokens[i-1]
                # Possessive form?
                role = "possessive" if re.search(r"['׳]?ס$", strip_nikud(prev)) else "surname"
                base = strip_possessive(prev) if role == "possessive" else prev
                spans.append(PersonSpan(prev, [base], role, tok))
                consumed_idx.add(i-1)
            i = max(j, i+1); continue

        # Family marker: X-פֿאַמיליע / X-משפחה / פאַמיליע X
        if any(f in tok for f in FRAME_FAMILY):
            # try before
            if i > 0:
                prev = tokens[i-1]
                if _is_content(prev):
                    for sub in _split_compound_surnames([prev]):
                        if sub: spans.append(PersonSpan(prev, sub, "family", tok))
            # try after
            if i + 1 < len(tokens):
                nxt = tokens[i+1]
                if _is_content(nxt):
                    for sub in _split_compound_surnames([nxt]):
                        if sub: spans.append(PersonSpan(nxt, sub, "family", tok))
            i += 1; continue

        # Brothers/sisters X
        if tok_n in {"ברידער", "שוועסטער"}:
            if i + 1 < len(tokens):
                nxt = tokens[i+1]
                if _is_content(nxt):
                    for sub in _split_compound_surnames([nxt]):
                        if sub: spans.append(PersonSpan(nxt, sub, "brothers", tok))
            i += 1; continue

        # "פֿון" / "פון" / abbreviations → person follows
        if tok_n in FRAME_FOLLOWS:
            # Collect a run of content tokens, splitting on coordinators (X און Y)
            run, j = [], i + 1
            current = []
            while j < len(tokens):
                t = tokens[j]; t_n = strip_nikud(t)
                if t_n in COORDINATORS:
                    if current: run.append(current); current = []
                    j += 1; continue
                if t_n in FRAME_FOLLOWS or t_n in FRAME_PRECEDES or t_n in FRAME_FAMILY:
                    break
                if INITIAL.match(t_n):
                    current.append(t); j += 1; continue
                if _is_content(t):
                    current.append(t); j += 1; continue
                # stopword — drop
                j += 1
            if current: run.append(current)
            for group in run:
                # group is a list of tokens forming a person's name (may include initials)
                # Detect compound surnames
                if len(group) == 1:
                    for sub in _split_compound_surnames(group):
                        if sub:
                            role = "initial+last" if any(INITIAL.match(strip_nikud(t)) for t in group) else "surname"
                            spans.append(PersonSpan(" ".join(sub), sub, role, tok))
                else:
                    # First+last (or initial+last)
                    role = "initial+last" if any(INITIAL.match(strip_nikud(t)) for t in group) else "full"
                    spans.append(PersonSpan(" ".join(group), group, role, tok))
            i = j; continue

        # Possessive (the token itself ends in ס or 'ס after a content word)
        if re.search(r"['׳]?ס$", tok_n) and len(strip_possessive(tok_n)) >= 3:
            base = strip_possessive(tok_n)
            if base not in STOPWORDS:
                # Check that the next token is a frame-precedes word (טרופּע, אַנסאַמבל, ...)
                if i + 1 < len(tokens) and any(f in strip_nikud(tokens[i+1]) for f in FRAME_PRECEDES if len(f) > 2):
                    spans.append(PersonSpan(tok, [base], "possessive", tokens[i+1]))
        i += 1

    # Bare-name fallback (opt-in): ONLY fire when there's evidence the cluster
    # is a person — must be ≤3 content tokens AND last token has surname-like
    # suffix, OR an initial-led pattern ("א. גאָלדענבערג"). Off by default for
    # production writes because the morphological filter is imperfect and
    # institutional clusters with surname-suffix-shaped words leak through.
    if not spans and allow_bare_name:
        content = [t for t in tokens if _is_namelike(t)]
        if not (1 <= len(content) <= 3):
            return spans
        split = _split_compound_surnames(content)
        flat = [g for grp in split for g in grp]  # flatten to per-token list
        # Check if any token is initial-like
        has_initial = any(INITIAL.match(strip_nikud(t)) for t in tokens if "." in t)
        # Last content token must look like a surname
        last_tok = flat[-1] if flat else ""
        if not _looks_surname_like(last_tok) and not has_initial:
            return spans
        # Fire — emit each compound piece as a span
        for group in split:
            if not group: continue
            role = "full" if len(group) >= 2 else "surname"
            if any(INITIAL.match(strip_nikud(t)) for t in group):
                role = "initial+last"
            spans.append(PersonSpan(" ".join(group), group, role, "(bare)"))
    return spans


# ---------------------------------------------------------------------------
# People lookup

@dataclass
class PersonHit:
    person_id: str
    hebname: str
    matched_surname: str
    matched_first: str | None = None

def build_people_index(people: list[dict]) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """Returns (literal_idx, phonetic_idx).

    literal_idx: surname_norm -> persons (exact string)
    phonetic_idx: beider-morse phonetic code -> persons (cross-spelling, cross-script)
    """
    literal_idx: dict[str, list[dict]] = defaultdict(list)
    phonetic_idx: dict[str, list[dict]] = defaultdict(list)
    for p in people:
        sources = []
        for field in ("hebname", "english", "alternative_name", "name_variants"):
            v = (p.get(field) or "").strip()
            if not v: continue
            for variant in v.split("|"):
                variant = variant.strip()
                if variant: sources.append(variant)
        for v in sources:
            toks = re.split(r"[\s,]+", v)
            if not toks: continue
            for cand in (toks[-1], toks[0]):
                surname = normalize_token(cand.lower())
                if len(surname) >= 3:
                    if p not in literal_idx[surname]:
                        literal_idx[surname].append(p)
                    # phonetic codes
                    for code in phonetic_encode(cand):
                        if p not in phonetic_idx[code]:
                            phonetic_idx[code].append(p)
    return literal_idx, phonetic_idx

def resolve_spans_to_persons(spans: list[PersonSpan], literal_idx: dict[str, list[dict]], phonetic_idx: dict[str, list[dict]] | None = None) -> list[tuple[PersonSpan, list[dict]]]:
    out = []
    for sp in spans:
        last = normalize_token(sp.tokens[-1])
        persons = list(literal_idx.get(last, []))
        # Phonetic fallback for spans the literal index missed
        if not persons and phonetic_idx is not None:
            for code in phonetic_encode(sp.tokens[-1]):
                for p in phonetic_idx.get(code, []):
                    if p not in persons:
                        persons.append(p)
        if not persons: continue
        if len(sp.tokens) >= 2 and sp.role in ("full", "initial+last"):
            first = normalize_token(sp.tokens[0])
            if not INITIAL.match(first):
                filtered = [p for p in persons
                            if normalize_token(((p.get("hebname") or "").split() or [""])[0]) == first]
                if filtered: persons = filtered
            else:
                filtered = [p for p in persons
                            if ((p.get("hebname") or "").split() or [""])[0].startswith(first.replace(".", ""))]
                if filtered: persons = filtered
        out.append((sp, persons))
    return out


# ---------------------------------------------------------------------------
# Person → org bridge (surname-pivot only in this prototype)

LATIN_TOKEN = re.compile(r"[A-Za-z][A-Za-z\-']{2,}")
LATIN_STOPWORDS = {"the","of","and","family","troupe","theatre","theater","jewish",
                   "yiddish","brothers","company","group","circle","studio","ensemble",
                   "school","union","club","new","old","first","great"}

def build_surname_to_orgs(core: list[dict]) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """Returns (literal_idx, phonetic_idx) for core_db rows."""
    literal_idx: dict[str, list[dict]] = defaultdict(list)
    phonetic_idx: dict[str, list[dict]] = defaultdict(list)
    for r in core:
        heb_text = " ".join((r.get(k) or "") for k in ("name_yiddish", "name_variants"))
        for tok in re.findall(HEB_WORD, heb_text):
            n = normalize_token(tok)
            if len(n) >= 3 and n not in STOPWORDS:
                if r not in literal_idx[n]: literal_idx[n].append(r)
                for code in phonetic_encode(tok):
                    if r not in phonetic_idx[code]: phonetic_idx[code].append(r)
        for tok in LATIN_TOKEN.findall(r.get("name") or ""):
            n = tok.lower().strip("'")
            n = re.sub(r"'s$|s'$", "", n)
            if len(n) >= 3 and n not in LATIN_STOPWORDS:
                if r not in literal_idx[n]: literal_idx[n].append(r)
                for code in phonetic_encode(tok):
                    if r not in phonetic_idx[code]: phonetic_idx[code].append(r)
    return literal_idx, phonetic_idx


@dataclass
class OrgCandidate:
    db_id: str
    why: list[str] = field(default_factory=list)

@dataclass
class OrgCandidate2:
    db_id: str
    score: float = 0.0
    why: list[str] = field(default_factory=list)

def person_to_org_candidates(spans_with_persons, surname_literal_idx, surname_phonetic_idx) -> list[OrgCandidate]:
    by_id: dict[str, OrgCandidate] = {}
    for sp, persons in spans_with_persons:
        for p in persons:
            heb = (p.get("hebname") or "").strip()
            eng = (p.get("english") or "").strip()
            surname_strings = set()
            if heb: surname_strings.add(heb.split()[-1])
            if eng:
                eng_tail = re.split(r"[\s,]+", eng)[-1]
                surname_strings.add(eng_tail)
            orgs_found: dict[str, dict] = {}
            # Literal match
            for s in surname_strings:
                n = re.sub(r"'s$|s'$", "", normalize_token(s.lower()))
                if len(n) >= 3:
                    for org in surname_literal_idx.get(n, []):
                        orgs_found[org["db_id"]] = org
            # Phonetic match (catches Adler↔אדלער and Yiddish↔Latin generally)
            for s in surname_strings:
                for code in phonetic_encode(s):
                    for org in surname_phonetic_idx.get(code, []):
                        orgs_found[org["db_id"]] = org
            for org_id, org in orgs_found.items():
                c = by_id.setdefault(org_id, OrgCandidate(org_id))
                reason = f"person={heb or eng} via span '{sp.text}' ({sp.role})"
                if reason not in c.why:
                    c.why.append(reason)
                c.__dict__.setdefault("score", 0.0)
                c.__dict__["score"] += 1.0 / max(1, len(orgs_found))
    return sorted(by_id.values(), key=lambda c: (-c.__dict__.get("score", 0.0), -len(c.why)))


# ---------------------------------------------------------------------------
# Evaluation harness

def evaluate_against_ruthie():
    people = load_tsv(PEOPLE_DB)
    core = load_tsv(CORE_DB)
    align = {r["cluster_id"]: r for r in load_tsv(ALIGN)}
    log = load_tsv(ACTIVITY)
    ruthie_aligns = [r for r in log
                     if r.get("reviewer") == "Ruthie"
                     and r.get("ts", "") >= "2026-06-21"
                     and r.get("action") == "alignment"
                     and r.get("decision") == "ALIGN"]

    print(f"people_db rows: {len(people)}")
    print(f"core_db rows:   {len(core)}")
    print(f"Ruthie ALIGNs to evaluate: {len(ruthie_aligns)}")

    print(f"phonetic available: {_PHONETIC_AVAILABLE}")
    people_literal, people_phonetic = build_people_index(people)
    surname_literal, surname_phonetic = build_surname_to_orgs(core)
    print(f"index sizes:  people_literal={len(people_literal)} people_phonetic={len(people_phonetic)}"
          f"  orgs_literal={len(surname_literal)} orgs_phonetic={len(surname_phonetic)}\n")

    top1 = top3 = top5 = any_match = 0
    no_spans = no_persons = no_candidates = 0
    samples_hit = []
    samples_miss = []
    for r in ruthie_aligns:
        cid = r["target_id"]
        a = align.get(cid)
        if not a: continue
        gold_db = a.get("aligned_db_id", "").strip()
        if not gold_db: continue
        text = a.get("canonical_yiddish", "") or ""
        spans = extract_person_spans(text, allow_bare_name=True)
        if not spans:
            no_spans += 1; continue
        spans_with_persons = resolve_spans_to_persons(spans, people_literal, people_phonetic)
        if not spans_with_persons:
            no_persons += 1; continue
        cands = person_to_org_candidates(spans_with_persons, surname_literal, surname_phonetic)
        if not cands:
            no_candidates += 1; continue
        cand_ids = [c.db_id for c in cands]
        if gold_db in cand_ids:
            any_match += 1
            rank = cand_ids.index(gold_db) + 1
            if rank == 1: top1 += 1
            if rank <= 3: top3 += 1
            if rank <= 5: top5 += 1
            if len(samples_hit) < 5:
                samples_hit.append((cid, text[:50], gold_db, rank, len(cand_ids)))
        elif len(samples_miss) < 5:
            samples_miss.append((cid, text[:50], gold_db, cand_ids[:5]))

    total = len(ruthie_aligns)
    print(f"=== EVALUATION ({total} ALIGNs) ===")
    print(f"  no person spans extracted:   {no_spans}")
    print(f"  spans found, no person hit:  {no_persons}")
    print(f"  persons found, no org cand:  {no_candidates}")
    print(f"  gold among candidates:       {any_match}  ({100*any_match/total:.0f}%)")
    print(f"    top-1: {top1}  ({100*top1/total:.0f}%)")
    print(f"    top-3: {top3}  ({100*top3/total:.0f}%)")
    print(f"    top-5: {top5}  ({100*top5/total:.0f}%)")
    print("\n--- HIT samples ---")
    for cid, t, g, rank, n in samples_hit:
        print(f"  {cid:18} rank={rank}/{n}  gold=db {g:>4}  {t}")
    print("\n--- MISS samples (gold not in candidates) ---")
    for cid, t, g, cands in samples_miss:
        print(f"  {cid:18} gold=db {g:>4}  cands={cands}  {t}")


if __name__ == "__main__":
    evaluate_against_ruthie()
