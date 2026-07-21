"""Tag schema for Transkribus PAGE-XML annotation."""

import re
import unicodedata

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"

PAGE_TYPES = {"titlePage", "castList", "body"}

_NIKUD = re.compile(r"[֑-ׇ]")  # cantillation + vowel points

ALLOWED_TAGS = {
    # speaker.xmlid: single role for solo turns; space-separated xmlids for
    # joint/duet turns (Noa 2026-06-14). The structurer expands a multi-xmlid
    # span into TEI <sp who="#a #b">.
    "speaker": {"xmlid"},
    "stage": {"type", "continued", "xmlid"},  # xmlid → TEI <stage who="#id">.
        # TEI <stage> carries @who via att.ascribed (verified against
        # ref-stage.html, 2026-07-19). Used for compound directions that name
        # the party they are addressed to — `(קאהר ביס)` = "chorus, repeat" →
        # <stage type="repeat" who="#kor">. Same space-separated convention as
        # speaker.xmlid for joint attribution.
    "trailer": {"type"},  # TEI <trailer>: closing label at the end of a division
        # (e.g. "ענדע דער X אקט", "ענדע דער פיעססע"). NOT a stage direction — the
        # 2026-05-24 PI review re-typed these out of `stage`. `type` is OPTIONAL.
    "heading": {"type", "n", "subtype"},  # subtype:songGroup marks song-appendix act labels (not structural acts)
    "role": {"xmlid"},
    "roleDesc": set(),
    "actor": set(),  # TEI castList <actor>: the performer's name printed beside
        # a role. Noa tags these routinely (e.g. Bas Sheva p.6). Added 2026-07-19
        # — it was missing, so every actor span linted as "unknown tag".
    "l": {"lg_id", "continued"},
    "head": {"lg_id", "unit-type"},  # `unit-type:act` is a legacy RA form for
        # act headings, predating heading{type:act}. Accepted so it does not
        # lint as an error; new annotation should use `heading`.
    "lg": {"n", "cont", "continued", "type"},  # legacy RA form `continued:true; type:cont` = the modern `cont:yes`.  # group marker: one per stanza/song. `cont` is OPTIONAL and only ever `yes` (stanza continues from previous page); omit it otherwise (absence = not a continuation). Per-line `lg` spans are wrong — lines are `l {lg_id}`. Structurer expands cont:yes → continued="yes" in TEI.
    "fw": {"type"},  # forme work (TEI <fw>): printed page numbers, running heads, catchwords, signatures. `type` REQUIRED; page numbers are type:pageNum.
}

# TEI editorial/transcription tags the RAs apply in Transkribus. They carry no
# YiDraCor-specific semantics — `build_tei` passes them through — but they must
# be *known* here or the linter reports each one as an unknown tag. `unclear`
# alone accounted for 912 false flags before this was added (2026-07-19).
EDITORIAL_TAGS = {
    "unclear", "sic", "corr", "orig", "reg", "abbr", "expan",
    "supplied", "add", "del", "gap", "note", "foreign", "hi",
}
for _t in EDITORIAL_TAGS:
    ALLOWED_TAGS.setdefault(_t, {"cert", "reason", "resp", "rend", "type"})

# Transkribus-native span types that are not part of our tagset at all. They are
# emitted by the Transkribus editor itself (styling, layout) and must be ignored
# by the validator rather than flagged.
IGNORED_TAGS = {"readingOrder", "textStyle", "Header", "Footer", "structure"}

# Structural division headings. `epilog` added per the 2026-05-24 PI review:
# a standalone "עפילאג" line opens an epilogue division parallel to the acts.
HEADING_TYPES = {"act", "scene", "epilog"}

# Collective / chorus speaker labels that intentionally have NO individual cast
# entry (PI review 2026-05-24: "confirm collective, no individual entry"). The
# flag generator must NOT report these as "missing cast member". Matched on the
# bare (nikud-stripped) consonant skeleton of the speaker label.
KNOWN_COLLECTIVE = {
    "אלע",      # alle — all
    "שטימען",   # shtimen — voices
    "ביידע",    # beyde — both
    "מענער",    # mener — men
    "מעדכען",   # meydkhen — girls
    "מ_דכען",
    "קאהר",     # khor — chorus
    "כאר",
    "קאר",
    "דועט",     # duet
    "איינער",   # eyner — someone
    "דאמען",    # damen — ladies
    "קינדער",   # kinder — children
    # Song-supplement voice rubrics. NOTE (DraCor alignment, 2026-07-02): these
    # should preferentially resolve to the NAMED singer (see prompts.py + §G of
    # castlist_tagging_conventions); they remain here only as the abstract-voice
    # fallback and to suppress spurious "missing cast" flags.
    "סאפראן", "אלט", "באס", "טענאר",
    # Spelling variants found 2026-07-20 while clearing speakers with no xmlid:
    # `אללע` (doubled ל, Hinke Pinke ×4) and `כאהר` (Dos Yudishe Herts ×3).
    # Same class as the `קאר` gap fixed 2026-06-04 — the skeleton match is only
    # as good as the spellings listed here.
    "אללע", "כאהר",
}

# Canonical xmlid per collective skeleton. AUTHORITATIVE — `auto_resolve_flags`
# and `lint_pages` import this rather than keeping their own copies. They each
# carried an identical hand-maintained duplicate until 2026-07-20; they had not
# drifted yet, but the act-heading matcher showed what happens when a rule lives
# in two places (25 of 51 headings untagged), so this is consolidated before it
# can.
COLLECTIVE_XMLID = {
    "אלע": "alle", "אללע": "alle",
    "שטימען": "shtimen", "ביידע": "beyde", "מענער": "mener",
    "מעדכען": "meydkhen", "מ_דכען": "meydkhen",
    "קאהר": "chor", "כאר": "chor", "קאר": "chor", "כאהר": "chor",
    "דועט": "duet", "איינער": "eyner", "דאמען": "damen", "קינדער": "kinder",
    "סאפראן": "sopran", "אלט": "alt", "באס": "bas", "טענאר": "tenor",
}


def collective_skeleton(text: str) -> str:
    """Nikud-stripped, punctuation-stripped speaker label."""
    return _NIKUD.sub("", (text or "").strip()).strip(":־ .")


def is_collective_label(text: str) -> bool:
    """True if a speaker label is a known collective/chorus (no cast entry)."""
    return collective_skeleton(text) in KNOWN_COLLECTIVE
# TEI <fw> @type values. Page numbers = pageNum (the common case here).
FW_TYPES = {"pageNum", "header", "footer", "catch", "sig"}
# TEI <stage> @type values. `type` is REQUIRED on every stage.
# Per TEI P5 spec (ref-stage.html), `@type` MAY carry multiple space-separated
# tokens for a single direction that performs multiple functions, e.g.
# `type="entrance modifier"`. The literal value `mixed` is a single-value
# fallback for when constituent functions can't be enumerated — if `mixed` is
# used it MUST be the only value.
# History: `mixed` was deprecated 2026-05-31, re-instated 2026-06-14 (Noa
# narrow rule), then deprecated again 2026-06-18 by Sinai+Noa in favor of
# TEI-principled space-separated multi-token typing — `(לעגט וועג דיא
# האַרפֿע— ערשיינט)` is now `type="entrance business"`, not `type="mixed"`.
# The literal `mixed` remains accepted as a fallback but the pipeline
# prefers multi-token output.
# `repeat` was used 2026-07-19 for the printed `(ביס)` mark, then RETIRED
# 2026-07-21 (Sinai): the mark is now `type="delivery"` like every other
# musical performance instruction, so `repeat` is no longer a valid token —
# lint will flag any straggler. History kept so the decision stays legible.
STAGE_TOKENS = {"setting", "entrance", "exit", "business",
                "delivery", "location", "costume", "novelistic", "modifier"}
# `mixed` is special: when present it must be the only token.
STAGE_TYPES = STAGE_TOKENS | {"mixed"}


def _validate_stage_type(t: str) -> str | None:
    """Validate a `@type` value on <stage>. Returns error message or None.

    Accepts: a single token from STAGE_TYPES, or space-separated tokens from
    STAGE_TOKENS (not `mixed`). The literal `mixed` must be the only value.
    """
    if t is None or t == "":
        return "stage span requires a type attribute (no bare stage)"
    toks = t.split()
    if not toks:
        return "stage span requires a non-empty type attribute"
    if "mixed" in toks and len(toks) > 1:
        return (f"stage.type {t!r}: per TEI spec, when `mixed` is used "
                "it must be the only value")
    bad = [tok for tok in toks if tok not in STAGE_TYPES]
    if bad:
        return (f"stage.type tokens not in vocab: {bad!r}; "
                f"allowed = {sorted(STAGE_TYPES)}")
    return None


def parse_custom(s: str) -> list[tuple[str, dict]]:
    """Parse a `custom` attribute into a list of (tag, attrs)."""
    out = []
    for m in re.finditer(r"(\w+)\s*\{([^}]*)\}", s or ""):
        tag = m.group(1)
        attrs = {}
        for kv in m.group(2).split(";"):
            kv = kv.strip()
            if not kv:
                continue
            if ":" in kv:
                k, v = kv.split(":", 1)
                attrs[k.strip()] = v.strip()
        out.append((tag, attrs))
    return out


def serialize_custom(entries: list[tuple[str, dict]]) -> str:
    """Serialize back to the Transkribus custom-attribute syntax."""
    parts = []
    for tag, attrs in entries:
        body = " ".join(f"{k}:{v};" for k, v in attrs.items())
        parts.append(f"{tag} {{{body}}}" if body else f"{tag} {{}}")
    return " ".join(parts)


def append_custom(existing: str, tag: str, attrs: dict) -> str:
    entries = parse_custom(existing)
    entries.append((tag, attrs))
    return serialize_custom(entries)


def dedup_entries(entries: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    """For each (tag, offset, length) triple keep only the LAST occurrence —
    i.e. the most recent decision wins. Used to clean up duplicate spans
    that accumulate when our pipeline re-pushes on top of a prior push
    (Transkribus layers spans rather than replacing). readingOrder spans
    are passed through unchanged. Preserves overall entry order."""
    # Walk in reverse, keeping first-seen (= last in original order)
    seen: set[tuple] = set()
    out_rev: list[tuple[str, dict]] = []
    for tag, a in reversed(entries):
        if tag == "readingOrder":
            out_rev.append((tag, a)); continue
        key = (tag, a.get("offset", ""), a.get("length", ""))
        if key in seen:
            continue
        seen.add(key)
        out_rev.append((tag, a))
    return list(reversed(out_rev))


def set_region_structure(existing: str, page_type: str) -> str:
    """Set/replace `structure {type:...;}` on a region custom attribute."""
    entries = [(t, a) for (t, a) in parse_custom(existing) if t != "structure"]
    entries.append(("structure", {"type": page_type}))
    return serialize_custom(entries)


def sanitize_xmlid(s: str) -> str:
    """Lowercase ASCII, alnum + underscore. Fallback to 'role' if empty."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if ord(c) < 128)
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", s).strip("_").lower()
    return s or "role"


def validate_span(line_text: str, span: dict) -> str | None:
    """Return None if valid, else an error string."""
    tag = span.get("tag")
    if tag in IGNORED_TAGS:
        return None  # Transkribus-native, not ours to validate
    if tag not in ALLOWED_TAGS:
        return f"unknown tag {tag!r}"
    off = span.get("offset")
    ln = span.get("length")
    if not isinstance(off, int) or not isinstance(ln, int):
        return "offset/length must be ints"
    if off < 0 or ln <= 0 or off + ln > len(line_text):
        return f"span out of range (line len={len(line_text)})"
    attrs = span.get("attrs") or {}
    unknown = set(attrs) - ALLOWED_TAGS[tag]
    if unknown:
        return f"unknown attrs on {tag}: {sorted(unknown)}"
    if tag == "heading":
        t = attrs.get("type")
        if t not in HEADING_TYPES:
            return f"heading.type must be one of {HEADING_TYPES}, got {t!r}"
    if tag == "stage":
        err = _validate_stage_type(attrs.get("type"))
        if err:
            return err
    if tag == "role" and not attrs.get("xmlid"):
        return "role span requires xmlid attribute"
    if tag == "speaker" and not attrs.get("xmlid"):
        return "speaker span requires xmlid attribute (link to a role xml:id)"
    if tag == "lg":
        cont = attrs.get("cont")
        if cont is not None and cont != "yes":
            return f"lg.cont, if present, must be 'yes' (omit it for non-continuations), got {cont!r}"
    if tag == "fw":
        t = attrs.get("type")
        if t is None:
            return "fw span requires a type attribute (page numbers are type:pageNum)"
        if t not in FW_TYPES:
            return f"fw.type must be one of {FW_TYPES}, got {t!r}"
    return None


# --- act/scene heading matcher -----------------------------------------------
# Sinai 2026-07-20. Lived in two copies (auto_annotate.HEADING_ACT_RX and
# heuristic_annotate.ACT_HEAD), both of which accepted ONLY a Hebrew *word*
# ordinal before `אקט`, anchored to end of line. Corpus audit: 26 act headings
# tagged, 25 untagged. The misses were
#   * Roman numerals, before or after the word — `I. אַקט`, `אַקט .II`,
#     `V. אַקט.`, `III אַקט` (Blimele, DovidsFidele, Ezra, Isha Raa). The
#     numeral-after forms are an RTL artifact of how the period is stored.
#   * content following the heading — Isha Raa p5 `I. אַקְט    (I. Rittornetto)`
#     failed on the `$` anchor alone, independently of the numeral.
#   * the ordinal spelling `פיערטער` (BasSheva p54), absent from the table.
# One matcher now, called by both.
ACT_ORDINALS = {
    "ערשטער": 1, "ראשון": 1, "ערשטע": 1, "ערשטען": 1,
    "צווייטער": 2, "צוייטער": 2, "צווייטע": 2, "צווייטען": 2, "שני": 2,
    "דריטער": 3, "דריטע": 3, "דריטען": 3, "שלישי": 3,
    "פערטער": 4, "פערטע": 4, "פיערטער": 4, "פיערטע": 4, "רביעי": 4,
    "פינפטער": 5, "פינפטע": 5, "חמישי": 5,
    "זעקסטער": 6, "זעקסטע": 6, "שישי": 6,
    "זיבעטער": 7, "זיבעטע": 7, "שביעי": 7,
}
_ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7}
_ORD_ALT = "|".join(sorted(map(re.escape, ACT_ORDINALS), key=len, reverse=True))
# `אקט` must not be followed by another Hebrew letter — otherwise `אקטען`
# ("acts") matches, and every title page says `אין 4 אקטען`.
_AKT = r"אקט(?![א-ת])"
_ROM = r"(?:VII|VI|IV|V|III|II|I)"
_TAIL = r"(?:\s*[.,׃:]?\s*(?:\(.*\))?\s*)"
_ACT_RX = re.compile(
    rf"^\s*(?:(?P<ord>{_ORD_ALT})\s+{_AKT}"
    # RTL storage puts the numeral's period on either side of it, and sometimes
    # at the very START of the line: Dovid's Fidele p.46 stores `III. אקט` as
    # `.III אַקט`. Found 2026-07-20, after the first sweep had already run —
    # that page kept only a legacy `head` span and its opening tableau stayed
    # untyped in consequence. Allow a leading period before the numeral.
    rf"|\.?\s*(?P<rom1>{_ROM})\s*\.?\s*{_AKT}"
    rf"|{_AKT}\s*\.?\s*(?P<rom2>{_ROM}))"
    rf"{_TAIL}$"
)
_SCENE_RX = re.compile(rf"^\s*סצענע\s+(?P<n>{_ORD_ALT}|{_ROM}|\d+){_TAIL}$")


def parse_act_heading(text: str):
    """Return the act number for an act-heading line, else None.

    Input may carry nikud; it is stripped here. Matches a Hebrew word ordinal
    or a Roman numeral on either side of `אקט`, tolerating a trailing period
    and a trailing parenthetical (`I. אַקְט (I. Rittornetto)`).
    """
    m = _ACT_RX.match(_NIKUD.sub("", text or ""))
    if not m:
        return None
    if m.group("ord"):
        return ACT_ORDINALS[m.group("ord")]
    return _ROMAN[m.group("rom1") or m.group("rom2")]


def parse_scene_heading(text: str):
    """Return the scene number (as a string) for a scene-heading line, else None."""
    m = _SCENE_RX.match(_NIKUD.sub("", text or ""))
    if not m:
        return None
    n = m.group("n")
    return str(ACT_ORDINALS.get(n) or _ROMAN.get(n) or n)
