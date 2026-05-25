"""Tag schema for Transkribus PAGE-XML annotation."""

import re
import unicodedata

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"

PAGE_TYPES = {"titlePage", "castList", "body"}

_NIKUD = re.compile(r"[֑-ׇ]")  # cantillation + vowel points

ALLOWED_TAGS = {
    "speaker": {"xmlid"},
    "stage": {"type"},
    "trailer": {"type"},  # TEI <trailer>: closing label at the end of a division
        # (e.g. "ענדע דער X אקט", "ענדע דער פיעססע"). NOT a stage direction — the
        # 2026-05-24 PI review re-typed these out of `stage`. `type` is OPTIONAL.
    "heading": {"type", "n", "subtype"},  # subtype:songGroup marks song-appendix act labels (not structural acts)
    "role": {"xmlid"},
    "roleDesc": set(),
    "l": {"lg_id"},
    "head": {"lg_id"},
    "lg": {"n", "cont"},  # group marker: one per stanza/song. `cont` is OPTIONAL and only ever `yes` (stanza continues from previous page); omit it otherwise (absence = not a continuation). Per-line `lg` spans are wrong — lines are `l {lg_id}`. Structurer expands cont:yes → continued="yes" in TEI.
    "fw": {"type"},  # forme work (TEI <fw>): printed page numbers, running heads, catchwords, signatures. `type` REQUIRED; page numbers are type:pageNum.
}

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
    "דועט",     # duet
    "איינער",   # eyner — someone
    "דאמען",    # damen — ladies
    "קינדער",   # kinder — children
    "סאפראן", "אלט", "באס", "טענאר",  # song-supplement voice rubrics
}


def is_collective_label(text: str) -> bool:
    """True if a speaker label is a known collective/chorus (no cast entry)."""
    skeleton = _NIKUD.sub("", (text or "").strip()).strip(":־ .")
    return skeleton in KNOWN_COLLECTIVE
# TEI <fw> @type values. Page numbers = pageNum (the common case here).
FW_TYPES = {"pageNum", "header", "footer", "catch", "sig"}
# TEI <stage> @type values (UVic TEI Drama tei_DRSTA). No `mixed`: pick the dominant
# function (Noa re-typed our `mixed` → specific). `type` is REQUIRED on every stage.
STAGE_TYPES = {"setting", "entrance", "exit", "business",
               "delivery", "location", "costume", "novelistic"}


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
        t = attrs.get("type")
        if t is None:
            return "stage span requires a type attribute (no bare stage)"
        if t not in STAGE_TYPES:
            return f"stage.type must be one of {STAGE_TYPES}, got {t!r}"
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
