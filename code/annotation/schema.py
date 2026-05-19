"""Tag schema for Transkribus PAGE-XML annotation."""

import re
import unicodedata

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"

PAGE_TYPES = {"titlePage", "castList", "body"}

ALLOWED_TAGS = {
    "speaker": {"xmlid"},
    "stage": {"type"},
    "heading": {"type", "n"},
    "role": {"xmlid"},
    "roleDesc": set(),
    "l": {"lg_id"},
    "head": {"lg_id"},
    "lg": {"n", "continued"},
}

HEADING_TYPES = {"act", "scene"}
# TEI <stage> @type values we support (a subset of the TEI Guidelines list).
STAGE_TYPES = {"setting", "entrance", "exit", "business",
               "delivery", "location", "mixed"}


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
        if t is not None and t not in STAGE_TYPES:
            return f"stage.type must be one of {STAGE_TYPES}, got {t!r}"
    if tag == "role" and not attrs.get("xmlid"):
        return "role span requires xmlid attribute"
    if tag == "speaker" and not attrs.get("xmlid"):
        return "speaker span requires xmlid attribute (link to a role xml:id)"
    return None
