"""General-purpose speaker-label detection and normalization.

Patterns this module handles (applicable to any Yiddish play, any edition):
  • pure label:       "name [:.?!]"
  • mixed before:     "name (stage)[:.?!]"
  • mixed after:      "name[:.?!] (stage)"

The general rule: in any speaker-label line, strip nikud from the name part.
Per-edition canonical forms (e.g. Sobele's dagesh, Turniver's shuruk) can be
applied on top via the `canonical` parameter (a dict mapping bare skeleton →
canonical form).
"""

import re

NIKUD_RE = re.compile(r"[ְ-ׇ]")


def strip_nikud(s: str) -> str:
    return NIKUD_RE.sub("", s)


PURE_LABEL_RE = re.compile(r"^\s*([א-ת\s]+?)\s*([\.\:\?\!\s]*)$")
PAREN_BEFORE_RE = re.compile(
    r"^(\s*)([א-תְ-ׇ][א-תְ-ׇ\s]*?)\s*(\([^)]*\))\s*([\:\.\?\!]?\s*)$"
)
PAREN_AFTER_RE = re.compile(
    r"^(\s*)([א-תְ-ׇ][א-תְ-ׇ\s]*?)\s*([\:\.\?\!])\s*(\([^)]*\))\s*$"
)


def _canonicalize(name_stripped: str, canonical: dict | None) -> str:
    skeleton = name_stripped.replace(" ", "")
    if canonical and skeleton in canonical:
        return canonical[skeleton]
    return name_stripped


def normalize_speaker_label(text: str, canonical: dict | None = None) -> str:
    """Return `text` with speaker-name vocalization normalized.

    `canonical`: optional {skeleton → canonical_form} for per-edition overrides.
    Returns `text` unchanged if it doesn't match any speaker-label pattern.
    """
    if not text.strip():
        return text

    # name (stage):
    m = PAREN_BEFORE_RE.fullmatch(text)
    if m:
        lead, name, paren, tail = m.group(1), m.group(2), m.group(3), m.group(4)
        new_name = _canonicalize(strip_nikud(name).rstrip(), canonical)
        return f"{lead}{new_name} {paren}{tail}"

    # name: (stage)
    m = PAREN_AFTER_RE.fullmatch(text)
    if m:
        lead, name, term, paren = m.group(1), m.group(2), m.group(3), m.group(4)
        new_name = _canonicalize(strip_nikud(name).rstrip(), canonical)
        return f"{lead}{new_name}{term} {paren}"

    # pure label: name [:.?!]
    stripped = strip_nikud(text)
    m = PURE_LABEL_RE.fullmatch(stripped)
    if m:
        skeleton = m.group(1).replace(" ", "")
        if canonical and skeleton in canonical:
            leading_ws = len(stripped) - len(stripped.lstrip())
            trailing = m.group(2)
            return " " * leading_ws + canonical[skeleton] + trailing
        # No canonical defined → leave the pure label alone (edition decides).

    return text
