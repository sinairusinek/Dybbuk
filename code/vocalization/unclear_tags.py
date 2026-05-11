"""
Insert Transkribus 'unclear' annotations into TextLine custom attributes
based on flagged tokens.

Transkribus uses an undocumented but consistent convention:
  custom="readingOrder {index:N;} unclear {offset:O; length:L;}"
Multiple unclear ranges stack space-separated.

We locate each flagged token in each line by searching for its consonantal
form (case- and nikkud-insensitive), so the offsets are correct in the
final vocalized text.
"""

import logging
import re
from lxml import etree

from rules import strip_nikkud

log = logging.getLogger(__name__)

PAGE_NS = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
LINE_TAG = f"{{{PAGE_NS}}}TextLine"


def _strip_punct(t: str) -> str:
    return t.strip(".,;:!?׳״\"' ")


def add_unclear_annotations(tree, flagged_tokens: list[str]) -> int:
    """For each flagged token, find every line where it appears and append
    an unclear annotation to the line's custom attribute. Returns count of
    annotations added."""
    if not flagged_tokens:
        return 0

    keys = sorted({strip_nikkud(_strip_punct(t)) for t in flagged_tokens
                   if _strip_punct(t)}, key=len, reverse=True)
    added = 0

    for line in tree.iter(LINE_TAG):
        # Get the line's text
        unicode_el = None
        for el in line.iter(f"{{{PAGE_NS}}}Unicode"):
            if el.getparent().getparent() is line:
                unicode_el = el
                break
        if unicode_el is None or not unicode_el.text:
            continue
        text = unicode_el.text

        new_ranges = []
        for key in keys:
            # Build a regex matching the consonantal key, allowing nikkud
            # marks (U+0591..U+05C7) interleaved between letters in the
            # actual line text.
            pattern = "".join(
                re.escape(ch) + r"[֑-ׇ]*" for ch in key
            )
            for m in re.finditer(pattern, text):
                new_ranges.append((m.start(), m.end() - m.start()))

        if not new_ranges:
            continue

        existing = line.get("custom", "") or ""
        # Avoid duplicating identical ranges
        already = set(re.findall(
            r"unclear\s*\{offset:(\d+);\s*length:(\d+);\}", existing))
        annot = []
        for off, length in new_ranges:
            sig = (str(off), str(length))
            if sig in already:
                continue
            annot.append(f"unclear {{offset:{off}; length:{length};}}")
            already.add(sig)
            added += 1
        if annot:
            line.set("custom", (existing + " " + " ".join(annot)).strip())

    return added
