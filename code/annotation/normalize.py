"""Text normalization applied to PAGE-XML line text BEFORE annotation.

Currently handles OCR-doubled diacritics: runs of identical combining marks on
the same base letter collapse to one. Examples seen in the wild:
  - חָָ  (double kamatz)            → חָ
  - נוּּר (double dagesh inside) → נוּר
  - שׁׁ  (double shin-dot)         → שׁ

The rule fires only on *adjacent identical* marks. Distinct combining marks on
the same base (e.g. בּ = base + dagesh + sheva) are untouched.
"""
from __future__ import annotations

import re
import unicodedata

# Hebrew points/cantillation range, plus rafe + sin/shin dots etc.
COMBINING = set(chr(c) for c in range(0x0591, 0x05C8))


def collapse_double_niqqud(text: str) -> str:
    """Collapse adjacent identical combining marks. Returns a possibly shorter
    string."""
    out = []
    last = None
    for c in text:
        if c in COMBINING and out and out[-1] == c:
            continue
        out.append(c)
        last = c
    return "".join(out)


def normalize_line(text: str) -> str:
    """Apply all line-level normalizations before annotation."""
    return collapse_double_niqqud(text)
