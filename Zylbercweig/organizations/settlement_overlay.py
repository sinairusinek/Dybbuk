"""Resolver overlay that augments the kimatch-backed settlement_resolver
with the curated mappings in unresolved_settlements_punchlist.tsv.

`resolve_anyplace(text) -> ResolvedPlace | None` asks the kimatch
resolver first (gated to settlement/neighborhood Wikidata categories),
and falls back to the punchlist for anything else — including ghettos,
countries, named regions, and orthographic variants kimatch hasn't been
updated to handle yet.

Unlike the base resolver, the overlay is NOT category-gated: the
question it answers is "do two strings refer to the same place?", not
"is this an inhabited city?". Use it for cross-source dedup (e.g.
collapsing a DB row's multiple `confirmed_locations` entries that all
point to the same place).
"""
from __future__ import annotations

import csv
import re
import sys
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from settlement_resolver import get_resolver  # noqa: E402

csv.field_size_limit(10**9)

_PUNCHLIST = _HERE / "unresolved_settlements_punchlist.tsv"


@dataclass(frozen=True)
class ResolvedPlace:
    qid: str
    english: str
    yiddish: str
    category: str   # settlement | neighborhood | country | region | ghetto | unknown
    source: str     # "kimatch" or "punchlist"


def _norm(s: str) -> str:
    """NFKD-decompose, drop combining marks, normalize maqaf — matches the
    normalization used in build_unresolved_settlements_punchlist.py."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("־", "-").strip()
    return s


_BIDI_MARKS = "‎‏‪‫‬‭‮⁦⁧⁨⁩"


def _aggressive_norm(s: str) -> str:
    """Stronger normalization for the third-pass overlay fallback.

    Drops combining marks, bidi marks, hyphens, whitespace, underscores;
    collapses Hebrew final letters to medial; lowercases. Lets
    ניו-יאָרק / ניו יאָרק / ניויאָרק collapse to one key.
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = unicodedata.normalize("NFKC", s)
    s = s.translate({ord(c): None for c in _BIDI_MARKS})
    s = s.replace("־", "-")
    s = s.translate(str.maketrans("ךםןףץ", "כמנפצ"))
    s = re.sub(r"[\s\-_]+", "", s)
    return s.lower()


class _Overlay:
    def __init__(self) -> None:
        self._by_norm: dict[str, ResolvedPlace] = {}
        self._by_aggressive: dict[str, ResolvedPlace] = {}
        if _PUNCHLIST.exists():
            with _PUNCHLIST.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    qid = (row.get("suggested_qid") or "").strip()
                    basis = (row.get("basis") or "").strip()
                    if not qid or basis == "exclude" or basis == "unknown":
                        continue
                    yiddish_raw = (row.get("yiddish") or "").strip()
                    key = _norm(yiddish_raw)
                    if not key:
                        continue
                    place = ResolvedPlace(
                        qid=qid,
                        english=(row.get("suggested_english") or "").strip(),
                        yiddish=yiddish_raw,
                        category=(row.get("suggested_category") or "").strip(),
                        source="punchlist",
                    )
                    self._by_norm[key] = place
                    agg = _aggressive_norm(yiddish_raw)
                    if agg:
                        self._by_aggressive.setdefault(agg, place)

        # Build aggressive index from kimatch resolver keys too. Reuses the
        # resolver's already-loaded _by_key dict (post-NFKD/final-letter norm)
        # and just strips hyphens/whitespace.
        resolver = get_resolver()
        for key, hit in getattr(resolver, "_by_key", {}).items():
            agg = re.sub(r"[\s\-_]+", "", key)
            if not agg:
                continue
            self._by_aggressive.setdefault(
                agg,
                ResolvedPlace(
                    qid=hit.qid, english=hit.english, yiddish=hit.yiddish,
                    category="settlement", source="kimatch",
                ),
            )

    def resolve(self, text: str) -> ResolvedPlace | None:
        # 1. Ask kimatch first (settlement/neighborhood only).
        hit = get_resolver().resolve(text or "")
        if hit:
            return ResolvedPlace(
                qid=hit.qid, english=hit.english, yiddish=hit.yiddish,
                category="settlement", source="kimatch",
            )
        # 2. Punchlist exact (combining-mark-tolerant) match.
        cur = self._by_norm.get(_norm(text))
        if cur:
            return cur
        # 3. Aggressive fallback: strip hyphens/whitespace and look up against
        # the union of curated punchlist + kimatch surface forms. Catches
        # spacing/hyphen variants like ניו-יאָרק vs ניו יאָרק vs ניויאָרק.
        agg = _aggressive_norm(text or "")
        if not agg:
            return None
        return self._by_aggressive.get(agg)


@lru_cache(maxsize=1)
def get_overlay() -> _Overlay:
    return _Overlay()


def resolve_anyplace(text: str) -> ResolvedPlace | None:
    """Top-level entry point. Returns None if neither source matches."""
    return get_overlay().resolve(text)
