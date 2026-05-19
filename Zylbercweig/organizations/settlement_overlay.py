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


class _Overlay:
    def __init__(self) -> None:
        self._by_norm: dict[str, ResolvedPlace] = {}
        if _PUNCHLIST.exists():
            with _PUNCHLIST.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    qid = (row.get("suggested_qid") or "").strip()
                    basis = (row.get("basis") or "").strip()
                    if not qid or basis == "exclude" or basis == "unknown":
                        continue
                    key = _norm(row.get("yiddish") or "")
                    if not key:
                        continue
                    self._by_norm[key] = ResolvedPlace(
                        qid=qid,
                        english=(row.get("suggested_english") or "").strip(),
                        yiddish=(row.get("yiddish") or "").strip(),
                        category=(row.get("suggested_category") or "").strip(),
                        source="punchlist",
                    )

    def resolve(self, text: str) -> ResolvedPlace | None:
        # 1. Ask kimatch first (settlement/neighborhood only).
        hit = get_resolver().resolve(text or "")
        if hit:
            return ResolvedPlace(
                qid=hit.qid, english=hit.english, yiddish=hit.yiddish,
                category="settlement", source="kimatch",
            )
        # 2. Fallback to punchlist (covers everything else).
        return self._by_norm.get(_norm(text))


@lru_cache(maxsize=1)
def get_overlay() -> _Overlay:
    return _Overlay()


def resolve_anyplace(text: str) -> ResolvedPlace | None:
    """Top-level entry point. Returns None if neither source matches."""
    return get_overlay().resolve(text)
