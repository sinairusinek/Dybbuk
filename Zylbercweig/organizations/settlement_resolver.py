"""Settlement → Wikidata QID resolver, backed by zibn-shtern kimatch outputs.

Loads `places_unified_corrected.csv` and `kimatch_matched_full.tsv` and exposes a
single `resolve(text) -> ResolvedSettlement | None` entry point. Use the QID as
the canonical settlement key for cross-script grouping (DB English ↔ cluster
Yiddish).
"""
from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

csv.field_size_limit(10**9)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_UNIFIED = _REPO_ROOT / "Zylbercweig/zibn-shtern/data/working/places_unified_corrected.csv"
_KIMATCH = _REPO_ROOT / "Zylbercweig/zibn-shtern/data/working/kimatch_matched_full.tsv"

# Wikidata categories that represent inhabited places usable as a "settlement"
# key for org grouping. Cemeteries / death sites / countries / provinces are
# excluded — we want cities.
_SETTLEMENT_CATEGORIES = {"settlement", "neighborhood"}

_BIDI_MARKS = "‎‏‪‫‬‭‮⁦⁧⁨⁩"

# Latin letters NFKD does not decompose (they are distinct letters, not
# base+combining), so "Lodz" never reaches the gazetteer key "Łódź".
_LATIN_FOLD = str.maketrans({"ł": "l", "ø": "o", "đ": "d", "ħ": "h", "ŧ": "t"})

# Trailing/leading punctuation that leaks in from address splitting ("Iași,").
# Parens are deliberately NOT stripped — "new york (n.y.)" is a real key.
_EDGE_PUNCT = " ,.;:'\"־-"

# Historical / colloquial exonyms → the normalized key the gazetteer actually
# carries (Wikidata's *current official* label). Values must be already-
# normalized strings present in `_by_key`; a miss is silently ignored, so a
# gazetteer change degrades to "unresolved", never to a wrong QID.
_ALIASES = {
    "new york": "new york city",
    "odessa": "odesa",
    "kiev": "kiev (ukraine)",
    "vilna": "vilnius",
    "lemberg": "lviv",
    "breslau": "wroclaw",
    "pressburg": "bratislava",
}


@dataclass(frozen=True)
class ResolvedSettlement:
    qid: str
    english: str
    yiddish: str


def _normalize(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    # Drop Hebrew niqqud + cantillation (combining marks U+0591–U+05C7) so
    # פֿילאַדעלפֿיע ↔ פֿילאדעלפֿיע match.
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = unicodedata.normalize("NFKC", s)
    s = s.translate({ord(c): None for c in _BIDI_MARKS})
    s = s.replace("־", "-")  # Hebrew maqaf → ASCII hyphen
    # Collapse Hebrew final letters to their medial form so stem-stripping
    # works ("ברוקלינער" → stem "ברוקלינ" matches kimatch key "ברוקלין").
    s = s.translate(str.maketrans("ךםןףץ", "כמנפצ"))
    s = re.sub(r"[\s\-_]+", " ", s).strip().lower()
    s = s.translate(_LATIN_FOLD)
    s = s.strip(_EDGE_PUNCT)
    return s


class SettlementResolver:
    def __init__(self) -> None:
        self._by_key: dict[str, ResolvedSettlement] = {}
        self._load()

    def _add(self, key: str, resolved: ResolvedSettlement) -> None:
        k = _normalize(key)
        if not k:
            return
        # First write wins — unified file is loaded first (PI-corrected).
        self._by_key.setdefault(k, resolved)

    def _load(self) -> None:
        if _UNIFIED.exists():
            with _UNIFIED.open() as f:
                for row in csv.DictReader(f):
                    if (row.get("resolved_category") or "") not in _SETTLEMENT_CATEGORIES:
                        continue
                    qid = (row.get("qid") or "").strip()
                    if not qid:
                        continue
                    res = ResolvedSettlement(
                        qid=qid,
                        english=(row.get("wikidata_label_en") or row.get("clustered_value") or "").strip(),
                        yiddish=(row.get("wikidata_label_yi") or "").strip(),
                    )
                    for field in ("source_value", "clustered_value", "wikidata_label_en", "wikidata_label_yi", "settlement"):
                        v = (row.get(field) or "").strip()
                        if v:
                            self._add(v, res)

        if _KIMATCH.exists():
            with _KIMATCH.open() as f:
                for row in csv.DictReader(f, delimiter="\t"):
                    if (row.get("resolved_category") or "") not in _SETTLEMENT_CATEGORIES:
                        continue
                    qid = (row.get("wikidata_qid") or "").strip()
                    if not qid:
                        continue
                    res = ResolvedSettlement(
                        qid=qid,
                        english=(row.get("english_name") or "").strip(),
                        yiddish=(row.get("wikidata_yi") or "").strip(),
                    )
                    for field in ("source_value", "english_name", "wikidata_yi", "kima_rom", "kima_heb"):
                        v = (row.get(field) or "").strip()
                        if v:
                            self._add(v, res)

    def resolve(self, text: str | None) -> ResolvedSettlement | None:
        if not text:
            return None
        key = _normalize(text)
        hit = self._by_key.get(key)
        if hit:
            return hit
        alias = _ALIASES.get(key)
        if alias:
            hit = self._by_key.get(alias)
            if hit:
                return hit
        # Yiddish adjectival suffix: "Xער" / "Xישער" / "Xישע" means "of X".
        # The stem often ends in a consonant while kimatch keys end in a vowel
        # (שיקאַגער → stem שיקאַג; kimatch key שיקאַגאָ). Try stem alone and stem
        # + common final vowels/letters.
        if any("א" <= c <= "ת" for c in key):
            for suffix in ("ישער", "ישע", "ער"):
                if key.endswith(suffix) and len(key) > len(suffix) + 2:
                    stem = key[: -len(suffix)]
                    for tail in ("", "ע", "א", "אָ", "ן", "ין"):
                        hit = self._by_key.get(stem + tail)
                        if hit:
                            return hit
        return None


@lru_cache(maxsize=1)
def get_resolver() -> SettlementResolver:
    return SettlementResolver()


# ─── Sub-city containment ─────────────────────────────────────────────────
# kimatch's `resolved_category` lumps "neighborhood" in with "settlement"
# (_SETTLEMENT_CATEGORIES above), so Brownsville and Brooklyn arrive as
# co-equal top-level buckets. Rather than change that categorisation — which
# would drop boroughs out of the lens entirely — we keep storage flat and roll
# up at query time using a small curated parent table.

_PARENTS_FILE = Path(__file__).resolve().parent / "settlement_parents.tsv"


@lru_cache(maxsize=1)
def load_parents() -> dict[str, str]:
    """qid → parent_qid, from settlement_parents.tsv. Missing file = no
    containment, which degrades to today's flat behaviour."""
    out: dict[str, str] = {}
    if not _PARENTS_FILE.exists():
        return out
    with _PARENTS_FILE.open() as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2 or parts[0] == "qid":
                continue
            qid, parent = parts[0].strip(), parts[1].strip()
            if qid and parent and qid != parent:
                out[qid] = parent
    return out


def parent_of(qid: str) -> str | None:
    return load_parents().get(qid)


def ancestors_of(qid: str) -> list[str]:
    """All containing QIDs, nearest first. Cycle-safe."""
    out: list[str] = []
    seen = {qid}
    cur = load_parents().get(qid)
    while cur and cur not in seen:
        out.append(cur)
        seen.add(cur)
        cur = load_parents().get(cur)
    return out


def rollup_qid(qid: str) -> str:
    """The top-level city a QID belongs to (itself if it is one)."""
    chain = ancestors_of(qid)
    return chain[-1] if chain else qid


def descendants_of(qid: str) -> list[str]:
    """All QIDs contained in `qid`, at any depth."""
    parents = load_parents()
    return [q for q in parents if qid in ancestors_of(q)]
