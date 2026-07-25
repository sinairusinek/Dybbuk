"""Shared helpers for the plays/production knowledge-graph pipeline.

Centralizes Yiddish title normalization so that A1 (registry), A2 (corpus
sweep), B1 (edition linking) and C2 (evaluation alignment) all match on the
same key. The normalization is deliberately lossy — it produces a MATCH KEY,
never a display form.
"""
from __future__ import annotations

import csv
import pathlib
import re
import sys
import unicodedata

csv.field_size_limit(sys.maxsize)

HERE = pathlib.Path(__file__).resolve().parent
PEOPLE_DIR = HERE.parent / "people"
ORGS_DIR = HERE.parent / "organizations"
ZIBN_WORKING = HERE.parent / "zibn-shtern" / "data" / "working"
YIDRACOR_DATA = HERE.parent.parent / "YiDraCor" / "data"
EDITION_METADATA_DIR = HERE.parent.parent / "YiDraCor" / "edition metadata"

PLAYS_DB_TSV = HERE / "plays_db.tsv"
COLLISIONS_TSV = HERE / "play_title_collisions.tsv"
TITLE_HITS_TSV = HERE / "play_title_hits.tsv"
FLAGSHIP_TSV = HERE / "kg_extraction_flagship.tsv"
DRAFTS_TSV = HERE / "kg_extraction_drafts.tsv"
ADJUDICATION_TSV = HERE / "kg_adjudication.tsv"
LINK_REVIEW_TSV = HERE / "kg_link_review.tsv"
KG_DIR = HERE / "kg"
EVAL_DIR = HERE / "eval"
GOLD_DIR = HERE / "gold"

ENTRY_TEXTS_TSV = PEOPLE_DIR / "entry_texts.tsv"
PEOPLE_DB_TSV = PEOPLE_DIR / "people_db.tsv"

# Default scope: Lateiner + Hurwitz. --author-db-ids overrides for scale-up.
DEFAULT_AUTHOR_DB_IDS = ["683", "684"]

# Surname patterns per author db_id, matched against NORMALIZED text
# (norm_yiddish output: no diacritics, finals folded, digraphs folded).
AUTHOR_SURNAME_PATTERNS = {
    "683": re.compile(r"לאטיינער|לאטענער"),
    "684": re.compile(r"הורוויץ|הורוויטש|הורביץ|האראוויץ|הורװיץ"),
}

_DIGRAPHS = {"װ": "וו", "ױ": "וי", "ײ": "יי"}
_FINALS = str.maketrans("ךםןףץ", "כמנפצ")


def norm_yiddish(s: str) -> str:
    """Yiddish match key: strip nikud/rafe, fold digraphs + final letters,
    drop punctuation, keep only Hebrew letters / digits / spaces."""
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    for k, v in _DIGRAPHS.items():
        s = s.replace(k, v)
    s = s.translate(_FINALS)
    s = re.sub(r"[^0-9א-ת ]", " ", s)
    return " ".join(s.split())


_ODER_SPLIT = re.compile(r"(?:^|\s)אדער(?:\s|$)")
_LEAD_ARTICLE = re.compile(r"^(?:דער|די|דאס|דעם|אן|א)\s+")


def title_segments(title: str) -> list[str]:
    """Normalized אָדער-split segments of a title, ≥4 chars, deduped in order."""
    norm = norm_yiddish(title)
    segs, seen = [], set()
    for part in _ODER_SPLIT.split(norm):
        part = part.strip()
        if len(part) >= 4 and part not in seen:
            seen.add(part)
            segs.append(part)
    return segs


def loose_key(segment: str) -> str:
    """Segment with the leading article stripped (fuzzy-tier key)."""
    return _LEAD_ARTICLE.sub("", segment).strip()


def read_tsv(path: pathlib.Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def write_tsv(path: pathlib.Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def load_plays_db() -> list[dict]:
    return read_tsv(PLAYS_DB_TSV)


def author_comention(norm_text: str, author_db_id: str) -> bool:
    pat = AUTHOR_SURNAME_PATTERNS.get(author_db_id)
    return bool(pat and pat.search(norm_text))


# Shared schema for every extraction surface (flagship, Gemini drafts,
# adjudication): one row per (fact, participant). Surface forms only —
# entity identification happens in link_entities.py, never at extraction.
EXTRACTION_FIELDS = [
    "fact_id", "person_id", "xml_id", "source", "window_id", "hit_ids",
    "play_title_surface", "play_id_hint", "fact_type",
    "person_surface", "person_role", "character",
    "org_surface", "venue_surface", "settlement_surface", "country",
    "date_start", "date_end", "date_precision",
    "evidence_quote", "evidence_ok", "confidence", "model", "drafted_at", "notes",
]

FACT_TYPES = ["production", "authorship", "translation_adaptation", "music",
              "publication", "premiere", "mention_only"]
PERSON_ROLES = ["actor", "director", "composer", "prompter", "translator",
                "adapter", "producer", "author", "other"]
DATE_PRECISIONS = ["day", "month", "year", "circa", "none"]
