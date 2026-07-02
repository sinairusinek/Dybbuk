"""Shared loaders for the Phase C person-hub pipeline.

Centralizes the joins that are easy to get wrong:
  - xml_id → person_id resolution (30 xml_ids collide across volumes; the
    review TSV's `volume` column is only 61% populated, so resolution is
    volume-aware where possible and flags the rest as ambiguous)
  - person_id → db_id via people_alignment_review.tsv, excluding the 19
    xml_ids parked in alignment_disagreements.tsv (pending PI)
  - heading → person_id index for the mention-validation chain

All consumers: derive_mention_alignments.py, build_person_hub.py,
resolve_surname_mentions.py.
"""
from __future__ import annotations

import csv
import pathlib
import sys
from collections import defaultdict

csv.field_size_limit(sys.maxsize)

HERE = pathlib.Path(__file__).resolve().parent

EXTRACTED_TSV = HERE / "people_extracted.tsv"
REVIEW_TSV = HERE / "people_alignment_review.tsv"
DISAGREEMENTS_TSV = HERE / "alignment_disagreements.tsv"
DB_TSV = HERE / "people_db.tsv"
NON_PERSON_TSV = HERE / "non_person_entries_phaseA.tsv"


def read_tsv(path: pathlib.Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def write_tsv(path: pathlib.Path, rows: list[dict], fields: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def load_extracted() -> list[dict]:
    return read_tsv(EXTRACTED_TSV)


def load_db_rows() -> list[dict]:
    return read_tsv(DB_TSV)


def non_person_person_ids() -> set[str]:
    return {r["person_id"] for r in read_tsv(NON_PERSON_TSV) if r.get("person_id")}


def volume_of_person_id(person_id: str) -> str:
    # person_id format: P-<vol>-<xml_id>
    parts = (person_id or "").split("-", 2)
    return parts[1] if len(parts) >= 3 else ""


def xml_id_of_person_id(person_id: str) -> str:
    parts = (person_id or "").split("-", 2)
    return parts[2] if len(parts) >= 3 else ""


class XmlResolver:
    """Resolve a bare xml_id (optionally with a volume hint) to person_id(s)."""

    def __init__(self, extracted: list[dict]):
        self.by_xml: dict[str, list[dict]] = defaultdict(list)
        self.by_person_id: dict[str, dict] = {}
        for r in extracted:
            self.by_xml[r["xml_id"]].append(r)
            self.by_person_id[r["person_id"]] = r

    def resolve(self, xml_id: str, volume_hint: str = "") -> tuple[str, str]:
        """Return (person_id, status). status in {ok, ambiguous, missing}."""
        rows = self.by_xml.get(xml_id, [])
        if not rows:
            return "", "missing"
        if len(rows) == 1:
            return rows[0]["person_id"], "ok"
        vh = _norm_volume(volume_hint)
        if vh:
            vol_rows = [r for r in rows if _norm_volume(r["volume"]) == vh]
            if len(vol_rows) == 1:
                return vol_rows[0]["person_id"], "ok"
        return "", "ambiguous"


def _norm_volume(v: str) -> str:
    """'2.0' / '2' / 2 → '2'."""
    v = str(v or "").strip()
    if not v:
        return ""
    try:
        return str(int(float(v)))
    except ValueError:
        return v


def load_disagreement_xml_ids() -> set[str]:
    return {r["xml_id"] for r in read_tsv(DISAGREEMENTS_TSV) if r.get("xml_id")}


def build_person_db_map(extracted: list[dict] | None = None):
    """person_id → db_id from RA review rows, volume-aware.

    Returns (mapping, report) where report lists skipped rows:
      [(xml_id, reason)] with reason in {conflicted_disagreement,
      ambiguous_xml_id, multiple_db_ids, missing_entry}.
    """
    extracted = extracted if extracted is not None else load_extracted()
    resolver = XmlResolver(extracted)
    disagreements = load_disagreement_xml_ids()

    per_person: dict[str, set[str]] = defaultdict(set)
    report: list[tuple[str, str]] = []
    for r in read_tsv(REVIEW_TSV):
        xml, db = (r.get("xml_id") or "").strip(), (r.get("db_id") or "").strip()
        if not xml or not db:
            continue
        if xml in disagreements:
            report.append((xml, "conflicted_disagreement"))
            continue
        pid, status = resolver.resolve(xml, r.get("volume", ""))
        if status == "missing":
            report.append((xml, "missing_entry"))
            continue
        if status == "ambiguous":
            report.append((xml, "ambiguous_xml_id"))
            continue
        per_person[pid].add(db)

    mapping: dict[str, str] = {}
    for pid, dbs in per_person.items():
        if len(dbs) == 1:
            mapping[pid] = next(iter(dbs))
        else:
            report.append((xml_id_of_person_id(pid), "multiple_db_ids"))
    return mapping, report


def build_heading_index(extracted: list[dict] | None = None) -> dict[str, list[str]]:
    """Exact stripped heading → [person_id]. Callers add their own normalized
    fallback if needed (only 3 of the 3,446 validated headings miss exact)."""
    extracted = extracted if extracted is not None else load_extracted()
    idx: dict[str, list[str]] = defaultdict(list)
    for r in extracted:
        h = (r.get("heading") or "").strip()
        if h:
            idx[h].append(r["person_id"])
    return idx


def mention_volume(src_file: str) -> str:
    """'volume_1IIIorg.json' / 'Volume5IIIorg.json' → '1' / '5'."""
    for ch in src_file or "":
        if ch.isdigit():
            return ch
    return ""
