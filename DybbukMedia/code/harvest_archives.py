"""Harvest the archival-holdings sheets the first pass left out.

Four further sheets of the catalogue workbook describe physical objects rather
than bare metadata:

  Hurwitz music        439  handwritten + printed scores, YIVO collections.
                            The Hurwitz counterpart to Score-print-editions,
                            whose absence is why Hurwitz looked thin.
  VilneMusicArchive     66  Vilne archive scores, with a Digital Object column.
  KaminskaPlaysR8       26  YIVO RG8 folders, with digipres.cjh.org links and
                            folder/object id ranges.
  ZachBakerLOCMarwick   11  Library of Congress copyright deposits.

Deliberately NOT harvested: PerlmutterMSScards transcribes catalogue CARDS that
are already covered as doc 899977 — those rows index the scans rather than
naming new objects, so turning them into items would double-count.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import openpyxl

from common import (CATALOGUE, clean, load_entities, load_title_index, merge,
                    propose_title, read_manifest, write_manifest, year_of)


def rows_of(wb, sheet: str, skip: int = 1) -> list[tuple]:
    return list(wb[sheet].iter_rows(values_only=True))[skip:]


def cell(row: tuple, idx: int):
    """Column `idx` of `row`, or None — read-only sheets yield ragged tuples."""
    return row[idx] if idx < len(row) else None


def harvest_hurwitz_music(wb, ents, idx) -> list[dict]:
    """Scores from the Hurwitz music sheet.

    `Type` distinguishes a handwritten score from a printed one, which is the
    difference between a manuscript and a print item.
    """
    out = []
    for i, r in enumerate(rows_of(wb, "Hurwitz music"), start=1):
        title, kind = clean(cell(r, 0)), clean(cell(r, 1))
        if not title:
            continue
        handwritten = "handwritten" in kind.lower() or "ms" == kind.lower()
        ids, status = propose_title(ents, idx, title)
        source = clean(cell(r, 10))
        out.append({
            "media_id": f"hurwmus-{i:04d}",
            "source": "dorot",
            "item_type": "manuscript" if handwritten else "sheet_music",
            "title": title,
            "date": year_of(cell(r, 2)),
            "holding_institution": ("YIVO" if "yivo" in source.lower()
                                    else source.split(",")[0][:60]),
            "collection": source,
            "source_url": "",
            "fetch_hint": (f"archival request: {source}" if source else ""),
            "rights": "unknown",
            "entity_ids": ids,
            "play_key": title,
            "link_status": status,
            "status": "PENDING",
            "notes": "; ".join(x for x in (
                kind, clean(cell(r, 3)),
                f"composer/arranger: {clean(cell(r, 4))}" if clean(cell(r, 4)) else "",
                f"cover: {clean(cell(r, 7))}" if clean(cell(r, 7)) else "",
                clean(cell(r, 5))[:160]) if x),
        })
    return out


def harvest_vilne(wb, ents, idx) -> list[dict]:
    """Vilne Music Archive scores."""
    out = []
    for i, r in enumerate(rows_of(wb, "VilneMusicArchive"), start=1):
        title, play_key = clean(cell(r, 0)), clean(cell(r, 2))
        if not (title or play_key):
            continue
        ids, status = propose_title(ents, idx, play_key or title)
        out.append({
            "media_id": f"vilne-{i:04d}",
            "source": "dorot",
            "item_type": "sheet_music",
            "title": title.rstrip(" ,"),
            "date": year_of(cell(r, 5)),
            "date_note": clean(cell(r, 5)),
            "holding_institution": "YIVO",
            "collection": "Vilne Music Archive",
            "signature": clean(cell(r, 7)),
            "source_url": clean(cell(r, 11)),
            "fetch_hint": ("Vilne Music Archive; see Digital Object column"
                           if clean(cell(r, 11)) else "archival request: YIVO"),
            "rights": "unknown",
            "entity_ids": ids,
            "play_key": play_key,
            "link_status": status,
            "status": "PENDING",
            "notes": "; ".join(x for x in (
                clean(cell(r, 6)), clean(cell(r, 8))[:160],
                f"parts: {clean(cell(r, 10))[:90]}" if clean(cell(r, 10)) else "") if x),
        })
    return out


def harvest_kaminska_r8(wb, ents, idx) -> list[dict]:
    """YIVO RG8 folders — manuscript playscripts with delivery links."""
    out = []
    for i, r in enumerate(rows_of(wb, "KaminskaPlaysR8"), start=1):
        play_key, folder = clean(cell(r, 0)), clean(cell(r, 2)).rstrip(";")
        desc = clean(cell(r, 1))
        if not (play_key or folder or desc):
            continue
        ids, status = propose_title(ents, idx, play_key or desc)
        objs = clean(cell(r, 3))
        out.append({
            "media_id": f"rg8-{i:04d}",
            "source": "transkribus",
            "item_type": "manuscript",
            "title": play_key or desc[:80],
            "holding_institution": "YIVO",
            "collection": "RG 8 (Esther-Rokhl Kaminska Theatre Museum)",
            "signature": f"folder {folder}" if folder else "",
            "source_url": clean(cell(r, 7)),
            "fetch_hint": (f"YIVO RG8 folder {folder}, objects {objs}"
                           if folder else "YIVO RG8"),
            "rights": "unknown",
            "entity_ids": ids,
            "play_key": play_key,
            "link_status": status,
            "status": "PENDING",
            "notes": "; ".join(x for x in (clean(cell(r, 5))[:200],
                                           clean(cell(r, 4))) if x),
        })
    return out


def harvest_loc(wb, ents, idx) -> list[dict]:
    """Library of Congress copyright deposits (Zachary Baker / Marwick list).

    A deposit is a submitted SCRIPT, so the item is a manuscript; several were
    never examined, which the notes preserve.
    """
    out = []
    for i, r in enumerate(rows_of(wb, "ZachBakerLOCMarwick"), start=1):
        play_key, long_title = clean(cell(r, 3)), clean(cell(r, 4))
        if not (play_key or long_title):
            continue
        ids, status = propose_title(ents, idx, play_key or long_title)
        date = clean(cell(r, 6))
        out.append({
            "media_id": f"loc-{i:04d}",
            "source": "dorot",
            "item_type": "manuscript",
            "title": play_key or long_title[:80],
            "date": year_of(date),
            "date_note": f"copyright deposited {date}" if date else "",
            "holding_institution": "Library of Congress",
            "collection": "Copyright deposits (Yiddish drama)",
            "signature": clean(cell(r, 5)),
            "fetch_hint": "LOC copyright deposit; request by signature",
            "rights": clean(cell(r, 7)),
            "entity_ids": ids,
            "play_key": play_key,
            "link_status": status,
            "status": "PENDING",
            "notes": "; ".join(x for x in (
                long_title[:140],
                f"produced {clean(cell(r, 8))}" if clean(cell(r, 8)) else "",
                clean(cell(r, 9)), clean(cell(r, 10))[:120]) if x),
        })
    return out


def main() -> None:
    ents = load_entities()
    idx = load_title_index(ents)
    wb = openpyxl.load_workbook(CATALOGUE, read_only=True, data_only=True)
    rows = (harvest_hurwitz_music(wb, ents, idx) + harvest_vilne(wb, ents, idx)
            + harvest_kaminska_r8(wb, ents, idx) + harvest_loc(wb, ents, idx))
    manifest = read_manifest()
    added, updated, protected = merge(manifest, rows)
    write_manifest(manifest)
    anchored = sum(1 for r in rows if r["entity_ids"])
    print(f"archives: {len(rows)} rows ({anchored} anchored) -> +{added} added, "
          f"{updated} updated, {protected} human fields kept")


if __name__ == "__main__":
    main()
