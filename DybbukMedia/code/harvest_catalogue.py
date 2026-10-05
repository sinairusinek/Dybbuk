"""Harvest visual items from the catalogue workbook into the media manifest.

Four sheets of `DybbukCatalogue May2024.xlsx` describe visual material:

  DorotScraped                 NYPL Dorot posters/placards, with permalinks
  documentsitems               programs/playbills, curated, with signatures
  Score-print-editions         printed sheet music (title pages are visual)
  cardCatalogAffishenProgramen the Perlmutter card catalogue of affishn

Every row is anchored to `YiDraCor/data/entity_graph.json` where a title or a
theatre name matches. Anchors land as PROPOSED — a human promotes them.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import openpyxl

from common import (CATALOGUE, clean, load_entities, load_title_index, merge,
                    propose, propose_title, read_manifest, write_manifest,
                    year_of)


def rows_of(wb, sheet: str, skip: int) -> list[tuple]:
    """Data rows of a sheet, minus the header and any description rows."""
    return list(wb[sheet].iter_rows(values_only=True))[skip:]


def cell(row: tuple, idx: int):
    """Column `idx` of `row`, or None.

    A read-only worksheet yields ragged tuples — trailing empty cells are simply
    absent — so a positional read past the last populated cell must not raise.
    """
    return row[idx] if idx < len(row) else None


def anchor(ents, idx, *candidates, kinds=None) -> tuple[str, str]:
    """First candidate that yields an anchor wins; otherwise GAP.

    Titles go through `propose_title`, which tolerates the romanisation
    differences between these sources; a theatre name goes through the plain
    label index.
    """
    for c in candidates:
        c = clean(c)
        if not c:
            continue
        if kinds is None or "edition" in kinds:
            ids, status = propose_title(ents, idx, c)
            if ids:
                return ids, status
        ids, status = propose(ents, c, kinds=kinds)
        if ids:
            return ids, status
    return "", "GAP"


def harvest_dorot(wb, ents, idx) -> list[dict]:
    """NYPL Dorot placards. Column 0 is the item permalink."""
    out = []
    for i, r in enumerate(rows_of(wb, "DorotScraped", 1), start=1):
        url = clean(cell(r, 0))
        title_yi, play_key = clean(cell(r, 1)), clean(cell(r, 2))
        if not (url or title_yi or play_key):
            continue
        genres = clean(cell(r, 16))
        item_type = ("photograph" if "Photograph" in genres or "Portrait" in genres
                     else "poster")
        ids, status = anchor(ents, idx, play_key, title_yi, cell(r, 4))
        out.append({
            "media_id": f"dorot-{i:04d}",
            "source": "dorot",
            "item_type": item_type,
            "title": play_key,
            "title_yiddish": title_yi,
            "date": year_of(cell(r, 13)) or year_of(cell(r, 9)),
            "date_note": clean(cell(r, 3)),
            "place": clean(cell(r, 12)) or clean(cell(r, 10)),
            "holding_institution": "NYPL",
            "collection": clean(cell(r, 8)),
            "signature": clean(cell(r, 14)),
            "source_url": url,
            "fetch_hint": ("NYPL Digital Collections permalink; IIIF image via "
                           "the item UUID" if url else ""),
            "rights": "PD-with-credit (NYPL)",
            "entity_ids": ids,
            "play_key": play_key,
            "link_status": status,
            "status": "PENDING",
            "notes": "; ".join(x for x in (genres, clean(cell(r, 4))) if x),
        })
    return out


def harvest_documents(wb, ents, idx) -> list[dict]:
    """Curated programs/playbills. Row 2 is a column-description row, not data."""
    type_map = {
        "תכניה": "program", "אפישה": "poster", "פלקט": "poster",
        "תווים": "sheet_music", "תמונה": "photograph", "צילום": "photograph",
        "כתב יד": "manuscript", "ספר": "print_edition",
    }
    out = []
    for i, r in enumerate(rows_of(wb, "documentsitems", 2), start=1):
        he_type, title = clean(cell(r, 1)), clean(cell(r, 7))
        if not (he_type or title):
            continue
        item_type = next((v for k, v in type_map.items() if k in he_type), "program")
        play_key = clean(cell(r, 10))
        ids, status = anchor(ents, idx, play_key, title, cell(r, 12))
        tk = clean(cell(r, 3))
        out.append({
            "media_id": f"docitem-{i:04d}",
            "source": "dorot",
            "item_type": item_type,
            "title": title,
            "title_yiddish": clean(cell(r, 8)),
            "date": year_of(cell(r, 14)),
            "date_note": clean(cell(r, 14)) if not year_of(cell(r, 14)) else "",
            "place": clean(cell(r, 15)),
            "holding_institution": clean(cell(r, 21)),
            "collection": clean(cell(r, 22)),
            "signature": clean(cell(r, 23)),
            "source_url": clean(cell(r, 27)),
            "fetch_hint": f"transkribus docId {tk}" if tk.isdigit() else "",
            "rights": clean(cell(r, 6)),
            "entity_ids": ids,
            "play_key": play_key,
            "expression_id": clean(cell(r, 11)),
            "link_status": status,
            "status": "PENDING",
            "notes": "; ".join(x for x in (he_type, clean(cell(r, 12)), clean(cell(r, 20))) if x),
        })
    return out


def harvest_scores(wb, ents, idx) -> list[dict]:
    """Printed sheet music. Title pages carry portraits and theatre imprints."""
    out = []
    for i, r in enumerate(rows_of(wb, "Score-print-editions", 1), start=1):
        title, play_key = clean(cell(r, 8)), clean(cell(r, 14))
        if not (title or play_key):
            continue
        ids, status = anchor(ents, idx, play_key, title)
        out.append({
            "media_id": f"score-{i:04d}",
            "source": "dorot",
            "item_type": "sheet_music",
            "title": title,
            "title_yiddish": clean(cell(r, 9)),
            "date": year_of(cell(r, 19)) or year_of(cell(r, 20)),
            "place": clean(cell(r, 21)),
            "holding_institution": clean(cell(r, 29)),
            "collection": clean(cell(r, 30)),
            "signature": clean(cell(r, 31)),
            "source_url": clean(cell(r, 35)),
            "fetch_hint": "",
            "rights": clean(cell(r, 7)),
            "entity_ids": ids,
            "play_key": play_key,
            "expression_id": clean(cell(r, 13)),
            "link_status": status,
            "status": "PENDING",
            "notes": "; ".join(x for x in (clean(cell(r, 23)), clean(cell(r, 27))) if x),
        })
    return out


def harvest_cards(wb, ents, idx) -> list[dict]:
    """Perlmutter card catalogue of affishn/programen.

    These are cards ABOUT posters, and the cards themselves are scanned in
    Transkribus collection 18874 doc 899977. Each row therefore stands for a
    card image to be located in that doc.
    """
    out = []
    for i, r in enumerate(rows_of(wb, "cardCatalogAffishenProgramen", 1), start=1):
        card_no, author = clean(cell(r, 0)), clean(cell(r, 1))
        title_yi, title_tr = clean(cell(r, 2)), clean(cell(r, 3))
        # A handful of cards carry only the transliterated title — no number,
        # no Yiddish — so all three must be checked before skipping a row.
        if not (card_no or title_yi or title_tr):
            continue
        card_no = card_no[:-2] if card_no.endswith(".0") else card_no
        ids, status = anchor(ents, idx, title_tr, title_yi)
        out.append({
            "media_id": f"card-{i:04d}",
            "source": "transkribus",
            "item_type": "card_catalog",
            "title": title_tr,
            "title_yiddish": title_yi,
            "holding_institution": "YIVO",
            "collection": "Perlmutter card catalogue (affishn un programen)",
            "signature": f"card {card_no}" if card_no else "",
            "source_url": "https://app.transkribus.org/collection/18874/doc/899977",
            "fetch_hint": (f"transkribus col 18874 doc 899977; locate card "
                           f"{card_no}" if card_no else
                           "transkribus col 18874 doc 899977"),
            "rights": "unknown",
            "entity_ids": ids,
            "play_key": title_tr,
            "link_status": status,
            "status": "PENDING",
            "notes": "; ".join(x for x in (author, clean(cell(r, 5)), clean(cell(r, 6))) if x),
        })
    return out


def main() -> None:
    ents = load_entities()
    idx = load_title_index(ents)
    wb = openpyxl.load_workbook(CATALOGUE, read_only=True, data_only=True)
    rows = (harvest_dorot(wb, ents, idx) + harvest_documents(wb, ents, idx)
            + harvest_scores(wb, ents, idx) + harvest_cards(wb, ents, idx))
    manifest = read_manifest()
    added, updated, protected = merge(manifest, rows)
    write_manifest(manifest)
    print(f"catalogue: {len(rows)} rows seen -> "
          f"+{added} added, {updated} updated, {protected} human fields kept")


if __name__ == "__main__":
    main()
