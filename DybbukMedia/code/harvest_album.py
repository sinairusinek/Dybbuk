"""Harvest Album (Zylbercweig 1937) photographs relevant to this db.

The Album tagger keeps its own KG in SQLite: 122 plates, 348 photos, 618 named
people, 802 appearances. Only a slice of that concerns Lateiner/Hurwitz, so a
photo earns a manifest row when a person, play or credit on its plate anchors to
`entity_graph.json`.

A photo is a crop of a plate, so plates are catalogued too and each photo
records `crop_of` back to its plate.
"""
from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (ALBUM_DB, clean, db_id_for_person, graph_person_by_db_id,
                    load_entities, load_person_bridge, merge, propose,
                    read_manifest, write_manifest)

ALBUM_REL = "Zylbercweig/TheAlbum"


def resolve_person(ents, bridge, by_db, name) -> str:
    """Entity-graph node id for an Album person name, or "".

    The Album writes Latin script ("Sigmund Mogulesco"); the graph mostly holds
    Yiddish "Surname, Given". Direct label matching cannot bridge the two, so
    try the label index first, then route through people_db, which carries both
    forms under one db_id.
    """
    nid, status = propose(ents, clean(name), kinds={"person"})
    if nid and status == "PROPOSED":
        return nid
    db_id = db_id_for_person(bridge, name)
    return by_db.get(db_id, "") if db_id else ""


def anchors_by_plate(con, ents, bridge, by_db) -> dict:
    """plate page_number -> {entity_id: why}.

    Three routes onto a plate: a person depicted, a play named in a caption,
    and an authorship credit. All are checked; each hit records its route so a
    reviewer can see why the plate was pulled in.
    """
    found = defaultdict(dict)

    q = """select a.page_number, p.name, p.name_yiddish
             from appearance a join person p using(person_id)"""
    for page, name, name_yi in con.execute(q):
        for cand in (name, name_yi):
            if not clean(cand):
                continue
            nid = resolve_person(ents, bridge, by_db, cand)
            if nid:
                found[page][nid] = f"person depicted: {clean(cand)}"

    q = """select a.page_number, pl.title, pl.title_yiddish
             from appearance a join play pl using(play_id)"""
    for page, title, title_yi in con.execute(q):
        for cand in (title, title_yi):
            if not clean(cand):
                continue
            ids, status = propose(ents, clean(cand), kinds={"edition"})
            if ids and status == "PROPOSED":
                found[page][ids] = f"play named: {clean(cand)}"

    q = """select c.page_number, p.name, p.name_yiddish, c.kind
             from credit c join person p using(person_id)
            where c.page_number is not null"""
    for page, name, name_yi, kind in con.execute(q):
        for cand in (name, name_yi):
            if not clean(cand):
                continue
            nid = resolve_person(ents, bridge, by_db, cand)
            if nid:
                found[page][nid] = f"{clean(kind) or 'credit'}: {clean(cand)}"

    return found


def main() -> None:
    if not ALBUM_DB.exists():
        print(f"album: {ALBUM_DB} not found — TheAlbum/ is git-ignored; skipping")
        return

    ents = load_entities()
    # immutable=1, not mode=ro: a plain read-only open still probes the
    # directory for write access, which fails under a sandboxed run.
    con = sqlite3.connect(f"file:{ALBUM_DB}?mode=ro&immutable=1", uri=True)
    con.row_factory = sqlite3.Row

    bridge = load_person_bridge()
    by_db = graph_person_by_db_id(ents)
    hits = anchors_by_plate(con, ents, bridge, by_db)
    rows = []

    for page in sorted(hits):
        pl = con.execute(
            "select * from plate where page_number = ?", (page,)).fetchone()
        if pl is None:
            continue
        ids = sorted(hits[page])
        why = "; ".join(sorted(hits[page].values()))
        plate_id = f"album-p{page:04d}"
        rows.append({
            "media_id": plate_id,
            "source": "album",
            "item_type": "page_scan",
            "title": clean(pl["headline_en"]) or f"Album plate {page}",
            "title_yiddish": clean(pl["headline_yi"]),
            "holding_institution": "Zylbercweig, Album fun yidishn teater (1937)",
            "collection": "The Album",
            "signature": f"p. {page}",
            "fetch_hint": f"{ALBUM_REL}: plate page {page} ({clean(pl['page_file'])})",
            "local_path": "",
            "rights": "unknown",
            "entity_ids": "|".join(ids),
            "link_status": "PROPOSED",
            "status": "PENDING",
            "notes": why,
        })

        photos = con.execute(
            "select * from photo where page_number = ? order by ordinal",
            (page,)).fetchall()
        for ph in photos:
            n_faces = con.execute(
                "select count(*) from face where photo_id = ?",
                (ph["photo_id"],)).fetchone()[0]
            rows.append({
                "media_id": f"album-p{page:04d}-{ph['ordinal']:02d}",
                "source": "album",
                "item_type": "photograph",
                "title": (f"{clean(pl['headline_en']) or f'Album plate {page}'} "
                          f"(photo {ph['ordinal']})"),
                "title_yiddish": clean(pl["headline_yi"]),
                "holding_institution": "Zylbercweig, Album fun yidishn teater (1937)",
                "collection": "The Album",
                "signature": f"p. {page} no. {ph['ordinal']}",
                "fetch_hint": f"{ALBUM_REL}: {clean(ph['file'])}",
                "rights": "unknown",
                "entity_ids": "|".join(ids),
                "link_status": "PROPOSED",
                "status": "PENDING",
                "crop_of": plate_id,
                "notes": f"{n_faces} face(s) detected; {why}",
            })

    con.close()
    manifest = read_manifest()
    added, updated, protected = merge(manifest, rows)
    write_manifest(manifest)
    print(f"album: {len(hits)} relevant plates, {len(rows)} rows -> "
          f"+{added} added, {updated} updated, {protected} human fields kept")


if __name__ == "__main__":
    main()
