"""Locate Zylbercweig Lexicon pages for the people in this db.

The structured Lexicon TEIs carry a `<facsimile>` per page (`<graphic url=…>`)
and every `<div type="entry">` anchors its text to those pages via `facs`. So
for a person in `entity_graph.json` we can name the exact page image that holds
their entry — and the Lexicon prints portraits inside the entries.

This produces a `page_scan` row per matched entry. The portrait itself is a crop
a human still has to make, which is why `status` stays PENDING.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (REPO, clean, db_id_for_person, graph_person_by_db_id,
                    load_entities, load_person_bridge, merge, propose,
                    read_manifest, sm, strip_parens, write_manifest)

LEXICON = REPO / "Zylbercweig" / "The Lexicon"
TEI = "{http://www.tei-c.org/ns/1.0}"


def page_images(root) -> dict:
    """facsimile xml:id -> graphic url."""
    out = {}
    for facs in root.iter(f"{TEI}facsimile"):
        fid = facs.get("{http://www.w3.org/XML/1998/namespace}id")
        gr = facs.find(f"{TEI}graphic")
        if fid and gr is not None and gr.get("url"):
            out[fid] = gr.get("url")
    return out


def page_of(facs_ref: str, images: dict) -> str:
    """Resolve a `facs` ref like `#facs_17_tr_123` to its page image.

    Entry-level refs carry a transcript suffix; the page id is the `facs_<n>`
    prefix, so trim back to it.
    """
    ref = (facs_ref or "").lstrip("#")
    if ref in images:
        return images[ref]
    m = re.match(r"(facs_\d+)", ref)
    return images.get(m.group(1), "") if m else ""


def main() -> None:
    if not LEXICON.exists():
        print(f"lexicon: {LEXICON} not found; skipping")
        return

    ents = load_entities()
    bridge = load_person_bridge()
    by_db = graph_person_by_db_id(ents)

    # Which people do we actually want? Those in the entity graph.
    wanted = {}          # normalised name form -> graph node id
    for nid, n in ents["nodes"].items():
        if n.get("kind") != "person":
            continue
        for form in (n.get("label"), n.get("matched")):
            if not clean(form):
                continue
            for key in {sm(form), sm(strip_parens(form))}:
                if len(key) >= 3:
                    wanted.setdefault(key, nid)

    rows, seen = [], set()
    files = sorted(LEXICON.glob("Structured_*.xml"))
    for path in files:
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError as exc:
            print(f"  ! {path.name}: unparseable ({exc}); skipped")
            continue
        images = page_images(root)
        vol = path.stem

        for div in root.iter(f"{TEI}div"):
            if div.get("type") != "entry":
                continue
            head = next((ab for ab in div.iter(f"{TEI}ab")
                         if ab.get("type") == "heading"), None)
            name = clean("".join(head.itertext())) if head is not None else ""
            if len(name) < 3:
                continue

            # Route the entry heading to a graph person: direct label match
            # first, then through people_db's db_id bridge.
            nid = wanted.get(sm(name)) or wanted.get(sm(strip_parens(name)))
            if not nid:
                db_id = db_id_for_person(bridge, name)
                nid = by_db.get(db_id, "") if db_id else ""
            if not nid:
                continue

            ref = (head.get("facs") if head is not None else "") or div.get("facs")
            img = page_of(ref, images)
            key = (nid, vol, img)
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "media_id": "",                      # assigned below
                "source": "lexicon",
                "item_type": "page_scan",
                "title": f"Lexicon entry: {name}",
                "title_yiddish": name,
                "holding_institution": "YIVO",
                "collection": f"Zylbercweig, Leksikon fun yidishn teater ({vol})",
                "signature": img or "",
                "fetch_hint": (f"Zylbercweig/The Lexicon/{path.name}: "
                               f"facsimile {img or ref}"),
                "rights": "unknown",
                "entity_ids": nid,
                "link_status": "PROPOSED",
                "status": "PENDING",
                "notes": f"entry heading {name!r}; portrait is a crop of this page",
            })

    for i, row in enumerate(sorted(rows, key=lambda r: r["entity_ids"]), start=1):
        row["media_id"] = f"lexicon-{i:04d}"

    manifest = read_manifest()
    added, updated, protected = merge(manifest, rows)
    write_manifest(manifest)
    people = len({r["entity_ids"] for r in rows})
    print(f"lexicon: {len(files)} volumes, {len(rows)} entry pages for "
          f"{people} people -> +{added} added, {updated} updated, "
          f"{protected} human fields kept")


if __name__ == "__main__":
    main()
