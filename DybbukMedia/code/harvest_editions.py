"""Catalogue the editions themselves — the printed books and the manuscripts.

Every edition in `YiDraCor/data/editions.json` is a physical object that was
photographed: a Transkribus document, often with a library permalink. Those page
images are visual material in their own right (title pages, frontispieces,
scribal hands), so each edition gets a manifest row.

`entity_ids` is exact here, not a guess: the edition node id in
`entity_graph.json` is built from the same `folder` key, so these rows are
LINKED rather than PROPOSED.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (ENTITY_GRAPH, REPO, clean, load_entities, merge,
                    read_manifest, write_manifest)

EDITIONS = REPO / "YiDraCor" / "data" / "editions.json"
TEI_DIRS = ("tei", "tei/ms", "tei/dracor")


def tei_for(folder: str) -> str:
    """Any built TEI whose filename matches this edition's folder, if present."""
    stem = folder.split("-")[0].replace("_", "").lower()
    for d in TEI_DIRS:
        for f in sorted((REPO / "YiDraCor" / d).glob("*.xml")):
            if f.stem.replace("-", "").replace("_", "").lower() == stem:
                return str(f.relative_to(REPO))
    return ""


def main() -> None:
    ents = load_entities()
    by_folder = {n.get("folder"): nid for nid, n in ents["nodes"].items()
                 if n.get("kind") == "edition" and n.get("folder")}

    data = json.loads(EDITIONS.read_text(encoding="utf-8"))["editions"]
    rows = []
    for i, ed in enumerate(data, start=1):
        folder = clean(ed.get("folder"))
        is_ms = folder.startswith("MS_")
        doc_id = clean(ed.get("transkribus_doc_id"))
        coll_id = clean(ed.get("transkribus_collection_id"))

        nid = by_folder.get(folder, "")
        rows.append({
            "media_id": f"edition-{i:04d}",
            "source": "edition",
            "item_type": "manuscript" if is_ms else "print_edition",
            "title": clean(ed.get("title")),
            "title_yiddish": clean(ed.get("catalogue_yiddish_name")),
            "date": clean(ed.get("year_printed")) or clean(ed.get("year_written")),
            "place": clean(ed.get("publication_place")),
            "holding_institution": clean(ed.get("library")),
            "collection": clean(ed.get("notes"))[:0],   # notes go to `notes`
            "signature": clean(ed.get("library_signature")),
            "source_url": clean(ed.get("transkribus_url")),
            "fetch_hint": (f"transkribus col {coll_id} doc {doc_id}"
                           if doc_id else ""),
            "local_path": "",
            "rights": "unknown",
            # The edition node is keyed on the same `folder`, so this is an
            # identity join, not a name guess — hence LINKED.
            "entity_ids": nid,
            "play_key": clean(ed.get("catalogue_play_key")),
            "expression_id": clean(ed.get("expression_id")),
            "link_status": "LINKED" if nid else "GAP",
            "reviewer": "derived from editions.json folder key" if nid else "",
            "status": "PENDING",
            "notes": "; ".join(x for x in (
                f"TEI: {tei_for(folder)}" if tei_for(folder) else "",
                clean(ed.get("publisher")),
                clean(ed.get("notes"))[:200]) if x),
        })

    manifest = read_manifest()
    added, updated, protected = merge(manifest, rows)
    write_manifest(manifest)
    linked = sum(1 for r in rows if r["link_status"] == "LINKED")
    print(f"editions: {len(rows)} rows ({linked} LINKED) -> "
          f"+{added} added, {updated} updated, {protected} human fields kept")


if __name__ == "__main__":
    main()
