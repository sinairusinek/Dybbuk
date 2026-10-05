"""Seed JPRESS press-notice rows — one per (play, premiere window) to search.

JPRESS links are not stable and often do not point at the intended notice, so
nothing here can be fetched automatically. What this DOES do is turn "search
JPRESS" into a finite, trackable worklist: one PENDING row per edition, carrying
the search terms that stand a chance of finding it (Yiddish title, author, and a
date window drawn from the edition's own dates).

A human then searches, screenshots/crops the notice, drops the file under
media/jpress/, and fills in local_path + source_url + status=FETCHED.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (REPO, clean, load_entities, merge, read_manifest,
                    write_manifest)

EDITIONS = REPO / "YiDraCor" / "data" / "editions.json"

# Yiddish dailies that carried theatre notices in the relevant decades, and are
# digitised in JPRESS. Searching all of them per play is the realistic unit of
# work, so they go in the hint rather than becoming rows of their own.
PAPERS = "Forverts, Morgn-zhurnal, Tog, Varhayt, Haynt, Moment"


def main() -> None:
    ents = load_entities()
    by_folder = {n.get("folder"): nid for nid, n in ents["nodes"].items()
                 if n.get("kind") == "edition" and n.get("folder")}

    data = json.loads(EDITIONS.read_text(encoding="utf-8"))["editions"]
    rows = []
    for i, ed in enumerate(data, start=1):
        folder = clean(ed.get("folder"))
        title = clean(ed.get("title"))
        title_yi = clean(ed.get("catalogue_yiddish_name"))
        written = clean(ed.get("year_written"))
        printed = clean(ed.get("year_printed"))

        # A premiere notice appears around the writing/first-performance year;
        # a review or advertisement can appear any time after. Give the searcher
        # the earliest known year as the window start.
        start = written or printed
        window = f"{start}–{int(start) + 5}" if start.isdigit() else "unknown"

        terms = " / ".join(x for x in (title_yi, title) if x)
        rows.append({
            "media_id": f"jpress-{i:04d}",
            "source": "jpress",
            "item_type": "press_notice",
            "title": title,
            "title_yiddish": title_yi,
            "date_note": f"search window {window}",
            "holding_institution": "NLI (JPRESS)",
            "collection": "Historical Jewish Press",
            "source_url": "https://www.nli.org.il/en/newspapers",
            "fetch_hint": (f"SEARCH JPRESS for {terms} in [{PAPERS}], "
                           f"{window}. Links are unstable — record the paper, "
                           f"issue date and page here once found, then crop."),
            "local_path": "",
            "rights": "unknown",
            "entity_ids": by_folder.get(folder, ""),
            "play_key": clean(ed.get("catalogue_play_key")),
            "link_status": "LINKED" if by_folder.get(folder) else "GAP",
            "reviewer": ("derived from editions.json folder key"
                         if by_folder.get(folder) else ""),
            "status": "PENDING",
            "notes": "no notice located yet; this row is the worklist item",
        })

    manifest = read_manifest()
    added, updated, protected = merge(manifest, rows)
    write_manifest(manifest)
    print(f"jpress: {len(rows)} search tasks -> +{added} added, "
          f"{updated} updated, {protected} human fields kept")


if __name__ == "__main__":
    main()
