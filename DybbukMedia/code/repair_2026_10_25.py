"""One-off repair of manifest rows the harvesters cannot correct by re-running.

`merge()` never blanks a column and never overwrites a human-owned one, which is
right for decisions but leaves machine output stuck when the machine was wrong.
Two such cases were found on 2026-10-25 while joining DybbukMedia to the
mandala:

1. Score-print-editions (score-*). `harvest_scores` read place, holding
   institution, collection, signature and URL one column early, and anchored
   on the sheet's numeric "Id" — which on all 41 rows that carry one names a
   work 14-15 ids from the play in the row's own title (Mogulesco's waltz from
   *Homon der Zweite* sat on *Yankev un Eysev*). The harvester is fixed; this
   rewrites each score row from it, blanks included.

2. Edition rows (edition-*). harvest_editions keys rows on list position, and
   when editions.json dropped a duplicate the human-owned `notes` of rows
   16-27 kept the text of the entry that used to sit there (the 1908 Khurbn
   Yerusholaim print carried the MS Emigration's title-page note). A note is
   replaced only if it is, verbatim, what the harvester writes for SOME
   edition, which proves it is machine text and not a person's.

3. Album p. 94 is headed "PLAYS BY ASCH, BIMKO AND GORDIN"; its "Kidush
   Hashem" is Asch's, not Lateiner's. harvest_album now skips such plates;
   this removes the anchor already written.

Guarded: a row is touched only if every human-owned field still holds exactly
what the old code wrote, so nothing a person set can be lost. Safe to re-run —
a repaired row no longer matches the old output and is left alone.

    python3 repair_2026_10_25.py           # report only
    python3 repair_2026_10_25.py --write
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import openpyxl

import common
from common import (CATALOGUE, COLUMNS, clean, load_entities, load_title_index,
                    read_manifest, write_manifest)
from harvest_catalogue import anchor, cell, harvest_scores, rows_of
from harvest_editions import STAMP, edition_rows

# Anchors written by an earlier run, against an earlier graph, so the old code
# no longer reproduces them; inspected by hand 2026-10-25. score-0025/0026 sat
# on the "Sholem Aleichem" from Dovid's fidele (song:217) though the row names
# Der kherem in Beys hamikdosh; score-0039 was already right.
INSPECTED = {"score-0025": "song:217", "score-0026": "song:217",
             "score-0039": "work:4043"}

# Notes that are an OLDER editions.json `notes` value, so the current harvester
# no longer produces them verbatim. Each was traced in git on 2026-10-25 to
# another edition's entry (c31e93a2b: MS_YaakovEsav, Lateiner_Meshumed,
# MS_BenHaDor, MS_Emigration; 222ec7258^: the dropped duplicate
# Lateiner_Meshumed at old position 16) — shifted machine text, not a person's.
INSPECTED_NOTES = {"edition-0016", "edition-0017", "edition-0018",
                   "edition-0021", "edition-0027"}

ASCH_PLATE = ("album-p0094", "album-p0094-01", "album-p0094-02")
ASCH_ANCHOR = "edition:KidushHashem"


def old_score_fields(ents, old_idx, r) -> dict:
    """The human-owned fields exactly as the pre-fix harvest_scores wrote them."""
    ids, status = anchor(ents, old_idx, clean(cell(r, 14)), clean(cell(r, 8)))
    notes = "; ".join(x for x in (clean(cell(r, 23)), clean(cell(r, 27))) if x)
    return {"entity_ids": ids, "notes": notes}


def untouched(cur: dict) -> bool:
    return not (cur.get("reviewer") or cur.get("local_path")
                or cur.get("crop_of")) and cur.get("status") == "PENDING"


def main(write: bool) -> None:
    ents = load_entities()
    idx = load_title_index(ents)
    saved, common.WORK_ALT_TITLES = common.WORK_ALT_TITLES, ()
    old_idx = load_title_index(ents)            # the index the old run used
    common.WORK_ALT_TITLES = saved

    manifest = read_manifest()
    wb = openpyxl.load_workbook(CATALOGUE, read_only=True, data_only=True)
    raw = [r for r in rows_of(wb, "Score-print-editions", 1)
           if clean(cell(r, 8)) or clean(cell(r, 13))]
    new = harvest_scores(wb, ents, idx)
    assert len(raw) == len(new)

    fixed, moved, skipped = 0, 0, []
    for r, row in zip(raw, new):
        cur = manifest.get(row["media_id"])
        if cur is None:
            continue
        old = old_score_fields(ents, old_idx, r)
        if cur.get("entity_ids") == INSPECTED.get(row["media_id"]):
            old["entity_ids"] = cur["entity_ids"]
        if not untouched(cur) or any(cur.get(k, "") != v for k, v in old.items()):
            skipped.append(row["media_id"])
            continue
        if any(cur.get(c, "") != row.get(c, "") for c in COLUMNS):
            moved += cur.get("entity_ids") != row["entity_ids"]
            cur.update({c: row.get(c, "") for c in COLUMNS})
            fixed += 1
    print(f"scores: {fixed} rows rewritten ({moved} re-anchored), "
          f"{len(skipped)} left alone: {', '.join(skipped) or '-'}")

    gen = edition_rows(ents)
    machine_notes = {r["notes"] for r in gen if r["notes"]}
    renoted, odd = 0, []
    for row in gen:
        cur = manifest.get(row["media_id"])
        if not cur or cur.get("notes", "") == row["notes"]:
            continue
        if cur.get("reviewer") in ("", STAMP) and (
                cur.get("notes") in machine_notes
                or row["media_id"] in INSPECTED_NOTES):
            cur["notes"] = row["notes"]
            renoted += 1
        else:
            odd.append(row["media_id"])
    print(f"editions: {renoted} shifted notes restored, "
          f"{len(odd)} differing notes left alone: {', '.join(odd) or '-'}")

    asch = 0
    for mid in ASCH_PLATE:
        cur = manifest.get(mid)
        ids = (cur or {}).get("entity_ids", "").split("|")
        if not cur or ASCH_ANCHOR not in ids:
            continue
        cur["entity_ids"] = "|".join(x for x in ids if x != ASCH_ANCHOR)
        cur["notes"] = "; ".join(x for x in (
            cur.get("notes"),
            "Kidush Hashem here is Asch's (plate headed PLAYS BY ASCH, BIMKO "
            "AND GORDIN), not Lateiner's: anchor removed 2026-10-25") if x)
        asch += 1
    print(f"album p.94: {asch} Asch 'Kidush Hashem' anchor(s) removed")

    if write:
        write_manifest(manifest)
        print("written")


if __name__ == "__main__":
    main("--write" in sys.argv)
