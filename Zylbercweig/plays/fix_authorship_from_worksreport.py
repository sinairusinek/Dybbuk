"""A1b — Correct play authorship from the catalogue works table.

Root cause (traced 2026-07-26): people_db's `created_expressions` for
Lateiner (683) and Hurwitz (684) came from the DiJeSt person report
(`ZylbercweigPeople/ZylbereportPeople.tsv`, col "Created expression(s)"),
whose generator pooled BOTH playwrights' works (plus unattributed ones) into
one alphabetically-sorted list and split it at an arbitrary boundary between
the two adjacent person rows: Lateiner got 400/א..דניאל, Hurwitz got דעם..ש.
The database's own works table (`worksReport` sheet in DybbukCatalogue:
Lateiner 116, Hurwitz 108, unattributed 52, each spanning the full alphabet)
is consistent with the lexicon entry texts in all 64 independently checkable
cases.

This script updates plays_db.tsv IN PLACE (play_ids stay stable):
  - author_db_id / author_heading from worksReport (matched by normalized title)
  - attribution_status: corrected / confirmed / unattributed_in_works /
    not_in_works (keeps `disputed` where set by cross-author collision)
  - notes documents the source

Usage:
    python3.11 fix_authorship_from_worksreport.py [--execute]
"""
from __future__ import annotations

import argparse
from collections import Counter

import openpyxl

import plays_common as pc

AUTH_MAP = {"יאָזעף לאַטיינער": "683", "משה איש הלוי הורוויץ": "684"}
HEADINGS = {"683": "יאָזעף לאַטיינער", "684": "פּראָפֿעסאָר משה איש הלוי הורוויץ"}


def load_worksreport() -> dict[str, str]:
    """normalized title -> auth ('683' / '684' / '' for unattributed)."""
    wb = openpyxl.load_workbook(pc.EDITION_METADATA_DIR / "DybbukCatalogue May2024.xlsx",
                                read_only=True, data_only=True)
    ws = wb["worksReport"]
    rows = list(ws.iter_rows(values_only=True))
    hdr = [str(h) for h in rows[0]]
    out: dict[str, str] = {}
    for r in rows[1:]:
        d = dict(zip(hdr, [("" if v is None else str(v).strip()) for v in r]))
        heb = d.get("heb", "")
        if not heb:
            continue
        key = pc.norm_yiddish(heb)
        if key:
            out[key] = AUTH_MAP.get(d.get("auth", ""), "")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    works = load_worksreport()
    plays = pc.load_plays_db()
    counts = Counter()
    for p in plays:
        key = pc.norm_yiddish(p["title_yiddish"])
        if key not in works:
            # try main segment (subtitled registry rows vs plain works title)
            segs = (p["title_segments_norm"] or "").split("|")
            key = next((s for s in segs if s in works), None)
        if key is None:
            counts["not_in_works"] += 1
            p["attribution_status"] = "not_in_works"
            p["notes"] = (p["notes"] + "; " if p["notes"] else "") + \
                "title absent from worksReport; author kept from (corrupt) created_expressions"
            continue
        auth = works[key]
        if not auth:
            counts["unattributed_in_works"] += 1
            p["attribution_status"] = "unattributed_in_works"
            p["notes"] = (p["notes"] + "; " if p["notes"] else "") + \
                "worksReport has no author; author kept from (corrupt) created_expressions"
        elif auth == p["author_db_id"]:
            counts["confirmed"] += 1
            if p["attribution_status"] in ("catalogue_conflict", "single"):
                p["attribution_status"] = "single"
        else:
            counts["corrected"] += 1
            p["author_db_id"] = auth
            p["author_heading"] = HEADINGS[auth]
            if p["attribution_status"] in ("catalogue_conflict", "single"):
                p["attribution_status"] = "single"
            p["notes"] = (p["notes"] + "; " if p["notes"] else "") + \
                "author corrected from worksReport (created_expressions split bug, 2026-07-26)"
    print(dict(counts))
    if not args.execute:
        print("dry-run — pass --execute to update plays_db.tsv")
        return
    from build_plays_db import PLAYS_FIELDS
    pc.write_tsv(pc.PLAYS_DB_TSV, plays, PLAYS_FIELDS)
    print(f"updated {pc.PLAYS_DB_TSV}")


if __name__ == "__main__":
    main()
