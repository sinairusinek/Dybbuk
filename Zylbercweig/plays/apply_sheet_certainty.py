"""A1c — Propagate the catalogue sheets' recorded ascription judgments.

The 51 plays left `unattributed_in_works` by fix_authorship_from_worksreport
all appear in the 'Lateiner Plays' / 'Hurwitz Plays' sheets with a
`certainty` value recorded by the (former) catalogue curators. This applies
those already-made judgments to plays_db:

  certain                -> adopt the sheet's author (attribution_status=single)
  false ascription/error -> attribution_status=ascription_rejected
                            (node kept; build_kg stops emitting authored_by)
  uncertain/blank        -> attribution_status=ascription_unvetted (for PI)

New judgments are NOT made here — unvetted rows go to the PI (REVIEW_TASKS).

Usage:
    python3.11 apply_sheet_certainty.py [--execute]
"""
from __future__ import annotations

import argparse
from collections import Counter

import openpyxl

import plays_common as pc

SHEET_AUTHOR = {"Lateiner Plays": "683", "Hurwitz Plays": "684"}
HEADINGS = {"683": "יאָזעף לאַטיינער", "684": "פּראָפֿעסאָר משה איש הלוי הורוויץ"}


def load_sheet_index() -> dict[str, tuple[str, str, str]]:
    """norm title -> (author_db_id, certainty, expression_id)."""
    wb = openpyxl.load_workbook(pc.EDITION_METADATA_DIR / "DybbukCatalogue May2024.xlsx",
                                read_only=True, data_only=True)
    idx: dict[str, tuple[str, str, str]] = {}
    for sheet, author in SHEET_AUTHOR.items():
        ws = wb[sheet]
        rows = list(ws.iter_rows(values_only=True))
        hdr = [str(h).strip() if h else f"c{i}" for i, h in enumerate(rows[0])]
        for r in rows[1:]:
            d = dict(zip(hdr, [("" if v is None else str(v).strip()) for v in r]))
            yid = d.get("Yiddish Name", "")
            if not yid:
                continue
            key = pc.norm_yiddish(yid)
            if key:
                idx.setdefault(key, (author, (d.get("certainty") or "").lower(),
                                     d.get("Expression ID", "").split(".")[0]))
    return idx


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    idx = load_sheet_index()
    plays = pc.load_plays_db()
    counts = Counter()
    for p in plays:
        if p["attribution_status"] != "unattributed_in_works":
            continue
        key = pc.norm_yiddish(p["title_yiddish"])
        hit = idx.get(key)
        if not hit:
            segs = [s for s in (p["title_segments_norm"] or "").split("|") if len(s) >= 5]
            hit = next((idx[s] for s in segs if s in idx), None)
        if not hit:
            counts["no_sheet_row"] += 1
            continue
        author, certainty, eid = hit
        note = f"sheet certainty={certainty or 'blank'} (eid {eid or '?'})"
        if certainty == "certain":
            p["author_db_id"], p["author_heading"] = author, HEADINGS[author]
            p["attribution_status"] = "single"
            counts["adopted_certain"] += 1
        elif "false" in certainty or "error" in certainty:
            p["attribution_status"] = "ascription_rejected"
            counts["rejected"] += 1
        else:
            p["attribution_status"] = "ascription_unvetted"
            counts["unvetted"] += 1
        p["notes"] = (p["notes"] + "; " if p["notes"] else "") + note
    print(dict(counts))
    if not args.execute:
        print("dry-run — pass --execute to update plays_db.tsv")
        return
    from build_plays_db import PLAYS_FIELDS
    pc.write_tsv(pc.PLAYS_DB_TSV, plays, PLAYS_FIELDS)
    print(f"updated {pc.PLAYS_DB_TSV}")


if __name__ == "__main__":
    main()
