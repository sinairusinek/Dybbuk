#!/usr/bin/env python3
"""Apply Sinai's rulings (2026-10-04) on the 28 place-split children.

  פֿאָלקס-שול (6)   -> GENERIC   "folk school" names a kind, not an institution
  תלמוד-תורה (2)    -> GENERIC   same
  ישיבה (3)         -> undecided not generic; back in the queue to be named/minted
  universities (2)  -> undecided same; Saratov already minted by Sinai as db2270
  פֿילאַזאָפֿישן פֿאָקולטעט (2) -> ALIGN to the Vienna and Zurich universities

Theatres are NOT touched: those 13 children come from Sinai's own Theatre and
Kleinkunst clusters, are already typed correctly, and were only in the approval
form because it was built from every human SPLIT rather than Education alone.

Idempotent; never overwrites a child a human has already decided.
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import sys
import unicodedata

csv.field_size_limit(10**9)

HERE = pathlib.Path(__file__).resolve().parent
REVIEW = HERE / "org_alignment_review.tsv"
CORE = HERE / "core_db.tsv"

REVIEWER = "Sinai"
RULE = "Sinai ruling 2026-10-04"

def key(s: str) -> str:
    """Yiddish points compose and decompose differently between sources;
    compare on NFC with the combining marks stripped."""
    s = unicodedata.normalize("NFD", (s or "").strip())
    return "".join(c for c in s if not unicodedata.combining(c))


GENERIC_NAMES = {key("פֿאָלקס-שול"), key("תלמוד-תורה")}
UNDECIDE_NAMES = {key("ישיבה"), key("אוניווערזיטעט")}
FACULTY = key("פֿילאַזאָפֿישן פֿאָקולטעט")
VIENNA_DB = "588"                     # ווינער אוניווערזיטעט, already in core_db


def load(path):
    with open(path, newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh, delimiter="\t")
        return list(rd.fieldnames or []), list(rd)


def write(path, headers, rows):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=headers, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)


def main(apply: bool) -> int:
    headers, rows = load(REVIEW)
    cheaders, core = load(CORE)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    zurich = next((c for c in core
                   if "ציריך" in (c.get("name_yiddish") or "") + (c.get("name") or "")
                   or "zurich" in (c.get("name") or "").lower()
                   or "zürich" in (c.get("name") or "").lower()), None)
    new_zurich = None
    if zurich is None:
        nid = max(int(c["db_id"]) for c in core if (c.get("db_id") or "").isdigit()) + 1
        new_zurich = {h: "" for h in cheaders}
        new_zurich.update({"db_id": str(nid), "name": "University of Zürich",
                           "name_yiddish": "ציריכער אוניווערזיטעט",
                           "org_type": "Education"})
        zurich = new_zurich

    counts = collections_counter = {"GENERIC": 0, "undecided": 0, "ALIGN": 0, "skipped": 0}
    links = {}
    for row in rows:
        if not (row.get("reviewer_notes") or "").startswith("Place-split"):
            continue
        name = key(row.get("canonical_yiddish"))
        place = (row.get("extracted_settlements") or "").strip()
        decided = (row.get("decision") or "").strip()

        if name in GENERIC_NAMES:
            verdict, db_id = "GENERIC", ""
        elif name in UNDECIDE_NAMES:
            verdict, db_id = "", ""
        elif name == FACULTY:
            verdict = "ALIGN"
            db_id = VIENNA_DB if key("ווין") in key(place) else zurich["db_id"]
        else:
            continue                       # theatres and anything else: untouched

        # never overturn a decision a person made in the app
        if decided and decided != verdict:
            counts["skipped"] += 1
            continue

        if apply:
            row["decision"] = verdict
            row["aligned_db_id"] = db_id
            row["reviewer"] = REVIEWER if verdict else ""
            row["reviewed_at"] = stamp if verdict else ""
            note = {"GENERIC": f"{RULE}: generic institution kind, not mintable.",
                    "ALIGN": f"{RULE}: philosophy faculty merged into the university.",
                    "": f"{RULE}: not generic — returned to the queue to be named."}[verdict]
            row["reviewer_notes"] = f"{note} {(row.get('reviewer_notes') or '')}"[:480]
        counts[verdict or "undecided"] += 1
        if verdict == "ALIGN":
            links.setdefault(db_id, []).append(row["cluster_id"])

    for db_id, cids in links.items():
        tgt = next((c for c in core if c["db_id"] == db_id), zurich)
        have = [i for i in (tgt.get("linked_cluster_ids") or "").split("|") if i.strip()]
        for cid in cids:
            if cid not in have:
                have.append(cid)
        if apply:
            tgt["linked_cluster_ids"] = "|".join(have)

    print(f"GENERIC stamped        : {counts['GENERIC']}")
    print(f"returned to undecided  : {counts['undecided']}")
    print(f"ALIGNed to a university: {counts['ALIGN']}")
    print(f"skipped (already decided differently): {counts['skipped']}")
    if new_zurich:
        print(f"minting db{new_zurich['db_id']} University of Zürich")
    print("theatres/Kleinkunst: untouched by design")

    if not apply:
        print("\n[dry run] nothing written. Re-run with --apply")
        return 0

    if new_zurich:
        core.append(new_zurich)
        write(CORE, cheaders, core)
    elif links:
        write(CORE, cheaders, core)
    write(REVIEW, headers, rows)
    print("\nwritten.")
    return 0


if __name__ == "__main__":
    sys.exit(main(apply="--apply" in sys.argv))
