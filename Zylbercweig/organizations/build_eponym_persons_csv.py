#!/usr/bin/env python3
"""CSV of the Zylbercweig persons list, with each person's lexicon entry.

Same people as the "persons" sheet of eponym_RA_workbook.xlsx -- those that
HAVE an id -- plus an `entry_text` column so the reviewer can read who someone
was without leaving the file. 2,575 of 2,591 have an entry (99%).

Entry text comes from people/entry_texts.tsv, which is already keyed by the
same person_id that person_hub.tsv's `entry_person_ids` carries. That avoids
parsing the TEI volumes directly, and with it the 30 xml_ids that collide
across volumes (see people/people_common.py).

  python3 build_eponym_persons_csv.py [-o OUT.csv] [--max-chars N]
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import re

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
PERSON_HUB = BASE / "people" / "person_hub.tsv"
ENTRY_TEXTS = BASE / "people" / "entry_texts.tsv"

# Institution markers -- person_hub.tsv's non_person flag is 0 for every row,
# so institutions are excluded by a positive test. Absence of a comma is NOT a
# usable signal: ~430 real people are written "surname given" with no comma.
INSTITUTION_WORDS = (
    "טעאַטער-פֿאַרבאַנד", "פֿאַרבאַנד", "פאַרבאַנד", "פֿאַראיין", "פאַראיין",
    "געזעלשאַפֿט", "געזעלשאפט", "אַקטיאָרן", "סטוּדיע", "סטודיע",
    "אַנסאַמבל", "קולטור-ליגע", "קאָמיטעט", "אָרגאַניזאַציע",
)

FIELDS = ["person_name", "heading", "person_db_id", "hub_id", "born", "died",
          "n_entries", "has_entry", "volumes", "entry_chars", "truncated",
          "entry_text"]

# Excel refuses to hold more than 32,767 characters in one cell, and 58 of
# these entries are longer (Gordin 185k, Arnshtein 151k, Akhsenfeld 112k --
# precisely the figures a reviewer looks up). Default to an excerpt that fits,
# and write the full text to a sidecar file for whoever needs all of it.
EXCEL_CELL_LIMIT = 32_767
DEFAULT_EXCERPT = 8_000


def read_tsv(p: pathlib.Path) -> list[dict]:
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def entry_ids(h: dict) -> list[str]:
    """`entry_person_ids` is PIPE-separated when a person has entries in more
    than one volume ('P-1-…|P-5-…'); 168 people do. Splitting on comma alone
    leaves them joined and the text lookup silently misses all of them."""
    raw = h.get("entry_person_ids") or ""
    return [p.strip() for p in re.split(r"[|,]", raw) if p.strip()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out", default=str(
        BASE / "organizations" / "eponym_persons_with_entries.csv"))
    ap.add_argument("--max-chars", type=int, default=DEFAULT_EXCERPT,
                    help=f"truncate entry_text (0 = full; default "
                         f"{DEFAULT_EXCERPT} keeps every cell Excel-safe)")
    ap.add_argument("--full-sidecar", action="store_true", default=True,
                    help="also write the untruncated texts next to the CSV")
    args = ap.parse_args()

    texts = {r["person_id"]: r for r in read_tsv(ENTRY_TEXTS)}
    hubs = read_tsv(PERSON_HUB)

    rows, seen, full_rows = [], set(), []
    n_with = 0
    for h in sorted(hubs, key=lambda x: x.get("canonical_heading") or ""):
        heading = (h.get("canonical_heading") or "").strip()
        dbid = (h.get("db_ids") or "").split(",")[0].strip()
        if not heading or not dbid:
            continue
        if heading.lstrip("\"'“„« ") != heading:
            continue
        if any(w in heading for w in INSTITUTION_WORDS):
            continue
        label = f"{heading} [{dbid}]"
        if label in seen:
            continue
        seen.add(label)

        got = [texts[p] for p in entry_ids(h) if p in texts]
        if len(got) > 1:
            body = "\n\n".join(
                f"[vol {g.get('volume','?')}] {g.get('entry_text','')}" for g in got)
        else:
            body = got[0].get("entry_text", "") if got else ""
        body = re.sub(r"\s+", " ", body).strip() if len(got) <= 1 else body
        full_len = len(body)
        cap = args.max_chars or EXCEL_CELL_LIMIT
        cap = min(cap, EXCEL_CELL_LIMIT)
        truncated = full_len > cap
        if truncated:
            body = body[:cap].rstrip() + " … [entry continues — see full text file]"
        if got:
            n_with += 1
            full_rows.append({"person_name": label, "person_db_id": dbid,
                              "entry_text": re.sub(r"\s+", " ", "\n\n".join(
                                  g.get("entry_text", "") for g in got)).strip()})

        rows.append({
            "person_name": label, "heading": heading, "person_db_id": dbid,
            "hub_id": h.get("hub_id") or "",
            "born": h.get("birth_date") or "", "died": h.get("death_date") or "",
            "n_entries": h.get("n_entries") or "",
            "has_entry": "yes" if got else "no",
            "volumes": ";".join(g.get("volume", "") for g in got),
            "entry_chars": full_len, "truncated": "yes" if truncated else "",
            "entry_text": body,
        })

    out = pathlib.Path(args.out)
    # utf-8-sig: Excel on Windows shows Yiddish as mojibake without the BOM.
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\r\n")
        w.writeheader()
        w.writerows(rows)

    if args.full_sidecar and full_rows:
        side = out.with_name(out.stem + "_full_texts.csv")
        with side.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["person_name", "person_db_id",
                                              "entry_text"],
                               lineterminator="\r\n")
            w.writeheader()
            w.writerows(full_rows)
        print(f"full texts  -> {side.name}")

    multi = sum(1 for r in rows if ";" in r["volumes"])
    trunc = sum(1 for r in rows if r["truncated"])
    print(f"people: {len(rows)}")
    print(f"  with an entry : {n_with} ({n_with * 100 // len(rows)}%)")
    print(f"  without       : {len(rows) - n_with}")
    print(f"  entries in 2+ volumes (merged into one cell): {multi}")
    print(f"  truncated to fit Excel: {trunc}  (full text in the sidecar)")
    print(f"  longest cell  : {max(len(r['entry_text']) for r in rows):,} chars "
          f"(Excel limit {EXCEL_CELL_LIMIT:,})")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
