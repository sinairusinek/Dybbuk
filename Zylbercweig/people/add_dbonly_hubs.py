#!/usr/bin/env python3
"""Give a hub to a person who has a people_db row but NO lexicon entry.

WHY THIS EXISTS -- the special case, documented
===============================================
`person_hub.tsv` is built by `build_person_hub.py` from lexicon ENTRIES: every
hub is anchored on at least one `entry_person_ids` member. A person the Leksikon
only MENTIONS therefore gets no hub, however well attested they are.

That is normally harmless, but it blocks one specific thing: an eponym edge
carries `person_hub_id`, so `apply_eponym_decisions.py` cannot write an edge for
a person with no hub. A reviewer can decide "this troupe is named after X" and
the decision can never be applied. On 2026-10-25 that was 24 of Ruthie's answers;
3 of those people (db 3832, 3833, 3837) already had db rows and needed only this.

These hubs are DIFFERENT from builder-made hubs and must stay recognisable:

  * `hub_id` is `HUB-D<db_id>`, the same convention `build_person_hub.py` uses
    for a db-anchored hub, so downstream joins need no special case.
  * `n_entries = 0` and `entry_person_ids` is EMPTY. No builder hub looks like
    this -- that is the marker. Anything counting lexicon entries per hub must
    keep treating 0 as 0, not as missing data.
  * `evidence = db_only_mention_attested` names the provenance, so these never
    masquerade as `ra_align` / `human_align` hubs.
  * `non_person = 0` is asserted, not inferred: these are minted people with
    `new_mints_evidence.tsv` rows stating `attested_in_leksikon=mentions_only`.

BEWARE THE REBUILD. `build_person_hub.py` regenerates person_hub.tsv from
entries and will DROP every row written here. Re-run this script after any hub
rebuild, or teach the builder to read `dbonly_hubs.tsv` (the ledger this script
writes) as an additional source. The ledger, not person_hub.tsv, is the record.

WHY NOT JUST MATCH BY NAME. Resolving a reviewer's free-text answer to an
existing hub by name similarity is how db864 `אידא קאמינסקי` lands on
`HUB-D3612` -- Regina Kaminska, Ida's sister, who died in 1913 and had 56 of
Ida's attestations mis-credited to her until `9a4927395` detached them. Minting
Ida her own hub is the fix; fuzzy matching is the bug.

  python3.11 add_dbonly_hubs.py --db-ids 3832,3833,3837 --reviewer Sinai
  python3.11 add_dbonly_hubs.py --db-ids 3832,3833,3837 --reviewer Sinai --apply
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import pathlib
import sys

csv.field_size_limit(10 ** 9)

HERE = pathlib.Path(__file__).resolve().parent
HUB = HERE / "person_hub.tsv"
DB = HERE / "people_db.tsv"
EVIDENCE = HERE / "new_mints_evidence.tsv"
LEDGER = HERE / "dbonly_hubs.tsv"

EVIDENCE_TAG = "db_only_mention_attested"
LEDGER_FIELDS = ["hub_id", "db_id", "canonical_heading", "reason",
                 "attested_in_leksikon", "reviewer", "added_at", "notes"]


def read_tsv(p: pathlib.Path) -> list[dict]:
    if not p.exists():
        return []
    with open(p, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def terminator(p: pathlib.Path) -> str:
    raw = p.read_bytes()
    crlf = raw.count(b"\r\n")
    return "\r\n" if crlf > (raw.count(b"\n") - crlf) else "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-ids", required=True,
                    help="comma-separated people_db ids to give a hub")
    ap.add_argument("--reviewer", required=True)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    want = [i.strip() for i in a.db_ids.split(",") if i.strip()]
    hubs = read_tsv(HUB)
    fields = list(hubs[0].keys())
    db = {r["db_id"].strip(): r for r in read_tsv(DB)}
    ev = {r["db_id"].strip(): r for r in read_tsv(EVIDENCE)}

    claimed: set[str] = set()
    for h in hubs:
        for d in (h.get("db_ids") or "").split("|"):
            if d.strip():
                claimed.add(d.strip())

    rows, skip = [], []
    for i in want:
        if i not in db:
            skip.append((i, "no people_db row")); continue
        if i in claimed:
            skip.append((i, "already has a hub")); continue
        r = db[i]
        e = ev.get(i)
        # Guard: only MENTION-attested people belong here. A person the Leksikon
        # does not know at all (probably_not_zylbercweig=1 / attested=no) must
        # not get a hub -- that would put a non-Zylbercweig person into the
        # corpus graph. See README_new_mints.md for the flag's contract.
        if (r.get("probably_not_zylbercweig") or "").strip() == "1":
            skip.append((i, "flagged probably_not_zylbercweig")); continue
        att = (e or {}).get("attested_in_leksikon", "").strip()
        if e and att == "no":
            skip.append((i, "attested_in_leksikon=no")); continue
        if not e:
            skip.append((i, "no new_mints_evidence row -- add evidence first"))
            continue
        heading = (r.get("hebname") or "").strip()
        rows.append({
            "hub_id": f"HUB-D{i}", "db_id": i, "canonical_heading": heading,
            "english": (r.get("english") or "").strip(),
            "attested": att,
            "evidence_quote": (e.get("leksikon_evidence") or "")[:60],
        })

    print(f"requested {len(want)}  ->  to add {len(rows)}, skipped {len(skip)}")
    for i, why in skip:
        print(f"  SKIP db{i}: {why}")
    for r in rows:
        print(f"  ADD  {r['hub_id']:<12} {r['canonical_heading'][:28]:28} "
              f"({r['english'][:22]}) attested={r['attested']}")
        print(f"       evidence: {r['evidence_quote']}")

    if not rows:
        print("\nnothing to do.")
        return 0
    if not a.apply:
        print("\n(dry run -- pass --apply to write)")
        return 0

    now = _dt.datetime.now().isoformat(timespec="seconds")
    new_hub_rows = []
    for r in rows:
        row = {k: "" for k in fields}
        row.update({
            "hub_id": r["hub_id"],
            "canonical_heading": r["canonical_heading"],
            "n_entries": "0",          # the marker: no builder hub has 0
            "n_db_rows": "1",
            "n_surfaces": "0",
            "entry_person_ids": "",    # the marker: empty by construction
            "db_ids": r["db_id"],
            "volumes": "",
            "evidence": EVIDENCE_TAG,
            "pending_drafts": "0",
            "multi_db": "0",
            "non_person": "0",
            "top_surfaces": "",
        })
        new_hub_rows.append(row)

    term = terminator(HUB)
    tmp = HUB.with_suffix(".tsv.tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                           lineterminator=term, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(hubs); w.writerows(new_hub_rows)
    tmp.replace(HUB)

    # Append-only ledger: person_hub.tsv is REGENERABLE and will lose these on
    # the next build, so the ledger is the durable record of what was added.
    existing = read_tsv(LEDGER)
    have = {(x["hub_id"], x["db_id"]) for x in existing}
    add = [{
        "hub_id": r["hub_id"], "db_id": r["db_id"],
        "canonical_heading": r["canonical_heading"],
        "reason": "eponym edge needs person_hub_id; person has no lexicon entry",
        "attested_in_leksikon": r["attested"],
        "reviewer": a.reviewer, "added_at": now,
        "notes": "db-only hub; dropped by build_person_hub.py -- re-run this script",
    } for r in rows if (r["hub_id"], r["db_id"]) not in have]
    if add:
        lterm = terminator(LEDGER) if LEDGER.exists() else "\n"
        write_header = not LEDGER.exists()
        with open(LEDGER, "a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=LEDGER_FIELDS, delimiter="\t",
                               lineterminator=lterm, quoting=csv.QUOTE_MINIMAL)
            if write_header:
                w.writeheader()
            w.writerows(add)

    print(f"\nAPPLIED: {len(hubs)} -> {len(hubs) + len(new_hub_rows)} hub rows "
          f"(terminator={term!r}); {len(add)} ledger rows appended")
    print("Re-run apply_eponym_decisions.py to turn the unblocked decisions "
          "into edges.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
