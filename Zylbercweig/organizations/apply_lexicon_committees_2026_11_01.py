"""Lexicon committees under a roof org (Sinai, 2026-11-01).

db 310 (the Lexicon of the Yiddish Theatre) is the roof: the project itself,
its correspondents and editorial board, and every committee mention that
names no country. The national publishing committees are distinct bodies
with their own members (P065-P067) and become children via parent_db_id —
the existing convention for branches of one organization:

  3593  Argentine committee (exists)          parent -> 310
  new   Polish committee, vol. 1 (ORG-C04471)  parent 310
  new   Romanian committee (ORG-C06032)        parent 310

ORG-C02398 'the committee for the Lexicon' (member Ari Furman) was marked
NOT_AN_ORG; it names the committee, so it joins the roof, 310.

Writes core_db.tsv, org_alignment_review.tsv, org_addresses_review.tsv
(parent on the cards, which the Cards view reads); ids from origin's
high-water mark; idempotent. Run sync_address_cards.py BEFORE this script
so the new rows have cards to stamp — or re-run this after the sync.

Usage: python3 apply_lexicon_committees_2026_11_01.py [--apply --reviewer Sinai]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import sys
from pathlib import Path

from apply_publisher_audit_mints import origin_high_water

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
ROOF = "310"
NEW = [  # (name_yiddish, name, cluster, why)
    ('פּוילישער פֿאַרלאַגס-קאָמיטעט פֿון „לעקסיקאָן פֿון יידישן טעאַטער"',
     "Polish publishing committee of the Lexicon of the Yiddish Theatre",
     "ORG-C04471", "the Polish committee for volume 1 (David Lederman, member)"),
    ('רומענישער פֿאַרלאַגס-קאָמיטעט פֿון „לעקסיקאָן פֿון יידישן טעאַטער"',
     "Romanian publishing committee of the Lexicon of the Yiddish Theatre",
     "ORG-C06032", "the Romanian committee (Itsikl Goldenberg, Moritz Zigler, members)"),
]
EXISTING_CHILDREN = ["3593"]
TO_ROOF = {"ORG-C02398": "'the committee for the Lexicon' (Ari Furman, member) — "
                         "a committee mention naming no country"}


def _rw(p: Path):
    term = "\r\n" if b"\r\n" in p.read_bytes()[:4096] else "\n"
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd), term


def _dump(p: Path, fields, rows, term) -> None:
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator=term)
        w.writeheader()
        w.writerows(rows)


def main(apply: bool, reviewer: str) -> None:
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cf, core, ct = _rw(HERE / "core_db.tsv")
    af, al, at = _rw(HERE / "org_alignment_review.tsv")
    df, cards, dt = _rw(HERE / "org_addresses_review.tsv")
    by = {r["db_id"]: r for r in core}
    rv = {r["cluster_id"]: r for r in al}
    linked = {c.strip(): r["db_id"] for r in core
              for c in (r.get("linked_cluster_ids") or "").split("|") if c.strip()}
    nxt = max(origin_high_water(),
              max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    log, children = [], list(EXISTING_CHILDREN)

    def note(r, text):
        r["reviewer"], r["reviewed_at"] = reviewer, now
        r["reviewer_notes"] = (r["reviewer_notes"] + " | " + text
                               if r.get("reviewer_notes", "").strip() else text)

    for yi, en, cid, why in NEW:
        if cid in linked:
            children.append(linked[cid])
            print(f"  skip {cid}: already db {linked[cid]}")
            continue
        db = str(nxt); nxt += 1
        row = {f: "" for f in cf}
        row.update(db_id=db, name=en, name_yiddish=yi, org_type="Publisher",
                   linked_cluster_ids=cid, parent_db_id=ROOF)
        core.append(row); by[db] = row; children.append(db)
        r = rv[cid]
        r["decision"], r["aligned_db_id"] = "ALIGN", db
        note(r, f"2026-11-01: minted db {db} under the Lexicon roof (db {ROOF}): {why}")
        log += [[now, reviewer, "org_review", "mint", db, "NEW", f"{yi} — {why}",
                 json.dumps({"clusters": [cid], "parent_db_id": ROOF,
                             "source": Path(__file__).name}, ensure_ascii=False)],
                [now, reviewer, "org_review", "alignment", cid, "ALIGN",
                 f"minted db {db}", json.dumps({"aligned_db_id": db})]]
        print(f"  mint db {db}  {yi}  <- {cid}  (parent {ROOF})")

    for db in children:
        if by[db].get("parent_db_id") != ROOF:
            by[db]["parent_db_id"] = ROOF
            log.append([now, reviewer, "org_addresses", "set_parent", db, f"parent_db_id={ROOF}",
                        "national Lexicon committee under the Lexicon roof",
                        json.dumps({"source": Path(__file__).name})])
            print(f"  core_db {db}: parent_db_id -> {ROOF}")
    for c in cards:
        if c["db_id"] in children and c.get("parent_db_id") != ROOF:
            c["parent_db_id"] = ROOF
            print(f"  card {c['db_id']}: parent_db_id -> {ROOF}")
    missing = [d for d in children if d not in {c["db_id"] for c in cards}]
    if missing:
        print(f"  NOTE: no card yet for {missing} — run sync_address_cards.py, then this again")

    roof = by[ROOF]
    cur = [x.strip() for x in roof["linked_cluster_ids"].split("|") if x.strip()]
    for cid, why in TO_ROOF.items():
        r = rv[cid]
        if r.get("decision") == "ALIGN" and r.get("aligned_db_id") == ROOF:
            continue
        r["decision"], r["aligned_db_id"] = "ALIGN", ROOF
        note(r, f"2026-11-01: was NOT_AN_ORG; {why} -> db {ROOF}")
        if cid not in cur:
            cur.append(cid)
        log.append([now, reviewer, "org_review", "alignment", cid, "ALIGN", why,
                    json.dumps({"aligned_db_id": ROOF, "before": "NOT_AN_ORG"})])
        print(f"  {cid}: NOT_AN_ORG -> ALIGN {ROOF}")
    roof["linked_cluster_ids"] = " | ".join(cur)

    print(f"\n{len(log)} log row(s)")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME to write")
        return
    _dump(HERE / "core_db.tsv", cf, core, ct)
    _dump(HERE / "org_alignment_review.tsv", af, al, at)
    _dump(HERE / "org_addresses_review.tsv", df, cards, dt)
    p = HERE / "activity_log.tsv"
    b = p.read_bytes()
    with p.open("ab") as f:
        if not b.endswith(b"\n"):
            f.write(b"\r\n")
        for row in log:
            f.write(("\t".join(row) + "\r\n").encode("utf-8"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    main(a.apply, a.reviewer.strip())
