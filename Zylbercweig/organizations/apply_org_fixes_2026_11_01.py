"""Sinai, 2026-11-01: one row each for three firms split across clusters,
a row for the Tel Aviv Yavneh, and db 1564 cancelled as not an org.

  mint  Poznański, Łódź      <- ORG-C02739, ORG-C04629 (one sentence read twice:
                                 'י.ק.' / 'י. ל.'), ORG-C06895 'the firm Poznański'
  mint  Heintzel & Kunitzer  <- ORG-C01235, ORG-C06813
  mint  Manischewitz         <- ORG-C01206, ORG-C07038
  mint  Yavneh, Tel Aviv     <- ORG-C07368. P064 (10-10) rejected it as db 74 —
                                 the Warsaw Yabne (Levitas, who left for Riga) —
                                 but never gave it a row: rejecting a match is not
                                 denying the entity.
  cancel db 1564 'אָדעסער סוחר פֿון דריטער גילדע' — a merchant's rank, not an
                                 org: deprecated, link cleared, ORG-C02251 ->
                                 NOT_AN_ORG (earlier cancellations left the cluster
                                 NEW and pointing at the dead row).

Ids come from origin's high-water mark at run time (never hand-assigned).
Idempotent. Run sync_address_cards.py afterwards.

Usage: python3 apply_org_fixes_2026_11_01.py [--apply --reviewer Sinai]
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
LABOUR = "Labour (factory/workshop)"

MINTS = [  # (name_yiddish, name, org_type, clusters, why)
    ("פֿירמע י. ק. פּאָזנאַנסקי (לאָדזש)", "I. K. Poznański", LABOUR,
     ["ORG-C02739", "ORG-C04629", "ORG-C06895"], "Łódź textile mills"),
    ("היינצל און קוניצער (לאָדזש)", "Heinzel & Kunitzer", LABOUR,
     ["ORG-C01235", "ORG-C06813"], "Łódź textile mills"),
    ("מאַנישעוויטש-פֿירמע", "B. Manischewitz Company", LABOUR,
     ["ORG-C01206", "ORG-C07038"], "matzo and food manufacturer"),
    ('הוצאת „יבנה" (תל-אביב)', "Yavneh (Tel Aviv)", "Publisher",
     ["ORG-C07368"], "founded 1932 by Joshua Orenstein; not db 74 (P064)"),
]
CANCEL = {"1564": ("ORG-C02251", "a merchant's rank (סוחר פֿון דריטער גילדע), not an organization")}


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
    rv = {r["cluster_id"]: r for r in al}
    linked = {c.strip(): r["db_id"] for r in core
              for c in (r.get("linked_cluster_ids") or "").split("|") if c.strip()}
    nxt = max(origin_high_water(),
              max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    log = []

    def stamp(row, note):
        row["reviewer"], row["reviewed_at"] = reviewer, now
        row["reviewer_notes"] = (row["reviewer_notes"] + " | " + note
                                 if row.get("reviewer_notes", "").strip() else note)

    for yi, en, typ, cids, why in MINTS:
        if any(c in linked for c in cids):
            print(f"  skip {yi}: already linked to db {linked[next(c for c in cids if c in linked)]}")
            continue
        db = str(nxt); nxt += 1
        row = {f: "" for f in cf}
        row.update(db_id=db, name=en, name_yiddish=yi, org_type=typ,
                   linked_cluster_ids=" | ".join(cids))
        core.append(row)
        for c in cids:
            r = rv[c]
            r["decision"], r["aligned_db_id"], r["org_type"] = "ALIGN", db, typ
            stamp(r, f"2026-11-01: minted db {db} {yi} for {', '.join(cids)} ({why})")
            log.append([now, reviewer, "org_review", "alignment", c, "ALIGN",
                        f"minted db {db} {yi}: {why}", json.dumps({"aligned_db_id": db})])
        log.append([now, reviewer, "org_review", "mint", db, "NEW", f"{yi} — {why}",
                    json.dumps({"clusters": cids, "org_type": typ,
                                "source": Path(__file__).name}, ensure_ascii=False)])
        print(f"  mint db {db}  {yi}  <- {', '.join(cids)}")

    for db, (cid, why) in CANCEL.items():
        row = next(r for r in core if r["db_id"] == db)
        if row.get("deprecated", "").strip() in ("true", "1"):
            print(f"  skip cancel {db}: already deprecated")
            continue
        row["deprecated"], row["linked_cluster_ids"] = "true", ""
        r = rv[cid]
        r["decision"], r["aligned_db_id"] = "NOT_AN_ORG", ""
        stamp(r, f"2026-11-01: {why}; db {db} cancelled")
        log.append([now, reviewer, "org_review", "deprecate", db, "NOT_AN_ORG",
                    f"db {db} cancelled: {why}", json.dumps({"cluster_id": cid})])
        print(f"  cancel db {db}  ({cid} -> NOT_AN_ORG)")

    print(f"\n{len(log)} log row(s)")
    if not apply:
        print("dry-run; pass --apply --reviewer NAME to write")
        return
    _dump(HERE / "core_db.tsv", cf, core, ct)
    _dump(HERE / "org_alignment_review.tsv", af, al, at)
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
