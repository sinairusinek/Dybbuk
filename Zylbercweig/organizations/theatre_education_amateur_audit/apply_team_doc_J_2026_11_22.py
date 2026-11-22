#!/usr/bin/env python3
"""Decide section J (possible duplicates from the TE/Amateur audit) of the team Doc
"Org types: agencies, managements and two movements"
https://docs.google.com/document/d/1WY11RX9rwUUjyRJQR0lBTUwVq68IKym1irNTKgTQZzc/edit

Sinai 2026-11-22: "merge the clear ones, leave J9 in the Doc". J9 (Kiev studios 5097 /
5098) is NOT touched. Each set was re-read against people/entry_texts.tsv first:

  J1  1502, 4299, 1132, 601 -> 1506 (the Belarusian Yiddish state studio, Moscow, under
      Rafalsky; it became the VIMT). 601 is Berozkin's "סטודיע פֿון יידישן מלוכה-טעאַטער
      אין ווייס-רוסלאַנד", after which he joined the state theatre in Minsk: the same
      studio. Vesioloio's mention (ORG-C06725, "4 years in a studio in Minsk ... when the
      studio turned into the VIMT") moves from 1122 to 1506. 1122 STAYS: Litvinov lists
      "1922 — in Minsk, Rafalsky's Yiddish studio" and then "1923 — the Yiddish state
      studio at the Belarusian theatre institute in Moscow, from which the VIMT grew"
      as two steps.
  J2  1285 -> 1105 (Granovsky's studio, Petrograd 1919).
  J3  674, 2760 -> 1832 (Kovno Yiddish dramatic society; Kadison's ten years).
  J4  512 -> 607 (Chicago Yiddish literary-dramatic society).
  J5  1501 -> 2283 (Pirozhnikov's literary-dramatic circle at the Vilna Bund).
  J6  4783 -> 4779 (drama circle) and 2710 -> 2730 (club of the kustari, Zhitomir).
  J7  2305 -> 3495 (Shneyer directed the drama circle "in the union" from the late 1930s,
      stayed its director after joining "the pressers' union" in 1941, and was a member
      of Dress-Pressers Local 60). The circle 4761 is reparented to 3495.
  J8  1179 -> 795 (Kiev Kultur-Lige studio, 1919–24, later Moscow); 795 takes 1179's
      parent 1233 (Kultur-Lige, Kiev).
  J10 NO MERGE, by the text: the Lexicon entry "יידישע טעאַטער-סטודיע" says Weichert
      opened the studio in 1929 "again" ("נאָך זיין פּרוּוו אין 1922-24"), so the 1922
      drama school (2033) and the 1929 studio (2037) are two institutions.

Writes (dry-run by default): core_db.tsv, org_alignment_review.tsv, org_addresses_review.tsv
(merged rows' cards dropped), activity_log.tsv. Idempotent.

Usage: python3 apply_team_doc_J_2026_11_22.py [--apply]
"""
from __future__ import annotations

import csv
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(10 ** 9)
HERE = Path(__file__).resolve().parent
ORG = HERE.parent
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
WHO = "Sinai"
TAG = "Team Doc §J 2026-11-22 (TE/Amateur duplicates)"
SRC = "theatre_education_amateur_audit/apply_team_doc_J_2026_11_22.py"

MOVE = [("J1", "ORG-C06725", "1122", "1506",
         "Vesioloio: the Minsk studio that 'turned into the VIMT' = the Belarusian state studio")]
MERGE = [  # children before parents
    ("J1", "1502", "1506"), ("J1", "4299", "1506"), ("J1", "1132", "1506"), ("J1", "601", "1506"),
    ("J2", "1285", "1105"),
    ("J3", "674", "1832"), ("J3", "2760", "1832"),
    ("J4", "512", "607"),
    ("J5", "1501", "2283"),
    ("J6", "4783", "4779"), ("J6", "2710", "2730"),
    ("J7", "2305", "3495"),
    ("J8", "1179", "795"),
]
EDIT = [("J8", "795", "parent_db_id", "1233", "1179's parent: the studio of the Kiev Kultur-Lige")]
KEEP = {"1122": "J1", "2033": "J10", "2037": "J10", "5097": "J9", "5098": "J9"}


def read(p):
    raw = p.read_bytes().decode("utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rd = csv.DictReader(io.StringIO(raw, newline=""), delimiter="\t")
    return list(rd.fieldnames), list(rd), nl


def write(p, f, rows, nl):
    b = io.StringIO()
    w = csv.DictWriter(b, fieldnames=f, delimiter="\t", lineterminator=nl)
    w.writeheader()
    w.writerows(rows)
    p.write_bytes(b.getvalue().encode("utf-8"))


def pipes(v):
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def main():
    apply = "--apply" in sys.argv
    kf, core, knl = read(ORG / "core_db.tsv")
    af, al, anl = read(ORG / "org_alignment_review.tsv")
    df, cards, dnl = read(ORG / "org_addresses_review.tsv")
    by = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    card = {r["db_id"]: r for r in cards}
    log, report, errors = [], [], []

    def L(src, action, target, decision, text, extra, case):
        log.append([NOW, WHO, src, action, target, decision, f"{TAG} {case}: {text}",
                    json.dumps({**extra, "source": SRC}, ensure_ascii=False)])

    def note(row, text):
        p = (row.get("reviewer_notes") or "").strip()
        row["reviewer_notes"] = f"{TAG}: {text}" + (" | " + p if p else "")
        row["reviewer"], row["reviewed_at"] = WHO, NOW

    def setlinks(d, ids):
        by[d]["linked_cluster_ids"] = " | ".join(ids)
        if d in card:
            card[d]["linked_cluster_ids"] = " | ".join(ids)

    def link(cid, d):
        if cid not in pipes(by[d]["linked_cluster_ids"]):
            setlinks(d, pipes(by[d]["linked_cluster_ids"]) + [cid])

    # ---- whole-cluster moves
    for case, cid, frm, to, why in MOVE:
        a = arow.get(cid)
        if a is None:
            errors.append(f"{case}: no review row {cid}")
            continue
        if a["aligned_db_id"].strip() == to and cid in pipes(by[to]["linked_cluster_ids"]):
            continue
        was = f"{a['decision']} {a['aligned_db_id']}".strip()
        a["decision"], a["aligned_db_id"], a["org_type"] = "ALIGN", to, by[to]["org_type"]
        note(a, f"{was} -> ALIGN db {to}. {why}")
        setlinks(frm, [x for x in pipes(by[frm]["linked_cluster_ids"]) if x != cid])
        link(cid, to)
        L("org_review", "alignment", cid, "ALIGN", why, {"aligned_db_id": to, "was": was}, case)
        report.append(f"{case} move {cid} {frm} -> {to}")

    # ---- merges
    for case, dup, into in MERGE:
        r, t = by[dup], by[into]
        if r["merged_into"] == into:
            continue
        if r["deprecated"].strip() or t["deprecated"].strip():
            errors.append(f"{case}: {dup} or {into} already deprecated")
            continue
        for c in pipes(r["linked_cluster_ids"]):
            if c in arow and arow[c]["aligned_db_id"].strip() != into:
                arow[c]["aligned_db_id"], arow[c]["decision"], arow[c]["org_type"] = into, "ALIGN", t["org_type"]
                note(arow[c], f"db {dup} merged into {into}")
            link(c, into)
        var =[v.strip() for v in t["name_variants"].split("|") if v.strip()]
        for n in (r["name_yiddish"], r["name"]):
            n = n.strip()
            if n and n not in var and n != t["name_yiddish"].strip() and n != t["name"].strip():
                var.append(n)
        t["name_variants"] = "|".join(var)
        for k in core:
            if k["parent_db_id"] == dup:
                k["parent_db_id"] = into
                if k["db_id"] in card and "parent_db_id" in card[k["db_id"]]:
                    card[k["db_id"]]["parent_db_id"] = into
                report.append(f"{case} reparent {k['db_id']} {dup} -> {into}")
        r["deprecated"], r["merged_into"], r["linked_cluster_ids"] = "true", into, ""
        cards[:] = [k for k in cards if k["db_id"] != dup]
        card.pop(dup, None)
        L("org_addresses", "db_merge", into, "MERGE_INTO", f"merged db {dup} into {into}", {"merged_db_id": dup}, case)
        report.append(f"{case} merge {dup} -> {into}")

    # ---- edits
    for case, d, k, v, why in EDIT:
        if by[d][k] != v:
            L("org_review", "set_parent" if k == "parent_db_id" else "edit_db", d, "",
              f"{k}: {by[d][k]!r} -> {v!r} ({why})", {k: v}, case)
            report.append(f"{case} edit {d} {k}: {by[d][k]!r} -> {v!r}")
            by[d][k] = v
            if d in card and k in card[d]:
                card[d][k] = v

    # ---- sanity
    for d, case in KEEP.items():
        if by[d]["deprecated"].strip():
            errors.append(f"{case}: {d} must stay live")
    touched = {c for _, c, *_ in MOVE} | {c for _, _, into in MERGE for c in pipes(by[into]["linked_cluster_ids"])}
    for c in touched:
        owners = [r["db_id"] for r in core if c in pipes(r["linked_cluster_ids"]) and not r["deprecated"].strip()]
        if c in arow and arow[c]["decision"] == "ALIGN" and owners != [arow[c]["aligned_db_id"].strip()]:
            errors.append(f"link check {c}: aligned {arow[c]['aligned_db_id']} owners {owners}")
    for r in core:
        if r["parent_db_id"] and by.get(r["parent_db_id"], {}).get("deprecated", "").strip() and not r["deprecated"].strip():
            errors.append(f"{r['db_id']} has deprecated parent {r['parent_db_id']}")

    print("\n".join(report))
    print(f"\n{len(log)} log rows; {len(errors)} errors")
    for e in errors:
        print("ERROR", e)
    if errors:
        return 2
    if not apply:
        print("DRY RUN — re-run with --apply")
        return 0
    write(ORG / "core_db.tsv", kf, core, knl)
    write(ORG / "org_alignment_review.tsv", af, al, anl)
    write(ORG / "org_addresses_review.tsv", df, cards, dnl)
    p = ORG / "activity_log.tsv"
    b = io.StringIO()
    csv.writer(b, delimiter="\t", lineterminator="\r\n").writerows(log)
    raw = p.read_bytes()
    p.write_bytes(raw + (b"" if raw.endswith(b"\n") else b"\r\n") + b.getvalue().encode("utf-8"))
    print("WROTE core_db, org_alignment_review, org_addresses_review, activity_log")
    return 0


if __name__ == "__main__":
    sys.exit(main())
