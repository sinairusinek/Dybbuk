#!/usr/bin/env python3
"""Warsaw Hazomir as a roof (Sinai, 2026-11-15).

Sinai: make Hazomir Warsaw (already minted) the roof organisation for every other
organisation with 'Hazomir' in its name in Warsaw; do NOT merge with, nor link to,
Hazomir in other places. Asked which record: "1872 roof, merge 4714 in".

  db 1872 פֿאַראיין „הזמיר“ (Maaty 08-22; clusters 'וואַרשעווער „הזמיר“') = Warsaw Hazomir, the roof.
  Its Warsaw sub-organisations are already parent_db_id = 1872: 913 drama section,
  917 Artistish Vinkele, 800 David Herman's studio (set earlier today).
  db 4714 „הזמיר“ (Judith 11-14) is the same Warsaw society -> merged into 1872.

4714's cluster ORG-C00114_Q01_P01 existed only in org_alignment_review.tsv: the
Q01 -> P01/P02 split was never written to organizations_clustered.tsv, so its 21
mentions still sat under ORG-C00114_Q01 and 4714 showed none. Each mention is
assigned here by its entry, read against the Lexicon text:
  _P01 -> 1872  Warsaw Hazomir itself (members, choir, music director, founding)
  _P02          Noworadomsk Hazomir (unchanged, undecided)
  _P03 -> 913   the Warsaw drama section
  _P04          not Warsaw (Vilna, Canada, Lodz?) — left undecided, not linked

Idempotent; dry-run by default.  Usage: python3 apply_hazomir_warsaw_roof_2026_11_15.py [--apply]
Run sync_address_cards.py afterwards.
"""
import csv
import io
import json
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(10 ** 9)
HERE = Path(__file__).resolve().parent
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
SRC = "Sinai 2026-11-15: Warsaw Hazomir roof (apply_hazomir_warsaw_roof_2026_11_15.py)"
BASE, ROOF, DUP, DRAMA = "ORG-C00114_Q01", "1872", "4714", "913"
HEAD = "_ - heading"
PARTS = {  # heading substring -> part
    "פּאָלאַקעוויטש": "P01", "פּאָלאָקעוויטש": "P01", "לובעלטשיק": "P01", "שניאור, מ.": "P01",
    "ליאָוו, לעאָ": "P01", "מאָנפֿריד": "P01", "מילנער": "P01", "סטוטשקאָוו": "P01",
    "ביראָ, פעלאַ": "P02",
    "פיביך": "P03", "שולווייס": "P03", "טייטעלבוים, אַברהם": "P03", "טיימאַן": "P03",
    "גליקמאַן, מאלווינע": "P03", "וויידע, מרים": "P03", "ווייסליץ": "P03", "שווייד": "P03",
    "אַרצישעווסקאַ": "P03",
    "רוסקין": "P04", "ראַבינאָוויטש, ישראל": "P04", "לובינסקי": "P04",
}
WHY = {
    "P01": "Warsaw Hazomir itself: members, the choir (Shneyer), music director (Liov), its founding (Milner, Monfried)",
    "P03": "the drama section of Warsaw Hazomir (Fibikh 1909, Shulweis 1908–10, Weislitz 1911, Glikman 1913, Teyman under Milner, Veyde, Shveyd, Teitelbaum; Artsishevska in Peretz's circle)",
    "P04": "NOT Warsaw — Ruskin (born Vilna 1907: the Vilna Hazomir's drama section), I. Rabinovitch (founded a 'Hazomir' farein in Canada 1914), L. Lubinski (grew up in Lodz). Left undecided; Sinai: no link to Hazomir in other places",
}


def sk(s):
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))


def read(p):
    raw = p.read_bytes().decode("utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rd = csv.DictReader(io.StringIO(raw, newline=""), delimiter="\t")
    return list(rd.fieldnames), list(rd), nl


def write(p, f, rows, nl):
    b = io.StringIO()
    w = csv.DictWriter(b, fieldnames=f, delimiter="\t", lineterminator=nl)
    w.writeheader(); w.writerows(rows)
    p.write_bytes(b.getvalue().encode("utf-8"))


def pipes(v):
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def main():
    apply = "--apply" in sys.argv
    kf, core, knl = read(HERE / "core_db.tsv")
    af, al, anl = read(HERE / "org_alignment_review.tsv")
    cf, cl, cnl = read(HERE / "organizations_clustered.tsv")
    df, cards, dnl = read(HERE / "org_addresses_review.tsv")
    by = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    log = []

    def note(a, t):
        p = (a.get("reviewer_notes") or "").strip()
        a["reviewer_notes"] = (p + " | " if p else "") + t
        a["reviewer"], a["reviewed_at"] = "Sinai", NOW

    # 1. write the Q01 split into the mentions file
    mine = [x for x in cl if x["cluster_id"].strip() == BASE]
    if mine:
        assign = {}
        for x in mine:
            hit = [p for h, p in PARTS.items() if sk(h) in sk(x[HEAD])]
            if len(set(hit)) != 1:
                print("ERROR unassigned/ambiguous:", x[HEAD], hit); return 2
            assign[id(x)] = hit[0]
        sizes = {p: sum(1 for x in mine if assign[id(x)] == p) for p in ("P01", "P02", "P03", "P04")}
        for x in mine:
            x["cluster_id"] = f"{BASE}_{assign[id(x)]}"
            x["cluster_size"] = str(sizes[assign[id(x)]])
        print("split", BASE, sizes)
    else:
        sizes = {p: sum(1 for x in cl if x["cluster_id"].strip() == f"{BASE}_{p}") for p in ("P01", "P02", "P03", "P04")}
        print("already split", sizes)

    # 2. alignment rows
    tmpl = arow[f"{BASE}_P01"]
    for p, target, typ in (("P03", DRAMA, "Amateur"), ("P04", "", "Musical organization")):
        c = f"{BASE}_{p}"
        if c not in arow:
            a = dict(tmpl)
            a.update(cluster_id=c, reviewer_notes="", decision="", aligned_db_id="", org_type=typ)
            al.insert(al.index(tmpl) + 1, a); arow[c] = a
    for p in ("P01", "P02", "P03", "P04"):
        arow[f"{BASE}_{p}"]["cluster_size"] = str(sizes[p])
    a1 = arow[f"{BASE}_P01"]
    if a1["aligned_db_id"] != ROOF:
        a1["decision"], a1["aligned_db_id"] = "ALIGN", ROOF
        note(a1, f"2026-11-15: {WHY['P01']}; db {DUP} merged into {ROOF}")
        log.append([NOW, "Sinai", "org_review", "alignment", a1["cluster_id"], "ALIGN", WHY["P01"], json.dumps({"aligned_db_id": ROOF, "source": SRC})])
    a3 = arow[f"{BASE}_P03"]
    if a3["aligned_db_id"] != DRAMA:
        a3["decision"], a3["aligned_db_id"] = "ALIGN", DRAMA
        note(a3, f"2026-11-15: {WHY['P03']}")
        log.append([NOW, "Sinai", "org_review", "alignment", a3["cluster_id"], "ALIGN", WHY["P03"], json.dumps({"aligned_db_id": DRAMA, "source": SRC})])
    a4 = arow[f"{BASE}_P04"]
    if "NOT Warsaw" not in (a4.get("reviewer_notes") or ""):
        a4["reviewer_notes"] = f"2026-11-15: {WHY['P04']}"

    # 3. core_db: merge 4714 into 1872; links
    d = by[DUP]
    if d["merged_into"] != ROOF:
        d["deprecated"], d["merged_into"], d["linked_cluster_ids"] = "true", ROOF, ""
        log.append([NOW, "Sinai", "org_addresses", "db_merge", ROOF, "MERGE_INTO",
                    f"merged db {DUP} into {ROOF}: both are the Warsaw Hazomir; 1872 is the roof of the Warsaw Hazomir units (913, 917, 800)",
                    json.dumps({"merged_db_id": DUP, "source": SRC})])
    for db_id, c in ((ROOF, f"{BASE}_P01"), (DRAMA, f"{BASE}_P03")):
        r = by[db_id]
        if c not in pipes(r["linked_cluster_ids"]):
            r["linked_cluster_ids"] = " | ".join(pipes(r["linked_cluster_ids"]) + [c])
        for k in cards:
            if k["db_id"] == db_id and c not in pipes(k["linked_cluster_ids"]):
                k["linked_cluster_ids"] = " | ".join(pipes(k["linked_cluster_ids"]) + [c])
    cards = [k for k in cards if k["db_id"] != DUP]  # merged rows have no card (cf. 4104 -> 4036)
    for u in ("913", "917", "800"):
        assert by[u]["parent_db_id"] == ROOF, u

    print(f"{len(log)} log rows; P01→{ROOF} {sizes['P01']}, P03→{DRAMA} {sizes['P03']}, P02 {sizes['P02']} and P04 {sizes['P04']} undecided")
    if not apply:
        print("DRY RUN"); return 0
    write(HERE / "core_db.tsv", kf, core, knl)
    write(HERE / "org_alignment_review.tsv", af, al, anl)
    write(HERE / "organizations_clustered.tsv", cf, cl, cnl)
    write(HERE / "org_addresses_review.tsv", df, cards, dnl)
    p = HERE / "activity_log.tsv"
    b = io.StringIO(); csv.writer(b, delimiter="\t", lineterminator="\r\n").writerows(log)
    raw = p.read_bytes()
    p.write_bytes(raw + (b"" if raw.endswith(b"\n") else b"\r\n") + b.getvalue().encode("utf-8"))
    print("WROTE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
