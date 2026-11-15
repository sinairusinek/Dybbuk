#!/usr/bin/env python3
"""Mint the remaining Hazomir clusters, one record per place (Sinai 2026-11-15: "yes, mint them").

Every Hazomir is its own local society: nothing here is merged with, or parented to,
Warsaw Hazomir (1872) or any other city's Hazomir. Musical organization for the
societies (CLASSIFICATION_POLICY type 16 names Hazomir); a drama section named in the
sentence is an Amateur child of its city's Hazomir (Rule 3).

ORG-C00114_Q01_P04 held three places; it is split first:
  _P04 Ruskin (born Vilna 1907; the drama section of the Vilna Hazomir)  -> new Vilna drama section
  _P05 I. Rabinovitch (founded a dramatic-musical farein Hazomir 1914; Montreal, Kanader Adler) -> new Montreal Hazomir
  _P06 L. Lubinski (back in Lodz after the army: 'Unzer Vinkl', 'Hazomir', dir. Becher Zwilling) -> existing Lodz Hazomir 1796

Ids from origin's high-water mark. Idempotent (skips names that already exist). Dry-run by default.
Run sync_address_cards.py afterwards.
"""
import csv
import io
import json
import subprocess
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(10 ** 9)
HERE = Path(__file__).resolve().parent
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
SRC = "Sinai 2026-11-15: mint the Hazomir clusters (mint_hazomir_cities_2026_11_15.py)"
MUS, AM = "Musical organization", "Amateur"
SPLIT = {"רוסקין": "ORG-C00114_Q01_P04", "ראַבינאָוויטש, ישראל": "ORG-C00114_Q01_P05", "לובינסקי": "ORG-C00114_Q01_P06"}

# key, name_yiddish, address, type, clusters, parent (db id or key), why
MINTS = [
    ("kherson", "הזמיר (כערסאָן)", "כערסאָן", MUS, ["ORG-C00114_Q05"], "", "W. Goldfaden head of the society 'Hazomir' in Kherson, 1919"),
    ("montreal", "הזמיר (מאָנטרעאַל)", "מאָנטרעאַל", MUS, ["ORG-C00114_Q01_P05"], "", "I. Rabinovitch co-founded the dramatic-musical farein 'Hazomir' in 1914 (Montreal)"),
    ("montreal_drama", "דראָמאָטישע סעקציע ביים „הזמיר“ (מאָנטרעאַל)", "מאָנטרעאַל", AM, ["ORG-C00114_Q06"], "montreal", "B. Goldstein two years director of the drama section at Hazomir, Montreal"),
    ("lublin", "הזמיר (לובלין)", "לובלין", MUS, ["ORG-C00114_Q07"], "", "Paula Prilutski 1912: a performance in the Hazomir hall, Lublin"),
    ("kovno", "הזמיר (קאָוונע)", "קאָוונע", MUS, ["ORG-C00114_Q08"], "", "Yetta Pikelchik 1912: the dramatic-musical society 'Hazomir', Kovno, mother roles"),
    ("mlawa", "הזמיר (מלאַווע)", "מלאַווע", MUS, ["ORG-C00114_Q09"], "", "Max Davis a member, Mława"),
    ("bialystok", "הזמיר (ביאַליסטאָק)", "ביאַליסטאָק", MUS, ["ORG-C00114_Q10"], "", "Esther Zevkina a member of the Białystok Hazomir"),
    ("london", "הזמיר (לאָנדאָן)", "לאָנדאָן", MUS, ["ORG-C00114_Q11", "ORG-C07040"], "", "S. D. Weinberg founded the 'Hazomir'-farein; Meir Pozner later its conductor (London)"),
    ("tomaszow", "הזמיר (טאָמאַשאָוו)", "טאָמאַשאָוו", MUS, [], "", "host of the existing drama circle db 2790"),
    ("noworadomsk", "הזמיר (נאָוואַראַדאָמסק)", "נאָוואַראַדאָמסק", MUS, ["ORG-C00114_Q01_P02"], "", "Fela Biro played in Gordin's 'Yidisher Kenig Lir' at the Noworadomsk Hazomir (dir. Shmuel Frank)"),
    ("vilna", "הזמיר (ווילנע)", "ווילנע", MUS, [], "", "host of the Vilna Hazomir drama section"),
    ("vilna_drama", "דראַמאַטישע סעקציע ביים „הזמיר“ (ווילנע)", "ווילנע", AM, ["ORG-C00114_Q01_P04"], "vilna", "Shimon Ruskin (born Vilna 1907) first appeared with the drama section of Hazomir"),
    ("bureau", "„הזמיר“ קאָנצערט-ביוראָ (קאַנאַדע)", "קאַנאַדע", "Business", ["ORG-C01382"], "", "I. Y. Weinrat opened a 'Hazomir' concert bureau for Canada (a service business)"),
    ("mexico_choir", "כאָר „הזמיר“ (מעקסיקע)", "מעקסיקע", MUS, ["ORG-C03780"], "3180", "the choir 'Hazomir' (dir. Abel Eisenberg) created by the culture department of the Ashkenazi Kehile in Mexico, after 1957"),
]
TO_EXISTING = [("ORG-C00114_Q12", "2790", "Clara Segalovitch 1913: performances of the drama circle at the local Hazomir, Tomaszów (= db 2790)"),
               ("ORG-C00114_Q01_P06", "1796", "L. Lubinski played with 'Hazomir' (dir. Becher Zwilling) after returning to Lodz")]
SET_PARENT = [("2790", "tomaszow", "drama circle at the Tomaszów Hazomir")]


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


def high_water():
    subprocess.run(["git", "fetch", "origin", "main"], cwd=HERE, capture_output=True, timeout=120)
    out = subprocess.run(["git", "show", "origin/main:Zylbercweig/organizations/core_db.tsv"], cwd=HERE,
                         capture_output=True, text=True, check=True).stdout
    return max(int(r["db_id"]) for r in csv.DictReader(io.StringIO(out), delimiter="\t") if r["db_id"].isdigit())


def main():
    apply = "--apply" in sys.argv
    kf, core, knl = read(HERE / "core_db.tsv")
    af, al, anl = read(HERE / "org_alignment_review.tsv")
    cf, cl, cnl = read(HERE / "organizations_clustered.tsv")
    by = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    linked = {c: r["db_id"] for r in core for c in pipes(r["linked_cluster_ids"])}
    log, made = [], {}

    # split P04 into P04/P05/P06
    tmpl = arow["ORG-C00114_Q01_P04"]
    for x in cl:
        if x["cluster_id"].strip() == "ORG-C00114_Q01_P04":
            hit = [c for h, c in SPLIT.items() if sk(h) in sk(x["_ - heading"])]
            assert len(hit) == 1, x["_ - heading"]
            x["cluster_id"] = hit[0]
    for c in SPLIT.values():
        n = sum(1 for x in cl if x["cluster_id"].strip() == c)
        assert n == 1, (c, n)
        for x in cl:
            if x["cluster_id"].strip() == c:
                x["cluster_size"] = "1"
        if c not in arow:
            a = dict(tmpl); a.update(cluster_id=c, reviewer_notes="", decision="", aligned_db_id="")
            al.insert(al.index(tmpl) + 1, a); arow[c] = a
        arow[c]["cluster_size"] = "1"

    def align(c, d, why):
        a = arow[c]
        if a.get("aligned_db_id") == d and c in pipes(by[d]["linked_cluster_ids"]):
            return
        a["decision"], a["aligned_db_id"], a["org_type"] = ("NEW" if d in made.values() else "ALIGN"), d, by[d]["org_type"]
        p = (a.get("reviewer_notes") or "").strip()
        a["reviewer_notes"] = (p + " | " if p else "") + f"2026-11-15: {why}"
        a["reviewer"], a["reviewed_at"] = "Sinai", NOW
        if c not in pipes(by[d]["linked_cluster_ids"]):
            by[d]["linked_cluster_ids"] = " | ".join(pipes(by[d]["linked_cluster_ids"]) + [c])
        log.append([NOW, "Sinai", "org_review", "alignment", c, a["decision"], why, json.dumps({"aligned_db_id": d, "source": SRC})])

    names = {sk(r["name_yiddish"]): r["db_id"] for r in core if not r["deprecated"].strip()}
    nxt = max(high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    for key, name, addr, typ, cids, parent, why in MINTS:
        for c in cids:
            if c in linked and linked[c] != names.get(sk(name)):
                print(f"ERROR {c} already linked to db {linked[c]}"); return 2
        if sk(name) in names:
            made[key] = names[sk(name)]; print("  exists", key, made[key]); continue
        d = str(nxt); nxt += 1
        pid = made.get(parent, parent)
        row = {f: "" for f in kf}
        row.update(db_id=d, name=name, name_yiddish=name, org_type=typ, address=addr, parent_db_id=pid)
        core.append(row); by[d] = row; made[key] = d
        log.append([NOW, "Sinai", "org_review", "mint", d, "NEW", f"{name} — {why}",
                    json.dumps({"clusters": cids, "org_type": typ, "parent_db_id": pid, "source": SRC}, ensure_ascii=False)])
        print(f"  mint {d} {name} [{typ}] parent={pid or '-'} <- {cids}")
    for key, name, addr, typ, cids, parent, why in MINTS:
        for c in cids:
            align(c, made[key], why)
    for c, d, why in TO_EXISTING:
        align(c, d, why)
    for d, key, why in SET_PARENT:
        if by[d]["parent_db_id"] != made[key]:
            assert not by[d]["parent_db_id"], d
            by[d]["parent_db_id"] = made[key]
            log.append([NOW, "Sinai", "org_review", "set_parent", d, made[key], why, json.dumps({"parent_db_id": made[key], "source": SRC})])
    print(f"{len(log)} log rows")
    if not apply:
        print("DRY RUN"); return 0
    write(HERE / "core_db.tsv", kf, core, knl)
    write(HERE / "org_alignment_review.tsv", af, al, anl)
    write(HERE / "organizations_clustered.tsv", cf, cl, cnl)
    p = HERE / "activity_log.tsv"
    b = io.StringIO(); csv.writer(b, delimiter="\t", lineterminator="\r\n").writerows(log)
    raw = p.read_bytes()
    p.write_bytes(raw + (b"" if raw.endswith(b"\n") else b"\r\n") + b.getvalue().encode("utf-8"))
    print("WROTE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
