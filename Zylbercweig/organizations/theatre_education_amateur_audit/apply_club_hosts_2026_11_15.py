#!/usr/bin/env python3
"""Hosting culture / workers' clubs (Sinai 2026-11-15).

Sinai: "club" as an Amateur subtype meant the American dramatic clubs, NOT the hosting
clubs; the hosts get a new canonical type "Cultural/workers' club" (added to
CLASSIFICATION_POLICY type 33, both app dropdowns, the cascade and the LLM prompt), and
their drama circles are minted as Amateur children (Rule 3), now.

Each of the 20 PENDING_SINAI records of decisions_2026_11_15.tsv is retyped; a child is
minted only where the sentence names a unit (circle, section, collective, studio) at the
club — otherwise the mention stays on the host (Rule 3 needs a named unit).
Same mechanics as apply_decisions_2026_11_15.py (split as <cid>_Pnn, ids from origin
high-water, every write logged). Dry-run by default. Run ../sync_address_cards.py after.
"""
import csv
import io
import json
import subprocess
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(10 ** 9)
HERE = Path(__file__).resolve().parent
ORG = HERE.parent
sys.path.insert(0, str(ORG.parent / "zalmen"))
import mention_removals  # noqa: E402

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
SRC = "Sinai 2026-11-15: hosting clubs (apply_club_hosts_2026_11_15.py)"
CLUB, AM, TE = "Cultural/workers' club", "Amateur", "Theatre education"
HEAD, SENT = "_ - heading", "_ - organizations - _ - relations - _ - original_sentence"

# host db: (child name or None, child type, [(heading, sentence)], evidence / reason)
HOSTS = {
    "914": (None, "", [], "An-ski culture club, New York: Brisman 'leads its dramatic work' — no unit named, mention stays"),
    "2284": ("דראַמאַטישע סעקציע ביים „לאָס אַנדזשעלעס יידיש קלאָבּ“", AM, [("טענענהאָלץ", "דראַמאַטישע סעקציע")],
             "Tenenholtz 'אָרגאַניזירט בּיים „לאָס אַנדזשעלאָס יידיש קלאָבּ\" אַ דראַמאַטישע סעקציע'"),
    "915": (None, "", [], "Zhitomir Jewish workers' club: Tombak debuted 1923 'in' it — no unit named, mention stays"),
    "918": (None, "", [], "a Kharkov Jewish workers' club: Strizhevski 'also as director in' it — no unit named, mention stays"),
    "4434": ("יידישער דראַמאַטישער קאָלעקטיוו ביים קולטור-פּאַלאַץ פֿון די פּראָפֿעסיאָנעלע פֿאַריינען (קאָוונע)", AM,
             [("בעלצער", "דראַמאַטישן קאָלעקטיוו")],
             "Beltser organised and led from 1960 the Yiddish dramatic collective at the culture palace of the trade unions, Kovno"),
    "2730": ("דראַמקרייז ביים אַרבעטער-קלוב פֿון קוסטאַרן (זשיטאָמיר)", AM, [("מערענזאָן", "דראַמקרייז")],
             "Merenzon 1924, Zhitomir: 'אָנפֿירער פֿון אַ דראַמקרייז ביים אַרבעטער-קלוב פֿון קוסטאַרן'"),
    "1134": ("דראָמקרייזן אין די מינסקער אַרבעטער-קלובן", AM, [("דושמאַן", "דראָמקרייזן")],
             "Dushman 1923–25 'אָנפֿירער פֿון דראָמקרייזן' in the Minsk workers' clubs"),
    "2588": ("אַרבעטער טעאַטער-סטודיע ביים אַרבעטער-קלוב „ספּאַרטאַק“ (בוענאָס איירעס)", TE, [("נאַאומאָוו", "טעאַטער-סטודיע")],
             "Naumov 'געגרינדעט און אָנגעפֿירט מיט דער אַרבעטער טעאַטער-סטודיע ביים אַרבעטער-קלוב \"ספּאַרטאַק\"' — a studio, Theatre education by default (Rule 2)"),
    "2642": ("דראָמקרייז ביים יידישן אַרבעטער-קלוב „דער דריטער אינטערנאַציאָנאַל“", AM, [("זאַלצמאַן", "דראָמקרייז")],
             "Nekhome Zaltsman 'אַן אַקטיוון אָנטייל אין דראָמקרייז ביים יידישן אַרבעטער-קלוב'"),
    "2710": ("דראַמקרייזן אין קלוב פֿון די קוסטאַרן", AM, [("טאָמבאק", "דראַמקרייזן")],
             "Tombak 'אָנגעפֿירט מיט דראַמקרייזן אין קלוב פֿון די קוסטארן' (probably the same club as 2730)"),
    "3490": ("דראַמקרייזן אין קלוב פֿון די מעטאַליסטן א\"נ טאָמסקי", AM, [("טאָמבאק", "דראַמקרייזן")],
             "Tombak 'אָנגעפֿירט מיט דראַמקרייזן … אין קלוב פון די מעטאַליסטן א\"נ טאָמסקי'"),
    "4606": ("דראַמקרייז ביים ראַדיקאַלן אַרבעטער-סענטער", AM, [("גלאַנץ", "דראַמקרייז")],
             "Glants: a play 'געשפּילט געוואָרן דורך דעם דראַמקרייז בעם ראַדיקאַלן אַרבעטער-סענטער'"),
    "2140": ("אַמאַטאָרן-קרייז ביים פֿאַריין „בילדונג“ (קראָקע)", AM, [("געבירטיג", "אַמאַטאָרן-קרייז")],
             "Gebirtig, Kraków: in the workers' association 'Bildung' 'אָרגאַניזירט אַן אַמאַטאָרן-קרייז'"),
    "2713": ("דראַמאַטישע סעקציע ביים קאָלאָמעער קולטור-פֿאַראיין", AM, [("יאַבלאָנסקי", "דראַמאַטישע סעקציע")],
             "Yablonski 'אָרגאַניזירט אין קאָלאָמעער קולטור-פֿאַראיין אַ דראַמאַטישע סעקציע'"),
    "2277": ("דראַמאַטישע קרייזן ביי די „נייע יידיש אַמעריקאַנער“ (פֿילאַדעלפֿיע)", AM, [("פֿורמאַן", "דראַמאַטישע")],
             "Ari Furman 'אָרגאַניזירט צוויי דראַמאַטישע קרייזן ביי די \"נייע-יידיש אַמעריקאַנער\"', Philadelphia; Chayele Ash's 1962 production with the organisation stays on the host"),
    "732": ("„ליבהאָבער“ פֿון פּרץ-פֿאַראיין (דאַנציג)", AM, [("מינץ", "ליבהאָבער")],
            "Eliyahu Mints, Danzig 1924: 'שפּילנדיק … מיט די „ליבהאָבער\" פֿון אַרטיקן פּרץ-פֿאַראיין'"),
    "2292": (None, "", [], "Forverts club, London 1902 (Abrams, Cohen) — no theatre activity in the text"),
    "4620": (None, "", [], "Libau literary club 'Kadima' — co-founder only; no theatre activity in the text"),
    "4683": (None, "", [], "'Folkshoyz', Luvitsh — co-founder only; no theatre activity in the text"),
    "4631": ("דראַמאַטישער קרייז ביי דער געזעלשאַפֿט „האַרפע“ (פּאַריז)", AM, [("אייזענשטאַט", "דראַמאַטישן קרייז")],
             "Mordkhe Eyzenshtat, Paris 1923: one-acts 'מיט דעם דראַמאַטישן קרייז ביי דער געז. האַרפע' (Harfe may be a song society — then Musical organization)"),
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


def high_water():
    subprocess.run(["git", "fetch", "origin", "main"], cwd=ORG, capture_output=True, timeout=120)
    out = subprocess.run(["git", "show", "origin/main:Zylbercweig/organizations/core_db.tsv"], cwd=ORG,
                         capture_output=True, text=True, check=True).stdout
    return max(int(r["db_id"]) for r in csv.DictReader(io.StringIO(out), delimiter="\t") if r["db_id"].isdigit())


def main():
    apply = "--apply" in sys.argv
    kf, core, knl = read(ORG / "core_db.tsv")
    af, al, anl = read(ORG / "org_alignment_review.tsv")
    cf, cl, cnl = read(ORG / "organizations_clustered.tsv")
    df, cards, dnl = read(ORG / "org_addresses_review.tsv")
    by = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    card = {r["db_id"]: r for r in cards}
    removed = mention_removals.load_removed_keys()
    used = {x["cluster_id"].strip() for x in cl} | set(arow)
    log, report = [], []

    def note(a, t):
        p = (a.get("reviewer_notes") or "").strip()
        a["reviewer_notes"] = (p + " | " if p else "") + t
        a["reviewer"], a["reviewed_at"] = "Sinai", NOW

    def move(rows, target, why):
        moved = []
        byc = defaultdict(list)
        for x in rows:
            byc[x["cluster_id"].strip()].append(x)
        for c, mine in byc.items():
            allr = [x for x in cl if x["cluster_id"].strip() == c]
            owner = next(r for r in core if c in pipes(r["linked_cluster_ids"]))
            if len(mine) == len(allr):
                new = c
                owner["linked_cluster_ids"] = " | ".join(x for x in pipes(owner["linked_cluster_ids"]) if x != c)
                if owner["db_id"] in card:
                    oc = card[owner["db_id"]]
                    oc["linked_cluster_ids"] = " | ".join(x for x in pipes(oc["linked_cluster_ids"]) if x != c)
                a = arow[c]
            else:
                n = 2
                while f"{c}_P{n:02d}" in used:
                    n += 1
                new = f"{c}_P{n:02d}"; used.add(new)
                ids = {id(x) for x in mine}
                for x in allr:
                    if id(x) in ids:
                        x["cluster_id"] = new
                    x["cluster_size"] = str(len(mine) if id(x) in ids else len(allr) - len(mine))
                parent = arow[c]
                a = dict(parent); a.update(cluster_id=new, cluster_size=str(len(mine)), reviewer_notes="")
                al.insert(al.index(parent) + 1, a); arow[new] = a
                parent["cluster_size"] = str(len(allr) - len(mine))
                note(parent, f"2026-11-15 Rule 3: drama-unit mention(s) split off to {new} → db {target}")
            t = by[target]
            t["linked_cluster_ids"] = " | ".join(pipes(t["linked_cluster_ids"]) + [new])
            a["decision"], a["aligned_db_id"], a["org_type"] = "ALIGN", target, t["org_type"]
            note(a, f"2026-11-15 Rule 3: {why}")
            log.append([NOW, "Sinai", "org_review", "alignment", new, "ALIGN", f"Rule 3: {why}",
                        json.dumps({"aligned_db_id": target, "from_cluster": c, "source": SRC}, ensure_ascii=False)])
            moved.append((c, new, len(mine), [(x[HEAD], x[SENT]) for x in mine]))
        return moved

    have = {(r["parent_db_id"], sk(r["name_yiddish"])): r["db_id"] for r in core}
    nxt = max(high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    for host, (cname, ctype, specs, why) in HOSTS.items():
        h = by[host]
        was = h["org_type"]
        if was != CLUB:
            h["org_type"] = CLUB
            for c in pipes(h["linked_cluster_ids"]):
                if c in arow:
                    arow[c]["org_type"] = CLUB
                    note(arow[c], f"2026-11-15: {was} → {CLUB} (hosting club; Sinai)")
            if host in card:
                card[host]["org_type"] = CLUB
            log.append([NOW, "Sinai", "org_review", "retype_db", host, CLUB, f"hosting club (Sinai 2026-11-15): {was} → {CLUB} — {why}",
                        json.dumps({"was": was, "source": SRC}, ensure_ascii=False)])
        entry = {"host": host, "host_name": h["name_yiddish"] or h["name"], "was": was, "why": why}
        if cname and (host, sk(cname)) not in have:
            cids = set(pipes(h["linked_cluster_ids"]))
            rows = []
            for hd, s in specs:
                m = [x for x in cl if x["cluster_id"].strip() in cids and sk(hd) in sk(x[HEAD]) and sk(s) in sk(x[SENT])]
                if not m:
                    print(f"ERROR {host}: no mention «{hd}» / «{s}»"); return 2
                rows += [x for x in m if mention_removals.mention_key(x) not in removed]
            d = str(nxt); nxt += 1
            row = {f: "" for f in kf}
            row.update(db_id=d, name=cname, name_yiddish=cname, org_type=ctype, parent_db_id=host)
            core.append(row); by[d] = row
            moved = move(rows, d, f"child unit of db {host} ({entry['host_name']}); {why}")
            log.append([NOW, "Sinai", "org_review", "mint", d, "NEW", f"Rule 3 child of hosting club db {host}: {cname}",
                        json.dumps({"parent_db_id": host, "org_type": ctype, "clusters": [m[1] for m in moved], "source": SRC}, ensure_ascii=False)])
            entry.update(child=d, child_name=cname, child_type=ctype, moved=[(m[0], m[1], m[2]) for m in moved])
            print(f"  {host} {was} → club; mint {d} [{ctype}] {cname} <- " + ", ".join(f"{m[0]}→{m[1]}({m[2]})" for m in moved))
        else:
            print(f"  {host} {was} → club; no child")
        report.append(entry)
    (HERE / "applied_club_hosts_2026_11_15.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    print(f"{len(log)} log rows")
    if not apply:
        print("DRY RUN"); return 0
    write(ORG / "core_db.tsv", kf, core, knl)
    write(ORG / "org_alignment_review.tsv", af, al, anl)
    write(ORG / "organizations_clustered.tsv", cf, cl, cnl)
    write(ORG / "org_addresses_review.tsv", df, cards, dnl)
    p = ORG / "activity_log.tsv"
    b = io.StringIO(); csv.writer(b, delimiter="\t", lineterminator="\r\n").writerows(log)
    raw = p.read_bytes()
    p.write_bytes(raw + (b"" if raw.endswith(b"\n") else b"\r\n") + b.getvalue().encode("utf-8"))
    print("WROTE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
