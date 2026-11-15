#!/usr/bin/env python3
"""Apply the Theatre education / Amateur decisions of 2026-11-15.

Source of the decisions: Sinai's answers in the Google Doc "Theatre education
and Amateur — categories and audit (2026-11-08)", read 2026-11-15:
  Q1 Rules 1–6 accepted (drama schools -> Theatre education)
  Q2 studio = Theatre education unless no pupils; studio -> theatre = Gimpel two-institution pattern
  Q3 dramatic societies that stage plays -> Amateur, "keep documentation of these specific moves"
  Q4 culture / workers' clubs "can become subtype of amateur"  -> NOT applied here: it contradicts
     Rule 3 ("we never type the host Amateur"); reported back to Sinai, the 19 club/culture-society
     records are untouched
  Q5 ballet school stays open; Mendl Elkin's children's studio (331) -> Theatre education
  Q6 circle as child record: "Do and document (report on each)"

Every verdict below was re-read against its mention sentences (and, for the
city-less host circles, against the entry text in The Lexicon/Structured_*.xml)
in the 2026-11-15 session; records whose reading was not clear-cut, or where a
human decision contradicts a rule, are NOT here — they go to Noa's review Doc.

What it writes (dry-run by default):
  core_db.tsv                 org_type retypes; new child rows; parent_db_id links; cluster links
  org_alignment_review.tsv    child cluster rows (ALIGN), org_type of linked clusters, notes + stamps
  organizations_clustered.tsv cluster_id of the mentions that move to a child (split as <cid>_Pnn)
  org_addresses_review.tsv    org_type + linked clusters of existing cards (run sync_address_cards.py after)
  activity_log.tsv            one row per write (retype_db, mint, alignment, set_parent)

Mentions with a recorded REMOVE in the overlay are never moved (their key includes cluster_id).
Ids are minted from origin's high-water mark at run time. Idempotent.

Usage: python3 apply_decisions_2026_11_15.py [--apply]
"""
from __future__ import annotations

import csv
import io
import json
import re
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

REVIEWER = "Sinai"
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
SRC = "Doc answers 2026-11-15 (theatre_education_amateur_audit/apply_decisions_2026_11_15.py)"
TE, AM = "Theatre education", "Amateur"

# ---------------------------------------------------------------- retypes
# db_id: (new_type, rule, evidence)
RETYPE: dict[str, tuple[str, str, str]] = {}
_TE_RULE1 = ["445", "1161", "1170", "1247", "1261", "1269", "1271", "1272", "1320", "1504", "1505",
             "1569", "1610", "1611", "1623", "1823", "1861", "1885", "2001", "2002", "2003", "2004",
             "2005", "2015", "2024", "2025", "2032", "2033", "2036", "2089", "2106", "2131", "2132",
             "2134", "2135", "2137", "2138", "2142", "2143", "2145", "2146", "2147", "2148", "2149",
             "2151", "2152", "2153", "2154", "2155", "2211", "2218", "3452", "3747", "3787", "3796",
             "3806", "3816", "3821", "3843", "3844", "320", "1167", "1085"]
_TE_RULE2 = ["601", "634", "942", "1105", "1132", "1168", "1177", "1179", "1248", "1250", "1506",
             "1507", "1928", "1977", "2026", "2037", "2144", "2150", "2164", "2188", "2216", "2219",
             "3678", "3729", "3750", "3753", "3774", "3779", "3808", "3828", "3831", "1094", "1121",
             "1122", "1190", "1285", "1502", "251", "358"]
for d in _TE_RULE1:
    RETYPE[d] = (TE, "Rule 1", "drama school / course / institute: training for the stage")
for d in _TE_RULE2:
    RETYPE[d] = (TE, "Rule 2", "studio with pupils or teachers; productions do not change the type")
RETYPE["331"] = (TE, "Q5", "Mendl Elkin's children's theatre studio — Sinai 2026-11-15 accepted the move")

_AM_SOC = {  # Q3: dramatic societies / circles typed Theatre or Society that stage their own plays
    "607": "Chicago Yiddish dramatic society: members join and play roles, productions 1916–1930",
    "512": "Chicago Yiddish dramatic-literary society: staged Asch's play 1928",
    "621": "Milwaukee Yiddish dramatic society founded by Joseph Singer",
    "674": "Kovno Yiddish dramatic society: a one-act play performed by it",
    "1832": "Kovno Yiddish dramatic farein: debuts, Gordin's 'Meturef' under Kadison's direction (1909)",
    "717": "children's liebhaber circle, Ekaterinoslav: roles in Goldfaden plays at age 12",
    "724": "Yiddish dramatic circle founded in Rezhitse 1917",
    "743": "drama circle 'M. Vintshevski', Montevideo 1928",
    "872": "Yiddish dramatic circle founded in Riga 1917 (Surits with the actor Eynes)",
    "1119": "drama circle at the Minsk club 'Royter Boyer' staged a play under M. Shats",
    "1501": "Pirozhnikov's literary-dramatic liebhaber circle, Vilna c. 1908",
    "1788": "Unzer Vinkl, drama circle in Erets-Yisroel (Haifa 1925, members listed)",
    "1921": "drama circle 'Di Bine' / Yiddish dramatic society, Reyshe 1928",
    "4360": "workers' studio at the central culture house, Tiflis: performances, no pupils (Rule 2 exception)",
    "4448": "literary-dramatic 'Kunst-Vinkl' joined 1922",
    "4485": "Silver Dramatic Society: member",
    "306": "Kunst-Ring, the dramatic group at the Yiddish Art Theatre: own productions 1924, 1927",
    "363": "Entin's Progressive Dramatic Club, New York 1902–: members, productions 1906, 1911",
    "2141": "Brisman founded and directed it among his 'dramatic groups', New York 1922",
    "2720": "Stanislav Goldfaden-farein: a play performed by it 1920",
    "2760": "Kovno Yiddish dramatic society: Kadison member and director",
    "2761": "Yiddish dramatic society in Libau: Jacob Sherman a member",
    "3046": "Jacob Gordin dramatic society, Paris: members",
    "3054": "Silver's dramatic club, New York: member",
    "3055": "local dramatic society, Brisk 1918, led by M. Yikhsn",
    "3065": "People's Crown Dramatic Society: co-founded by W. Epstein",
    "3067": "Independent Dramatic Society — the text calls it a dramatic circle",
    "3101": "dramatic society 'Muza' staged An-ski's Dybbuk (dir. David Herman)",
    "3149": "Mohilev dramatic circle: member",
    "3510": "Russian military dramatic circle, Ashkhabad 1910: plays 2–3 times a week",
    "972": "Morris Morrison's dramatic club: member",
    "1745": "Neyer Dramatisher Shtern — one of Brisman's 'dramatic groups', New York 1922",
    "2252": "Dramatishe Kunst, Lodz amateur society (the SPLIT from the London group is left to Noa)",
}
for d, ev in _AM_SOC.items():
    RETYPE[d] = (AM, "Rule 4 / Q3" if d not in ("4360",) else "Rule 2", ev)
RETYPE.update({
    "2278": ("Company on Tour", "Rule 5", "Lemberg Chaverim troupe: ליבהאָבער-ראָלן = lover roles; professional troupe (dir. Gimpel)"),
    "384": ("Traveling Company", "Rule 5", "Shlifershteyn's troupe toured as professionals"),
    "2298": ("Traveling Company", "Rule 5", "the Mohilev troupe: a professional company"),
    "2009": ("Musical organization", "Rule 1", "a students' orchestra, not drama training"),
    "1235": ("Education", "Rule 1", "Kultur-Lige art studio, Kiev: fine-arts training"),
    "1028": ("Non-Yiddish Theatre", "audit", "the local Russian drama theatre: professional performances"),
    "206": ("Education", "policy", "Camp Kinderland: camps are Education (CLASSIFICATION_POLICY type 15)"),
    "2305": ("Trade Union / Professional Association", "Rule 3", "the Pressers' Local of the International is a union; its drama circle becomes a child"),
})

# ---------------------------------------------------------------- moves (Rule 3)
# (host_db, heading_contains, sentence_contains)  ->  target
CHILDREN = [  # new child records: (key, host, name_yiddish, address, [(heading, sentence)], evidence)
    ("lassal", "3773", "דראַמקרייז ביים בילדונגס-פֿאַראיין „פערדינאַנד לאַסאַל“ (ווין)", "ווין",
     [("מייזעלס", "דראָמקרייז ביים יידישן בילדונגס-פֿאַראיין")],
     "Klara Meisels, Vienna 1912: 'ווירקט זי מיט אין דראָמקרייז ביים יידישן בילדונגס-פֿאַראיין \"פערדינאַנד לאַסאַל\"'"),
    ("kunmz", "3805", "דראַמאַטישער קרייז אין מאָסקווער מערבֿ-אוניווערסיטעט", "מאָסקווע",
     [("גאָלדבלאַט", "דראַמאַטישן קרייז אין מאָסקווער")],
     "Moyshe Goldblat 'אָנגעפירט אַ דראַמאַטישן קרייז אין מאָסקווער מערבאוניווערסיטעט'"),
    ("bund_riga", "997", "דראָמאָטישע קרייזן ביים „בונד“ (ריגע)", "ריגע",
     [("וואָרקעל", "דראָמאָטישע קרייזן ביים")],
     "M. Vorkel, 1900: 'פירט ער אָן אין ריגע מיט די דראָמאָטישע קרייזן ביים \"בונד\"'"),
    ("tseire_yehuda", "2435", "דראַמאַטישער קרייז ביים „צעירי יהודה“ (לעמבערג)", "לעמבערג",
     [("אלוני", "דראַמאַטישן קרייז ביים")],
     "Sam Aloni, Lemberg 1919: 'שפּילן קלענערע ראָלן אין דראַמאַטישן קרייז ביים ציוניסטישן פֿאַראיין \"צעירי יהודה\"'"),
    ("tseire_tsiyon", "2436", "דראַמקרייז ביי „צעירי ציון“ (וואַרשע)", "וואַרשע",
     [("פּאַטראָן", "דראַמקרייזן ביי")],
     "Elias Patron (raised in Warsaw, left 1925): 'געשפּילט אין דראַמקרייזן ביי \"צעירי ציון\", \"התאַחדות\"' — city from the entry"),
    ("hitachdut", "4602", "דראַמקרייז ביי „התאַחדות“ (וואַרשע)", "וואַרשע",
     [("פּאַטראָן", "דראַמקרייזן ביי")],
     "same sentence as the Tseire-Tsiyon circle (Patron, Warsaw before 1925)"),
    ("tsukunft", "2446", "דראַמקרייז ביי דער יוגנט-אָרגאַניזאַציע „צוקונפֿט“ (וואַרשע)", "וואַרשע",
     [("ליפּינסקי", "דראַמקרייז ביי דער יוגנט")],
     "Anka Lipinski, Warsaw, before 1922: 'מיטגעשפּילט אין דראַמקרייז ביי דער יוגנט-אָרגאַניזאַציע צוקונפט' — city from the entry"),
    ("kom_tsukunft", "2496", "דראַמסעקציע ביי דער „קאָמ. צוקונפֿט“", "",
     [("פּאָזנאַנסקי", "דראַמסעקציע ביי דער")],
     "Hersh Poznanski 'געשאַפן, צוזאַמען מיט איציק קאָוונער … אַ דראַמסעקציע ביי דער \"קאָמ. צוקונפט\"' — no city in the entry"),
    ("gordonia_riga", "2573", "דראַמאַטישע סעקציע ביים ריגער „גורדוניה“ (ביאַליק-קלוב)", "ריגע",
     [("סמאַרגאַנסקי", "דראַמאַטישער סעקציע ביים")],
     "F. Smarganski 1926–27: 'אָנגעפֿירט מיט דער דראַמאַטישער סעקציע ביים ריגער גורדוניה\" (ביאָליק קלוב)'"),
    ("verein_zion", "3229", "דראַמאַטישער קלוב פֿון פֿאַריין „ציון“ (ווין)", "ווין",
     [("ווייסבערג", "ליבהאָבער"), ("בלייך", "דראָמאָטישן קלוב")],
     "Y. Bleich, Vienna 1917: 'טרעט ב. אַריין אין דראָמאָטישן קלוב פֿון פֿאַריין \"ציון\"' (dir. Mestel); Rachel Weisberg, Vienna: its liebhaber performances"),
    ("ekaterinoslav", "720", "„ליבהאָבער“-קאָלעקטיוו ביים יעקאַטערינאָסלאַווער פֿאַראיין פֿון „סצענישע קונסט-טוער“", "יעקאַטערינאָסלאַוו",
     [("מערענזאָן", "קאָלעקטיוו פֿון")],
     "Arn Merenzon 1917: 'זיך באַטייליקט אין קאָלעקטיוו פֿון \"ליבהאָבער\" ביים יעקאַטערינאָסלאַווער פֿאַראיין'"),
    ("pressers", "2305", "דראַמאַטישער קרייז ביים פּרעסערס לאָקאָל (ניו יאָרק)", "ניו יאָרק",
     [("שניאור", "אָנגאַזשירט")],
     "Chaim Shneyer, late 1930s: the Pressers' Local 'אַראַנזשירט אַ דראַמאַטישן קרייז אין דער יוניע' and engaged him as director (entry text)"),
    ("hazomir_lodz_circle", "1796", "דראַמאַטישער קרייז ביים לאָדזשער „הזמיר“", "לאָדזש",
     [("קאַצענעלסאָן", "דראַמאַטישער קרייז")],
     "Yitskhok Katzenelson 'פֿירט דורך, אַז ביים אָרטיקן געזאַנג-פֿאַריין \"הזמיר\" זאָל זיך גרינדן אַ דראַמאַטישער קרייז' (Lodz)"),
]
TO_EXISTING = [  # (host, target existing db, [(heading, sentence)], evidence)
    ("1872", "913", [("לייפּציקער", "דראַמקרייז ביים"), ("טרייסטמאַן", "דראַמאַטישן קרייז")],
     "Warsaw Hazomir's drama section already exists as db 913 (Estrin, Bellerina, Godik — Warsaw)"),
    ("1796", "797", [("מאָשקאָוויטש", "טעאַטער-סטודיאָ"), ("מענדעלע", "טעאַטער-סטודיע ביים")],
     "the theatre studio at Lodz Hazomir already exists as db 797 (1932 production)"),
]
SET_PARENT = {  # existing unit record -> host (Rule 3)
    "913": ("1872", "drama section at Warsaw Hazomir"),
    "917": ("1872", "'Artistish Vinkele' at Warsaw Hazomir (Leipziger, Tarlo — Warsaw)"),
    "800": ("1872", "David Herman's studio at Hazomir, Warsaw (Norwid)"),
    "797": ("1796", "theatre studio at Lodz Hazomir"),
    "2286": ("1925", "drama section at Hazomir, Sosnowiec 1908–09"),
    "2283": ("997", "literary-dramatic circle at the Bund, Vilna 1908"),
    "801": ("997", "dramatic studio 'Baginen' at the Bund, Lodz"),
    "1179": ("1233", "Kiev drama studio at the Kultur-Lige"),
    "1235": ("1233", "Kiev art studio at the Kultur-Lige"),
}

HEAD, SENT = "_ - heading", "_ - organizations - _ - relations - _ - original_sentence"


def sk(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))


def read(p: Path):
    raw = p.read_bytes().decode("utf-8")
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rd = csv.DictReader(io.StringIO(raw, newline=""), delimiter="\t")
    return list(rd.fieldnames), list(rd), nl


def write(p: Path, fields, rows, nl):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, delimiter="\t", lineterminator=nl, quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    w.writerows(rows)
    p.write_bytes(buf.getvalue().encode("utf-8"))


def pipes(v: str) -> list[str]:
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def origin_high_water() -> int:
    subprocess.run(["git", "fetch", "origin", "main"], cwd=ORG, capture_output=True, timeout=120)
    out = subprocess.run(["git", "show", "origin/main:Zylbercweig/organizations/core_db.tsv"],
                         cwd=ORG, capture_output=True, text=True, timeout=120, check=True).stdout
    return max(int(r["db_id"]) for r in csv.DictReader(io.StringIO(out), delimiter="\t") if r["db_id"].isdigit())


def main() -> int:
    apply = "--apply" in sys.argv
    kf, core, knl = read(ORG / "core_db.tsv")
    af, al, anl = read(ORG / "org_alignment_review.tsv")
    cf, cl, cnl = read(ORG / "organizations_clustered.tsv")
    df, cards, dnl = read(ORG / "org_addresses_review.tsv")
    byid = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    card = {r["db_id"]: r for r in cards}
    removed = mention_removals.load_removed_keys()
    log: list[list[str]] = []
    report: list[dict] = []
    errors = 0

    def note(row, text):
        prev = (row.get("reviewer_notes") or "").strip()
        row["reviewer_notes"] = (prev + " | " if prev else "") + text
        row["reviewer"], row["reviewed_at"] = REVIEWER, NOW

    def L(view, action, target, decision, text, extra):
        log.append([NOW, REVIEWER, view, action, target, decision, text,
                    json.dumps(extra, ensure_ascii=False)])

    # ---------------- retypes
    for d, (typ, rule, ev) in RETYPE.items():
        r = byid.get(d)
        if r is None or r["deprecated"].strip() or r["merged_into"].strip():
            print(f"ERROR retype {d}: missing or retired"); errors += 1; continue
        was = r["org_type"]
        if was == typ:
            continue
        r["org_type"] = typ
        txt = f"2026-11-15 {rule}: {was} → {typ} — {ev}"
        for c in pipes(r["linked_cluster_ids"]):
            if c in arow and arow[c]["org_type"] != typ:
                arow[c]["org_type"] = typ
                note(arow[c], txt)
        if d in card and card[d]["org_type"] != typ:
            card[d]["org_type"] = typ
        L("org_review", "retype_db", d, typ, txt, {"was": was, "rule": rule, "source": SRC})
        report.append({"kind": "retype", "db_id": d, "name": r["name_yiddish"] or r["name"], "was": was,
                       "now": typ, "rule": rule, "evidence": ev})

    # ---------------- mention selection
    def select(host: str, specs):
        cids = set(pipes(byid[host]["linked_cluster_ids"]))
        rows = [x for x in cl if x["cluster_id"].strip() in cids]
        hit = []
        for h, s in specs:
            m = [x for x in rows if sk(h) in sk(x[HEAD]) and sk(s) in sk(x[SENT])]
            if not m:
                return None, f"no mention of host {host} under «{h}» with «{s}»"
            hit += m
        keep = [x for x in hit if mention_removals.mention_key(x) not in removed]
        return keep, ""

    next_id = max(origin_high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    used_cids = {x["cluster_id"].strip() for x in cl} | set(arow)

    def move(rows, target: str, why: str):
        """Move clustered rows to db `target`: whole clusters relink, partial ones split as _Pnn."""
        by_c = defaultdict(list)
        for x in rows:
            by_c[x["cluster_id"].strip()].append(x)
        moved = []
        for c, mine in by_c.items():
            allrows = [x for x in cl if x["cluster_id"].strip() == c]
            owner = next((r for r in core if c in pipes(r["linked_cluster_ids"])), None)
            if len({id(x) for x in mine}) == len(allrows):
                new_c = c  # whole cluster relinks
                if owner is not None and owner["db_id"] != target:
                    owner["linked_cluster_ids"] = " | ".join(x for x in pipes(owner["linked_cluster_ids"]) if x != c)
                    oc = card.get(owner["db_id"])
                    if oc:
                        oc["linked_cluster_ids"] = " | ".join(x for x in pipes(oc["linked_cluster_ids"]) if x != c)
                a = arow[c]
            else:
                n = 2
                while f"{c}_P{n:02d}" in used_cids:
                    n += 1
                new_c = f"{c}_P{n:02d}"
                used_cids.add(new_c)
                ids = {id(x) for x in mine}
                for x in allrows:
                    if id(x) in ids:
                        x["cluster_id"] = new_c
                left = len(allrows) - len(mine)
                for x in allrows:
                    x["cluster_size"] = str(len(mine) if id(x) in ids else left)
                parent = arow[c]
                a = dict(parent)
                a["cluster_id"], a["cluster_size"] = new_c, str(len(mine))
                a["reviewer_notes"] = ""
                al.insert(al.index(parent) + 1, a)
                arow[new_c] = a
                parent["cluster_size"] = str(left)
                note(parent, f"2026-11-15 Rule 3: {len(mine)} drama-unit mention(s) split off to {new_c} → db {target}")
            t = byid[target]
            if new_c not in pipes(t["linked_cluster_ids"]):
                t["linked_cluster_ids"] = " | ".join(pipes(t["linked_cluster_ids"]) + [new_c])
                tc = card.get(target)
                if tc is not None:
                    tc["linked_cluster_ids"] = " | ".join(pipes(tc["linked_cluster_ids"]) + [new_c])
            a["decision"], a["aligned_db_id"], a["org_type"] = "ALIGN", target, t["org_type"]
            note(a, f"2026-11-15 Rule 3: {why}")
            L("org_review", "alignment", new_c, "ALIGN", f"Rule 3: {why}",
              {"aligned_db_id": target, "from_cluster": c, "n_mentions": len(mine), "source": SRC})
            moved.append((c, new_c, len(mine), [(x[HEAD], x[SENT]) for x in mine]))
        return moved

    # ---------------- new child records
    have = {(r["parent_db_id"], sk(r["name_yiddish"])) : r["db_id"] for r in core}
    for key, host, name, addr, specs, ev in CHILDREN:
        if (host, sk(name)) in have:
            print(f"  skip {key}: child already db {have[(host, sk(name))]}")
            continue
        rows, err = select(host, specs)
        if rows is None:
            print("ERROR", err); errors += 1; continue
        if not rows:
            print(f"  skip {key}: every matching mention is removed"); continue
        d = str(next_id); next_id += 1
        row = {f: "" for f in kf}
        row.update(db_id=d, name=name, name_yiddish=name, org_type=AM, address=addr, parent_db_id=host)
        core.append(row); byid[d] = row
        moved = move(rows, d, f"child drama unit of db {host} ({byid[host]['name_yiddish'] or byid[host]['name']}); {ev}")
        L("org_review", "mint", d, "NEW", f"Rule 3 child of db {host}: {name}",
          {"parent_db_id": host, "org_type": AM, "clusters": [m[1] for m in moved], "source": SRC})
        report.append({"kind": "child", "db_id": d, "name": name, "host": host,
                       "host_name": byid[host]["name_yiddish"] or byid[host]["name"], "host_type": byid[host]["org_type"],
                       "moved": moved, "evidence": ev})
        print(f"  mint db {d} {name}  <- " + ", ".join(f"{m[0]}→{m[1]} ({m[2]})" for m in moved))

    for host, target, specs, ev in TO_EXISTING:
        rows, err = select(host, specs)
        if rows is None:
            print("  skip/ERR", err, "(already moved?)" if target else ""); continue
        moved = move(rows, target, f"moved from host db {host} to existing unit db {target}; {ev}")
        report.append({"kind": "to_existing", "db_id": target, "name": byid[target]["name_yiddish"], "host": host,
                       "host_name": byid[host]["name_yiddish"] or byid[host]["name"], "moved": moved, "evidence": ev})
        print(f"  move {host} -> {target}: " + ", ".join(f"{m[0]}→{m[1]} ({m[2]})" for m in moved))

    for d, (host, ev) in SET_PARENT.items():
        r = byid[d]
        if r["parent_db_id"].strip() == host:
            continue
        if r["parent_db_id"].strip():
            print(f"ERROR parent {d}: already has parent {r['parent_db_id']}"); errors += 1; continue
        r["parent_db_id"] = host
        if d in card:
            card[d]["parent_db_id"] = host
        L("org_review", "set_parent", d, host, f"Rule 3: {ev}", {"parent_db_id": host, "source": SRC})
        report.append({"kind": "parent", "db_id": d, "name": r["name_yiddish"] or r["name"], "host": host,
                       "host_name": byid[host]["name_yiddish"] or byid[host]["name"], "evidence": ev})

    print(f"\n{sum(1 for x in report if x['kind']=='retype')} retypes, "
          f"{sum(1 for x in report if x['kind']=='child')} child mints, "
          f"{sum(1 for x in report if x['kind']=='to_existing')} moves to existing units, "
          f"{sum(1 for x in report if x['kind']=='parent')} parent links; {len(log)} log rows; {errors} errors")
    (HERE / "applied_2026_11_15.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    if errors:
        print("errors: nothing written"); return 2
    if not apply:
        print("DRY RUN — re-run with --apply"); return 0
    write(ORG / "core_db.tsv", kf, core, knl)
    write(ORG / "org_alignment_review.tsv", af, al, anl)
    write(ORG / "organizations_clustered.tsv", cf, cl, cnl)
    write(ORG / "org_addresses_review.tsv", df, cards, dnl)
    p = ORG / "activity_log.tsv"
    b = p.read_bytes()
    buf = io.StringIO()
    w = csv.writer(buf, delimiter="\t", lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
    w.writerows(log)
    p.write_bytes(b + (b"" if b.endswith(b"\n") else b"\r\n") + buf.getvalue().encode("utf-8"))
    print("WROTE core_db, org_alignment_review, organizations_clustered, org_addresses_review, activity_log")
    return 0


if __name__ == "__main__":
    sys.exit(main())
