#!/usr/bin/env python3
"""Education close-out 2026-11-22: Q2 (same name minted and generic), the Berlin
conservatory, and the 10-04 unmint that a stale app save undid.

Decisions approved by Sinai 2026-11-22 ("yes, go ahead") on the table built in
that session from the source passages of every row (organizations_clustered.tsv
+ people/entry_texts.tsv). The ledger is this file: every change below names the
passage words that decide it.

  A  Re-apply Sinai's 2026-10-04 unmint (943cecb95). Commit 020c0d4ac ("chore:
     save core DB", 19:06, four minutes later) wrote core_db back from a stale app
     checkout: all 17 deprecated rows came back live and 26 freed alignments
     reverted with their old stamps. Re-applied, except three clusters whose OWN
     name individuates (quoted title / ordinal), which keep their db row.
  T  Sinai 2026-11-22: Realschule, commercial, trade and city schools follow the
     gymnasium rule -- named only by their town, they are a kind, not one school.
  B  Lumped clusters: mentions from other towns split off a city-specific org.
  C/D/E  Rows the text individuates (rare kind + city, founder, owner + address,
     quoted title) minted or aligned; rare kinds swept GENERIC by rule_2026_10_04
     reopened per the rule itself.
  L  Live core_db links to rows the review file marks GENERIC are cleared.

Writes (dry-run by default): core_db.tsv, org_alignment_review.tsv,
organizations_clustered.tsv (partial moves become <cid>_Pnn), activity_log.tsv.
Mentions with a recorded REMOVE are never moved. New ids are minted from
origin's high-water mark at run time. Idempotent: a second --apply changes nothing.

Usage: python3 apply_education_q2_2026_11_22.py [--apply]
Run sync_address_cards.py afterwards.
"""
from __future__ import annotations

import csv
import io
import json
import subprocess
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

csv.field_size_limit(10 ** 9)
ORG = Path(__file__).resolve().parent
sys.path.insert(0, str(ORG.parent / "zalmen"))
import mention_removals  # noqa: E402

REVIEWER = "Sinai"
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
TAG = "Education close-out 2026-11-22"
SRC = "apply_education_q2_2026_11_22.py"
HEAD, SENT = "_ - heading", "_ - organizations - _ - relations - _ - original_sentence"

UNMINT_NOTE = ("re-applied Sinai's 2026-10-04 unmint (943cecb95), lost to the stale app save "
               "020c0d4ac: a gymnasium/yeshiva named only by its town is a kind, not one school")
TIER_NOTE = ("Sinai 2026-11-22: Realschule / commercial / trade / city schools follow the gymnasium "
             "rule -- named only by the town, a kind, not one school")

# ------------------------------------------------------------------ mints
MINTS = {
    "czernowitz": ("Conservatory, Czernowitz", "טשערנאָוויצער קאָנסערוואַטאָריע", "טשערנאָוויץ", ""),
    "munich_cons": ("Conservatory, Munich", "מינכענער קאָנסערוואַטאָריע", "מינכען", ""),
    "iasi_cons": ("Conservatory, Iași", "יאַסער קאָנסערוואַטאָריע", "יאָסי", ""),
    "cincinnati_cons": ("Conservatory, Cincinnati", "סינסינעטיער קאָנסערוואַטאָריע", "סינסינעטי", ""),
    "antwerp_acad": ("Academy of Fine Arts, Antwerp", "קונסט-אַקאַדעמיע (אַנטווערפּן)", "אַנטווערפן", ""),
    "abramson_cons": ("Conservatory founded by Ivan Abramson and Herman Kaminsky, New York",
                      "קאָנסערוואַטאָריע פֿון אַבראַמסאָן און קאַמינסקי", "ניו יאָרק", ""),
    "irkutsk_ped": ("Pedagogical institute, Irkutsk region", "פּעדאַגאָגישער אינסטיטוט (אירקוטסק)", "אירקוטסק", ""),
    "medvedev_cons": ("Yakov Medvedev's conservatory, Kiev", "מעדוועדיעווס קאָנסערוואַטאָריום", "קיעוו", ""),
    "odessa_handverker": ("Craftsmen's school, Odessa", "האַנטווערקער-שול (אָדעס)", "אָדעס", ""),
    "berlin_hochschule": ("Königliche Hochschule für Musik, Berlin (founded by Joseph Joachim)",
                          "בערלינער קאָנסערוואַטאָריע פֿון יאָאַכים", "בערלין", ""),
    "weissfeld_school": ("Moshe Weissfeld's commercial school, Warsaw (corner Dzika / Wołyńska)",
                         "ווייספעלדס האַנדלס-שול", "וואַרשע, עק דזשיקאַ און וואָלינסקאַ", ""),
    "ar_camp_boston": ("Workmen's Circle Camp, Boston", "אַרבעטער רינג קעמפּ (באָסטאָן)", "באָסטאָן", "189"),
    "hirsch_vienna": ("Baron Hirsch school, Vienna", "באַראַן הירש-שול (ווין)", "ווין", ""),
    "hirsch_ny": ("Baron Hirsch school, East Broadway, New York", "באַראַן הירש-שול (איסט בראָדוויי)",
                  "ניו יאָרק, איסט בראָדוויי", ""),
    "goldfaden_school": ("Abraham Goldfaden's dramatic school", "גאָלדפֿאַדענס דראַמאַטישע שול", "", ""),
    "korolenko_school": ("Korolenko's dramatic school", "קאָראָלענקאָס דראַמאַטישער שול", "", ""),
}
# db 1945 (באַראַן הירש-שול, linked to the GENERIC network row) is re-pointed to Sosów
REPOINT = {"1945": ("Baron Hirsch school, Sosów", "באַראַן הירש-שול (סאָסאָוו)", "סאָסאָוו")}

# ------------------------------------------------------------------ partial moves
# (parent cid, [(heading contains, sentence contains)], decision, target, child settlement, evidence)
PARTIAL = [
    ("ORG-C00001_Q01", [("עדעלמאַן", "")], "ALIGN", "1107", "אָדעס",
     "Edelman, born Odessa, studied at the university there, then 'אַריינגעטראָטן אין קאָנסערוואַטאָריע' -- Odessa, not Warsaw"),
    ("ORG-C00001_Q01", [("פֿורמאַן", "")], "NEW", "MINT:czernowitz", "טשערנאָוויץ",
     "Ari Furman 'געבוירן אין 1924 אין טשערנאָוויץ ... אַ שילער אין קאָנסערוואַטאָריע' -- his other entry is in the Czernowitz cluster C00094_Q01"),
    ("ORG-C00001_Q01", [("כענקין", ""), ("קראַסטאָשעווסקי", ""), ("שוואַרץ, מאַרגאַרעטאַ", ""),
                        ("סעגאַלעסקאָ", ""), ("סעקונדאַ", ""), ("איינהאָרן", ""), ("גאָלדשטיין, איידא", "")],
     "GENERIC", "", "",
     "bare 'a/the conservatory' with no Warsaw in the passage (Bucharest, Romania, America, New York, unplaced)"),
    ("ORG-C01439_Q01", [("ראַפּעל", ""), ("דרייקורס", ""), ("גלעזער", "")], "GENERIC", "", "",
     "bare 'אַ דראַמאַטישע שולע', no place -- not the Buenos Aires school"),
    ("ORG-C01439_Q01", [("פרענק", "")], "NEW", "MINT:goldfaden_school", "",
     "Frenk 'ווערט זיין ערשטער תלמיד' of 'אַ דראַמאַטישע שול פֿון אַברהם גאָלדפֿאָדן' -- named by its founder"),
    ("ORG-C01439_Q01", [("ווייסבערג, לואיס", "")], "NEW", "MINT:korolenko_school", "",
     "Weissberg (Kiev, 1904) engaged by Korolenko 'אין וועמענס דראָמאַטישער שול ער האָט געלערנט' -- named by its head"),
    ("ORG-C01969_Q01", [("ראַסקין", "")], "GENERIC", "", "",
     "Raskin 'שטודירט אין קונסט-אַקאַדעמיעס' -- plural, across Germany/France/Switzerland/Italy, not Munich"),
    ("ORG-C00001_Q03", [("געצמאַן", ""), ("וויעסיאָלאָיאָ", "")], "GENERIC", "", "בערלין",
     "only 'שטודירט אין קאָנסערוואַטאָריע' in Berlin (1903, 1914); Berlin had several conservatories"),
    ("ORG-C01903_Q02", [("קרעלמאַן", "")], "GENERIC", "", "וואַרשע", "bare 'געענדיקט אַ האַנדלס-שול'"),
    ("ORG-C03897_Q01", [("פֿריימאַן", "")], "GENERIC", "", "",
     "'געשפּילט אין אַרבעטער רינג קעמפּ' with no place"),
    ("ORG-C00369_Q01", [("מרגליות, בער", ""), ("מרגליות, יצחק", "")], "ALIGN", "1945", "סאָסאָוו",
     "Ber Margulies a pupil, Yitskhok Margulies 'פֿון 1892 ביז 1914 געווען רעליגיע-לערער אין דער באַראַן הירש-שול אין סאָסאָוו'"),
    ("ORG-C00369_Q01", [("פּרייס", "")], "NEW", "MINT:hirsch_vienna", "ווין",
     "Preis, fled to Vienna 1914, 'טרעט אַריין אין באַראַן הירש-שול, וועלכע ער פֿאַרענדיקט אין 1918'"),
    ("ORG-C00369_Q01", [("שוואַרץ, מאָריס", "")], "NEW", "MINT:hirsch_ny", "ניו יאָרק",
     "'אין דער באַראַן הירש שול אויף איסט בראָדוויי'"),
]

# ------------------------------------------------------------------ whole-row decisions
UNMINT_GENERIC = ["ORG-C00059", "ORG-C00709", "ORG-C00839_Q01", "ORG-C01133_Q05", "ORG-C01366_Q01",
                  "ORG-C01394", "ORG-C01478", "ORG-C01731_Q01", "ORG-C02022_Q03", "ORG-C02591_Q01",
                  "ORG-C02723", "ORG-C02734", "ORG-C02984_Q01", "ORG-C02984_Q02", "ORG-C03042_Q01",
                  "ORG-C03117", "ORG-C03248", "ORG-C03614", "ORG-C04003", "ORG-C04140", "ORG-C04397",
                  "ORG-C04777", "ORG-C07158"]
WHOLE: list[tuple[str, str, str, str]] = [(c, "GENERIC", "", UNMINT_NOTE) for c in UNMINT_GENERIC]
WHOLE += [(c, "GENERIC", "", TIER_NOTE) for c in
          ["ORG-C02178_Q01", "ORG-C00839_Q05", "ORG-C04756", "ORG-C01903_Q05", "ORG-C00271_Q05", "ORG-C01418"]]
WHOLE += [
    ("ORG-C00271_Q01", "GENERIC", "",
     "13 passages of 'די שטאָטישע שול' from 11 towns (Khotin, Shavel, Warsaw, Vilna, Yekaterinoslav, Berdichev, "
     "Belts, Atik, Odessa...); none names Efrati -- not db 1573"),
    ("ORG-C00091_Q03", "NEW", "MINT:medvedev_cons",
     "Medvedev 'דאָרטן האָט ער געעפֿנט אַ קאָנסערוואַטאָריום' -- his own school, not the Kiev Conservatory db 1091"),
    ("ORG-C00091_Q01", "ALIGN", "1962", "'געפֿאָרן קיין ווין שטודירן אין קאָנסערוואַטאָריום' -- rare kind + city = the Vienna conservatory"),
    ("ORG-C00094_Q04", "ALIGN", "1857", "Kelter, born Warsaw, 'געענדיקט די אָרטיקע קאָנסערוואַטאָריע' -- the Warsaw conservatory"),
    ("ORG-C00094_Q01", "NEW", "MINT:czernowitz", "Meiman and Furman, 'דער אָרטיקער קאָנסערוואַטאָריע' in Czernowitz -- rare kind + city"),
    ("ORG-C00001_Q07", "NEW", "MINT:munich_cons", "'פֿאַרענדיקט זי אין מינכען ... אַ קאָנסערוואַטאָריע' -- rare kind + city"),
    ("ORG-C00001_Q11", "NEW", "MINT:iasi_cons", "'אין קאָנסערוואַטאָריע אין יאָסי' -- rare kind + city"),
    ("ORG-C00094_Q05", "NEW", "MINT:cincinnati_cons", "'אין דער אָרטיקער קאָנסערוואַטאָריע' in Cincinnati -- rare kind + city"),
    ("ORG-C01969_Q03", "NEW", "MINT:antwerp_acad", "'אין דער קונסט-אַקאַדעמיע קיין אַנטווערפן' -- rare kind + city"),
    ("ORG-C00001_Q12", "NEW", "MINT:abramson_cons", "'געגרינדעט מיטן רוסישן אָפּערע-זינגער הערמאַן קאַמינסקי אַ קאָנסערוואַטאָריע' -- named by its founders"),
    ("ORG-C00405_Q03", "NEW", "MINT:irkutsk_ped", "'פּעדאָגאַגישן אינסטיטוט אין אירקוטסקער געגנט' -- rare kind + place"),
    ("ORG-C00626_Q03", "NEW", "MINT:odessa_handverker", "'אין דער דאָרטיקער \"האַנטווערקער שול\"' -- quoted title, as Kiev db 1169"),
    ("ORG-C00001_Q03", "NEW", "MINT:berlin_hochschule",
     "Brauner 'באַזוכט די קאָנסערוואַטאָריע, וואָס איז געגרינדעט געוואָרן דורכן באַרימטן קאָמפּאָזיטאָר יאָאַכים' -- the Hochschule für Musik"),
    ("ORG-C01903_Q02", "NEW", "MINT:weissfeld_school",
     "Weissfeld 'באַזיצער פֿון אַ האַנדלס-שול אין וואַרשע (עק דזשייקאַ און וואָלינסקאַ), אין וועלכער ... 1883 ... יידישע טעאַטער-פֿאָרשטעלונגען'"),
    ("ORG-C03897_Q01", "NEW", "MINT:ar_camp_boston",
     "Tauber 'סאָשעל-דירעקטאָר אין אַרבעטעררינג קעמפּ אין באָסטאָן' (1931) -- a sub-unit like db 2087, parent 189"),
]
DEPRECATE = {d: UNMINT_NOTE for d in
             "906 1130 1158 1568 1606 1608 1863 1948 1965 1981 2162 2182 2183 2271".split()}
DEPRECATE.update({d: TIER_NOTE for d in "1032 1088 1961 1264".split()})
# db rows whose links to GENERIC rows are stale; deprecated only if nothing live remains
STALE_LINK_DBS = ["689", "1949", "1989", "1990", "1945"]


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
    byid = {r["db_id"]: r for r in core}
    arow = {r["cluster_id"].strip(): r for r in al}
    removed = mention_removals.load_removed_keys()
    log: list[list[str]] = []
    errors = 0
    counts: dict[str, int] = {}

    def bump(k):
        counts[k] = counts.get(k, 0) + 1

    def L(action, target, decision, text, extra):
        log.append([NOW, REVIEWER, "org_review", action, target, decision, f"{TAG}: {text}",
                    json.dumps({**extra, "source": SRC}, ensure_ascii=False)])

    def note(row, text):
        prev = (row.get("reviewer_notes") or "").strip()
        row["reviewer_notes"] = f"{TAG}: {text}" + (" | " + prev if prev else "")
        row["reviewer"], row["reviewed_at"] = REVIEWER, NOW

    def unlink_everywhere(cid, keep=""):
        for r in core:
            if r["db_id"] != keep and cid in pipes(r["linked_cluster_ids"]):
                r["linked_cluster_ids"] = " | ".join(x for x in pipes(r["linked_cluster_ids"]) if x != cid)
                L("unlink", r["db_id"], "", f"cluster {cid} unlinked", {"cluster": cid})
                bump("unlink")

    def link(cid, d):
        t = byid[d]
        if cid not in pipes(t["linked_cluster_ids"]):
            t["linked_cluster_ids"] = " | ".join(pipes(t["linked_cluster_ids"]) + [cid])
            bump("link")

    # ---- mint targets (idempotent on name_yiddish)
    next_id = max(origin_high_water(), max(int(r["db_id"]) for r in core if r["db_id"].isdigit())) + 1
    have = {sk(r["name_yiddish"]): r["db_id"] for r in core if not r["deprecated"].strip()}
    camp_type = byid["2087"]["org_type"]

    def resolve(target: str) -> str:
        nonlocal next_id
        if not target.startswith("MINT:"):
            return target
        key = target[5:]
        name, yid, addr, parent = MINTS[key]
        if sk(yid) in have:
            return have[sk(yid)]
        d = str(next_id)
        next_id += 1
        row = {f: "" for f in kf}
        row.update(db_id=d, name=name, name_yiddish=yid, address=addr, parent_db_id=parent,
                   org_type=camp_type if key == "ar_camp_boston" else "Education")
        core.append(row)
        byid[d] = row
        have[sk(yid)] = d
        L("mint", d, "NEW", name, {"name_yiddish": yid, "parent_db_id": parent})
        bump("mint")
        print(f"  mint db {d}  {name}")
        return d

    for d, (name, yid, addr) in REPOINT.items():
        r = byid[d]
        if r["name_yiddish"] != yid:
            old = r["name_yiddish"]
            r["name"], r["name_yiddish"], r["address"] = name, yid, addr
            r["name_variants"] = " | ".join(pipes(r["name_variants"]) + [old])
            L("rename_db", d, "", f"{old} -> {yid} (the Sosów school; the network row is GENERIC)", {})
            bump("repoint")

    # ---- partial moves
    used = {x["cluster_id"].strip() for x in cl} | set(arow)
    for parent, specs, dec, target, place, ev in PARTIAL:
        prow = arow.get(parent)
        if prow is None:
            print(f"ERROR no review row {parent}")
            errors += 1
            continue
        mine = [x for x in cl if x["cluster_id"].strip() == parent
                and any(sk(h) in sk(x[HEAD]) and sk(s) in sk(x[SENT]) for h, s in specs)]
        mine = [x for x in mine if mention_removals.mention_key(x) not in removed]
        if not mine:
            print(f"  skip {parent} {specs[0][0]}…: no matching mention left (already moved?)")
            continue
        heads = {sk(h) for h, _ in specs}
        got = {h for h in heads if any(h in sk(x[HEAD]) for x in mine)}
        if got != heads:
            print(f"ERROR {parent}: no mention for {heads - got}")
            errors += 1
            continue
        allrows = [x for x in cl if x["cluster_id"].strip() == parent]
        if len(mine) == len(allrows):
            print(f"ERROR {parent}: spec selects the whole cluster")
            errors += 1
            continue
        n = 1
        while f"{parent}_P{n:02d}" in used:
            n += 1
        child = f"{parent}_P{n:02d}"
        used.add(child)
        d = resolve(target) if target else ""
        ids = {id(x) for x in mine}
        left = len(allrows) - len(mine)
        for x in allrows:
            if id(x) in ids:
                x["cluster_id"] = child
            x["cluster_size"] = str(len(mine) if id(x) in ids else left)
        a = dict(prow)
        a.update(cluster_id=child, cluster_size=str(len(mine)), decision=dec, aligned_db_id=d,
                 extracted_settlements=place, candidate_db_ids="", candidate_scores="", candidate_methods="",
                 reviewer_notes="")
        note(a, f"split off {parent} ({len(mine)} mention(s)) -> {dec}{' db ' + d if d else ''}. {ev}")
        al.insert(al.index(prow) + 1, a)
        arow[child] = a
        prow["cluster_size"] = str(left)
        note(prow, f"{len(mine)} mention(s) split off to {child} ({dec}{' db ' + d if d else ''})")
        if d:
            link(child, d)
        L("split", child, dec, f"{len(mine)} mention(s) of {parent} -> {child}; {ev}",
          {"from_cluster": parent, "aligned_db_id": d, "n_mentions": len(mine),
           "mentions": [x[HEAD] for x in mine]})
        bump("split")
        print(f"  split {parent} -> {child} ({len(mine)}) {dec} {d}")

    # ---- whole-row decisions
    for cid, dec, target, why in WHOLE:
        r = arow.get(cid)
        if r is None:
            print(f"ERROR no review row {cid}")
            errors += 1
            continue
        d = resolve(target) if target else ""
        if (r["decision"], r["aligned_db_id"].strip()) != (dec, d):
            was = (r["decision"], r["aligned_db_id"])
            r["decision"], r["aligned_db_id"] = dec, d
            note(r, f"{was[0]}{' db ' + was[1] if was[1] else ''} -> {dec}{' db ' + d if d else ''}. {why}")
            L("alignment", cid, dec, why, {"aligned_db_id": d, "was": list(was)})
            bump("decision")
        unlink_everywhere(cid, keep=d)
        if d:
            link(cid, d)

    # ---- stale links: any live db row linking a GENERIC / NOT_AN_ORG Education row
    for r in core:
        if r["deprecated"].strip():
            continue
        for c in pipes(r["linked_cluster_ids"]):
            a = arow.get(c)
            if a and a["org_type"] == "Education" and a["decision"] in ("GENERIC", "NOT_AN_ORG"):
                r["linked_cluster_ids"] = " | ".join(x for x in pipes(r["linked_cluster_ids"]) if x != c)
                L("unlink", r["db_id"], "", f"stale link to {a['decision']} row {c}", {"cluster": c})
                bump("unlink_stale")
    for d in STALE_LINK_DBS:
        r = byid[d]
        if not r["deprecated"].strip() and not pipes(r["linked_cluster_ids"]):
            DEPRECATE.setdefault(d, "no live links left: every cluster it held is GENERIC")

    # ---- deprecations
    for d, why in DEPRECATE.items():
        r = byid[d]
        if r["deprecated"].strip():
            continue
        live = [c for c in pipes(r["linked_cluster_ids"])
                if arow.get(c) and arow[c]["decision"] in ("NEW", "ALIGN") and arow[c]["aligned_db_id"].strip() == d]
        for c in pipes(r["linked_cluster_ids"]):  # stray link: the row is aligned elsewhere
            a = arow.get(c)
            if c not in live and a and a["decision"] in ("NEW", "ALIGN") and a["aligned_db_id"].strip() in byid:
                link(c, a["aligned_db_id"].strip())
                L("relink", a["aligned_db_id"].strip(), "", f"stray link of {c} moved off db {d}", {"cluster": c})
        if live:
            print(f"ERROR deprecate {d}: still holds live clusters {live}")
            errors += 1
            continue
        r["deprecated"] = "1"
        r["linked_cluster_ids"] = ""
        r["name_variants"] = " | ".join(pipes(r["name_variants"]) + [f"Deprecated 2026-11-22: {why}"])[:600]
        L("deprecate_db", d, "", why, {})
        bump("deprecate")

    print(f"\n{counts}; {len(log)} log rows; {errors} errors")
    if errors:
        print("errors: nothing written")
        return 2
    if not apply:
        print("DRY RUN -- re-run with --apply")
        return 0
    write(ORG / "core_db.tsv", kf, core, knl)
    write(ORG / "org_alignment_review.tsv", af, al, anl)
    write(ORG / "organizations_clustered.tsv", cf, cl, cnl)
    if log:
        p = ORG / "activity_log.tsv"
        b = p.read_bytes()
        buf = io.StringIO()
        csv.writer(buf, delimiter="\t", lineterminator="\r\n").writerows(log)
        p.write_bytes(b + (b"" if b.endswith(b"\n") else b"\r\n") + buf.getvalue().encode("utf-8"))
    print("WROTE core_db, org_alignment_review, organizations_clustered, activity_log")
    return 0


if __name__ == "__main__":
    sys.exit(main())
