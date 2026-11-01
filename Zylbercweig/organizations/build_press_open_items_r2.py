#!/usr/bin/env python3
"""Items for the press audit's round-2 form (2026-11-01).

  dup    rows the Wikidata pass tied to one item that may be the same paper
         (hand-picked: most shared ids were namesakes in other cities)
  qid    every UNSURE row of press_wikidata_qids.tsv whose row is still live;
         suggestion ACCEPT when nothing contradicts and city or years agree,
         REJECT when city or years clash or the id is already accepted for
         another row, none otherwise
  other  the Kinder-Velt camp split and the publisher row left empty

Writes press_open_items_r2.json for build_press_open_form.py (--items).
Answers come back as artifact db docs {choice, note}, keyed by item id.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).resolve().parent
OUT = HERE / "press_open_items_r2.json"
REL = "_ - organizations - _ - relations - _ - original_sentence"


def load(name: str) -> list[dict]:
    with open(HERE / name, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


core = {r["db_id"]: r for r in load("core_db.tsv")}
cm = {r["db_id"]: r for r in load("press_comments.tsv")}
qids = load("press_wikidata_qids.tsv")
pipes = lambda v: [x.strip() for x in (v or "").split("|") if x.strip()]  # noqa: E731

mentions: dict[str, list[str]] = defaultdict(list)
for r in load("organizations_clustered.tsv"):
    s = (r.get(REL) or "").strip()
    if s:
        mentions[r["cluster_id"].strip()].append(f"{r['_ - heading'].strip()} — {s}")


def live(d: str) -> bool:
    r = core.get(d)
    return bool(r) and not r["merged_into"].strip() and r["deprecated"].strip().lower() not in ("true", "1")


def row_line(d: str) -> str:
    c, r = cm.get(d, {}), core[d]
    bits = [f"db {d}", r["name_yiddish"] or r["name"], c.get("name_latin", ""),
            " ".join(filter(None, [c.get("settlement", ""), c.get("years_attested", "")]))]
    return " · ".join(b for b in bits if b) + (f" — {c['comment']}" if c.get("comment") else "")


def ctx(d: str, n: int = 2) -> list[str]:
    out = []
    for cid in pipes(core[d]["linked_cluster_ids"]):
        out += mentions.get(cid, [])[:n]
    return out[:n]


def wd(q: str) -> list:
    return [f"Wikidata {q}", f"https://www.wikidata.org/wiki/{q}"]


items: list[dict] = []

# ---------------- dup ----------------
DUPS = [
    ("dup-tsefira", ["68", "270"], "Q2906544",
     [("merge", "Same paper: merge 270 into 68"), ("keep", "Keep both")], "merge",
     "270 is an authority row with no mentions; both are the Warsaw Hebrew Ha-tsefira."),
    ("dup-ivri", ["4036", "4104", "4037"], "Q6880995",
     [("merge", "4104 Ivri anokhi = 4036 Ha-Ivri (Brody): merge 4104 into 4036"),
      ("keep", "Keep all three")], "merge",
     "Ivri anokhi (Brody, 1865–90) also appeared as Ha-Ivri; 4037 is Rikhter's Lemberg Zionist weekly of 1895, a different paper."),
    ("dup-tsayt", ["224", "637", "3882"], "Q5270059",
     [("petersburg", "224 is the Petersburg Di tsayt (1913–14): give it this Wikidata id"),
      ("keep", "Leave 224 as it is")], "keep",
     "224 is an authority row with no mentions and no city. Wikidata's Di tsayt is the Petersburg weekly; 637 is London, 3882 New York."),
]
for iid, ds, q, opts, rec, why in DUPS:
    items.append(dict(id=iid, section="dup", title=" / ".join(core[d]["name_yiddish"] or core[d]["name"] for d in ds),
                      sub=why, context=[row_line(d) for d in ds] + sum((ctx(d, 1) for d in ds), []),
                      links=[wd(q)], options=opts, rec=rec, db=", ".join(ds)))

# ---------------- qid ----------------
# Claude's read of each UNSURE hit (2026-11-01), which replaces the mechanical
# suggestion: stale signals (347), our wider attested years (219, 692), and
# bare Wikidata items that cannot be checked (suggested REJECT, not ACCEPT).
REVIEW = {'219': ('ACCEPT', 'Claude: same paper (Der Yud, Kraków 1899–1902; our later years are other papers)'),
    '265': ('ACCEPT', "Claude: same paper (Sokolov's Warsaw annual Ha-Asif)"),
    '267': ('ACCEPT', "Claude: same paper (Frishman's Ha-Dor, 1901)"),
    '272': ('ACCEPT', 'Claude: same paper'),
    '274': ('ACCEPT', 'Claude: same paper (Ha-Zman moved from Petersburg to Vilna)'),
    '347': ('ACCEPT', 'Claude: same paper (the earlier clash came from the misattached New York comment, since fixed)'),
    '352': ('REJECT', 'Claude: different paper (New Jersey, 1971)'),
    '399': ('ACCEPT', 'Claude: same paper (Petersburg theatre weekly 1897–1918)'),
    '463': ('REJECT', 'Claude: different paper (a Petersburg paper)'),
    '637': ('REJECT', "Claude: different paper (Wikidata's is the Petersburg weekly; ours is London)"),
    '692': ('ACCEPT', 'Claude: same paper (our 1890 is a stray early citation)'),
    '693': ('REJECT', 'Claude: keep the id on 692 only; this is a regional edition'),
    '694': ('REJECT', 'Claude: keep the id on 692 only; this is a regional edition'),
    '695': ('REJECT', 'Claude: keep the id on 692 only; this is a regional edition'),
    '935': ('REJECT', 'Claude: different paper (Paris 1911)'),
    '1144': ('REJECT', 'Claude: different paper'),
    '1220': ('REJECT', 'Claude: different paper (an 1898 paper; ours is Kyiv 1927)'),
    '1292': ('ACCEPT', 'Claude: same paper'),
    '1527': ('ACCEPT', 'Claude: same paper'),
    '1549': ('REJECT', 'Claude: Wikidata item too bare to verify, so no'),
    '1550': ('REJECT', 'Claude: different paper (Slovak)'),
    '1551': ('REJECT', 'Claude: different paper'),
    '1660': ('ACCEPT', 'Claude: same paper (the Kishinev daily)'),
    '1864': ('REJECT', 'Claude: different paper (postwar Folks-shtime)'),
    '1865': ('REJECT', "Claude: different paper (Wikidata's is the 1921–43 Bundist daily; our row starts 1899)"),
    '1893': ('REJECT', 'Claude: different paper (a Saxon tabloid)'),
    '1897': ('REJECT', 'Claude: Wikidata item too bare to verify, so no'),
    '1911': ('REJECT', 'Claude: different paper'),
    '1932': ('ACCEPT', 'Claude: same paper'),
    '3303': ('ACCEPT', 'Claude: same paper'),
    '3863': ('REJECT', 'Claude: different paper (Hungarian)'),
    '3864': ('REJECT', 'Claude: different paper (Hungarian)'),
    '3865': ('REJECT', 'Claude: different paper'),
    '3876': ('REJECT', 'Claude: Wikidata item too bare to verify, so no'),
    '3877': ('REJECT', 'Claude: Wikidata item too bare to verify, so no'),
    '3878': ('REJECT', 'Claude: Wikidata item too bare to verify, so no'),
    '3882': ('REJECT', 'Claude: different paper'),
    '3883': ('REJECT', 'Claude: different paper'),
    '3890': ('REJECT', 'Claude: different paper'),
    '3891': ('REJECT', 'Claude: different paper'),
    '3892': ('REJECT', 'Claude: different paper'),
    '3893': ('REJECT', 'Claude: different paper'),
    '3903': ('ACCEPT', 'Claude: same paper (Davar, Tel Aviv)'),
    '3905': ('ACCEPT', 'Claude: same paper (Buenos Aires Yiddish monthly)'),
    '3937': ('ACCEPT', 'Claude: same paper (Levner\'s children\'s weekly; our "c. 1905" is approximate)'),
    '3938': ('REJECT', 'Claude: different paper (a 2009 Haredi paper)'),
    '3939': ('ACCEPT', "Claude: same paper (Brenner's Ha-Me'orer)"),
    '3947': ('REJECT', 'Claude: different paper (Danish)'),
    '3949': ('REJECT', 'Claude: different paper (Tenerife; ours is Mexico)'),
    '3958': ('ACCEPT', 'Claude: same paper'),
    '3969': ('ACCEPT', 'Claude: same paper (Vilna, 1905–07)'),
    '3976': ('ACCEPT', 'Claude: same paper'),
    '3979': ('ACCEPT', 'Claude: same paper (Petersburg journal 1910–16)'),
    '3995': ('REJECT', 'Claude: different paper'),
    '4019': ('ACCEPT', 'Claude: same paper'),
    '4020': ('REJECT', 'Claude: different paper'),
    '4021': ('REJECT', 'Claude: different paper'),
    '4030': ('ACCEPT', 'Claude: same paper (Bucharest Jewish weekly 1879–85)'),
    '4031': ('ACCEPT', 'Claude: same paper'),
    '4036': ('ACCEPT', 'Claude: same paper (Ha-Ivri / Ivri anokhi, Lemberg then Brody)'),
    '4037': ('REJECT', "Claude: different paper (Rikhter's 1895 weekly)"),
    '4046': ('REJECT', 'Claude: different paper (Luxembourg)'),
    '4049': ('REJECT', 'Claude: Wikidata item too bare to verify, so no'),
    '4061': ('ACCEPT', 'Claude: same paper'),
    '4062': ('REJECT', "Claude: different paper (Wikidata's is the Łódź paper; ours has no city)"),
    '4068': ('REJECT', 'Claude: different paper (the Moscow literary journal)'),
    '4094': ('ACCEPT', 'Claude: same paper'),
    '4108': ('REJECT', 'Claude: different paper'),
    '4120': ('REJECT', 'Claude: different paper'),
    '4140': ('REJECT', 'Claude: Wikidata item too bare to verify, so no'),
    '4149': ('REJECT', 'Claude: different paper (Dominican)'),
    '4153': ('ACCEPT', "Claude: same paper (Gorky's Letopis)"),
    '4159': ('REJECT', 'Claude: different paper'),
    '4165': ('ACCEPT', 'Claude: same paper (Eynikeyt, Moscow 1942–48)'),
    '4189': ('REJECT', 'Claude: different paper'),
    '4202': ('ACCEPT', 'Claude: same paper'),
    '4207': ('ACCEPT', 'Claude: same paper'),
    '4219': ('ACCEPT', 'Claude: same paper (Vienna Poale-Zion paper)'),
    '4222': ('REJECT', 'Claude: Wikidata item too bare to verify, so no'),
    '4227': ('ACCEPT', 'Claude: same paper'),
    '4231': ('ACCEPT', 'Claude: same paper'),
    '4238': ('ACCEPT', 'Claude: same paper'),
    '4248': ('REJECT', 'Claude: different paper'),
    '4250': ('REJECT', 'Claude: different paper'),
    '4268': ('REJECT', 'Claude: different paper (a 1950 journal)'),
    '4269': ('ACCEPT', 'Claude: same paper'),
    '4271': ('ACCEPT', 'Claude: same paper (Satirikon 1908–14; our 1906 is approximate)'),
    '4272': ('ACCEPT', 'Claude: same paper'),
    '4273': ('REJECT', 'Claude: different paper (San Francisco)'),
    '4279': ('ACCEPT', 'Claude: same paper'),
    '4280': ('ACCEPT', 'Claude: same paper'),
    '4282': ('REJECT', 'Claude: different paper'),
    '4285': ('REJECT', 'Claude: probably different (Moscow; ours is likely the Petersburg Zhizn 1897–1901)')}

accepted = {r["qid"]: r["db_id"] for r in qids if r.get("verdict") == "ACCEPT"}
for d, r in cm.items():
    if r.get("wikidata_qid"):
        accepted.setdefault(r["wikidata_qid"], d)
handled = {"270", "224", "4104"}
for r in sorted(qids, key=lambda r: int(r["db_id"])):
    d = r["db_id"]
    if r["status"] != "UNSURE" or not live(d) or d in handled or r.get("verdict"):
        continue
    sig = r["signals"]
    other = accepted.get(r["qid"])
    if other and other != d:
        rec, why = "REJECT", f"this id is already on db {other}"
    elif "clash" in sig:
        rec, why = "REJECT", "city or years disagree"
    elif "agree" in sig:
        rec, why = "ACCEPT", "nothing disagrees, and city or years agree"
    else:
        rec, why = "", "Wikidata gives no city or years to compare"
    if d in REVIEW:
        rec, why = REVIEW[d]
    c = cm.get(d, {})
    items.append(dict(
        id=f"qid-{d}", section="qid",
        title=f"{core[d]['name_yiddish'] or core[d]['name']}  ↔  {r['wd_label']} ({r['qid']})",
        sub=(f"Ours: {c.get('settlement') or 'no city'}, {c.get('years_attested') or 'no years'}. "
             f"Wikidata: {r['wd_description'] or 'no description'}; {r['wd_place'] or 'no place'}, {r['wd_years']}. "
             f"{why[0].upper() + why[1:]}."),
        context=[row_line(d)] + ctx(d) + ([f"Other Wikidata candidates: {r['others']}"] if r["others"] else []),
        links=[wd(r["qid"])], options=[("ACCEPT", "Same paper: take the id"), ("REJECT", "Not this paper")],
        rec=rec, db=d))

# ---------------- other ----------------
items.append(dict(
    id="other-kindervelt-camp", section="other", title="קינדער-וועלט — the camp (ORG-C00547_P02)",
    sub="Split from the Kinder-Velt periodical this morning and typed Education; it has no row yet. "
        "Camp Kinderland (db 206) is a different camp.",
    context=mentions.get("ORG-C00547_P02", []),
    options=[("mint", "Give it its own row: Camp Kinder-Velt (Education)"),
             ("generic", "Generic: no row"), ("link", "Same as an existing row (name it in the note)")],
    rec="mint", clusters=["ORG-C00547_P02"]))
items.append(dict(
    id="other-3573", section="other", title="פֿאַרלאַג „אַרבעטער וועלט\" (שיקאַגאָ) — db 3573, now empty",
    sub="Its only cluster (ORG-C02146, Boris Kader's book, 1939) went to the paper Idishe arbeter velt (3304) "
        "on your conflict choice. The row has no clusters left.",
    context=mentions.get("ORG-C02146", []),
    options=[("remove", "Remove the empty row"), ("keep", "Keep it: the paper's publishing house"),
             ("back", "Undo: the cluster belongs to this publisher row")],
    rec="remove", db="3573"))

OUT.write_text(json.dumps(items, ensure_ascii=False, indent=1), encoding="utf-8")
from collections import Counter  # noqa: E402
print(f"wrote {OUT.name}: {len(items)} items", dict(Counter(i["section"] for i in items)),
      "qid recs", dict(Counter(i["rec"] for i in items if i["section"] == "qid")))
