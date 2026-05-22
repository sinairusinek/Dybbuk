#!/usr/bin/env python3
"""Audit the auto-linked kima_name_exact toponyms for homograph false positives
(e.g. שול 'synagogue' → Šiauliai Ghetto, קהלה 'community' → Kahla DE).

For each distinct (spelling, kima-derived QID), independently resolve the spelling
via the Wikidata Yiddish-label search (type-verified) and compare:
  AGREE            Wikidata's top place hit == the Kima-derived QID  → trust
  DISAGREE_PLACE   Wikidata's top place hit is a DIFFERENT place QID → review
  NO_WD_PLACE      Wikidata yi finds no place for the spelling, yet Kima gave one
                   (the homograph-risk bucket — common word matched an obscure variant)
Outputs kima/audit_name_exact.tsv (DISAGREE/NO_WD_PLACE first, by occ).
"""
import csv, json, re, time, unicodedata, urllib.parse, urllib.request
from pathlib import Path
from collections import Counter
WORK = Path(__file__).resolve().parent.parent / "data" / "working"
UA = "DybbukToponyms/1.0 (sinai.rusinek@gmail.com)"
CACHE = Path("/tmp/wd_cache.json")
_cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
PLACE_OK = {"Q6256","Q3624078","Q3024240","Q1520223","Q15634554","Q1048835","Q56061","Q133442",
 "Q5107","Q82794","Q1620908","Q15916867","Q3957","Q515","Q1549591","Q486972","Q532","Q7275",
 "Q1763527","Q12888135","Q149621","Q34876","Q202216","Q1066984","Q19953632","Q1496967",
 "Q1798622","Q15238777","Q11828004","Q1637706","Q123705","Q2074737","Q188509","Q3257686"}

def _get(url):
    if url in _cache: return _cache[url]
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for _ in range(3):
        try: d = json.load(urllib.request.urlopen(req, timeout=25)); break
        except Exception: time.sleep(1.0); d = {}
    _cache[url] = d; return d

def search(term, lang="yi"):
    q = urllib.parse.urlencode({"action":"wbsearchentities","search":term,"language":lang,
        "uselang":lang,"format":"json","limit":6,"type":"item"})
    return _get("https://www.wikidata.org/w/api.php?"+q).get("search", [])

def p31(qids):
    out = {}; qids = [q for q in qids if q]
    for i in range(0, len(qids), 50):
        u = "https://www.wikidata.org/w/api.php?"+urllib.parse.urlencode(
            {"action":"wbgetentities","ids":"|".join(qids[i:i+50]),"props":"claims|descriptions",
             "languages":"en","format":"json"})
        for q, e in _get(u).get("entities", {}).items():
            out[q] = ([c["mainsnak"]["datavalue"]["value"]["id"] for c in
                       e.get("claims", {}).get("P31", []) if c["mainsnak"].get("datavalue")],
                      e.get("descriptions", {}).get("en", {}).get("value", ""))
    return out
def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    return re.sub(r"[\s\-\".,']+","", "".join(c for c in s if not unicodedata.combining(c))).lower()

att = list(csv.DictReader(open(WORK/"toponyms_attestations.csv")))
ne = {}
for a in att:
    if a["link_method"] == "kima_name_exact":
        k = (a["source_value"], a["qid"])
        ne.setdefault(k, {"rom": a["kima_rom"], "occ": 0}); ne[k]["occ"] += 1

# gather wikidata yi candidates per spelling
wd = {}
for (sv, q), v in ne.items():
    wd[sv] = [h["id"] for h in search(sv, "yi")[:5]]
CACHE.write_text(json.dumps(_cache))
meta = p31({x for ids in wd.values() for x in ids})
CACHE.write_text(json.dumps(_cache))
def is_place(qid):
    t, d = meta.get(qid, ([], ""))
    return bool(set(t) & PLACE_OK) or bool(re.search(
        r"\b(city|town|village|region|municipalit|capital|settlement|country|district|"
        r"province|governorate|county|oblast|locality)", d, re.I))

rows = []
for (sv, q), v in ne.items():
    wd_places = [x for x in wd.get(sv, []) if is_place(x)]
    if not wd_places:
        verdict = "NO_WD_PLACE"      # homograph-risk
    elif q in wd_places:
        verdict = "AGREE"
    else:
        verdict = "DISAGREE_PLACE"
    rows.append({"verdict": verdict, "occ": v["occ"], "yiddish": sv, "kima_qid": q,
                 "kima_rom": v["rom"], "wd_top_place": wd_places[0] if wd_places else "",
                 "wd_top_desc": meta.get(wd_places[0], ([], ""))[1][:48] if wd_places else ""})
order = {"DISAGREE_PLACE": 0, "NO_WD_PLACE": 1, "AGREE": 2}
rows.sort(key=lambda r: (order[r["verdict"]], -r["occ"]))
cols = ["verdict","occ","yiddish","kima_qid","kima_rom","wd_top_place","wd_top_desc"]
with open(WORK/"kima"/"audit_name_exact.tsv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, delimiter="\t"); w.writeheader(); w.writerows(rows)
print("verdicts:", dict(Counter(r["verdict"] for r in rows)))
print("flagged-for-review occ:", sum(r["occ"] for r in rows if r["verdict"] != "AGREE"))
