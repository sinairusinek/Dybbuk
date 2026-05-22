#!/usr/bin/env python3
"""Independent Wikidata cross-check for flagged (yiddish, qid) pairs.

Generalizes audit_kima_name_exact.py: instead of only the kima_name_exact set, it
reads any TSV with `yiddish` and a QID column and, for each distinct spelling,
resolves it via the Wikidata Yiddish-label search (type-verified to places) and
compares the top WD place hit to the linked QID:
    AGREE          WD's top place == linked QID                      -> trust
    DISAGREE_PLACE WD's top place is a DIFFERENT place QID           -> review
    NO_WD_PLACE    WD yi finds no place for the spelling             -> exonym/region/descriptor or homograph

Usage:
    verify_flags_wikidata.py <in.tsv> <yiddish_col> <qid_col> <out.tsv>
Caches WD responses in /tmp/wd_cache.json (shared with audit_kima_name_exact.py).
"""
import csv, json, re, sys, time, unicodedata, urllib.parse, urllib.request
from pathlib import Path

csv.field_size_limit(10**7)
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
    d = {}
    for _ in range(3):
        try: d = json.load(urllib.request.urlopen(req, timeout=25)); break
        except Exception: time.sleep(1.0)
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

infile, ycol, qcol, outfile = sys.argv[1:5]
rows_in = list(csv.DictReader(open(infile), delimiter="\t"))
# distinct spelling -> (qid, any extra row for occ/severity passthrough)
pairs = {}
for r in rows_in:
    sv = r[ycol]
    pairs.setdefault((sv, r[qcol]), r)

wd = {}
for (sv, q) in pairs:
    if sv not in wd:
        wd[sv] = [h["id"] for h in search(sv, "yi")[:5]]
CACHE.write_text(json.dumps(_cache))
meta = p31({x for ids in wd.values() for x in ids} | {q for _, q in pairs})
CACHE.write_text(json.dumps(_cache))

def is_place(qid):
    t, d = meta.get(qid, ([], ""))
    return bool(set(t) & PLACE_OK) or bool(re.search(
        r"\b(city|town|village|region|municipalit|capital|settlement|country|district|"
        r"province|governorate|county|oblast|locality|neighborhood|neighbourhood)", d, re.I))

out = []
for (sv, q), r in pairs.items():
    wd_places = [x for x in wd.get(sv, []) if is_place(x)]
    if not wd_places:
        verdict = "NO_WD_PLACE"
    elif q in wd_places:
        verdict = "AGREE"
    else:
        verdict = "DISAGREE_PLACE"
    out.append({"verdict": verdict, "yiddish": sv, "linked_qid": q,
                "linked_label": r.get("kima_rom") or r.get("linked_label") or r.get("label_en",""),
                "wd_top_place": wd_places[0] if wd_places else "",
                "wd_top_desc": meta.get(wd_places[0], ([], ""))[1][:60] if wd_places else "",
                "severity": r.get("severity",""), "occ": r.get("occ","")})
order = {"DISAGREE_PLACE":0, "NO_WD_PLACE":1, "AGREE":2}
out.sort(key=lambda r: (order[r["verdict"]], -int(r["occ"] or 0)))
cols = ["verdict","occ","severity","yiddish","linked_qid","linked_label","wd_top_place","wd_top_desc"]
with open(outfile, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, delimiter="\t"); w.writeheader(); w.writerows(out)
from collections import Counter
print("verdicts:", dict(Counter(r["verdict"] for r in out)), file=sys.stderr)
