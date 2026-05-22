#!/usr/bin/env python3
"""Autonomous resolver for the residual unlinked toponyms.

For each non-descriptor unlinked spelling, gather candidate QIDs from:
  1. the internal index (spellings already linked elsewhere in the corpus), and
  2. the Wikidata API (wbsearchentities) in yi, then he, then de.
Every candidate QID is type-verified via P31 (wbgetentities) against a place
allow-list / reject-list. Output is GRADED by confidence so a human reviews
only what needs it:

  A_autolink  exact normalized label match in yi/he + place-typed + unambiguous
  B_review    place-typed candidate but matched loosely / via de / mild ambiguity
  C_review    only weak or wrong-typed candidates, or nothing — needs human/Strategy-4

Caches all API responses to /tmp/wd_cache.json so re-runs are cheap.
"""
import csv, json, re, sys, time, unicodedata, urllib.parse, urllib.request
from pathlib import Path
from collections import Counter

WORK = Path(__file__).resolve().parent.parent / "data" / "working"
UA = "DybbukToponyms/1.0 (sinai.rusinek@gmail.com)"
CACHE = Path("/tmp/wd_cache.json")
_cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}

# P31 values that ARE places (allow), and that are NOT (reject, hard veto).
PLACE_OK = {
 "Q6256","Q3624078","Q3024240","Q1520223","Q15634554","Q1048835","Q56061","Q133442",  # countries/states
 "Q5107","Q82794","Q1620908","Q15916867","Q3957","Q515","Q1549591","Q486972","Q532",   # continent/region/town/village
 "Q7275","Q1763527","Q12888135","Q149621","Q34876","Q202216","Q1066984","Q19953632",   # admin/district
 "Q1496967","Q1798622","Q15238777","Q11828004","Q1637706","Q123705","Q2074737",        # governorate/neighborhood
 "Q188509","Q3257686","Q5084","Q702492","Q17343829","Q39614","Q205495","Q1115575"}     # suburb/locality/hamlet/cemetery? no
REJECT = {"Q8142","Q34770","Q11424","Q5","Q482994","Q215380","Q7889","Q4022","Q11446", # currency/language/film/human/album/band/river? keep rivers? no
          "Q16521","Q4830453","Q43229","Q5398426","Q11410"}  # taxon/business/org/tv series/game

def _get(url):
    if url in _cache: return _cache[url]
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for _ in range(3):
        try:
            d = json.load(urllib.request.urlopen(req, timeout=25)); break
        except Exception:
            time.sleep(1.0); d = {}
    _cache[url] = d
    return d

def search(term, lang):
    q = urllib.parse.urlencode({"action":"wbsearchentities","search":term,"language":lang,
        "uselang":lang,"format":"json","limit":6,"type":"item"})
    return _get("https://www.wikidata.org/w/api.php?"+q).get("search", [])

def p31_of(qids):
    out = {}
    qids = [q for q in qids if q]
    for i in range(0, len(qids), 50):
        b = qids[i:i+50]
        u = "https://www.wikidata.org/w/api.php?"+urllib.parse.urlencode(
            {"action":"wbgetentities","ids":"|".join(b),"props":"claims|labels|descriptions",
             "languages":"en","format":"json"})
        d = _get(u)
        for q, e in d.get("entities", {}).items():
            types = [c["mainsnak"]["datavalue"]["value"]["id"]
                     for c in e.get("claims", {}).get("P31", []) if c["mainsnak"].get("datavalue")]
            out[q] = {"p31": types,
                      "en": e.get("labels", {}).get("en", {}).get("value", ""),
                      "desc": e.get("descriptions", {}).get("en", {}).get("value", "")}
    return out

def is_place(meta):
    t = set(meta.get("p31", []))
    if t & REJECT: return False
    if t & PLACE_OK: return True
    # description fallback for thin P31
    d = meta.get("desc", "").lower()
    return bool(re.search(r"\b(country|city|town|village|region|municipalit|settlement|"
                          r"governorate|district|county|province|state|locality|neigh] ?bo)", d))

def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[\s\-\"'.,]+", "", s).strip().lower()

def main():
    rows = [r for r in csv.DictReader(open(WORK/"toponyms_unlinked.csv"))
            if r["is_descriptor"] != "True"]
    done = {r["yiddish"] for r in csv.DictReader(open(WORK/"kima"/"unlinked_confirmed.tsv"), delimiter="\t")}
    rows = [r for r in rows if r["variant"] not in done]

    # internal index from already-linked attestations
    idx = {}; meta_int = {}
    for a in csv.DictReader(open(WORK/"toponyms_attestations.csv")):
        q = a["qid"].strip()
        if q.startswith("Q") and a["link_status"] in ("linked", "needs_review"):
            idx.setdefault(norm(a["source_value"]), Counter())[q] += 1
            meta_int.setdefault(q, a["label_en"])

    # gather candidate qids
    cand = {}  # variant -> list of (qid, source)
    for i, r in enumerate(rows):
        v = r["variant"]; nv = norm(v); c = []
        if nv in idx:
            for q, _ in idx[nv].most_common(3): c.append((q, "internal"))
        for lang in ("yi", "he", "de"):
            res = search(v, lang)
            for hit in res[:3]:
                c.append((hit["id"], lang+("_exact" if norm(hit.get("label",""))==nv else "")))
            if res: break
        cand[v] = c
        if i % 40 == 0:
            CACHE.write_text(json.dumps(_cache)); print(f"  searched {i}/{len(rows)}", file=sys.stderr)
    CACHE.write_text(json.dumps(_cache))

    allq = {q for cs in cand.values() for q, _ in cs}
    meta = p31_of(sorted(allq))
    CACHE.write_text(json.dumps(_cache))

    occ = {r["variant"]: int(r["occurrences"]) for r in rows}
    fields = {r["variant"]: r["fields"] for r in rows}
    A, B, C = [], [], []
    for v, cs in cand.items():
        nv = norm(v)
        placed = [(q, s) for q, s in cs if is_place(meta.get(q, {}))]
        exact = [(q, s) for q, s in placed if s.endswith("_exact") or s == "internal"]
        uniqq = list(dict.fromkeys(q for q, _ in placed))
        def row(q, grade, why):
            m = meta.get(q, {})
            return {"yiddish": v, "occ": occ[v], "fields": fields[v], "qid": q,
                    "label_en": m.get("en") or meta_int.get(q, ""), "desc": m.get("desc", ""),
                    "n_place_cands": len(uniqq), "source": why, "grade": grade}
        if exact and len({q for q, _ in exact}) == 1:
            A.append(row(exact[0][0], "A_autolink", exact[0][1]))
        elif exact:
            B.append(row(exact[0][0], "B_review", "exact-but-multi:"+";".join(uniqq[:4])))
        elif placed:
            B.append(row(placed[0][0], "B_review", placed[0][1]+":"+";".join(uniqq[:4])))
        else:
            top = cs[0] if cs else ("", "")
            r = row(top[0], "C_review", (top[1] or "no-candidate"))
            r["label_en"] = meta.get(top[0], {}).get("en", "")
            r["desc"] = meta.get(top[0], {}).get("desc", "")
            C.append(r)

    cols = ["yiddish","occ","fields","qid","label_en","desc","n_place_cands","source","grade"]
    for name, lst in (("A_autolink", A), ("B_review", B), ("C_review", C)):
        lst.sort(key=lambda r: -r["occ"])
        with open(WORK/"kima"/f"residual_{name}.tsv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, delimiter="\t"); w.writeheader(); w.writerows(lst)
    print(f"A_autolink={len(A)} ({sum(r['occ'] for r in A)} occ) | "
          f"B_review={len(B)} ({sum(r['occ'] for r in B)} occ) | "
          f"C_review={len(C)} ({sum(r['occ'] for r in C)} occ)")

if __name__ == "__main__":
    main()
