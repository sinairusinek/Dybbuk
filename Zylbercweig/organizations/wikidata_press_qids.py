#!/usr/bin/env python3
"""Look up Wikidata ids for every live periodical row (press_comments.tsv).

Decided on the press open-items form 2026-11-01 (policy-qids: yes). For each
live core_db periodical without a wikidata_qid, search Wikidata by its Latin,
Yiddish/Hebrew and Cyrillic titles, then judge each hit:

  periodical   the item's P31 reaches periodical / newspaper / magazine
               (P279 walked up to 4 levels)
  title        a label or alias equals one of the row's titles once articles,
               points, diacritics and final letters are folded
  place        P291 (place of publication) or P159, or the places they lie in
               (P131, two levels), or the item's description, names the settlement
  era          P571/P576 contain the years the lexicon attests

  STRONG  periodical + title + exactly one such hit + nothing contradicts,
          and either place agrees, or there is no settlement and the era agrees
  UNSURE  any other periodical hit with a matching title (comes back to Sinai)

Output: press_wikidata_qids.tsv (one row per db_id, the ledger). STRONG rows
carry verdict=ACCEPT; apply_press_audit.py copies ACCEPTed ids into
press_comments.tsv. UNSURE rows are left for a human (verdict empty).

HTTP responses are cached under --cache, so a re-run is cheap. Rows already
in the ledger are not looked up again.

Usage: python3 wikidata_press_qids.py [--cache DIR] [--limit N]
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
from hashlib import sha1
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
COMMENTS = HERE / "press_comments.tsv"
LEDGER = HERE / "press_wikidata_qids.tsv"
LEDGER_COLUMNS = ["db_id", "name_latin", "settlement", "years_attested", "qid",
                  "wd_label", "wd_description", "wd_place", "wd_years", "signals",
                  "status", "verdict", "reviewer", "note", "others"]
API = "https://www.wikidata.org/w/api.php"
UA = "DybbukPressAudit/1.0 (https://github.com/sinairusinek/Dybbuk; research use)"
ROOTS = {"Q1002697", "Q11032", "Q41298", "Q737498", "Q5633421", "Q1110794"}
ARTICLES = {"di", "der", "dos", "das", "die", "the", "ha", "la", "le", "el", "de", "דער", "די", "דאס", "ה"}
FINALS = str.maketrans("ךםןףץ", "כמנפצ")


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower().translate(FINALS)
    s = s.replace("װ", "וו").replace("ױ", "וי").replace("ײ", "יי").replace("ײַ", "יי")
    s = re.sub(r"\(.*?\)", " ", s)
    toks = [t for t in re.split(r"[^\w]+", s) if t]
    toks = [t[2:] if t.startswith("ha") and len(t) > 4 and t[2] in "-" else t for t in toks]
    toks = [t for t in toks if t not in ARTICLES]
    return "".join(toks)


class WD:
    def __init__(self, cache: Path):
        self.cache = cache
        cache.mkdir(parents=True, exist_ok=True)
        self.ents: dict[str, dict] = {}

    def get(self, params: dict) -> dict:
        params = dict(params, format="json")
        key = sha1(json.dumps(params, sort_keys=True).encode()).hexdigest()
        f = self.cache / f"{key}.json"
        if f.exists():
            return json.loads(f.read_text())
        url = API + "?" + urllib.parse.urlencode(params)
        for attempt in range(8):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=60) as r:
                    data = json.loads(r.read())
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 7:
                    raise
                wait = 2 * (attempt + 1)
                if getattr(e, "code", None) == 429:
                    wait = max(int(e.headers.get("Retry-After") or 0), 15 * (attempt + 1))
                time.sleep(wait)
        f.write_text(json.dumps(data))
        time.sleep(0.1)
        return data

    def search(self, term: str, lang: str) -> list[str]:
        d = self.get({"action": "wbsearchentities", "search": term, "language": lang,
                      "uselang": lang, "type": "item", "limit": 10, "strictlanguage": 0})
        return [h["id"] for h in d.get("search", [])]

    def entities(self, qids) -> None:
        need = sorted({q for q in qids if q not in self.ents})
        for i in range(0, len(need), 50):
            d = self.get({"action": "wbgetentities", "ids": "|".join(need[i:i + 50]),
                          "props": "claims|labels|aliases|descriptions"})
            for q, e in d.get("entities", {}).items():
                self.ents[q] = e

    def ids(self, q: str, prop: str) -> list[str]:
        out = []
        for c in self.ents.get(q, {}).get("claims", {}).get(prop, []):
            v = c.get("mainsnak", {}).get("datavalue", {}).get("value")
            if isinstance(v, dict) and "id" in v:
                out.append(v["id"])
        return out

    def year(self, q: str, prop: str) -> int | None:
        for c in self.ents.get(q, {}).get("claims", {}).get(prop, []):
            v = c.get("mainsnak", {}).get("datavalue", {}).get("value")
            if isinstance(v, dict) and "time" in v:
                m = re.match(r"[+-](\d{4})", v["time"])
                if m:
                    return int(m.group(1))
        return None

    def names(self, q: str) -> set[str]:
        e = self.ents.get(q, {})
        out = {v["value"] for v in e.get("labels", {}).values()}
        out |= {a["value"] for vs in e.get("aliases", {}).values() for a in vs}
        return out

    def label(self, q: str) -> str:
        labs = self.ents.get(q, {}).get("labels", {})
        for l in ("en", "yi", "he", "ru", "pl", "de"):
            if l in labs:
                return labs[l]["value"]
        return next(iter(labs.values()), {}).get("value", "")

    def desc(self, q: str) -> str:
        ds = self.ents.get(q, {}).get("descriptions", {})
        return (ds.get("en") or next(iter(ds.values()), {})).get("value", "")

    def is_periodical(self, q: str, memo: dict) -> bool:
        frontier, seen = set(self.ids(q, "P31")), set()
        for _ in range(5):
            if frontier & ROOTS:
                return True
            frontier -= seen
            if not frontier:
                return False
            seen |= frontier
            todo = [c for c in frontier if c not in memo]
            self.entities(todo)
            for c in todo:
                memo[c] = self.ids(c, "P279")
            frontier = {p for c in frontier for p in memo.get(c, [])}
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=str(HERE / ".wikidata_cache"))
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    wd = WD(Path(a.cache))

    core = {r["db_id"]: r for r in csv.DictReader(open(CORE_DB, newline="", encoding="utf-8"), delimiter="\t")}
    comments = list(csv.DictReader(open(COMMENTS, newline="", encoding="utf-8"), delimiter="\t"))
    done = {}
    if LEDGER.exists():
        done = {r["db_id"]: r for r in csv.DictReader(open(LEDGER, newline="", encoding="utf-8"), delimiter="\t")}
    todo = [c for c in comments
            if c["db_id"] in core and not core[c["db_id"]]["merged_into"].strip()
            and re.search("journal|newspaper", core[c["db_id"]]["org_type"], re.I)
            and not c["wikidata_qid"].strip() and c["db_id"] not in done]
    if a.limit:
        todo = todo[:a.limit]
    print(f"looking up {len(todo)} periodicals", file=sys.stderr)
    memo: dict[str, list[str]] = {}
    out = list(done.values())
    def one(c: dict) -> dict:
        r = core[c["db_id"]]
        titles = {t.strip() for t in (c["name_latin"], r["name"], r["name_yiddish"], c["name_yiddish"])
                  if t and t.strip()}
        titles |= {re.sub(r"\s*\(.*?\)\s*", " ", t).strip() for t in titles}
        keys = {fold(t) for t in titles if fold(t)}
        hits: list[str] = []
        for t in titles:
            langs = ["yi", "he"] if re.search("[֐-׿]", t) else (["ru"] if re.search("[Ѐ-ӿ]", t) else ["en"])
            for lang in langs:
                hits += [h for h in wd.search(t, lang) if h not in hits]
        wd.entities(hits)
        settle = fold(c["settlement"])
        yrs = [int(y) for y in re.findall(r"\b(1[6-9]\d\d|20\d\d)\b", c["years_attested"])]
        cands = []
        for q in hits:
            if not ({fold(x) for x in wd.names(q)} & keys):
                continue
            if not wd.is_periodical(q, memo):
                continue
            places = wd.ids(q, "P291") + wd.ids(q, "P159")
            wd.entities(places)
            up = [u for p in places for u in wd.ids(p, "P131")]
            wd.entities(up)
            up += [u for p in up for u in wd.ids(p, "P131")]
            wd.entities(up)
            pnames = {fold(x) for p in places + up for x in wd.names(p)}
            pnames = {x for x in pnames if len(x) >= 4}
            in_desc = bool(settle) and len(settle) >= 4 and settle in fold(wd.desc(q))
            start, end = wd.year(q, "P571"), wd.year(q, "P576")
            agree = bool(settle) and (in_desc or any(settle in x or x in settle for x in pnames))
            place = "agree" if agree else ("clash" if settle and pnames else "unknown")
            if yrs and (start or end):
                ok = (start is None or min(yrs) >= start - 1) and (end is None or max(yrs) <= end + 1)
                era = "agree" if ok else "clash"
            else:
                era = "unknown"
            cands.append(dict(qid=q, place=place, era=era,
                              wd_place="; ".join(sorted({wd.label(p) for p in places})),
                              wd_years=f"{start or '?'}–{end or '?'}"))
        best = None
        status = "NONE"
        if cands:
            good = [x for x in cands if "clash" not in (x["place"], x["era"])]
            strong = [x for x in good if x["place"] == "agree" or (not settle and x["era"] == "agree")]
            if len(cands) == 1 and len(strong) == 1:
                best, status = strong[0], "STRONG"
            else:
                order = {"agree": 0, "unknown": 1, "clash": 2}
                best = sorted(cands, key=lambda x: (order[x["place"]] + order[x["era"]]))[0]
                status = "UNSURE"
        row = {k: "" for k in LEDGER_COLUMNS}
        row.update(db_id=c["db_id"], name_latin=c["name_latin"], settlement=c["settlement"],
                   years_attested=c["years_attested"], status=status)
        if best:
            row.update(qid=best["qid"], wd_label=wd.label(best["qid"]), wd_description=wd.desc(best["qid"]),
                       wd_place=best["wd_place"], wd_years=best["wd_years"],
                       signals=f"place={best['place']} era={best['era']} hits={len(cands)}",
                       others=" | ".join(x["qid"] for x in cands if x is not best))
            if status == "STRONG":
                row.update(verdict="ACCEPT", reviewer="auto", note="title+periodical+place/era, single hit")
        return row

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(3) as ex:
        for n, row in enumerate(ex.map(one, todo), 1):
            out.append(row)
            if n % 50 == 0:
                print(f"  {n}/{len(todo)}", file=sys.stderr)
    out.sort(key=lambda r: int(r["db_id"]))
    with open(LEDGER, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=LEDGER_COLUMNS, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(out)
    from collections import Counter, defaultdict
    print(Counter(r["status"] for r in out), file=sys.stderr)
    by_q = defaultdict(list)
    for r in out:
        if r["qid"]:
            by_q[r["qid"]].append(r["db_id"])
    for q, ds in sorted(by_q.items()):
        if len(ds) > 1:
            print(f"  shared {q}: db {', '.join(ds)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
