"""Context-checked place linking for the D–G stations (2026-11-24).

The itinerary resolver (plays/itinerary/build_itinerary_data.py) links by spelling alone and keeps
only the FIRST gazetteer record per spelling, so homonyms go wrong: אַלעקסאַנדריע is two Ukrainian
Oleksandriias in the gazetteer and Alexandria in Egypt in the Lexicon's Egypt tours. This module:

  1. keeps EVERY record per spelling (gazetteer rows, confirmed Kima rows, plays-KG places,
     the 11-22 place decisions, and HOMONYMS below);
  2. checks a qualifier in the place string itself ("X, וואָלין", "X (עגיפּטן)"): the linked place
     must lie within the qualifier region's radius, else it is rejected;
  3. picks among homonyms by context: a region named in the station's evidence sentence, else the
     centroid of the person's other linked stations; no support -> '?'.

Every decision is recorded in LinkTrace so the pack's README numbers can be recomputed.
"""
from __future__ import annotations

import csv
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

ZYL = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ZYL / "plays" / "itinerary"))
import build_itinerary_data as bid  # noqa: E402

ATTEST = ZYL / "zibn-shtern" / "data" / "working" / "toponyms_attestations.csv"  # reviewed, per occurrence
DECISIONS = ZYL / "extraction_v2" / "qa" / "kima" / "defg_place_decisions_2026_11_22.tsv"
KIMA_CSV = Path("/Users/sinairusinek/Documents/GitHub/Kimatch/20250126KimaPlacesCSVx.csv")

# Far namesakes the gazetteer lacks, taken from the Kima table (id -> read at load time, never typed QIDs).
# spelling (unpointed, normalized) -> Kima ids
HOMONYMS = {
    "אלעקסאנדריע": ["667"],   # Alexandria (Egypt)
    "אלעקסאנדרייע": ["667"],
}

# radius (km) a place may lie from a region's reference point and still be "in" it
BIG = {"America (USA)": 4500, "Russia": 6000, "Soviet Union": 6000, "Canada": 4500, "Europe": 2500,
       "the Orient": 2500, "Western Europe": 1500, "Southern Russia": 1200, "South Africa": 1500,
       "Caucasus": 800, "Turkey": 1000}
DEFAULT_R = 900


def km(a, b):
    (la1, lo1), (la2, lo2) = a, b
    p = math.pi / 180
    h = (math.sin((la2 - la1) * p / 2) ** 2
         + math.cos(la1 * p) * math.cos(la2 * p) * math.sin((lo2 - lo1) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(h))


def region_of(word):
    """A word or phrase -> (name, (lat, lon)) if it names a REGIONS entry (incl. -ער/-ען/-ישער forms)."""
    w = bid.norm(word)
    for c in (w, re.sub(r"(ישע?ר|ער|ען|ן)$", "", w), re.sub(r"ען$", "ן", w), w + "ן"):
        if c in bid.REGIONS:
            en, la, lo = bid.REGIONS[c]
            return en, (la, lo)
    return None


class Linker:
    def __init__(self):
        self.multi = defaultdict(dict)  # norm spelling -> {key: (label, qid, kima, lat, lon)}
        with open(bid.GAZ, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if not r.get("lat"):
                    continue
                rec = (r["label_en"], r["qid"], "", float(r["lat"]), float(r["lon"]))
                for v in [r.get("label_yi", "")] + (r.get("variants", "") or "").split(";"):
                    v = bid.norm(v)
                    if v:
                        self.multi[v][r["qid"]] = rec
        self.lut = bid.load_gazetteer()  # confirmed Kima rows + KG places + gazetteer first-hit
        for v, (q, en, la, lo) in self.lut.items():
            if q and q not in self.multi[v]:
                self.multi[v][q] = (en, q, "", la, lo)
        kima = {}
        with open(KIMA_CSV, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                kima[r["id"]] = r
        self.kima = kima
        self.dec = {}
        for r in csv.DictReader(open(DECISIONS, encoding="utf-8"), delimiter="\t"):
            self.dec[bid.norm(r["yiddish"])] = r
        # zibn-shtern reviewed attestations (hand overrides, Maaty relinks…): they win over spelling lookups
        self.att_entry, sp_q = {}, defaultdict(lambda: defaultdict(int))
        self.att_rec = {}
        for r in csv.DictReader(open(ATTEST, encoding="utf-8")):
            if r["link_status"] != "linked" or not r["qid"]:
                continue
            try:
                la, lo = float(r["lat"]), float(r["lon"])
            except ValueError:
                la = lo = None
            self.att_rec[r["qid"]] = (r["label_en"] or r["kima_rom"], r["qid"], r["kima_id"], la, lo)
            n = bid.norm(r["source_value"])
            if r["source_corpus"] == "person":
                self.att_entry[("P-" + r["source_record_id"], n)] = r["qid"]
            sp_q[n][r["qid"]] += 1
        self.att_spelling = {n: next(iter(qs)) for n, qs in sp_q.items() if len(qs) == 1}  # unanimous only
        for sp, ids in HOMONYMS.items():
            for k in ids:
                r = kima[k]
                try:
                    la, lo = float(r["lat"]), float(r["lon"])
                except ValueError:
                    continue
                q = (r["WikiData_Id"] or "").strip()
                self.multi[sp][q or f"kima:{k}"] = (r["primary_rom_full"], q, k, la, lo)

    # ---- one spelling -> candidate records
    def candidates(self, p):
        n = bid.norm(p)
        out = dict(self.multi.get(n, {}))
        if not out and n in bid.SUPPLEMENT:
            q, en, la, lo = bid.SUPPLEMENT[n]
            out[q] = (en, q, "", la, lo)
        d = self.dec.get(n)
        if d and d["decision"] in ("KIMA", "QID"):
            kid = d["kima_id"] if d["decision"] == "KIMA" else ""
            la = lo = None
            if kid and kid in self.kima:
                try:
                    la, lo = float(self.kima[kid]["lat"]), float(self.kima[kid]["lon"])
                except ValueError:
                    pass
            key = d["qid"] or f"kima:{kid}"
            if key not in out:
                out[key] = (d["english"] or d["kima_name"], d["qid"], kid, la, lo)
        return list(out.values())

    def fmt(self, rec):
        en, q, kid, *_ = rec
        ids = " / ".join(x for x in (q, f"Kima {kid}" if kid else "") if x)
        return f"{en} ({ids})" if ids else en

    def link(self, place, evidence="", centroid=None, entry_id=""):
        """-> (label, how). label '' = not linked."""
        p = bid.norm(place)
        if not p:
            return "", "empty"
        q = self.att_entry.get((entry_id, p))
        if q:
            return self.fmt(self.att_rec[q]), "reviewed_same_entry"
        q = self.att_spelling.get(p)
        if q:
            return self.fmt(self.att_rec[q]), "reviewed_spelling"
        parts = [bid.norm(x) for x in re.split(r"[,(;]|\)", place or "") if bid.norm(x)]  # split BEFORE norm (it drops parens)
        head, quals = (parts[0], parts[1:]) if parts else (p, [])
        cands = self.candidates(p) or self.candidates(head)
        if not cands:
            # a bare region / country
            r = region_of(p) or (region_of(head) if not quals else None)
            if r:
                return r[0], "region"
            d = self.dec.get(p)
            if d and d["decision"] == "IDENTIFIED":
                return d["english"], "identified"
            return "", "unlinked"
        # qualifier inside the place string: hard filter
        qreg = [r for r in (region_of(x) for x in quals) if r]
        how = "spelling"
        if qreg:
            keep = [c for c in cands if c[3] is None or all(
                km((c[3], c[4]), rc) <= BIG.get(rn, DEFAULT_R) for rn, rc in qreg)]
            if not keep:
                return "", "qualifier_rejected"
            if len(keep) < len(cands):
                how = "qualifier_chose"
            cands = keep
        if len(cands) > 1:
            # homonym: evidence region, then the person's other stations
            ereg = [r for r in (region_of(w) for w in re.split(r"[\s,;()….]+", evidence or "")) if r]
            ref = None
            if ereg:
                ref, how = ereg[0][1], "evidence_chose"
            elif centroid:
                ref, how = centroid, "itinerary_chose"
            geo = [c for c in cands if c[3] is not None]
            if not ref or not geo:
                return "", "homonym_undecided"
            geo.sort(key=lambda c: km((c[3], c[4]), ref))
            d0 = km((geo[0][3], geo[0][4]), ref)
            d1 = km((geo[1][3], geo[1][4]), ref) if len(geo) > 1 else 1e9
            if how == "itinerary_chose" and d1 < 2 * d0 + 100:  # centroid must clearly prefer one
                return "", "homonym_undecided"
            if how == "evidence_chose" and d0 > 2500:
                return "", "homonym_undecided"
            cands = geo[:1]
        return self.fmt(cands[0]), how

    def coords(self, place):
        c = self.candidates(place)
        if len(c) == 1 and c[0][3] is not None:
            return c[0][3], c[0][4]
        return None
