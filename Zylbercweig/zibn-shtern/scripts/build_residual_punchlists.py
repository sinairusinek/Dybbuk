#!/usr/bin/env python3
"""Turn the resolver's B/C grades + Kima-fuzzy evidence into graded punch lists,
ordered by need-for-review. Adds a heuristic recommendation so review is fast.

  review_1_confirm.tsv   HIGH  — a concrete place QID is proposed; accept/reject
  review_2_identify.tsv  MED   — looks like a real place but no QID found; supply one
  review_3_triage.tsv    LOW   — obscure/OCR/compound/non-toponym; deal with differently
"""
import csv, re, unicodedata
from pathlib import Path
WORK = Path(__file__).resolve().parent.parent / "data" / "working"
K = WORK / "kima"

def rd(p, d="\t"): return list(csv.DictReader(open(p), delimiter=d))

# Kima fuzzy candidates as extra evidence: spelling -> "rom (kima#id)"
kima_ev = {}
for x in rd(K/"residual_kima_matched.csv", ","):
    if x["_match_status"] == "fuzzy" and x["_candidates"]:
        kima_ev[x["label_yi"]] = f"{x['_kima_name_rom']}|cand:{x['_candidates'][:40]}"

PLACE = re.compile(r"city|town|village|region|municipalit|capital|settlement|state|county|"
    r"province|district|commune|locality|oblast|voivod|borough|neighbou?rhood|hamlet|"
    r"country|seat|governorate|island|mountain|river|land\b", re.I)
NONPLACE_DESC = re.compile(r"civil war|TV series|film|football|player|song|album|"
    r"newspaper|hospital|company|band|language|currency", re.I)
# textual signals the spelling itself is not a single toponym
CONJ = re.compile(r"\bאון\b| און ")          # "X and Y"
ADDR = re.compile(r"\d")                       # contains a house number
INSTIT = re.compile(r"האָספּיטאָל|האספיטאל|שול\b|פֿאַראיין|פאראיין|קלוב|טעאַטער|אַסאָסיאיישאָן")

def looks_real_place(yi):
    # multi-word with a known place head, or a single plausible toponym token
    return not (CONJ.search(yi) or INSTIT.search(yi))

def main():
    B = rd(K/"residual_B_review.tsv"); C = rd(K/"residual_C_review.tsv")
    L1, L2, L3 = [], [], []
    for r in B + C:
        yi = r["yiddish"]; occ = int(r["occ"]); qid = r["qid"]; desc = r.get("desc", "")
        lab = r.get("label_en", ""); kev = kima_ev.get(yi, "")
        has_place_qid = qid.startswith("Q") and PLACE.search(desc) and not NONPLACE_DESC.search(desc)
        # recommendation heuristic
        if NONPLACE_DESC.search(desc) or (qid.startswith("Q") and not PLACE.search(desc)):
            rec = "REJECT-cand (non-place)"
        elif INSTIT.search(yi):
            rec = "ROUTE→venue/institution"
        elif CONJ.search(yi) or (ADDR.search(yi) and "place" in r["fields"]):
            rec = "ROUTE→descriptor/compound"
        elif has_place_qid and int(r.get("n_place_cands", "0") or 0) <= 1:
            rec = "LIKELY-ACCEPT"
        elif has_place_qid:
            rec = "CHOOSE among candidates"
        elif kev:
            rec = "CHECK Kima fuzzy"
        else:
            rec = "needs identification"
        row = {"yiddish": yi, "occ": occ, "fields": r["fields"],
               "proposed_qid": qid if qid.startswith("Q") else "",
               "proposed_label": lab, "wikidata_desc": desc[:60],
               "kima_fuzzy": kev, "recommendation": rec}
        if rec in ("LIKELY-ACCEPT", "CHOOSE among candidates", "REJECT-cand (non-place)"):
            L1.append(row)
        elif rec in ("ROUTE→venue/institution", "ROUTE→descriptor/compound"):
            L3.append(row)
        elif looks_real_place(yi) and (kev or occ >= 2):
            L2.append(row)
        else:
            L3.append(row)
    cols = ["yiddish","occ","fields","proposed_qid","proposed_label","wikidata_desc",
            "kima_fuzzy","recommendation"]
    for name, lst in (("review_1_confirm", L1), ("review_2_identify", L2), ("review_3_triage", L3)):
        lst.sort(key=lambda r: (-r["occ"], r["yiddish"]))
        with open(K/f"{name}.tsv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, delimiter="\t"); w.writeheader(); w.writerows(lst)
        print(f"{name}: {len(lst)} rows ({sum(r['occ'] for r in lst)} occ)")

if __name__ == "__main__":
    main()
