#!/usr/bin/env python3
"""Stage-2 donation export for the Fischer→Kima match (variants only).

Builds the set of CONFIRMED Fischer place→Kima links, then emits the HebName spellings Kima
does NOT already carry for those places as new-variant donations.

Confirmed = union of
  - clean grade-A anchors where all spellings of the UID agree (from .by_uid.tsv), and
  - conflict UIDs the coord-arbiter resolved RESOLVED_near / RESOLVED_mid (from .conflicts_resolved.tsv),
minus
  - phonetic_mismatch rows the arbiter marked REJECT (those individual spellings are dropped),
  - NO_GOOD_MATCH conflicts (whole UID dropped — no good Kima place).

A HebName is a donation candidate when its normalized form is NOT already in Kima's
primary_heb + registered variants for that place.

Outputs (in the fischer dir):
  - fischer_donations.tsv   flat, one row per (kima_id, new variant) — for spreadsheet review
  - fischer_donations.json  grouped per Kima place — the hand-off artifact
  - fischer_confirmed_decisions.tsv  one row per confirmed (UID, spelling) → kima_id (provenance,
        also reusable as --prior-resolutions for future runs)
"""
import csv, sys, os, json
from collections import defaultdict
from kimatch.core.normalizers import normalize_name

csv.field_size_limit(sys.maxsize)

KIMA_CSV = "/Users/sinairusinek/Documents/GitHub/Kimatch/20250126KimaPlacesCSVx.csv"
KIMA_VARIANTS = "/Users/sinairusinek/Documents/GitHub/Kimatch/Kima-Variants-20250929.tsv"
SOURCE = "Fischer gazetteer (Expanded-Gaz-TENTATIVE)"


def load_kima_names():
    """{place_id: set(normalized existing names)} from primary_heb/rom + variants."""
    existing = defaultdict(set)
    rom = {}
    with open(KIMA_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            kid = r["id"].strip()
            rom[kid] = r.get("primary_rom_full", "")
            for col in ("primary_heb_full", "primary_rom_full"):
                v = (r.get(col) or "").strip()
                if v and v != "NULL":
                    existing[kid].add(normalize_name(v))
    with open(KIMA_VARIANTS, encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            kid = (r.get("PlaceId") or "").strip()
            v = (r.get("variant") or "").strip()
            if kid and v:
                existing[kid].add(normalize_name(v))
    return existing, rom


def main(matched_csv):
    d = os.path.dirname(matched_csv)
    stem = os.path.splitext(matched_csv)[0]
    rows = list(csv.DictReader(open(matched_csv, encoding="utf-8")))
    by_uid = csv.DictReader(open(stem + ".by_uid.tsv", encoding="utf-8"), delimiter="\t")
    conf_res = list(csv.DictReader(open(stem + ".conflicts_resolved.tsv", encoding="utf-8"), delimiter="\t"))

    # 1. anchor per confirmed UID
    anchor = {}      # uid -> (kima_id, basis)
    for u in by_uid:
        if u["anchor_kima_id"] and u["all_agree"] == "Y" and u["anchor_grade"] == "A_autolink":
            anchor[u["UID"]] = (u["anchor_kima_id"], "grade_A_all_agree")
    drop_uids = set()
    for c in conf_res:
        q = c["resolution_quality"]
        if q in ("RESOLVED_near", "RESOLVED_mid"):
            anchor[c["UID"]] = (c["resolved_kima_id"], "coord_arbitrated_" + q)
        elif q == "NO_GOOD_MATCH":
            drop_uids.add(c["UID"])
    for uid in drop_uids:
        anchor.pop(uid, None)

    # 2. spellings to drop: phonetic REJECT rows (by UID+HebName)
    reject = set()
    pres = csv.DictReader(open(stem + ".phonetic_resolved.tsv", encoding="utf-8"), delimiter="\t")
    for p in pres:
        if p["verdict"] == "REJECT":
            reject.add((p["UID"], p["HebName"]))

    existing, rom = load_kima_names()

    # 3. gather confirmed spellings per UID, emit donations + decisions
    spellings_by_uid = defaultdict(list)  # uid -> list of (HebName, grade, status, sourcing)
    for r in rows:
        uid = r["UID"]
        if uid not in anchor:
            continue
        heb = (r.get("HebName") or "").strip()
        if not heb or (uid, heb) in reject:
            continue
        spellings_by_uid[uid].append((heb, r.get("_grade", ""), r.get("_match_status", ""), r.get("Sourcing", "")))

    donations = []     # flat rows
    grouped = defaultdict(lambda: {"variants": [], "rom": "", "eng": ""})
    decisions = []
    for uid, (kid, basis) in anchor.items():
        seen_norm = set()
        for heb, grade, status, sourcing in spellings_by_uid.get(uid, []):
            decisions.append({"UID": uid, "name": heb, "kima_id": kid, "basis": basis,
                              "grade": grade, "match_status": status})
            n = normalize_name(heb)
            if n in existing.get(kid, set()) or n in seen_norm:
                continue              # Kima already has it (or dup within UID)
            seen_norm.add(n)
            donations.append({
                "kima_id": kid, "kima_rom": rom.get(kid, ""), "variant": heb,
                "source": SOURCE, "fischer_uid": uid, "match_basis": basis,
                "match_status": status, "grade": grade, "sourcing": sourcing,
            })
            g = grouped[kid]; g["rom"] = rom.get(kid, "")
            g["variants"].append({"variant": heb, "source": SOURCE, "fischer_uid": uid,
                                   "provenance": {"basis": basis, "match_status": status, "grade": grade,
                                                  "sourcing": sourcing}})

    dcols = ["kima_id", "kima_rom", "variant", "source", "fischer_uid", "match_basis",
             "match_status", "grade", "sourcing"]
    with open(os.path.join(d, "fischer_donations.tsv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=dcols, delimiter="\t"); w.writeheader(); w.writerows(donations)
    with open(os.path.join(d, "fischer_donations.json"), "w", encoding="utf-8") as f:
        json.dump({"source": SOURCE, "contribution_type": "new_variant",
                   "places": [{"kima_id": k, "primary_rom": v["rom"], "new_variants": v["variants"]}
                              for k, v in sorted(grouped.items())]}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(d, "fischer_confirmed_decisions.tsv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["UID", "name", "kima_id", "basis", "grade", "match_status"],
                           delimiter="\t"); w.writeheader(); w.writerows(decisions)

    print(f"confirmed places (UIDs): {len(anchor)}  | dropped NO_GOOD_MATCH UIDs: {len(drop_uids)}  | dropped REJECT spellings: {len(reject)}")
    print(f"confirmed (UID,spelling) decisions: {len(decisions)}")
    print(f"NEW variant donations (Kima missing): {len(donations)}  across {len(grouped)} Kima places")
    print(f"  → fischer_donations.tsv / .json / fischer_confirmed_decisions.tsv")


if __name__ == "__main__":
    main(sys.argv[1])
