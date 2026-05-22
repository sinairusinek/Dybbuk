#!/usr/bin/env python3
"""Arbitrate Fischer UID conflicts (and phonetic_mismatch rows) using Fischer's own coords.

A UID conflict = one Fischer place whose multiple Hebrew spellings resolved to >1 distinct
Kima id. Because the Fischer gazetteer carries its own lat/lon + EngName, the *correct* Kima
place is simply the candidate nearest to Fischer's own coordinates. We compute, per UID, the
great-circle distance from Fischer's coords to every candidate Kima place, pick the nearest
as the resolved id, and report the runner-up gap so a human can sanity-check.

phonetic_mismatch rows are arbitrated the same way: keep the engine's pick only if it is the
nearest Kima candidate to Fischer's coords; otherwise flag it as a likely wrong-city FP and
name the nearer alternative.

Inputs:  fischer_matched.csv (per-row), fischer_matched.by_uid.tsv (conflicts), Kima places CSV.
Outputs: <stem>.conflicts_resolved.tsv  and  <stem>.phonetic_resolved.tsv
"""
import csv, sys, os, math

KIMA_CSV = "/Users/sinairusinek/Documents/GitHub/Kimatch/20250126KimaPlacesCSVx.csv"


def haversine(la1, lo1, la2, lo2):
    R = 6371.0
    p1, p2 = math.radians(la1), math.radians(la2)
    dp = math.radians(la2 - la1)
    dl = math.radians(lo2 - lo1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))


def load_kima():
    coords, rom, heb = {}, {}, {}
    with open(KIMA_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            kid = r["id"].strip()
            rom[kid] = r.get("primary_rom_full", "")
            heb[kid] = r.get("primary_heb_full", "")
            try:
                coords[kid] = (float(r["lat"]), float(r["lon"]))
            except (ValueError, KeyError):
                pass
    return coords, rom, heb


def fnum(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def nearest(cands, flat, flon, coords):
    """Return list of (kid, dist_km) sorted nearest-first; dist None if no kima coords."""
    out = []
    for kid in cands:
        c = coords.get(kid)
        d = haversine(flat, flon, c[0], c[1]) if (c and flat is not None) else None
        out.append((kid, d))
    out.sort(key=lambda t: (t[1] is None, t[1] if t[1] is not None else 0))
    return out


def main(matched_csv):
    stem = os.path.splitext(matched_csv)[0]
    coords, rom, heb = load_kima()
    rows = list(csv.DictReader(open(matched_csv, encoding="utf-8")))

    # --- conflicts: arbitrate per UID ---
    uid_rows = {}
    for r in rows:
        uid_rows.setdefault(r["UID"], []).append(r)

    conf = list(csv.DictReader(open(stem + ".by_uid.tsv", encoding="utf-8"), delimiter="\t"))
    conf = [c for c in conf if c["all_agree"] == "N" and c["distinct_assigned_ids"]]

    res = []
    for c in conf:
        uid = c["UID"]
        grp = uid_rows[uid]
        flat = next((fnum(r.get("Latitude")) for r in grp if fnum(r.get("Latitude")) is not None), None)
        flon = next((fnum(r.get("Longitude")) for r in grp if fnum(r.get("Longitude")) is not None), None)
        cands = [k for k in c["distinct_assigned_ids"].split("|") if k]
        ranked = nearest(cands, flat, flon, coords)
        winner, wd = ranked[0]
        runner, rd = (ranked[1] if len(ranked) > 1 else ("", None))
        # resolved place's distance from Fischer's own coords tells us if the winner is real:
        #   near = a true match (the loser is usually a duplicate Kima record / wrong homograph);
        #   far = neither candidate is the place → genuine no-good-match needing manual search.
        if wd is None:
            quality = "NO_COORD"
        elif wd <= 50:
            quality = "RESOLVED_near"
        elif wd <= 300:
            quality = "RESOLVED_mid"
        else:
            quality = "NO_GOOD_MATCH"
        res.append({
            "UID": uid, "EngClean": c["EngClean"], "resolution_quality": quality,
            "fischer_lat": flat, "fischer_lon": flon,
            "resolved_kima_id": winner, "resolved_rom": rom.get(winner, ""),
            "resolved_dist_km": "" if wd is None else round(wd, 1),
            "runner_up_id": runner, "runner_up_rom": rom.get(runner, ""),
            "runner_up_dist_km": "" if rd is None else round(rd, 1),
            "gap_km": "" if (wd is None or rd is None) else round(rd - wd, 1),
            "all_candidate_ids": c["distinct_assigned_ids"],
        })
    res.sort(key=lambda x: (x["gap_km"] == "", x["gap_km"] if x["gap_km"] != "" else 0))
    cols = list(res[0].keys()) if res else []
    with open(stem + ".conflicts_resolved.tsv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t"); w.writeheader(); w.writerows(res)

    # --- phonetic_mismatch: keep pick only if it is the nearest candidate ---
    pm = [r for r in rows if "phonetic_mismatch" in (r.get("_flags") or "")]
    pres = []
    for r in pm:
        flat, flon = fnum(r.get("Latitude")), fnum(r.get("Longitude"))
        pick = r.get("_kima_id", "")
        cands = [pick] + [k for k in (r.get("_candidates", "") or "").split("|") if k]
        cands = list(dict.fromkeys([k for k in cands if k]))  # dedupe, keep order
        ranked = nearest(cands, flat, flon, coords)
        nearest_id, nd = (ranked[0] if ranked else ("", None))
        pick_d = next((d for k, d in ranked if k == pick), None)
        # Fischer carries the place's true coords, so distance of the pick from those coords
        # is the verdict: a settlement match >300km away is a different place (wrong-city FP).
        if pick_d is None:
            verdict = "NO_COORD"
        elif pick_d <= 50:
            verdict = "KEEP"
        elif pick_d <= 300:
            verdict = "CHECK"
        else:
            verdict = "REJECT"
        pres.append({
            "UID": r["UID"], "HebName": r.get("HebName", ""), "EngClean": r.get("EngClean", ""),
            "engine_pick_id": pick, "engine_pick_rom": rom.get(pick, ""),
            "pick_dist_km": "" if pick_d is None else round(pick_d, 1),
            "verdict": verdict,
            "nearest_id": nearest_id, "nearest_rom": rom.get(nearest_id, ""),
            "nearest_dist_km": "" if nd is None else round(nd, 1),
        })
    pres.sort(key=lambda x: x["verdict"])
    pcols = list(pres[0].keys()) if pres else []
    with open(stem + ".phonetic_resolved.tsv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=pcols, delimiter="\t"); w.writeheader(); w.writerows(pres)

    from collections import Counter
    print(f"conflicts arbitrated: {len(res)} → {stem}.conflicts_resolved.tsv")
    print(f"  (sorted by gap_km; large gap = confident resolution)")
    print(f"phonetic_mismatch rows: {len(pres)} → {stem}.phonetic_resolved.tsv")
    print(f"  verdicts: {dict(Counter(p['verdict'] for p in pres))}")


if __name__ == "__main__":
    main(sys.argv[1])
