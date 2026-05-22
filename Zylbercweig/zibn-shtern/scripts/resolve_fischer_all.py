#!/usr/bin/env python3
"""Unified coordinate-arbitrated resolution of the whole Fischer→Kima match.

Fischer carries each place's true lat/lon, so for EVERY input row the correct Kima place is
the candidate nearest those coordinates. This single pass therefore does three jobs at once:

  - recovers fuzzy/name_ambiguous rows (pick the nearest of their _candidates),
  - geo-verifies name_exact/exact_id rows (a far-away exact-spelling hit is a homograph FP —
    the ~9% cross-border-homograph problem; Wayne NJ for Vienna, Apt France for Opatów),
  - assigns a distance-based verdict so downstream donation export only uses sound links.

Candidate pool per row = the engine's chosen _kima_id (if any) ∪ _candidates. Verdict by the
resolved place's distance from Fischer's coords: KEEP ≤50km, REVIEW 50–300km, REJECT >300km,
NO_COORD / NO_CANDIDATE otherwise.

Output: fischer_resolved.tsv (one row per input spelling, resolution applied).
"""
import csv, sys, os, math, re, unicodedata

KIMA_CSV = "/Users/sinairusinek/Documents/GitHub/Kimatch/20250126KimaPlacesCSVx.csv"
csv.field_size_limit(sys.maxsize)
# Distance verdict thresholds. A settlement match >300km away is a different place. But a large
# entity (country/region) legitimately sits far from its Kima point (polygon centroid), so when
# the Kima name EQUALS Fischer's own EngName we treat that as independent identity confirmation
# and allow up to NAME_MATCH_KM — capped so transcontinental homographs (Sydney NS vs Sydney AU)
# are still rejected.
KEEP_KM, REVIEW_KM, NAME_MATCH_KM = 50.0, 300.0, 1500.0


def _norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", s.casefold())


def _core(rom):
    return re.split(r"[(,]", rom or "")[0].strip()


def haversine(la1, lo1, la2, lo2):
    R = 6371.0
    dp, dl = math.radians(la2-la1), math.radians(lo2-lo1)
    a = math.sin(dp/2)**2 + math.cos(math.radians(la1))*math.cos(math.radians(la2))*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))


def fnum(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def load_kima():
    coords, rom = {}, {}
    with open(KIMA_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            kid = r["id"].strip()
            rom[kid] = r.get("primary_rom_full", "")
            la, lo = fnum(r.get("lat")), fnum(r.get("lon"))
            if la is not None and lo is not None:
                coords[kid] = (la, lo)
    return coords, rom


def main(matched_csv):
    d = os.path.dirname(matched_csv)
    coords, rom = load_kima()
    rows = list(csv.DictReader(open(matched_csv, encoding="utf-8")))

    out = []
    for r in rows:
        flat, flon = fnum(r.get("Latitude")), fnum(r.get("Longitude"))
        pool = []
        if r.get("_kima_id"):
            pool.append(r["_kima_id"])
        pool += [c for c in (r.get("_candidates") or "").split("|") if c.strip()]
        pool = list(dict.fromkeys(pool))  # dedupe, keep order

        ranked = []
        for kid in pool:
            c = coords.get(kid)
            dist = haversine(flat, flon, c[0], c[1]) if (c and flat is not None) else None
            ranked.append((kid, dist))
        # nearest first; rows with a coord beat rows without
        ranked.sort(key=lambda t: (t[1] is None, t[1] if t[1] is not None else 0.0))

        if not pool:
            resolved, dist, verdict = "", None, "NO_CANDIDATE"
        else:
            resolved, dist = ranked[0]
            name_match = bool(resolved) and _norm(_core(rom.get(resolved, ""))) == _norm(r.get("EngClean", "")) \
                and _norm(r.get("EngClean", "")) != ""
            if flat is None or dist is None:
                verdict = "NO_COORD"
            elif dist <= KEEP_KM:
                verdict = "KEEP"
            elif name_match and dist <= NAME_MATCH_KM:
                verdict = "KEEP"          # large entity confirmed by exact name (polygon centroid)
            elif dist <= REVIEW_KM:
                verdict = "REVIEW"
            else:
                verdict = "REJECT"

        engine_pick = r.get("_kima_id", "")
        if not resolved:
            method = "none"
        elif resolved == engine_pick and r.get("_match_status") in ("name_exact", "exact_id"):
            method = "exact_geoverified"
        elif resolved == engine_pick:
            method = "engine_pick_geoverified"
        else:
            method = "coord_rearbitrated"     # nearest candidate differs from engine pick

        out.append({
            "UID": r["UID"], "HebName": r.get("HebName", ""), "EngClean": r.get("EngClean", ""),
            "orig_status": r.get("_match_status", ""), "orig_grade": r.get("_grade", ""),
            "engine_pick_id": engine_pick,
            "resolved_kima_id": resolved, "resolved_rom": rom.get(resolved, ""),
            "dist_km": "" if dist is None else round(dist, 1),
            "method": method, "verdict": verdict,
            "Latitude": r.get("Latitude", ""), "Longitude": r.get("Longitude", ""),
            "Sourcing": r.get("Sourcing", ""),
            "KaganID": r.get("KaganID", ""), "JGenID": r.get("JGenID", ""),
            "USBGN": r.get("USBGN", ""), "YS_id": r.get("YS_id", ""),
            "candidates": r.get("_candidates", ""),
        })

    cols = list(out[0].keys())
    outpath = os.path.join(d, "fischer_resolved.tsv")
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t"); w.writeheader(); w.writerows(out)

    from collections import Counter
    print(f"rows: {len(out)} → {outpath}")
    print("verdict:", dict(Counter(x["verdict"] for x in out)))
    print("method :", dict(Counter(x["method"] for x in out)))
    kept = [x for x in out if x["verdict"] == "KEEP"]
    print(f"KEEP rows: {len(kept)} | distinct places: {len({x['resolved_kima_id'] for x in kept})} | "
          f"distinct UIDs anchored: {len({x['UID'] for x in kept})}")
    rearb = [x for x in out if x["method"] == "coord_rearbitrated" and x["verdict"] == "KEEP"]
    print(f"  recovered/corrected by coord re-arbitration (KEEP, pick≠engine): {len(rearb)}")


if __name__ == "__main__":
    main(sys.argv[1])
