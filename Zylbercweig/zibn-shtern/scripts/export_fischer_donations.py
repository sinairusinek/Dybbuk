#!/usr/bin/env python3
"""Stage-2 donation export for the Fischer→Kima match.

Consumes fischer_resolved.tsv (the unified coord-arbitrated resolution) and emits, for every
CONFIRMED link (verdict == KEEP), three donation streams:

  1. NEW VARIANTS — HebName spellings Kima does not already carry for the place.
  2. EXTERNAL IDs — Fischer's KaganID / JGenID / USBGN / YS_id. The live Kima API (checked
     2026-05-22) exposes only MAZAL/NAF/VIAF/GeoNames/WikiData, so these are *new external-id
     types* Kima does not yet track — emitted as proposals, flagged for the Kima team to decide.
  3. CONFIRMED DECISIONS — one row per confirmed (UID, spelling)→kima_id (provenance; reusable
     as --prior-resolutions).

Per UID the anchor place = the most common KEEP resolution; UIDs whose KEEP rows disagree on the
place are noted (anchor = majority, but flagged) — these are rare with ground-truth coords.

Outputs (fischer dir): fischer_donations.tsv / .json, fischer_external_id_donations.tsv,
fischer_confirmed_decisions.tsv.
"""
import csv, sys, os, json
from collections import defaultdict, Counter
from kimatch.core.normalizers import normalize_name

csv.field_size_limit(sys.maxsize)
KIMA_CSV = "/Users/sinairusinek/Documents/GitHub/Kimatch/20250126KimaPlacesCSVx.csv"
KIMA_VARIANTS = "/Users/sinairusinek/Documents/GitHub/Kimatch/Kima-Variants-20250929.tsv"
SOURCE = "Fischer gazetteer (Expanded-Gaz-TENTATIVE)"
EXT_TYPES = [("KaganID", "kagan_id"), ("JGenID", "jewishgen_id"),
             ("USBGN", "us_bgn_id"), ("YS_id", "ys_id")]
EXT_NOTE = ("Kima does not currently track this id-type (live API 2026-05-22 exposes only "
            "MAZAL/NAF/VIAF/GeoNames/WikiData) — proposed new external-id type")


def load_kima_names():
    existing, rom = defaultdict(set), {}
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
            kid, v = (r.get("PlaceId") or "").strip(), (r.get("variant") or "").strip()
            if kid and v:
                existing[kid].add(normalize_name(v))
    return existing, rom


def main(resolved_tsv):
    d = os.path.dirname(resolved_tsv)
    rows = list(csv.DictReader(open(resolved_tsv, encoding="utf-8"), delimiter="\t"))
    keep = [r for r in rows if r["verdict"] == "KEEP" and r["resolved_kima_id"]]
    existing, rom = load_kima_names()

    # per-UID anchor = majority KEEP resolution
    uid_keep = defaultdict(list)
    for r in keep:
        uid_keep[r["UID"]].append(r)
    uid_anchor, uid_split = {}, {}
    for uid, grp in uid_keep.items():
        c = Counter(r["resolved_kima_id"] for r in grp)
        uid_anchor[uid] = c.most_common(1)[0][0]
        if len(c) > 1:
            uid_split[uid] = dict(c)

    # 1. variant donations + decisions (only spellings whose own row is KEEP and matches the anchor)
    donations, decisions = [], []
    grouped = defaultdict(lambda: {"rom": "", "variants": []})
    seen_per_place = defaultdict(set)
    for r in keep:
        uid, kid = r["UID"], uid_anchor[r["UID"]]
        if r["resolved_kima_id"] != kid:
            continue                       # spelling disagrees with its UID's majority anchor
        heb = (r.get("HebName") or "").strip()
        decisions.append({"UID": uid, "name": heb, "kima_id": kid,
                          "method": r["method"], "orig_status": r["orig_status"],
                          "dist_km": r["dist_km"]})
        if not heb:
            continue
        n = normalize_name(heb)
        if n in existing.get(kid, set()) or n in seen_per_place[kid]:
            continue
        seen_per_place[kid].add(n)
        rec = {"kima_id": kid, "kima_rom": rom.get(kid, ""), "variant": heb, "source": SOURCE,
               "fischer_uid": uid, "method": r["method"], "orig_status": r["orig_status"],
               "dist_km": r["dist_km"], "sourcing": r.get("Sourcing", "")}
        donations.append(rec)
        g = grouped[kid]; g["rom"] = rom.get(kid, "")
        g["variants"].append({"variant": heb, "source": SOURCE, "fischer_uid": uid,
                              "provenance": {"method": r["method"], "orig_status": r["orig_status"],
                                             "dist_km": r["dist_km"], "sourcing": r.get("Sourcing", "")}})

    # 2. external-id donations: per confirmed place, collect distinct ids across its UIDs
    place_ids = defaultdict(lambda: defaultdict(set))   # kima_id -> id_type -> {(value, uid)}
    for uid, kid in uid_anchor.items():
        sample = uid_keep[uid][0]
        for col, idtype in EXT_TYPES:
            v = (sample.get(col) or "").strip()
            if v:
                place_ids[kid][idtype].add((v, uid))
    ext_rows = []
    for kid, types in sorted(place_ids.items()):
        for idtype, vals in types.items():
            for v, uid in sorted(vals):
                note = EXT_NOTE
                if idtype == "us_bgn_id" and v.lstrip().startswith("-"):
                    note += " | WARNING: negative value — likely sign/encoding artifact in Fischer source, verify before use"
                ext_rows.append({"kima_id": kid, "kima_rom": rom.get(kid, ""),
                                 "id_type": idtype, "id_value": v, "fischer_uid": uid,
                                 "source": SOURCE, "note": note})

    # write
    def w(path, cols, recs, delim="\t"):
        with open(os.path.join(d, path), "w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=cols, delimiter=delim); wr.writeheader(); wr.writerows(recs)

    w("fischer_donations.tsv",
      ["kima_id", "kima_rom", "variant", "source", "fischer_uid", "method", "orig_status", "dist_km", "sourcing"],
      donations)
    w("fischer_external_id_donations.tsv",
      ["kima_id", "kima_rom", "id_type", "id_value", "fischer_uid", "source", "note"], ext_rows)
    w("fischer_confirmed_decisions.tsv",
      ["UID", "name", "kima_id", "method", "orig_status", "dist_km"], decisions)
    with open(os.path.join(d, "fischer_donations.json"), "w", encoding="utf-8") as f:
        json.dump({"source": SOURCE, "contribution_type": "new_variant",
                   "places": [{"kima_id": k, "primary_rom": v["rom"], "new_variants": v["variants"]}
                              for k, v in sorted(grouped.items())]}, f, ensure_ascii=False, indent=2)

    print(f"confirmed KEEP rows: {len(keep)} | confirmed places: {len(set(uid_anchor.values()))} | UIDs: {len(uid_anchor)}")
    print(f"  UIDs whose KEEP rows disagree on place (anchor=majority): {len(uid_split)}")
    print(f"NEW variant donations: {len(donations)} across {len(grouped)} places")
    print(f"NEW external-id donations: {len(ext_rows)}  by type: {dict(Counter(r['id_type'] for r in ext_rows))}")
    print(f"  → fischer_donations.tsv/.json, fischer_external_id_donations.tsv, fischer_confirmed_decisions.tsv")


if __name__ == "__main__":
    main(sys.argv[1])
