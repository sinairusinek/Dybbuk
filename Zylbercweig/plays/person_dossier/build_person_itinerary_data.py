#!/usr/bin/env python3.11
"""Resolve a person dossier's extracted stations and cross-check them against
the KG's own dated facts.

A local copy of itinerary/build_itinerary_data.py: that script hardcodes the
pilot's paths and carries an Istanbul-specific SUPPLEMENT, so the dossier track
keeps its own resolver rather than bending the pilot's.

Adds `kg_anchors`: every dated/placeable KG fact on the ego node, with the
org's located_in place attached, and a verdict against the extraction —
  match           KG fact and an extracted station agree on place and time
  conflict        they overlap in time but disagree on place
  kg_only         the KG has it, the extraction missed it
  extraction_only a dated station with no KG fact behind it

Usage: python3.11 build_person_itinerary_data.py --config rumshinsky.json
"""
import argparse, csv, json, re, unicodedata
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
GAZ = HERE.parent.parent / "zibn-shtern" / "data" / "working" / "toponyms_gazetteer.csv"
KG = HERE.parent / "kg"

csv.field_size_limit(10**8)
POINTS = re.compile(r"[֑-ׇ]")

# region/country fallbacks (centroids; drawn differently on the map)
REGIONS = {
    "רוסלאנד": ("Russia", 55.75, 37.62), "ליטע": ("Lithuania", 54.9, 23.9),
    "פוילן": ("Poland", 52.23, 21.01), "אוקראינע": ("Ukraine", 49.0, 32.0),
    "אמעריקע": ("America (USA)", 40.71, -74.0), "ענגלאנד": ("England", 51.5, -0.12),
    "דייטשלאנד": ("Germany", 52.52, 13.4), "עסטרייך": ("Austria", 48.2, 16.37),
    "פראנקרייך": ("France", 48.85, 2.35), "אייראפע": ("Europe", 48.0, 15.0),
    "מערב-אייראפע": ("Western Europe", 48.85, 2.35),
    "ווייסרוסלאנד": ("Belarus", 53.9, 27.57), "גאליציע": ("Galicia", 49.55, 24.0),
    "רומעניע": ("Romania", 45.94, 24.97), "ארץ-ישראל": ("Palestine/Land of Israel", 32.08, 34.78),
    "ראטנפארבאנד": ("Soviet Union", 55.75, 37.62), "קאנאדע": ("Canada", 43.65, -79.38),
}

# Curated place decisions for this dossier: settlements missing from the
# gazetteer, PLUS overrides where the gazetteer is ambiguous. Checked BEFORE the
# gazetteer, so a curated call wins.
#   באריסאוו is listed in the gazetteer both as Barysaw (Q19313, correct) and as
#   a variant of Babruysk (Q207294) — a contaminated variant list. The entry's
#   "באָריסאָווס רוסישער אָפּערע" is Borisov/Barysaw.
SUPPLEMENT = {
    "באריסאוו": ("Q19313", "Barysaw (Borisov)", 54.226, 28.505),
    "לאנדאן": ("Q84", "London", 51.507, -0.128),
    "באסטאן": ("Q100", "Boston", 42.360, -71.059),
    "דווינסק": ("Q134301", "Daugavpils (Dvinsk)", 55.875, 26.536),
    "יעליסאוועטגראד": ("Q157725", "Kropyvnytskyi (Yelisavetgrad)", 48.507, 32.262),
    "גראדנע": ("Q170577", "Grodno", 53.677, 23.829),
}


def norm(s):
    s = unicodedata.normalize("NFC", s or "")
    s = POINTS.sub("", s)
    s = re.sub(r"[?!„“\"'()\[\]]", "", s)
    s = s.replace("־", "-").replace("–", "-")
    return re.sub(r"\s+", " ", s).strip()


def load_gazetteer():
    lut = {}
    with open(GAZ, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if not row.get("lat"):
                continue
            rec = (row["qid"], row["label_en"], float(row["lat"]), float(row["lon"]))
            for v in [row.get("label_yi", "")] + (row.get("variants", "") or "").split(";"):
                v = norm(v)
                if v and v not in lut:
                    lut[v] = rec
            en = norm(row.get("label_en", "")).lower()
            if en and en not in lut:
                lut[en] = rec
    with open(KG / "nodes.tsv", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            if row["node_type"] != "place":
                continue
            try:
                attrs = json.loads(row["attrs"] or "{}")
                lat, lon = float(attrs["lat"]), float(attrs["lon"])
            except Exception:
                continue
            rec = (row["ext_ref_id"], row["label_english"] or row["label_yiddish"], lat, lon)
            for v in (row["label_yiddish"], row["label_english"]):
                v = norm(v)
                if v and v not in lut:
                    lut[v] = rec
                if v and v.lower() not in lut:
                    lut[v.lower()] = rec
    return lut


def resolve(place, lut):
    """-> (status, qid_or_region, label_en, lat, lon)"""
    p = norm(place)
    if not p:
        return ("empty", "", "", None, None)
    cands = [p, p.lower()]
    if "," in p:
        cands.append(p.split(",")[0].strip())
    for c in cands:
        if c in SUPPLEMENT:
            return ("settlement", *SUPPLEMENT[c])
        if c in lut:
            q, en, lat, lon = lut[c]
            return ("settlement", q, en, lat, lon)
    for c in cands:
        if c in REGIONS:
            en, lat, lon = REGIONS[c]
            return ("region", c, en, lat, lon)
    parts = re.split(r"[,;]| און | איבער ", p)
    for part in (norm(x) for x in parts):
        if part in SUPPLEMENT:
            return ("settlement_first_of_list", *SUPPLEMENT[part])
        if part in lut:
            q, en, lat, lon = lut[part]
            return ("settlement_first_of_list", q, en, lat, lon)
        if part in REGIONS:
            en, lat, lon = REGIONS[part]
            return ("region_first_of_list", part, en, lat, lon)
    return ("unresolved", "", "", None, None)


def year_of(d):
    m = re.match(r"(\d{4})", d or "")
    return int(m.group(1)) if m else None


def interpolate(stations):
    idx = [(i, year_of(s.get("date_start")) or year_of(s.get("date_end")))
           for i, s in enumerate(stations)]
    anchors = [(i, y) for i, y in idx if y]
    for i, s in enumerate(stations):
        y = idx[i][1]
        if y:
            s["t_start"] = year_of(s.get("date_start")) or y
            s["t_end"] = year_of(s.get("date_end")) or s["t_start"]
            s["t_inferred"] = False
            continue
        prev = max((a for a in anchors if a[0] < i), key=lambda a: a[0], default=None)
        nxt = min((a for a in anchors if a[0] > i), key=lambda a: a[0], default=None)
        s["t_start"] = prev[1] if prev else (nxt[1] if nxt else None)
        s["t_end"] = nxt[1] if nxt else (prev[1] if prev else None)
        s["t_inferred"] = True
    return stations


# ---------------- KG anchors ----------------

DISCOURSE = {"wrote_about", "associated_with", "mention"}


def build_anchors(cfg, lut):
    """Dated/placeable KG facts on the ego node, with org located_in places."""
    ego = cfg["node_id"]
    nodes = {}
    with open(KG / "nodes.tsv", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            nodes[r["node_id"]] = r
    edges = list(csv.DictReader(open(KG / "edges.tsv", encoding="utf-8"), delimiter="\t"))

    org_place = {}
    for e in edges:
        if e["edge_type"] == "located_in" and e["target_id"].startswith("place:"):
            org_place.setdefault(e["source_id"], e["target_id"])

    def place_of(node_id):
        """-> (yiddish label, english label, qid, lat, lon) for a node's located_in."""
        pid = org_place.get(node_id)
        if not pid:
            return ("", "", "", None, None)
        p = nodes.get(pid, {})
        yi = POINTS.sub("", p.get("label_yiddish", "") or "")
        en_lbl = p.get("label_english", "") or ""
        _st, qid, en, lat, lon = resolve(yi or en_lbl, lut)
        if not lat and (p.get("ext_ref_id") or "").startswith("Q"):
            # place node carries its own coords in attrs
            try:
                attrs = json.loads(p.get("attrs") or "{}")
                lat, lon = float(attrs["lat"]), float(attrs["lon"])
                qid = qid or p["ext_ref_id"]
            except Exception:
                pass
        return (yi, en or en_lbl, qid, lat, lon)

    anchors = []
    for e in edges:
        if e["source_id"] != ego and e["target_id"] != ego:
            continue
        if e["edge_type"] in DISCOURSE:
            continue
        other = e["target_id"] if e["source_id"] == ego else e["source_id"]
        n = nodes.get(other, {})
        pyi, pen, qid, lat, lon = place_of(other)
        y0, y1 = year_of(e["date_start"]), year_of(e["date_end"])
        if not (y0 or y1 or lat):
            continue
        anchors.append({
            "edge_id": e["edge_id"], "type": e["edge_type"],
            "role": POINTS.sub("", e.get("role_detail", "") or ""),
            "other_id": other,
            "other_yi": POINTS.sub("", n.get("label_yiddish", "") or ""),
            "other_en": n.get("label_english", "") or "",
            "other_type": n.get("node_type", ""),
            "place_yi": pyi, "place_en": pen, "qid": qid, "lat": lat, "lon": lon,
            "y0": y0, "y1": y1 or y0,
            "ev": POINTS.sub("", e.get("evidence_sentence", "") or "")[:300],
        })
    anchors.sort(key=lambda a: (a["y0"] or 9999, a["edge_id"]))
    return anchors


def judge(anchors, stops):
    """Assign each anchor a verdict against the extracted stations."""
    dated = [s for s in stops if s.get("t_start") and not s.get("t_inferred")]
    for a in anchors:
        if not a["y0"]:
            # placeable but undated: match if any station ever sits at that place
            same = [s for s in stops if a["qid"] and s.get("qid") == a["qid"]]
            a["verdict"] = "match" if same else "kg_only"
            a["matched_seq"] = [s["seq"] for s in same][:3]
            continue
        y0, y1 = a["y0"], a["y1"]
        overlap = [s for s in dated
                   if s["t_start"] <= (y1 or y0) and (s["t_end"] or s["t_start"]) >= y0]
        a["matched_seq"] = [s["seq"] for s in overlap]
        if not overlap:
            a["verdict"] = "kg_only"
        elif not a["qid"]:
            # no place on the KG side (e.g. a play): time overlap is all we can check
            a["verdict"] = "match"
        elif any(s.get("qid") == a["qid"] for s in overlap):
            a["verdict"] = "match"
        else:
            a["verdict"] = "conflict"
            a["conflict_with"] = [
                {"seq": s["seq"], "place": s["place"], "en": s["label_en"],
                 "t0": s["t_start"], "t1": s["t_end"]} for s in overlap]
    # stations no anchor speaks to
    claimed = {q for a in anchors for q in a.get("matched_seq", [])}
    for s in stops:
        s["kg_backed"] = s["seq"] in claimed
    return anchors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    cfg = json.loads((HERE / args.config).read_text(encoding="utf-8"))
    slug = cfg["slug"]
    drafts = HERE / f"{slug}_stations.jsonl"
    out_json = HERE / f"{slug}_itinerary.json"
    out_tsv = HERE / f"{slug}_stations_resolved.tsv"

    lut = load_gazetteer()
    print(f"gazetteer lookup: {len(lut):,} variant keys")

    itineraries, rows = [], []
    res_counts, n_ev = Counter(), 0
    entry_stops = []
    for line in open(drafts, encoding="utf-8"):
        r = json.loads(line)
        if "error" in r or not r.get("stations"):
            print("skipping", r.get("person_id"), r.get("error", "no stations"))
            continue
        by_subj = {}
        for s in r["stations"]:
            by_subj.setdefault(s.get("subject", "entry"), []).append(s)
        for subj, sts in by_subj.items():
            sts = interpolate(sorted(sts, key=lambda s: s.get("seq", 0)))
            stops = []
            for s in sts:
                status, qid, en, lat, lon = resolve(s.get("place", ""), lut)
                res_counts[status] += 1
                n_ev += len(s.get("events") or [])
                stop = {
                    "seq": s.get("seq"), "place": s.get("place", ""),
                    "place_kind": s.get("place_kind", ""), "org": s.get("org", ""),
                    "role": s.get("role", ""), "verb": s.get("verb_class", ""),
                    "date_start": s.get("date_start", ""), "date_end": s.get("date_end", ""),
                    "certainty": s.get("date_certainty", ""),
                    "t_start": s.get("t_start"), "t_end": s.get("t_end"),
                    "t_inferred": s.get("t_inferred"),
                    "res_status": status, "qid": qid, "label_en": en,
                    "lat": lat, "lon": lon,
                    "events": s.get("events") or [],
                    "evidence": s.get("evidence_quote", ""),
                }
                stops.append(stop)
                rows.append({"person_id": r["person_id"], "heading": r["heading"],
                             "subject": subj,
                             **{k: (json.dumps(v, ensure_ascii=False) if k == "events" else v)
                                for k, v in stop.items()}})
            itineraries.append({
                "person_id": r["person_id"], "heading": r["heading"],
                "volume": r["volume"], "subject": subj,
                "is_entry_subject": subj == "entry", "stations": stops})
            if subj == "entry":
                entry_stops = stops

    anchors = judge(build_anchors(cfg, lut), entry_stops)
    verdicts = Counter(a["verdict"] for a in anchors)
    extraction_only = [s["seq"] for s in entry_stops
                       if not s["kg_backed"] and s["t_start"] and not s["t_inferred"]]

    out = {"slug": slug, "person_id": cfg["entry_person_id"], "node_id": cfg["node_id"],
           "name": cfg["display_name"], "name_yi": cfg["display_name_yi"],
           "life": cfg["life"], "timeline": cfg["timeline"], "map": cfg["map"],
           "itineraries": itineraries, "kg_anchors": anchors,
           "extraction_only_seq": extraction_only}
    out_json.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")

    with open(out_tsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter="\t")
        w.writeheader(); w.writerows(rows)

    print(f"itineraries: {len(itineraries)}  stations: {sum(res_counts.values())}  events: {n_ev}")
    print("resolution:", dict(res_counts.most_common()))
    unres = Counter(norm(r["place"]) for r in rows if r["res_status"] == "unresolved")
    print("top unresolved:", unres.most_common(25) or "none — all places resolved")
    print(f"kg anchors: {len(anchors)}  verdicts: {dict(verdicts.most_common())}")
    for a in anchors:
        if a["verdict"] == "conflict":
            print(f"  CONFLICT {a['edge_id']} {a['type']} {a['other_yi'][:28]} "
                  f"@{a['place_en']} {a['y0']}-{a['y1']} vs "
                  f"{[(c['en'], c['t0']) for c in a['conflict_with']]}")
    print(f"extraction-only dated stations: {extraction_only}")
    print(f"wrote {out_json.name} + {out_tsv.name}")


if __name__ == "__main__":
    main()
