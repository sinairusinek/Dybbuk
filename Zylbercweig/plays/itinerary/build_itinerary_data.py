#!/usr/bin/env python3.11
"""Post-process itinerary_drafts.jsonl:
  1. resolve station places against the unified toponyms gazetteer (+ KG place nodes)
  2. interpolate date ranges for undated stations from narrative order
  3. emit stations_resolved.tsv (review) and istanbul_itineraries.json (viz feed)
"""
import csv, json, re, unicodedata
from pathlib import Path

HERE = Path(__file__).resolve().parent
GAZ = HERE.parent.parent / "zibn-shtern" / "data" / "working" / "toponyms_gazetteer.csv"
KG_NODES = HERE.parent / "kg" / "nodes.tsv"
DRAFTS = HERE / "itinerary_drafts.jsonl"
OUT_TSV = HERE / "stations_resolved.tsv"
OUT_JSON = HERE / "istanbul_itineraries.json"

csv.field_size_limit(10**8)
POINTS = re.compile(r"[֑-ׇ]")

# region/country fallbacks (centroids, drawn differently on the map)
REGIONS = {
    "רומעניע": ("Romania", 45.94, 24.97), "רוסלאנד": ("Russia", 55.75, 37.62),
    "גאליציע": ("Galicia", 49.55, 24.0), "בוקאווינע": ("Bukovina", 47.9, 25.9),
    "בעסאראביע": ("Bessarabia", 47.0, 28.85), "פוילן": ("Poland", 52.23, 21.01),
    "ליטע": ("Lithuania", 54.9, 23.9), "אוקראינע": ("Ukraine", 49.0, 32.0),
    "טערקיי": ("Turkey", 39.9, 32.85), "בולגאריע": ("Bulgaria", 42.7, 25.5),
    "עגיפטן": ("Egypt", 30.05, 31.23), "אמעריקע": ("America (USA)", 40.71, -74.0),
    "ענגלאנד": ("England", 51.5, -0.12), "דייטשלאנד": ("Germany", 52.52, 13.4),
    "עסטרייך": ("Austria", 48.2, 16.37), "פראנקרייך": ("France", 48.85, 2.35),
    "אונגארן": ("Hungary", 47.5, 19.05), "קרים": ("Crimea", 45.0, 34.1),
    "קאווקאז": ("Caucasus", 43.0, 44.0), "דרום-אפריקע": ("South Africa", -26.2, 28.05),
    "דרום-רוסלאנד": ("Southern Russia", 47.0, 39.7), "גריכנלאנד": ("Greece", 37.98, 23.73),
    "איטאליע": ("Italy", 41.9, 12.5), "שפאניע": ("Spain", 40.42, -3.7),
    "ארץ-ישראל": ("Palestine/Land of Israel", 32.08, 34.78), "פאלעסטינע": ("Palestine", 32.08, 34.78),
    "בעלגיע": ("Belgium", 50.85, 4.35), "קאנאדע": ("Canada", 43.65, -79.38),
    "אריענט": ("the Orient", 38.0, 30.0), "ראטנפארבאנד": ("Soviet Union", 55.75, 37.62),
    "אייראפע": ("Europe", 48.0, 15.0), "מערב-אייראפע": ("Western Europe", 48.85, 2.35), "מאראקא": ("Morocco", 33.57, -7.59),
    "טשיליי": ("Chile", -33.45, -70.67), "פערו": ("Peru", -12.05, -77.04),
    "ווייסרוסלאנד": ("Belarus", 53.9, 27.57), "וואלין": ("Volhynia", 50.75, 25.33),
    "מזרח-גאליציע": ("Eastern Galicia", 49.84, 24.03),
}

# settlements missing from the gazetteer (manual, pilot-scope)
SUPPLEMENT = {
    "זבאראזש": ("Q156095", "Zbarazh", 49.663, 25.775),
    "זבאראש": ("Q156095", "Zbarazh", 49.663, 25.775),
    "באקאו": ("Q173203", "Bacău", 46.567, 26.914),
    "טולטשא": ("Q16898", "Tulcea", 45.19, 28.80),
    "סאלאניקי": ("Q17151", "Thessaloniki", 40.64, 22.94),
    "ליבוי": ("Q168997", "Liepāja", 56.511, 21.014),
    "ראמאן": ("Q212469", "Roman", 46.93, 26.92),
    "הארנאסטאיפאליע": ("Q4143502", "Hornostaipil", 51.05, 30.15),
    "בראאילא": ("Q16897", "Brăila", 45.269, 27.957),
    "באטום": ("Q25475", "Batumi", 41.646, 41.641),
    "סוטשאווא": ("Q170357", "Suceava", 47.653, 26.256),
    "בראנקס": ("Q18426", "Bronx", 40.837, -73.865),
    "דובעלן": ("Q3436919", "Dubulti (Jūrmala)", 56.968, 23.775),
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
    with open(KG_NODES, encoding="utf-8") as f:
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
    if "," in p:  # "יאסי, רומעניע" -> try the city part
        cands += [p.split(",")[0].strip()]
    for c in cands:
        if c in lut:
            q, en, lat, lon = lut[c]
            return ("settlement", q, en, lat, lon)
        if c in SUPPLEMENT:
            return ("settlement", *SUPPLEMENT[c])
    for c in cands:
        if c in REGIONS:
            en, lat, lon = REGIONS[c]
            return ("region", c, en, lat, lon)
    # multi-place tour strings: resolve the first resolvable token
    parts = re.split(r"[,;]| און | איבער ", p)
    for part in (norm(x) for x in parts):
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
    """Fill inferred_start/inferred_end (years) for undated stations from neighbors."""
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


def main():
    lut = load_gazetteer()
    print(f"gazetteer lookup: {len(lut):,} variant keys")

    itineraries, rows = [], []
    counts = {"stations": 0, "events": 0}
    res_counts = {}
    with open(DRAFTS, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if "error" in r or not r.get("stations"):
                continue
            # split by subject: entry-subject itinerary + third-person fragments
            by_subj = {}
            for s in r["stations"]:
                by_subj.setdefault(s.get("subject", "entry"), []).append(s)
            for subj, sts in by_subj.items():
                sts = interpolate(sorted(sts, key=lambda s: s.get("seq", 0)))
                stops = []
                for s in sts:
                    status, qid, en, lat, lon = resolve(s.get("place", ""), lut)
                    res_counts[status] = res_counts.get(status, 0) + 1
                    counts["stations"] += 1
                    counts["events"] += len(s.get("events") or [])
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
                    rows.append({
                        "person_id": r["person_id"], "heading": r["heading"],
                        "subject": subj, **{k: (json.dumps(v, ensure_ascii=False)
                                                if k == "events" else v)
                                            for k, v in stop.items()},
                    })
                itineraries.append({
                    "person_id": r["person_id"], "heading": r["heading"],
                    "volume": r["volume"], "subject": subj,
                    "is_entry_subject": subj == "entry", "stations": stops,
                })

    fields = list(rows[0].keys())
    with open(OUT_TSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(itineraries, f, ensure_ascii=False)
    print(f"itineraries: {len(itineraries)}  stations: {counts['stations']}  "
          f"events: {counts['events']}")
    print("resolution:", dict(sorted(res_counts.items(), key=lambda x: -x[1])))
    unres = {}
    for row in rows:
        if row["res_status"] == "unresolved":
            unres[norm(row["place"])] = unres.get(norm(row["place"]), 0) + 1
    top = sorted(unres.items(), key=lambda x: -x[1])[:25]
    print("top unresolved:", top)


if __name__ == "__main__":
    main()
