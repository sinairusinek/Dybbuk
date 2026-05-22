#!/usr/bin/env python3
"""Unified, gated, deduped Kima donation export across all three datasets.

Sources (each already gated to confident/grade-A, Kima-confirmed places):
  - Zylbercweig: data/working/kimatch_matched_full.tsv   (grade==A_autolink, is_new_variant==yes)
  - YIVO Yiddishland: data/working/yivo_yiddishland_kima.A_autolink.csv  (yiddish_name → _kima_id;
    novelty checked here against the live Kima variant index)
  - Fischer: data/working/kima/fischer/fischer_donations.tsv  (already KEEP-gated + new-only)

Dedup key is (kima_id, normalized-Hebrew variant): the same spelling proposed for the same
Kima place from >1 dataset collapses to one row, merging the source provenance.

External IDs (Fischer only) are passed through to a second file, deduped and with the
malformed negative us_bgn values split out for review.

Run with the Kimatch venv (needs nothing special; uses only csv + the Kima CSVs for novelty).
Outputs to data/working/kima/.  Prints judgment-call samples (epithet/calque candidates,
negative us_bgn) for a human decision before hand-off.
"""
import csv, re, sys, unicodedata
from pathlib import Path
from collections import defaultdict

csv.field_size_limit(10**7)
ZIBN = Path(__file__).resolve().parent.parent
WORK = ZIBN / "data" / "working"
KIMA = WORK / "kima"
KREPO = Path.home() / "Documents" / "GitHub" / "Kimatch"
VARIANTS_TSV = KREPO / "Kima-Variants-20250929.tsv"
PLACES_CSV = KREPO / "20250126KimaPlacesCSVx.csv"

def heb_norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[֑-ׇ׳״'\"`.,()\[\]‘’“”/]+", "", s)
    return re.sub(r"[\s\-־–—]+", "", s).strip()

# ── Kima index: existing variants per place (to verify novelty) + rom lookup ────
existing = defaultdict(set)   # kima_id(str) -> {normalized existing variants}
for r in csv.DictReader(open(VARIANTS_TSV), delimiter="\t"):
    pid = r["PlaceId"].strip()
    for fld in ("primary_heb_full", "variant"):
        n = heb_norm(r.get(fld, ""))
        if n:
            existing[pid].add(n)
rom_by_id = {}
for r in csv.DictReader(open(PLACES_CSV)):
    rom_by_id[r["id"].strip()] = (r.get("primary_rom_full") or "").strip()
print(f"Kima index: {len(existing)} places with variants", file=sys.stderr)

# donation registry: (kima_id, variant_norm) -> row
don = {}
def add(kima_id, variant, dataset, detail, provenance):
    kima_id = str(kima_id).strip()
    variant = (variant or "").strip()
    n = heb_norm(variant)
    if not kima_id or not n:
        return "skip_empty"
    if n in existing.get(kima_id, ()):
        return "already_in_kima"
    key = (kima_id, n)
    if key in don:
        d = don[key]
        d["datasets"].add(dataset)
        d["sources"].append(f"{dataset}:{detail}")
        if provenance and provenance not in d["provenance"]:
            d["provenance"].append(provenance)
        # prefer a fully-pointed variant spelling as the display form
        if len(variant) > len(d["variant"]):
            d["variant"] = variant
        return "merged"
    don[key] = {"kima_id": kima_id, "kima_rom": rom_by_id.get(kima_id, ""),
                "variant": variant, "variant_norm": n, "datasets": {dataset},
                "sources": [f"{dataset}:{detail}"],
                "provenance": [provenance] if provenance else []}
    return "new"

stats = defaultdict(lambda: defaultdict(int))

# ── Zylbercweig ────────────────────────────────────────────────────────────────
zf = WORK / "kimatch_matched_full.tsv"
for r in csv.DictReader(open(zf), delimiter="\t"):
    if r.get("grade") == "A_autolink" and r.get("is_new_variant") == "yes":
        prov = (r.get("entry_ids", "") or "")[:200]
        stats["Zylbercweig"][add(r["kima_id"], r["source_value"], "Zylbercweig",
                                 r.get("wikidata_qid", ""), prov)] += 1

# ── YIVO Yiddishland (grade-A; novelty checked above) ───────────────────────────
yf = WORK / "yivo_yiddishland_kima.A_autolink.csv"
for r in csv.DictReader(open(yf)):
    kid = (r.get("_kima_id") or "").strip()
    stats["YIVO"][add(kid, r.get("yiddish_name", ""), "YIVO",
                      r.get("current_official_name", ""), "")] += 1

# ── Fischer (already KEEP-gated + new) ──────────────────────────────────────────
ff = KIMA / "fischer" / "fischer_donations.tsv"
for r in csv.DictReader(open(ff), delimiter="\t"):
    stats["Fischer"][add(r["kima_id"], r["variant"], "Fischer",
                         r.get("fischer_uid", ""), r.get("method", ""))] += 1

# ── classify problematic variants (glosses/calques/conjunctions/locators) ───────
# These are not clean name variants; split to a review file rather than donate as-is.
_VTYPE = [
    ("conjunction", re.compile(r"\bאון\b| און ")),         # "X און Y" names two places
    ("parenthetical", re.compile(r"[()]")),                 # gloss in parens / abbrev
    ("locator", re.compile(r"מדינת|במדינ|\bביי\b|\bאויף\b|אומגעגנט")),  # "in/near …"
    ("descriptor", re.compile(r"מרחץ|\bעיר\b|קהל")),       # calque/epithet
]
def variant_type(v):
    for name, pat in _VTYPE:
        if pat.search(v):
            return name
    return ""

# ── write unified variants (clean) + a NEEDS_REVIEW split for the flagged ones ──
rows = sorted(don.values(), key=lambda d: (-len(d["datasets"]), d["kima_id"]))
clean, review = [], []
for d in rows:
    (review if variant_type(d["variant"]) else clean).append(d)

def _write(path, data, with_type=False):
    cols = ["kima_id", "kima_rom", "variant", "variant_norm", "n_datasets",
            "datasets", "sources", "provenance"] + (["variant_type"] if with_type else [])
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t"); w.writeheader()
        for d in data:
            row = {"kima_id": d["kima_id"], "kima_rom": d["kima_rom"],
                   "variant": d["variant"], "variant_norm": d["variant_norm"],
                   "n_datasets": len(d["datasets"]),
                   "datasets": ";".join(sorted(d["datasets"])),
                   "sources": " | ".join(d["sources"][:8]),
                   "provenance": " | ".join(d["provenance"][:3])}
            if with_type:
                row["variant_type"] = variant_type(d["variant"])
            w.writerow(row)

out = KIMA / "donations_unified_variants.tsv"
_write(out, clean)
_write(KIMA / "donations_variants_NEEDS_REVIEW_epithets.tsv", review, with_type=True)

# ── external IDs (Fischer only): dedup + split malformed negatives ──────────────
eid_seen, eid_rows, eid_bad = set(), [], []
ef = KIMA / "fischer" / "fischer_external_id_donations.tsv"
if ef.exists():
    for r in csv.DictReader(open(ef), delimiter="\t"):
        key = (r["kima_id"].strip(), r["id_type"].strip(), r["id_value"].strip())
        if key in eid_seen:
            continue
        eid_seen.add(key)
        val = r["id_value"].strip()
        (eid_bad if val.lstrip("-").isdigit() and val.startswith("-") else eid_rows).append(r)
    for name, data in [("donations_unified_external_ids.tsv", eid_rows),
                       ("donations_external_ids_NEEDS_REVIEW_negative.tsv", eid_bad)]:
        if data:
            with open(KIMA / name, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(data[0].keys()), delimiter="\t")
                w.writeheader(); w.writerows(data)

# ── report ──────────────────────────────────────────────────────────────────────
for ds, c in stats.items():
    print(f"{ds:12} {dict(c)}", file=sys.stderr)
print(f"\nUNIFIED variant donations: {len(rows)} distinct (kima_id, variant)", file=sys.stderr)
print(f"  CLEAN (ready to donate): {len(clean)} → {out.name}", file=sys.stderr)
print(f"  NEEDS_REVIEW (gloss/calque/conjunction/locator): {len(review)} → "
      f"donations_variants_NEEDS_REVIEW_epithets.tsv", file=sys.stderr)
from collections import Counter
print(f"    by type: {dict(Counter(variant_type(d['variant']) for d in review))}", file=sys.stderr)
print(f"  corroborated by >1 dataset: {sum(len(d['datasets']) > 1 for d in clean)}", file=sys.stderr)
print(f"  external IDs: {len(eid_rows)} clean, {len(eid_bad)} negative→review", file=sys.stderr)
