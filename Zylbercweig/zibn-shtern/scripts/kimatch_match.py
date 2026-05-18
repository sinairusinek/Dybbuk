"""
kimatch_match.py
----------------
Match places against the Kima Gazetteer and produce:

  kimatch_matched.tsv  — auto-confirmed matches (WIKIDATA or NAME_EXACT)
  kimatch_review.tsv   — fuzzy / no-match rows for manual review

Matching strategy (in priority order):
  1. WikidataQID        → db.get_by_wikidata()   → WIKIDATA
  2. source_value       → exact name search      → NAME_EXACT
  3. wikidata_yi        → exact name search      → NAME_EXACT  (Wikidata Yiddish label)
  4. english_name       → exact name search      → NAME_EXACT
  5. Fuzzy (trigram)                             → FUZZY
  6. No match                                    → NO_MATCH

Deduplication: rows sharing (source_value, WikidataQID) are grouped;
entry_ids and contexts are merged.

Usage:
  # Default: run on ZylbercweigPlacesMaaty.tsv
  python scripts/kimatch_match.py

  # Full pipeline: run on places_unified_corrected.csv via kimatch_bridge
  python scripts/kimatch_match.py --full

  # Explicit input file and output prefix
  python scripts/kimatch_match.py --input data/working/kimatch_input_full.tsv --suffix _full
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

import yaml

# ── locate zibn-shtern root ───────────────────────────────────────────────────
HERE      = Path(__file__).resolve().parent
ROOT      = HERE.parent
DATA_WORK = ROOT / "data" / "working"

# ── load config ───────────────────────────────────────────────────────────────
CONFIG_PATH = ROOT / "configs" / "kimatch.yaml"
if not CONFIG_PATH.exists():
    CONFIG_PATH = ROOT / "configs" / "kimatch.example.yaml"

with CONFIG_PATH.open(encoding="utf-8") as _f:
    _cfg = yaml.safe_load(_f) or {}

_kimatch_repo_raw = os.environ.get("KIMATCH_REPO") or _cfg.get("kimatch", {}).get("repo_path", "")
KIMATCH_REPO = (ROOT / _kimatch_repo_raw).resolve() if _kimatch_repo_raw else None

if KIMATCH_REPO and KIMATCH_REPO.exists():
    sys.path.insert(0, str(KIMATCH_REPO))
else:
    _fallback = Path("/Users/sinairusinek/Documents/GitHub/Kimatch")
    if _fallback.exists():
        sys.path.insert(0, str(_fallback))
        KIMATCH_REPO = _fallback

try:
    from kimatch.core.matcher import match_place
    from kimatch.core.models import InputPlace
    from kimatch.data.loader import KimaDB, _normalise_qid
except ImportError as e:
    sys.exit(f"Cannot import kimatch: {e}\nSet KIMATCH_REPO or configure configs/kimatch.yaml.")

PLACES_CSV   = KIMATCH_REPO / "20250126KimaPlacesCSVx.csv"
VARIANTS_TSV = KIMATCH_REPO / "Kima-Variants-20250929.tsv"
MAATY_TSV    = ROOT.parent / "ZylbercweigPlacesMaaty.tsv"

SOURCE_LABEL = "Zylbercweig lexicon"


# ── helpers ───────────────────────────────────────────────────────────────────

def load_tsv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def group_rows(rows: list[dict]) -> list[dict]:
    """Deduplicate by (source_value, WikidataQID), merging entry_ids, contexts, extra names."""
    groups: dict[tuple, dict] = {}
    for row in rows:
        sv  = row.get("source_value", "").strip()
        qid = _normalise_qid(row.get("WikidataQID", ""))
        key = (sv, qid)
        if key not in groups:
            groups[key] = {
                "source_value":      sv,
                "english_name":      row.get("english_name", row.get("English Name", "")).strip(),
                "wikidata_yi":       row.get("wikidata_yi", "").strip(),
                "wikidata_qid":      qid,
                "resolved_category": row.get("resolved_category", "").strip(),
                "entry_ids":   [],
                "contexts":    [],
            }
        groups[key]["entry_ids"].append(row.get("entry_id", "").strip())
        ctx = row.get("context", "").strip()
        if ctx and ctx not in groups[key]["contexts"]:
            groups[key]["contexts"].append(ctx)
        # Accumulate wikidata_yi if present
        yi = row.get("wikidata_yi", "").strip()
        if yi and yi != groups[key]["wikidata_yi"]:
            groups[key]["wikidata_yi"] = yi
    return list(groups.values())


def existing_variant_strings(db: KimaDB, kima_id: int) -> set[str]:
    return {v.variant for v in db.get_variants(kima_id) if v.variant}


def match_group(record: dict, db: KimaDB) -> dict:
    sv  = record["source_value"]
    en  = record["english_name"]
    yi  = record.get("wikidata_yi", "")
    qid = record["wikidata_qid"]

    # 1. Wikidata QID
    if qid:
        hits = db.get_by_wikidata(qid)
        if hits:
            return {**record, "status": "WIKIDATA", "kima_place": hits[0]}

    # 2. Exact — Yiddish (source_value)
    if sv:
        hits = db.search_by_name(sv)
        if hits:
            return {**record, "status": "NAME_EXACT", "kima_place": hits[0]}

    # 3. Exact — Wikidata Yiddish label
    if yi and yi != sv:
        hits = db.search_by_name(yi)
        if hits:
            return {**record, "status": "NAME_EXACT", "kima_place": hits[0]}

    # 4. Exact — English/romanized
    if en:
        hits = db.search_by_name(en)
        if hits:
            return {**record, "status": "NAME_EXACT", "kima_place": hits[0]}

    # 5. Fuzzy
    names = [n for n in [sv, yi, en] if n]
    result = match_place(InputPlace(input_id=qid or sv, names=names), db)
    if result.status == "fuzzy":
        return {**record, "status": "FUZZY", "kima_place": None,
                "candidates": [{"kima_id": c.kima_id, "rom": c.primary_rom, "heb": c.primary_heb}
                                for c in result.candidates],
                "confidence": result.confidence}
    return {**record, "status": "NO_MATCH", "kima_place": None, "candidates": []}


# ── main ──────────────────────────────────────────────────────────────────────

def run(input_tsv: Path, out_matched: Path, out_review: Path) -> None:
    print(f"Kimatch repo : {KIMATCH_REPO}")
    print(f"Input        : {input_tsv}")

    print("\nLoading KimaDB…")
    db = KimaDB.load(places_csv=PLACES_CSV, variants_tsv=VARIANTS_TSV)
    print(f"  {db.place_count:,} places, {db.variant_count:,} variants")

    print("Loading input TSV…")
    rows   = load_tsv(input_tsv)
    groups = group_rows(rows)
    print(f"  {len(rows)} rows → {len(groups)} unique (name, QID) groups")

    matched_rows: list[dict] = []
    review_rows:  list[dict] = []

    for rec in groups:
        result        = match_group(rec, db)
        status        = result["status"]
        entry_ids_str = "|".join(result["entry_ids"])
        contexts_str  = "|".join(result["contexts"])

        if status in ("WIKIDATA", "NAME_EXACT"):
            place  = result["kima_place"]
            ev     = existing_variant_strings(db, place.kima_id)
            is_new = result["source_value"] not in ev
            matched_rows.append({
                "source_value":      result["source_value"],
                "wikidata_yi":       result.get("wikidata_yi", ""),
                "english_name":      result["english_name"],
                "wikidata_qid":      result["wikidata_qid"],
                "resolved_category": result.get("resolved_category", ""),
                "kima_id":           place.kima_id,
                "kima_rom":          place.primary_rom,
                "kima_heb":          place.primary_heb,
                "match_status":      status,
                "entry_ids":         entry_ids_str,
                "contexts":          contexts_str,
                "is_new_variant":    "yes" if is_new else "no",
                "source":            SOURCE_LABEL,
            })
        else:
            review_rows.append({
                "source_value":      result["source_value"],
                "wikidata_yi":       result.get("wikidata_yi", ""),
                "english_name":      result["english_name"],
                "wikidata_qid":      result["wikidata_qid"],
                "resolved_category": result.get("resolved_category", ""),
                "match_status":      status,
                "fuzzy_candidates":  json.dumps(result.get("candidates", []), ensure_ascii=False),
                "fuzzy_confidence":  result.get("confidence", ""),
                "entry_ids":         entry_ids_str,
                "contexts":          contexts_str,
            })

    DATA_WORK.mkdir(parents=True, exist_ok=True)

    matched_fields = ["source_value", "wikidata_yi", "english_name", "wikidata_qid",
                      "resolved_category", "kima_id", "kima_rom", "kima_heb",
                      "match_status", "entry_ids", "contexts", "is_new_variant", "source"]
    with out_matched.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=matched_fields, delimiter="\t")
        w.writeheader()
        w.writerows(matched_rows)

    review_fields = ["source_value", "wikidata_yi", "english_name", "wikidata_qid",
                     "resolved_category", "match_status", "fuzzy_candidates", "fuzzy_confidence",
                     "entry_ids", "contexts"]
    with out_review.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=review_fields, delimiter="\t")
        w.writeheader()
        w.writerows(review_rows)

    wikidata_n   = sum(1 for r in matched_rows if r["match_status"] == "WIKIDATA")
    name_exact_n = sum(1 for r in matched_rows if r["match_status"] == "NAME_EXACT")
    new_var_n    = sum(1 for r in matched_rows if r["is_new_variant"] == "yes")
    fuzzy_n      = sum(1 for r in review_rows  if r["match_status"] == "FUZZY")
    no_match_n   = sum(1 for r in review_rows  if r["match_status"] == "NO_MATCH")

    print(f"\nResults:")
    print(f"  WIKIDATA   : {wikidata_n:>4}  (matched by QID)")
    print(f"  NAME_EXACT : {name_exact_n:>4}  (matched by name)")
    print(f"  ─ new variants proposed: {new_var_n}")
    print(f"  FUZZY      : {fuzzy_n:>4}  → {out_review.name}")
    print(f"  NO_MATCH   : {no_match_n:>4}  → {out_review.name}")
    print(f"\nWrote: {out_matched}")
    print(f"Wrote: {out_review}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  help="Input TSV (default: ZylbercweigPlacesMaaty.tsv)")
    parser.add_argument("--suffix", default="", help="Suffix for output filenames e.g. _full")
    parser.add_argument("--full",   action="store_true",
                        help="Run on full places_unified_corrected.csv via kimatch_bridge")
    args = parser.parse_args()

    if args.full:
        from zibn_shtern.kimatch_bridge import export_for_kimatch
        sys.path.insert(0, str(ROOT / "src"))
        input_path = export_for_kimatch()
        suffix = args.suffix or "_full"
    elif args.input:
        input_path = Path(args.input)
        suffix = args.suffix
    else:
        input_path = MAATY_TSV
        suffix = args.suffix

    run(
        input_tsv=input_path,
        out_matched=DATA_WORK / f"kimatch_matched{suffix}.tsv",
        out_review=DATA_WORK  / f"kimatch_review{suffix}.tsv",
    )
