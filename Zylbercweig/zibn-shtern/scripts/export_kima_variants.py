"""
export_kima_variants.py
-----------------------
Export new Yiddish place name variants for contribution to the Kima Gazetteer.

Sources:
  1. data/working/kimatch_matched.tsv  — auto-confirmed matches (is_new_variant=yes)
  2. data/working/kimatch_decisions.json — manual review decisions (action=map_to:<kima_id>)
  3. data/working/kimatch_review.tsv   — original review rows (for Yiddish names + attestations)

Output:
  data/working/kima_variants_export.tsv
  Columns: kima_id | kima_rom | variant (Yiddish) | source | attestations | contexts | wikidata_qid | notes

Usage:
  python scripts/export_kima_variants.py
  python scripts/export_kima_variants.py --output data/corrected/kima_variants_export.tsv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

HERE      = Path(__file__).resolve().parent
ROOT      = HERE.parent
DATA_WORK = ROOT / "data" / "working"

MATCHED_TSV   = DATA_WORK / "kimatch_matched_full.tsv"
REVIEW_TSV    = DATA_WORK / "kimatch_review_full.tsv"
# Decisions JSON is now stored in the Kimatch repo (accessible on Streamlit Cloud).
# Fall back to the local working copy if the Kimatch copy doesn't exist.
_KIMATCH_DECISIONS = _KIMATCH / "data" / "zylbercweig" / "kimatch_decisions_full.json"
DECISIONS_JSON = _KIMATCH_DECISIONS if _KIMATCH_DECISIONS.exists() else DATA_WORK / "kimatch_decisions_full.json"
DEFAULT_OUT   = DATA_WORK / "kima_variants_export.tsv"

SOURCE_LABEL = "Zylbercweig lexicon (Maaty reconciliation)"

FIELDNAMES = [
    "kima_id", "kima_rom", "variant", "source",
    "attestations", "contexts", "wikidata_qid", "notes",
]

_KIMATCH = Path("/Users/sinairusinek/Documents/GitHub/Kimatch")
if str(_KIMATCH) not in sys.path and _KIMATCH.exists():
    sys.path.insert(0, str(_KIMATCH))


def load_matched(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            if row.get("is_new_variant", "").strip() == "yes":
                rows.append(row)
    return rows


def load_review(path: Path) -> dict[str, dict]:
    """Return source_value → row dict for review rows."""
    result = {}
    with path.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            sv = row.get("source_value", "").strip()
            if sv:
                result[sv] = row
    return result


def load_decisions(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def kima_rom(kima_id: str) -> str:
    """Look up the romanized name for a Kima ID."""
    try:
        from kimatch.data.loader import KimaDB
        _db = _get_db()
        place = _db.get(int(kima_id))
        return place.primary_rom if place else ""
    except Exception:
        return ""


_db_cache = None

def _get_db():
    global _db_cache
    if _db_cache is None:
        from kimatch.data.loader import KimaDB
        _db_cache = KimaDB.load(
            places_csv=_KIMATCH / "20250126KimaPlacesCSVx.csv",
            variants_tsv=_KIMATCH / "Kima-Variants-20250929.tsv",
        )
    return _db_cache


def run(output: Path = DEFAULT_OUT) -> None:
    print("Loading data…")
    matched   = load_matched(MATCHED_TSV)
    review    = load_review(REVIEW_TSV)
    decisions = load_decisions(DECISIONS_JSON)

    print(f"  {len(matched)} auto-matched new variants")
    print(f"  {len(decisions)} manual decisions")

    rows: list[dict] = []

    # ── 1. auto-confirmed matches ─────────────────────────────────────────────
    for m in matched:
        rows.append({
            "kima_id":      m["kima_id"],
            "kima_rom":     m["kima_rom"],
            "variant":      m["source_value"],
            "source":       SOURCE_LABEL,
            "attestations": m.get("entry_ids", ""),
            "contexts":     m.get("contexts", ""),
            "wikidata_qid": m.get("wikidata_qid", ""),
            "notes":        f"match_status:{m.get('match_status','')}",
        })

    # ── 2. manually confirmed review decisions ────────────────────────────────
    for sv, dec in decisions.items():
        action  = dec.get("action", "")
        kima_id = dec.get("kima_id", "")
        if not action.startswith("map_to:") or not kima_id or kima_id == "__manual__":
            continue
        # Skip if already covered by auto-matched
        if any(r["variant"] == sv and r["kima_id"] == kima_id for r in rows):
            continue

        rev_row  = review.get(sv, {})
        rom      = kima_rom(kima_id)
        rows.append({
            "kima_id":      kima_id,
            "kima_rom":     rom,
            "variant":      sv,
            "source":       SOURCE_LABEL,
            "attestations": rev_row.get("entry_ids", ""),
            "contexts":     rev_row.get("contexts", ""),
            "wikidata_qid": rev_row.get("wikidata_qid", ""),
            "notes":        f"manual_review; match_status:{rev_row.get('match_status','')}",
        })

    print(f"  {len(rows)} total variants to export")

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote: {output}")

    # Summary
    by_status: dict[str, int] = {}
    for r in rows:
        key = r["notes"].split(";")[0].replace("match_status:", "").strip() or "manual"
        by_status[key] = by_status.get(key, 0) + 1
    for k, v in sorted(by_status.items()):
        print(f"  {k:<20}: {v}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export new Yiddish variants for Kima contribution")
    parser.add_argument("--output", default=str(DEFAULT_OUT), help="Output TSV path")
    args = parser.parse_args()
    run(Path(args.output))
