"""
kimatch_bridge.py
-----------------
Maps places_unified_corrected.csv → a Kimatch-ready TSV for ingestion by
scripts/kimatch_match.py.

Column mapping (unified → Kimatch input):
  entry_id          → entry_id
  context           → context
  source_role       → source_role
  source_value      → source_value      (Yiddish name from the lexicon)
  clustered_value   → english_name      (romanized/English)
  qid               → WikidataQID
  wikidata_label_yi → wikidata_yi       (Wikidata's own Yiddish label — extra matching name)

Filters applied:
  - needs_review == False  (only QID-resolved rows)
  - source_role == place   (skip province/country roles)
"""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import yaml

_HERE      = Path(__file__).resolve().parents[2]   # zibn-shtern/
_DATA_WORK = _HERE / "data" / "working"


def load_kimatch_config(config_path: str | Path) -> dict:
    with Path(config_path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def export_for_kimatch(
    input_csv: str | Path = _DATA_WORK / "places_unified_corrected.csv",
    output_tsv: str | Path = _DATA_WORK / "kimatch_input_full.tsv",
) -> Path:
    """
    Filter and reshape places_unified_corrected.csv into a Kimatch-ready TSV.

    Returns the output path.
    """
    input_csv  = Path(input_csv)
    output_tsv = Path(output_tsv)

    rows_out = []
    with input_csv.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("needs_review", "").strip() != "False":
                continue
            if row.get("source_role", "").strip() != "place":
                continue

            rows_out.append({
                "entry_id":          row.get("entry_id", "").strip(),
                "context":           row.get("context", "").strip(),
                "source_role":       row.get("source_role", "").strip(),
                "source_value":      row.get("source_value", "").strip(),
                "english_name":      row.get("clustered_value", "").strip(),
                "WikidataQID":       row.get("qid", "").strip(),
                "wikidata_yi":       row.get("wikidata_label_yi", "").strip(),
                "resolved_category": row.get("resolved_category", "").strip(),
            })

    output_tsv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["entry_id", "context", "source_role", "source_value",
                  "english_name", "WikidataQID", "wikidata_yi", "resolved_category"]
    with output_tsv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Exported {len(rows_out)} rows → {output_tsv}")
    return output_tsv
