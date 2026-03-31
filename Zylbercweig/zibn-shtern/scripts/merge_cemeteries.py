"""Merge research-assistant cemetery data into the unified places file.

Reads the RA cemeteries Excel file and integrates it with
places_unified_corrected.csv:

- Entries that already have burial rows: backfill QID when the RA found one
  and the pipeline did not; flag conflicts when both disagree.
- Entries with no burial rows yet: insert new burial/place rows (and
  burial/province, burial/country where the RA supplied that info).
- Death-date and death-place columns are *not* imported (tracked elsewhere).
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd

from zibn_shtern.triage import (
    UNIFIED_COLUMNS,
    classify_qid,
    ensure_unified_schema,
)

_QID_RE = re.compile(r"Q\d+")


def _extract_qid(url: object) -> str:
    """Pull 'Q…' from a Wikidata URL or bare QID string, or return ''."""
    if pd.isna(url) or not str(url).strip():
        return ""
    m = _QID_RE.search(str(url))
    return m.group(0) if m else ""


def _empty_row(entry_id: str, context: str, source_role: str) -> dict:
    """Return a minimal unified-schema dict with blanks for every column."""
    row = {c: "" for c in UNIFIED_COLUMNS}
    row["entry_id"] = entry_id
    row["context"] = context
    row["source_role"] = source_role
    row["place_qid_conflict"] = False
    row["needs_review"] = False
    row["death_burial_conflict"] = False
    return row


def load_cemetery_data(path: Path) -> pd.DataFrame:
    """Load and lightly normalise the RA cemeteries spreadsheet."""
    df = pd.read_excel(path)
    df = df.rename(columns={
        "unique-id":                "entry_id",
        "_ - burial_place - name":  "cem_name",
        "wikidata":                 "cem_qid_url",
        "_ - burial_place - province": "cem_province",
        "_ - burial_place - country":  "cem_country",
    })
    df["cem_qid"] = df["cem_qid_url"].apply(_extract_qid)
    # Keep only the columns we need.
    keep = ["entry_id", "cem_name", "cem_qid", "cem_province", "cem_country"]
    return df[keep].copy()


def merge(
    unified: pd.DataFrame,
    cemetery: pd.DataFrame,
    cache_path: Path | None = None,
) -> pd.DataFrame:
    """Return a unified DataFrame with RA cemetery data merged in."""
    # Load Wikidata cache for label/type lookups on new QIDs.
    cache: dict = {}
    if cache_path and cache_path.exists():
        cache = json.loads(cache_path.read_text(encoding="utf-8"))

    # Index existing burial/place rows by entry_id for fast lookup.
    burial_place_mask = (unified["context"] == "burial") & (unified["source_role"] == "place")
    burial_place_ids = set(unified.loc[burial_place_mask, "entry_id"])

    new_rows: list[dict] = []
    update_count = 0
    insert_count = 0

    for _, cem in cemetery.iterrows():
        eid = str(cem["entry_id"])
        qid = str(cem.get("cem_qid") or "")
        name = str(cem.get("cem_name") or "")
        province = str(cem.get("cem_province") or "")
        country = str(cem.get("cem_country") or "")

        if eid in burial_place_ids:
            # Update existing burial/place rows for this entry.
            mask = burial_place_mask & (unified["entry_id"] == eid)
            for idx in unified.index[mask]:
                existing_qid = str(unified.at[idx, "qid"] or "").strip()
                if not existing_qid and qid:
                    # Backfill QID from RA.
                    detail = cache.get(qid, {})
                    label_en = detail.get("label_en", "")
                    label_yi = detail.get("label_yi", "")
                    p31_text = ", ".join(detail.get("p31_labels", [])) if detail else ""
                    category, other_type = classify_qid(detail, "burial", "place") if detail else ("cemetery", "")

                    unified.at[idx, "qid"] = qid
                    unified.at[idx, "qid_source"] = "ra_cemetery"
                    unified.at[idx, "wikidata_label_en"] = label_en
                    unified.at[idx, "wikidata_label_yi"] = label_yi
                    unified.at[idx, "wikidata_type"] = p31_text
                    unified.at[idx, "resolved_category"] = category
                    unified.at[idx, "other_type"] = other_type or ""
                    if category == "cemetery":
                        unified.at[idx, "cemetery"] = label_en or name
                    update_count += 1
                elif existing_qid and qid and existing_qid != qid:
                    # Both have QIDs but they differ — flag for review.
                    existing_flags = str(unified.at[idx, "review_flags"] or "")
                    flag = f"ra_qid_conflict:{qid}"
                    if flag not in existing_flags:
                        sep = ";" if existing_flags else ""
                        unified.at[idx, "review_flags"] = existing_flags + sep + flag
                        unified.at[idx, "needs_review"] = True
                    update_count += 1
        else:
            # Create new burial/place row.
            detail = cache.get(qid, {}) if qid else {}
            label_en = detail.get("label_en", "") if detail else ""
            label_yi = detail.get("label_yi", "") if detail else ""
            p31_text = ", ".join(detail.get("p31_labels", [])) if detail else ""
            category, other_type = classify_qid(detail, "burial", "place") if detail else ("cemetery", "")

            row = _empty_row(eid, "burial", "place")
            row["source_value"] = name
            row["clustered_value"] = name
            row["qid"] = qid
            row["qid_source"] = "ra_cemetery" if qid else ""
            row["wikidata_label_en"] = label_en
            row["wikidata_label_yi"] = label_yi
            row["wikidata_type"] = p31_text
            row["resolved_category"] = category
            row["other_type"] = other_type or ""
            if category == "cemetery":
                row["cemetery"] = label_en or name
            new_rows.append(row)
            insert_count += 1

            # Optional province row.
            if province and province != "nan":
                prow = _empty_row(eid, "burial", "province")
                prow["source_value"] = province
                prow["clustered_value"] = province
                new_rows.append(prow)

            # Optional country row.
            if country and country != "nan":
                crow = _empty_row(eid, "burial", "country")
                crow["source_value"] = country
                crow["clustered_value"] = country
                new_rows.append(crow)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        unified = pd.concat([unified, new_df], ignore_index=True)

    unified = ensure_unified_schema(unified)

    print(f"Updated {update_count} existing burial rows")
    print(f"Inserted {insert_count} new burial/place rows (+province/country)")
    print(f"Total rows: {len(unified)}")
    return unified


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge RA cemetery data into the unified places file")
    parser.add_argument(
        "--cemeteries",
        default="data/raw/Zylbercweig-Extraction2026-02-05cemeteries-tsv.xlsx",
        help="RA cemeteries Excel file",
    )
    parser.add_argument(
        "--unified",
        default="data/working/places_unified_corrected.csv",
        help="Unified places CSV (input and output)",
    )
    parser.add_argument(
        "--cache",
        default="data/working/qid_lookup.json",
        help="Wikidata QID cache for label lookups",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path (defaults to overwriting --unified)",
    )
    args = parser.parse_args()

    cem_path = Path(args.cemeteries)
    unified_path = Path(args.unified)
    cache_path = Path(args.cache)
    out_path = Path(args.output) if args.output else unified_path

    cemetery = load_cemetery_data(cem_path)
    unified = pd.read_csv(unified_path)
    merged = merge(unified, cemetery, cache_path=cache_path)
    merged.to_csv(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
