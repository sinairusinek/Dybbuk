"""Automated pre-review corrections for the place-triage pipeline.

Three correction functions are applied in order before ``add_review_flags``
re-evaluates the data:

1. ``fix_death_site_burial`` – death and burial site values are mirrored so
    concentration camps and other death sites can count as both.
2. ``fix_city_state``        – province-typed QIDs in the place column that
   likely represent cities (e.g. "New York" resolved to Q1384) are replaced
   with the correct city QID obtained from the Wikidata search API.
3. ``fix_column_assignment`` – any remaining non-settlement entries misfiled in
   the place/province/country column are moved to their correct column.

Each function stamps the ``correction_applied`` column for auditability.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .triage import SETTLEMENT_KEYWORDS, classify_qid, is_country
from .wikidata_client import (
    fetch_entity_data,
    fetch_qid_labels,
    load_cache,
    save_cache,
    search_settlement_by_label,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CATEGORY_COLS = (
    "cemetery",
    "burial_city",
    "death_site",
    "settlement",
    "province",
    "country",
    "neighborhood",
    "other",
)

# Quick settlement signal drawn from wbsearchentities description strings.
_SETTLEMENT_DESCRIPTION_HINTS = {
    "city",
    "town",
    "village",
    "municipality",
    "borough",
    "commune",
    "settlement",
    "capital",
}


def _label_from_row(row: pd.Series) -> str:
    return str(row.get("wikidata_label_en") or row.get("clustered_value") or "")


def _set_category_value(out: pd.DataFrame, idx: Any, category: str, value: str) -> None:
    for col in _CATEGORY_COLS:
        out.at[idx, col] = ""
    if category in _CATEGORY_COLS:
        out.at[idx, category] = value


def _enrich_detail(
    qid: str,
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Fetch entity data and enrich p31/p17/p131 labels, storing in *cache*."""
    if qid not in cache:
        detail = fetch_entity_data(qid)
        if detail is None:
            return None
        cache[qid] = detail

    detail = cache[qid]

    # Enrich labels for linked QIDs if not already done.
    if "p31_labels" not in detail:
        ref_qids = detail.get("p31", []) + detail.get("p17", []) + detail.get("p131", [])
        label_map = fetch_qid_labels(ref_qids)
        detail["p31_labels"] = [
            label_map.get(q, {}).get("label_en") or q for q in detail.get("p31", [])
        ]
        detail["p17_labels"] = [
            label_map.get(q, {}).get("label_en") or q for q in detail.get("p17", [])
        ]
        detail["p131_labels"] = [
            label_map.get(q, {}).get("label_en") or q for q in detail.get("p131", [])
        ]
        cache[qid] = detail

    return detail


def _has_settlement_type(description: str, detail: dict[str, Any] | None) -> bool:
    """Return True if *description* or *detail* p31_labels contain a settlement keyword."""
    desc_lower = description.lower()
    if any(h in desc_lower for h in _SETTLEMENT_DESCRIPTION_HINTS):
        return True
    if detail:
        type_text = " ".join(detail.get("p31_labels", [])).lower()
        return any(k in type_text for k in SETTLEMENT_KEYWORDS)
    return False


# ---------------------------------------------------------------------------
# Correction 1: mirror death_site and burial_city values for place rows
# ---------------------------------------------------------------------------

def fix_death_site_burial(df: pd.DataFrame) -> pd.DataFrame:
    """Mirror death and burial values for place rows.

    A concentration camp, ghetto, or mass-murder site where someone was killed
    is effectively their burial place for the purposes of this dataset.

    Behavior:
    - if ``death_site`` is populated and ``burial_city`` is empty, copy to burial
    - if ``burial_city`` is populated and ``death_site`` is empty, copy to death
    - if both are populated with different values, preserve both and mark
      ``death_burial_conflict=True`` for downstream reporting

    No Wikidata calls are needed; this is a pure DataFrame transform.
    """
    out = df.copy()
    if "correction_applied" not in out.columns:
        out["correction_applied"] = ""
    if "death_burial_conflict" not in out.columns:
        out["death_burial_conflict"] = False

    place_mask = out["source_role"] == "place"
    if not place_mask.any():
        return out

    death_vals = out.loc[place_mask, "death_site"].fillna("").astype(str).str.strip()
    burial_vals = out.loc[place_mask, "burial_city"].fillna("").astype(str).str.strip()

    death_to_burial = place_mask.copy()
    death_to_burial.loc[place_mask] = death_vals.ne("") & burial_vals.eq("")

    burial_to_death = place_mask.copy()
    burial_to_death.loc[place_mask] = burial_vals.ne("") & death_vals.eq("")

    conflict = place_mask.copy()
    conflict.loc[place_mask] = death_vals.ne("") & burial_vals.ne("") & death_vals.ne(burial_vals)

    out.loc[death_to_burial, "burial_city"] = out.loc[death_to_burial, "death_site"]
    out.loc[burial_to_death, "death_site"] = out.loc[burial_to_death, "burial_city"]
    out.loc[conflict, "death_burial_conflict"] = True

    # Mark rows touched by copy operations; do not overwrite an existing marker.
    touched = death_to_burial | burial_to_death
    can_stamp = touched & out["correction_applied"].eq("")
    out.loc[can_stamp, "correction_applied"] = "death_burial_mirrored"

    mirrored_count = int(touched.sum())
    conflict_count = int(conflict.sum())
    print(
        f"  fix_death_site_burial: mirrored {mirrored_count} row(s), "
        f"conflicts {conflict_count} row(s)"
    )
    return out


# ---------------------------------------------------------------------------
# Correction 2: state QID in place column → substitute city QID
# ---------------------------------------------------------------------------

def fix_city_state(
    df: pd.DataFrame,
    details: dict[str, dict[str, Any]],
    cache_path: str | Path,
) -> pd.DataFrame:
    """Replace province-typed QIDs in the place column with the corresponding
    city QID when one can be found via the Wikidata search API.

    After the Phase-1 classify_qid fix (SETTLEMENT before PROVINCE), the
    only remaining ``source_role=place, resolved_category=province`` rows are
    those where the QID truly is a province/state (e.g. Q1384 – New York state)
    rather than a city.  This function searches for the city counterpart.

    Mutates *details* in-place so that newly fetched QIDs are immediately
    available to subsequent correction steps and flag generation.
    """
    out = df.copy()
    if "correction_applied" not in out.columns:
        out["correction_applied"] = ""

    mask = (out["source_role"] == "place") & (out["resolved_category"] == "province")
    if not mask.any():
        print("  fix_city_state: no applicable rows found")
        return out

    cache = load_cache(cache_path)
    # Map clustered_value → new city QID (None = no city found)
    resolved_labels: dict[str, str | None] = {}

    unique_labels = out.loc[mask, "clustered_value"].dropna().unique()
    for label_val in unique_labels:
        label_str = str(label_val).strip()
        if not label_str or label_str in resolved_labels:
            continue

        candidates = search_settlement_by_label(label_str)
        city_qid: str | None = None

        for cand in candidates:
            cqid = cand["qid"]
            description = cand.get("description", "")

            # Quick pre-filter using description before fetching full entity.
            if not _has_settlement_type(description, cache.get(cqid)):
                # Try full entity fetch if description alone isn't conclusive.
                enriched = _enrich_detail(cqid, cache)
                if enriched is None or not _has_settlement_type(description, enriched):
                    continue

            # Fully enrich so classify_qid can operate on this candidate.
            enriched = _enrich_detail(cqid, cache)
            if enriched is None:
                continue

            city_qid = cqid
            break  # First candidate with a settlement type wins.

        resolved_labels[label_str] = city_qid

    save_cache(cache_path, cache)

    substitutions = 0
    for idx, row in out.loc[mask].iterrows():
        lbl = str(row.get("clustered_value") or "").strip()
        city_qid = resolved_labels.get(lbl)
        if city_qid is None:
            continue  # No city found; fix_column_assignment will handle this.

        detail = cache.get(city_qid)
        if detail is None:
            continue

        # Update the details dict used by add_review_flags.
        details[city_qid] = detail

        new_cat, new_other = classify_qid(
            detail,
            str(row.get("context", "")),
            "place",  # source_role stays "place"
        )

        entity_label = detail.get("label_en") or lbl

        out.at[idx, "qid"] = city_qid
        out.at[idx, "wikidata_label_en"] = entity_label
        out.at[idx, "wikidata_label_yi"] = detail.get("label_yi") or ""
        out.at[idx, "wikidata_type"] = ", ".join(detail.get("p31_labels", []))
        out.at[idx, "resolved_category"] = new_cat
        out.at[idx, "other_type"] = new_other if new_other else ""
        _set_category_value(out, idx, new_cat, entity_label)
        out.at[idx, "correction_applied"] = "state_to_city"
        substitutions += 1

    skipped = int(mask.sum()) - substitutions
    print(
        f"  fix_city_state: {substitutions} QID(s) substituted with city, "
        f"{skipped} left for column reassignment"
    )
    return out


# ---------------------------------------------------------------------------
# Correction 3: reassign misfiled roles
# ---------------------------------------------------------------------------

def fix_column_assignment(
    df: pd.DataFrame,
    details: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Move non-settlement entries that are in the wrong source column.

    Rules applied:
    - place + province-type   -> province
    - place + country-type    -> country
    - country + province-type -> province  (e.g. a US state entered as "country")
    - province + country-type -> country

    Does NOT move settlements: a city in the province/country column is left
    for human review (it may reflect a genuine bad reconciliation).
    """
    out = df.copy()
    if "correction_applied" not in out.columns:
        out["correction_applied"] = ""

    def _is_country(qid: str) -> bool:
        return is_country(details.get(qid))

    reassignments = 0

    for idx, row in out.iterrows():
        role = str(row.get("source_role", ""))
        category = str(row.get("resolved_category", ""))
        other_type = str(row.get("other_type") or "")
        already_corrected = str(row.get("correction_applied") or "")

        # Skip rows already handled by earlier corrections.
        if already_corrected:
            continue

        new_role: str | None = None

        if role == "place":
            if category == "province":
                new_role = "province"
            elif category == "country" or (category == "other" and other_type == "country"):
                new_role = "country"

        elif role == "country":
            if category == "province":
                new_role = "province"
            # A country-column entry that is a settlement is left for human review.

        elif role == "province":
            if category == "country" or (category == "other" and other_type == "country"):
                new_role = "country"
            elif category != "province" and _is_country(str(row.get("qid") or "")):
                new_role = "country"
                # Ensure resolved_category reflects explicit country typing.
                out.at[idx, "resolved_category"] = "country"
                out.at[idx, "other_type"] = ""
                entity_label = _label_from_row(row)
                _set_category_value(out, idx, "country", entity_label)

        if new_role is not None:
            out.at[idx, "source_role"] = new_role
            out.at[idx, "correction_applied"] = f"moved_to_{new_role}"
            reassignments += 1

    print(f"  fix_column_assignment: {reassignments} row(s) reassigned to correct column")
    return out
