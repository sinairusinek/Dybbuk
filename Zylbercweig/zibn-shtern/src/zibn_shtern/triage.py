from __future__ import annotations

from collections import defaultdict
from typing import Any

import pandas as pd

from .wikidata_client import extract_qids


CEMETERY_KEYWORDS = {
    "cemetery",
    "graveyard",
    "burial ground",
    "necropolis",
}

DEATH_SITE_KEYWORDS = {
    "concentration camp",
    "extermination camp",
    "ghetto",
    "massacre",
    "mass grave",
    "killing",
}

SETTLEMENT_KEYWORDS = {
    "human settlement",
    "city",
    "town",
    "village",
    "borough",
    "municipality",
    "commune",
}

NEIGHBORHOOD_KEYWORDS = {
    "neighborhood",
    "neighbourhood",
    "urban district",
    "district of",
    "quarter",
    "suburb",
}

PROVINCE_KEYWORDS = {
    "administrative territorial entity",
    "province",
    "governorate",
    "oblast",
    "voivodeship",
    "county",
    "region",
    "state",
    "guberniya",
    "district",
}

COUNTRY_KEYWORDS = {
    "country",
    "sovereign state",
    "empire",
    "kingdom",
    "republic",
}

UNIFIED_COLUMNS = [
    "entry_id",
    "context",
    "source_role",
    "source_value",
    "clustered_value",
    "qid",
    "qid_source",
    "place_qid_conflict",
    "wikidata_label_en",
    "wikidata_label_yi",
    "wikidata_type",
    "resolved_category",
    "other_type",
    "cemetery",
    "burial_city",
    "death_site",
    "settlement",
    "province",
    "country",
    "neighborhood",
    "other",
    "review_flags",
    "needs_review",
    "correction_applied",
    "death_burial_conflict",
]

_CATEGORY_COLUMNS = [
    "cemetery",
    "burial_city",
    "death_site",
    "settlement",
    "province",
    "country",
    "neighborhood",
    "other",
]

_LEGACY_BASE_COLUMNS = [
    "entry_id",
    "context",
    "source_role",
    "source_value",
    "clustered_value",
    "qid",
    "qid_source",
    "place_qid_conflict",
    "wikidata_label_en",
    "wikidata_label_yi",
    "wikidata_type",
    "resolved_category",
    "other_type",
    *_CATEGORY_COLUMNS,
    "review_flags",
    "needs_review",
]


def ensure_unified_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame padded/reordered to the canonical unified schema."""
    out = df.copy()

    bool_defaults = {
        "place_qid_conflict": False,
        "needs_review": False,
        "death_burial_conflict": False,
    }

    for col in UNIFIED_COLUMNS:
        if col in bool_defaults:
            if col not in out.columns:
                out[col] = bool_defaults[col]
            else:
                out[col] = out[col].fillna(bool_defaults[col])
        elif col not in out.columns:
            out[col] = ""

    return out[UNIFIED_COLUMNS]


def derive_review_queue(unified_df: pd.DataFrame, include_corrections: bool = False) -> pd.DataFrame:
    """Derive a reviewer-oriented queue view from the unified table."""
    unified = ensure_unified_schema(unified_df)
    queue = unified.loc[unified["needs_review"]].copy()

    preferred = [
        "entry_id",
        "context",
        "source_role",
        "source_value",
        "clustered_value",
        "qid",
        "wikidata_label_en",
        "wikidata_type",
        "resolved_category",
        "other_type",
        "review_flags",
        "qid_source",
        "place_qid_conflict",
        "wikidata_label_yi",
        *_CATEGORY_COLUMNS,
        "needs_review",
    ]
    if include_corrections:
        preferred.extend(["correction_applied", "death_burial_conflict"])

    cols = [c for c in preferred if c in queue.columns]
    return queue[cols]


def derive_legacy_outputs(
    unified_df: pd.DataFrame,
    include_corrections: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Derive resolved and review-queue legacy outputs from unified data."""
    unified = ensure_unified_schema(unified_df)
    resolved_cols = list(_LEGACY_BASE_COLUMNS)
    if include_corrections:
        resolved_cols.extend(["correction_applied", "death_burial_conflict"])

    resolved = unified[resolved_cols].copy()
    queue = derive_review_queue(unified, include_corrections=include_corrections)
    return resolved, queue


def _type_text(detail: dict[str, Any] | None) -> str:
    if not detail:
        return ""
    labels = detail.get("p31_labels", []) or []
    return " ".join(x.lower() for x in labels if isinstance(x, str))


def _label(detail: dict[str, Any] | None, fallback: str) -> str:
    if not detail:
        return fallback
    return detail.get("label_en") or fallback


def _primary_type(detail: dict[str, Any] | None) -> str:
    if not detail:
        return ""
    labels = detail.get("p31_labels", []) or []
    for label in labels:
        if isinstance(label, str) and label.strip():
            return label
    return ""


def is_country(detail: dict[str, Any] | None) -> bool:
    t = _type_text(detail)
    return any(k in t for k in COUNTRY_KEYWORDS)


def _collect_p131_ancestors(start_qid: str, details: dict[str, dict[str, Any]], max_depth: int = 6) -> set[str]:
    if not start_qid or max_depth <= 0:
        return set()

    ancestors: set[str] = set()
    frontier: set[str] = {start_qid}
    visited: set[str] = set()

    for _ in range(max_depth):
        if not frontier:
            break

        next_frontier: set[str] = set()
        for qid in frontier:
            if qid in visited:
                continue
            visited.add(qid)

            detail = details.get(qid)
            if not detail:
                continue

            for parent in detail.get("p131", []):
                if parent not in ancestors:
                    ancestors.add(parent)
                if parent not in visited:
                    next_frontier.add(parent)

        frontier = next_frontier

    return ancestors


def _collect_country_candidates(
    qid: str,
    details: dict[str, dict[str, Any]],
    max_depth: int = 6,
) -> set[str]:
    countries: set[str] = set()
    detail = details.get(qid)
    if detail:
        countries.update(detail.get("p17", []))

    for anc in _collect_p131_ancestors(qid, details, max_depth=max_depth):
        anc_detail = details.get(anc)
        if anc_detail:
            countries.update(anc_detail.get("p17", []))

    return countries


def classify_qid(detail: dict[str, Any] | None, context: str, source_role: str) -> tuple[str, str | None]:
    text = _type_text(detail)
    has_country = any(k in text for k in COUNTRY_KEYWORDS)
    has_settlement = any(k in text for k in SETTLEMENT_KEYWORDS)
    has_neighborhood = any(k in text for k in NEIGHBORHOOD_KEYWORDS)

    if any(k in text for k in DEATH_SITE_KEYWORDS):
        return "death_site", None

    if any(k in text for k in CEMETERY_KEYWORDS):
        return "cemetery", None

    if has_neighborhood:
        return "neighborhood", None

    # SETTLEMENT is checked before PROVINCE so that cities whose Wikidata type
    # strings contain province-keyword substrings (e.g. "city in the United
    # States" contains "state", "county seat" contains "county") are correctly
    # classified as settlements rather than provinces.
    if has_settlement:
        # Context (birth/death/burial) is modeled separately from type.
        # A city referenced in burial context is still a settlement.
        return "settlement", None

    if has_country:
        return "country", None

    if any(k in text for k in PROVINCE_KEYWORDS):
        return "province", None

    return "other", _primary_type(detail) or "unknown"


def _append_entity_rows(
    rows: list[dict[str, Any]],
    record: dict[str, Any],
    source_role: str,
    source_col: str,
    cluster_col: str | None,
    qid_col: str,
) -> None:
    qids = extract_qids(record.get(qid_col))
    for qid in qids:
        rows.append(
            {
                "entry_id": record.get("Column 1"),
                "context": record.get("context"),
                "source_role": source_role,
                "source_value": record.get(source_col),
                "clustered_value": record.get(cluster_col) if cluster_col else None,
                "qid": qid,
                "qid_source": source_role,
                "place_qid_conflict": False,
            }
        )


def explode_resolved_places(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        initial_place_qids = extract_qids(rec.get("initial reconciliation for place"))
        dodgy_place_qids = extract_qids(rec.get("dodgy reconciliation for place"))
        place_qids = initial_place_qids or dodgy_place_qids

        qid_conflict = bool(initial_place_qids and dodgy_place_qids and set(initial_place_qids) != set(dodgy_place_qids))
        qid_source = "initial" if initial_place_qids else ("dodgy" if dodgy_place_qids else "")

        for qid in place_qids:
            rows.append(
                {
                    "entry_id": rec.get("Column 1"),
                    "context": rec.get("context"),
                    "source_role": "place",
                    "source_value": rec.get("place"),
                    "clustered_value": rec.get("place clustered"),
                    "qid": qid,
                    "qid_source": qid_source,
                    "place_qid_conflict": qid_conflict,
                }
            )

        _append_entity_rows(
            rows,
            rec,
            source_role="province",
            source_col="province",
            cluster_col="province clustered",
            qid_col="province Qid",
        )

        _append_entity_rows(
            rows,
            rec,
            source_role="country",
            source_col="country",
            cluster_col="country cluster",
            qid_col="Country Qid",
        )

    return pd.DataFrame(rows)


def enrich_and_classify(resolved_df: pd.DataFrame, details: dict[str, dict[str, Any]]) -> pd.DataFrame:
    if resolved_df.empty:
        return resolved_df

    out = resolved_df.copy()

    labels_en: list[str | None] = []
    labels_yi: list[str | None] = []
    p31_text: list[str | None] = []
    categories: list[str] = []
    other_types: list[str | None] = []

    for rec in out.to_dict(orient="records"):
        qid = rec["qid"]
        detail = details.get(qid)

        label_en = detail.get("label_en") if detail else rec.get("clustered_value")
        label_yi = detail.get("label_yi") if detail else None
        p31 = ", ".join(detail.get("p31_labels", [])) if detail else None

        category, other_type = classify_qid(detail, str(rec.get("context", "")), str(rec.get("source_role", "")))

        labels_en.append(label_en)
        labels_yi.append(label_yi)
        p31_text.append(p31)
        categories.append(category)
        other_types.append(other_type)

    out["wikidata_label_en"] = labels_en
    out["wikidata_label_yi"] = labels_yi
    out["wikidata_type"] = p31_text
    out["resolved_category"] = categories
    out["other_type"] = other_types

    out["cemetery"] = out.apply(lambda r: _label(details.get(r["qid"]), str(r.get("clustered_value") or "")) if r["resolved_category"] == "cemetery" else "", axis=1)
    out["burial_city"] = out.apply(lambda r: _label(details.get(r["qid"]), str(r.get("clustered_value") or "")) if r["resolved_category"] == "burial_city" else "", axis=1)
    out["death_site"] = out.apply(lambda r: _label(details.get(r["qid"]), str(r.get("clustered_value") or "")) if r["resolved_category"] == "death_site" else "", axis=1)
    out["settlement"] = out.apply(lambda r: _label(details.get(r["qid"]), str(r.get("clustered_value") or "")) if r["resolved_category"] == "settlement" else "", axis=1)
    out["province"] = out.apply(lambda r: _label(details.get(r["qid"]), str(r.get("clustered_value") or "")) if r["resolved_category"] == "province" else "", axis=1)
    out["country"] = out.apply(lambda r: _label(details.get(r["qid"]), str(r.get("clustered_value") or "")) if r["resolved_category"] == "country" else "", axis=1)
    out["neighborhood"] = out.apply(lambda r: _label(details.get(r["qid"]), str(r.get("clustered_value") or "")) if r["resolved_category"] == "neighborhood" else "", axis=1)
    out["other"] = out.apply(lambda r: _label(details.get(r["qid"]), str(r.get("clustered_value") or "")) if r["resolved_category"] == "other" else "", axis=1)

    out["review_flags"] = ""
    return out


def add_review_flags(
    classified_df: pd.DataFrame,
    details: dict[str, dict[str, Any]],
    chain_depth: int = 6,
) -> pd.DataFrame:
    if classified_df.empty:
        return classified_df

    out = classified_df.copy()
    flags_by_index: dict[int, set[str]] = defaultdict(set)

    # per-row role/type mismatch flags
    for idx, rec in out.iterrows():
        role = str(rec.get("source_role", ""))
        category = str(rec.get("resolved_category", ""))
        detail = details.get(str(rec.get("qid")))

        if rec.get("place_qid_conflict") is True:
            flags_by_index[idx].add("place_qid_conflict")

        if role == "province" and category != "province":
            flags_by_index[idx].add("province_role_mismatch")

        if role == "country" and not is_country(detail):
            flags_by_index[idx].add("country_role_mismatch")

        if role == "place" and category == "province":
            flags_by_index[idx].add("place_is_admin_region")

        if category == "other":
            flags_by_index[idx].add("needs_manual_type_review")

    # per-entry, per-context geographic coherence
    for (_, _), group in out.groupby(["entry_id", "context"], dropna=False):
        place_rows = group[group["source_role"] == "place"]
        province_rows = group[group["source_role"] == "province"]
        country_rows = group[group["source_role"] == "country"]

        province_qids = set(province_rows["qid"].dropna().astype(str).tolist())
        country_qids = set(country_rows["qid"].dropna().astype(str).tolist())

        # validate province rows against country rows using direct and ancestor-derived country evidence
        for idx, rec in province_rows.iterrows():
            province_qid = str(rec.get("qid"))
            province_detail = details.get(province_qid)
            if not province_detail:
                flags_by_index[idx].add("qid_lookup_missing")
                continue

            province_countries = _collect_country_candidates(province_qid, details, max_depth=chain_depth)
            if country_qids:
                if province_countries and country_qids.isdisjoint(province_countries):
                    flags_by_index[idx].add("province_country_mismatch")
                if not province_countries:
                    flags_by_index[idx].add("province_country_unresolved")

        for idx, rec in place_rows.iterrows():
            place_qid = str(rec.get("qid"))
            detail = details.get(place_qid)
            if not detail:
                flags_by_index[idx].add("qid_lookup_missing")
                continue

            p17 = set(detail.get("p17", []))
            p131 = set(detail.get("p131", []))
            p131_ancestors = _collect_p131_ancestors(place_qid, details, max_depth=chain_depth)
            place_countries = _collect_country_candidates(place_qid, details, max_depth=chain_depth)

            if country_qids:
                if place_countries and country_qids.isdisjoint(place_countries):
                    flags_by_index[idx].add("place_country_mismatch")
                if not place_countries:
                    flags_by_index[idx].add("place_country_unresolved")

            province_matches = set()
            province_matches.update(p131.intersection(province_qids))
            province_matches.update(p131_ancestors.intersection(province_qids))
            if province_qids and not province_matches:
                flags_by_index[idx].add("place_province_mismatch")

    out["review_flags"] = [";".join(sorted(flags_by_index.get(i, set()))) for i in out.index]
    out["needs_review"] = out["review_flags"].ne("")
    return out


def build_qid_review_queue(classified_df: pd.DataFrame) -> pd.DataFrame:
    if classified_df.empty:
        return classified_df
    include_corrections = all(
        col in classified_df.columns
        for col in ["correction_applied", "death_burial_conflict"]
    )
    return derive_review_queue(classified_df, include_corrections=include_corrections)
