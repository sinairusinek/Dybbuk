from __future__ import annotations

import pandas as pd

from zibn_shtern.triage import (
    UNIFIED_COLUMNS,
    derive_legacy_outputs,
    derive_review_queue,
    ensure_unified_schema,
)


BASE_QUEUE_COLUMNS = [
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
    "cemetery",
    "burial_city",
    "death_site",
    "settlement",
    "province",
    "country",
    "neighborhood",
    "other",
    "needs_review",
]

CORRECTED_QUEUE_COLUMNS = [
    *BASE_QUEUE_COLUMNS,
    "correction_applied",
    "death_burial_conflict",
]

BASE_RESOLVED_COLUMNS = [
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
]

CORRECTED_RESOLVED_COLUMNS = [
    *BASE_RESOLVED_COLUMNS,
    "correction_applied",
    "death_burial_conflict",
]


def _records(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    return sorted(tuple(row) for row in df[columns].fillna("").itertuples(index=False, name=None))


def _sample_unified() -> pd.DataFrame:
    rows = [
        {
            "entry_id": "e1",
            "context": "birth_place",
            "source_role": "place",
            "source_value": "Lodz",
            "clustered_value": "lodz",
            "qid": "Q580",
            "qid_source": "place",
            "place_qid_conflict": False,
            "wikidata_label_en": "Lodz",
            "wikidata_label_yi": "לאדז",
            "wikidata_type": "city",
            "resolved_category": "settlement",
            "other_type": "",
            "cemetery": "",
            "burial_city": "",
            "death_site": "",
            "settlement": "Q580",
            "province": "",
            "country": "",
            "neighborhood": "",
            "other": "",
            "review_flags": "",
            "needs_review": False,
            "correction_applied": "",
            "death_burial_conflict": False,
        },
        {
            "entry_id": "e2",
            "context": "death_place",
            "source_role": "place",
            "source_value": "Auschwitz",
            "clustered_value": "auschwitz",
            "qid": "Q7342",
            "qid_source": "place",
            "place_qid_conflict": True,
            "wikidata_label_en": "Auschwitz concentration camp",
            "wikidata_label_yi": "",
            "wikidata_type": "concentration camp",
            "resolved_category": "death_site",
            "other_type": "",
            "cemetery": "",
            "burial_city": "",
            "death_site": "Q7342",
            "settlement": "",
            "province": "",
            "country": "",
            "neighborhood": "",
            "other": "",
            "review_flags": "place_country_mismatch;needs_manual_type_review",
            "needs_review": True,
            "correction_applied": "death_burial_mirrored",
            "death_burial_conflict": True,
        },
        {
            "entry_id": "e3",
            "context": "birth_place",
            "source_role": "country",
            "source_value": "Poland",
            "clustered_value": "poland",
            "qid": "Q36",
            "qid_source": "country",
            "place_qid_conflict": False,
            "wikidata_label_en": "Poland",
            "wikidata_label_yi": "",
            "wikidata_type": "country",
            "resolved_category": "country",
            "other_type": "",
            "cemetery": "",
            "burial_city": "",
            "death_site": "",
            "settlement": "",
            "province": "",
            "country": "Q36",
            "neighborhood": "",
            "other": "",
            "review_flags": "",
            "needs_review": False,
            "correction_applied": "",
            "death_burial_conflict": False,
        },
    ]
    return ensure_unified_schema(pd.DataFrame(rows))


def test_ensure_unified_schema_column_order() -> None:
    df = ensure_unified_schema(pd.DataFrame([{"entry_id": "x", "qid": "Q1"}]))
    assert list(df.columns) == UNIFIED_COLUMNS


def test_base_legacy_derivation_parity() -> None:
    unified = _sample_unified()
    resolved, queue = derive_legacy_outputs(unified, include_corrections=False)

    assert list(resolved.columns) == BASE_RESOLVED_COLUMNS
    assert list(queue.columns) == BASE_QUEUE_COLUMNS

    assert _records(unified, BASE_RESOLVED_COLUMNS) == _records(resolved, BASE_RESOLVED_COLUMNS)

    unified_queue = unified.loc[unified["needs_review"]].copy()
    assert _records(unified_queue, BASE_QUEUE_COLUMNS) == _records(queue, BASE_QUEUE_COLUMNS)


def test_corrected_legacy_derivation_parity() -> None:
    unified = _sample_unified()
    resolved, queue = derive_legacy_outputs(unified, include_corrections=True)

    assert list(resolved.columns) == CORRECTED_RESOLVED_COLUMNS
    assert list(queue.columns) == CORRECTED_QUEUE_COLUMNS

    assert _records(unified, CORRECTED_RESOLVED_COLUMNS) == _records(resolved, CORRECTED_RESOLVED_COLUMNS)

    unified_queue = unified.loc[unified["needs_review"]].copy()
    assert _records(unified_queue, CORRECTED_QUEUE_COLUMNS) == _records(queue, CORRECTED_QUEUE_COLUMNS)


def test_direct_review_queue_matches_legacy_queue() -> None:
    unified = _sample_unified()
    _, queue_legacy = derive_legacy_outputs(unified, include_corrections=False)
    queue_direct = derive_review_queue(unified, include_corrections=False)
    assert _records(queue_legacy, BASE_QUEUE_COLUMNS) == _records(queue_direct, BASE_QUEUE_COLUMNS)
