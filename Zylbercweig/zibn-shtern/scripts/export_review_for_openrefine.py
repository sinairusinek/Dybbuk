from __future__ import annotations

import argparse
import re

import pandas as pd

from zibn_shtern.io import load_places, save_dataframe


DEFAULT_COLUMNS = [
    "entry_id",
    "row_type",
    "context",
    "source_role",
    "source_value",
    "clustered_value",
    "wikidata_url",
    "wikidata_label_en",
    "wikidata_type",
    "resolved_category",
    "review_flags",
    "qid_source",
    "ra_qid",
    "ra_resolved_category",
    "ra_notes",
]

MISMATCH_FLAGS = {
    "place_country_mismatch",
    "place_province_mismatch",
    "province_country_mismatch",
    "province_role_mismatch",
    "country_role_mismatch",
    "place_country_unresolved",
    "province_country_unresolved",
}

SOURCE_LABELS = {
    "initial": "automatic reconciliation",
    "dodgy": "reviewer reconciliation",
}

QID_PATTERN = re.compile(r"^Q\d+$")


def _as_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    if text.lower() == "nan":
        return ""
    return text.strip()


def _to_wikidata_url(value: object) -> str:
    text = _as_text(value)
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if QID_PATTERN.match(text):
        return f"https://www.wikidata.org/wiki/{text}"
    return text


def _has_mismatch_flag(value: object) -> bool:
    flags = [flag.strip() for flag in _as_text(value).split(";") if flag.strip()]
    return any(flag in MISMATCH_FLAGS for flag in flags)


def _row_key(row: pd.Series) -> tuple[str, str, str, str, str, str]:
    return (
        _as_text(row.get("entry_id")),
        _as_text(row.get("context")),
        _as_text(row.get("source_role")),
        _as_text(row.get("source_value")),
        _as_text(row.get("clustered_value")),
        _as_text(row.get("qid")),
    )


def _to_output_row(row: pd.Series, row_type: str, blank_entry_id: bool = False, clear_review_flags: bool = False) -> dict[str, str]:
    qid_source = _as_text(row.get("qid_source"))
    return {
        "entry_id": "" if blank_entry_id else _as_text(row.get("entry_id")),
        "row_type": row_type,
        "context": _as_text(row.get("context")),
        "source_role": _as_text(row.get("source_role")),
        "source_value": _as_text(row.get("source_value")),
        "clustered_value": _as_text(row.get("clustered_value")),
        "wikidata_url": _to_wikidata_url(row.get("qid")),
        "wikidata_label_en": _as_text(row.get("wikidata_label_en")),
        "wikidata_type": _as_text(row.get("wikidata_type")),
        "resolved_category": _as_text(row.get("resolved_category")),
        "review_flags": "" if clear_review_flags else _as_text(row.get("review_flags")),
        "qid_source": SOURCE_LABELS.get(qid_source, qid_source),
        "ra_qid": _to_wikidata_url(row.get("ra_qid")),
        "ra_resolved_category": _as_text(row.get("ra_resolved_category")),
        "ra_notes": _as_text(row.get("ra_notes")),
    }


def _build_review_key_set(review_df: pd.DataFrame) -> set[tuple[str, str, str, str, str, str]]:
    return {_row_key(row) for _, row in review_df.iterrows()}


def _add_context_rows(review_df: pd.DataFrame, unified_df: pd.DataFrame) -> pd.DataFrame:
    review_keys = _build_review_key_set(review_df)
    grouped_unified: dict[tuple[str, str], pd.DataFrame] = {
        (entry_id, context): group
        for (entry_id, context), group in unified_df.groupby(["entry_id", "context"], dropna=False)
    }

    rows: list[dict[str, str]] = []
    context_inserted_for_group: set[tuple[str, str]] = set()

    for _, review_row in review_df.iterrows():
        rows.append(_to_output_row(review_row, row_type="flagged"))

        if not _has_mismatch_flag(review_row.get("review_flags")):
            continue

        group_key = (_as_text(review_row.get("entry_id")), _as_text(review_row.get("context")))
        if group_key in context_inserted_for_group:
            continue
        context_inserted_for_group.add(group_key)

        sibling_group = grouped_unified.get(group_key)
        if sibling_group is None or sibling_group.empty:
            continue

        sibling_group = sibling_group.sort_values(["source_role", "source_value", "qid"], kind="stable")
        for _, sibling_row in sibling_group.iterrows():
            if _row_key(sibling_row) in review_keys:
                continue
            rows.append(
                _to_output_row(
                    sibling_row,
                    row_type="context",
                    blank_entry_id=True,
                    clear_review_flags=True,
                )
            )

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export reviewer queue in OpenRefine-friendly TSV format")
    parser.add_argument("--input", required=True, help="Review queue CSV/TSV/JSON")
    parser.add_argument("--output", required=True, help="OpenRefine export path (recommended: .tsv)")
    parser.add_argument(
        "--unified-input",
        default="data/working/places_unified_corrected.csv",
        help="Unified places CSV/TSV used to attach sibling context rows for mismatch flags",
    )
    parser.add_argument(
        "--keep-all-columns",
        action="store_true",
        help="Keep all input columns and prepend reviewer task columns",
    )
    args = parser.parse_args()

    df = load_places(args.input)
    if df.empty:
        raise ValueError("Input queue is empty; nothing to export")

    out = df.copy()
    out["ra_qid"] = out.get("ra_qid", "")
    out["ra_resolved_category"] = out.get("ra_resolved_category", "")
    out["ra_notes"] = out.get("ra_notes", "")
    out = out.sort_values(["review_flags", "entry_id", "context", "source_role"], kind="stable")

    if not args.keep_all_columns:
        unified = load_places(args.unified_input)
        out = _add_context_rows(out, unified)
        out = out[DEFAULT_COLUMNS]
    else:
        out["qid_source"] = out["qid_source"].map(lambda v: SOURCE_LABELS.get(_as_text(v), _as_text(v)))
        out["qid"] = out["qid"].map(_to_wikidata_url)
        out["ra_qid"] = out["ra_qid"].map(_to_wikidata_url)

    save_dataframe(out, args.output)


if __name__ == "__main__":
    main()