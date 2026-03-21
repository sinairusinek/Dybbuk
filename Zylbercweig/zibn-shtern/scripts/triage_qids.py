from __future__ import annotations

import argparse

from zibn_shtern.io import load_places, save_dataframe
from zibn_shtern.triage import (
    add_review_flags,
    derive_legacy_outputs,
    ensure_unified_schema,
    enrich_and_classify,
    explode_resolved_places,
)
from zibn_shtern.wikidata_client import get_qid_details


def main() -> None:
    parser = argparse.ArgumentParser(description="Triage place/province/country QIDs into a unified place table")
    parser.add_argument("--input", required=True, help="Input CSV/TSV/JSON/XLSX")
    parser.add_argument("--cache", default="data/working/qid_lookup.json", help="Wikidata lookup cache path")
    parser.add_argument(
        "--output",
        default="data/working/places_unified.csv",
        help="Unified output file with all triage and review metadata",
    )
    parser.add_argument(
        "--legacy-outputs",
        action="store_true",
        help="Also derive legacy resolved/review outputs from the unified table",
    )
    parser.add_argument(
        "--resolved-output",
        default=None,
        help="Optional legacy resolved output path (implies legacy output generation)",
    )
    parser.add_argument(
        "--review-output",
        default=None,
        help="Optional legacy review output path (implies legacy output generation)",
    )
    parser.add_argument("--refresh-cache", action="store_true", help="Re-fetch Wikidata entities even if already cached")
    parser.add_argument(
        "--ancestor-depth",
        type=int,
        default=6,
        help="How many P131 levels to fetch from Wikidata for geographic validation",
    )
    parser.add_argument(
        "--chain-depth",
        type=int,
        default=6,
        help="How many P131 levels to traverse in coherence checks",
    )
    args = parser.parse_args()

    emit_legacy = bool(args.legacy_outputs or args.resolved_output or args.review_output)
    legacy_resolved_path = args.resolved_output or "data/working/resolved_places.csv"
    legacy_review_path = args.review_output or "data/working/qid_review_queue.csv"

    df = load_places(args.input)

    resolved = explode_resolved_places(df)
    qids = sorted(set(resolved["qid"].dropna().astype(str).tolist())) if not resolved.empty else []

    details = get_qid_details(
        qids,
        cache_path=args.cache,
        refresh=args.refresh_cache,
        ancestor_depth=args.ancestor_depth,
    )
    classified = enrich_and_classify(resolved, details)
    flagged = add_review_flags(classified, details, chain_depth=args.chain_depth)
    unified = ensure_unified_schema(flagged)

    save_dataframe(unified, args.output)

    if emit_legacy:
        resolved_legacy, queue_legacy = derive_legacy_outputs(unified, include_corrections=False)
        save_dataframe(resolved_legacy, legacy_resolved_path)
        save_dataframe(queue_legacy, legacy_review_path)


if __name__ == "__main__":
    main()
