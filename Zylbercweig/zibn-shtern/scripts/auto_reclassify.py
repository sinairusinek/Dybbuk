"""Auto-reclassify: full triage + automated corrections pass.

Runs the complete place-triage pipeline (picking up the classify_qid
keyword-order fix) and then applies three automated correction passes before
building the human-review queue.  Outputs go to new *_corrected files so the
base triage outputs are preserved for comparison.

Usage example
-------------
python scripts/auto_reclassify.py \\
    --input  data/raw/Zylbercweig-Extraction2026-02-05-places.tsv \\
    --output data/working/places_unified_corrected.csv \\
    --legacy-outputs

By default the existing QID cache is reused (no new Wikidata fetches unless
QID substitution in fix_city_state adds new entries).  Pass --refresh-cache
to force a full re-fetch.
"""
from __future__ import annotations

import argparse
from collections import Counter

from zibn_shtern.corrections import fix_city_state, fix_column_assignment, fix_death_site_burial
from zibn_shtern.io import load_places, save_dataframe
from zibn_shtern.triage import (
    add_review_flags,
    derive_legacy_outputs,
    derive_review_queue,
    ensure_unified_schema,
    enrich_and_classify,
    explode_resolved_places,
)
from zibn_shtern.wikidata_client import get_qid_details, load_cache


def _print_category_counts(df, label: str) -> None:
    counts = Counter(df["resolved_category"].dropna().astype(str))
    total = len(df)
    needs_review = int(df["needs_review"].astype(str).str.lower().eq("true").sum()) if "needs_review" in df.columns else "n/a"
    print(f"\n{label}  (total rows: {total}, needs_review: {needs_review})")
    for cat, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {cat:<25} {count}")


def _print_correction_summary(df) -> None:
    if "correction_applied" not in df.columns:
        return
    counts = Counter(df["correction_applied"].dropna().astype(str))
    counts.pop("", None)
    if not counts:
        print("  (no corrections applied)")
        return
    for kind, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {kind:<35} {count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Triage + automated reclassification pipeline"
    )
    parser.add_argument("--input", required=True, help="Raw input TSV/CSV/XLSX")
    parser.add_argument(
        "--cache",
        default="data/working/qid_lookup.json",
        help="Wikidata lookup cache path",
    )
    parser.add_argument(
        "--output",
        default="data/working/places_unified_corrected.csv",
        help="Unified corrected output file",
    )
    parser.add_argument(
        "--legacy-outputs",
        action="store_true",
        help="Also derive legacy corrected resolved/review outputs from unified data",
    )
    parser.add_argument(
        "--resolved-output",
        default=None,
        help="Optional legacy corrected resolved output path (implies legacy output generation)",
    )
    parser.add_argument(
        "--review-output",
        default=None,
        help="Optional legacy corrected review output path (implies legacy output generation)",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Re-fetch Wikidata entities even if already cached",
    )
    parser.add_argument("--ancestor-depth", type=int, default=6)
    parser.add_argument("--chain-depth", type=int, default=6)
    args = parser.parse_args()

    emit_legacy = bool(args.legacy_outputs or args.resolved_output or args.review_output)
    legacy_resolved_path = args.resolved_output or "data/working/resolved_places_corrected.csv"
    legacy_review_path = args.review_output or "data/working/qid_review_queue_corrected.csv"

    # ------------------------------------------------------------------
    # Step 1: Full triage pipeline (classify_qid ordering fix is live)
    # ------------------------------------------------------------------
    print("=== Step 1: Triage pipeline ===")
    df = load_places(args.input)
    resolved = explode_resolved_places(df)
    qids = sorted(set(resolved["qid"].dropna().astype(str).tolist())) if not resolved.empty else []
    print(f"  {len(qids)} unique QIDs to classify")

    details = get_qid_details(
        qids,
        cache_path=args.cache,
        refresh=args.refresh_cache,
        ancestor_depth=args.ancestor_depth,
    )
    classified = enrich_and_classify(resolved, details)

    # Compute a preliminary needs_review so _print_category_counts can show it.
    classified["needs_review"] = False
    _print_category_counts(classified, "BEFORE corrections")

    # ------------------------------------------------------------------
    # Step 2: Automated corrections
    # ------------------------------------------------------------------
    print("\n=== Step 2: Automated corrections ===")

    classified = fix_death_site_burial(classified)
    classified = fix_city_state(classified, details, cache_path=args.cache)

    # Reload details after fix_city_state may have added new QIDs to cache.
    updated_cache = load_cache(args.cache)
    for qid, val in updated_cache.items():
        if qid not in details:
            details[qid] = val

    classified = fix_column_assignment(classified, details)

    print("\nCorrections applied:")
    _print_correction_summary(classified)

    # ------------------------------------------------------------------
    # Step 3: Re-run flagging on corrected data
    # ------------------------------------------------------------------
    print("\n=== Step 3: Re-run review flagging ===")
    flagged = add_review_flags(classified, details, chain_depth=args.chain_depth)
    unified = ensure_unified_schema(flagged)
    queue = derive_review_queue(unified, include_corrections=True)

    _print_category_counts(flagged, "AFTER corrections")

    # Flag breakdown for review queue
    flag_counts: Counter = Counter()
    for flags_str in queue["review_flags"].dropna():
        flag_counts.update(f for f in str(flags_str).split(";") if f)
    print(f"\nReview queue: {len(queue)} rows")
    print("Top review flags:")
    for flag, count in flag_counts.most_common(12):
        print(f"  {flag:<35} {count}")

    # ------------------------------------------------------------------
    # Step 4: Save outputs
    # ------------------------------------------------------------------
    print("\n=== Step 4: Saving outputs ===")
    save_dataframe(unified, args.output)
    print(f"  Unified output  → {args.output}")

    if emit_legacy:
        resolved_legacy, queue_legacy = derive_legacy_outputs(unified, include_corrections=True)
        save_dataframe(resolved_legacy, legacy_resolved_path)
        print(f"  Resolved places → {legacy_resolved_path}")
        save_dataframe(queue_legacy, legacy_review_path)
        print(f"  Review queue    → {legacy_review_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
