# Workflow

## 1) Ingest

- Place extraction output in `data/raw/`.
- Do not edit files in `data/raw/` directly.
- Use dated source snapshots for provenance.

## 2) Unified QID triage (primary output)

Run base triage:

- `python scripts/triage_qids.py --input data/raw/Zylbercweig-Extraction2026-02-05-places.tsv --output data/working/places_unified.csv`

This creates one unified table containing:

- source identity/context
- QID reconciliation metadata
- Wikidata enrichment
- resolved category outputs
- review flags and `needs_review`

## 3) Optional legacy outputs (derived)

If downstream consumers still require split files, derive them from unified data:

- `python scripts/triage_qids.py --input data/raw/Zylbercweig-Extraction2026-02-05-places.tsv --output data/working/places_unified.csv --legacy-outputs`

This writes:

- `data/working/resolved_places.csv`
- `data/working/qid_review_queue.csv`

These are projections of the unified table (no additional information).

## 4) Auto-reclassification pass (corrected unified output)

Run corrected pipeline:

- `python scripts/auto_reclassify.py --input data/raw/Zylbercweig-Extraction2026-02-05-places.tsv --output data/working/places_unified_corrected.csv`

Optional legacy corrected outputs:

- `python scripts/auto_reclassify.py --input data/raw/Zylbercweig-Extraction2026-02-05-places.tsv --output data/working/places_unified_corrected.csv --legacy-outputs`

This writes:

- `data/working/resolved_places_corrected.csv`
- `data/working/qid_review_queue_corrected.csv`

## 5) Reviewer handoff

Primary reviewer handoff is the unified table filtered to `needs_review = true`.
Legacy handoff remains `qid_review_queue*.csv` when `--legacy-outputs` is used.

## 6) Kimatch handoff

After review/corrections are approved:

- export Kimatch input using `scripts/export_kimatch_input.py`
- map fields to Kimatch schema in `src/zibn_shtern/kimatch_bridge.py`
- keep mapping versioned and documented

## 7) Reviewer adapter (Hasidigital)

- Inspect Hasidigital reviewer schema and event model.
- Implement schema adapter in `src/zibn_shtern/reviewer_adapter.py`.
- Preserve immutable source identifiers for roundtrip consistency.
