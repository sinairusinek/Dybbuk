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

`auto_reclassify.py` emits only the unified corrected table; `places_unified_corrected.csv` is the single source of truth for corrected data.

## 5) Reviewer handoff — OpenRefine manual QID correction

Primary reviewer handoff is the unified table filtered to `needs_review = true`.
Legacy handoff remains `qid_review_queue.csv` from `triage_qids.py --legacy-outputs`.

For OpenRefine review sessions, export a dedicated reviewer task TSV:

- `python scripts/export_review_for_openrefine.py --input data/working/qid_review_queue_corrected.csv --output data/working/openrefine_review_queue.tsv`

Recommended OpenRefine workflow:

- Import `openrefine_review_queue.tsv` (1624 rows, auto-reconciled — many QIDs are wrong/disambiguation pages)
- Reconcile `clustered_value` against Wikidata
- Fill `ra_qid`, `ra_resolved_category`, and `ra_notes`
- Export rows as TSV/CSV (or full OpenRefine project archive)

**Status (2026-04-09):** Matty has completed a partial review of the OpenRefine queue, producing
`ZylbercweigPlacesMaaty.tsv` (934 rows with human-verified QIDs). This file is the reviewed
subset of the OpenRefine queue — it replaces the auto-reconciled QIDs with correct ones.
The remaining ~690 rows in the queue have not yet been reviewed.

`ZylbercweigPlacesMaaty.tsv` lives at the root of the Zylbercweig folder (sibling of `zibn-shtern/`).

## 6) Kimatch matching — map to Kima Gazetteer

Run `kimatch_match.py --full` to match all resolved places from `places_unified_corrected.csv`
against the Kima Gazetteer. The bridge (`kimatch_bridge.py`) extracts `source_role=place` +
`needs_review=False` rows and passes all relevant fields to the matcher, including
`resolved_category` (place type) which flows through to the review UI.

Matching strategy (in priority order):
1. Wikidata QID → `db.get_by_wikidata()`
2. Exact Yiddish name (`source_value`)
3. Exact Wikidata Yiddish label (`wikidata_yi`)
4. Exact English/romanized name (`english_name`)
5. Fuzzy — trigram similarity + **phonetic pass** (DM soundex + cross-script IPA)
6. No match

The phonetic pass (added 2026-04-09) uses:
- `dybbuk_phonetic` (`dybbuk-phonetic/src/`) for Yiddish→IPA→DM soundex
- `abydos.DaitchMokotoff` for the soundex index (built at KimaDB load time)
- `_best_name_sim()` to score against the core toponym only (strips regional qualifiers
  like "Jedrzejow (Województwo Świętokrzyskie, Poland)")

```
python scripts/kimatch_match.py --full
```

**Current state** (2026-04-09):

Outputs:
- `data/working/kimatch_input_full.tsv` — 2,689 filtered rows (bridge output)
- `data/working/kimatch_matched_full.tsv` — 539 confirmed (493 WIKIDATA + 46 NAME_EXACT), 496 new variants
- `data/working/kimatch_review_full.tsv` — 125 rows for manual review (22 FUZZY + 103 NO_MATCH)

Note: `ZylbercweigPlacesMaaty.tsv` (Matty's manually-reconciled subset) was an earlier partial run.
Its outputs (`kimatch_matched.tsv`, `kimatch_review.tsv`, `kimatch_decisions.json`) are superseded
by the full pipeline and kept only as an audit trail.

The ~690 unreviewed OpenRefine rows remain unresolved and are not passed to Kimatch.

## 7) Kimatch review — resolve FUZZY and NO_MATCH

Manually resolve rows in `data/working/kimatch_review_full.tsv` via the Kimatch Streamlit app:

```
cd /Users/sinairusinek/Documents/GitHub/Kimatch
streamlit run ui/app.py
# → 🗺 Zylbercweig Review page
```

Review queue breakdown:
- **FUZZY (22 rows)**: fuzzy candidates found — confirm or reject
- **NO_MATCH (103 rows)**: not found automatically
  - ~47 American cemeteries: likely not in Kima
  - ~56 settlements: search manually or mark "No match found"

Filter by place type (sidebar multiselect): cemetery · settlement · death_site · neighborhood

Actions: `map_to:<kima_id>` | `no_match_found` | `ambiguous` (name = multiple places) | `skip`

Decisions saved to `data/working/kimatch_decisions_full.json`.

**Status (2026-04-09):** 9 manual map_to decisions confirmed. 125 rows remain.

## 8) Variant export — contribute back to Kima

After review, regenerate the export:

```
python scripts/export_kima_variants.py
```

Sources:
- `kimatch_matched_full.tsv` — auto-confirmed matches (`is_new_variant=yes`)
- `kimatch_decisions_full.json` — manually confirmed `map_to:` decisions

Output: `data/working/kima_variants_export.tsv`

**Current state:** ~502 variants (496 auto + 6 manual, pre-completion of review).
Format: `kima_id | kima_rom | variant | source | attestations | contexts | wikidata_qid | notes`

## 9) Contribute variants to Kima

Submit `kima_variants_export.tsv` to the Kima team for ingestion into the gazetteer.
