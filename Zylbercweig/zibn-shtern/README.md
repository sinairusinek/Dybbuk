# Zibn Shtern

Workspace for auditing and normalizing place names extracted from Zalmen Zylbercweig's *Lexicon of Yiddish Theater*.

## Scope

This repository is organized around four phases:

1. **Review extracted data**
2. **Audit and flag issues**
3. **Apply and track corrections**
4. **Bridge to Kimatch and adapt the kima reviewer from Hasidigital**

## Project layout

- `data/raw/` — immutable source exports (CSV/TSV/JSON/XLSX)
- `data/working/` — intermediate cleaned tables and generated review queues
- `data/corrected/` — approved corrections and canonicalized outputs
- `data/reference/` — optional authority files (GeoNames, Wikidata subsets, gazetteers)
- `scripts/` — command-line entry scripts
- `src/zibn_shtern/` — core Python package
- `configs/` — audit rules and integration configuration
- `docs/` — workflow notes and field definitions

## Quick start

1. Create/activate a Python 3.11+ environment.
2. Install dependencies:
   - `pip install -e .`
3. Add a source file to `data/raw/` (CSV/TSV/JSON/XLSX).
4. Run audit:
   - `python scripts/audit_places.py --input data/raw/Zylbercweig-Extraction2026-02-05-places.tsv --report data/working/audit_report.csv`
5. Build reviewer queue:
   - `python scripts/build_review_queue.py --audit data/working/audit_report.csv --output data/working/review_queue.csv`
6. Triage reconciled QIDs and create unified output (default):
   - `python scripts/triage_qids.py --input data/raw/Zylbercweig-Extraction2026-02-05-places.tsv --output data/working/places_unified.csv --ancestor-depth 6 --chain-depth 6`
7. (Optional) also emit legacy resolved/review outputs from the unified table:
   - `python scripts/triage_qids.py --input data/raw/Zylbercweig-Extraction2026-02-05-places.tsv --output data/working/places_unified.csv --legacy-outputs --resolved-output data/working/resolved_places.csv --review-output data/working/qid_review_queue.csv --ancestor-depth 6 --chain-depth 6`
8. Auto-triage the review queue before any human review (resolve mechanical
   cases; hand off only judgment calls). `auto_reclassify.py` runs these
   correction passes in order: `fix_qid_overrides`, `fix_unlink_nonplace`
   (clear non-place QIDs — human/taxon/film… — corpus-wide), `fix_rematch_backfill`
   (re-link spellings recovered by the kimatch re-match), `fix_death_site_burial`,
   `fix_city_state`, `fix_column_assignment`, then `add_review_flags` (which
   applies the `HISTORICAL_REGIONS` / `HISTORICAL_SOVEREIGNTY` allow-lists to
   suppress expected historical-vs-current hierarchy conflicts).
   To recover unlinked spellings (step 6 / re-match):
   - build the input from the `unlinked_nonplace` rows → `data/working/kima/rematch_unlinked_input.tsv` (clustered_value as `english_name`)
   - `python scripts/kimatch_match.py --input data/working/kima/rematch_unlinked_input.tsv --suffix _rematch`
   - map `kima_id`→`WikiData_Id` into `data/working/kima/rematch_confirmed.tsv`, then re-run `auto_reclassify.py`
9. Prepare OpenRefine reviewer task file from the residual judgment calls
   (`needs_review` minus rows still pending re-match):
   - `python scripts/export_review_for_openrefine.py --input data/working/qid_review_queue_corrected.csv --output data/working/openrefine_review_queue.tsv`

## Current assumptions

- Source table includes at least a `place` column.
- Current export includes: `context`, `Column 1`, `place`, `place clustered`, `initial reconciliation for place`, `province`, `province clustered`, `province Qid`, `country`, `country cluster`, `Country Qid`.
- Metadata columns `File`, `"""action"""`, and `toKima` are ignored in triage/classification.
- Corrections are tracked as explicit row-level decisions, never destructive in-place edits.

See `docs/workflow.md` for process details and `docs/data_dictionary.md` for field-level definitions.

## Output migration note

- Primary outputs are now unified tables:
   - `data/working/places_unified.csv`
   - `data/working/places_unified_corrected.csv`
- Legacy files are still supported and are derived views, produced by `triage_qids.py`:
   - `data/working/resolved_places.csv`
   - `data/working/qid_review_queue.csv`
- To produce legacy files, run `triage_qids.py` with `--legacy-outputs` (or pass explicit `--resolved-output` / `--review-output` paths). `auto_reclassify.py` emits only the unified corrected table; `places_unified_corrected.csv` is the single source of truth for corrected data.
