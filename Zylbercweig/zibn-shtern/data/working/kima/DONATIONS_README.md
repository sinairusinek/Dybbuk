# Unified Kima donations (2026-05-22)

Gated, deduped contribution payload across all three datasets, built by
`scripts/export_unified_donations.py`. The Kima API is read-only — these files are
handed to the Kima team manually.

## How it's gated
Each source contributes only its **confident, Kima-confirmed** matches, and only
**new** variants (those Kima doesn't already have for that place are kept; existing
ones are dropped):
- **Zylbercweig** — `kimatch_matched_full.tsv` rows with `grade==A_autolink` AND
  `is_new_variant==yes` (the guarded-engine re-match). → 293 new
- **YIVO Yiddishland** — `yivo_yiddishland_kima.A_autolink.csv`; novelty checked
  against the live Kima variant index. → 2 new (its grade-A spellings are almost all
  already Kima variants)
- **Fischer** — `fischer_donations.tsv` (already KEEP-gated, new-only). → 10,648 new

Dedup key: `(kima_id, normalized-Hebrew variant)`. The same spelling for the same
place from >1 dataset collapses to one row, merging provenance (**78 corroborated
across datasets**).

## Files
- **`donations_unified_variants.tsv`** — **10,888 clean variant donations.** Cols:
  `kima_id, kima_rom, variant, variant_norm, n_datasets, datasets, sources, provenance`.
  Ready to hand off.
- **`donations_variants_NEEDS_REVIEW_epithets.tsv`** — **55** variants that are not
  clean name forms (carry a `variant_type`): `parenthetical` (14, e.g. סאַנדאָמיר (צויזמער)),
  `locator` (21, e.g. קאלין במדינת בעהמין = "Kolín in Bohemia"), `descriptor`/calque
  (14, e.g. מרחץ רייכענהאל, עיר לבן), `conjunction` (6, e.g. זאמאשטש און לובלין —
  names *two* places). A human (or Kima) decides which to keep, with what label.
- **`donations_unified_external_ids.tsv`** — **6,303** external IDs (Fischer only:
  kagan/jewishgen/us_bgn/ys) for confirmed places. Kima's live API tracks only
  MAZAL/NAF/VIAF/GeoNames/WikiData, so these are *proposed new id-types*.
- **`donations_external_ids_NEEDS_REVIEW_negative.tsv`** — **3,606** `us_bgn` values
  that are negative (a sign/encoding artifact in the Fischer source) — quarantined
  until the source is fixed; do not donate as-is.

## Provenance / audit
All variant donations trace to grade-A / KEEP matches that passed the guarded engine
(ambiguity + phonetic-mismatch + geo guards). The matching-corrections registry
(`matching_corrections_log.tsv`) records the manual fixes folded in upstream.
