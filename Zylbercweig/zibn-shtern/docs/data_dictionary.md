# Data dictionary

This dictionary describes the unified QID-triage output written by:

- `scripts/triage_qids.py` (base triage)
- `scripts/auto_reclassify.py` (triage + automated corrections)

The unified table contains one row per resolved QID candidate and is the
single source from which legacy outputs are derived.

## Unified output schema

### 1) Identity and source context

- `entry_id`: stable row identifier from the extraction source (`Column 1`).
- `context`: lifecycle context of the place mention (`birth`, `death`, `burial`, etc.).
- `source_role`: source field role that produced this row (`place`, `province`, `country`).
- `source_value`: original text value from the role column.
- `clustered_value`: clustered/normalized value used for reconciliation lookup.

### 2) QID reconciliation metadata

- `qid`: resolved Wikidata entity id (`Q...`).
- `qid_source`: source of the selected place QID (`initial`, `dodgy`, `province`, `country`).
- `place_qid_conflict`: true when initial and dodgy place reconciliations disagree.

### 3) Wikidata enrichment

- `wikidata_label_en`: English label from Wikidata for `qid`.
- `wikidata_label_yi`: Yiddish label from Wikidata when available.
- `wikidata_type`: comma-joined `instance of` labels (`P31`) used in classification.

### 4) Classification fields

- `resolved_category`: primary type classification for this row.
- `other_type`: fallback primary type label when `resolved_category = other`.

Category value columns (exactly one is populated per row, the rest empty):

- `cemetery`
- `burial_city`
- `death_site`
- `settlement`
- `province`
- `country`
- `neighborhood`
- `other`

Resolved category vocabulary:

- `cemetery`
- `burial_city`
- `death_site`
- `settlement`
- `province`
- `country`
- `neighborhood`
- `other`

### 5) Review fields

- `review_flags`: semicolon-delimited review flags for row-level and group-level checks.
- `needs_review`: true when `review_flags` is non-empty.

Review flag semantics:

- `place_qid_conflict`: place initial/dodgy QIDs disagree.
- `province_role_mismatch`: a province-role row did not classify as province.
- `country_role_mismatch`: a country-role row did not classify as country.
- `place_is_admin_region`: place-role row classified as province/admin-region.
- `needs_manual_type_review`: classifier fell back to `other`.
- `place_country_mismatch`: place country evidence conflicts with explicit country row(s).
- `place_province_mismatch`: place administrative chain does not include explicit province row(s).
- `province_country_mismatch`: province country evidence conflicts with explicit country row(s).
- `place_country_unresolved`: no country evidence found for place via `P17`/`P131` chain.
- `province_country_unresolved`: no country evidence found for province via `P17`/`P131` chain.
- `qid_lookup_missing`: no Wikidata detail found for a referenced QID.

### 6) Automated correction tracking

- `correction_applied`: correction label applied by auto-reclassify pipeline, else empty.
- `death_burial_conflict`: true when both `death_site` and `burial_city` are populated with different values.

Correction labels:

- `death_burial_mirrored`: death and burial fields mirrored for death-site logic.
- `state_to_city`: province-typed place QID replaced by settlement/city QID.
- `moved_to_province`: row reassigned to province role.
- `moved_to_country`: row reassigned to country role.

## Legacy outputs (derived views)

Legacy files are optional and derivable from the unified table:

- `resolved_places.csv`: full unified rows without correction columns (base mode)
- `qid_review_queue.csv`: `needs_review = true` subset, reviewer-first column order
- `resolved_places_corrected.csv`: full rows including correction columns
- `qid_review_queue_corrected.csv`: review subset including correction columns

No information exists only in legacy files; they are projections of unified data.

## Correction principles

- Never overwrite source values (`source_value`) or source identity (`entry_id`, `context`, `source_role`).
- Keep corrections row-level and attributable via `correction_applied`.
- Keep review semantics explicit via `review_flags` and `needs_review`.
