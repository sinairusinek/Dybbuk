# Unified toponym dataset

An **attestation spine** plus two derived views, linking every place mention —
person *and* organization — to Wikidata / Kima while preserving a back-pointer to
its source record. Built by `scripts/build_unified_toponyms.py` (re-run after any
source list changes).

## Outputs

### `data/working/toponyms_attestations.csv` — the spine (18,790 rows)
One row per attestation, **never deduplicated**, for both corpora. Each row keeps
the link back to its source record so resolved QIDs/Kima IDs can be enriched back
into the people and organization data:

| corpus | `source_record_id` | enrich-back chain |
|---|---|---|
| `person` (7,167) | `entry_id` | → person edition record |
| `org` (11,623) | `cluster_id` (+ `org_db_id`) | → org cluster → core_db org |

Key columns: `attestation_id`, `source_corpus`, `source_record_id`, `org_db_id`,
`source_field` (place/province/country | settlements/venues/addresses/countries),
`context`, `source_value` (Yiddish, verbatim), `source_value_script`,
`link_status` (`linked` | `needs_review` | `unlinked`), the QID-keyed resolution
(`qid`, `label_en`, `label_yi`, `place_type`, `category`, `kima_id`, `kima_rom`,
`kima_heb`, `lat`, `lon`), `maaty_qid`/`maaty_qid_conflict` (person alt-resolution,
flagged — never overwrites), `suggested_qid`/`suggested_english` (for unlinked),
`is_descriptor`, `review_flags`.

Status spread: 5,339 `linked` (incl. 203 relinked via Maaty — `relink_source=maaty`,
original bad QID kept in `rejected_qid`) + 747 `needs_review` + 14 `misresolved`
(non-place QID, no valid alternate — see below) + 12,690 `unlinked`.

**Attested vs resolved:** `source_value` is always Yiddish (the attested toponym, for
both person and LLM-extracted org places); the Latin forms (`label_en`, `kima_rom`)
are the *resolved* canonical names — matching output, not source data.

### `data/working/toponyms_gazetteer.csv` — per place (883), DERIVED
Group-by of linked attestations on `qid`: canonical labels, type, Kima IDs, coords,
`n_attestations` / `n_person` / `n_org` / `n_flagged_mentions`, `fields`, all
`variants` (`;`-joined), `corpora`, `external_sources` (`wikidata`[`;kima`]).
Every QID has ≥1 attestation. Only **54** places currently carry org attestations —
the rest of the org place backlog is still unlinked. **448/883 places (50%)** are
linked to Kima (336 from the kimatch pipeline + 112 backfilled by QID via the
kimatch skill — see `data/working/kima/`).

### `data/working/toponyms_misresolved.csv` — per rejected QID (13), DERIVED
QIDs whose Wikidata type proves they are **not places** — the Yiddish toponym matched a
wrong entity. Surfaced by the Kima run; detected in-build by `nonplace_kind()` from
`place_type`/`category`. The attestation is marked `link_status=misresolved` (bad QID
moved to `rejected_qid`, kept out of the gazetteer); the Yiddish `source_value` is
preserved for re-linking. Columns: `rejected_qid`, `wrong_kind`, `wrong_label_en`,
`wrong_type`, `n_attestations`, `corpora`, `variants`, `attestation_ids`.

Originally 111 QIDs / 217 attestations. **203 were relinked** to a Maaty alternate QID
validated as a place (Kima membership or Wikidata P31 — see `kima/maaty_relink_validated.tsv`),
leaving only **14 attestations / 13 QIDs** with no valid alternate.

### `data/working/toponyms_unlinked.csv` — per unresolved spelling (4,469), DERIVED
The to-be-linked worklist. One row per distinct spelling, but **every underlying
attestation still lives in the spine** — `attestation_ids` lists them, so each entry
maps straight back. Columns: `variant`, `script`, `corpora` (`person`/`org`/both),
`occurrences`, `fields`, `contexts`, `suggested_qid`/`suggested_english` (131 have
one), `is_descriptor` (160 generic non-places like "a town near Warsaw" — kept,
filterable; detection is NFKD-normalized so it is point-insensitive),
`attestation_ids`.

## Source lists
| List | Role |
|---|---|
| `places_unified_corrected.csv` | linked person attestations (resolved hub) |
| `data/raw/Zylbercweig-Extraction…places.tsv` | recovers **unlinked** person attestations (with `entry_id`) |
| `kimatch_matched_full.tsv` / `kima_variants_export.tsv` | Kima IDs + variants per QID |
| `../ZylbercweigPlacesMaaty.tsv` | alternate person QID/English (flagged) |
| `../organizations/org_alignment_review.tsv` | **all** org place fields per `cluster_id` |
| `../organizations/settlement_variant_collapse_audit_2026-05-20.tsv` | org `(cluster_id, variant)` → QID resolution |
| `../organizations/settlement_coords.tsv` | lat/lon per QID |
| `../organizations/unresolved_settlements_punchlist.tsv` | `suggested_qid` for unlinked org spellings |
| `data/working/kima/kima_backfill_confirmed.tsv` | QID-confirmed Kima IDs from the kimatch skill (supplements kimatch, never overrides) |
| `data/working/kima/maaty_relink_validated.tsv` | Maaty alternate QIDs validated as places — relink source for mis-resolved attestations (+ Kima IDs for 43) |

Notes:
- 4 malformed "Belarus" rows (country in the place slot) are parked in
  `data/raw/_belarus_country_in_place_TODO.tsv`, excluded, pending curation.
- The large unlinked **org** backlog (3,804 distinct spellings) is the main lever
  for better org blocking/clustering: a settlement→QID-per-cluster spine is a strong
  blocking key.
