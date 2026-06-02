# TODO: Coordinate the four new core_db.tsv schema columns

PI-approved across 2026-05-31 → 2026-06-02 as part of closing the off-candidate
audit (see `HANDOFF_data_defects_and_matcher.md` + the
`project_off_candidate_audit` memory). Four new columns on `core_db.tsv`:

| column | added | meaning |
|---|---|---|
| `deprecated` | 05-31 | `"true"` if the row has been merged into another; empty otherwise |
| `merged_into` | 05-31 | the canonical `db_id` it was merged into (when deprecated) |
| `name_variants` | 06-01 | pipe-separated alternate attested spellings (orthographic / declension) |
| `out_of_project` | 06-02 | `"true"` if the row is outside the Zylbercweig corpus scope (modern Israeli publishers etc.) |

`core_db.tsv` is now 12 columns wide (was 8 before this audit).

## What's already shipped (Dybbuk side)

- `core_db.tsv` carries the four new columns; in active use:
  - **27 rows** flagged `deprecated=true, merged_into=<canonical>` (the dedup queue from Class B).
  - **5 rows** flagged `out_of_project=true` (db3, 4, 9, 13, 14).
  - **9 rows** with populated `name_variants` (db67, 470, 483, 485, 488, 503, 510, 543, 622).
  - **8 rows** with `parent_db_id` linkage (Forverts/HH/Yudenrat umbrellas — `parent_db_id` itself isn't new).
- `prepare_alignment.py::main` skips rows where `deprecated="true"` OR `out_of_project="true"` from the candidate pool.
- `detect_data_defects.py` skips both classes from all three punchlists and respects `parent_db_id`, `confirmed_distinct_pairs.tsv`, `confirmed_clean_buckets.tsv`, and `name_variant_pairs_confirmed.tsv`.
- Zalmen `org_review.py`:
  - `CORE_DB_CANONICAL_HEADERS` updated to include all four columns, so `save_core_db` preserves them on every app write.
  - `active_db_rows()` hides both `deprecated` and `out_of_project` rows from candidate dropdowns; applied to manual-search sites in `org_review.py` and `org_alignment.py`.

## What still needs coordination (Zalmen-app side)

1. **Deploy the schema-aware Zalmen app.**
   The local code is correct (see above), but the deployed Streamlit Cloud instance must be redeployed to pick up the canonical-headers + `active_db_rows()` changes. Until redeployed, an RA's edit through the deployed app will save with the OLD canonical headers and **silently drop `parent_db_id`, `deprecated`, `merged_into`, `name_variants`, and `out_of_project`** from any row it touches. Highest-priority item.

2. **UI affordances for the new dispositions** (PI to specify):
   - Deprecated rows: show a "deprecated → db&lt;merged_into&gt;" badge in the lexicon view? Allow editing of name fields, or freeze them?
   - Out-of-project rows: similar — show with a distinct badge ("out of project scope") so RAs don't try to align to them.
   - Name variants: should the lexicon-display fall back to a variant if `name_yiddish` is empty? Should new variants entered via the UI append to `name_variants` automatically?
   - `parent_db_id`-linked rows: display children grouped under their parent in dropdowns / lexicon view?

3. **Auto-redirect of alignments on a deprecated id.**
   If any RA action lands a cluster on a row that's `deprecated="true"`, the app should auto-rewrite the alignment to `merged_into`. Cheap once the app honors the schema; defends against stale candidate caches in the meantime.

4. **`build_core_db.py` is non-idempotent and stale.**
   Its `fieldnames` is `["db_id","name","org_type","address","linked_cluster_ids"]` (5 cols vs the live 12). Per `feedback_build_core_db_nonidempotent` memory, it is **not safe to rerun** — would drop `name_yiddish`, `name_yiddish_translit`, `parent_db_id`, plus all four columns from this audit. If a regen is ever needed, update its field list first.

## PI-curated tracking files in this directory

Read by `detect_data_defects.py` to suppress re-flagging of vetted cases:
- `confirmed_distinct_pairs.tsv` — 16 LEAVE pairs (FPs at the 0.60 sim threshold).
- `confirmed_clean_buckets.tsv` — 13 PI-confirmed clean buckets (multi-spelling consolidations).
- `name_variant_pairs_confirmed.tsv` — 9 populated variant-pair rows (historical record).
- `db_pairs_pending_review.tsv` — 20 CHECK/DISCUSS pairs **still awaiting per-row scholarly call**.

The last one is the only **open scholarly work queue** left from this audit.
