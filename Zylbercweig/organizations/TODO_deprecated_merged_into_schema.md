# TODO: Coordinate `deprecated` / `merged_into` schema on core_db.tsv

PI-approved policy (2026-05-31) for handling duplicate DB rows surfaced by the
off-candidate audit: rather than deleting orphaned duplicates, mark them with
two new fields on `core_db.tsv`:

| column | meaning |
|---|---|
| `deprecated` | `"true"` if this row has been merged into another; empty otherwise |
| `merged_into` | the canonical `db_id` it was merged into |

## What this commit shipped

- Added the two columns to `core_db.tsv` (8 → 10 columns).
- First use: **db264 (`Guzik Troupe`)** marked `deprecated=true, merged_into=486`.
- `prepare_alignment.py::main` skips deprecated rows from the candidate pool so
  the matcher never proposes them again.

## What still needs coordination

1. **Zalmen app — `save_core_db` must preserve the new fields.**
   Per [`feedback_zalmen_stale_headers`](../../../.claude/projects/-Users-sinairusinek-Documents-GitHub-Dybbuk/memory/feedback_zalmen_stale_headers.md):
   the app's save uses cached headers from boot; unknown columns are dropped on
   write. Update the app's canonical core_db header list to include
   `deprecated` and `merged_into`, then redeploy. **Until this lands, any
   Zalmen edit to a core_db row will silently strip these fields.**

2. **`build_core_db.py` is non-idempotent and stale.**
   Its current `fieldnames` is `["db_id","name","org_type","address","linked_cluster_ids"]`
   (5 cols vs the live 10). Per [`feedback_build_core_db_nonidempotent`](../../../.claude/projects/-Users-sinairusinek-Documents-GitHub-Dybbuk/memory/feedback_build_core_db_nonidempotent.md)
   it is **not safe to rerun**: it would drop both `name_yiddish` columns,
   `parent_db_id`, and the new `deprecated`/`merged_into`. If a regen is ever
   needed, update the field list first.

3. **App UI behavior on deprecated rows** (PI to decide):
   - hide from candidate dropdowns? (matches what `prepare_alignment.py` now does)
   - show with a "deprecated → db<merged_into>" badge in the lexicon view?
   - allow editing of the deprecated row's name fields, or freeze them?

4. **Auto-redirect of alignments on the deprecated id.**
   If any cluster is later aligned to a row that's `deprecated=true`, the app
   should auto-rewrite the alignment to `merged_into`. Out of scope for this
   commit; trivial once the app honors the schema.

## Future use of these fields

The `db_duplicate_pairs_punchlist.tsv` lists 589 candidate dup pairs. As each
is reviewed and a canonical chosen, the **losing** side gets
`deprecated=true, merged_into=<winner>`, and any alignments on the losing side
get re-pointed. Same pattern for any future dedup work in the Class-B
garbage-bucket queue.
