# Duplicate-Merge Schema (v2)

The old V+B `dup_merge.tsv` only flagged that duplicates existed; Sinai then had
to figure out which row survives, which fields to carry over, and how to merge
the children. This v2 schema lets your decisions execute mechanically — no
guessing on Sinai's side.

## File: `<batch>_dup_merge_template.tsv`

| column | required | filled by | meaning |
| --- | --- | --- | --- |
| `merge_group_id` | yes | pre-filled | `M001`, `M002`, … one group per duplicate set |
| `role` | yes | pre-filled (you override) | `SURVIVOR` (1 per group) or `MERGE_IN` (N per group) |
| `cluster_id` | yes | pre-filled | the cluster being merged |
| `QID` | yes (SURVIVOR) | you | Wikidata QID anchoring the merge |
| `name_latin` | yes (SURVIVOR) | you | canonical name post-merge |
| `canonical_address` | optional (SURVIVOR only) | you | historic address post-merge |
| `canonical_type` | optional (SURVIVOR only) | you | org_type post-merge (e.g. `Theatre`) |
| `evidence_url` | yes (SURVIVOR) | you | one URL (Wikidata / Wikipedia / archive) proving same-entity |
| `notes` | optional | you | one-line rationale |

## Rules

1. **Each merge group has exactly one `SURVIVOR` row + ≥1 `MERGE_IN` rows.**
2. **Canonical fields** (`name_latin`, `canonical_address`, `canonical_type`)
   are filled **only on the SURVIVOR row**. Leave blank on `MERGE_IN` rows.
3. **The pre-filled SURVIVOR is a guess** (largest cluster_size, lowest
   cluster_id). If a different cluster_id should win, change its `role` to
   `SURVIVOR` and demote the old one to `MERGE_IN`.
4. **You never touch Yiddish-side fields.** The survivor cluster's existing
   Yiddish name and variants carry over automatically.
5. **`evidence_url` is required on the SURVIVOR row.** One link is enough —
   Wikidata, Wikipedia, archival catalog, scholarly source.
6. If on review you decide a row is **not actually a duplicate**, change its
   `role` to `KEEP` — it will be excluded from the merge entirely.

## Example

```
merge_group_id  role        cluster_id        QID       name_latin           canonical_address      canonical_type  evidence_url                                   notes
M001            SURVIVOR    ORG-C00190_Q05    Q617244   Volksbühne Berlin    Rosa-Luxemburg-Platz   Theatre         https://de.wikipedia.org/wiki/Volksbühne       founded 1914; same building 1914→today
M001            MERGE_IN    ORG-C02488                                                                                                                            dup of M001
M001            MERGE_IN    ORG-C03012                                                                                                                            dup of M001
M002            SURVIVOR    ORG-C03517        Q...      Theateratelier Hugo Baruch  Berlin Mitte    Stagecraft      https://www.wikidata.org/wiki/Q...            company for sets/costumes
M002            MERGE_IN    ORG-C04001                                                                                                                            dup of M002
M003            KEEP        ORG-C01234                                                                                                                            initial flag wrong — actually distinct, see notes
```

## What the ingest script does with this file

For each `merge_group_id`:

1. Identify the SURVIVOR row's `cluster_id` as the merge target.
2. Reassign every `MERGE_IN` cluster's children, addresses, mentions,
   `linked_cluster_ids`, and DB alignments to the SURVIVOR.
3. Overwrite the SURVIVOR's `name_latin`, `confirmed_address`, `org_type`,
   and `wikidata_qid` with the canonical values from the SURVIVOR row.
4. Mark `MERGE_IN` clusters as `deprecated=yes` with `merged_into=<survivor cluster_id>`.
5. Skip any group containing a `KEEP` row entirely (no merge).

## What this does *not* cover

- Cross-batch duplicates (an Arne-Berlin cluster duplicating a future
  Arne-Munich cluster). Flag those in `notes` with the other cluster_id; Sinai
  handles cross-batch reconciliation.
- Splits (one cluster that should become two). Use the questions TSV for those.
