# D–G flattened tables (defg_flatten.py)

Source: `defg_full.jsonl`. Keys: `{part_id}:{local_id}`.

- `mentions.tsv`: 137,035 rows; 143 with flags
- `stations.tsv`: 34,091 rows; evidence exact 92.2%, near 6.9%, not found 0.9%; 834 with flags
- `events.tsv`: 17,687 rows; evidence exact 87.6%, near 11.1%, not found 1.3%; 69 with flags
- `event_participants.tsv`: 25,769 rows; 5 with flags
- `performance_facts.tsv`: 27,391 rows; evidence exact 89.8%, near 9.1%, not found 1.1%; 5,516 with flags
- `schema_issues.tsv`: 1,198 rows
- `event_orgs.tsv`: 5,410 rows
- `source_uses.tsv`: 8,093 rows; evidence exact 87.7%, near 11.4%, not found 0.9%; 59 with flags
- `work_relations.tsv`: 1,843 rows; evidence exact 84.1%, near 14.7%, not found 1.2%; 11 with flags

Repairs: inline_mention 21, new_illness 1, new_kind_dialogue 4, new_kind_dramatic_poem 2, new_kind_lebnsbild 18, new_kind_shadow_play 1, undefined_id 153, verb_missing 672
