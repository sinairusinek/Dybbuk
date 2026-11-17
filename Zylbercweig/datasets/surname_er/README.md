# Surname-only person mentions: an entity-resolution dataset

The [Lexicon of the Yiddish Theatre](../README.md) often names people by surname alone
("Adler", "Kessler") or by initials ("מ. ח. טיטעלמאַן"). The task: **decide which
person (hub) each such mention refers to.** No reading knowledge of Yiddish is needed.
Every signal below is already structured, so this is a matching and scoring problem.
See the [main dataset guide](../README.md) for background on the source and the
Yiddish script.

Funded by the project "Historical Digital Analysis of Zalmen Zylbercweig's Lexicon of
Yiddish Theatre", ISF grant number 284/24, headed by Ruthie Abeliovich, Tel Aviv
University.

## Files

| file | one row per | rows |
|---|---|---|
| `er_mention_context.tsv` | surname-only or initials mention to resolve | 12,942 |
| `er_candidate_profiles.tsv` | person hub, i.e. a possible referent | 2,900 |
| `build_er_context.py` | rebuilds both from the project's structured tables | – |

### `er_mention_context.tsv`

- **The mention:** `mention_name` (as written), `surname_key` (normalized; candidate lookup key), `gender`, `person_description`.
- **Where it appears:** `host_hub_id` / `host_heading` is the person whose Lexicon entry
  mentions it (always known). The host's `host_birth_year`, `host_birth_place`,
  `host_org_ids` and `host_active_settlements` describe the host's world. People usually
  mention colleagues from that world.
- **How it relates to the host:** `relation_category`, `relation_type` (e.g. Family/spouse,
  Professional_Hierarchy/employer_director), `relation_sentence`, `rel_settlement`, `rel_date_start`…
- **Organization in the sentence** (`c_*`): e.g. "Kessler's troupe". `c_org_id` is that
  organization's id (`db:` = curated organization, `cl:` = org cluster), and
  `c_named_after_mention` says the troupe bears the mention's name. If a candidate's
  `org_ids` contains `c_org_id`, that is strong evidence.
- **Already resolved inside the entry** (`in_entry_full_form`): about 9% of mentions are
  named in full elsewhere in the same entry. `in_entry_method` says how this was found
  (`det:` = string match, `llm:` = Gemini; `kin_of_subject` = a relative, with the surname inferred).
- **Candidates:** `candidate_hub_ids` (surname lookup, incl. spelling variants; the host
  is excluded). `candidate_features` gives simple per-candidate overlap counts as a starting point.
- **Labels:** `gold_hub_id` is a human-validated answer for 5,727 mentions; use it for
  train/test. `resolver_verdict` / `resolver_hub_id` are the current rule-based resolver's output, as a baseline.

### `er_candidate_profiles.tsv`

`heading`, `surname_keys`, `name_surfaces`, `gender`, `birth_year`, `death_year`,
`birth_place` / `death_place` / `burial_place` (with Wikidata QIDs), `org_ids` +
`orgs_detail` (organization, relation category, dates, settlement), `active_settlements`,
`org_years`, `roles_db`, `wikidata`.

## Baselines (on the 3,401 gold mentions with ≥2 candidates)

| method | correct |
|---|---|
| existing rule-based resolver | 1,282 (38%) |
| "most shared organizations with the host" alone | 437 right, 851 wrong, 2,113 undecided |

The signals need combining: time (are the birth years compatible with the relation
date?), place, the shared organization, gender, and relation type.

## Known issues

- About 11.8k of the mentions are **not** resolvable inside their own entry; that is the point of the task.
- `c_org_id` is matched by sentence overlap (`c_org_match_score`); below about 0.6 it is sometimes wrong.
- 110 gold answers are not among the candidates (spelling variants the lookup misses): a recall problem to study.
- Hubs are not a closed world: some mentioned people have no Lexicon entry.
