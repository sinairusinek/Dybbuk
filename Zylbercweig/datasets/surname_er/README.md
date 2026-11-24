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
  Since 2026-11-22 every categorized relation comes from the re-typing pass (`relation_source =
  v2_8f_retyped`, `|hand_read` = read and corrected by hand), with direction and finer types
  (new: `publisher`, `award_recipient`, `subject_of`); the earlier value is in `relation_old`.
- **The host's own itinerary** (new 2026-11-22): `host_defg_places`, `host_defg_years`, the same
  as the candidates' `defg_places` / `defg_years`: compare them with each candidate's.
  `host_defg_places_en` (new 2026-11-24) is the same list in English with Wikidata ids (see below).
- **Organization in the sentence** (`c_*`): e.g. "Kessler's troupe". `c_org_id` is that
  organization's id (`db:` = curated organization, `cl:` = org cluster), and
  `c_named_after_mention` says the troupe bears the mention's name. If a candidate's
  `org_ids` contains `c_org_id`, that is strong evidence.
- **Already resolved inside the entry** (`in_entry_full_form`): about 9% of mentions are
  named in full elsewhere in the same entry. `in_entry_method` says how this was found
  (`det:` = string match, `llm:` = Gemini; `kin_of_subject` = a relative, with the surname inferred). `in_entry_status`: `stated` = the full name is in the entry; **`inferred`** = first name + the family's surname (580 relatives); usable as a candidate or feature, but **not** a verified answer: stage names and married names make it wrong sometimes; `rejected` = the check failed, full form blanked.
- **Candidates:** `candidate_hub_ids` (surname lookup, incl. spelling variants; the host
  is excluded). `candidate_features` gives simple per-candidate overlap counts as a starting point.
- **Labels (changed 2026-11-24, tag `datasets-2026-11-24e`; read [Labels](#labels-read-this-before-training-or-scoring) below):**
  - `checked_verdict` / `checked_hub_id` is a **per-mention** answer, read in context, for all 3,446 ambiguous mentions that have a surname prior.
    - `checked_verdict = hub`: `checked_hub_id` is the person (2,751), and `checked_confidence` is `high`, `medium` or `low`.
    - `checked_verdict = none`: none of the candidates is this person (309), for example the host's brother "אַברהם", or the Russian playwright Ostrovsky.
    - `checked_verdict = unsure`: the text leaves it open (386). `checked_alternatives` lists the plausible hubs.
    - `checked_how` names the reading pass.
  - `surname_prior_hub_id` (formerly `gold_hub_id`) is one research assistant's pick **per surname**, the same for every mention of that surname (5,726 mentions). It is **not** a per-mention answer.
  - `surname_prior_alt_hub_ids` lists the other person(s) the assistant named for that surname.
  - `resolver_verdict` / `resolver_hub_id` are the output of the current rule-based resolver.

### `er_candidate_profiles.tsv`

`heading`, `surname_keys`, `name_surfaces`, `gender`, `birth_year`, `death_year`,
`birth_place` / `death_place` / `burial_place` (with Wikidata QIDs), `org_ids` +
`orgs_detail` (organization, relation category, dates, settlement), `active_settlements`,
`org_years`, `roles_db`, `wikidata`.

New 2026-11-22, from the person's **own** Lexicon entry (full-text extraction, steps D–G):
`defg_places` (the stations of their life and career in order, `place:year` when a year is
stated; 2,710 hubs), `defg_years` (first–last year stated; 2,526), `defg_works` (works they
performed, directed, composed, translated…, as `function:title (character)`; 1,729). Facts
stated only inside quotations are left out.

New 2026-11-24: `defg_places_en` is `defg_places` linked, station by station (position *i* in
one list is position *i* in the other): `Daugavpils (Q80021):1866|United States (Q30):1901`.
Links come, in order, from (1) the project's reviewed place links for the same entry and spelling
(hand-checked, e.g. אוישוויץ = the Auschwitz camp, not the town), (2) the same spelling where every
reviewed occurrence agrees, (3) the place gazetteer and the 2026-11-22 place decisions (Kima ids as
`Kima N`, Wikidata `Qnnn`), checked against any qualifier in the name ("X, Volhynia", "X (Egypt)")
and, for spellings shared by several places, against the country in the sentence or the person's
other stations. `?` = not linked (unclear spelling, not a place, an undecided homonym, or still with
the human reviewer). **92% of the 23,073 stations are linked, 91% with a Wikidata QID.** Compare QIDs, not names, across host and candidates.

### D–G relation tables (`defg/`, new 2026-11-24)

The full-text extraction of every Lexicon entry (steps D–G), as tables that join to the two files
above. `entry_hub_id` = the person whose entry it is (a hub in `er_candidate_profiles.tsv`; empty for
the ~1% of parts that are another person's article glued into the entry). Facts stated only inside
quotations are left out. Places carry `place_en`, linked as in `defg_places_en`.

| file | one row per | rows |
|---|---|---|
| `defg/defg_people.tsv` | person named in an entry (`mkey`), with the entry's own expansion of the name (`full_form_in_entry`); `hub_id` only for the entry's subject | 55,347 |
| `defg/defg_events.tsv` | event (performance, founding, death, marriage, debut, guest appearance, arrest…) with date, place, venue, work | 16,587 |
| `defg/defg_event_participants.tsv` | person in an event (`ekey` + `person_key` = an `mkey`) with role: performer, director, organizer, victim, spouse, honoree… | 24,146 |
| `defg/defg_performance_facts.tsv` | "person did function F on work W (as character C) with org O at venue V, place P, date D" | 25,595 |
| `defg/defg_work_relations.tsv` | work ↔ work: same_as, adaptation_of, based_on, translation_of, part_of | 1,843 |
| `defg/er_mention_defg_links.tsv` | ER mention ↔ a D–G person mention in its host entry with the same surname | 12,957 (10,279 mentions) |

Use: through `er_mention_defg_links` an ER mention reaches the events and performances the host's
entry puts it in (co-performers, works, dates, places), which can be compared with each candidate's
own `defg_*` rows. **`full_form_in_entry` is a model's reading of the entry, not a label**: on the
ambiguous mentions that have a surname prior, it names exactly one candidate 714 times, and that candidate is the prior in 394 of them. The surname
link is loose (every same-surname person in the host entry), so a mention can link to a relative.

## Labels: read this before training or scoring

**The earlier `gold_hub_id` was not a per-mention answer, and it has been renamed to
`surname_prior_hub_id`.** It comes from a review sheet on which a research assistant chose
*one* person per bare surface form, for example "קעסלער" → Joseph Kessler. That one choice was
then copied onto every mention of the surname. The consequences:

- All 3,446 ambiguous mentions that have a prior share 429 surnames, and no surname has more than
  one label. A model that only learns "surname → its usual person" would score 100% against it,
  and splitting train and test by mention leaks the answer.
- The sheet often names a second person (`alternative_heading`), and this was dropped.
  It is now `surname_prior_alt_hub_ids` (on 2,588 of the 3,446).
- Some priors are simply wrong. "הירשביין" points to A. Glanz instead of Peretz Hirshbein (29 mentions)
  and "לייוויק" to Leivick Khanukov instead of H. Leivick (24).

**`checked_*` is the per-mention label.** Every one of the 3,446 ambiguous mentions with a prior
was read in context, using the host entry's text, the relation sentence, dates, roles, works,
places and gender, against the profile of every candidate. Two passes did this:

- `two_way_fullform_vs_prior` (312 mentions): the mentions where the entry's own full form named a
  different candidate from the prior, judged as a two-way choice. The full form was right in 302
  of these, the prior in 7, and someone else in 3.
- `blind_all_candidates` (3,134, including 8 the first pass left undecided): the rest, judged among all candidates, **without being told
  which one was the prior**.

The readers were a language model (Claude Sonnet) working under our instructions. To estimate how reliable
each confidence level is, we checked random samples by hand:

| `checked_confidence` | answers | checked by hand | right |
|---|---|---|---|
| high | 1,039 | 20 | 20 |
| medium | 1,425 | 25 | 21 (1 wrong, 3 doubtful) |
| low | 287 | – | treat as a guess |

The samples are small, so read these as rough figures: high-confidence answers are very likely right, and
medium about 85%. Use the `high` answers as a strict test set, and `high` + `medium` for training or a
larger, noisier test. Two cases are weaker than their level suggests: Kaminski troupe mentions,
where Avrom-Yitskhok and Ester-Rokhl are hard to tell apart, have been set to `low`, and
bare "Fishzon" mentions were split between Avrom and Misha mainly by date. Reasons for every answer are in
[`people/er_mention_checks_2026_11_24.tsv`](../../people/er_mention_checks_2026_11_24.tsv).

**What the check showed about the old labels.** Of the 2,751 mentions with a checked person,
the surname prior was right on only 1,129 (41%). On 309 more it named a person when the mention is
someone with no candidate at all: relatives named by first name, or outsiders like Ostrovsky.

**Split by surname.** Even when read mention by mention, most surnames do refer to one person:
predicting each surname's most frequent checked person gets 2,632 of the 2,751 right. 54
surnames have mentions that point to different people. If train and test share surnames, a model
can memorize the surname. Split by `surname_key` and report results on unseen surnames, with the
54 mixed surnames as a separate, harder subset.

## Baselines (on the checked labels)

| method | high (1,039) | high + medium (2,464) |
|---|---|---|
| surname prior | 524 right | 1,048 right |
| existing rule-based resolver | 433 right (it answers 528) | 776 right (it answers 1,095) |

Earlier versions of this README reported "38% correct" for the resolver. That figure was its
agreement with the surname prior, not correctness.

The signals need combining: time (are the birth years compatible with the relation
date?), place, the shared organization, gender, and relation type.

## What changed (tag `datasets-2026-11-24e`)

- All 3,446 ambiguous mentions with a prior now have a per-mention reading: `checked_verdict`,
  `checked_hub_id`, `checked_alternatives`, `checked_confidence`, `checked_how`. In 11-24d only
  312 did. The baselines and the reliability check are above. All other columns and rows are unchanged.

## Earlier change (tag `datasets-2026-11-24d`)

- `gold_hub_id` renamed to `surname_prior_hub_id`, plus new `surname_prior_alt_hub_ids`,
  `checked_hub_id` and `checked_how` (312 mentions). The baselines were restated.

## Earlier change (tag `datasets-2026-11-24c`)

- New `defg/` folder: the D–G events, participants, performance facts, work relations and people,
  joined to hubs and to the ER mentions (see above). Built by `build_defg_tables.py`.
- Place links (`defg_places_en`, `place_en`) now use the project's reviewed place links first, then
  context checks (`place_linker.py`): 92% of stations linked, 91% with a Wikidata QID.

## Earlier change (tag `datasets-2026-11-24`)

- New `defg_places_en` (candidates) and `host_defg_places_en` (mentions): the D–G stations in English
  with Wikidata / Kima ids. `host_defg_places` is no longer cut at 600 characters (same 1,500 limit
  as the candidates', so the two lists line up).
- Organization links refreshed after the 11-22 organization review rounds (merges, new organizations,
  network parents): `org_ids` / `orgs_detail` changed on 457 hubs, `host_org_ids` on 2,757 mentions,
  `c_org_id` on 26. Gold labels, candidates and relations are unchanged.

## Earlier change (tag `datasets-2026-11-22`)

- Relations re-typed (3,290 mentions changed category or type); `relation_old` keeps the previous value.
- New D–G features on candidates (`defg_places`, `defg_years`, `defg_works`) and on the host (`host_defg_places`, `host_defg_years`).
- `flag = host_segmentation:*` on 375 mentions whose host entry had digitization errors that were
  repaired in the full-text pass: another article glued onto it (`merged_article`), another
  person's entry glued on with its heading lost (`glued_entry`), or a section cut off from its
  parent entry (`split_section`). For these, the "host" may be wrong for part of the text.
- Gold labels, candidates and organization links are unchanged.

## Known issues

- About 11.8k of the mentions are **not** resolvable inside their own entry; that is the point of the task.
- `c_org_id` is matched by sentence overlap (`c_org_match_score`) and then checked by a Gemini judge; the 142 matches it rejected are removed (`|check_failed` in the score). Expect a few residual errors.
- `in_entry_method` ending in `|check_failed` = a relative's inferred full name the check rejected (24); the full form is blanked.
- `relation_source = v2_job_B` rows (634) were classified by Gemini from text windows; `|checked_ok` / `|check_failed` marks the 29 'Source' relations that were checked.
- `flag = not_a_person`: the "mention" is a placename (e.g. ביעלאָצערקאָוו = Bila Tserkva); `rejected_alignment`: a curated rejection removed a wrong surname-prior or resolver answer.
- 110 surname-prior answers are not among the candidates (spelling variants the lookup misses): a recall problem to study.
- Hubs are not a closed world: some mentioned people have no Lexicon entry.
- Place links in `defg_places_en` not covered by a reviewed link (about 13%) go by spelling plus the
  context checks above; homonyms without a qualifier or a clear itinerary are left `?` rather than guessed.
  Treat a single place disagreement as weak evidence.
