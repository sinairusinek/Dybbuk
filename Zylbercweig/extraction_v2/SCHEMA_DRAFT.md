# Zylbercweig extraction v2: draft schema (2026-11-15)

Costs are estimates from token counts at assumed Gemini Flash prices ($0.50/M in, $3/M out).
**Status:** draft, for Sinai's review before the extraction prompt is written.
**REVISED 2026-11-17:** the 11-15 scope wrongly said person↔person relations were
missing. They exist for all 7 volumes (see §0). v2 is now a **delta pass**: it extracts
only what no earlier run produced, and fills gaps in old output from the extracted
sentences, not by re-reading whole entries. Where §2–§5 below conflict with §0, §0 wins.

**Original scope (11-15, superseded in part):** one new pass over every Lexicon entry (all 7 volumes) that adds what the
2026-03 extraction lacks: person↔person relations, works, dramatic roles, events,
movements (migration, touring, deportation…), and sources. It does **not** redo
person→organization relations (18.5k edges, already aligned through core_db). The new
pass only names organizations where they take part in an event, a movement or a
person↔person relation, and links them later by the same resolution path.

## 0. What already exists: do NOT re-extract (inventory 2026-11-17)

| layer | where | coverage |
|---|---|---|
| Person mentions + person↔person relations (category, specific type, evidence sentence, place, dates, gender, description) | `people/mentions_all.tsv` ← Drive `ZylbercweigMentionedPeople20260228.tsv` | **all 7 vols**, 38,269 mentions / 3,079 entries; 32k with a relation; 6.1k blank category; 15.6k flagged single-word, of which **5,931 have an empty name** (entry-level artifacts) → ~9.7k real surname-only + 3.2k initials. ⚠ `host_xml_id` / `host_entry_text` are filled only on an entry's first row: group by `host_heading_fill` |
| Vitals: names, birth/death/burial date+place, education | `Zylbercweig_extraction/*III.json` | ~2,400 entries; education 5,066 items |
| Family background (description, religion, financial, theatre connection) | `*IIIorg.json` `family_background` | 2,563 entries |
| Person→organization relations (+ role_title, dates, locations, org_type) | `*IIIorg.json` `organizations` → core_db | 2,924 entries, 21,266 org items |
| Editions / works (author, title, publication) | `plays/editions/editions_full2.tsv`, `works_full2.tsv` | all Lexicon; 5,059 editions / 3,525 works / 662 entries |
| Production / premiere / authorship / music / translation facts, with person role + **dramatic character** | `plays/kg_extraction_drafts_pass2.tsv` | **L/H scope only**: 669 entries, 2,154 facts |
| Itinerary stations (+ embedded performance events) | `plays/itinerary/`, `france_dossier/`, `dybbuk_*`, `person_dossier/` | ~500 entries (Istanbul 79, Danzig 28, France ~400, Dybbuk, Rumshinsky) |
| Migrations with reason (gold) | `pilot_2024/drive_2024_25/` Migrations2025 | 919 moves, Lasky 400-entry pilot |
| Performance-event assertions w/ dramatic role (gold) | `People in Performance Events.xlsx` (DiJeSt) | 852 assertions |

### Revised v2 work list

| # | job | input | method | why not a full re-read |
|---|---|---|---|---|
| A | In-entry `full_form` for surname-only / initial mentions (18,873 targets) | `mentions_all` + `entry_texts.tsv` (joined by filled-down heading) | deterministic first (175 resolved), Gemini on the residue with text windows | new field; no relations re-extracted. The 11-17 "≥5,861" estimate was WRONG: `host_xml_id` is filled only on an entry's first row, so it pooled entries |
| B | Classify the 6,565 blank/`Other` `relation_category` rows with the 11-15 scheme (+ Source, + None). Re-typing the 26k already-categorized rows to the new types is NOT done | blank rows have no sentence, so text windows around the mention | Gemini on windows/sentences | the 26k categorized rows were already extracted |
| C | Eponym orgs: `org_surface`, `named_after_mention`, leader/owner role, subject role | the 3,844 `employer_director`/`employee_student` sentences naming a troupe/theatre/company (supersedes the "249 his troupe" set, which was never saved as a file) | Gemini on sentences | tiny |
| D | Stations / movements (§5) | entries **not** in the itinerary/dossier sets, ~2,500 | full entry text | genuinely new |
| E | Events beyond performance (§4: jubilee, founding, strike, arrest, death-with-others…) | same pass as D | full entry text | genuinely new |
| F | Person→work performed/directed + dramatic roles (§3b) | entries outside the L/H 669 | same pass as D | new outside L/H; authorship stays with editions |
| G | Sources / informants (§2 `source`) | same pass as D | full entry text | new |

D–G share one full-text pass, but its output **omits** person mentions, person↔person
relations, vitals, family and person→org. Those are referenced by name only where an
event, station or work needs a participant. A–C are cheap sentence-level jobs and do not
wait for the schema review. They are what the MEHDIE surname-ER students need.

### Decisions (asked 11-17/20/21; Sinai's answers 11-21)

1. Person→org: **kept OUT of D–G** (11-21). The existing 21k person→org items will be **enriched later** with the new fields (`org_role`, dates, event context) by a cheap sentence-level pass over their own evidence sentences (step 8g).
2. New core_db org types **Hospitality** and **Theatre/concert agency**: **YES** (11-21). Re-classification: a name search over all 16k org mentions finds **~20 hospitality orgs** (hotels, taverns, cafés, restaurants, beer halls; only 7 are in core_db yet) and **~10 agencies / managements** (concert and theatre bureaus, Relkin's theatre agency, Edelstein's management, Filip & Wachtel management, a publicity bureau, 2 lecture bureaus; 6 in core_db). Orgs whose name doesn't say "hotel" (e.g. resorts known by proper name) are not caught. Summer camps (Arbeter-ring, Boiberik, Kinderland, Lakeland: 21) are NOT hospitality. **Not needed before D–G**: extraction never assigns org types (§2).
3. D–G: **re-run the ~500 itinerary entries too** and use the comparison with the old itineraries as QA (11-21).
4. Re-type the already-categorized relations (a Gemini 3 Flash run, not Claude): **YES in principle** (11-21). Measured pilot 11-21: **31,704 rows, not 26k**, ~$13 at full output, above the ~$10 approved. **Awaiting approval of the measured amount** or of the trimmed variant.
5. Meir Eisenberg: Google Doc prepared 11-21 ("Meir Eisenberg: one person or two?"). **DECIDED 11-21: two people** (split recorded; Doc sent to Ruthie).
6. Job A relatives: **ACCEPTED as inferred** (11-21). 580 passed the check → `in_entry_status = inferred` in the ER dataset (a candidate or feature, never gold); 24 rejected stay blank.
7. **Performance facts + production resolution (§3b):** **ACCEPTED** 11-21.
8. **New event fields (§4)** `status`, `language`, `org_role`: **ACCEPTED** 11-21.
9. **Vocabulary additions:** **ACCEPTED** 11-21. `librettist` added to occupation and function; `comedy`, `tragedy`, `farce`, `Purim play` added to work_kind; `vocab_alignment.tsv` updated.
10. **Genre as stated, per mention:** **ACCEPTED** 11-21.
11. Wikidata contribution + publication: **LATER** (11-21).
12. **NEW 11-21: Amateur subtypes** (§6b): proposal awaiting Sinai. **Not needed before D–G** (core_db only).

### Planned sequence: status (update after EVERY extraction run)

| step | what | status | output | counts / cost |
|---|---|---|---|---|
| 1 | Inventory of earlier extractions (§0) | done 11-17 | this file | — |
| 2 | A: in-entry full form | done 11-17 | `a_full_form.tsv` (+ `a_full_form_llm.jsonl`) | 12,942 real targets (5,931 empty-name artifact rows excluded); 1,136 resolved in-entry (kin_of_subject 604 = inferred surname, earlier_full_name 249, det 175, subject 57, context 51); 11.8k need cross-entry ER · ~$8 |
| 3 | B: blank/Other relation categories | done 11-17, corrected | `b_recategorized.tsv` | 634 real rows (5,931 empty-name artifact rows dropped; the first report counted them, wrongly): None 279 · Social 108 · Hierarchy 101 · Creative 45 · Family 36 · Collab 36 · Source 29 · ~$4 |
| 4 | C: eponym org-mediated ties | done 11-17 | `c_eponym_orgs.tsv` | 3,843 rows: org-mediated 3,745; named after the mention 1,883; mention = leader 2,033 / director 559 / owner 496; 579 also a direct tie; 2,264 distinct org surfaces · ~$2 |
| 5 | Sinai reviews this schema (§1–§9 vs §0) | done 11-22: decisions 1–10 answered (4 = the approved 8f run); open: 12 (amateur subtypes, not blocking) | — | — |
| 6 | Prompt for the D–G full-text pass | **DONE 11-22** | `prompts/defg.txt`, runner `defg_job.py` (one entry-part per call; entries > 30k chars split at sentence ends into ~24k parts: 3,285 parts in all) | 3-entry probe (Gemini 3 Flash, thinking off): **compact output** (omit empty/default keys; persons = name only, since mentions_all holds occupations) cut output ~30% with the same facts found. Measured: 2,670 prompt tokens per call + 0.61 in / 1.30 out tokens per char. **Prompt v2 (11-22)** after the Claude reference: Gemini had **stopped at the main emigration** (Frizant: 12 stations, the 1916–45 American career dropped) → rule 8 "cover the whole text", cities named one by one = one station each, plain engagements = stations only (no duplicate `season` events), stations without a stated place kept, `in_quote` on stations, function `staged` (אויפֿגעפֿירט), role lines without a play, א"ר = director / א"ד = organizer, `part_of`, event types dissolution/congress/election/military_service. Frizant now 45 stations (reference 41). **Thinking hurts**: with a 2k budget Gemini normalizes Yiddish spellings (evidence verbatim 21/45 vs 35/36 off) → thinking stays off. v2 output **~2.0 tokens per char** on these dense entries · probes in all $0.23 (v1 kept as `qa/defg_prompt_v1.txt`) |
| 7 | D–G Volume-3 pilot vs gold (old itineraries) + a **Claude reference** (Max plan, Sonnet subagents, no API spend) on 20 of the 24 entries | **DONE 11-22** | `defg_pilot.jsonl` (24k parts), `defg_pilot_long3k.jsonl` + `defg_probe5_p*.jsonl` (small parts), `qa/defg_pilot_report.py`, `qa/defg_claude_ref/` | **Stations = reference** (230 vs 228 on 20 entries); evidence verbatim 94%. But with 24k parts, facts **thin out with length** (output 1.5 tok/char on short entries, 0.2–0.4 on long): Fried 14k chars 5 performance facts vs reference 47. **3k parts fix it** (Fried 47 = 47, Schlossberg 18 vs 20); the 4 long entries give 4–10× more facts (e.g. Kesher 233 vs 23 perf facts); a read sample of 24 is mostly right, weak spots = bibliography written as `author` facts, generic stations, evidence verbatim 89%. Old itineraries (8 entries): old places found again in most, the misses mostly spelling variants ("קעשענעוו, בעסאַראַביע"); real misses Paris (Sverdlin), Łódź (Fridman). **Pulver's 240k-char entry contains the Purim-play essay** (entry segmentation), which yields generic 'facts'. Spend **$2.29** (pilot $0.75 + long re-run $1.54) + probes $0.39 |
| 8 | D–G all volumes, **including** the ~500 itinerary-done entries (decision 3: re-run, compare as QA) | **RUNNING (staged, approved 11-22 ~$42)**: shard 1/5 done 11-22 (1,499/1,500 parts, vols 1–2; 6.76M in / 3.77M out tokens = **$7.34** batch; 9,903 stations, 3,892 events, 5,811 performance facts, 1,027 source uses, 329 schema issues; evidence verbatim 91.6%; 18 empty entries = see-references; "last year missed" 101/1,012 = mostly bibliography years, rightly skipped; read sample 12/16 right, noise = role facts without a play, see-also author facts, stations missing verb_class). Remaining 4 shards re-measured **$32.30**. Pre-run fixes: (1) prompt v3: no `author` facts from title lists / publication data; inputs `kind: topic` (thematic article: no generic descriptions as facts) and `kind: continuation`; (2) **segmentation** (`qa/segment_fixes.tsv`, Claude read all 686 comma-less or subject-absent candidates): 13 entries had another article glued on (Pulver + Purim plays 237k, Furman + the new Yiddish theatre 198k, Kesher + Fraye Yidishe Folksbine, Prager + Progressive Dramatic Club, Bern + actors' union, 2 hidden person entries Hart and M. Eisenberg…; a donor list skipped), 29 cut-off sections (Goldfaden's posters / title page / Di intrige; Sholem Aleichem's evaluations; memorial plaques) go to their parent's subject, 67 genuine topic entries; (3) **Batch API runner** `defg_batch.py` (tested: 4/4 requests) | `defg_full.jsonl` | **6,980 parts** (6,148 person, 560 topic, 272 continuation), 15.94M chars, 3k parts; measured model: in = 3,397 + 0.555/char, out = 1,955 + 0.529/char per call (+~400 prompt tokens since) → **~$42 via the Batch API** (~$84 live) at $0.50/$3 per M |
| 8e | QA: D–G stations/events vs the old itinerary, dossier and France outputs for the same ~500 entries | not started (after 8) | `qa_itinerary_compare.tsv` | no LLM |
| 8a | ER context from ALREADY-STRUCTURED data (no LLM) | done 11-17 | `datasets/surname_er/er_mention_context.tsv`, `er_candidate_profiles.tsv` (+ README) | 12,942 mentions × 2,900 hub profiles; 5,727 gold; C orgs → core_db/cluster 1,746/2,191; baseline: resolver 1,282/3,401 ambiguous gold, org-overlap alone 437 right / 851 wrong |
| 8b | Gemini verification of C org matches, A relatives, B Sources + hub-surface rejections (Bila Tserkva, Ida→Regina, א. ר.→Regina, Fridman→Ash) | done 11-17 | `verify_*.jsonl`, `people/mention_surface_rejections.tsv` | C 1,596/1,746 ok (142 removed) · A-kin 580/604 · B-src 26/29 real · <$1 |
| 8c | Schema check against external models (LA-PA v0.9, SAPA; §10): performance facts + production resolution, event status/language/org_role, mapping table | done 11-20 | SCHEMA_DRAFT §3b, §4, §10 | — |
| 8d | Vocabulary alignment: our terms → Wikidata, LoC relators, BnF roles, Getty AAT, SAPA (SKOS exact/close/broad/none) | done 11-21 | `vocab_alignment.tsv`, SCHEMA_DRAFT §6a | 163 rows; occupations: WD 35/36, AAT 28, BnF 26, LoC 24, SAPA 23; 6 gaps (meshorer, broder singer, lessee, theatrical agent, stage jubilee, landsmanshaft) · no LLM |
| 9b | **Production resolution**: group performance facts (D–G + plays-KG facts) into productions; fact → production links with reasons | not started (after D–G) | `production_links.tsv` | no LLM for the rule pass |
| 9 | Resolution (surname ER, incl. MEHDIE students) on A–C + `mentions_all` | not started | mention→entity links | — |
| 10 | Graph assembly + review loop | not started | — | — |
| 11 | Publication of the schema + vocabulary choices **with statistics** (per term / fact type / volume, external coverage, verification precision); Wikidata items for the §6a gaps | not started (after D–G) | paper + `vocab_alignment.tsv` + a stats script | — |
| 8g | Person→org enrichment: add `org_role`, dates, event context to the existing person→org items from their own evidence sentences (decision 1) | not started | — | sentence-level Gemini; pilot first |
| 8f | R: re-type the already-categorized relations to the 11-15 types (decision 4) | **DONE 11-21** (full Gemini run, approved) | `r_retyped.jsonl` → `r_retyped.tsv` (`bc_jobs.py r`, `flatten`); pilot `r_pilot.jsonl`; `prompts/r_retype.txt` | **31,702 / 31,704 rows** (2 failed twice). Tokens 4,903,332 in / 3,611,998 out (155 / 114 per row, thinking off) = **$13.29** at $0.50/$3 per M (+ pilot $0.13). Category unchanged **84.2%** (26,708); main moves: Creative_Intellectual → **Source 2,590** (chronicler / informant), Collaboration → Hierarchy 752, → **None 519**; `via_org` filled 6,950, `via_work` 6,221, `schema_issue` 219. **QA 11-21** (Claude read 150 of the 4,994 changed rows, weighted to → Source / → None; `qa/r_spotcheck_2026_11_21.tsv`): category clearly wrong in **6 / 150 (4%)**, doubtful 7, so the new category is right in 91–96%. Collab → Hierarchy 20/20 right. **117 off-vocabulary pairs normalized in `flatten`** (Family grandparent/ancestor/uncle_aunt → `extended` + qualifier; a valid type under the wrong category takes its own category; noted `[norm …]` in schema_issue); 2 left. **Three types added (Sinai 11-21)** and applied in `flatten`, no API spend: `publisher` (110, from role_title) and `award_recipient` (53, the Kessel-prize entry) by rule `[rule …]`; **`subject_of` 165** from Claude reading all 262 rows the old pass called subject_of_work that went to None (145) or Source (117) — the Source ones were mostly the reversed direction (the subject wrote about the mention); 7 → client (the subject published the mention's memoirs / letters), 10 → None; sample errors fixed. **11-22: 6 of the 7 doubtful sample rows decided by reading the entry** (Hochberg → publisher, Skolnik → interpreter, Titta Ruffo → influence, Mogulesko burial → acquaintance, Wilensky → colleague, Zylbercweig's library copy → None); Kompaneyets left co_performer (the text says only "with"). Overrides in `qa/r_overrides.tsv` `[read …]` (188 rows). Prompt + `vocab_alignment.tsv` carry the new types |

## 1. Principles

1. **Mention-level output.** The extractor writes what the entry says, with names as
   they appear. It assigns no global ids. Entity resolution happens afterwards, and its
   decisions are stored as mention → entity links, separate from this output, so
   re-resolving never needs re-extracting.
2. **Same-entry resolution is the extractor's job.** "פּ.", "ער", a bare surname named
   in full earlier in the same entry, "his father": the extractor resolves these within
   the entry and writes both forms (`surface` + `full_form`). Which "Kessler" a name means
   *across* the Lexicon is not its job.
3. **Every item carries evidence:** a short verbatim quote from the entry
   (`evidence`), plus dates and places only as stated.
4. **Only categories the text can decide.** Where the text rarely decides a
   distinction (building vs company, hall vs theatre), the schema has no field for it.
   Stated facts are recorded instead. Where the text does not state a value, the field
   is `""` or `"unstated"`, never a guess.
5. **Organizations by the role they play here, not by a type.** "his troupe", "Shor's
   theatre": write the organization mention (with the person it is named after) and the
   person's role in it (leader/owner), not a person→person hierarchy.
6. **`schema_issues`.** Anything that does not fit goes there with a short question,
   rather than being forced into the nearest category.

## 2. Mentions (the nodes, as written)

| kind | fields | notes |
|---|---|---|
| `person` | `surface`, `full_form`, `is_subject`, `gender` (if stated), `occupation[]` (controlled, §6), `description` (a short phrase from the text, e.g. "דירעקטאָר פֿון…") | anonymous kin ("his father") → `surface: "פֿאָטער"`, `full_form: ""`, kinship in the relation |
| `organization` | `surface`, `full_form`, `named_after` (person surface, for eponyms like "Fishzon's troupe") | type is NOT assigned here; core_db holds it |
| `place` | `surface`, `place_kind` (settlement / region / country / venue_only / unknown) | resolved later via the gazetteer (Kima / Wikidata) |
| `work` | `surface` (title verbatim), `work_kind` (§6), `language` (if stated), `based_on` (surface of a source work or author, if stated) | the editions pipeline keeps publication data; this pass covers works performed, written, translated, composed |
| `role` (dramatic character) | `surface` (character name verbatim), `work` (title surface) | e.g. "Ben Hador", "the Queen" in *Shulamis* |
| `source` | `surface`, `source_kind`: informant / cited_text / memoir / interview / correspondence / other | the person or text the Lexicon draws on |

## 3. Relations

### 3a. Person ↔ person (the 2026-02 five-category scheme + the 11-15 edits)

| category | types | notes |
|---|---|---|
| **family** | parent, child, spouse, sibling, extended (in-law, cousin, uncle/aunt, grandparent, ancestor) | qualifiers: `foster` / `raised_by`, `ancestor` (yikhes), `divorced`, `widowed`, `second_marriage` |
| **collaboration** (peers) | co_performer, partner (business / stage duo), co_founder, colleague (same body or committee) | "same troupe as" → write both as members of the organization (§3c) instead |
| **hierarchy** | employer_or_director → employee, teacher → student, agent_or_impresario → client, **publisher** (published or printed the other's work), **award_recipient** (added 11-21) | `role_title` required for craft and service roles: prompter, props, assistant director, conductor, choreographer, set or title-page designer, meshorer… |
| **creative** | creator → interpreter (wrote the play / song / music the other performed), translator_of, adapter_of, influence (was influenced by / came before), **subject_of** (the subject made a work about or depicting the mention; reverse of source/wrote_about; added 11-21) | the work goes in `via_work` when named |
| **social** | friend, acquaintance, supporter_or_advocate, conflict (rivalry, lawsuit, strike against) | friendship is a category in its own right (Sinai, 11-15) |
| **source** | informant_about, wrote_about (biography, review, memoir), quoted_in_entry, corrected_or_contradicted | how the Lexicon knows: provenance, not a social tie |

Each relation: `from`, `to` (mention refs), `category`, `type`, `direction_note` if
needed, `role_title`, `via_work`, `via_org`, `date_start`, `date_end`, `place`,
`evidence`.

### 3b. Person → work and work → work

- person → work: `author`, `translator`, `adapter`, `composer`, `lyricist`, `librettist` (11-21), `director`
  (of a production), `designer` (sets, costumes), `choreographer`, `performer`
  (+ `role` when the character is named), `producer`.
- **Performance facts (revised 11-20, after Linked Art PA / SAPA / Linked Stage Graph, §10).**
  The extractor writes each "X played/directed/designed … in W" as ONE fact with
  everything the text gives and nothing more: `person`, `function` (actor, director,
  prompter… §6), `character` (verbatim, if named), `work`, and **optional context**:
  `org` (troupe/company), `venue`, `place`, `date_start`/`date_end`, `language`
  (of the staging, if stated and different from the play's), `status` (§4), `evidence`.
  **The extractor never creates a production.** "Played Ben Hador in *Shulamis*" with no
  troupe, place or year is a complete, valid fact.
- **Productions are a resolution target, not an extracted field.** After extraction, a
  grouping step (like person ER) clusters facts into productions: same work + same org
  + compatible dates or place, plus facts from other entries describing the same
  staging. Each grouping is stored as a fact → production link with its rule and
  reason, so it can be audited and undone. Facts with no anchor stay attached to the
  work with `production: unspecified`. This fixes the plays-KG weakness where
  `build_kg.py` keys events on an exact (play, venue, org, place, date) match, so
  "Kessler's troupe, 1912" and "Kessler's troupe" become two productions.
- **Export rule.** Models that require a production (Linked Art PA, SAPA) get a minimal
  stub production for an unspecified fact, typed "production, unspecified", never
  presented as a known staging.
- work → work: `translation_of`, `adaptation_of`, `based_on`, `same_as` (alternative title
  stated in the text).

### 3c. Organization-mediated facts (the "his troupe" decision)

- "X played in Y's troupe" → `Y leads O` + `X performed_in O`, where O is the
  organization mention ("Y's troupe", `named_after: Y`).
- "Shor's theatre in Baltimore" → `Shor owns_or_manages O` + whatever the subject did there.
- These become person→organization edges in the existing categories
  (Leadership_Ownership, Employment_Performance) at assembly time.

## 4. Events

An event has: `event_type`, `date_start`, `date_end`, `date_certainty`
(explicit / approximate / none), `place`, `venue` (organization or place mention),
`work` (if any), `participants[]` (person mention + role: performer / director /
organizer / speaker / honoree / victim…, + dramatic `role` if named), `evidence`.

Added 11-20 (§10):
- `status`: **performed** (default) / **planned** (announced, rehearsed, "prepared") /
  **banned** (censor, police) / **cancelled** / **unstated**. After Linked Art PA's
  Projected Event: an announced or banned play must not become a performance.
- `language`: of the performance/staging, only when stated (e.g. a Polish-language
  staging by a Yiddish actor; a Yiddish staging of a translated play). Neither Linked Art
  PA nor SAPA handles staging language vs work language well, so we define it ourselves.
- `org_role` for each organization in an event: **producer** / **presenter_or_host** /
  **venue** / **touring_company** / **other**, after AusStage's organisation involvement
  (production, presenting, touring…). Only when the text decides it.

| event_type | examples |
|---|---|
| `performance`, `premiere`, `debut`, `guest_appearance`, `season`, `benefit` | the existing plays-KG and itinerary types |
| `jubilee`, `honor`, `memorial` | "was honored at his 25-year jubilee" |
| `founding` | of any organization, theatre or not (school, union, party, newspaper) |
| `strike`, `conflict` | actors' strikes, lawsuits, boycotts |
| `marriage`, `divorce`, `birth_of_child` | when dated or placed |
| `retirement`, `illness` (added 11-22, Sinai) | "left the stage", "became paralysed" |
| `arrest`, `imprisonment`, `death`, `burial` | incl. Holocaust deaths: "perished with him in Klooga" = one `death` event, two participants |

**Venue is a role, not a type:** a café, a hotel, a synagogue or a park can each be the
`venue` of an event. Whether something is a theatre is core_db's business.

## 5. Movements (migration, touring, deportation…)

This is the itinerary extractor's **station** model, extended with a `reason` from the
2025 Migrations sheet (`pilot_2024/drive_2024_25/migrations/`), whose 919 hand-curated
moves are the reference.

A station: `seq`, `subject` (person or organization mention; a troupe can tour),
`place`, `place_kind`, `org` (who the subject is with there), `role`, `verb_class`,
`date_start`, `date_end`, `date_certainty`, `reason`, `reason_stated` (true/false),
`events[]` (the §4 events that happened at this station), `evidence`.

- `verb_class` (existing): born / settled / stay / pass_through / tour / flight /
  emigration / return / visit / death / unknown, + new: **deportation**, **evacuation**,
  **expulsion**, **conscription**.
- `reason` (from the Migrations sheet, harmonized): **work_engagement** (Labour),
  **tour_or_guest_appearance** (tour, guest-starring, the "labor migration" table),
  **family**, **education**, **emigration** (settling in a new country: "Immigration"),
  **forced** (deportation, expulsion, flight, evacuation), **health**, **other**,
  **unstated**. `reason_stated: false` whenever the text gives no reason; this follows the
  decidability rule. The sheet's Labour share (806/919) was partly an RA's inference,
  which `reason_stated` now makes visible.
- The 2025 split between a **Migration** table and a **Work table (labor migration)** is
  kept as a derived view: `settled` / `emigration` / `forced` vs `tour` / `stay` with a
  work reason.

## 6. Controlled vocabularies

- **occupation** (on persons, from the text): actor, actress, singer, chorister, dancer,
  comedian, prompter, director (regisseur), stage manager, conductor, composer, lyricist,
  librettist (added 11-21), playwright, translator, critic, journalist, editor, publisher, teacher, cantor,
  meshorer, badkhn, folk singer (broder singer), manager, lessee, impresario, agent,
  cashier, administrator, publicist, set designer, costume designer, choreographer,
  photographer, film director, radio announcer, other (+ text). The DiJeSt
  "People in Performance Events" professional roles are the starting list.
- **work_kind:** play, operetta, opera, musical comedy, melodrama, sketch, one-act,
  revue, song, music (score), film, radio play, book, memoir, article, periodical,
  comedy, tragedy, farce, Purim play (the last four added 11-21), lebnsbild, dramatic poem, dialogue, shadow play (added 11-22, Sinai, from the shard-1 schema issues), other.
  **Recorded as stated, per mention** (decision 10): the same work may be called
  "drama" in one entry and "melodrama" in another; both are kept, with their evidence.
- **source_kind:** see §2.

### 6a. Alignment to external vocabularies (11-21): `vocab_alignment.tsv`

Our terms stay the extraction labels, because they are what the text decides. Each term
also gets a SKOS-style match (`exact:` / `close:` / `broad:` / `none`) to five outside
vocabularies:
- **Wikidata**: items, plus properties for functions and relations.
- **LoC relator terms**: id.loc.gov, 306 codes.
- **BnF roles**: data.bnf.fr, 525 codes, the vocabulary LA-PA uses for gaps.
- **Getty AAT**: what LA-PA uses for genres.
- **SAPA**: vocab.performing-arts.ch. Activity `mu/`, performance type `hl/`, genre
  `mn/`, group type `vr/`.

The table has 163 rows (librettist added 11-21). Every code was checked against the downloaded list or a live
query; nothing comes from memory.

**Coverage of the 36 named occupations** (any match, with exact matches in brackets):

| Wikidata | LoC | BnF | AAT | SAPA |
|---|---|---|---|---|
| 35 (27) | 24 (19) | 26 (13) | 28 (23) | 23 (13) |

- **Function** (person → work): LoC, BnF, SAPA and Wikidata properties (P50, P57, P86,
  P161, P162, P655, P676, P1809, P2515, P4608, P5202) cover all of it. The **character**
  exists only as WD P453 / LA-PA `portrayed`.
- **work_kind**: Wikidata 19/21, AAT 13, SAPA 12. LoC and BnF have no genres.
- **event_type**: Wikidata covers all 19. AAT 7, SAPA 4 (SAPA splits premieres by
  scope: world, national, language).
- **event status, migration reason, source/social relations**: no outside vocabulary.
  These stay ours. "Planned/banned/cancelled" maps only to the LA-PA Projected Event.
- **org types**: no outside standard. 33/36 have a Wikidata class (mostly close), 16 a
  SAPA group type.

**Gaps: terms nobody has**, and candidates for Wikidata items:
- **meshorer** (none anywhere).
- **broder singer** (only broad "singer"; BnF *Chansonnier* is nearest).
- **theatre lessee**.
- **theatrical agent** (only "talent agent").
- **jubilee**: the actor's stage jubilee; Wikidata's items are religious.
- **landsmanshaft**.
- Partly: theatre administrator (Q28054726 has 0 sitelinks).

**Traps found** (written into the table's notes):
- Yiddish *regisseur* = stage **director** (sgd / BnF 500), not French *régisseur*
  (= stage manager, BnF 2260).
- cantor = hazzan **Q1067977**, not Q330679 (church cantor). AAT 300136390 is correct.
- informant ≠ Q1364673 (police informant). Use Q118869598.
- AAT "melodramas" exists only as a film genre.
- core_db "Theatre" mixes company and building, so it maps only *close*.

**Vocabulary changes this suggests** (see decisions 9–11):
- add **librettist**: every outside vocabulary has it (Q8178443, lbt, BnF 470, AAT
  300025674, SAPA mu/jmh).
- add genre terms the Lexicon actually uses (**comedy, tragedy, farce, Purim play**).
- record genre **as stated, per mention**: LA-PA's rule. The same play is called
  "drama" in one entry and "melodrama" in another.

### 6b. PROPOSAL 11-21: subtypes of `Amateur` organizations (decision 12)

Based on the 167 active core_db rows typed `Amateur`: their names, plus `parent_db_id`
where it is set (33 rows). The counts come from a name-pattern pass, so expect ±10%.
A subtype is assigned only when the name or the parent decides it (§1.4). Otherwise it
stays plain `Amateur`.

**Facet 1: how the group is constituted** (one value per org)

| subtype | what decides it | ≈ n | examples |
|---|---|---|---|
| `amateur_section` | drama section / circle **of** a host org: "דראַמקרייז ביים …", "דראַמאַטישע סעקציע ביי …", or `parent_db_id` set | ~50 | Bund, Poale Zion, Hazomir, Arbeter-heym, Kultur-lige, Pressers' Local |
| `amateur_society` | a free-standing dramatic society / club / circle / *liebhober* group | ~95 | Chicago Yiddish Dramatic Society, Kovno Yiddish Dramatic Society, "Unzer Vinkl" |
| `amateur_personal` | a group led by, or formed around, a named person (א"ר / א"א / א"ל פֿון X = under the direction of X) | ~7 | Ben-Ami's Vilna circle, Adam Domb's group, Zylbercweig's *liebhober* troupe |
| `amateur_studio` | a workers' theatre studio, between amateur and training | ~3 | ARTES (Chicago), "Arbeter-studio", the workers' studio at the Central Yiddish Culture House |

- **Eponyms are not a subtype.** In "Jacob Gordin Dramatic Club" or "David Kessler
  Dramatic Club" (~17) the person is an honorific, not the leader. Store it in
  `named_after`, as for troupes (§2).
- **Read the abbreviations carefully.** א"נ = *oyfn nomen* ("named after") is an eponym.
  א"ר / א"א / א"ל ("directed / led by") is `amateur_personal`.

**Facet 2: host type, only for `amateur_section`.** Derived from the parent org's
core_db type, not a new field.

| host type | ≈ n |
|---|---|
| workers' / cultural club (Cultural/workers' club) | 23 |
| party or youth movement (Jewish political bodies: Bund, Poale Zion, Tseirei Zion, Hashomer Hatsair, Gordonia, Tsukunft) | 12 |
| music / choral society (Musical organization: Hazomir ×7, Harfe) | 8 |
| school / university / student body (Education) | 2–4 |
| trade union (Pressers' Local; craftsmen's union) | 2 |
| Jewish community (kehile), cooperative colony, sports center, military | 1 each |

**Optional attributes, only when stated:**
- membership: workers 17, youth/children 7, students 4, soldiers 1.
- language: Hebrew 4, Russian 5, English 1. The default Yiddish is not written.
- `literary_dramatic` flag: 7 combined literary + drama circles.

**Clean-up the pass surfaced:**
- 11 rows have generic names with no place ("Dramatic Circle", "דראַמאַטישן קרייז"). These are candidates for the GENERIC decision.
- ~20 `amateur_section` rows still lack `parent_db_id` even though the host is named.
- Hebrew amateur groups (Habima Lwów, "Habama") stay `Amateur` + language Hebrew, not Non-Yiddish Theatre, unless Sinai prefers otherwise.

## 7. Output envelope (one JSON object per entry)

```
{ "entry_id": "...", "mentions": {"persons": [...], "organizations": [...], "places": [...],
  "works": [...], "roles": [...], "sources": [...]},
  "person_relations": [...], "work_relations": [...], "org_mediated": [...],
  "events": [...], "stations": [...],
  "schema_issues": [ {"evidence": "...", "issue": "doesn't fit | ambiguous | new type?",
                      "question": "...", "suggested": "..."} ] }
```

Mentions get local ids (`p1`, `o2`, `w3`…) that relations point to. These ids are
local to the entry and are never used outside it.

## 10. External models consulted (11-20) and how ours maps to them

Read 2026-11-20: **Linked Art performing-arts extension** (LA-PA v0.9, Bardiot /
Bruseker / Eide, CIDOC SIG March 2026, in community review; github.com/stage-to-data/linked-art-pa:
Models + CookBook); **SAPA Swiss Performing Arts** spec (sapa.github.io/spa-specifications;
the 2017 data-model PDF was not readable); from search summaries only: LRMoo F31,
Linked Stage Graph 2.0, Wikidata WikiProject Performing arts, AusStage, DOREMUS,
Bio CRM, the KCL factoid model.

**What we take**
- Production as a level between work and performance (LA-PA, SAPA, LSG), but as a
  **resolution target** (§3b), because the Lexicon often names a role without a production.
- Planned vs performed (LA-PA Projected Event, DOREMUS) → event `status` (§4).
- Organisation involvement role (AusStage) → `org_role` (§4).
- Statement-level evidence: LA-PA's `attributed_by` (source) + `referred_to_by` (literal
  transcription) = our `evidence` + `surface`. Our extraction is the factoid model
  (name as written, source, assertion), which Bio CRM and LA-PA also follow.
- **Date of the statement**, after the factoid model: every fact inherits the year of its
  Lexicon volume (vol 1 1931 … vol 7 1969), joined from metadata, not extracted. "Now
  plays in…" means "as of that volume".
- Function ≠ character ≠ occupation (Wikidata, LA-PA `technique` vs `portrayed`): already ours.

**What we do not take**
- LA-PA's *Work* = the director's staging concept (Chéreau's Hamlet). The Lexicon rarely
  decides it (§1.4), so we don't model it.
- Full CRM reification inside the extraction (too heavy for an LLM to fill reliably);
  music-specific DOREMUS casting / medium vocabularies; Overall Event (festival edition)
  unless the text names one.

**Mapping (for export; the extraction format stays flat)**

| ours | Linked Art PA | SAPA | LRMoo / CRM | Wikidata |
|---|---|---|---|---|
| `work` (the play) | Textual Work (`LinguisticObject`), source text of a Work | F22 Self-Contained Expression / F20 | F1 Work / F2 Expression | play item; `representation of` (P4646) from a production |
| production (resolved) | Production (`Activity` typed Production), `influenced_by` → Work | F25 Performance Plan | F25 / E7 | theatrical production item |
| performance event | Performance (`Activity`), part of Production | F31 Performance (part of the default F31) | F31 Performance, P80 performed | performance item |
| event `status: planned/banned/cancelled` | Projected Event (CC12, CRMpro) + `CP4_projects` | — | — | — |
| performance fact: person + function | `produced_by` → part, `technique` (LoC/BnF role), `carried_out_by` | E7 Activity typed by SPA vocabulary, P14 carried out by | E7 + P14.1 in the role of | cast member (P161) / director (P57)… |
| `character` | `portrayed` | (no class found) | — | character role (P453) qualifier; `notable role` (P10606) |
| `org` + `org_role` | Group, contribution via `technique` (producer, funder…) | E40 Legal Body as P14 actor | E74 Group | production company (P272) |
| `venue` | Place (E27 Site), `took_place_at` | E22 typed venue, P8 took place on or within | E27 / E53 | location (P276) |
| `evidence` / `surface` | `attributed_by` / `referred_to_by` (literal transcription) | F29 Recording Event | E13 Attribute Assignment | reference (S248/S1683 quote) |

## 8. Not in this pass

- Person→organization relations: kept from the 2026-03 extraction.
- Org types, building vs company, hall vs theatre: no fields (§1.4).
- Cross-entry identity: the resolution step, using these relations as evidence.

## 9. Pilot plan

1. Volume 3, which already has a person↔person extraction to compare against. Also run
   the 11 long Lasky entries (`private/…/long_articles_100_migration`) and the
   performance-event assertions as gold checks.
2. Read every `schema_issues` item, edit this schema, re-run the pilot sample.
3. Then all volumes, on Gemini (bulk LLM work goes to Gemini, not Claude).
