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

### Planned sequence: status (update after EVERY extraction run)

| step | what | status | output | counts / cost |
|---|---|---|---|---|
| 1 | Inventory of earlier extractions (§0) | done 11-17 | this file | — |
| 2 | A: in-entry full form | done 11-17 | `a_full_form.tsv` (+ `a_full_form_llm.jsonl`) | 12,942 real targets (5,931 empty-name artifact rows excluded); 1,136 resolved in-entry (kin_of_subject 604 = inferred surname, earlier_full_name 249, det 175, subject 57, context 51); 11.8k need cross-entry ER · ~$8 |
| 3 | B: blank/Other relation categories | done 11-17 | `b_recategorized.tsv` | 6,565 rows: None 4,187 · Family 1,329 · Source 611 (wrote_about 483, informant 119) · Hierarchy 213 · Social 111 · Collab 66 · Creative 48; 148 role_titles · ~$4 |
| 4 | C: eponym org-mediated ties | done 11-17 | `c_eponym_orgs.tsv` | 3,843 rows: org-mediated 3,745; named after the mention 1,883; mention = leader 2,033 / director 559 / owner 496; 579 also a direct tie; 2,264 distinct org surfaces · ~$2 |
| 5 | Sinai reviews this schema (§1–§9 vs §0) | waiting | — | — |
| 6 | Prompt for the D–G full-text pass | not started | `prompts/defg.txt` | — |
| 7 | D–G Volume-3 pilot vs gold (Lasky migrations, DiJeSt, old itineraries) | not started | — | est. $3–5 |
| 8 | D–G all volumes, skipping ~500 itinerary-done entries | not started | — | est. $15–30, overnight |
| 9 | Resolution (surname ER, incl. MEHDIE students) on A–C + `mentions_all` | not started | mention→entity links | — |
| 10 | Graph assembly + review loop | not started | — | — |
| — | Optional: re-type the 26k categorized relations to the 11-15 types (B on all rows) | not decided | — | est. ~$10 |

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
| **hierarchy** | employer_or_director → employee, teacher → student, agent_or_impresario → client | `role_title` required for craft and service roles: prompter, props, assistant director, conductor, choreographer, set or title-page designer, meshorer… |
| **creative** | creator → interpreter (wrote the play / song / music the other performed), translator_of, adapter_of, influence (was influenced by / came before) | the work goes in `via_work` when named |
| **social** | friend, acquaintance, supporter_or_advocate, conflict (rivalry, lawsuit, strike against) | friendship is a category in its own right (Sinai, 11-15) |
| **source** | informant_about, wrote_about (biography, review, memoir), quoted_in_entry, corrected_or_contradicted | how the Lexicon knows: provenance, not a social tie |

Each relation: `from`, `to` (mention refs), `category`, `type`, `direction_note` if
needed, `role_title`, `via_work`, `via_org`, `date_start`, `date_end`, `place`,
`evidence`.

### 3b. Person → work and work → work

- person → work: `author`, `translator`, `adapter`, `composer`, `lyricist`, `director`
  (of a production), `designer` (sets, costumes), `choreographer`, `performer`
  (+ `role` when the character is named), `producer`.
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

| event_type | examples |
|---|---|
| `performance`, `premiere`, `debut`, `guest_appearance`, `season`, `benefit` | the existing plays-KG and itinerary types |
| `jubilee`, `honor`, `memorial` | "was honored at his 25-year jubilee" |
| `founding` | of any organization, theatre or not (school, union, party, newspaper) |
| `strike`, `conflict` | actors' strikes, lawsuits, boycotts |
| `marriage`, `divorce`, `birth_of_child` | when dated or placed |
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
  playwright, translator, critic, journalist, editor, publisher, teacher, cantor,
  meshorer, badkhn, folk singer (broder singer), manager, lessee, impresario, agent,
  cashier, administrator, publicist, set designer, costume designer, choreographer,
  photographer, film director, radio announcer, other (+ text). The DiJeSt
  "People in Performance Events" professional roles are the starting list.
- **work_kind:** play, operetta, opera, musical comedy, melodrama, sketch, one-act,
  revue, song, music (score), film, radio play, book, memoir, article, periodical, other.
- **source_kind:** see §2.

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
