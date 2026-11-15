# Zylbercweig extraction v2: draft schema (2026-11-15)

**Status:** draft, for Sinai's review before the extraction prompt is written.
**Scope:** one new pass over every Lexicon entry (all 7 volumes) that adds what the
2026-03 extraction lacks: person↔person relations, works, dramatic roles, events,
movements (migration, touring, deportation…), and sources. It does **not** redo
person→organization relations (18.5k edges, already aligned through core_db). The new
pass only names organizations where they take part in an event, a movement or a
person↔person relation, and links them later by the same resolution path.

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
