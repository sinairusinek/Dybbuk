# Troupe Typology — Proposal

**Status:** PROPOSAL. Not yet ratified, not yet implemented. Nothing in `core_db.tsv`, `map_canonical_types_v2.py`, or `llm_review_sample.py` has been changed.

**For:** Ruthie + PI review.
**Companion to:** `CLASSIFICATION_POLICY.md` (the 31-type canonical typology). This document proposes a *sub-typology* for troupes; it does not alter the 31 canonical types except where noted in §4.

---

## 1. The problem

`Traveling Company` is the largest bucket in the DB and does no discriminating work.

| org_type (case-folded) | rows | share of 1,714 |
|---|---|---|
| traveling company | 682 | 40% |
| theatre | 316 | 18% |
| *(empty)* | 246 | 14% |
| company on tour | 16 | 1% |
| amateur | 17 | 1% |
| kleinkunst | 6 | <1% |

Troupe-ish rows (`traveling|tour|amateur|kleinkunst|circus`) total **725 — 42% of the database**. Within that 42% we currently record nothing beyond the fact of being a touring company.

`core_db.tsv` has **no subtype or attribute columns**. Its only structural fields are `parent_db_id`, `deprecated`/`merged_into`, `out_of_project`, and `linked_cluster_ids`. So every distinction a cataloguer might want to draw about a troupe has to be squeezed into the single `org_type` string.

**The observable damage:** `Shlifershteyn Troupe` is typed `Amateur` while structurally identical companies are typed `Traveling Company`. That is not annotator error — the schema forced a choice between two facts that are both true. `Amateur` and `Traveling Company` are not alternatives.

---

## 2. The proposal: facets, not more types

Ruthie's examples — children's troupes, amateur troupes, one-star troupes, Jewish-German troupes — are **not mutually exclusive**. A children's troupe can be amateur. A one-star troupe is professional by definition. They are answers to *different questions* about the same company.

Splitting `Traveling Company` into eight flat values would:
- produce combinatorial explosion (children × amateur × operetta × …),
- force annotators to assert one true thing and discard two others,
- reproduce the exact `Amateur`/`Traveling Company` collision we already have.

**Recommendation:** keep `org_type = Traveling Company` as the genus. Add six independent facet columns to `core_db.tsv`. Each is independently assignable. **Each may be left blank** — blank means "not established", not "no".

This also settles an open question already on the books: `CLASSIFICATION_POLICY.md` §"Out of scope" defers "whether to allow per-relation type labels at DB level". Facets answer it — one genus, many orthogonal attributes.

### 2.1 `troupe_constitution` — how is it assembled, whose name is on it?

| value | definition |
|---|---|
| `star-vehicle` | Built around and named for one leading player, who is its artistic and commercial centre. *(Ruthie's "one-star" — see §5 open question 1.)* |
| `partnership` | Two or more billed principals sharing the billing. e.g. `Finkel, Feinman and Mogulesko's Troupe`, `Fishzon-Spivakovski Troupe`, `Glickman and Michalesko Troupe` |
| `manager-led` | Headed by an impresario, agent, or manager who is not himself a performer. e.g. `E. Relkin Troupe` |
| `collective` | Self-governing ensemble, named for a place or a principle rather than a person. e.g. Vilner Trupe, `קינדער-קאָלעקטיוו א"ר פֿון מאַרק מייערסאָן` |
| `institutional` | The troupe *of* a parent body — school, union, Kultur-lige, party, landsmanshaft. Should normally also carry `parent_db_id`. |

### 2.2 `troupe_personnel` — who performs?

| value | definition |
|---|---|
| `children` | Child performers. *(Ruthie's children's troupes.)* |
| `youth` | Adolescent/young-adult company, often attached to a movement or school. |
| `family` | A theatre family performing as a unit — the Kaminski/Adler pattern. See §5 open question 3. |
| `women` | All-women or women-led company. |
| `mixed-adult` | Default. **Leave blank rather than assert this** — it carries no information. |

### 2.3 `troupe_professional_status` — livelihood

| value | definition |
|---|---|
| `professional` | Members earn their living from performance. |
| `semi-professional` | Paid but not sole livelihood; seasonal companies, paid amateur circles. |
| `amateur` | Drama circle, club, unpaid. Absorbs the current `Amateur` org_type (see §4). |

### 2.4 `troupe_repertoire` — what do they play?

`drama` · `operetta` · `kleinkunst/revue` · `variety` · `marionette` · `badkhones/folk` · `mixed`

Absorbs the current `Kleinkunst` org_type (see §4). `Vilner Operetta / ווילנער אָפּערעטע` is the type case for `operetta`; `Bunes Badkhan (Tshizshik)` for `badkhones/folk`.

### 2.5 `troupe_language_register` — language and audience orientation

| value | definition |
|---|---|
| `yiddish` | Standard/literary or vernacular Yiddish. |
| `daytshmerish` | Germanized Yiddish, pitched at assimilated or German-Jewish audiences. *(Provisional reading of Ruthie's "Jewish-German" — see §5 open question 2.)* |
| `hebrew` | Hebrew-language company. |
| `bilingual` | Sustained two-language operation. |
| `vernacular` | Performing in the local non-Jewish language (Polish, Russian, Romanian, English…). |

### 2.6 `troupe_itinerancy` — touring pattern

| value | definition |
|---|---|
| `itinerant` | No home stage; touring is constitutive. |
| `resident-touring` | Has a home venue, tours from it. Absorbs the current `Company on Tour` org_type (see §4). |
| `resident` | Home venue, does not tour. (Rare on a Traveling-Company row; its presence usually signals a mistyped `Theatre`.) |

---

## 3. Auto-derivable first pass

Two facets can be pre-populated mechanically, so Ruthie corrects a draft rather than filling 698 blank rows by hand.

**`troupe_constitution`** — from the naming pattern. The dominant form is eponymous: `<Surname>'s Troupe` / `טרופּע פֿון <Surname>` / `<Surname>ס טרופּע`. Multiple surnames or a conjunction → `partnership`. Single surname → `star-vehicle` or `manager-led`, distinguished by whether the eponym appears elsewhere in the corpus as a performer (checkable against the people-matcher hub). Toponymic or `-kolektiv` forms → `collective`.

⚠️ **Yiddish-side caveat:** the Yiddish headword is frequently the bare inflected/possessive surname with **no troupe token at all** (`ראָפּעל`, `שוואַרצבאַרדן`, `גוזיק`, `צאָלמאַנען`) while the Latin name carries "Troupe". A regex keyed on `טרופּע` will silently miss these. Derive from the Latin side, or from the pair.

**`troupe_itinerancy`** — from signal that already exists upstream but is not carried into `core_db`:
- `org_alignment_review.tsv` — `extracted_settlements`, `extracted_venues`, `extracted_countries` per cluster.
- `organizations_clustered.tsv` — per-mention `locations - settlement`, `Venue`, `datefrom`/`dateto`.

Many distinct settlements + no venue = `itinerant`. One venue + outlying settlements = `resident-touring`. Prior work on exactly this boundary: `decided_multi_place_audit.tsv`, `reclassify_amateur.py`.

The other four facets need human or LLM-drafter judgement. If drafted by LLM, calibrate against a Ruthie-annotated gold set first — same protocol as the org-matching drafter (86% agreement benchmark).

---

## 4. Migration: three org_types to retire

`Amateur`, `Kleinkunst`, and `Company on Tour` are facet values wearing a genus costume. Each answers a question that is orthogonal to "what kind of organization is this", which is why each collides with `Traveling Company`.

| current org_type | rows | → new org_type | → facet |
|---|---|---|---|
| `Amateur` (#4) | 17 | `Traveling Company` or `Theatre` per case | `troupe_professional_status = amateur` |
| `Kleinkunst` (#5) | 6 | `Traveling Company` or `Theatre` per case | `troupe_repertoire = kleinkunst/revue` |
| `Company on Tour` (#3) | 16 | `Traveling Company` | `troupe_itinerancy = resident-touring` |

39 rows total — small enough to migrate by hand with Ruthie in one sitting.

**Preserve the original value in a new `org_type_legacy` column.** Do not overwrite in place: these three types are referenced by `map_canonical_types_v2.py` keyword rules and by the `llm_review_sample.py` prompt, and any migration needs to stay auditable and reversible.

If ratified, the canonical count moves 31 → 28, and `CLASSIFICATION_POLICY.md` §"Tag `company`" needs its cascade updated.

⚠️ **Do not regenerate `core_db.tsv` from scratch as part of this** — `build_core_db.py` is non-idempotent and drops app-appended NEW rows (725→442 observed). Migrate in place.

---

## 5. Open questions for Ruthie

1. **"one-star troupe"** — I have read this as *star-vehicle*: a company organized around and named for one leading actor. This fits the data, where the eponymous naming pattern dominates. But it could instead mean *a touring star with local pickup casts* — a different arrangement, which would need its own `troupe_constitution` value (`star-with-pickup`). Which did you mean? Both may exist and both may be worth recording.

2. **"Jewish-German troupe"** — genuinely ambiguous between (a) *daytshmerish* register, Germanized Yiddish for assimilated audiences, and (b) Jewish troupes operating in Germany/Austria. I have provisionally taken (a) and put it under `troupe_language_register`. Reading (b) is a geographic fact already captured by address/settlement fields and would not need a facet. Please confirm — they sit on different axes.

3. **`family`** — is this a personnel facet, or is it already covered by the family-continuity merge rules (Kaminski/Kaminska, 6→147)? There is a risk of double-encoding the same fact in two places.

4. **Are any facets multi-valued?** A company's repertoire may genuinely be `drama` + `operetta` across its life; its register may shift. If so, those columns need a delimiter convention (the TEI side already uses space-separated multi-token `@type` — precedent exists).

5. **Is the facet set time-invariant?** A troupe can go amateur → professional, or resident → itinerant. Facets as proposed record the *predominant* character. If period-specific values matter, that is a per-mention rather than per-entity model and a substantially larger change.

---

## 6. If ratified — implementation order

1. Add six facet columns + `org_type_legacy` to the `core_db.tsv` canonical header. **Enforce in `save_*` at write time** — the Zalmen app's cached boot headers go stale and silently drop schema columns.
2. Migrate the 39 `Amateur`/`Kleinkunst`/`Company on Tour` rows by hand, with Ruthie.
3. Auto-derive `troupe_itinerancy` from `org_alignment_review.tsv`, then `troupe_constitution` from naming patterns. Commit the TSV after each run.
4. Ruthie-annotated gold set (~100 rows) before any LLM drafter touches the remaining four facets.
5. Update `CLASSIFICATION_POLICY.md`, `map_canonical_types_v2.py`, and `llm_review_sample.py` together — the policy doc is the stated source of truth for both.
6. Zalmen app: facet editing UI. New view needs **both** VIEWS-dict registration and an `elif` branch, or it silently falls through.
