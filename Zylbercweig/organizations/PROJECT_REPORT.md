# Zylbercweig Organizations — Classification Project Report

> Methodology, decisions, and artifacts produced during the mapping of free-text organization types extracted from the Zalmen Zylbercweig Yiddish theatre lexicon into a canonical typology suitable for downstream research and the Zalmen application.

## 1. Context

The Zylbercweig lexicon is a Yiddish-language biographical reference work on Yiddish theatre. Each entry concerns a person; embedded in each entry are mentions of *organizations* the person was affiliated with — theatres, troupes, unions, schools, businesses, political bodies, etc. An earlier extraction pipeline ran an LLM over the source text and emitted free-text `org_type` tags per mention (`theatre`, `troupe`, `union`, `company`, `geverkshaft`, ...), with no fixed typology. This produced ~200 distinct tag strings across English, Yiddish, and Hebrew, including malformed values (whole sentences pasted into the type field by the upstream LLM).

The Zalmen application needs a fixed canonical typology so the research team and end users can browse by org type. The goal of this project: replace every per-row `org_type` value across **mentions**, **clusters**, **addresses-review (DB working copy)**, and **core-DB** files with a value from a fixed canonical list, and produce reviewable artifacts for the PI to spot-check and resolve genuinely ambiguous cases.

## 2. Source data

Four TSVs are the live state of the organizations pipeline:

| File | Level | Rows | Role |
|---|---|---|---|
| `organizations_clustered.tsv` | mention | 16,454 | one row per organization mention extracted from the source text |
| `org_alignment_review.tsv` | cluster | 7,499 | one row per dedup'd organization cluster |
| `org_addresses_review.tsv` | DB working | 576 | per-entity working copy with addresses |
| `core_db.tsv` | DB canonical | 578 | canonical per-entity record |

The `org_type` column in each (named slightly differently per file) carried the free-text LLM extraction.

## 3. Canonical typology — design and evolution

### 3.1 Starting point (from PI / RA)

A 25-item canonical list defined for the project, organized around theatre-domain semantics:

> Publisher, Printer, Theatre, Library, Heritage Institution, Printer/Publisher, Amateur, Circus, Company on Tour, Education, Kleinkunst, Media (Radio/Film), Musical organization, Society/Union, Traveling Company, Journals/Newspapers, OTHER - elaborate!, Political bodies, Religious institutions/organizations, Business, Labour, Health institutions, Military, Theatre education, Not an organization

### 3.2 Refinements made during the project

Following dialogue with the PI, the canonical list was tightened to **31 types** (28 by the end of the LLM verification phase; 3 more added after the PI reviewed the punchlist on 2026-05-12):

1. **Renamed** `Society/Union` → **Theatre-related Society/ Union** to encode the policy (only theatre-industry membership bodies live here; pure trade unions or generic societies belong elsewhere).
2. **Renamed** `Labour` → **Labour (factory/workshop)** to encode that the bucket is for *places* of physical labour, not labour *movements* (which are political).
3. **Renamed** `Media (Radio/Film)` → **Media (Radio/ Film/TV)** to include TV.
4. **Split** `Political bodies` → **Jewish political bodies** + **Non-Jewish political bodies**, motivated by the distinct historiographical and research interest in Jewish political-organizational history (Zionist parties, Bund, Histadrut, Judenrats, etc.) vs. surrounding non-Jewish polities (Polish Sejm, Soviet commissariats, US departments).
5. **Added** **Welfare/Aid organization** to capture the rich Jewish welfare/mutual-aid landscape (HIAS, JDC, ORT, UJA, Hadassah, WIZO, Workmen's Welfare Board, settlement houses, burial societies, old-age homes) that the original 25-item list had no home for.
6. **Added** **Trade Union / Professional Association** (after Gemini verification surfaced >80 rows with no canonical home — printers' unions, garment-trade unions, bar associations, professional/scientific societies — none of which fit Theatre-related Society/Union, Labour, or Political bodies cleanly).
7. **Added** **Judenrat** (PI 2026-05-12 — Nazi-imposed Jewish councils 1939–1945, separated from Jewish political bodies to reflect their distinct coerced-administrative character).
8. **Added** **Sports/Recreation** (PI 2026-05-12 — Maccabi and Jewish sports organizations; the sports identity is primary and deserves its own canonical rather than being collapsed under Education).
9. **Added** **Fraternal order** (PI 2026-05-12 — Knights of Pythias, Masonic lodges, B'nai B'rith and similar non-Jewish-labour-political fraternal bodies; previously collapsed under Welfare/Aid).

### 3.3 Policy documentation

The full operational policy — what belongs in each bucket, disambiguation rules, named-entity allow-lists, PI-dilemma flag taxonomy — was committed to [CLASSIFICATION_POLICY.md](CLASSIFICATION_POLICY.md). The policy doc is the single source of truth: both the rule-based mapper and the LLM verification prompt reference it.

## 4. Mapping methodology — three versions

### 4.1 v1 — naive tag lookup (failure mode)

A first-pass tag-lookup table mapped each free-text tag to a canonical. Produced two systematic errors the PI immediately caught:

- All `union` rows → `Labour`. Wrong — most "unions" in the lexicon are theatre-industry artist/actor unions (Society/Union), with a minority being non-theatre trade unions or workers' fraternal-political organizations.
- All `company` rows → `Traveling Company`. Wrong — most "company" mentions are commercial businesses (`Ford Motor Company`, `Standard Financial Corporation`, insurance companies, hotels).

**Lesson:** tag-only mapping is too lossy when the source tag is a generic word with multiple senses. Per-row evidence (name, sentence, relation) must drive the decision for ambiguous tags.

### 4.2 v2 — two-pass rule mapper

Reorganized into:
- **Pass A**: unambiguous tags (`theatre`, `troupe`, `synagogue`, `hospital`, `factory`, etc.) → canonical by tag alone (~22k rows total).
- **Pass B**: ambiguous tags (`union`, `company`, `society`, `club`, `association`, `organization`, `studio`, `institute`, `academy`, `committee`, `agency`, etc.) → resolved by keyword cascade against the row's `name + sentence + relation_category`.

For each Pass-B tag, a cascade of keyword sets resolved the canonical:

- `THEATRE_KW`, `MUSIC_KW`, `WRITERS_KW`, `FILM_KW`, `LABOUR_PLACE_KW`, `RELIGIOUS_KW`, `JEWISH_POLITICAL_KW`, `NONJEW_POLITICAL_KW`, `EDUCATION_KW`, `BUSINESS_KW`, `WELFARE_KW`, `HEALTH_KW`, `YOUTH_KW`, etc.

A row was marked `decided_via=context` when a positive cue matched; `decided_via=context_weak` when no cue matched and the cascade took its default branch (with `needs_review=yes`).

**Diagnostic insight from a Sonnet 4.6 sample** run against v2: my `LABOUR_KW` included `אַרבעטער` ("worker"), which over-matched on famous *fraternal-political* organizations like `אַרבעטער-רינג` (Arbeter Ring / Workmen's Circle) and `יידיש-נאַציאָנאַלן אַרבעטער-פֿאַרבאַנד` (Jewish National Workers Alliance). These are not factories; they are Yiddish-cultural, fraternal-mutual-aid, and socialist-Zionist organizations. A purely-Yiddish-aware reviewer caught this; my keyword rule did not.

### 4.3 v3 — policy-aligned + named-entity allow-list + sub-entity flagging

v3 implements the 27-type policy with three architectural additions:

1. **Named-entity allow-list (`NAMED_ENTITY_RULES`)** — declarative rules for specific historically-known organizations: `HIAS → Welfare/Aid`, `Keren Hayesod → Jewish political`, `YIVO → Heritage`, `Polish Army → Military`, `Red Cross → Health`, etc. Matching uses **word boundaries** (Hebrew/Latin) to prevent substring overfire (e.g., the 3-character `אָרט` not matching every word that happens to contain those three characters in sequence).

2. **PI-dilemma flags** for dual-/multi-identity organizations encoded directly in the named-entity rules:
   - `pi_dilemma:fraternal_political_dual_identity` — Arbeter Ring, Arbeter-Farband (cultural-fraternal + labour-political + theatre-patron).
   - `pi_dilemma:judenrat` — Nazi-imposed Jewish councils (historiographically contested).
   - `pi_dilemma:zionist_welfare_dual_identity` — Hadassah, WIZO (Zionist + welfare).
   - `pi_dilemma:sports_zionist_youth_triple_identity` — Maccabi.
   - `pi_dilemma:fraternal_welfare_or_other` — generic lodges/fraternal orders.
   - `pi_dilemma:brewery_relation_conflict` — breweries: Business when ownership relation, Labour when employment relation.
   - `pi_dilemma:sub_entity_vs_parent_classification` — sub-units of named entities (Kultur-Lige's theatre studio, Arbeter Ring's school, IKUF's publishing arm).

3. **Brewery / factory class** uses the `relation_category` column for per-row decision: `Leadership_Ownership` → Business; `Employment_Performance` → Labour.

v3 also dropped earlier `union → Labour` and `union → Society/Union` blanket defaults in favour of cascade-then-context-weak-flag, and added new keyword sets for Jewish-vs-non-Jewish political distinction.

## 5. Verification methodology — multi-model validation

The v3 rule output covered ~14k rows trivially (Pass A) and ~3.3k rows via shallow Pass B / named-entity decisions. Those 3.3k shallow decisions were the actual quality bottleneck — too many to manually review, too important to leave un-verified.

### 5.1 Deep review with Opus 4.7 (calibration of failure modes)

A stratified sample of 196 rows from the keyword-resolved subset was sent to Claude Opus 4.7 with the full policy in the system prompt, asked to:
1. Confirm or correct the auto canonical.
2. Tag the finding with a `concern_theme` for aggregation.
3. Suggest new canonical types if any cases didn't fit.

**Result:** 53% agree, 47% concerns flagged.

The 47% disagreement clustered into three diagnostic failure modes:

- **Failure 1 (`shallow_keyword_missed_cue`, 58 cases)** — Many `union` rows defaulted to Theatre-related Society/Union by v3's keyword fallback, but in reality were Jewish political bodies (workers' movements, Zionist orgs, congresses, trade-union federations).
- **Failure 2 (`wrong_named_entity`, 15 cases + `shallow_keyword_overfire`, 11 cases)** — Named-entity allow-list strings (`אָרט` for ORT, `מזרחי`, `הזמיר`) matched as substrings inside unrelated words (`אָרטיק` = local, `אָרטיסט` = artist, `מזרחי` = a film company unrelated to the political movement).
- **Failure 3 (sub-entity affiliations)** — Sub-units of named entities inherited the parent's classification incorrectly (Kultur-Lige's theatre studio → Heritage instead of Theatre education; Arbeter Ring's school → Jewish political instead of Education).

### 5.2 Decisions in response to Opus findings

The PI explicitly engaged with each failure mode:

- **Failure 1**: Accept that the rules can't reliably differentiate without world knowledge. **Don't patch with more keywords** — let the LLM handle ambiguous unions in the verification step.
- **Failure 2**: Apply a **mechanical fix** (word-boundary matching) but keep the named-entity list. The list expresses *intent* (these specific entities have these specific PI-dilemma flags) that we don't want to re-derive from scratch on every LLM call.
- **Failure 3**: **Flag, don't auto-decide.** When a sub-entity pattern is detected (sub-entity head + named-entity match), set `pi_dilemma:sub_entity_vs_parent_classification` and let the PI decide whether the policy should classify by sub-entity head, by parent entity, or case-by-case.

Additionally, **Maccabi, WIZO, Hadassah, generic lodges/fraternal orders** were elevated to PI-dilemma flags because they are intrinsically multi-identity organizations whose canonical bucket is a research-decision, not a rule-decision.

### 5.3 Model choice for the full verification pass

The next question: which LLM to use for the full 3.3k-row verification?

A **calibration pass** sent the same 196-row sample through **Gemini 3 Pro** and compared verdicts row-by-row to Opus's verdicts. After fixing a token-budget issue (Gemini 3's internal "thinking" output was being truncated by the original 400-token cap), the final agreement numbers:

- Gemini canonical == Opus canonical: **173/196 (88%)**
- On rows where Opus disagreed with the rule (hard cases): Gemini reached the same conclusion **67/84 (80%)**
- On rows where Opus agreed with the rule (easy cases): Gemini also agreed **106/112 (95%)**

Reading through the 17 hard-case disagreements showed roughly:
- ~7 are equally-defensible judgment calls,
- ~3 cases where Gemini was *more* nuanced than Opus (Ostjudenverband welfare identification, Haskalah-as-historical-phenomenon, film-title-as-not-an-organization, Opus self-contradiction on State Bar of Michigan),
- ~3 cases where Opus was more nuanced (relation-aware Ford-as-Labour for an employee mention).

**Decision: use Gemini 3 Pro for the full verification pass.** Quality is essentially equivalent to Opus on this task at ~3× lower cost. The PI-dilemma flags from the rule layer ensure the genuinely hard cases get human review regardless of model choice.

## 6. Final pipeline architecture

```
free-text org_type extractions
            │
            ▼
┌───────────────────────────────────────────┐
│ map_canonical_types_v3.py                 │
│                                            │
│  Pass A:  tag-only lookup (TAG_MAP_A)      │
│   - ~22k rows, unambiguous tags             │
│                                            │
│  Named-entity rules (NAMED_ENTITY_RULES)   │
│   - word-boundary matching                  │
│   - applies PI-dilemma flags for known      │
│     multi-identity organizations            │
│                                            │
│  Sub-entity flagging                        │
│   - if sub-head + named-entity match,       │
│     flag pi_dilemma:sub_entity_vs_parent    │
│                                            │
│  Pass B:  keyword-cascade resolver          │
│   - resolve_ambiguous() per ambiguous tag   │
│   - relation-aware for brewery/factory class│
│   - emits decided_via: tag | named_entity | │
│     context | context_weak                  │
└───────────────────────────────────────────┘
            │
            ▼
┌───────────────────────────────────────────┐
│ llm_verify_shallow.py (Gemini 3 Pro)      │
│                                            │
│  Verifies every row where decided_via in    │
│  {context, context_weak, named_entity}      │
│  with full policy + per-row context         │
│  (name, sentence, relation, role, heading)  │
│                                            │
│  When LLM disagrees with auto: override     │
│  canonical, mark decided_via=llm_verified,  │
│  preserve any pre-existing pi_dilemma flag  │
│                                            │
│  Parallel: 20 concurrent requests           │
└───────────────────────────────────────────┘
            │
            ▼
   final canonical values in 4 source TSVs
            +
   per-row decision history in mapping TSVs
            +
   PI punchlist (rows with pi_dilemma:* flags)
```

## 7. Key decisions, summarized

| Decision | Reasoning |
|---|---|
| 25 → 27 canonical types | Split political into Jewish/non-Jewish (research-relevant distinction); added Welfare/Aid (large gap in original list). |
| Renamed Society/Union → Theatre-related Society/Union; Labour → Labour (factory/workshop) | Self-documenting names: the policy is encoded in the bucket name, not hidden in a doc. |
| Word-boundary matching, not substring | Cheap mechanical fix to a real false-positive problem with short Hebrew/Yiddish allow-list strings. |
| Sub-entity affiliation = PI dilemma, not auto-decided | Policy question: should a Kultur-Lige theatre studio be Theatre-education (by sub-unit) or Heritage Institution (by parent)? PI to decide. |
| Brewery/factory: use `relation_category` | Same physical entity is Business for an owner, Labour for an employee. The "correct" classification depends on the mention's relation, not the entity's intrinsic nature. |
| Arbeter Ring, Workmen's Circle, Arbeter-Farband, Judenrats: route to Jewish political + flag for PI | These are genuine dual-identity (cultural-fraternal + labour-political) and historiographically contested (Judenrats). Default routing must be reviewable. |
| Hadassah, WIZO: route to Welfare/Aid + flag | Zionist by name, welfare/health by primary activity. Bucket choice is a research decision. |
| Maccabi: route to Education + flag | Sports + Zionist + youth-education triple identity; canonical list has no Sports category. |
| Don't add "Sports/Recreation" canonical (yet) | Insufficient row volume. If volume grows, revisit. |
| Don't add "Event/Festival" canonical | Opus suggested it once for a single row; not enough evidence for a new type. |
| Skip RA-list comparison after first pass | The RA's tag-level mapping flagged useful issues but is tag-only, not row-level. Its decisions are not ground truth; we used it as a sanity check, not as an authority. |
| Gemini 3 Pro for full LLM verification (not Opus) | Calibrated 88% agreement with Opus, lower cost; PI-dilemma flagging catches the hard cases regardless of model. |
| Don't LLM-verify Pass A `tag` decisions | By construction unambiguous; would waste tokens. |
| Apply LLM corrections directly (LLM is the verification authority) | When the LLM disagrees with the rule, the LLM's verdict overrides. `decided_via=llm_verified` records this. |

## 8. Artifacts

### Code
- [map_canonical_types_v3.py](map_canonical_types_v3.py) — the v3 rule mapper.
- [llm_verify_shallow.py](llm_verify_shallow.py) — the Gemini 3 Pro verification pass (parallel, resumable, override-on-disagree).
- [llm_deep_review.py](llm_deep_review.py) — the Opus 4.7 stratified-sample deep review (calibration of failure modes).
- [llm_calibrate_gemini.py](llm_calibrate_gemini.py) + [llm_calibrate_gemini_retry.py](llm_calibrate_gemini_retry.py) — the Gemini-vs-Opus calibration scripts.
- [build_ra_comparison.py](build_ra_comparison.py) — the RA-list comparison (used once for sanity check, then deprioritized).
- Older / superseded: `map_canonical_types.py` (v1), `map_canonical_types_all.py` (v1 multi-file), `map_canonical_types_v2.py` (v2).

### Policy & documentation
- [CLASSIFICATION_POLICY.md](CLASSIFICATION_POLICY.md) — the binding canonical-typology policy (canonical types, definitions, disambiguation rules, named-entity rules, flag taxonomy).
- [PROJECT_REPORT.md](PROJECT_REPORT.md) — this document.

### Data
- Source TSVs (canonical type applied in-place, with backups):
  - `organizations_clustered.tsv` + `.pre_canonical_backup`
  - `org_alignment_review.tsv` + `.pre_canonical_backup`
  - `org_addresses_review.tsv` + `.pre_canonical_backup`
  - `core_db.tsv` + `.pre_canonical_backup`
- Per-row decision-history mapping TSVs (auto + LLM verdict, concern themes, review reasons):
  - `organizations_clustered_canonical_mapping.tsv`
  - `org_alignment_review_canonical_mapping.tsv`
  - `org_addresses_review_canonical_mapping.tsv`
  - `core_db_canonical_mapping.tsv`
- Reference / verification outputs:
  - [ra_tag_canonical.tsv](ra_tag_canonical.tsv) — the RA's tag-level mapping (used as sanity check).
  - [llm_deep_review.tsv](llm_deep_review.tsv) — Opus 196-row sample verdicts.
  - [llm_calibrate_gemini.tsv](llm_calibrate_gemini.tsv) — Gemini-vs-Opus calibration.

## 9. Open questions for the PI

These remain on the PI's plate after the LLM verification pass completes:

1. **Sub-entity vs parent classification policy**: when a name like "X studio at Y" or "Y school" is encountered, should the canonical type follow the sub-entity (`Theatre education` for a theatre studio) or the parent (`Heritage Institution` for Kultur-Lige)? Or case-by-case?
2. **Arbeter Ring / Workmen's Circle class**: dual cultural-fraternal + labour-political identity. Confirm `Jewish political bodies` is the right canonical, or specify a different policy.
3. **Judenrats**: Jewish political bodies (current default) — confirm or pick alternative.
4. **Hadassah, WIZO**: Welfare/Aid (current default) — confirm or move to Jewish political bodies.
5. **Maccabi**: Education (current default) — confirm or add a `Sports/Recreation` canonical.
6. **Generic lodges / fraternal orders**: Welfare/Aid (current default) — confirm.
7. **Breweries / factories at the cluster/DB level**: when the same physical entity appears as Business in one mention (owner) and Labour in another (employee), should the DB record one type (and which)? Or allow per-mention typing only?
8. **Trade unions with no theatre cue** (`State Bar of Michigan`, printers' unions outside the Vilna printers' union case): currently OTHER or Theatre-related Society/Union by default. Confirm canonical or add a "Professional/Trade association" type.
9. **Generic "society" / "club" / "association" with no clear cue**: currently default to Theatre-related Society/Union (Zalmen-is-theatre-prior). Confirm or change default.
10. **Suggested new canonicals from LLM**: confirm whether to add Sports/Recreation, Event/Festival, Professional/Trade association.

All rows requiring PI judgement carry an explicit `pi_dilemma:*` value in their `review_reason` column in the mapping TSVs; filter by `needs_review=yes` to produce the punchlist.

## 10. Final results

### Verification outcomes

The Gemini 3 Pro verification pass covered every shallow-rule decision (`decided_via in {context, context_weak, named_entity}`):

| File | Verified | Overrides | Override rate |
|---|---|---|---|
| `organizations_clustered_canonical_mapping.tsv` (mentions) | 2,128 | 826 | 39% |
| `org_alignment_review_canonical_mapping.tsv` (clusters) | 1,133 | 600 | 53% |
| `org_addresses_review_canonical_mapping.tsv` (DB working) | 3 | 0 | 0% |
| `core_db_canonical_mapping.tsv` (DB canonical) | 3 | 0 | 0% |
| **Total** | **3,267** | **1,426** | **44%** |

The 44% override rate means the shallow rule layer's "confident" decisions were wrong (in Gemini's judgement) in ~4 out of 10 cases — a strong vindication of the multi-model architecture. Pass A (tag-only) was not verified because tags like `theatre`, `troupe`, `synagogue` are reliable by construction; verifying them would only waste tokens.

### Final canonical-type distribution (mention level, 16,454 rows)

| Canonical | Count |
|---|---|
| Theatre | 6,495 |
| Traveling Company | 3,221 |
| Education | 2,108 |
| Journals/ Newspapers | 1,536 |
| Theatre-related Society/ Union | 713 |
| Publisher | 581 |
| **Jewish political bodies** | **471** *(up from 234 in v3 pre-verification)* |
| **Welfare/Aid organization** | **211** *(new canonical, did not exist in v2)* |
| Heritage Institution | 161 |
| Musical organization | 142 |
| Religious institutions/organizations | 122 |
| Non-Jewish political bodies | 122 |
| Military | 107 |
| Media (Radio/ Film/TV) | 99 |
| Business | 89 |
| **Trade Union / Professional Association** | **54** *(new canonical, added 2026-05-12 after LLM verification)* |
| Labour (factory/workshop) | 49 |
| Not an organization | 41 |
| Amateur | 36 |
| OTHER - elaborate! | 25 *(down from 78 after Trade Union split-off)* |
| Health institutions | 25 |
| Theatre education | 21 |
| Library | 15 |
| Circus | 5 |
| Printer | 2 |
| empty / unknown_tag | 17 |

### PI punchlist

[`pi_punchlist.tsv`](pi_punchlist.tsv) — 279 deduped unique rows requiring PI judgement (488 total instances across the four files):

| Review tag | Rows |
|---|---|
| `llm:pi_judgement_needed` (Gemini-flagged dual-identity cases) | 226 |
| `pi_dilemma:sub_entity_vs_parent_classification` | 93 |
| `pi_dilemma:fraternal_political_dual_identity` (Arbeter Ring class) | 62 |
| `llm:data_quality` (upstream extraction errors flagged by Gemini) | 36 |
| `llm:missing_canonical` (suggested new types from Gemini) | 28 |
| `pi_dilemma:judenrat` | 11 |
| `pi_dilemma:zionist_welfare_dual_identity` (Hadassah/WIZO) | 9 |
| `pi_dilemma:sports_zionist_youth_triple_identity` (Maccabi) | 8 |
| `pi_dilemma:fraternal_welfare_or_other` (generic lodges) | 6 |
| `pi_dilemma:brewery_relation_conflict` | 3 |
| `pi_dilemma:ostrovski_institute_theatre_or_general` (core_db row 484) | 2 |
| `pi_dilemma:vilna_printers_union_trade_or_theatre` (core_db row 508) | 2 |
| `pi_dilemma:sao_paulo_yiddish_society_scope` (core_db row 493) | 2 |

### Strongest LLM-suggested addition to the canonical list

Gemini repeatedly suggested **"Trade Union" / "Professional Association"** as a missing canonical:
- ~45 rows in mentions, ~38 in clusters (>80 total across the corpus).
- Covers: printers' unions, bar associations, occupational guilds, employer associations, scientific/professional societies — entities that are unions or professional bodies but have no theatre cue and don't fit Labour (factory/workshop), Theatre-related Society/Union, or Political bodies cleanly.

Recommendation to PI: consider adding **`Professional/Trade association`** as the 28th canonical type. The volume is meaningful (>80 rows), the rule layer can detect these mechanically (named-entity + tag combo), and they have no current home.

Other suggested types (low volume, probably not worth adding):
- `Sports/Recreation` — 2 rows. Maccabi-class already handled via PI flag on Education.
- `Event/Festival` — 1 row. Route to OTHER.

### Outputs delivered

1. **Source TSVs** updated in-place with canonical types (backups in `*.pre_canonical_backup`).
2. **Per-row decision-history mapping TSVs** with full audit trail: original tag → rule decision → LLM verdict → final canonical, plus concern themes and reasons.
3. **[`pi_punchlist.tsv`](pi_punchlist.tsv)** — deduped flagged rows for the PI.
4. **[`CLASSIFICATION_POLICY.md`](CLASSIFICATION_POLICY.md)** — binding canonical policy.
5. **This [`PROJECT_REPORT.md`](PROJECT_REPORT.md)** — methodology + outcomes.

## 11. Completion (2026-05-17)

PI reviewed [`pi_punchlist.tsv`](pi_punchlist.tsv) via the [`PI_DECISIONS_COMPANION.md`](PI_DECISIONS_COMPANION.md) workflow. All originally-flagged rows have been resolved, and three additional canonical types were added at PI request:

- **Judenrat** — Nazi-era Jewish councils, previously routed to Jewish political bodies (11 rows).
- **Sports/Recreation** — Maccabi class previously routed to Education (~10 rows).
- **Fraternal order** — generic lodges/fraternal orders previously routed to Welfare/Aid (~5 rows).

This brings the canonical typology from 28 to **31 types**. The Zalmen application's `_ORG_TYPE_OPTIONS` dropdowns in [org_review.py](../zalmen/views/org_review.py) and [org_addresses.py](../zalmen/views/org_addresses.py) have been synced to the new list.

Other PI verdicts applied:
- São Paulo Yiddish Society (core_db row 493) → Jewish political bodies (was Theatre-related Society/Union).
- Generic `society`/`club`/`association` default with no cue → Trade Union / Professional Association (was Theatre-related Society/Union).
- Maccabi class → Sports/Recreation.
- Hadassah/WIZO → kept at Welfare/Aid (default confirmed).
- Arbeter Ring class → kept at Jewish political bodies (default confirmed).
- Sub-entity vs parent classification → LLM's per-row sub-entity-head decisions accepted.

**Punchlist is now empty.** All `needs_review=yes` flags cleared across the four mapping TSVs.

### Open deferred items

- **Brewery/factory DB schema change** — allow dual relation-typed labels (`business_role_type` + `worker_role_type`) at DB level. Deferred to coordinate with Zalmen app schema changes.
- **104 empty `org_type` values in core_db.tsv** — DB rows with no type tag; rule mapper had nothing to classify. Candidates for future LLM backfill if alignment recall remains weak.
