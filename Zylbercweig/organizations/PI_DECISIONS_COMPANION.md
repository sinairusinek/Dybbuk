> **STATUS: APPLIED 2026-05-17.** All PI verdicts have been integrated into the classification pipeline and source TSVs. The 31-type canonical typology (28 + Judenrat + Sports/Recreation + Fraternal order) is in place. The summary checklist at the bottom can be considered ticked. Original document preserved below for reference.

# Organization Classification — PI Decisions Companion {#organization-classification-—-pi-decisions-companion}

[**Organization Classification — PI Decisions Companion	1**](#organization-classification-—-pi-decisions-companion)

[1\. Classification status	2](#1.-classification-status)

[What has been done	2](#what-has-been-done)

[Final type distribution (mention level)	3](#final-type-distribution-\(mention-level\))

[Composition of the punchlist for PI review	5](#composition-of-the-punchlist-for-pi-review)

[2\. Global decisions already made (for PI confirmation)	6](#2.-global-decisions-already-made-\(for-pi-confirmation\))

[2.1 Canonical typology (28 types)	6](#2.1-canonical-typology-\(28-types\))

[a) Rename Society/Union → Theatre-related Society/ Union	6](#a\)-rename-society/union-→-theatre-related-society/-union)

[b) Rename Labour → Labour (factory/workshop)	6](#b\)-rename-labour-→-labour-\(factory/workshop\))

[c) Rename Media (Radio/ Film) → Media (Radio/ Film/TV)	6](#c\)-rename-media-\(radio/-film\)-→-media-\(radio/-film/tv\))

[d) Split Political bodies → Jewish political bodies \+ Non-Jewish political bodies	7](#d\)-split-political-bodies-→-jewish-political-bodies-+-non-jewish-political-bodies)

[e) Add Welfare/Aid organization	7](#e\)-add-welfare/aid-organization)

[f) Add Trade Union / Professional Association (added 2026-05-12 after Gemini surfaced the gap)	7](#f\)-add-trade-union-/-professional-association-\(added-2026-05-12-after-gemini-surfaced-the-gap\))

[2.2 Methodology decisions	7](#2.2-methodology-decisions)

[Pass A → keyword cascade → Gemini verification	7](#pass-a-→-keyword-cascade-→-gemini-verification)

[LLM as verification authority	8](#llm-as-verification-authority)

[Named-entity allow-list	8](#named-entity-allow-list)

[3\. Per-dilemma decisions (PI verdict needed)	8](#3.-per-dilemma-decisions-\(pi-verdict-needed\))

[3.1 pi\_dilemma:fraternal\_political\_dual\_identity — 62 rows	8](#3.1-pi_dilemma:fraternal_political_dual_identity-—-62-rows)

[3.2 pi\_dilemma:judenrat — 11 rows	9](#3.2-pi_dilemma:judenrat-—-11-rows)

[3.3 pi\_dilemma:zionist\_welfare\_dual\_identity — 9 rows	10](#3.3-pi_dilemma:zionist_welfare_dual_identity-—-9-rows)

[3.4 pi\_dilemma:sports\_zionist\_youth\_triple\_identity — 8 rows	10](#3.4-pi_dilemma:sports_zionist_youth_triple_identity-—-8-rows)

[3.5 pi\_dilemma:fraternal\_welfare\_or\_other — 6 rows	11](#3.5-pi_dilemma:fraternal_welfare_or_other-—-6-rows)

[3.6 pi\_dilemma:sub\_entity\_vs\_parent\_classification — 93 rows	11](#3.6-pi_dilemma:sub_entity_vs_parent_classification-—-93-rows)

[3.7 pi\_dilemma:brewery\_relation\_conflict — 3 rows	12](#3.7-pi_dilemma:brewery_relation_conflict-—-3-rows)

[3.8 pi\_dilemma:vilna\_printers\_union\_trade\_or\_theatre — 2 rows (core\_db row 508\)	13](#3.8-pi_dilemma:vilna_printers_union_trade_or_theatre-—-2-rows-\(core_db-row-508\))

[3.9 pi\_dilemma:ostrovski\_institute\_theatre\_or\_general — 2 rows (core\_db row 484\)	13](#3.9-pi_dilemma:ostrovski_institute_theatre_or_general-—-2-rows-\(core_db-row-484\))

[3.10 pi\_dilemma:sao\_paulo\_yiddish\_society\_scope — 2 rows (core\_db row 493\)	14](#3.10-pi_dilemma:sao_paulo_yiddish_society_scope-—-2-rows-\(core_db-row-493\))

[3.11 llm:pi\_judgement\_needed — 226 rows	14](#3.11-llm:pi_judgement_needed-—-226-rows)

[3.12 llm:data\_quality — 36 rows	15](#3.12-llm:data_quality-—-36-rows)

[3.13 llm:missing\_canonical — 28 rows (most already resolved)	15](#3.13-llm:missing_canonical-—-28-rows-\(most-already-resolved\))

[4\. Additional cross-cutting policy questions (from earlier discussion)	16](#4.-additional-cross-cutting-policy-questions-\(from-earlier-discussion\))

[4.1 Brewery / factory / workshop policy at the DB level	16](#4.1-brewery-/-factory-/-workshop-policy-at-the-db-level)

[4.2 Generic society / club / association default when no cue found	16](#4.2-generic-society-/-club-/-association-default-when-no-cue-found)

[4.3 Defaults for other context\_weak categories	16](#4.3-defaults-for-other-context_weak-categories)

[5\. Summary checklist (for the PI to confirm at the end)	16](#5.-summary-checklist-\(for-the-pi-to-confirm-at-the-end\))

Companion to [`pi_punchlist.tsv`](https://docs.google.com/spreadsheets/d/1fQj908fbyZQ0C7QhD-MLihOwTy51plSv-9-mw2--o8o/edit?usp=sharing). For each dilemma category and each open policy question, this document gives plain-English context, the current default classification (and reasoning), and a blank slot for the PI's verdict.

**Workflow:** open the punchlist TSV alongside this doc. The `review_tag` column in each row maps to a section heading below. Fill in the **PI decision** lines (and any notes) directly. Upload to a Google Doc if convenient; we can re-integrate the answers from the doc back into the code/policy afterwards.

---

## 1\. Classification status {#1.-classification-status}

### What has been done {#what-has-been-done}

The organization-type classification pipeline has been built and run end-to-end. Source data was four TSV files covering \~25,000 organization rows across three levels of granularity:

- **Mentions** (per-occurrence in the source text): 16,454 rows.  
- **Clusters** (deduplicated organization names): 7,499 rows.  
- **DB working copy** (org\_addresses\_review): 576 rows.  
- **DB canonical** (core\_db): 578 rows.

Each row carries an `org_type` value that was originally a free-text LLM extraction (\~200 distinct values, mixed English / Yiddish / Hebrew). All `org_type` values have now been mapped to a fixed 28-item canonical typology.

**Decision pipeline summary (mention level, 16,454 rows):**

| Decision path | Rows | What it means |
| :---- | :---- | :---- |
| Pass A — tag lookup | 14,325 (87%) | Unambiguous source tag (e.g., `theatre` → Theatre). |
| Gemini 3 Pro verified (overrode rule) | 826 (5%) | LLM disagreed with the rule layer; LLM's verdict applied. |
| Rule cue matched, LLM confirmed | 623 (4%) | Rule found a positive keyword cue \+ Gemini agreed. |
| Named-entity allow-list, LLM confirmed | 436 (3%) | Well-known org (HIAS, YIVO, etc.) matched by name \+ LLM agreed. |
| Rule default fallback (`context_weak`) | 241 (1.5%) | No cue found; rule picked a default. Flagged for review. |
| Unknown tag | 1 | Upstream extraction error (Yiddish sentence in tag field). |

**\~99.5% of rows have a confident canonical assignment.** The remaining \~0.5% (the 241 `context_weak` defaults) plus the \~1,100 dual-identity flagged rows are what the PI sees in [`pi_punchlist.tsv`](https://docs.google.com/spreadsheets/d/1fQj908fbyZQ0C7QhD-MLihOwTy51plSv-9-mw2--o8o/edit?usp=sharing).

### Final type distribution (mention level) {#final-type-distribution-(mention-level)}

Theatre                                  6495  ████████████████████████████████████████████████  39.5%

Traveling Company                        3221  ████████████████████████                          19.6%

Education                                2108  ███████████████                                   12.8%

Journals/ Newspapers                     1536  ███████████                                        9.3%

Theatre-related Society/ Union            714  █████                                              4.3%

Publisher                                 581  ████                                               3.5%

Jewish political bodies                   471  ███                                                2.9%

Welfare/Aid organization                  211  █                                                  1.3%

Heritage Institution                      161  █                                                  1.0%

Musical organization                      142  █                                                  0.9%

Religious institutions/organizations      122  █                                                  0.7%

Non-Jewish political bodies               122  █                                                  0.7%

Military                                  107  █                                                  0.7%

Media (Radio/ Film/TV)                     99  ▌                                                  0.6%

Business                                   89  ▌                                                  0.5%

Trade Union / Professional Association     54  ▍                                                  0.3%  ← new (2026-05-12)

Labour (factory/workshop)                  49  ▍                                                  0.3%

Not an organization                        41  ▎                                                  0.2%

Amateur                                    36  ▎                                                  0.2%

Health institutions                        25  ▏                                                  0.2%

OTHER \- elaborate\!                         25  ▏                                                  0.2%  ← was 78 before Trade Union split-off

Theatre education                          21  ▏                                                  0.1%

Library                                    15  ▏                                                  0.1%

Circus                                      5                                                     0.0%

Printer                                     2                                                     0.0%

\[empty / unknown\_tag\]                       4                                                     0.0%

(Bar scale: each `█` ≈ 130 rows. Generated 2026-05-12.)

### 

### Composition of the punchlist for PI review {#composition-of-the-punchlist-for-pi-review}

[`pi_punchlist.tsv`](https://docs.google.com/spreadsheets/d/1fQj908fbyZQ0C7QhD-MLihOwTy51plSv-9-mw2--o8o/edit?usp=sharing) contains **279 unique rows** (488 total instances across the four files, deduped by name+original-type+canonical+tag) grouped by `review_tag`:

| Review tag | Rows | What it is |
| :---- | :---- | :---- |
| `llm:pi_judgement_needed` | 226 | Gemini-flagged dual/multi-identity cases not covered by a specific dilemma. |
| `pi_dilemma:sub_entity_vs_parent_classification` | 93 | A sub-unit of a named entity (e.g., Kultur-Lige's theatre studio). |
| `pi_dilemma:fraternal_political_dual_identity` | 62 | Arbeter Ring / Arbeter-Farband class. |
| `llm:data_quality` | 36 | Upstream LLM extraction error (the row isn't really an organization). |
| `llm:missing_canonical` | 28 | Gemini suggested a type that doesn't exist in the canonical list. (Most resolved by adding Trade Union / Professional Association; \~15 still open.) |
| `pi_dilemma:judenrat` | 11 | Nazi-era Jewish councils — historiographically contested. |
| `pi_dilemma:zionist_welfare_dual_identity` | 9 | Hadassah, WIZO. |
| `pi_dilemma:sports_zionist_youth_triple_identity` | 8 | Maccabi class. |
| `pi_dilemma:fraternal_welfare_or_other` | 6 | Generic lodges / fraternal orders. |
| `pi_dilemma:brewery_relation_conflict` | 3 | Same entity owned by one person, worked at by another. |
| `pi_dilemma:ostrovski_institute_theatre_or_general` | 2 | core\_db row 484\. |
| `pi_dilemma:vilna_printers_union_trade_or_theatre` | 2 | core\_db row 508\. |
| `pi_dilemma:sao_paulo_yiddish_society_scope` | 2 | core\_db row 493\. |

---

## 2\. Global decisions already made (for PI confirmation) {#2.-global-decisions-already-made-(for-pi-confirmation)}

### 2.1 Canonical typology (28 types) {#2.1-canonical-typology-(28-types)}

The canonical list was extended from the original 25 to 28 through these design decisions. Each is the working assumption — confirm or revise:

#### a) Rename `Society/Union` → `Theatre-related Society/ Union` {#a)-rename-society/union-→-theatre-related-society/-union}

The original `Society/Union` carried a policy note "only when theatre-related." Renamed to encode that policy in the bucket name, so future RAs / LLM passes don't have to remember the constraint.

- [x] Confirmed by PI  
- [ ] Revise → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

#### b) Rename `Labour` → `Labour (factory/workshop)` {#b)-rename-labour-→-labour-(factory/workshop)}

Original `Labour` was ambiguous between "places of physical labour" and "labour movements." Renamed to make explicit that this is a *place* type (factories, sweatshops, workshops). Labour movements, workers' parties, and fraternal-political organizations are routed to **Jewish political bodies** or **Non-Jewish political bodies** instead.

- [x] Confirmed by PI  
- [ ] Revise → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

#### c) Rename `Media (Radio/ Film)` → `Media (Radio/ Film/TV)` {#c)-rename-media-(radio/-film)-→-media-(radio/-film/tv)}

Added TV to the name (and recipients) so 20th-century broadcasters fit cleanly.

- [x] Confirmed by PI  
- [ ] Revise → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

#### d) Split `Political bodies` → `Jewish political bodies` \+ `Non-Jewish political bodies` {#d)-split-political-bodies-→-jewish-political-bodies-+-non-jewish-political-bodies}

Research interest in Jewish-specific political-organizational history (Zionist parties, Bund, Histadrut, Judenrats, Israeli political bodies, JNF, Keren Hayesod, etc.) makes a separate bucket from surrounding non-Jewish polities (Polish Sejm, Soviet commissariats, US Department of X) valuable.

- [x] Confirmed by PI  
- [ ] Revise → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

#### e) Add `Welfare/Aid organization` {#e)-add-welfare/aid-organization}

The original 25-item list had no home for the rich Jewish welfare/mutual-aid landscape: HIAS, JDC, ORT, UJA, ADL, Hadassah, WIZO, Workmen's Welfare Board, mutual-aid burial societies (חסד של אמת), old age homes (מושב זקנים), social self-help orgs (יידישער אַליינהילף).

- [x] Confirmed by PI  
- [ ] Revise → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

#### f) Add `Trade Union / Professional Association` (added 2026-05-12 after Gemini surfaced the gap) {#f)-add-trade-union-/-professional-association-(added-2026-05-12-after-gemini-surfaced-the-gap)}

For occupational unions, trade unions, professional associations, employer associations, scientific/professional societies, guilds, and chambers that are *not* theatre-industry. Examples: printers' unions, garment-trade unions, fur workers, cigarmakers, tailors, State Bar of Michigan, professional scientific societies. Distinct from **Theatre-related Society/Union** (which is specifically theatre/arts industry) and from **Labour (factory/workshop)** (which is the physical place, not the union).

- [x] Confirmed by PI  
- [ ] Revise → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

### 2.2 Methodology decisions {#2.2-methodology-decisions}

#### Pass A → keyword cascade → Gemini verification {#pass-a-→-keyword-cascade-→-gemini-verification}

Hybrid pipeline: (1) trivially-unambiguous tags get a tag-only lookup; (2) ambiguous tags get rule-based keyword resolution with the row's name \+ sentence \+ relation; (3) every shallow-rule decision gets second-opinioned by Gemini 3 Pro using the full policy in the prompt.

- [x] Confirmed by PI

#### LLM as verification authority {#llm-as-verification-authority}

When the LLM disagrees with the rule layer, the LLM's verdict overrides. This affected 1,426 rows out of 3,267 verifiable (44% override rate).

- [x] Confirmed by PI  
- [ ] Revise → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

#### Named-entity allow-list {#named-entity-allow-list}

A curated list of well-known organizations (HIAS, JDC, Arbeter Ring, Bund, YIVO, Kultur-Lige, Polish Army, Red Cross, etc.) gets classified by exact name match with word-boundary checking, regardless of what the LLM extractor tagged them as. PI-dilemma flags are attached directly in the allow-list for known contested organizations.

- [x] Confirmed by PI  
- [ ] Add other named entities → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

## 

## 3\. Per-dilemma decisions (PI verdict needed) {#3.-per-dilemma-decisions-(pi-verdict-needed)}

For each tag below, the punchlist contains rows currently classified by the working default. The PI needs to either confirm the default or specify a different policy.

### 3.1 `pi_dilemma:fraternal_political_dual_identity` — 62 rows {#3.1-pi_dilemma:fraternal_political_dual_identity-—-62-rows}

**What it is:** Workers' fraternal-political-cultural organizations — primarily **Arbeter Ring / Workmen's Circle** (`אַרבעטער-רינג`) and **Yidish-Natsionaler Arbeter-Farband / Jewish National Workers Alliance** (`אַרבעטער-פֿאַרבאַנד`). These are simultaneously:

- mutual-aid / fraternal societies (insurance, burial, sickness benefits),  
- labour-political movements (socialist or Labor-Zionist),  
- sponsors of Yiddish cultural infrastructure (theatres, choirs, schools).

No single canonical bucket captures all three identities.

**Current default:** **Jewish political bodies** (because the political-ideological identity is what distinguishes them from generic fraternal lodges). Sub-units (schools, camps, summer camps, theatre groups) are reclassified to their sub-type when detected.

**Examples in the punchlist:** `אַרבעטער-רינג`, `יידיש-נאַציאָנאַלן אַרבעטער-פֿאַרבאַנד`, branches of these.

**Alternatives:**

- Welfare/Aid organization (foregrounds the mutual-aid function)  
- Theatre-related Society/Union (foregrounds the cultural sponsorship)  
- A new "Fraternal-Political Society" type (only if many similar orgs exist)

**PI decision:**

- [x] Keep default (Jewish political bodies)  
- [ ] Use → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_  
- [ ] Case-by-case (specify in the punchlist TSV)  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.2 `pi_dilemma:judenrat` — 11 rows {#3.2-pi_dilemma:judenrat-—-11-rows}

**What it is:** Jewish councils imposed by Nazi authorities in occupied territories, 1939–1945. Historiographically contested: were they legitimate Jewish self-administration, coerced collaboration, or a third thing?

**Current default:** **Jewish political bodies** (since they performed political-administrative functions over Jewish communities).

**Examples:** `יודענראַט`, `יידנראָט`, `וואַרשעווער יודענראַט`.

**Alternatives:**

- Non-Jewish political bodies (foregrounds the Nazi-imposed nature)  
- Not an organization (rejects them as a coerced extension of Nazi administration)  
- A new "Holocaust-era forced administrative body" type

**PI decision:**

- [ ] Keep default (Jewish political bodies)  
- [x] Use → \_\_\_\_\_\_\_\_Judenrat\_\_\_\_\_\_  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.3 `pi_dilemma:zionist_welfare_dual_identity` — 9 rows {#3.3-pi_dilemma:zionist_welfare_dual_identity-—-9-rows}

**What it is:** Hadassah (`הדסה`) and WIZO (`וויצאָ`). Zionist by name and founding ideology, but the primary observable activity is women's welfare, education, and hospitals.

**Current default:** **Welfare/Aid organization** (primary activity is welfare/health/education).

**Alternatives:**

- Jewish political bodies (foregrounds the Zionist political identity)  
- Both? Multi-typing the entity (would require a schema change)

**PI decision:**

- [x] Keep default (Welfare/Aid organization)  
- [ ] Use → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.4 `pi_dilemma:sports_zionist_youth_triple_identity` — 8 rows {#3.4-pi_dilemma:sports_zionist_youth_triple_identity-—-8-rows}

**What it is:** **Maccabi** (`מכבי`) and Maccabi branches. Triple identity: Jewish sports movement \+ Zionist political ideology \+ youth education.

**Current default:** **Education** (the youth-education function is the most consistent across branches).

**Alternatives:**

- Jewish political bodies (foregrounds the Zionist ideology)  
- Welfare/Aid organization (foregrounds youth-services aspect)  
- Add a new **Sports/Recreation** canonical (the gap is real but the volume of sports orgs in the lexicon is small — \~2 rows besides Maccabi)

**PI decision:**

- [ ] Keep default (Education)  
- [ ] Use → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_  
- [x] Add new `Sports/Recreation` canonical  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.5 `pi_dilemma:fraternal_welfare_or_other` — 6 rows {#3.5-pi_dilemma:fraternal_welfare_or_other-—-6-rows}

**What it is:** Generic fraternal orders not affiliated with a specific Jewish political movement — Knights of Pythias (`נייטס אָוו פּיטיס`), Masonic lodges (`מעסאניק לאָדזש`), Grand Street Boys (`גרענד סטריט-באָיס`).

**Current default:** **Welfare/Aid organization** (most Jewish fraternal orders functioned as mutual-aid societies; non-Jewish fraternal orders that appear in the lexicon are usually noted because they admitted Jewish members).

**Alternatives:**

- OTHER \- elaborate\! (acknowledge the bucket doesn't fit cleanly)  
- Add a new **Fraternal order** canonical

**PI decision:**

- [ ] Keep default (Welfare/Aid organization)  
- [x] Use → \_\_\_\_**Fraternal order**\_\_  
- [x] Notes: \_\_\_Grand Street Boys is not related as it was an informal street-based youth association\_\_

---

### 3.6 `pi_dilemma:sub_entity_vs_parent_classification` — 93 rows {#3.6-pi_dilemma:sub_entity_vs_parent_classification-—-93-rows}

**What it is:** A named entity (Kultur-Lige, Arbeter Ring, IKUF, YIVO, etc.) has sub-units (a theatre studio, a school, a publishing arm, a choir, a library). The sub-unit's name contains both the parent's name and the sub-unit head. Should the canonical type follow the sub-unit (Theatre studio → Theatre education) or the parent (Kultur-Lige → Heritage Institution)?

**Current default:** **The LLM verification pass already decided per-row** (mostly by sub-entity head: theatre studio → Theatre education; choir → Musical organization; school → Education; publishing arm → Publisher; library → Library). The rule layer flagged these for PI awareness; the LLM applied the sub-entity-head classification.

**Examples in the punchlist:**

- `יידישער טעאַטער-שול ביי דער „קולטור-ליגע"` → currently **Theatre education**.  
- `טעאַטער-סטודיע „קולטור-ליגע"` → currently **Theatre education**.  
- `אַרבעטער רינג-שול` → currently **Education**.  
- `„הזמיר"-כאָר` → currently **Musical organization**.  
- `איקוף-פֿאַרלאַג` → currently **Publisher**.

**PI options:**

- [x] **Accept current policy: classify by sub-entity head** (the LLM's choice, on a per-row basis).  
- [ ] **Override: always classify by parent organization** (revert all 93 rows back to the parent's canonical type).  
- [ ] **Case-by-case**: review individual rows in the punchlist.  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.7 `pi_dilemma:brewery_relation_conflict` — 3 rows {#3.7-pi_dilemma:brewery_relation_conflict-—-3-rows}

**What it is:** Physical workplaces (breweries, factories, workshops, bakeries, farms, shops, firms) where the same entity is a Business for an owner (in one mention) and Labour for an employee (in another mention).

**Current default:** Use the **`relation_category`** column on each mention — `Leadership_Ownership` routes to Business, `Employment_Performance` routes to Labour. When relation is missing or ambiguous, default to Business and flag.

**Examples:** `פּילנער ביר-ברויעריי` (Brewery), `פֿאָרד מאָטאָר קאָמפּאני` (Ford Motor Company — employee mention → Labour). Note: at cluster/DB level the relation is lost; one canonical type per entity.

**Open policy question for the cluster/DB level:** When a single DB-entity has both Business mentions and Labour mentions, what should the DB record show?

**PI decision:**

- [ ] Confirm per-mention type follows relation  
- [ ] At DB level: pick majority-relation  
- [x] At DB level: allow two relation-typed labels (`business_role_type=Business; worker_role_type=Labour`) — schema change  
- [ ] At DB level: pick the entity's intrinsic type regardless of relation (e.g., always Business for an enterprise)  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.8 `pi_dilemma:vilna_printers_union_trade_or_theatre` — 2 rows (core\_db row 508\) {#3.8-pi_dilemma:vilna_printers_union_trade_or_theatre-—-2-rows-(core_db-row-508)}

**What it is:** Vilna Printers' Union (`ווילנער דרוקער-פאַריין`) — historically a trade union of Vilna printing workers, but the lexicon mentions it as a *venue* where theatre performances took place.

**Current default:** **Theatre-related Society/ Union** (because the lexicon's interest in it is as a theatre venue/sponsor).

**Alternatives:**

- Trade Union / Professional Association (its primary identity outside the lexicon)  
- Labour (factory/workshop) (foregrounds the printers' workplace identity)

**PI decision:**

- [x] Keep default (Theatre-related Society/ Union)  
- [ ] Use → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.9 `pi_dilemma:ostrovski_institute_theatre_or_general` — 2 rows (core\_db row 484\) {#3.9-pi_dilemma:ostrovski_institute_theatre_or_general-—-2-rows-(core_db-row-484)}

**What it is:** `אינסטיטוט א'נ פֿון אָסטראָווסקי` (Ostrowski Institute). Ambiguous — could refer to Alexander Ostrovsky (Russian playwright; the institute would be a theatre-research institute) or to a different Ostrowski (general academic institute).

**Current default:** **Education** (general).

**Alternatives:**

- Theatre education (if the Yiddish-theatre Ostrowski is meant)  
- Heritage Institution (research institute on theatre history)

**PI decision:**

- [x] Keep default (Education)  
- [ ] Use → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.10 `pi_dilemma:sao_paulo_yiddish_society_scope` — 2 rows (core\_db row 493\) {#3.10-pi_dilemma:sao_paulo_yiddish_society_scope-—-2-rows-(core_db-row-493)}

**What it is:** `סאַן פּאָולאַ יידישער געזעלשאַפט` (São Paulo Yiddish Society). Mentioned in the lexicon as supporting Yiddish cultural activity; unclear if its primary function is theatre-related, welfare, or generic communal.

**Current default:** **Theatre-related Society/ Union** (Zalmen-is-theatre-prior).

**Alternatives:**

- Welfare/Aid organization (if welfare/mutual-aid is the primary function)  
- Jewish political bodies (if Zionist/political)

**PI decision:**

- [ ] Keep default (Theatre-related Society/ Union)  
- [x] Use → \_\_Jewish political bodies\_\_\_\_\_\_\_\_\_\_\_  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.11 `llm:pi_judgement_needed` — 226 rows {#3.11-llm:pi_judgement_needed-—-226-rows}

**What it is:** Catch-all bucket — rows the LLM verified but flagged as needing PI judgement because the dilemma didn't match a specific named category above. Most of these are clearly classified (current canonical is reasonable) but have a subtle dual-identity worth a second pair of eyes. Browse the punchlist for examples.

**Recommended PI workflow:** scan column `llm_reason` in the punchlist for the LLM's specific reason. Override the canonical only where the reasoning is unconvincing.

- [x] Accept LLM's choices for the 226 rows  
- [ ] Spot-check and adjust specific rows  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.12 `llm:data_quality` — 36 rows {#3.12-llm:data_quality-—-36-rows}

**What it is:** Rows where the upstream LLM extraction created a malformed entry. Examples: `האָליוויד` ("Hollywood" — a place, not an org), `ציב` (acronym with insufficient context), `אברהם קאָפּקע` (a person mis-tagged as an organization).

**Current default:** Whatever the LLM verification picked (usually "Not an organization" or "OTHER \- elaborate\!").

**Recommended PI action:** confirm "Not an organization" for these and consider flagging at the upstream extractor pipeline so similar errors don't recur.

- [ ] Confirm — route all to Not an organization  
- [x] Case-by-case  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

### 3.13 `llm:missing_canonical` — 28 rows (most already resolved) {#3.13-llm:missing_canonical-—-28-rows-(most-already-resolved)}

**What it is:** Rows where Gemini suggested adding a new canonical type. The biggest cluster (\~22 of 28\) wanted `Trade Union / Professional Association`, which has now been added — those rows have been reclassified.

**Remaining (\~6) suggestions** are low-volume one-offs: `Sports/Recreation` (2), `Event series / Forum` (1), `Historical movement` (1), `Cultural/Intellectual Society` (1), `Commemorative Organization` (1), `Research Project/Expedition` (1). None hit the volume threshold to justify a new canonical.

**Recommended PI action:** confirm these belong in `OTHER - elaborate!`, or accept any additional new canonical types you want to add.

- [x] Route remaining 6 to OTHER \- elaborate\!  
- [ ] Add new canonical(s) → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

## 4\. Additional cross-cutting policy questions (from earlier discussion) {#4.-additional-cross-cutting-policy-questions-(from-earlier-discussion)}

### 4.1 Brewery / factory / workshop policy at the DB level {#4.1-brewery-/-factory-/-workshop-policy-at-the-db-level}

When a single brewery DB-entity has mentions of both ownership (→ Business) and employment (→ Labour), what's the DB-level canonical?

- [ ] Pick majority-relation (lossy but simple)  
- [x] Allow two typed labels (schema change)  
- [ ] Always use the entity's intrinsic type (e.g., Business for any commercial enterprise regardless of relation)  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

### 4.2 Generic `society` / `club` / `association` default when no cue found {#4.2-generic-society-/-club-/-association-default-when-no-cue-found}

About 250 rows in `context_weak` had a generic tag (society/club/association/organization) with no theatre/welfare/political/religious/musical/labour cue in name or sentence. Currently defaulted to **Theatre-related Society/Union** under the Zalmen-is-theatre prior.

- [ ] Keep default (Theatre-related Society/Union)  
- [ ] Default to OTHER \- elaborate\! instead  
- [x] Default to Trade Union / Professional Association instead  
- [ ] Notes: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

### 4.3 Defaults for other context\_weak categories {#4.3-defaults-for-other-context_weak-categories}

The same Zalmen-is-theatre prior was applied to defaults for: institute (→ Education), academy (→ Education), production (→ Business), camp (→ Not an organization), youth\_organization (→ Education).

- [ ] Confirm all  
- [ ] Specify overrides → \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

---

## 5\. Summary checklist (for the PI to confirm at the end) {#5.-summary-checklist-(for-the-pi-to-confirm-at-the-end)}

- [x] Canonical typology of 28 types accepted as-is, or with the revisions noted above  
- [ ] Decisions for each `pi_dilemma:*` category recorded above  
- [ ] All `llm:pi_judgement_needed` rows reviewed (or accepted as the LLM proposed)  
- [ ] All `llm:data_quality` rows confirmed as Not an organization  
- [ ] All `llm:missing_canonical` rows confirmed in OTHER (or with new types added)  
- [ ] DB-level brewery/factory policy decided  
- [ ] context\_weak default policy decided  
- [ ] Sign-off date: \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

Once this is filled in, the answers will be integrated:

- New canonical types (if any) → added to [CLASSIFICATION\_POLICY.md](http://CLASSIFICATION_POLICY.md), [map\_canonical\_types\_v3.py](http://map_canonical_types_v3.py), and applied to the data TSVs.  
- Per-row PI decisions for sub-entities, fraternal-political dual identities, etc. → applied to the four mapping TSVs and source TSVs.  
- Default policy revisions → re-run the affected rule branches and the LLM verification pass for the relevant subset.

