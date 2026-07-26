# PI Decisions — Zylbercweig Organizations Alignment

Each section below asks for one decision. Tick the box that fits, or — if it's
complicated — write a longer note in the "**Notes**" field at the bottom of the
section. Don't worry about voting in order; pick the easy ones first.

The bigger picture: we ran a Gemini-3-Flash drafter over all 7158 undecided
clusters and it proposed an alignment decision for each. We have 6859 high-
confidence drafts ready for batch confirmation. The questions below are the
*edge cases* where the drafter (or the matcher) needs PI input before we can
move forward at scale.

---

## 1. Five itinerant clusters the drafter wants to SPLIT

These are five Traveling Company clusters where the drafter proposed SPLIT
because the mentions span many cities. Our codified rule (in
`pre_explode_clusters.TROUPE_TYPES`) says that touring across cities does **not**
by itself indicate distinct organizations — a single troupe travels by
definition. So the auto-stamper held these back for you.

The question per cluster is the same: is it really one touring entity, or did
the drafter spot something real that our rule missed?

### 1a. `ORG-C00095` יידישע פֿאָלקס-בינע (Yiddish People's Stage)
Drafter rationale: "Name is a common designation; settlements (Kiev, LA, Latvia) are geographically disparate and unlikely to represent a single traveling company."

- [ ] **Keep as ONE cluster** (touring troupe — the drafter is wrong)
- [ ] **SPLIT** (the drafter is right; these are distinct local stages sharing a generic name)
- [ ] **Defer / not sure**

### 1b. `ORG-C00101` יידישע בינע (Yiddish Stage)
Drafter rationale: "Generic name 'Yiddish Stage' used by distinct entities in Vienna, Melbourne, and Poland; no candidates available."

- [ ] **Keep as ONE cluster**
- [ ] **SPLIT**
- [ ] **Defer / not sure**

### 1c. `ORG-C01280` קאָאָפּעראַטיווער טרופּע (Cooperative Troupe)
Drafter rationale: "Generic name 'Cooperative Troupe' combined with distinct locations (Montreal, Cleveland, Krakow, Homel) indicates multiple unrelated local cooperatives."

- [ ] **Keep as ONE cluster**
- [ ] **SPLIT**
- [ ] **Defer / not sure**

### 1d. `ORG-C02111` יידיש רעווי-טעאַטער (Yiddish Revue Theatre)
Drafter rationale: "Cluster spans distinct permanent locations (Warsaw, Tashkent, Bucharest) and specific venues ('Novostshi') that represent different local revue theaters."

Note: this one may be **mis-typed** in our DB as Traveling Company. If it's really a venue-based theatre, the tour rule doesn't apply and SPLIT is correct.

- [ ] **Keep as ONE cluster** (touring troupe)
- [ ] **SPLIT and re-type as Theatre** (these are local venues)
- [ ] **SPLIT, keep type as Traveling Company**
- [ ] **Defer / not sure**

### 1e. `ORG-C05162` יידישער טרופּע (Yiddish Troupe)
Drafter rationale: "Cluster name is generic and variants explicitly list distinct troupes with different leaders and locations (Montreal, South Africa, Lublin, etc.)."

- [ ] **Keep as ONE cluster**
- [ ] **SPLIT**
- [ ] **Defer / not sure**

**Notes for section 1 (it's complicated, or general comment):**
>


---

## 2. Two clusters the drafter left as DISCUSS

These two were previously aligned to the **wrong** DB row (we caught crossed
alignments and reverted them). The drafter re-ran but couldn't bridge the
cross-script gap on its own — it left them as DISCUSS. Best guesses below:

### 2a. `ORG-C00043` שמ׳רן (Shomers, abbreviated)
This looks like the Yiddish abbreviation of "שמ״ר" (Shomer = Nokhem-Meyer Shaykevitsh's pen name), in the possessive/plural form. Our DB has **db_id 385 = Shomer Troupe**. The matcher missed this because it can't normalize the geresh abbreviation marker (׳).

- [ ] **ALIGN to db_id 385 (Shomer Troupe)** ← our best guess
- [ ] **NEW** (this is a different entity)
- [ ] **GENERIC / DISCUSS / other** — explain in notes

### 2b. `ORG-C00044` גאַרטענשטיין (Gartenstein)
Single name, no other context. No DB row currently named Gartenstein. Most likely a person's name being used as a troupe shorthand → NEW.

- [ ] **NEW** (create a new DB row for Gartenstein's troupe) ← our best guess
- [ ] **ALIGN** to an existing DB row → which db_id? ___
- [ ] **GENERIC / DISCUSS / other** — explain in notes

**Notes for section 2:**
>


---

## 3. Five Yiddish-variant proposals held for review

We're enriching Latin-only DB rows with their Yiddish forms so cross-script
matching can find them next time. Most were auto-approved. These five had
concerns I flagged. For each, tick whether to APPROVE the proposed Yiddish,
REJECT it, or provide a correction.

### 3a. `db_id 122` Perry Theatre → `פּערי-טעאָ טער`
Mid-word space looks like a typo. Standard form would be `פּערי-טעאַטער`.

- [ ] APPROVE as-is (`פּערי-טעאָ טער`)
- [ ] APPROVE with correction: `פּערי-טעאַטער`
- [ ] APPROVE with different correction: ____________
- [ ] REJECT (leave name_yiddish empty)

### 3b. `db_id 257` Gimpel Theatre → `גימפּעלס טרופּע`
The proposed Yiddish means "Gimpel's **troupe**", but the DB row name says
**Theatre** (a venue). These are conceptually different — Gimpel had both a
permanent theatre in Lemberg and touring activities. Worth checking which the
DB row is supposed to represent.

- [ ] APPROVE as-is (treat DB row as the touring troupe)
- [ ] REJECT (these are two different concepts; leave name_yiddish empty)
- [ ] APPROVE with correction: ____________ (for the Theatre/venue)
- [ ] Split the DB row into two (one Theatre, one Traveling Company)

### 3c. `db_id 385` Shomer Troupe → (depends on §2a)
This auto-resolves once you decide §2a. If you ALIGN ORG-C00043→385 there, the
right Yiddish form (likely `שמ׳רן` or `שמ״ר`'ס טרופּע') will be picked up by the
next sweep.

- [ ] No separate decision needed — depends on §2a
- [ ] Force a specific name_yiddish: ____________

### 3d. `db_id 391` Spivakovski Troupe → (no good Yiddish source)
After we reverted the bad alignment, there's no Yiddish-cluster pointing to
this row anymore. Either leave name_yiddish empty (matcher won't help) or
write a canonical form by hand.

- [ ] Leave empty for now
- [ ] Provide canonical Yiddish: ____________ (e.g. `ספּיוואַקאָווסקיס טרופּע`)

**Notes for section 3:**
>


---

## 4. Empty-type DB review (104 rows)

We have 104 DB rows whose `org_type` is empty. Empty types disable the
matcher's blocking optimization for those rows (they fall into a fail-open
pool that's matched against every cluster). Filling them in makes matching
much more selective.

For each row in [db_empty_type_review.tsv](db_empty_type_review.tsv) the
keyword classifier already proposed a `suggested_type`:

- **39 high-confidence** keyword matches (Theatre / Traveling Company /
  Publisher / Journals / etc.) — likely fine to bulk-approve
- **15 medium-confidence**
- **50 no-match** (need PI eyes — mostly Hebrew/Yiddish newspaper titles,
  organizations, transliterated names without clear keyword)

PI does NOT need to review row-by-row in a doc — better workflow:

- [ ] Open the TSV directly, set `pi_decision=APPROVE` (and optionally edit
      `pi_type`) on every row you want applied
- [ ] Skim and approve only the 39 high-confidence rows for now
- [ ] Skip entirely (low priority)
- [ ] Other: explain in notes

**Notes for section 4:**
>


---

## 5. Batch-confirm the high-confidence drafts (~6859 clusters)

This is the **single biggest lever** on the project. The drafter produced
6859 high-confidence alignment proposals, and the Streamlit app now has a
batch-confirm panel for working through them in pages of 25 with one-click
accept.

We measured 86% entity-match agreement against held-out RA decisions, so
batching with quick visual review per page is realistic.

Breakdown of what's in the queue:
- **~179 ALIGN** drafts (existing DB row proposed) — recommend reviewing carefully
- **~5850 NEW** drafts — most are correctly new orgs
- **~727 GENERIC** drafts — generic descriptors, not real entities
- **~265 DISCUSS** drafts — drafter thinks PI needs to weigh in
- **~27 DEFER** drafts — drafter punted

How would you like to divide this up?

- [ ] **Sinai / Bella works through it as time allows** (status quo)
- [ ] **Bring in additional RAs** for a coordinated push
- [ ] **PI does ALIGN drafts first**, RAs do NEW drafts in bulk
- [ ] **Other** — explain in notes

**Notes for section 5:**
>


---

## 6. Two deferred technical decisions

These are matcher-quality tweaks that have been deferred since the original
project plan. Both require code changes plus a re-test pass against our
calibration holdout. Estimated time per item: an afternoon.

### 6a. Generic-token over-weighting
Right now "X טרופּע" and "Y טרופּע" can score high similarity even when X and
Y disagree, because the shared generic suffix dominates. Two known holdout
misses are caused by this. Options for the fix:

- [ ] **Kernel-preference** (compare the proper-name kernel directly, ignore the generic tail)
- [ ] **TF-IDF token weighting** (downweight common tokens like טרופּע, טעאַטער automatically)
- [ ] **Token-level Jaccard with IDF**
- [ ] **Multiplicative kernel-sim penalty**
- [ ] **Defer indefinitely**
- [ ] **Other / discuss** — explain in notes

### 6b. IPA threshold tweak
Current threshold is 0.60. Second Avenue Theatre scored 0.538 (a known miss).
Lowering would catch more true positives but risks new false positives.

- [ ] **Lower to 0.55** and re-test
- [ ] **Lower to 0.50** and re-test
- [ ] **Keep at 0.60** for now
- [ ] **Other / discuss** — explain in notes

**Notes for section 6:**
>


---

## General comments / things we missed
>

---

## Multi-place decided clusters (2026-05-20 audit cohort)

After adding the QID-based exploder for **undecided / SPLIT** clusters, the
following decided non-itinerant clusters still span ≥2 distinct QIDs. The PI's
guidance on each is needed before we either force-split or accept as a true
multi-location entity. Full list in
[decided_multi_place_audit.tsv](decided_multi_place_audit.tsv); these are the
items Sinai flagged for PI.

### Already actioned this session (no decision needed)
- `ORG-C00103` ווילנער טרופּע (Vilna Troupe) — retyped DB 551 + cluster + all 178 mentions to **Company on Tour**; PI confirmed.
- `ORG-C00276` הבימה (Habima) — retyped 32 cluster mentions to **Company on Tour** (DB 266 already correct); PI confirmed.
- `ORG-C00682` סעקאָנד עוועניו טעאַטער — single anomalous Newark mention (Serebrov entry) was an extraction error; cleared, cluster is NYC-only now.
- `ORG-C04319` ניו יאָרקער יידישן קונסט-טעאַטער — 2 Vienna mentions were tour/filming **action locations**, not the theatre's home; cleared, cluster is NYC-only.
- `ORG-C03238` ווינער אוניווערזיטעט — one mis-clustered row was a **Vilna** University mention (ווילנער ≠ ווינער); relabeled to ORG-C03238_Q02 and removed from cluster.
- `ORG-C00257` פֿאַרווערטס + `ORG-C00831` השומר הצעיר — see "Top-organization / sub-organization schema" below.

### Top-organization / sub-organization schema (PI confirmed 2026-05-20)
- Added `parent_db_id` column to `core_db.tsv` (empty for existing rows).
- Forverts (DB 249) and Hashomer Hatzair (DB 602) split per city; new sub-rows allocated db_ids 692–698 with `parent_db_id` pointing at the umbrella. Each city sub-cluster auto-ALIGN'd to its sub-row.
- Cross-reference: deferred Brewery DB schema TODO ([memory: todo_brewery_db_schema.md]) — this is the same kind of relation modeling and they should converge.
- Zalmen app does not yet render parent/child. UI work deferred.

### Pending PI decision

Each of the following needs a one-line PI call: **(a)** Keep as multi-location ALIGN/NEW (the entity really had operations in those cities), **(b)** Force-split per city (separate orgs sharing the name), **(c)** Apply the top-organization schema (Forverts-style: umbrella + city sub-rows), or **(d)** Other / discuss.

- [ ] **`ORG-C00145`** גימנאַזיע (DESCRIPTIVE, Education, 21 cities) — generic word; kept DESCRIPTIVE this session. Confirm OK?
- [ ] **`ORG-C00539`** טאָג (ALIGN, Newspapers, 6 cities: Warsaw / Vilnius / NYC / St. Petersburg / Kraków / Philadelphia) — likely separate "Der Tog" papers per city.
- [x] **`ORG-C00752`** אונזער ווינקל — **Resolved 2026-05-20 (cluster-research skill, Sinai)**: Boymvol-founded Yiddish theatre Kharkov 1918 → Kyiv 1919-1920 (briefly Kyiv Yiddish State Theatre); Boymvol murdered by Polish soldiers 1920 ended the entity. Source: Yiddish Leksikon entry for Yehude-Leyb Boymvol. Minsk 1919 retained as affiliated wartime branch. **Hrodna mention** (Zok writing reviews) split to **ORG-C00752_PUB** — separate periodical called "Unzer Vinkel". **Haifa+Tel Aviv 1925 mentions** (from "Yiddish theatre in Eretz-Yisrael" entry) split to **ORG-C00752_PAL** — separate amateur drama circle in Mandate Palestine, members from Warsaw/Hrodne/Galicia. DB 427 gained `name_yiddish=אונזער ווינקל`.
  - **Updated 2026-07-26 (cluster-research rerun, Sinai): upgraded to the top-organization schema.** The lexicon's own entry on the theatre (vol 6, `P-6-facs_71_r_6`) attests founding Kharkov 1918 by Boymvol/Rafalski/Zilberberg/Stutshkov, a contemporaneous Kyiv art-theatre under Boymvol (1919-21), and a Minsk instance under Rafalski from 1919 — the mother troupe migrated there 1920 and was nationalized 1921 ("name taken off the posters"). Three concurrent city operations with distinct leaderships ⇒ umbrella DB 427 (`org_type=Theatre`) + children **1785 Kharkov / 1786 Kyiv / 1787 Minsk** (`parent_db_id=427`). `ORG-C00752` force-split `_Q01`(Kharkov,10)/`_Q02`(Kyiv,5)/`_Q03`(Minsk,1); remainder (org's own entry + 2 unlocalized mentions) stays ALIGNed to the umbrella. City clusters re-pointed: C04372/C04513/C05083→1785, C00504→1786, C04174/C04763→1787. Homonym splits decided: **ORG-C00752_PAL → NEW DB 1788**, **ORG-C00752_PUB → NEW DB 1789** (Journals/ Newspapers), both outside the umbrella. Defect fixed: bogus `ORG-C00794` (Boiberik camp) removed from 427's `linked_cluster_ids`.
- [ ] **`ORG-C00005`** פּאַוויליאָן-טעאַטער (UNCLUSTER, Theatre, 4 cities: Warsaw / Chernivtsi / London / Chicago) — already UNCLUSTER; force-split to realize it?
- [x] **`ORG-C00390`** לענאָקס-טעאַטער — **Resolved 2026-05-20 (Sinai)**: keep as ALIGN multi-location. One theatre under Goldberg & Jacobs across Harlem (1911-c1916) and the Bronx (1922+); same operators throughout. The Bronx/Harlem/NYC labels are NYC-borough/neighborhood variants of one theatre's locations.
- [x] **`ORG-C00595`** פּראָספּעקט-טעאַטער — **Resolved 2026-05-20 (Sinai)**: keep as ALIGN multi-location. One Bronx theatre under Goldberg & Jacobs; sources literally write "פּראָספּעקט-טעאַטער אין ניו-יאָרק (בראָנקס)". Bronx and NYC are interchangeable labels for the same venue.
- [x] **`ORG-C00051`** באַלאַגעניידען — **Resolved 2026-05-20 (cluster-research skill, Sinai)**: one kleynkunst-theater founded 1938 in Vilna by Yoel Bergman + Yakov Reynglas (both ex-Vilner Trupe), lasted until WWII. The org's own lexicon entry says "organized in Danzig" (likely founders' planning/meeting), but Bergman's, Birnbaum's, and Reynglas's biographical entries all anchor it as a Vilna 1938-9 operation. Sinai's manual ALIGN of `_Q01` (Vilna) + `_Q02` (Danzig) to DB 497 confirmed. Additional cluster `ORG-C01023` (a typo-separated mention: `גליינקונסט` instead of `קליינקונסט`) also aligned to DB 497. DB 497 `linked_cluster_ids` updated to `ORG-C00051_Q01 | ORG-C00051_Q02 | ORG-C01023` (previous `ORG-C00054_S01` was a stale orphan reference). External corroboration: Caplan, *Yiddish Empire*, lists Balaganeydn among Yiddish theater companies led by ex-Vilner-Trupe members in Poland.
- [ ] **`ORG-C00559`** אונדער לעבען (ALIGN, Newspaper, Warsaw + Odessa) — historical question: same "Unzer Lebn" paper that moved, or two? Spelling אונדער is consistent across all 9 mentions (not a per-row typo).
- [ ] **~12 q=2 Theatre batch** — Boston/Chelsea, Vilnius/Kaunas, Warsaw/NYC, Vilnius/Łódź ×2, Vienna/NYC, Białystok/Minsk, Kharkiv/Samarkand, NYC/Philadelphia. Default-recommended action: force-split each, unless PI knows specific cases that should stay merged.
- [ ] **~4 q=2 Newspapers batch** — Toronto/London, Warsaw/London, Brest/Rakaŭ, NYC/Philadelphia (Di Varhayt). Default: force-split (separate papers).
- [ ] **Ghetto pairs kept as ALIGN/NEW** — ORG-C02258 עלדאָראַדאָ Warsaw + Warsaw Ghetto; ORG-C02738 געטאָ-טעאַטער Vilnius + Vilna Ghetto. Same physical space at different periods. Confirm keep?

### Memory pointer
Cohort decision rules are documented in the session memory note
`project_settlement_collapse_pipeline.md` and `project_org_matching_drafter.md`.

