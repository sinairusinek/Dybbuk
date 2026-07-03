# Zalmen People Matcher — Autonomous Scaffold

> **Phase C (2026-07-02) — person hub + draft review + surname disambiguation.**
> See the "Phase C" section at the bottom; the original scaffold notes below it
> predate Phases A–C and some counts are stale.

State as of the initial autonomous build. Everything here was produced without your in-the-loop approval, so treat each artifact as a draft to verify and adjust.

## TL;DR

A first-cut people-matching pipeline now exists at `Zylbercweig/people/`, mirroring the org pipeline shape:

| Stage | Org pipeline | People pipeline (new) |
|---|---|---|
| Source flatten | `cluster_orgs.py` → `organizations_clustered.tsv` | `flatten_volumes.py` → `people_extracted.tsv` |
| Target DB | `core_db.tsv` | `people_db.tsv` |
| Candidate gen | `prepare_alignment.py` → `org_alignment_review.tsv` | `prepare_alignment.py` → `people_alignment_queue.tsv` |
| Drafter | `llm_draft_alignment.py` → `org_alignment_drafts.tsv` | `llm_draft_alignment.py` → `people_alignment_drafts.tsv` (not yet executed) |
| Similarity | reused `kimatch` + `dybbuk_phonetic` | `people_similarity.py` (new — surname-first, order-invariant, bracket-alias) |
| Dedup | (org-side: cluster pairs) | `find_dedup_candidates.py` → `people_dedup_candidates.tsv` |

The matcher targets THREE distinct jobs (we identified these from the existing review artifacts before building):

1. **Subject-entry → external DB** (DiJeSt / Zylbercweig-people DB) — analog of org alignment.
2. **Subject-entry → subject-entry dedup across volumes** — the Lexicon writes some people up twice; the `Duplication Check` xlsx sheet has ~67 confirmed pairs as ground truth.
3. **In-text mention → subject-entry** — Vol 3 alone has 4,393 in-text people mentions with `name/gender/description/relation`; plus 5,617 mention validations in `extracted names validations.xlsx`. Hardest of the three; not yet scaffolded — see "Deferred" below.

Jobs 1 and 2 are scaffolded; the LLM drafter is scaffolded but **not yet run** (costs real money; needs your few-shot verification first).

## Files produced

All under `Zylbercweig/people/`:

### Scripts
- [flatten_volumes.py](flatten_volumes.py) — read all 6 rich-schema volumes + Vol 3 mentions schema; emit two TSVs.
- [import_review_xlsx.py](import_review_xlsx.py) — convert the 3 ZylbercweigPeople xlsx files into 7 TSVs (DB, alignment decisions, unmatched index, mention validations, credits).
- [people_similarity.py](people_similarity.py) — the similarity layer; `python people_similarity.py` runs a smoke test.
- [find_dedup_candidates.py](find_dedup_candidates.py) — cross-volume dedup candidate generation; produces `people_dedup_candidates.tsv` + a gold-eval file.
- [prepare_alignment.py](prepare_alignment.py) — build the per-entry alignment queue with top-5 DB candidates.
- [llm_draft_alignment.py](llm_draft_alignment.py) — Gemini-3-Flash drafter scaffold. `--dry-run` prints the prompt; `--execute` calls the API.

### Data TSVs
- [people_extracted.tsv](people_extracted.tsv) — 3,081 subject-entries flattened from all 7 volumes (24 columns).
- [people_mentions_extracted.tsv](people_mentions_extracted.tsv) — 4,393 in-text mentions from Vol 3 only.
- [people_db.tsv](people_db.tsv) — 825 DiJeSt/Zylbercweig-people target DB rows (db_id, hebname, english). Many have only the English form.
- [people_alignment_review.tsv](people_alignment_review.tsv) — 3,164 RA decisions imported from `Json Review (8).xlsx` (Duplication Check + alignment sheets).
- [people_index_unmatched.tsv](people_index_unmatched.tsv) — index names not found in any entry heading (per-volume `align` sheets).
- [mention_validations_full.tsv](mention_validations_full.tsv) — 5,617 frequency-ranked mentions → heading mappings.
- [mention_validations_initials.tsv](mention_validations_initials.tsv) — 1,622 initial-form → heading.
- [mention_validations_surnames.tsv](mention_validations_surnames.tsv) — 2,941 bare surnames → heading with disambiguation.
- [credited_persons.tsv](credited_persons.tsv) — 50 byline credits (`ש. ע. פֿון X`) awaiting full-name resolution.
- [people_dedup_candidates.tsv](people_dedup_candidates.tsv) — produced by `find_dedup_candidates.py`.
- [people_dedup_gold_eval.tsv](people_dedup_gold_eval.tsv) — recall report against Duplication Check sheet.
- [people_alignment_queue.tsv](people_alignment_queue.tsv) — produced by `prepare_alignment.py`.

## Results so far

### Dedup candidate pass (job 2)
- 14,156 candidate pairs at composite score ≥ 0.55 (~779 high-conf ≥ 0.85).
- **Recall vs 67 RA-confirmed same-person pairs: 62/67 = 92.5%.**
- Of the 5 missed pairs, 4 are "phantom" — gold rows where one side has a numeric volume number stored where the xml_id should be (data-quality issue in the source xlsx, not a matcher miss). Real recall is ≈62/63 = **98.4%**.
- One genuine miss: `בליאכאַר שבתי` vs `בליאַכער, שבתי` — surname spelling drift below the trigram floor. Phonetic match would catch this; it's currently disabled for same-script comparisons.

### Subject→DB alignment (job 1)
- 3,081 alignment rows → `people_alignment_queue.tsv`.
- 2,393 (78%) have at least one DB candidate.
- 2,353 carry an existing RA decision from xlsx (treated as ALIGN).
- **recall@5: 425/458 = 92.8%** against the subset of RA decisions whose `db_id` is present in our extracted `people_db.tsv`.
- **Important caveat:** only 458 of 2,353 carried decisions had their gold `db_id` in our DB at all. The rest reference db_ids NOT in `people_db.tsv` — because we extracted the DB from xlsx review sheets, not from the canonical external source. **Action item:** get the actual DiJeSt/Zylbercweig-people DB file and replace `people_db.tsv` with it. Until then, alignment is hobbled.
- Cross-script fail-open is intentionally OFF in `prepare_alignment.py` — adds 300+ candidates per Hebrew person and dominates runtime for low marginal recall. A separate pass should be added later if needed.

### Drafter (jobs 1+2)
- Vocab: ALIGN / NEW / MERGE / PSEUDONYM / DISAMBIG / DEFER.
- Prompt includes top-5 DB candidates AND top-3 dedup candidates per row — so the LLM can pick MERGE-to-another-volume in the same call as ALIGN-to-DB.
- Few-shot examples come from the imported RA decisions: Duplication Check sheet → MERGE examples (with the *other* xml_id filled in), alignment sheet → ALIGN examples (only ones whose gold db_id is actually in our candidate list — see "carried db_ids not in people_db.tsv" issue above).
- 728 undecided rows in the queue (the rest have a carried-over RA decision).
- **Not yet run.** Verify the prompt before spending API credits: `python llm_draft_alignment.py --dry-run --limit 3`.
- Right now there are 0 ALIGN few-shot examples because none of the carried db_ids overlap our DB candidates. Fixing `people_db.tsv` (above) will surface them.

## Decisions for you to verify (or reverse)

1. **Decision vocabulary.** Org pipeline has 7 verbs; I went with 6 for people. **MERGE** vs **PSEUDONYM** as separate decisions is a judgment call — they may be redundant. Drop PSEUDONYM if the RAs prefer to handle aliases as a flavor of MERGE.

2. **"Alignment review" import.** I treated rows with a non-empty `DB-ID` field in the xlsx alignment sheet as ALIGN decisions. Many of those are pre-attribution-feature (no `reviewer` column), so we're inheriting whatever was on disk. Verify a sample before promoting them to canonical decisions.

3. **Blocking strategy.** Currently uses *all heading tokens* (no comma) or *surname-part tokens* (with comma) — plus bracket aliases. This over-blocks intentionally (98% gold recall) but generates many false-positive candidate pairs (14k). The drafter is the dedupe step.

4. **Cross-script fail-open.** Only fires for Hebrew-side persons with <3 candidates, and uses a length-window prefilter on the English DB rows. May miss persons whose English transliteration has very different length than the Hebrew. Calibrate against your gold set once we have queue output.

5. **142 review xml_ids missing from extracted.** The imported `people_alignment_review.tsv` references xml_ids not present in `people_extracted.tsv`. Likely stale ids from older volume re-extractions or vol-3 mention-context ids. Sample: `facs_464_tr_1755030126`. Not blocking anything; just flag.

6. **Vol 3 isn't in the rich schema.** It has subject-entry-level `heading` + `span` but lacks `names[]`, `birth_date`, etc. (it does have `entry` body + `people[]` mentions). The dedup + alignment passes still include vol-3 entries (using just heading + brackets), but they'll under-match anyone with a Vol-3 entry on one side. If we want full cross-volume dedup, Vol 3 needs the same enrichment the others got.

7. **Pseudonyms aren't caught by similarity alone.** `שלום-עליכם` ↔ `שלום ראַבינאָוויטש` scores 0.33 — won't show up as a candidate. We need a pseudonym dictionary (the `subheading` field + `names_variants` are where these live in the rich schema; an extraction pass over those would seed it). The drafter has a separate `PSEUDONYM` decision but won't have anything to draft against unless candidates surface them.

## Deferred (not started)

- **Job 3** (in-text mention → subject-entry). The data is there — `people_mentions_extracted.tsv` (4,393 rows from Vol 3) + the three `mention_validations_*.tsv` from the RA frequency-ranked sheets. But disambiguation requires *context features* the current similarity layer doesn't compute: dates derivable from the host entry, co-mentioned people, the host-entry subject as a relational anchor. This is its own design conversation.
- **Pseudonym dictionary.** Mine `subheading` + `names_variants` for `[X]` aliases across rich-schema volumes; build a forward + reverse map. Probably ~200 entries given how many headings have bracketed alt-names.
- **PI workflow doc.** Org pipeline has `PI_DECISIONS.md` — the people pipeline will need its own once decisions actually start landing.
- **Streamlit panel.** The Zalmen app needs a "People matching" view (analog of "Organizations matching"). Should be straightforward port of the org_review.py view, with MERGE as an extra decision mode that swaps the panel from "DB candidates" to "other-volume entries".
- **Drafter calibration / held-out test.** Org pipeline split decisions by `reviewed_at` for Tests 1 & 2 — the imported people review TSV has no `reviewed_at` column, so we can't replicate cleanly. Need to either ask Bella/Maaty for review timestamps, or just start logging them going forward and let a test holdout accumulate.

## How to re-run

```
cd /Users/sinairusinek/Documents/GitHub/Dybbuk
python3 Zylbercweig/people/flatten_volumes.py
python3 Zylbercweig/people/import_review_xlsx.py
python3 Zylbercweig/people/find_dedup_candidates.py
python3 Zylbercweig/people/prepare_alignment.py
# dry-run the drafter before spending money:
python3 Zylbercweig/people/llm_draft_alignment.py --dry-run --limit 3
```

Everything is idempotent and writes to fixed paths in `Zylbercweig/people/`.

---

## Phase C (2026-07-02) — person hub + draft review + surname disambiguation

### Entity model
- `people_common.py` — shared joins: volume-aware xml_id→person_id resolution
  (30 colliding xml_ids), person_id→db_id via review TSV (excludes the 19
  `alignment_disagreements.tsv` xml_ids pending PI), heading index.
- `derive_mention_alignments.py` → `derived_mention_alignments.tsv` (3,446 rows)
  — materialized mention→heading→db_id chain. **2,902 mention→db_id** (the
  ~2,950 memory estimate minus rows now correctly excluded: 64 disagreement
  rows + 15 ambiguous xml_ids). Surname-sheet rows = DOMINANT referent only,
  used as fame prior; full/initials rows = global lexicon.
- `build_person_hub.py` → `person_hub.tsv` (2,935 hubs) + `person_hub_members.tsv`
  + `person_hub_conflicts.tsv`. Union-find over CONFIRMED evidence only
  (ra_align 2,455 / ra_dup 63 / human_dedup / human_align). 140 multi-entry
  hubs, 2,313 DB-aligned, **0 multi-db conflicts**. Phase B drafts attach as
  `pending_drafts` counts, never as edges. Re-run after new B1/B2 decisions.

### Draft review (Zalmen B2 · Person → DB)
- `zalmen/views/person_alignment.py` — batch-confirm mode (data_editor table,
  325 ALIGN-high first) + card mode for DISAMBIG/DEFER/medium/low.
- Decisions → `people_alignment_decisions.tsv` keyed by person_id; consumed by
  build_person_hub.py as `human_align` evidence.

### Bare-surname mention resolution (job 3, first cut)
- `resolve_surname_mentions.py` → `mention_surname_resolutions.tsv` (6,441
  mentions) + `surname_groups.tsv` (1,221 groups). Resolution unit =
  (surname × host entry): within-entry expansion first, then hard gates
  (gender, birth-impossibility), then scoring (co-mention lexicon hits,
  career overlap, fame prior — prior disabled inside family clusters, i.e.
  ≥2 corpus-famous candidates). Verdicts (2026-07-03, after the forward-fill
  fix below): RESOLVED 4,103 (unique 3,223 / scored 798 / within_entry 82),
  AMBIGUOUS 1,940, UNKNOWN 398.
- **mentions_all.tsv forward-fill trap (found 2026-07-03):** only the FIRST
  row of each host-entry block carries host_xml_id/host_heading/
  host_entry_text (3,078 of 38,269 rows), and host_xml_id is volume-prefixed
  (`1-facs_…`). Read it ONLY via `people_common.load_mentions_with_host()`
  (validated forward-fill; adds host_person_id). The original resolver run
  silently scoped "within-entry" to whole volumes — 2,157 of its 2,239
  within_entry hits were contamination.
- Candidate universe = heading surnames (comma-aware; comma-less headings drop
  common given names from BOTH ends — orders are mixed in this corpus) PLUS
  RA-validated surname-sheet referents (catches given-name/mononym address
  forms like לייוויק → כאַנוקאָוו).
- Gold check vs the surname sheet (dominant-referent semantics, so an
  imperfect yardstick): **91.4%** of RESOLVED agree; scored 99.1%,
  unique_candidate 90.6%, within_entry 32.8% (within-entry deliberately
  overrides the corpus-dominant referent — the known hard case is
  famous-relative collision, e.g. גאָלדפֿאַדען-רעפּערטואָר resolving to the
  in-entry Naftali instead of Avrom; the set is small, B3 review covers it).
- `zalmen/views/surname_review.py` (B3) — review BY SURNAME: fixed candidate
  panel, sentence context, resolver suggestion pre-selected, one-click
  family-level abstain, chips stored for calibration. Decisions →
  `mention_resolution_decisions.tsv`.
- `zalmen/views/person_hub.py` — read-only person-centric hub browser.

### Entry context in the views (2026-07-03)
- `export_entry_context.py` → `entry_texts.tsv` (3,032 entries, ~29 MB;
  person_id → full entry text, ⏎ = newline) + `entry_mentions_slim.tsv`
  (38,269 mention rows with host_person_id, no text payload).
- B2: both sides open fully — entry card gets "📜 full entry text" +
  "💬 mentions in this entry" expanders; DB candidate cards get an
  "all DB fields" expander; batch-confirm mode gets an "🔍 Inspect a row"
  panel (full entry ↔ draft target side by side, MERGE targets included).
- B3: host entries open with the surname mentions <mark>-highlighted;
  candidate cards get an "📜 own entry" expander.

### Known limitations / next levers
- Within-entry expansion currently uses only OTHER MENTION ROWS of the same
  entry (82 hits). Scanning the entry TEXT for fuller candidate forms
  (entry_texts.tsv is now on disk) would recover the rest of the coref
  signal — next lever for the resolver.
- Surname candidate matching is exact-token; spelling drift (אזרא/עזרא) needs
  the same trigram/phonetic fallback as the dedup gate (bliakhar-class misses).
- `alternative_heading` column of the surnames sheet not yet used as candidates.
- 29 validated surfaces skipped as cross-hub ambiguous (duplicate headings in
  different hubs) — they double as dedup leads; see person_hub_conflicts.tsv.
- Mentions with empty host_xml_id (~NOXID rows) resolve without host features.
