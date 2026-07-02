# Phase A spec — gated regeneration of people dedup/alignment candidates

Executor: any model. Everything here is operational; where judgment is needed the
rule is stated explicitly. **Acceptance criteria at the bottom are binding — do not
declare done without running them and reporting the numbers.**

Context: the current "Pseudonym candidates (208)" batch measured **0/111 precision**
in human review (see `person_dedup_decisions.tsv` — all "different"). Root causes:
bare given-name aliases used as match keys, no gender gate, no DB-contradiction
gate, hard-coded composite=0.7 defeating ranking. This spec fixes candidate
generation; it does NOT touch the Zalmen view or the LLM drafter.

## Environment / repo conventions (read first)

- Run with **python3.11** (not the .venv's 3.9): `python3.11 Zylbercweig/people/<script>.py`
- The Dybbuk index often carries unrelated pre-staged work. Commit ONLY with
  explicit paths: `git commit -- Zylbercweig/people/`
- Back up every TSV you regenerate: `cp X.tsv X.tsv.pre_phaseA` before overwrite.
- **Never write to `person_dedup_decisions.tsv`** — that file is human ground
  truth, append-only by the Zalmen app.

## Inputs (all under `Zylbercweig/people/`)

| File | Role |
|---|---|
| `people_extracted.tsv` | 3,081 subject entries (heading, names_variants, gender, dates, places, volume) |
| `people_aliases.tsv` | 3,842 mined aliases (person_id → alias forms) |
| `people_alignment_review.tsv` | RA decisions; gives xml_id → db_id map (~2,509 distinct xml_ids) |
| `people_db.tsv` | 3,476 DB target rows |
| `person_dedup_decisions.tsv` | 111 human decisions (all "different") — READ-ONLY; use as regression set |
| Gold: the `Duplication Check` pairs already materialized in `people_dedup_gold_eval.tsv` | 67 rows, 63 real (4 phantom with malformed xml_id) |

Scripts to modify: `find_dedup_candidates.py`, `mine_pseudonyms.py` (alias
filtering only), `prepare_alignment.py` (gates 2–4 apply there too where both
sides have the field).

## Changes

### 1. Alias hygiene (blocking + scoring)

An alias form that is a **single token equal to a common given name** must not be
used as a blocking key nor as a standalone variant for scoring. Operational rule:
build `given_name_counts` = frequency of each token appearing in the given-part of
headings (the part AFTER the comma when a comma exists). Any single-token alias
whose token has `given_name_counts >= 3` is dropped from blocking keys and from
the variant set. Multi-token aliases are kept whole. Log dropped aliases to
`aliases_suppressed_phaseA.tsv` (person_id, alias, reason, count).

### 2. Gender gate (hard drop)

If both sides have non-empty gender and they differ → do not emit the pair.
No exceptions (same person = same gender; maiden-name cases are same-gender).
Count drops.

### 3. DB-contradiction auto-close

If both sides have a db_id (via `people_alignment_review.tsv` xml_id→db_id) and
the db_ids **differ** → do not emit; instead append to a NEW file
`dedup_auto_closed.tsv` with columns
`a_xml_id, b_xml_id, verdict=different, reason=db_contradiction, a_db_id, b_db_id, a_heading, b_heading`.
(Same db_id on both sides → keep the pair and tag `db_agreement=1`; those are the
easiest confirms.)

### 4. Date-contradiction hard drop

Currently a >3yr disagreement scores date=0.0 but only weighs 20%. Change: if
BOTH sides have a birth year and they differ by >3, OR both have a death year
differing by >3 → drop the pair entirely. Count drops.

### 5. Surname gate in scoring

`person_pair_score` must require overlap on the **surname part** (token(s) before
the comma when present; for comma-less headings use the FIRST token, which in this
corpus is the surname) or on a multi-token/surname-bearing alias — before any
token/trigram score counts. A pair whose only shared token is a given-part token
scores 0. Mirror the comma-aware gate in Shidduch `person_name_similarity`
(`/Users/sinairusinek/Documents/GitHub/Shidduch/shidduch/core/normalizers.py`) —
copy the logic, do not import Shidduch.

### 6. Real ranking + review bands

- Recompute real composite scores for ALL emitted pairs, including alias-derived
  ones (no fixed 0.7 anywhere).
- Add a `band` column: `high` (composite ≥ 0.85), `review` (0.65–0.85),
  `low` (0.55–0.65). Emit all three but the Zalmen batches will consume high+review
  first.
- Regenerate `pseudonym_candidates_review.tsv` from the gated alias pathway with
  real scores, or fold alias-derived pairs into the main file with a
  `source=alias` column — your choice, but the fixed-0.7 file must not survive.

## Acceptance criteria (run and REPORT all numbers)

1. **Gold recall:** ≥ 62/63 real gold pairs still emitted (any loss: name the pair
   and which gate killed it). Use the same eval as `find_dedup_candidates.py`
   currently produces in `people_dedup_gold_eval.tsv`.
2. **Regression vs human decisions:** of the 111 human "different" pairs, report
   how many the new pipeline still emits (target: large reduction; every survivor
   must now carry a real score and band, not 0.7).
3. **Queue size:** report pair counts before → after, per band, and per-gate drop
   counts (gender / db_contradiction / date / surname-gate / alias-suppression).
4. **Idempotence:** run the generator twice; outputs must be byte-identical.
5. Spot-print the top 15 `review`-band pairs for human sanity check in the final
   report.

Deliverables: modified scripts, regenerated TSVs + `.pre_phaseA` backups,
`dedup_auto_closed.tsv`, `aliases_suppressed_phaseA.tsv`, and a summary of the
acceptance numbers. Commit with message
`people: Phase A gated candidate regeneration` scoped to `Zylbercweig/people/`.

## Out of scope for the executor

- `llm_draft_alignment.py` execution (API spend — human decision)
- Zalmen view changes
- Vol-3 re-extraction, person-hub model, mention matcher (Phase C)
