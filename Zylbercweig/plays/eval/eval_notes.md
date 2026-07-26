# Evaluation notes — Lateiner/Hurwitz KG pilot (2026-07-25)

Companion to the generated `eval_report.md` / `eval_findings.tsv`. Written after the
first full pipeline run; the numbered improvements below are the concrete changes to
make before scaling to all playwrights.

## Headline finding — RESOLVED 2026-07-26 (root cause found)

**Update:** the scramble described below was traced to an export bug, not to
history. `created_expressions` in people_db came from the DiJeSt
person-report (`ZylbercweigPeople/ZylbereportPeople.tsv`), whose generator
pooled BOTH playwrights' works into one alphabetical list and split it
between the two adjacent person rows (Lateiner got 400/א..דניאל, Hurwitz
דעם..ש). Authorship was rebuilt from the works table (`worksReport`) via
`fix_authorship_from_worksreport.py` (104 corrected / 118 confirmed), and the
former curators' `certainty` judgments were applied via
`apply_sheet_certainty.py` (9 adopted / 18 ascriptions rejected / 24 unvetted
→ Ruthie, see REVIEW_TASKS.md). The 64-row swap punchlist below is
**superseded**. Standing caveat: never reuse person-report
"Created expression(s)" exports.

## Original finding (superseded, kept for the record): the author split of the play lists is scrambled

121 of 271 registry titles that match a catalogue works row sit under the **opposite
author** (77 lexicon-Hurwitz → catalogue-Lateiner, 46 lexicon-Lateiner →
catalogue-Hurwitz), with **character-identical Yiddish title strings** on both sides —
so the two datasets share a common list ancestor and one (or both) author assignments
got scrambled somewhere upstream. Independent signals point the same way:

- The YiDraCor **print editions** (all catalogued as Lateiner) title-match plays in
  *Hurwitz's* `created_expressions` list (עזרא, יידעלע, קידוש השם, ציון/על נהרות בבל,
  דער מאן אונטערן טיש) — these `published_as` edges carry `match_status=candidate`.
- Gold annotation caught text-vs-registry flips in **both directions** inside the
  lexicon itself: an entry calls „שלום בית" *Lateiner's* while the registry lists it
  under Hurwitz (PL-0268); another entry calls „דאָס פּוילישע יונגל" *Hurwitz's* while
  the registry lists it under Lateiner (PL-0063).
- The catalogue disagrees with itself on קידוש השם: works sheet = Hurwitz, print
  edition = Lateiner. (Historically both playwrights had same-titled plays, so some
  "conflicts" may be two real works — the disputed-node design in plays_db was built
  for exactly this.)

**Action:** the 121 plays are flagged `attribution_status=catalogue_conflict` in
`plays_db.tsv`. Before scale-up, check the provenance of `created_expressions` in
`people_db.tsv` (was it seeded from the DybbukCatalogue works sheets? did columns
shift?) — a bulk upstream fix beats 121 manual adjudications. PI decision needed on
same-title-both-authors cases.

## Extraction quality (measured)

| metric | value | note |
|---|---|---|
| evidence verbatim (sweep, 2,085 facts) | 93.6% yes + 1.8% partial | partial = ellipsis-stitched quotes, all fragments verified |
| evidence verbatim (flagship, 219 facts) | 81.3% + partial | flagship windows are denser; stitching more frequent |
| schema-valid fact_type / role | 100% | |
| gold recall (production facts, 23 complete entries) | 13/26 | see breakdown below |
| gold homonym false-positive rate | 11/24 | other-author plays extracted as L/H facts |
| API errors over 713 calls | 0 | ~$3-4 total Gemini cost |

## Error taxonomy and process improvements

1. **Homonym false positives (11/24)** — famous same-titled plays by other authors
   (Anski, Pinski, Zolatarevsky, Sharkansky, Gutzkow/Habima, Asch's קידוש השם) still
   get extracted as L/H facts despite the homonym_risk flag.
   *Improve:* add the 29 gold `excluded_homonym` rows as few-shot negatives; grow a
   curated known-homonym list (title → true author) consulted at linking time; require
   author co-mention for any homonym_risk title before `fact_type != mention_only`.
2. **Character-name-as-title** — quoted character names matching play titles (e.g. the
   role „בת שבע" in Sholem Aleichem's גאָלדגרעבער) produce spurious production events.
   *Improve:* prompt rule + sweep guard: quoted span immediately preceded/followed by
   ראָל / אין …ס <other title> is a character, not a title.
3. **Date artifacts** — 43 out-of-range dates (one entry contributes an 1854-10-09
   block; Yiddish theater begins ~1876). *Improve:* build_kg should demote
   out-of-range dates to `confidence=low` + review rather than carrying them silently.
4. **Recall gaps (13/26)** — main causes seen in gold: spelled-out numerals
   (פיר הונדערט יאָר ≠ 400 יאָר), unnamed-play adaptations ("a Lateiner play"),
   facts outside any title-hit window. *Improve:* numeral folding in
   `norm_yiddish`; harvest alt spellings from gold/flagship into plays_db
   `alt_titles`; consider a second sweep pass keyed on author-surname sentences
   (לאַטיינערס פּיעסע …) rather than titles only.
5. **Venue cross-script comparison** — 79 venue checks were skipped (Yiddish KG venue
   vs roman catalogue venue). *Improve:* small venue-name transliteration table
   (Thalia, Windsor, People's, Grand, Poole's… ≈ 20 names covers most events).

## Caveats

- The works-sheet attribution "corroborations" (152) are **not independent evidence** —
  the works sheets appear to descend from the same lexicon lists.
- Gold was annotated by Claude agents (a different model/path than the Gemini
  extractor, but not human). Treat recall/FP numbers as indicative; Sinai's spot-check
  of `eval_findings.tsv` rows is the ground truth step.
- `kg_link_review.tsv` (971 surfaces) awaits adjudication; the KG already carries
  candidate/unmatched flags, so this improves precision but blocks nothing.

## Scale-up readiness

The pipeline is parameterized (`--author-db-ids`); marginal cost per additional
playwright is Gemini-only (~$0.5-1 per 100 windows). Fix items 1-4 first, rerun the
two-author pilot, re-measure against this baseline, then widen.
