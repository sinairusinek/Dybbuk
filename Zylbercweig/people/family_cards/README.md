# Family cards — disambiguating a surname family in one sitting

Input of the Zalmen view **People · family cards** (`zalmen/views/family_cards.py`).
Built by `build_family_cards.py` from `<family>/pilot/`. Proposals are suggestions;
only a reviewer's save writes to `people/mention_resolution_decisions.tsv` (the B3 ledger).

## Why families
Open person work (2026-11-15): 3,302 open tokens (eponym 225, B3 1,509, KG 1,568)
over 1,528 surname types; the top 50 families hold ~30%. Family members share
troupes, cities and stages, so a profile of *every* member is what separates them.

## Method (Adler pilot, 2026-11-15)
1. **Profiles** — each member's Leksikon entry read by an LLM into
   `pilot/profiles/<hub>.json`: life dates, timeline (years, place, troupe, role),
   relatives, collaborators, each fact with a verbatim quote (289/299 quotes verified).
2. **Cases** — each open B3 mention with ±650 chars of its host entry; the target
   occurrence wrapped in ⟦ ⟧. **Marking the target is essential**: 35% of contexts
   contain several Adlers; without the marker, wife/daughter mentions were read as
   Jacob P. (first, discarded round).
3. **Two independent blind readings** (different batching/order/wording, neutral ids,
   test and real cases mixed): `pilot/judge2_out_*` (reading 1), `pilot/judge3_out_*`
   (reading 2). Answers: a hub, FAMILY, NOT_IN_FAMILY, UNSURE + confidence.

## Measured precision
Test set: 97 full-name Adler mentions from *other people's* entries, given name hidden,
answer = the validated surface's hub (`pilot/cases_test_sample_v2.json`; ≤12 per member).

| rule | answered /97 | precision |
|---|---|---|
| one reading alone | 59–61 | 0.80–0.82 |
| both readings agree | 46 | 0.89 |
| **both agree, neither low-confidence** (= `agree=1`, pre-selected) | **41** | **0.93** |

Errors are genuine near-misses (two Adlers in one city in one year; sister for sister).
Some test answers may themselves be wrong (validated surfaces can sit on the wrong hub —
e.g. T034 "Adler with Sierotzki in Lodz" is Julius by his entry; the surface says Yisrael).
Human B3 picks: 2 of 3 agree (B023: human Julius, both readings Jacob P.).

**Watch:** 51 of 60 agreed open proposals are Jacob P. — over-attribution to the most
famous member is the main failure to check for.

## Adding a family
Put its pilot outputs in `<family>/pilot/` with the same layout and re-run the builder.
Re-measure on that family's own masked test set before trusting `agree=1`.
