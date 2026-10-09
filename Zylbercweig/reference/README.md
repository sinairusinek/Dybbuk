# Galron Union List of Digitized Jewish Historic Newspapers

`galron_union_list.tsv` — 2,424 rows, both scripts in one file.

## Source

The **Union List of Digitized Jewish Historic Newspapers, Periodicals and
e-Journals**, compiled and maintained by **Joseph (Yossi) Galron-Goldschläger**,
Head of the Hebraica & Jewish Studies Library, The Ohio State University.

- <https://go.osu.edu/jpress> (301 →)
- <https://library.osu.edu/projects/hebrew-lexicon/Jewish-Press.htm>

It lives inside Galron's *Online Bio-Bibliographic Lexicon of Modern Hebrew
Literature* project directory at OSU — the same lexicon mirrored at Ben-Yehuda
(<https://benyehuda.org/lexicon/>), which Galron founded in 2004 to continue
Getzel Kressel's *לקסיקון הספרות העברית בדורות האחרונים*. The newspaper index is
a sibling dataset of that lexicon, not hosted by Ben-Yehuda itself. A handful of
rows cite the lexicon as their own depository.

## Captured state

`library.osu.edu` returns **HTTP 403 to scripted requests** (browser UA included),
so the list cannot be refreshed by script. The HTML here was captured via the
Wayback Machine, using a `<timestamp>id_/` prefix to get the original file
without the Archive's injected wrapper:

| Input | Sheet | Capture |
|---|---|---|
| `jewish_press_hebraic.htm` | `sheet001.htm` — Hebrew/Yiddish/Ladino | **2025-09-09** |
| `jewish_press_latin.htm` | `sheet002.htm` — Latin + Cyrillic | **2026-08-25** |

The Hebraic half is therefore about a year behind the live list. To refresh,
save each sheet from a browser — note the tab labels are the reverse of the file
numbering, and `Jewish-Press.htm` is only a frameset holding no rows:

    .../Jewish-Press_files/sheet001.htm   (~650 KB)
    .../Jewish-Press_files/sheet002.htm   (~850 KB)

Then re-run `python3 parse_galron_union_list.py`.

## Columns

| Column | Notes |
|---|---|
| `row_id` | `galron-NNNN`, assigned on parse — **not stable across a re-parse** |
| `script` | `hebraic` or `latin` |
| `title` | as printed, frequently bilingual in one cell |
| `title_latin` / `title_hebrew` | `title` split by script, for blocking |
| `place_of_publication` | Hebrew sheet names places in Hebrew |
| `years_covered` | digitised range, not the run's full lifespan; Hebrew sheet sometimes uses Hebrew years (תשע״א–תשע״ט) |
| `language` | Hebrew sheet labels in Hebrew (`יידיש`, `עברית`, `לאדינו`) |
| `depository` | where the scans live; 318 distinct |
| `scan_url` | 2,421 of 2,424 rows |
| `gated` | `Y` where the source marked the depository ⛔ (subscription) |
| `source_sheet` | input filename |

**One row is one (title, depository, years) triple, not one title.** 2,013
distinct titles across 2,424 rows: a paper digitised in several places repeats.
Any match against the DB should dedupe on title first.

## Caveats for matching

- **Years are digitisation coverage.** A gap means nobody scanned it, not that
  the paper paused.
- **Place strings are not normalised** and differ by sheet — `תל־אביב (ישראל)`
  vs `Warsaw (Poland)`, some bare (`Prague, Brünn`). Route through the
  settlement resolver rather than string-matching.
- **Bilingual title cells** are split heuristically by script. 21 rows have
  both halves populated; parentheticals and bracketed glosses stay in `title`.
- **`row_id` is positional.** Don't persist it as a join key — match on
  title + depository, or mint stable ids first.

## Relevance

519 Yiddish-language rows, plus Hebrew, Ladino and Judeo-Arabic. core_db holds
86 `Journals/ Newspapers` orgs and 82 publishers, which this list can enrich
with digitisation locations and date ranges. See also
`DybbukMedia/code/seed_jpress.py`, which treats JPRESS as a manual search
worklist — the depository URLs here give those searches a direct target.

---

# Matching core_db and clusters to the union list

`match_press_to_galron.py` → `galron_press_match_review.tsv` (530 rows).
**A review ledger, not a write.** Nothing is applied to core_db; a wrong link
sends a researcher to the wrong newspaper's scans.

## Why cross-script matching is the primary path, not a fallback

core_db records most periodicals **only in Hebrew script** — of 182 live
press-type rows, most have no Latin `name` at all and `name_yiddish_translit`
is empty throughout. Galron's Latin sheet records 1,369 titles **only in Latin
letters**. There is no shared surface to compare, so the match has to be made
across scripts.

Spellings also disagree *within* each script: core_db has `פֿאַרווערטס`,
Galron `פֿאָרװערטס`; both mean Forverts.

## Two independent channels, neither trusted alone

Per the `entity-matching` skill's cascade (stage 3 fuzzy + stage 4 phonetic):

**1. Orthographic skeleton** (`press_match_key.skeleton`) — how a title is
*spelled*, with vowels dropped. Vowels are where orthographies and
romanizations disagree most, and where unpointed text carries least
information.

The existing `organizations/translit_yiddish_to_latin.py` cannot do this job:
it assumes **pointed** YIVO orthography and maps bare `א` to nothing, so on
unpointed historical titles it yields טאג→`tg`, פארווערטס→`frverts`,
אמעריקאנער→`merikner` — **11 of 14 test words mangled**, silently. It is kept
for its intended purpose (an RA readability scaffold) and not used for matching.

**2. Daitch-Mokotoff phonetics** (`press_match_key.phonetic_agreement`) — how a
title *sounds*, via `dybbuk-phonetic`'s Yiddish→IPA map then DM soundex from
`abydos`. This keeps exactly the vowel information the skeleton throws away.

Their failure modes do not overlap, which is the point of running both:

| Pair | Skeleton | Phonetic | Truth |
|---|---|---|---|
| אונזער וואָרט / Undzer Worṭ | 0.583 | 1.00 | same paper (DB omits the ד) |
| דער האַמער / המעורר | **1.000** | 0.00 | different papers |
| בבל / יובל | **1.000** | 0.00 | different (both → `bl`) |
| אויפקום / אופקים | 1.000 | 1.00 | different — still needs a human |

**244 of 530 rows exist only because of the phonetic pass** (skeleton < 0.60,
phonetic = 1.00). Running the orthographic channel alone would have missed
nearly half the candidates.

`epitran` is not installed here, so `dybbuk_phonetic`'s Latin→IPA path degrades
to a lowercase strip. DM is therefore fed Latin strings directly (it is built
for Latin-script surnames) and IPA conversion applied only to the Hebrew side.

## Bands — all three need human review

| Band | Meaning | Rows |
|---|---|---|
| `STRONG` | both channels agree outright | 134 |
| `REVIEW` | one channel perfect, the other corroborating | 295 |
| `WEAK` | surfaced but contradicted — mostly skeleton collisions | 101 |

Three guards keep known-bad pairs out of `STRONG`:
- **Short-skeleton floor** (< 4 consonants): בבל/יובל and רוס/אורות both
  collide on 2-consonant keys.
- **Perfect-spelling-but-silent veto**: skeleton 1.000 with phonetic 0.00 is
  the signature of a vowel collision, not a match.
- **Qualifier clash**: both sources disambiguate homonyms inline —
  `די אידישע וועלט (פֿילאדעלפֿיע)` vs `די אידישע וועלט <סט. פטרבורג>` are
  different papers with identical title keys. A named-place disagreement vetoes.

## Coverage

- **core_db**: 74 of 182 live press rows matched.
- **clusters**: 1,097 press-like clusters found in `organizations_clustered.tsv`
  (by press org_type *or* a press word in a name — the untyped ones are the
  reason this stage exists), of which 456 matched.
- 220 distinct Galron rows cited; 34 point at subscription-gated depositories.

Place is a **confirmation signal, never a filter** (except the qualifier clash
above): only 11 of 183 press rows have any address, and papers moved.

## Reviewing

Fill `decision` / `reviewer` / `note`. Suggested vocabulary: `CONFIRM`,
`REJECT`, `WRONG_PAPER` (homonym), `SAME_FAMILY` (related but distinct run).
`row_id`/`galron_row_id` are positional — re-running the parser renumbers them,
so complete a review round against one generated file.
