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
