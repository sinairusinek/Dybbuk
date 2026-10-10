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

## Place and date cross-check

Each candidate is also tested against what the Zylbercweig entry itself says
about where and when — `place_agree` / `place_conflict` / `span_check` /
`db_dates`. The two behave differently on purpose.

**Place demotes.** Compared with `press_match_key.place_agree`, which bridges
orthographies: the consonant skeleton does NOT, failing all 9 probe pairs
(`וואַרשע`/`ווארשה`, `קראָקע`/`קרקוב`, `טשערנאָוויץ`/`צ׳רנוביץ`,
`קאָוונע`/`קובנה`…) because Yiddish marks a final schwa with `ע` where Hebrew
uses `ה`, and spells /v/ as `וו` where Hebrew uses `ב`. A first pass on raw
skeletons flagged **257 place conflicts, most of them the same city twice.**

The fix compares Daitch-Mokotoff codes on a **3-digit prefix**. DM is
zero-padded and the digit that differs across orthographies is the one for the
word's final sound — Warsaw is `7945xx` in Yiddish against `7940xx` in Hebrew,
Krakow `595000`/`595700` — while three digits still separate Vilna (786), Riga
(795) and Lodz (583). Now **12/12 same-city pairs agree and 0/11 different-city
pairs do**. A real conflict costs one band; it is not a veto, because papers
moved (Warsaw to New York, repeatedly, over these decades).

**Years are reported, never used to demote.** Neither side bounds the paper's
run: Galron's years are digitisation coverage, and a cluster's date is usually
the year of one mention. A first pass that demoted on a >10-year gap moved 125
rows to WEAK and was wrong on every one checked — *Birobidzhaner Shtern*
(running since 1930, listed 2010–2022 because that is what was scanned),
*Meksikaner Lebn*, *Vilner Togblat*, *Der Nayer Veg* are all the same paper on
both sides. So `span_check` reports `overlap` / `near` / `apartN` for the
reviewer's eye and the band ignores it.

Coverage of the check: 46% of matched clusters carry a settlement, 52% a date.
core_db has **no date field at all** and an address on only 11 of 183 press
rows, so the cross-check is mostly a cluster-stage signal.

## REVIEW sub-tiers (`tier` column)

295 rows is too many to meet in arbitrary order, and 268 of them share one
shape: phonetics vouched for the pair while the skeleton did not. That group
runs from obviously-right to obviously-wrong, so the band is sub-ranked by how
much of each title the agreement actually covers. A skeleton score of 0.18
means one token matched out of many — the rest of the name unaccounted for,
which is the signature of a coincidental token hit, not the same periodical.

The file is sorted STRONG → REVIEW (by tier) → WEAK, so it can be worked
top-down and abandoned at any point.

| Tier | Rows | What it is | Expect |
|---|---|---|---|
| `A-exact-skeleton` | 12 | spelling identical, phonetics hesitant | mostly true; fast to confirm |
| `B-both-agree` | 42 | both channels agree, titles comparable in size | mostly true |
| `C-phonetic-plus` | 142 | phonetics agrees, skeleton partly there | genuinely mixed — the bulk of the work |
| `D-partial-both` | 9 | both channels *half* agree | mostly true |
| `E-phonetic-only` | 84 | phonetics alone, titles very unequal in length | mostly noise; a long publisher name hitting a short title |
| `F-weak-signal` | 6 | neither channel convincing | mostly noise, but see below |

`D-partial-both` ranks above `E-phonetic-only` on purpose: two channels
half-agreeing beats one channel agreeing on a single token. DM also undercounts
across scripts — it compares a Yiddish IPA transcription against a raw Latin
string — so `טריבונאַ אַקאַדעמיצקאַ` / `Trybuna Akademicka`, a true match,
scores only 0.50 phonetically and would otherwise sink.

**The tiers rank, they do not decide.** `F-weak-signal` still contains at least
three true pairs that phonetics scored 0.00 (`איסט און וועסט`/`Ost und West`,
`טאָגמאָרגען זשורנאַל`/`דער טאָג מאָרגען זשורנאל`,
`לעמבערנער טאָגבלאַט`/`לעמבערגער טאגבלאט` — the last an OCR'd `נ` for `ג`).
Don't discard a tier wholesale.

## Reviewing

Work top-down. Fill `decision` / `reviewer` / `note`. Suggested vocabulary: `CONFIRM`,
`REJECT`, `WRONG_PAPER` (homonym), `SAME_FAMILY` (related but distinct run).
`row_id`/`galron_row_id` are positional — re-running the parser renumbers them,
so complete a review round against one generated file.

---

# Friedberg, *Toldot ha-dfus ha-ivri be-Polanyah*

Ch. B. Friedberg, **תולדות הדפוס העברי בפולניה** (*History of Hebrew Printing
in Poland*), 2nd ed., Tel Aviv 1950. 202 pp.

**Not kept in the repo** — it is an 8.4 MB scan and public domain. Sinai's copy
came from HebrewBooks as `Hebrewbooks_org_36967.pdf`
(<https://hebrewbooks.org/36967>); the Internet Archive also carries it. The
PDF ships a usable OCR text layer, so `pdftotext` is enough — no re-OCR needed.

## Why it matters for the org pipeline

It is a **firm-by-firm, family-by-family** register of the Warsaw Hebrew
printing houses, which is exactly the evidence `core_db` lacks when a cluster
names a printer by a fuller firm name than the DB row does. Two of the
publisher audit's three "needs a human" printer flags were settled from it in
one pass (2026-10-10):

- **Baumritter** (db 157). p.115: Efraim b. Binyamin Baumritter founded his
  press in תרכ"ד/1864 with Yitshak Rotheblat; in תרנ"א/1891 he **took in his
  son-in-law Naftali Ganshar**, and in תרנ"ה/1895 they separated. So the
  cluster `בוימריטטער וחתנו גאנשאר` is the same press during its 1891–95
  partnership, not a second firm.
- **Lebenzohn** (db 24). pp.111–112: a four-generation dynasty — Avigdor
  (press founded תקע"ח/1818) → his son **Yoel** (from תרי"ב/1852, when Avigdor
  left for Eretz Yisrael) → Yoel's son **Yosef** (to ~תר"ס/1900). Yoel's sons
  Gershon and Pesach opened a further press in תרט"ז/1856. So db 24's *Yoel*
  and the cluster's *Yosef* are father and son, successive proprietors of one
  house.
- **Goldfarb** (db 50) is **absent** — 0 hits on every spelling. Expected:
  Friedberg covers *Hebrew* printing, so a Yiddish-only publisher never
  appears. A negative result here is not evidence of anything.

It also yields relations the Leksikon states only obliquely: **הצפירה** was
launched by Chaim Zelig Slonimski in תרכ"ב/1862 *at the Lebenzohn press*
(p.112), linking db 68 to db 24's family as newspaper-to-printer.

## Searching the text

Two gotchas, both of which make a naive `grep` return nothing:

1. The OCR preserves the book's **letter-spaced emphasis** — a firm's name is
   set as `ב ו י מ ר י ט ט ע ר`, so the string `בוימריטטער` does not occur.
   Strip all spaces into a flattened copy and search that, then map back.
2. The text carries **bidi control characters** and uses `׳`/`״` (geresh,
   gershayim) where a typed query uses `'`/`"`.

        pdftotext book.pdf out.txt        # layout mode not needed
        # strip U+200E/200F/202A-202E/2066-2069/061C, strip points,
        # normalise ׳״ -> '" , then search a space-collapsed copy

The index (pp. 176–182) is the fastest entry point: it lists each printer by
surname with given names beneath, so it answers "are these two the same
family?" directly. Printed page numbers run ~5 behind the PDF page numbers.

---

# Yiddish Book Center — Digital Yiddish Library

`ybc_publisher_lookup.py` — publisher lookups against the YBC's digitized
holdings, **13,661 volumes**.

## Why this one works when the others don't

Every catalogue this project actually wants refuses scripted access:
`yiddishbookcenter.org` itself returns **403**, and so do NLI, WorldCat and the
OSU lexicon host. But the Digital Yiddish Library is **hosted on the Internet
Archive** as collection `nationalyiddishbookcenter`, and IA's
`advancedsearch.php` is open and unauthenticated. Query that instead of the
YBC front end:

    https://archive.org/advancedsearch.php?q=collection:"nationalyiddishbookcenter"+AND+publisher:(goldfarb)&output=json

## What it settles, and what it can't

It gives, for a surname, **every imprint with its city and year**. That is
exactly the evidence needed to tell a spelling variant from a second firm:

| Pattern in the results | Reading |
|---|---|
| one initial, one city, continuous years | one firm |
| two initials, same city, overlapping years | two firms, often one family |
| one name, two cities, clean break | the firm moved |

It is **not** an authority file. `publisher` is a transcribed imprint line, so
one firm shows up as `Sh. Goldfarb`, `S. Goldfarb` and `Shelomoh Goldfarb`, and
one city as `Ṿarshe` / `Ṿarsha` / `Ṿarshoy` / `Warszawa`. The script clusters
both; don't read a raw facet count.

It is also **not complete** — one library's holdings, weighted to literary
Yiddish and to what reached America. Alapin, Lidski, Jatshkovski, Morgenstern
and Katsenelbogn all return **0**, and every one of them is a real Warsaw
house. *An absence here is not evidence.*

## Two traps

1. **Search romanized, not Hebrew.** Hebrew-script publisher strings are on
   only a fraction of records; `publisher:"קולטור"` finds 3 items where
   `publisher:kultur` finds 20. Pass several spellings — `bzhoza OR bzshoza`
   finds 135, `bzhoza` alone finds few.
2. **The romanization arrives DECOMPOSED.** `Ṿ` is `V` + U+0307, so a pattern
   written with a precomposed `Ṿ` silently never matches and one city
   fragments into a row per transcription. Strip combining marks before
   comparing — `strip_marks()` in the script does this.

## What it has settled so far (2026-10-10)

- **Goldfarb** (db 50), which Friedberg could not answer because his scope is
  Hebrew printing: **Sh. = S. = Shelomoh**, one Warsaw firm, 57 items
  1900–1933. **M. Goldfarb is separate**, 10 items 1924–33, running
  concurrently — a different firm, probably a relative. No `L.` Goldfarb
  exists, so the cluster's `ל. גאָלדפאַרב` is likely an OCR slip for `מ.`
- **Bletnitski**: a cautionary case. The catalogue says `Ṿilna : ha-Aḥim
  Bleṭnitsḳi`, the corpus says **Odessa**, and the catalogue is **wrong** — see
  *Read the title page, not the publisher field* below.
- **Kultur**: no pre-1918 "Kultur" imprint in the collection at all; every hit
  is Kultur-Lige (from 1922) or a later namesake. Consistent with treating the
  Minsk and Warsaw imprints as unrelated, but not itself evidence.

## Read the title page, not the publisher field

The single most important lesson from this resource so far, learned by getting
it wrong first.

A Hebrew/Yiddish title page routinely carries the **publisher on one line and
the place of printing on another**. IA's `publisher` field collapses them into
one string, in the order they appear — which for these books puts the *printing
city* first and reads as though it were the firm's seat:

| Item | What the page says | What `publisher` says |
|---|---|---|
| nybc204446 (1897) | `בהוצאת האחים בלאמניצקי באדעססא` … then `ווילנא` | `Ṿilna : ha-Aḥim Bleṭnitsḳi` |
| nybc202364 (1909) | `בהוצאת האחים בלעטניצקי. באדעססא` … then `ווילנא. תרסט` | `Ṿilna : ha-Aḥim Bleṭnitsḳi` |
| nybc214795 (1895) | `הזכות נמכר לצמיתות להאחים בלעטניצקי מויס באדעססא` … then `ווילנא` | `Ṿilna : Bleṭnitsḳi` |

`באדעססא` names the **firm's** city; `ווילנא` names where it was **printed**.
The metadata states the reverse. On the strength of that field alone this
project briefly recorded the Bletnitski Brothers as a Vilna house that moved to
Odessa — when they were an Odessa bookshop all along, commissioning printing in
whatever city was cheapest or had the censor. The third item makes the
relationship explicit: *the rights were sold in perpetuity to the Bletnitski
Brothers **of Odessa***, on a book printed in Vilna.

So whenever a catalogue city contradicts the corpus, **read the page**:

    curl -L https://archive.org/download/<identifier>/<identifier>_djvu.txt

then grep the first ~200 lines for `הוצאת`, `דפוס`, and the city names. The OCR
of a title page is short and legible even when the body text is poor. A
publisher field is a cataloguer's reading of that page; the page is the source.

This also means a **city disagreement between catalogue and corpus is not
evidence of two firms** — check which line each one is quoting before splitting
anything.
