# YiDraCor — OCR-correction queue for Judith, 2026-07-02

Consolidated + re-verified. Supersedes `handoff_judith_2026-06-14_ocr.md`.

**How this was built:** every candidate from the 2026-06-14 queue PLUS the new
OCR errors surfaced in Noa's 2026-06-28 review were checked against the live
Transkribus state (page_annotated/ refreshed 2026-07-02). Rows already fixed on
the server were dropped. **38 still-open confirmed fixes** below.

Each row: the wrong `<label>:` still appears on that page; change the surface text
to the Correction. The cast_dict already maps these to the right role, so no
role/tagging change is needed — just the transcript text.

## 1. Confirmed OCR errors — still open (38)

| Play | Page | OCR'd surface | Correction | Role xmlid | Flagged |
|---|---|---|---|---|---|
| Bas Sheva | 32 | `בוימין` | `בנימין` | `bnimin` | 2026-06-28 |
| Bas Sheva | 47 | `בנילין` | `בנימין` | `bnimin` | 2026-06-28 |
| Bas Sheva | 57 | `שלומאיל` | `שלומיאל` | `shlumil` | 2026-06-28 |
| Blimele | 23 | `מאלסים` | `מאקסים` | `maksim` | 2026-06-28 |
| Das Yudishe Kind | 3 | `פעטיפי` | `גראף פעטעפי` | `graf_petefi` | 2026-06-14 |
| Das Yudishe Kind | 4 | `פעטיפי` | `גראף פעטעפי` | `graf_petefi` | 2026-06-14 |
| Das Yudishe Kind | 12 | `פעטיפי` | `גראף פעטעפי` | `graf_petefi` | 2026-06-14 |
| Di Seder Nakht | 9 | `ולמן` | `זלמן` | `zelmen_kahn` | 2026-06-28 |
| Di Seder Nakht | 14 | `דימיטריע` | `דומיטריע ריזוואן` | `dumitriye_rizvan` | 2026-06-14 |
| Di Seder Nakht | 14 | `דעמיטריע` | `דומיטריע ריזוואן` | `dumitriye_rizvan` | 2026-06-14 |
| Di Seder Nakht | 29 | `טודרוס` | `טודריס ביק` | `tudris_bik` | 2026-06-14 |
| Di Seder Nakht | 68 | `ראשיל` | `ראשעל` | `rashel` | 2026-06-14 |
| Dovid's Fidele | 10 | `נא` | `נח` | `nkh` | 2026-06-28 |
| Dovid's Fidele | 37 | `ענקעל` | `יענקעלע` | `ienkele` | 2026-06-28 |
| Dovid's Fidele | 38 | `יעקעל` | `יענקעלע` | `ienkele` | 2026-06-28 |
| Dovid's Fidele | 60 | `חיה` | `חוה` | `khuh` | 2026-06-28 |
| Hinke Pinke | 34 | `אינקע` | `חינקע` | `khinke` | 2026-06-28 |
| Hinke Pinke | 48 | `אינקע` | `חינקע` | `khinke` | 2026-06-28 |
| Hinke Pinke | 48 | `הינקע` | `חינקע` | `khinke` | 2026-06-28 |
| Isha Raa | 10 | `מלכה` | `מילכה` | `milkah` | 2026-06-28 |
| Isha Raa | 11 | `אנטינגוס` | `אנטיגנוס` | `antignos` | 2026-06-28 |
| Isha Raa | 22 | `שלומיר` | `שלומית` | `shlomit` | 2026-06-28 |
| Isha Raa | 38 | `חנוד` | `חנוך` | `khanokh` | 2026-06-28 |
| Isha Raa | 40 | `אינטיגוס` | `אנטיגנוס` | `antignos` | 2026-06-28 |
| Isha Raa | 48 | `פרי` | `פרץ` | `perets` | 2026-06-28 |
| Isha Raa | 53 | `אבנד` | `אבנר` | `avner` | 2026-06-28 |
| Isha Raa | 57 | `שלומי` | `שלומית` | `shlomit` | 2026-06-28 |
| Mishke Mashke | 10 | `סאלע` | `סאלא` | `sala` | 2026-06-14 |
| Mishke Mashke | 10 | `עמיליא` | `עמיליע` | `emilye` | 2026-06-14 |
| Mishke Mashke | 13 | `עמיליא` | `עמיליע` | `emilye` | 2026-06-14 |
| Mishke Mashke | 15 | `שארלאטע` | `שארלאטא` | `sharlata` | 2026-06-14 |
| Mishke Mashke | 20 | `ביסיגג` | `ביסינג` | `bising` | 2026-06-14 |
| Mishke Mashke | 21 | `מאנקאדזשאר` | `מאנקאדזשאו` | `monkadshu` | 2026-06-14 |
| Yudale der Blinder | 9 | `פריידע` | `פריידא'לע` | `freydale` | 2026-06-14 |
| Yudale der Blinder | 10 | `פישעל` | `פישל` | `fishl` | 2026-06-14 |
| Yudale der Blinder | 49 | `פריידע` | `פריידא'לע` | `freydale` | 2026-06-14 |
| Yudale der Blinder | 50 | `פריידע` | `פריידא'לע` | `freydale` | 2026-06-14 |
| Yudale der Blinder | 64 | `פישעל` | `פישל` | `fishl` | 2026-06-14 |

## 2. Possible OCR — please judge (4)

Noa marked these as spelling variants, but they look like letter-level OCR
mutations. Fix the text only if the print is genuinely wrong; otherwise leave and
tell us it's a real variant.

| Play | Page | OCR'd surface | Correction | Role xmlid | Flagged |
|---|---|---|---|---|---|
| Di Seder Nakht | 32 | `דו` | `דוד` | `dovid_kahn` | 2026-06-28 |
| Dovid's Fidele | 60 | `הוה` | `חוה` | `khuh` | 2026-06-28 |
| Hinke Pinke | 54 | `נבריאל` | `גבריאל` | `gbril` | 2026-06-28 |
| Kidush Hashem | 12 | `פיקאלע` | `פיקאלא` | `pikola` | 2026-06-28 |

## 3. NOT OCR — cast-mapping only, do NOT change the text (5)

Listed for transparency. The printed surface differs from the character's name,
but it is not a letter-level OCR error — changing it would falsify the source.
These are handled in `cast_dict`/`speaker_overrides`, not by you.

| Play | Page | OCR'd surface | Correction | Role xmlid | Flagged |
|---|---|---|---|---|---|
| Bas Sheva | 26 | `זלמן` | `למך` | `lmkh` | 2026-06-28 |
| Bas Sheva | 62 | `זלמן` | `למך` | `lmkh` | 2026-06-28 |
| Bas Sheva | 66 | `זלמן` | `למך` | `lmkh` | 2026-06-28 |
| Isha Raa | 16 | `דוד` | `חנוך` | `khanokh` | 2026-06-28 |
| Isha Raa | 47 | `ה` | `מילכה` | `milkah` | 2026-06-28 |

---

**Since 2026-06-14:** 21 candidates from the prior queue were verified
as already fixed on Transkribus and dropped (incl. all of Ezra + Blimele §1).

**Cross-play OCR patterns** (from the 2026-06-14 analysis, still useful):
- Word-internal space split (`וו לאד`→`וולאד`, `מארט הא`→`מארטהא`, `בּיסיגג`→`ביסינג`)
- Final-vowel ע/א confusion (`סאלע`/`סאלא`, `שארלאטע`/`שארלאטא`, `עמיליא`/`עמיליע`)
- ה/ח confusion (`ירוהם`→`ירוחם`, `הינקע`→`חינקע`); ד/ר (`אבנד`→`אבנר`); ר/ת (`שלומיר`→`שלומית`)
- Letter doubling/dropping (`מאקים`→`מאקסים`, `גראס`/`גראפ`→`גראף`, `מלכה`→`מילכה`)
- Transposition (`אנטינגוס`/`אינטיגוס`→`אנטיגנוס`)
