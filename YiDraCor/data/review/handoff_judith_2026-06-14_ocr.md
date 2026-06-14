# YiDraCor — OCR-correction punchlist for Judith, 2026-06-14

Scope: speaker-label OCR errors flagged in cast_dict variant tables across all plays, plus a reference list of OCR-style text corrections already made by RAs (Noa / Maati) on the 2026-06-14 layer. Built by comparing `layer_diff_2026-06-14/<NNNN>_{pipeline,final}.xml` and scanning each play's `cast_dict.json` `prefix_variants` against the latest transcript pull. Focus is concrete Transkribus-actionable fixes: page + speaker surface form → correct form.

## 1. Confirmed speaker-label OCR errors (high priority — cast_dict already maps to the right role, surface text is wrong)

| Play | Page | OCR'd surface | Correction | Role xmlid |
|---|---|---|---|---|
| Ezra-Emkroyt1908 | 17 | `גיזיזמונד:` | `זיגיזמונד` | `zigizmund` |
| Ezra-Emkroyt1908 | 17 | `(זיי גיזיזמונד אב — קומט מיט דעבארא, זיגיזמונד אב) [stage]` | `זיגיזמונד` | `zigizmund` |
| Ezra-Emkroyt1908 | 27 | `סטאניסלאוי:` | `סטאניסלאוו` | `stanislav` |
| Ezra-Emkroyt1908 | 33 | `געזרא:` | `עזרא` | `ezra` |
| Ezra-Emkroyt1908 | 33 | `ערא:` | `עזרא` | `ezra` |
| Ezra-Emkroyt1908 | 35 | `לעבארא:` | `דעבארא` | `debora` |
| Ezra-Emkroyt1908 | 36 | `אוויצקי:` | `סאוויצקי` | `savitski` |
| Blimele-AhronFaust1903 | 21 | `מאקים:` | `מאקסים` | `maksim` |
| Blimele-AhronFaust1903 | 16 | `דיעפע:` | `ליעפע` | `liepe` |
| Blimele-AhronFaust1903 | 8 | `געליקל:` | `זעליקל` | `zelikl` |
| Blimele-AhronFaust1903 | 9 | `עליק:` | `זעליקל` | `zelikl` |
| Blimele-AhronFaust1903 | 22 | `גראס:` | `גראף` | `graf` |
| Blimele-AhronFaust1903 | 41 | `גראס:` | `גראף` | `graf` |
| Blimele-AhronFaust1903 | 41 | `גראפ:` | `גראף` | `graf` |

## 2. Other suspected OCR errors flagged by cast_dict-variant scan

Method: for each role, every `prefix_variants` entry that (a) is not the canonical bare of another role, (b) is not a prefix/substring of its canonical bare (legitimate shortenings excluded), and (c) shares ≥50% of its letter set with the canonical bare (so role-aliases like שמש→ר׳ געציל are excluded). Matched as standalone speaker labels (followed by `:`) on the latest Transkribus pull.

### Di_seyder_nakht_Emkroyt_1908 (5)

| Page | OCR'd surface | Correction | Role xmlid |
|---|---|---|---|
| 14 | `דימיטריע:` | `דומיטריע ריזוואן` | `dumitriye_rizvan` |
| 14 | `דעמיטריע:` | `דומיטריע ריזוואן` | `dumitriye_rizvan` |
| 29 | `טודרוס:` | `טודריס ביק` | `tudris_bik` |
| 52 | `העלפנאט:` | `העלפגאט` | `helpgot` |
| 68 | `ראשיל:` | `ראשעל` | `rashel` |

### MishkeMashke-Kultur1910 (6)

| Page | OCR'd surface | Correction | Role xmlid |
|---|---|---|---|
| 10 | `סאלע:` | `סאלא` | `sala` |
| 10 | `עמיליא:` | `עמיליע` | `emilye` |
| 13 | `עמיליא:` | `עמיליע` | `emilye` |
| 15 | `שארלאטע:` | `שארלאטא` | `sharlata` |
| 20 | `ביסיגג:` | `ביסינג` | `bising` |
| 21 | `מאנקאדזשאר:` | `מאנקאדזשאו` | `monkadshu` |

### Yudale_der_blinder,_Emkroyt1908 (9)

| Page | OCR'd surface | Correction | Role xmlid |
|---|---|---|---|
| 9 | `פריידע:` | `פריידא'לע` | `freydale` |
| 10 | `פישעל:` | `פישל` | `fishl` |
| 19 | `פריידעדע:` | `פריידא'לע` | `freydale` |
| 27 | `ירוהם:` | `ירוחם` | `yerukhem` |
| 27 | `ערעלמאן:` | `פראפעסאר עדעלמאן` | `profesor_edelman` |
| 49 | `פריידע:` | `פריידא'לע` | `freydale` |
| 50 | `פריידע:` | `פריידא'לע` | `freydale` |
| 53 | `יידאלע:` | `יודאלע` | `yudale` |
| 64 | `פישעל:` | `פישל` | `fishl` |

### דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete (6)

| Page | OCR'd surface | Correction | Role xmlid |
|---|---|---|---|
| 3 | `פעטיפי:` | `גראף פעטעפי` | `graf_petefi` |
| 4 | `פעטיפי:` | `גראף פעטעפי` | `graf_petefi` |
| 6 | `וו לאד:` | `וולאדיסלאוו` | `vladislav` |
| 8 | `מארט הא:` | `מארטהא` | `marta` |
| 9 | `מארט הא:` | `מארטהא` | `marta` |
| 12 | `פעטיפי:` | `גראף פעטעפי` | `graf_petefi` |

## 3. RA-already-corrected text changes (FYI — no action needed)

Lines whose `<Unicode>` text differs between our pipeline ancestor and the RA-final layer. These OCR corrections have already been made (mostly by Noa). Only the first 5 examples per play are shown.

| Play | Corrections | Pages newly transcribed (was blank) |
|---|---|---|
| AlNaharotBavel-Amkreut&Freund1909 | 11 | 0 |
| Di_seyder_nakht_Emkroyt_1908 | 8 | 0 |
| IshahRaah | 1 | 399 |
| KidushHashem | 1 | 0 |
| Yudale_der_blinder,_Emkroyt1908 | 3 | 0 |
| דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete | 1 | 0 |

**AlNaharotBavel-Amkreut&Freund1909** — 11 corrections

- p5: `. . .` → `АЛЬ НАГАРОТЪ БАБЕЛЬ.`
- p5: `()      .` → `Издательство Амкраута и Фрейнда въ Перемишлю (Австрія).`
- p5: `арарша .  .` → `Типографiя С. Л. Дейтшера, Подгурже-Краковъ.`
- p10: `גוּט, אָבֶּער לָאסְט זִיך דֶען אֵיין מֶענְשׁ פוּן שׁווערִיגנְקֵייטֶען אָבּשרעקען` → `גוּט, אָבֶּער לָאסְט זִיך דֶען אֵיין מֶענְשׁ פוּן שְׁוֶוערִיגְקֵייטֶען אָבְּשְרֶעקֶען`
- p10: `מוֶעזן מִיר אוּן ווֶעלֶען אוִיך אוּנְזֶער הַיילִיגֶען צְוֶועק ערְרֵייכֶען!` → `מוּזֶען מִיר אוּן ווֶעלֶען אוֹיך אוּנְזֶער הֵיילִיגֶען צְוֶועק ערְרֵייכֶען!`

**Di_seyder_nakht_Emkroyt_1908** — 8 corrections

- p3: `אַ לֶעבֶּענְסבִּילְד אִין 2 אקטֶען אוּן 12 בִּילְדֶער` → `אַ לֶעבֶּענְסבִּילְד אִין 4 אקטֶען אוּן 12 בִּילְדֶער`
- p8: `דֶער צַייט דָאך טרֵייסטֶען אוּן זָאָגען נם זוּ לטובה, אָבּער זייַן קִינְד הָאט` → `דֶער צַייט דָאך טרֵייסטֶען אוּן זָאָגען גם זוּ לטובה, אָבּער זייַן קִינְד הָאט`
- p8: `ראשעל: (לויפט אויין) פָאטֶער! פָאטֶער! וואוּ האסטוּ זאוואַס מִיר געֶקעֶנְט` → `ראשעל: (לויפט אריין) פָאטֶער! פָאטֶער! וואוּ האסטוּ זאוואַס מִיר געֶקעֶנְט`
- p18: `זלמן: (קומט אויין) וַוייסְט דִיד אִיך בּין הייַנט גֶעווֶעהְן גֶעשׁטֶערְט בּיים` → `זלמן: (קומט אריין) וַוייסְט דִיד אִיך בּין הייַנט גֶעווֶעהְן גֶעשׁטֶערְט בּיים`
- p20: `ליעד אָהגע מוזיק.` → `ליעד אָהנע מוזיק.`

**IshahRaah** — 1 corrections

- p4: `פָּעלִים — אֵיין פערזישׁער העער אנפֿיהרער.` → `סעלִים — אֵיין פערזישׁער העער אנפֿיהרער.`

**KidushHashem** — 1 corrections

- p7: `טאביאס: אִיךְ בִּין עֶם! וואָס וואוּנְדֶערְט אִיהר אַייךְ אַזוֹי, הֵיילִיגער` → `טאביאס: אִיךְ בִּין עֶס! וָואס וואוּנְדֶערְט אִיהר אַייךְ אַזוֹי, הֵיילִיגֶער`

**Yudale_der_blinder,_Emkroyt1908** — 3 corrections

- p5: `אִין הֶערְצֶען אַריין.ף,` → `אִין הֶערְצֶען אַריין.`
- p5: `ירוחם: רבּש״ע מָלֵא משאַלות אַ.ז. וו.` → `ירוחם: רבּש״ע מָלֵא משאַלות אַ. ז. וו.`
- p6: `ירוהם: אַזוֹי, ר' אִיסֶר פְּרָאצֶענְטְנִיק אִיז גוּר גֶעווֶארֶען אַ שַׁדְכָן? זאָגְט מִיר` → `ירוחם: אַזוֹי, ר' אִיסֶר פְּרָאצֶענְטְנִיק אִיז גוּר גֶעווֶארֶען אַ שַׁדְכָן? זאָגְט מִיר`

**דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete** — 1 corrections

- p2: `דִיענֶער בּייַם רב` → `דִיענֶער בּייַ'ם רב`

## 4. Lint flags — turn-like labels not matching any cast role (OCR-candidate)

Generated by `code/annotation/lint_pages.py` against the 2026-06-14 pull: **68 flagged speaker labels** across 6 plays. (24 prior flags resolved on 2026-06-14 by Sinai adding `פרע`/`וועכטער`/`חזן`/`קעניג` to the relevant cast_dicts as variants or new body-only roles.) Each remaining line starts with a `<label>:` that doesn't match any role in the play's cast_dict. Most are OCR mutations of a known role; some are genuine missing roles (e.g. Blimele `בערל` is open question Q1 to Noa; `ער`/`זיא` are pronouns; the Blimele joint-speakers and ensemble-solo labels in §3 are Q3/Q4 to Noa, not OCR). Judith: please skip rows already covered by the open-questions handoff; everything else is fair game for OCR fix-up.

**Al Naharot Bavel** — 1 flagged label

| Label | Pages | Count |
| --- | --- | --- |
| `הויפטמאן` | 35 | 1 |

**Blimele (di Perle von Warsha)** — 43 flagged labels *(most are open Noa Q1–Q4, not OCR)*

| Label | Pages | Count |
| --- | --- | --- |
| `בערל` | 7, 8, 9 | 17 |
| `ער` | 13, 14, 61 | 9 |
| `זיא` | 13, 14, 61 | 8 |
| `דועט ביידע` | 13 | 1 |
| `דאניאל בליהמעלע דועט` | 16 | 1 |
| `ליעפע דאניאל זעליקל` | 39 | 1 |
| `ליעפע זעליק` | 39 | 1 |
| `מאקסים גראף` | 39 | 1 |
| `דאניאל ליעפע זעליקל` | 39 | 1 |
| `טויבען` | 61 | 1 |
| `ציגיינער` | 61 | 1 |
| `דער איינער` | 64 | 1 |

**Di Seder Nakht** — 5 flagged labels

| Label | Pages | Count |
| --- | --- | --- |
| `ולמן` | 9 | 1 |
| `דו` | 32 | 1 |
| `סאלא אלט` | 56 | 1 |
| `2) קינד` | 65 | 1 |
| `רעפריין` | 69 | 1 |

**Isha Raa** — 15 flagged labels

| Label | Pages | Count |
| --- | --- | --- |
| `פאזשע` | 33, 34 | 4 |
| `אללע` | 6 | 1 |
| `מלכה` | 10 | 1 |
| `אנטינגוס` | 11 | 1 |
| `דוד` | 16 | 1 |
| `שלומיר` | 22 | 1 |
| `חנוד` | 38 | 1 |
| `אינטיגוס` | 40 | 1 |
| `ה` | 47 | 1 |
| `פרי` | 48 | 1 |
| `אבנד` | 53 | 1 |
| `שלומי` | 57 | 1 |

**Kidush Hashem** — 3 flagged labels

| Label | Pages | Count |
| --- | --- | --- |
| `פיקאלע` | 12 | 1 |
| `איזראעל` | 13 | 1 |
| `בראווא` | 22 | 1 |

**Yudale der Blinder** — 1 flagged label

| Label | Pages | Count |
| --- | --- | --- |
| `געאנטווארטעט` | 43 | 1 |

Full CSV (all 1643 lint flags, not just OCR): [`lint_flags_2026-06-14.csv`](lint_flags_2026-06-14.csv) — filter `suggested_action == "fix OCR / link to a cast xmlid"` for the 92-row set above.

---

**Totals:** §1 = 14 confirmed; §2 = 26 suspected (4 plays).

**Cross-play OCR patterns observed:**
- Word-internal space split (`וו לאד` for `וולאד`, `מארט הא` for `מארטהא`, `בּיסיגג` for `ביסינג` with `נג→גג`) — Transkribus often splits glyphs at narrow-letter junctures.
- Final-vowel ע/א confusion (`סאלע`/`סאלא`, `שארלאטע`/`שארלאטא`, `עמיליא`/`עמיליע`) — feminine endings systematically swapped.
- ה/ח confusion (`ירוהם` for `ירוחם`) — recurrent in Yudale.
- Letter doubling/dropping (`פריידעדע` for `פריידע`, `מאקים` for `מאקסים`, `גראס`/`גראפ` for `גראף`).
- ז/ג/ע confusions in initial position (Ezra: `געזרא`/`ערא`/`גיזיזמונד`/`אוויצקי` all initial-letter glitches).
