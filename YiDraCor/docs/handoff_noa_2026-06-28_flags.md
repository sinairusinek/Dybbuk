# YiDraCor — flag triage for Noa, 2026-06-28

**98 flags needing your decision** (41 additional rows are auto-resolvable next run and are folded into `<details>` per play).

Cumulative changes since 2026-06-24:

- **Sore Sheyndel: 50 → 0 unknown-speaker rows remaining.** All your decisions applied (cast\_dict variants/OCR fixes \+ duet overrides \+ 2 'do not tag as role' labels suppressed).  
- **Dos Yudishe Herts: 32 → 1 unknown-speaker rows remaining.** Your decisions \+ paren-before-colon fix.  
- Across the corpus, `<name> (stage cue):` rows now auto-resolve to the named speaker. **All 8 of your `סיני` rows are gone.** Pure `(stage cue):` rows (no name) are still flagged.  
- Judith's latest OCR fixes unlocked many additional first-pass speaker matches, which is why some plays now show new `untagged speaker (named)` rows (auto-resolvable next run; surfaced for review).

## How to use this doc

- **Untagged speaker (unknown)** — label appears as a speaker but isn't in cast. Decide: (a) coin a new role, (b) variant of an existing role, (c) OCR error → correct surface form, (d) collective, OR write 'do not tag as role' / 'tag as speaker'.  
- **Untyped stage** — parenthesized stage direction the rules couldn't classify. Decide: setting / entrance / exit / business / delivery / mixed.  
- **Speaker missing xmlid** — your speaker span lacks an xmlid; not in cast and not a known collective. Likely a new role or a variant.  
- **Untagged speaker (named)** — collapsed inside `<details>` per play; only inspect if a label looks wrong (these will auto-resolve next pipeline run).

**Reply format:** for each row, write the option letter (or fix text). Sinai will batch-apply.

---

## Das Yudishe Kind — 26 actionable

- Cast dictionary for reference `graf_petefi` — `גראף פעטעפי` / variants: פעטעפי, פעטיפי, פעט  
- `vladislav` — `וולאדיסלאוו` / variants: וולאד, וו לאד  
- `zaslavek` — `זאסלאוועק` / variants: זאסל  
- `hofnar` — `הויפ⸗נאר` / variants: נאר  
- `graf_zezemir` — `גראף זעזעמיר`  
- `rov` — `רב`  
- `henele` — `הענעלע` / variants: הענ  
- `shmerl` — `שמערל`  
- `shprintse` — `שפרינצע`  
- `dyener_rov` — `דיענער`  
- `marta` — `מארטהא` / variants: מארט הא  
- `kenig_zigmund` — `קעניג זיגמונד`  
- `kerker_meyster` — `קערקער⸗מייסטער`  
- `dyener` — `א דיענער`  
- `gayst` — `א גייסט`  
- `kor` — `קאהר`  
- `beyde` — `ביידע`  
- `yudn` — `יודען`  
- `grafn` — `גראפען`

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `הענע` | 6 | 35, 36, 37, 38, 44 | הענע: נִימאַלס, איִך בּין בּערייט צוּ שט | (b) variant of `henele`   |
| `ער` | 5 | 20, 26 | ער: ליעבּעס לִיעדֶער נוּר אימער זיִנְגעֶ |  `וולאדיסלאוו` |
| `זעז` | 5 | 29, 43, 45 | זעז: פון וועמען האסטוּ געבּען גֶערֶעדְט? | variant of `זאסלאוועק` |
| `זי` | 4 | 20, 26 | זי: נִיע אִין לעבּען וֶועל איִך פֶערְגֶע | `הענעלע` |
| `זען` | 2 | 29, 44 | זען: נֵיין, איִך דֶענְק יֶעצְט ווֶעגֶען |  (c) OCR → `זעז` |
| `זאל` | 1 | 16 | זאל: וַוייל מיִר האָבּען הַיינְט גְרויִס | variant of `זאסלאוועק` |
| `העני` | 1 | 21 | העני: ערלויבּ מיִר, פָאטֶער, אִיך זאָל ד |  (b) variant of `henele`  |
| `גייסט` | 1 | 42 | גייסט: רויבּערין, מֶערְדֶערִין\! נֶעהם די |  (b) variant of `א גייסט` |
| `זעזעמיר)` | 1 | 45 | זעזעמיר): גראַדע דיִר, מַיין פרייַנד, דא | Tag as: Stage-\> Delivering  |

---

## Dovid's Fidele — 21 actionable

- Cast dictionary for reference `tubih` — `טוביה`  / variants: טובה  
- `ienkele` — `יענקעלע` / variants: יענקע, יענקעל  
- `nkh` — `נח`  
- `khuh` — `חוה`   
- `slmn` — `סאלאמאן`  
- `dvid_geyger` — `דאוויד גייגער` / variants: גייגער, דאוויד, דוד  
- `tbele` — `טאבעלע`   
- `r_zisse` — `ר' זיססע` / variants: זיססע, זיסע  
- `keyle_beyle` — `קיילע ביילע` / variants: ביי, ביילע, קיי, קיילע, קיילע ביי  
- `lteruniu` — `אלטערוניו` / variants: אלטער  
- `itsele_fifek` — `איצעלע פיפעק` / variants: איצע, איצעלע, פיפעק  
- `tile` — `טילע`

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `מינא` | 11 | 54, 55, 56, 57, 58, 61 | מינא: נִיכְט וַואהְר ר' טובִּיה אִיהר ענ | (a) coin role  |
| `נא` | 1 | 10 | נא: שאט נִיט\! אִין קֶעלֶער אַריין וֶועל |  (c) OCR → `נח` |
| `ענקעל` | 1 | 37 | ענקעל: אָנקעל\! |  (c) OCR → `יענקעלע` |
| `יעקעל` | 1 | 38 | יעקעל: אַנו נח\! נעהם אַרויס לאָמיִר זֶעה | ( (c) OCR → `יענקעלע` |
| `סטודענט` | 1 | 40 | סטודענט: אַך דַיינֶע פרֵיינְד זַייא קאמֶ | (a) coin role  |
| `יענקעל טאבעלע` | 1 | 41 | יענקעל טאבעלע: פְרֵיידֶענְסטאגֶע הַייטֶע |  (b) variant of `יענקעלע` |
| `חיה` | 1 | 60 | חיה: נוּ? | typo `חוה` |
| `הוה` | 1 | 60 | הוה: וואָס? |  (b) variant of `חוה` |
| `בר ככנא` | 1 | 71 | בר ככנא: דִיא באבע מיט דעם אייניקעל: דִי | Promotional material, non-theatrical  |
| `טהיילע)` | 1 | 71 | טהיילע): מליץ יושר ר' יאזעלמאן: משִׁיח'ס | Promotional material, non-theatrical  |
| `בריינדיל קאזאק` | 1 | 71 | בריינדיל קאזאק: נִיעבֶּע נִיעמֶע נִינקוּ | Promotional material, non-theatrical  |

---

## Isha Raa — 15 actionable

- Cast dictionary for reference `avner` — `אבנר`  
- `izebel` — `איזבל`  
- `shlomit` — `שלומית`  
- `antignos` — `אנטיגנוס`  
- `khanokh` — `חנוך`  
- `perets` — `פרץ` / variants: פרע  
- `milkah` — `מילכה`  
- `kenig_hurknos` — `קעניג הורקנוס.`  
- `kerker_meyster` — `איין קערקערמייסטער.`  
- `talmai` — `תלמי`  
- `selim` — `סעלים`  
- `perzer` — `פערזער`  
- `yuden_ensemble` — `יודען`  
- `militer` — `מיליטער`  
- `wechtter` — `וועכטער`

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | ----- |
| `פאזשע` | 4 | 33, 34 | פאזשע: מַיין הֶערְר אוּנד קֶענִיג\! | (a) coin role  |
| `אללע` | 1 | 6 | אללע: יַא יַא עֶר לעבּע לאַנְג | מעדכען (נערות) |
| `מלכה` | 1 | 10 | מלכה: פרֶצְ'ל \! | OCR → `מילכה` |
| `אנטינגוס` | 1 | 11 | אנטינגוס: אִיזְט דַיינֶע הֶערְרִין צוּ ה |  (c) OCR → `אנטיגנוס` |
| `דוד` | 1 | 16 | דוד: (זיפצט) בּרוּך דיין אמְת. |  (c) OCR → `חנוך` |
| `שלומיר` | 1 | 22 | שלומיר: געליעבּטער פָאטֶער\! זַייא נִיכְט |  (c) OCR → `שלומית` |
| `חנוד` | 1 | 38 | חנוד: אַלץ דאס אייגֶענֶע אוּנְד וִויעדֶע |  (c) OCR → `חנוך` |
| `אינטיגוס` | 1 | 40 | אינטיגוס: וואָס קִימֶערְט מִיך וואָס עֶר |  (c) OCR → `אנטיגנוס` |
| `ה` | 1 | 47 | ה: אָה הֶערְרִין, נָאך אִיזְט עֶס צַייט, |  (c) OCR → `מילכה` |
| `פרי` | 1 | 48 | פרי: אַז. |  (c) OCR → `פרץ` |
| `אבנד` | 1 | 53 | אבנד: וֶועלְכֶע שׁטִימֶע? |  (c) OCR → `אבנר` |
| `שלומי` | 1 | 57 | שלומי: דוּא בּיזט עֶס? תלמי\! |  (c) OCR → `שלומית` |

---

## Hinke Pinke — 10 actionable

- Cast dictionary for reference `hertsg` — `הערצאג.`  
- `dr_brhm` — `ד"ר אברהם` / variants: אברהם, ד"ר  
- `drin` — `אדריאן:`  
- `gbril` — `גבריאל`  
- `dinh` — `דינה`   
- `finke` — `פינקע`  
- `khinke` — `חינקע:`  
- `frits` — `פריץ:`   
- `fzsh` — `פאזש`  
- `rikhter` — `ריכטער.`  
- `henker` — `הענקער`  
- `iener` — `יענער`  
- `boyeren` — `בויערען,`  
- `iuden` — `יודען`   
- `sldten` — `סאלדאטען.`

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `אינקע` | 3 | 34, 48 | אינקע: פּיִנְקעֶ, וואוּ זעֶנעֶן מיִר? |  (c) OCR → `חינקע` |
| `נבריאל` | 3 | 54 | נבריאל: צוּריק געקוּמען? פוּן וואַנעֶן ז |  (b) variant of `גבריאל` |
| `הינקע` | 1 | 48 | הינקע: זעהסטוּ שׁוֹין וואָס אַ ווייבּ אי |  (c) OCR → `חינקע` |
| `קינדער כאר` | 1 | 48 | קינדער כאר: געֶבּעֶנטשט זאָלְט איִהר זיי |  (d) collective |
| `קינד` | 1 | 50 | קינד: (סאָלאַ) דאָס איִז גרעֶסטעֶ... (אַ | part of the collective- `קינדער כאר`  |
| `הויפטמאן` | 1 | 53 | הויפטמאן: וואָס איִזט דעֶן איִהנעֶן דעֶר | (a) coin role |

---

## Di Seder Nakht — 8 actionable \+ 3 auto-resolvable

- Cast dictionary for reference `zelmen_kahn` — `זלמן קאהן` / variants: זלמן  
- `dovid_kahn` — `דוד קאהן` / variants: דוד  
- `miryam` — `מרים`  
- `rashel` — `ראשעל` / variants: ראשיל  
- `meir_dreyer` — `מאיר דרייער` / variants: מאיר  
- `tudris_bik` — `טודריס ביק` / variants: טודריס, טודרוס  
- `getsil` — `ר׳ געציל` / variants: שמש  
- `karl_rizvan` — `קארל ריזוואן` / variants: קארל  
- `dumitriye_rizvan` — `דומיטריע ריזוואן` / variants: דומיטריע, דימיטריע, דעמיטריע  
- `katinka` — `קאטינקא`  
- `vasile_talhar` — `וואסילע טאלהאר` / variants: וואסילע  
- `unterzukhungs_rikhter` — `איין אונטערזוכונגס ריכטער` / variants: ריכטער  
- `prefekt` — `פרעפעקט`  
- `helpgot` — `העלפגאט` / variants: העלפנאט  
- `kor` — `קאהר`  
- `dyener` — `דיענער`  
- `alle` — `אלע`

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `דו` | 2 | 32 | דו: מיין שולד מיין אייגענע שוּלד\! |  (c) OCR → `דוד` |
| `סאלא אלט` | 2 | 56 | סאלא אלט: דיא שעהנע ראזע בּליהט |   |
| `רעפריין` | 2 | 69 | רעפריין: יוּדאלע נישט האָבּ קיין מורה | Musical Direction  |
| `ולמן` | 1 | 9 | ולמן: עַם וואוּנְדֶערְט דִיך מֵיין קִינְ |  (c) OCR → `זלמן` |
| `2) קינד` | 1 | 65 | 2\) קינד.: איך זאָל נוּר דערלעבּען דיא גד | (a) coin role |

3 untagged speakers (named) — auto-resolvable next run, review only if a row looks wrong 

| Surface label | × | Pages |
| :---- | ----: | :---- |
| `דוד` | 2 | 5, 7 |
| `מאיר` | 1 | 6 |

---

## Bas Sheva — 7 actionable

- Cast dictionary for reference `tsduk` — `צדוק` / variants: צדיק  
- `bs_shbe` — `בת שבע` / variants: שבע  
- `shlumil` — `שלומיאל`  
- `ezrih` — `עזריה`  
- `lmkh` — `למך`  
- `bnr` — `אבנר`  
- `bnimin` — `בנימין` / variants: בנימן, בתשבע  
- `ssr` — `אסתר`  
- `teyfel` — `טייפעל`  
- `shefer` — `שעפער`  
- `sheferigen` — `שעפעריגען,`  
- `militer` — `מיליטער`  
- `teyfel_2` —  `טייפעל,`  
- `boyen` — `בויען`  
- `etts` — `עטצ.`

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `זלמן` | 3 | 26, 62, 66 | זלמן: הֶערְט\! הֶערְט נֵייעס\! הֶערְט\! |  (c) OCR → `למך` |
| `אבנר, בנימין` | 1 | 16 | אבנר, בנימין: נִישְט דֶער רֶעדֶע ווערְטה | **Tag each one as a separate character** |
| `בוימין` | 1 | 32 | בוימין: נוּ, וואָס? | TYPO(b) variant of `בנימין` |
| `בנילין` | 1 | 47 | בנילין: דאמִיט הָאט דָאך דֶער פֶעלְדְמאר | TYPO (b) variant of `בנימין` |
| `שלומאיל` | 1 | 57 | שלומאיל: נוּ, וואַס'זשׁע אִיז דאפוּן? | TYPO (b) variant of `שלומיאל` |

---

## Kidush Hashem — 4 actionable \+ 4 auto-resolvable

- Cast dictionary for reference `sancto` — `סאנקטא`  
- `helena` — `העלענא`  
- `avraham_abarbanel` — `אברהם בן אליהו אבארבאנעל`  
- `don_yisrael` — `דאן איזראעל` / variants: איזראעל  
- `mendes` — `מענדעס`  
- `pikola` — `פיקאלא`  
- `graf_larash` — `גראף לאראש`  
- `izabella` — `איזאבעלא`  
- `tobyas` — `טאביאס`  
- `yulye` — `יוליע`  
- `andree` — `אנדרעע` / variants: שבת גוי, שבת-גוי

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `פיקאלע` | 2 | 12 | פיקאלע: דֶער גרַאף הָאט דָאך נִישְט מֶעה |  (b) variant of `פיקאלא` |
| `בראווא` | 2 | 22 | בראווא: דאָס אִיז גֶעווֶען אַ קוּנסטפוּל | **No tagging needed, part of the text** |

4 untagged speakers (named) — auto-resolvable next run, review only if a row looks wrong 

| Surface label | × | Pages |
| :---- | ----: | :---- |
| `איזראעל` | 1 | 13 |
| `יוליע` | 1 | 32 |
| `טאביאס` | 1 | 32 |
| `מענדעס` | 1 | 57 |

---

## Yudale der Blinder — 4 actionable \+ 1 auto-resolvable

- Cast dictionary for reference `hertsl_valdman` — `הערצעל וואלדמאן`  
- `raza` — `ראזא`  
- `alteril` — `אלטעריל`  
- `yudale` — `יודאלע`  
- `yerukhem` — `ירוחם`  
- `yakhne` — `יאכנע`  
- `dvorele` — `דבורה'לע`  
- `freydale` — `פריידא'לע` / variants: פריידעלע, פריידע  
- `berman` — `בערמאן`  
- `iser` — `איסר`  
- `profesor_edelman` — `פראפעסאר עדעלמאן`  
- `kor` — `קאהר`  
- `fishl` — `פישל` / variants: פישעל, פישעלע

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `געאנטווארטעט` | 2 | 43 | געאנטווארטעט: אָנקעל\! אִיך ליעבּע רָאזא' | **No tagging needed, part of the text** |
| `חזן` | 2 | 64 | חזן: (פאַנגט אן) בּרוּך אַתה ד'\! | (a) coin role  |

1 untagged speakers (named) — auto-resolvable next run, review only if a row looks wrong 

| Surface label | × | Pages |
| :---- | ----: | :---- |
| `איסר` | 1 | 41 |

---

## Al Naharot Bavel — 1 actionable \+ 5 auto-resolvable

- Cast dictionary for reference `daniel` — `דניאל`  
- `zerubavel` — `זרובבל`  
- `elyakim` — `אליקים`  
- `yehudis` — `יהודית`  
- `zimri` — `זמרי`  
- `palmira` — `פאלמירא`  
- `ben_kaspi` — `בן כספי`  
- `dovid` — `דוד`  
- `belshatsar` — `בלשצר` / variants: קעניג, קעניג בלשצר, בלשצר  
- `opetes` — `אפעטעס`  
- `kulyan` — `קוליאן`  
- `delila` — `דלילה`

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `הויפטמאן` | 1 | 35 | הויפטמאן: אִיהר שווייגט? | (a) coin role  |

5 untagged speakers (named) — auto-resolvable next run, review only if a row looks wrong 

| Surface label | × | Pages |
| :---- | ----: | :---- |
| `זמרי` | 2 | 10, 53 |
| `קעניג` | 1 | 19 |
| `פאלמירא` | 1 | 44 |
| `אליקים` | 1 | 61 |

---

## Blimele (di Perle von Warsha) — 1 actionable

- Cast dictionary for reference `kenig_oygust` — `קעניג אויגוסט`  
- `graf_stanislav` — `גראף סטאניסלאוו יאבלאנאוויטש` / variants: גראס, גראפ  
- `maksim` — `מאקסים` / variants: מאקסים, מאקים  
- `liepe` — `ליעפע` / variants: דיעפע  
- `doktor_daniel` — `דאקטאר דאניאל` / variants: דאניאל  
- `tsierele` — `ציערעלע` / variants: ציערעלע, ציערעלע זעליקל  
- `blimele` — `בליהמעלע` / variants: בליהמעלע  
- `zelikel_mnagen` — `זעליקעל מנגן` / variants: זעליקל, זעליק, געליקל, עליק  
- `miriam` — `מרים ליעפעס` / variants: מרים  
- `berele` — `בערעלע` / variants: פאויל, בערל  
- `bote` — `באטע`  
- `pazshe` — `פאזשע`  
- `der_einer` — `דער איינער`

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `מאלסים` | 1 | 23 | מאלסים: נָאך לאַנגע נִיכְט. |  (c) OCR → `מאקסים` |

---

## Dos Yudishe Herts — 1 actionable

- Cast dictionary for reference `lmkh` — `למך` / variants: זלמן, למן  
- `serkele` — `סערקעלע` / variants: סערקע, פערקע, סענקע  
- `dinhle` — `דינהלע` / variants: דינה, די נו  
- `iekb_shteren` — `יעקב שטערען` / variants: יעקב, שטערען  
- `rz` — `ראזא`  
- `hermn` — `הערמאן`  
- `feter_mshh` — `פעטער משה` / variants: משה, פעטער  
- `ful_ffesk` — `פאולא פאפעסקא` / variants: פאולא, פאפעסקא  
- `viktr` — `וויקטאר`  
- `lid` — `לידא` / variants: ליידא  
- `kmisr` — `קאמיסאר`  
- `diener` — `דיענער`  
- `er` — `ער`  
- `zi` — `זי`  
- `tsvey_kinder` — `2 קינדער`  
- `veyber` — `ווייבער`  
- `refrn` — `רעפריין`  
- `sl` — `סאלא`  
- `tsveyte` — `צווייטע`  
- `drite` — `דריטע`

### Untagged speakers (unknown) — group by surface label

| Surface label | × | Pages | Sample line | Your call |
| :---- | ----: | :---- | :---- | :---- |
| `(שרייט)` | 1 | 16 | (שרייט): למְך\! | Tag as stage—\> delivery |

---

## Mishke Mashke — 0 actionable \+ 28 auto-resolvable

- Cast dictionary for reference `bising` — `ביסינג` / variants: ביסיגג, בּיסינג, בּיסְינג  
- `monkadshu` — `מאנקאדזשאו` / variants: מאנקאדזשאר, מאָנקאַדזשאוּ, מאנקאדושאו, מאָנקאַדזשאו  
- `natanzon` — `נאטאנזאהן`  
- `emilye` — `עמיליע` / variants: עמיליא, עמיליען  
- `karl` — `קארל`  
- `sharlata` — `שארלאטא` / variants: שארלאטע, שאַרלאָטאַ, שאַרלאָטא, שאַרלאָטאַ'ס, שארלאטאס, שאַרלאָטאס  
- `mashke` — `מאשקע`  
- `mishke` — `מישקע`  
- `lieberman` — `ליעבערמאן`  
- `mina` — `מינא`  
- `sala` — `סאלא` / variants: סאלע  
- `tirale` — `טיראללע`  
- `dos` — `דוס`  
- `todt` — `טאדט`  
- `chinezer` — `חינעזער` / variants: חינעזע

28 untagged speakers (named) — auto-resolvable next run, review only if a row looks wrong 

| Surface label | × | Pages |
| :---- | ----: | :---- |
| `עמיליא` | 9 | 10, 13 |
| `סאלא` | 6 | 15 |
| `סאלע` | 3 | 10 |
| `שארלאטע` | 3 | 15 |
| `טיראללע` | 2 | 9, 10 |
| `טאדט` | 1 | 8 |
| `דוס` | 1 | 10 |
| `חינעזע` | 1 | 10 |
| `ביסיגג` | 1 | 20 |
| `מאנקאדזשאר` | 1 | 21 |

---

