# Lateiner/Hurwitz knowledge graph — questions for the team

*Updated 2026-11-01 (v3: new section A below). You can answer directly in
this file, under each question — we transfer answers into the data files.
Everything that could be resolved mechanically has been; only the questions
below need you.*

*Every question also shows on the works it concerns in the
[entity tree](https://sinairusinek.github.io/Dybbuk/Visualizations/lateiner_hurwitz_entities.html)
(filter "Open questions") and on the play's panel in the
[Plays KG graph](https://sinairusinek.github.io/Dybbuk/Visualizations/plays_kg_graph.html).
Writing an answer here marks it answered there after the next rebuild.*

## What changed since v1 (background, 2 minutes)

The "121 attribution conflicts" from v1 are **gone** — we traced them to a
bug, not to history. The play lists attached to Lateiner and Hurwitz in our
people database were exported from the DiJeSt person report, whose generator
pooled **both** playwrights' works into one alphabetical list and split it at
an arbitrary point: Lateiner got all titles from *400 יאָר* up to *דניאל*,
Hurwitz everything from *דעם זייגערמאַכערס* onward. (No real repertoire stops
mid-alphabet.) The database's own works table is fine and agrees with the
lexicon entries in every checkable case, so we rebuilt the play authorship
from it: **104 plays reassigned, 118 confirmed**.

We also found and applied the ascription vetting that the former catalogue
curators recorded in the `certainty` column of the "Lateiner Plays" /
"Hurwitz Plays" sheets: 9 plays marked **certain** got their author; 18
marked **false ascription / error** were detached from Lateiner/Hurwitz in
the graph (the play stays, the authorship link goes). Since those curators
are no longer with the project, all remaining catalogue-level judgments come
to Ruthie below.

*Standing caveat for the whole project: the "Created expression(s)" column
in DiJeSt person-report exports is corrupt (the pooled-list bug above) — it
should not be used again for any person.*

---

## A. New 2026-11-01 — for Ruthie (PI): translated plays and shared titles

**Background.** Until now, whenever a lexicon sentence mentioned a title that
Lateiner or Hurwitz also used, we linked it to *their* play. That was wrong
for plays that other writers wrote under the same title: every Dybbuk in
the lexicon, for example, was attached to Lateiner's comedy „דער דבוק", including
Ansky's. We now check every mention against its own sentence, and other
writers' plays are kept for the future general graph, not the L/H one.

That check cannot settle **translations**. Several L/H plays are their own
versions of a European play: Schiller's Tell, Kotzebue's *Die wüste Insel*,
Racine's *Athalie*, Dumas's *Monte Cristo*, *Kean* and *La Dame aux camélias*,
Nestroy's *Lumpazivagabundus*. Other Yiddish translations of the same
originals were playing at the same time. When an entry says only "Dumas's
Kameliendame", we cannot tell whose Yiddish text was on stage.

Below: one general question (A1), then each case with every sentence we
have. Each case ends with our proposal. "Agree" is enough; correct any line
you know better. The bracketed codes are for us to transfer your answers.
Nothing in this section has been applied yet.

### A1. The general rule for translated plays
Proposal: count a production as the L/H version only when
(a) the sentence names Lateiner or Hurwitz, or
(b) it is their own troupe or theatre, at a time when their version was the
one in the repertoire.
Otherwise the production goes to the general graph as a production of the
original play, with the translator unknown. Agree?

> Answer:

### A2. ווילהעלם טעל (Schiller) — Lateiner's, not Hurwitz's?
Our play list gives it to **Hurwitz** (it came from the corrupt export).
The catalogue's *Lateiner* sheet claims it, marked "uncertain" (see
question 3 below). Hurwitz's entry never mentions Tell. The lexicon gives it
to the Lateiners:
- Lateiner's entry: „אין 1896 איז אויך געשפּילט געוואָרן ל.'ס און זיין פרויס „פרייע" איבערזעצונג פון שילערס „ווילהעלם טעל"" `[P-2-facs_90_tr_1744131428:3#37]`
- Izela Lateiner's entry: „אין 1896 איז — לויט ב. גאָרין — אויפֿגעפֿירט געוואָרן ל.'ס איבערזעצונג פֿון שילערס „ווילהעלם טעל"" `[P-2-facs_90_tr_1744131418:1#1-2]`

A separate, later translation:
- הערשעלע (Danilevitch): translated Tell for a troupe, then published his translation, Warsaw: Gitlin, 1929 `[P-1-facs_335_r_6:1#1; P-5-facs_250_tr_1739221688:1#1-4]`

**Proposal:** Tell is Joseph and Izela Lateiner's (1896), not Hurwitz's.
Hershele's 1929 translation is a different text and goes out.

> Answer:

### A3. וויסטער אינזעל (Kotzebue) — Hurwitz's and Goldfaden's are two translations?
- Hurwitz's entry, Bucharest 1877–78: „אַלס דריטע פּיעסע די איבערזעצונג פון רומעניש „די ווייסטע אינזל" [איבערזעצט און אויפגעפירט אויך פון גאָלדפאַדען אין זיין טרופּע]". B. Gorin's list also has „אָפּגעקומענער עושר" (also called „וויסטער אינזעל").
- Shoengold's entry: Hurwitz staged „די איבערזעצונג „די ווייסטע אינזל"" (1878) `[P-4-facs_270_tr_1754998435:1#2]`
- Goldfaden's entry: „ג. איבערזעצט דאַן קאָצעבויס „די וויסטע אינזל" אַ טראַגעדיע אין 2 אַקטן, וועלכע איז א"נ „די ווילדע אינזל" באַלד אויפגעפירט געוואָרן מיט גראַדנער אַלס „דער אייראָפּעער" שכר גאָלדשטיין אַלס „רעגינע" און מאָגולעסקאָ אַלס „די נעגערין"" `[P-1-facs_160_r_4:1#1-4]`

**Proposal:** these are two translations: Hurwitz's from Romanian, and
Goldfaden's „די ווילדע אינזל". The Goldfaden translation and its three cast
lines (Grodner, Goldshteyn, Mogulesko) go out. Please also confirm the
ascription to Hurwitz (it is one of the 24 unvetted plays in question 3).

> Answer:

### A4. עתליהו (Racine) — all Hurwitz's?
Hurwitz's entry: „„עתליהו" (נאָך ראסין)", printed without his knowledge
(Kraków: Faust, 1903) under his name. The productions we have:
- Segalesco as the false priest Matan, 1881, in „זיין איבערזעצונג פֿון ראַסיןס „עתליהו"" (Hurwitz's) `[P-2-facs_364_tr_1744366859:1#5-6]`
- Rosa Axelrod in „הורוויץס אָפּערעטע „עתליהו"" `[P-1-facs_61_tr_1740521158:1#1-2]`
- Berta Hart, age 7, as Yehoash, under Yankev-Ber Gimpel (1894) `[P-1-facs_310_tr_1741260419:1#1-2]`
- Malvina Lobel, age 6, as Yoash, in her father Treitler's troupe, Kolomea 1884 `[P-2-facs_87_tr_1744131446:1#1-2]`
- Hurwitz's own entry: staged in Romania (per Leon Blank); printed 1903 `[P-1-facs_312_TextRegion_1708860417410_829:1#6, 3#15]`

(We have already removed a sentence about „גמול עתליה", a different play
written after Racine.)

**Proposal:** all of these are Hurwitz's. That includes the Galician
children's productions (Gimpel 1894, Treitler 1884), unless you know of
another Yiddish *Athalie* in that repertoire. Please also confirm the
ascription (one of the 24 unvetted plays).

> Answer:

### A5. מאָנטע קריסטאָ (Dumas) — which productions are Hurwitz's?
- Hurwitz's entry: „דעם 12 יאַנואַר 1894 — „מאָנטע קריסטאָ" [אַ באַאַרבעטונג פון דיומאַס ראָמאַן]" `[P-1-facs_312_TextRegion_1708860417410_829:2#19]`
- Yarikhovsky, London **1889**, Hollandish Club troupe: „דאָ האָט י. פֿאַרפֿאַסט מוזיק צו „מאָנטאָ קריסטאָ"". This is five years before Hurwitz's 1894 version. `[P-2-facs_74_r_3:1#1]`
- Dina Feinman, photo caption „דיינע פֿיינמאַן אין „גראָף מאָנטע קריסטאָ"" (no date or place) `[P-6-facs_203_tr_1747929008:1#7]`
- (Already out: Gebil's staging of „דזשעמס אָניעלס „מאָנטאָ קריסטאָ"".)

**Proposal:** keep only Hurwitz's 1894 version. Yarikhovsky's 1889 music
belongs to an earlier adaptation, so it goes out. The Feinman photo goes out
unless you know it is Hurwitz's text. Please also confirm the ascription
(one of the 24 unvetted plays).

> Answer:

### A6. די קאמעליען דאמע (Dumas fils) — is there a Lateiner version at all?
Our play list gives a Kameliendame to **Lateiner**, but no lexicon sentence
connects him to it. What the lexicon has:
- Max Bukansky: „אין 1896 איז אויפֿגעפֿירט געוואָרן אין זיין איבערזעצונג דיומאָס „די קאָמעליען-דאָמע"" `[P-1-facs_93_r_1:1#1-2]`
- A.-D. Finkel: his translation printed Warsaw 1913, and „ביידע איבערזעצונגען, אָן דעם נאָמען פֿון איבערזעצער, זיי אָפֿט געשפּילט געוואָרן אויף דער יידישער בינע" `[P-3-facs_279_tr_1755674208:1#2]`
- A. R. Kaminski, America 1910: „אַלעקסאַנדער דיומאַס פּיעסע „די קאַמעליען-דאַמע"" `[P-6-facs_327_tr_1747930566:2#4]`
- Kaminska, Brooklyn Novelty Theatre, Jan. 1912 `[P-6-facs_327_tr_1747930566:3#1]`
- Esther-Rachel Kaminska, Warsaw Elizeum 1916 (a role that did not suit her) `[P-6-facs_210_tr_1747928994:2#1-2]`
- Dina Feinman: London 1911, Lemberg 1912, and as Marguerite Gautier in Feinman's Yiddish People's Theatre („פֿון אַלעקסאַנדער דיומאָ") `[P-6-facs_203_tr_1747929008:1#8, 2#1-3; P-6-facs_166_tr_1747825168:1#1]`
- Lydia Potocka, New York Irving Place Theatre 1926, in the title role of „דיומא'ס „די [קאַמעליען-דאַמע]"" `[P-3-facs_19_tr_1752653066:1#1]`
- (Already out: Label's Buenos Aires production, from your gold check.)

**Proposal:** none of these is Lateiner's text, so all of them go out.
Lateiner's Kameliendame stays in the catalogue without productions. If you
know where the Lateiner ascription comes from (a printing? a playbill?),
please say.

> Answer:

### A7. קיען, אָדער ליבע און ליידענשאַפט (Dumas's *Kean*) — and the other „ליבע און ליידנשאַפט"s
- Lateiner's entry: „דעם 1 יאנואר 1898 איז אין ווינדזאָר-טעאַטער אויפגעפירט געוואָרן צו בּאָריס טאָמאַשעווסקיס בּענעפיס ל'ס בּאַאַרבּעטונג פון אַלעקסאנדער דיומאַס „קין, אָדער, ליבּע אונד ליידנשאַפט"" `[P-2-facs_90_tr_1744131428:3#41]`
- Moyshe Shor's entry: YIVO holds plays „אויף וועלכע ש. ווערט אָנגעגעבן אַלס מחבר: … „ליעבע און ליידענשאַפט"" `[P-4-facs_123_tr_1753936429:1#7]`
- Anshel Shor's entry: „אין 1914 ווערט אויך אויפֿגעפֿירט ש.'ס פּיעסע „ליבע און ליידנשאַפט"" `[P-4-facs_252_tr_1754998536:1#11]`
- Sara Landau, age 14 (1916), debuted with Dina Feinman as „יונגל" in „ליבע און ליידנשאַפט" `[P-2-facs_106_tr_1744279088:1#1-2]`
- (Already out: Zolotarevsky's own play of this title, staged 1921 at Gebil's theatre and printed under his name, Warsaw 1926, and its 1930s film.)

**Proposal:** the two Shor plays go out, since a Shor is named as author.
Landau's 1916 debut goes out unless you know it was Lateiner's *Kean*.

> Answer:

### A8. לומפּאַציוס וואַגאַבונדוס (Nestroy) — Lateiner's text under Grodner's name?
- Lateiner's entry: in Iași he „איבּערזעצט אַ פּיעסע פון רומענישן „לוּמפּאַציוס וואַגאַבּונדוס", וועלכע גראַדנער פלעגט זייער אָפט שפּילן, אָנגעבנדיק זיך אַלס מחבר פון דער פּיעסע" `[P-2-facs_90_tr_1744131428:1#5-6]`
- Grodner's entry: „„לומפּאַציוס וואַגאַבונדוס", איבערזעצט פֿון רומעניש, אויפֿגעפירט 1878", which makes Grodner the translator `[P-1-facs_270_r_2:1#1-2]`

**Proposal:** one text, Lateiner's. Grodner's 1878 production stays on
Lateiner's play, but Grodner is recorded as the *claimed* translator, not
the translator.

> Answer:

### A9. ווירדזשיניוס — Lateiner's, or Jacob Bilder's translation?
Our list has a Virginius by Lateiner (and a separate one by Hurwitz, with
no lexicon evidence).
- Jacob Bilder's entry: „משה זייפערט גיט אָן, אַז עס זיינען געשפּילט געוואָרן אין ב.'ס איבערזעצונג פֿאָלגנרע פּיעסן: „די היימאָט" פֿון זודערמאַן (1892), „נאָפּאָלעאָן באָנאָפּאַרטע", „דער שטורמווינד", „ווירדזשיניוס", „פויסט"" `[P-1-facs_97_tr_1740520953:1#2]`
- Paula Patroni's entry: „„ווירדזשיניוס", וואָס קעסלער האָט דער ערשטער אויפגעפירט", and Tomashefsky played it ten years later `[P-3-facs_21_tr_1752653037:1#1-2]`

Note that Bilder's list also has „דער שטורמווינד", which our list gives to
Lateiner too (PL-0163).
**Question:** is Lateiner's Virginius the same text as Bilder's
translation, or a different one? And which did Kessler and Tomashefsky
play? We have no proposal here.

> Answer:

### A10. דאָס לעבן אין ניו-יאָרק — Lateiner's, or Shenkman's?
Our list gives it to **Lateiner** (from the corrupt export). His own entry
never mentions it. The lexicon gives it to others:
- Zigmund Shenkman's entry: „ווי גאָרין גיט איבער … אין 1890 זיינען אין ניו יאָרק אויפֿגעפֿירט געוואָרן זיינע פּיעסן „דאָס לעבן אין ניו יאָרק, אָדער, 2 קעץ אין איין זאַק"" `[P-4-facs_87_tr_1753938277:1#1-2]`
- Sigmund Feinman's entry: staged as Feinman's play according to Sholem Perlmuter. YIVO holds a manuscript „דאָס לעבען אין ניו יאָרק, פֿאָלקסבילד אין 4 אַקטען, מוז. ז. מאָגולעסקאָ". Perlmuter also says it was really written by „אַ געוויסן [שוישפּילער] שענקמאַן". `[P-4-facs_70_r:1#4-6]`
- Moyshe Simonoff „צוגעפיקסט" (fixed up) many plays, „דערונטער „דאָס לעבן אין ניו יאָרק"" `[P-2-facs_353_tr_1744365776:1#1]`

At the moment the graph is inconsistent: the Shenkman sentences are out,
but the Feinman and Simonoff ones still sit on Lateiner's play.
**Proposal:** unless you know a Lateiner source, mark the Lateiner
ascription as rejected. The play is Shenkman's (staged under Feinman's
name, music by Mogulesko).

> Answer:

---

## For Ruthie (PI)

### 1. Confirm two applied corrections (done, need your blessing)
a. Play authorship rebuilt from the works table (116 Lateiner / 108 Hurwitz;
   the lexicon entries agreed in all 64 checkable cases).
b. The former curators' certainty judgments applied (9 adopted, 18
   ascriptions rejected). Among the rejected: **דאס יודישע קינד** is marked
   "false ascription" in the catalogue **even though our printed 1911
   edition (a Transkribus edition we work on) names Lateiner on the title
   page** — if that judgment is right, the edition metadata should carry a
   note; if it's wrong, say so and we restore the authorship link.
   *Added 2026-11-01:* Lateiner's own lexicon entry settles this. It says
   the printing „דאָס יודישע קינד, אַ קאָמישע אָפּערעטע פון י. לאַטיינער …
   וואַרשע … 1911" „איז באמת אָנגעשריבּן געוואָרן פון שמ"ר-שייקעוויטש". The
   same entry says the 1907 „שלמה גאָרגעל … פון י. לאַטיינער" is really
   Hurwitz's „הדסה". So the curators were right, and the note belongs on our
   edition.

> Answer:

### 2. משפּט שלמה — one play or two?
The title משפּט שלמה appears in **both** playwrights' lexicon entries, but the
works table lists it only under **Lateiner**. Did Hurwitz also have a
Mishpat Shloyme (two plays), or is the mention in his entry the same
(Lateiner) play?

> Answer:

### 3. 24 ascriptions the former curators never vetted (decide or defer)
These sit in the catalogue's per-author sheets with an empty certainty
column. Until decided they stay in the graph as plays WITHOUT an authorship
link. Mark any you can decide (his / not his / leave open); "defer all" is a
fine answer too.

**Claimed for Lateiner (8):** דאס גאָלדענע קאלב; די נייע פרימאַדאָנע; דיא מייא
בלומע; ד״ר דניאל; ווילהעלם טעל *(marked uncertain)*; יאַקאָב דער מוזיקאַנט;
עזריה; שלאמקע און ריקל *(marked uncertain)*

**Claimed for Hurwitz (16):** ברכה, אָדער דער יידישער קעניג פון פּוילן אויף
איין נאַכט; דאָן יאָזעף אברבנל; דאס פּוילישע יינגל; די וועשין; דער טיראנישער
באַנקער; דער ייד אין סאַביעצקי צײַטן; דער פֿאַרקויפֿטער שלאָף; דער שבתי צבי;
וויסטער אינזעל; חכמת נשים; יהודה וישראל, אדער די קראפט פון שמע ישראל; מאָנטע
קריסטאָ; מוטער-ליבע; עוזר לייזער, געציל מיכאל; עתליהו; קאָלאָניע
שומרון־סאַמאַריע אָדער אוריאל אַקאָסטא אין כאַלאַט

*(Hint: many of the Hurwitz-claimed ones do appear in Hurwitz's own lexicon
entry — e.g. דאס פּוילישע יינגל, דער שבתי צבי, עתליהו — which supports the
ascription.)*

> Answers / notes:

---

## For the RA — six concrete questions

Each question shows the lexicon entry it comes from and the exact sentence.
Write the answer under the question.

### 1. Was Malvina Lobel's theatre a place we already know?
In the entry on **מאַרק אַרנשטיין** (vol. 5), his 1914 New York production is
placed at a theatre we read as "מאַלווינאַ לאָבעלס ראָיאָל טעאַטער":

> „דעם 6 פעברואַר 1914 זיין הייסטאָריש-ראָמאַנטישע דראַמע «דער לעצטער משיח» (מיט דוד קעסלער אַלס שבתי צבי)…"

Our organizations list has a New York theatre called **מאָלווינע לאָבעלס
ראָיאַל-טעאַטער**. Same theatre? (yes/no)

> Answer:

### 2. Where did Lateiner's ייִדעלע premiere in 1899 — Windsor or People's?
**Lateiner's own lexicon entry** (vol. 2) says:

> „אין 1899 איז אין ווינדזאָר-טעאַטער אויפגעפירט געוואָרן דורך טאָמאַשעווסקין ל.'ס «יידעלע, אָדער, דער אמת און דער שקר»"

But the newspaper-based catalogue (our Google-Sheet performance events; also
the Transkribus edition *Yudale der Blinder*) has the premiere on
**18 Sept. 1899 at the People's Theatre, NY**. Which is right — or are these
two different 1899 productions? (Worth a JPRESS look.)

> Answer:

### 3. Thomashefsky's 1918 די מחותנים — National Theatre or "Thomashefsky Theatre"?
In the entry on **באָריס טאָמאַשעווסקי**:

> „דעם 11 אָקטאָבער 1918 — לאַטיינערס פּיעסע «דאָס שפּיל פֿון לעבן, אָדער, די מחותנים»"

The lexicon context places it at the **National Theatre**; the catalogue says
**Thomashefsky Theatre**. Are these the same house under two names in 1918,
or a real discrepancy?

> Answer:

### 4. Same building? "Roumanian Opera House" vs "The Roumanian Opera Company"
In the entry on **באָריס טאָמאַשעווסקי**, season 1891-92:

> „אין סעזאָן 1891-92 האָט ט. ווייטער געשפּילט אין רומעניע-אָפּערע-הויז און דאַ אויפֿגעפֿירט יאָזעף לאַטיינערס «אשת חיל»…"

Catalogue entry for the same production says "The Roumanian Opera Company".
We assume house = company here. Confirm? (yes/no)

> Answer:

### 5. The 1895 Bucharest אליהו הנביא — premiere or just a production? Whose play?
In the entry on **איזאַק כץ** (vol. 1):

> „דעם 14 יאַנואַר 1895 האָט די טרופּע פֿון קלמן יוווילער אויפגעפֿירט אין בוקאַרעסט «אליהו הנביאַ, אָדער אַריסתבלוס מלך יהודה»…"

The catalogue knows a premiere of Hurwitz's אליהו הנביא (אָדער דער מיליאָנער)
in **1889**. Note the subtitles differ (אַריסתבלוס מלך יהודה vs דער
מיליאָנער). Is the 1895 Bucharest piece the same Hurwitz play (so 1895 is
just a later production), or a different Elijah play?

> Answer:

### 6. The 1854 Berdichev performance — which play was it?
The entry on **חיים בראָמבערג** (vol. 1) describes an amateur performance on
**9 Oct. 1854 in Berdichev** — a remarkable pre-Goldfaden date, and the
sentence itself looks genuine:

> „…אַ יידישער טעאַטער-פֿאַרשטעלונג, וואָס איז פֿאָרגעקומען אין זומער אָדער דעם 9טן אָקט. 1854 אין בערדיטשעוו. געשפּילט איז געוואָרן די פּיעסע [לויט שטיף] «קהל אין אַ שטעטל» אָדער [לויט דר. ש. ווייסבערג] «די אַלמנה»… ב. האָט געשפּילט די ראָל פֿון דער «אלמנה»"

Our pipeline wrongly attached this to **Hurwitz's** play די אלמנה (Hurwitz
was ten years old in 1854). We will detach it; please just confirm the 1854
piece «די אלמנה» is an anonymous/folk play unrelated to Hurwitz's later one.
(yes/no + any source you know)

> Answer:

### 7. (Optional, when time permits) Trust check of the automatic decisions
We auto-resolved ~970 name-matching decisions (rules + LLM). If you can,
open `kg_link_review.tsv` in the plays folder, filter `decision` starting
with `GEMINI_`, pick ~20 random rows and mark any you disagree with in
`reviewer_notes`. This tells us how much to trust automation before we run
the same pipeline on all the other playwrights. Skip if pressed for time.

---

## Not needed from you
- The 64 attribution swaps from v1 — superseded by the root-cause fix above.
- The ~970 auto-resolved name matches (except the optional sample in #7).
- Known extraction bugs (11 wrong-author play mentions, some too-early dates)
  — already logged as pipeline fixes in `eval/eval_notes.md`.
