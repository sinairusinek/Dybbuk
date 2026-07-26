# Lateiner/Hurwitz knowledge graph — questions for the team

*Updated 2026-07-26 (v2). You can answer directly in this file, under each
question — we transfer answers into the data files. Everything that could be
resolved mechanically has been; only the questions below need you.*

## What changed since v1 (background, 2 minutes)

The "121 attribution conflicts" from v1 are **gone** — we traced them to a
bug, not to history. The play lists attached to Lateiner and Hurwitz in our
people database were exported from the DiJeSt person report, whose generator
pooled **both** playwrights' works into one alphabetical list and split it at
an arbitrary point: Lateiner got all titles from *400 יאָר* up to *דניאל*,
Hurwitz everything from *דעם זייגערמאַכערס* onward. (No real repertoire stops
mid-alphabet.) The database's own works table is fine and agrees with the
lexicon entries in every checkable case, so we rebuilt the play authorship
from it: **104 plays reassigned, 118 confirmed**. Whoever maintains the
DiJeSt database should know the "Created expression(s)" column in person
reports is unreliable.

---

## For the PI

### 1. Confirm the authorship rebuild (done, needs your blessing)
We replaced the corrupted person-report play lists with the works-table
authorship (116 Lateiner / 108 Hurwitz). The lexicon entries themselves
agreed with the works table in all 64 cases where we could check. **OK to
keep? Anything you'd like spot-checked?**

> Answer:

### 2. משפּט שלמה — one play or two?
The title משפּט שלמה appears in **both** playwrights' lexicon entries, but the
works table lists it only under **Lateiner**. Did Hurwitz also have a
Mishpat Shloyme (two plays), or is the mention in his entry the same
(Lateiner) play?

> Answer:

### 3. 51 plays with no author in the works table (background task, no rush)
These titles are in the works catalogue without an author. They currently
stay in the graph flagged "unattributed". If any are obviously Lateiner's /
Hurwitz's / someone else's, mark them here whenever convenient:

בוקאַרעשטער פּונגאַש; בעליזאַריע און איזאַבעל; ברכה, אָדער דער יידישער קעניג פון פּוילן אויף איין נאַכט; גבריאל, אָדער די ליבע פֿון אַ ייִדישער פֿרוי; דאָן יאָזעף אברבנל; דאס גאָלדענע קאלב; דאס יודישע קינד; דאס פּוילישע יינגל; דגל מחנה יהודה; דודס פֿידעלע; די וואָרהייט; די וועשין; די ליבע פֿון ירושלים; די לייכטזיניגער; די מאַכט פֿון קונסט; די נייע פרימאַדאָנע; די ערע; די ציגײַנערין; די קאמעליען דאמע; די שנײַדערין; דיא מייא בלומע; ד״ר דניאל; דער בלינדער מוזיקאַנט; דער טיראנישער באַנקער; דער ייִד אין רומעניע; דער ייד אין סאַביעצקי צײַטן; דער ישיבֿה־בחור; דער ליגנער; דער נייער שטערן; דער פֿאַרקויפֿטער שלאָף; דער שבתי צבי; דער שקר; ווילהעלם טעל; וויסטער אינזעל; חכמת נשים; יאַקאָב דער מוזיקאַנט; יהודה וישראל, אדער די קראפט פון שמע ישראל; לומפּאַציוס וואַגאַבונדוס; מאָנטע קריסטאָ; מוטער-ליבע; נחום גענדזעלע; נחמיה קוגל; עוזר לייזער, געציל מיכאל; עזריה; ערשטע ליבע; עתליהו; צוויי שמואל-שמעלקעס; קאָלאָניע שומרון־סאַמאַריע אָדער אוריאל אַקאָסטא אין כאַלאַט; קורחס אוצרות אָדער ממון דער געלדגאָט; קעניג און בויער; שלאמקע און ריקל

> Notes:

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
