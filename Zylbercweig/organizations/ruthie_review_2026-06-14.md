# For Ruthie — three questions from Sinai
_Written 14 June 2026. Please write your answers directly into this file (any place I've marked **Your answer:**), then send it back or leave it in the repo._
This file is self-contained — you shouldn't need to open any other file to answer.
---
## Question 1 — Bobruisk Soviet State Theatre
**What we know.** In the Zylbercweig text there's a theatre called **`באָברויסקער סאָוויעטישן מלוכה-טעאַטער`** ('Bobruisk Soviet State Theatre') mentioned in two places. The Yiddish word `סאָוויעטישן` ('Soviet') and `מלוכה-טעאַטער` ('state theatre') both appear — but the name has **no `יידיש` marker**, so it is not automatically clear whether this was:
- **(A)** a Yiddish-language branch of the Belarusian SSR state theatre system (there was a Belarusian Yiddish State Theatre — we have it in our database as *ווייסרוסישן יידישן מלוכה-טעאַטער*), or
- **(B)** the Belarusian-language Bobruisk state theatre — i.e. a non-Yiddish institution that Yiddish actors happened to work in and got mentioned in Zylbercweig for that reason.

**What's already been decided.** During this session, someone (probably you or Sinai via the app) marked this as its **own new database entity** — it isn't merged into the Belarusian Yiddish State Theatre. That decision stands. But its **type** is currently just 'Theatre'. We recently added a new category **'Non-Yiddish Theatre'** for exactly this case: institutions that are structurally non-Yiddish but appear in Zylbercweig because Jewish artists passed through them.

**The remaining question:** Should the Bobruisk Soviet State Theatre be typed as:

- [ ] **Theatre** (i.e. a Yiddish theatre — the Bobruisk branch of the Belarusian SSR Yiddish state theatre system, even though the name doesn't spell out 'Yiddish')
- [ ] **Non-Yiddish Theatre** (i.e. a Belarusian-language state theatre; Zylbercweig lists it only because Jewish actors passed through)
- [ ] I don't know — please investigate further before deciding

**Your answer:** _(check one, and add a sentence if you have one)_

> 

---
## Question 2 — The Kaminski / Kaminska family
**What we know.** The database currently has **six separate entries** for Kaminski/Kaminska institutions. They span roughly a century of Warsaw Yiddish theatre — from the founder Avrom-Yitshok Kaminski's turn-of-the-century troupe, through his wife Ester-Rokhl Kaminska's own touring company, through commemorative institutions named after her after her 1925 death, all the way to the postwar Polish State Yiddish Theatre (founded 1950, still operating today as *Teatr Żydowski w Warszawie*).

The six entries are:

| # | What it says in the database | Historical reading |
|---|---|---|
| 1 | *Kaminski's Theatre* (Ulica Obozna 1–3, Warsaw) | Pre-1918 family venue — A. Y. Kaminski's fixed theatre in Warsaw |
| 2 | *טרופּע פֿון א. י. קאַמינסקי* | A. Y. Kaminski's own touring troupe (he died in 1918) |
| 3 | *Ester Rukhl Kaminska Troupe* | E-R Kaminska's own touring company (she died in 1925) |
| 4 | *Kaminska Troupe* | Generic — could be E-R Kaminska's troupe or her daughter Ida Kaminska's |
| 5 | *יידישן באַוועגלעכן טעאַטערקאָלעקטיוו אויפן נאָמען פון אסתר-רחל קאמינסקאַ* | Commemorative mobile theatre 'named after' E-R Kaminska (post-1925) |
| 6 | *יידישער מלוכה-טעאַטער אויפֿן נאָמען פֿון אסתר-רחל קאמינסקאַ* | The postwar Polish Yiddish State Theatre (1950–present) |

(Note: a seventh row, *'Sam Adler and Kaminski Troupe'* — a joint troupe — stays separate no matter what you choose here. That's already policy.)

**The question.** A draft merge was prepared last month that would fold **all six** of these into a single canonical entity (following the working principle that theatre-family entities — founder + spouse-led troupe + later state theatre + family venue — belong together as one continuous organisation). It has **not been applied** to the database yet. We want your call before it is.

The choices, from most-merged to least-merged:

- [ ] **Option A — keep all six merged into one.** Maximises family continuity. The current draft.
- [ ] **Option B — split off just #6 (the postwar State Theatre).** Merge 1+2+3+4+5 into one; leave #6 alone. Rationale: the State Theatre is state-funded, communist-era, an organisationally distinct institution 'named after' Kaminska, not run by her.
- [ ] **Option C — split off both 'named after' institutions (#5 and #6).** Merge 1+2+3+4 into one; leave #5 and #6 alone. Rationale: the Yiddish prefix *'אויפן נאָמען פון'* ('named after') is a strong textual signal that these are commemorative successor institutions, not continuations she led.
- [ ] **Option D — split by person.** Combine #2 (Avrom-Yitshok's troupe) with #1 (his venue); combine #3 with #4 (Ester-Rokhl's troupes); leave #5 and #6 alone. Rationale: treats each person's operation as distinct.
- [ ] **Option E — split the venue from the troupes.** #1 alone (Warsaw venue); #2+3+4+5 merged (all family touring companies); #6 alone (State Theatre).
- [ ] **Option F — keep all six separate.** Do not apply any merge.

**Sinai's recommendation:** B or C. The `אויפן נאָמען פון` prefix is a strong textual disambiguator, and #6 is unambiguously a different organisation from the prewar family business.

**How much does this matter?** Small in terms of raw mentions — the corpus has 7 mentions across these 6 entities, and the Yiddish text mostly disambiguates them cleanly on its own (via the prefixes *`א. י.`*, *`מלוכה-טעאַטער`*, *`אויפן נאָמען פון`*). Bigger picture: whatever you choose here becomes the precedent for the other 'super-org' family clusters in the database — **Adler**, **Thomashefsky**, **Goldfaden**, **Schwartz / Yiddish Art Theatre**, **Fishzon**, **Spivakovsky**, **Kompaneyets**. So this call shapes the modelling for all of them.

**Your answer:** _(check one; if you'd like, add a note on the general principle)_

> 

**Follow-up:** Should we apply the same review procedure (entity table + split options + mention-evidence check) to each of the other family clusters listed above, one memo per family?

- [ ] Yes
- [ ] No — just apply my Kaminski choice mechanically to the analogous cases
- [ ] Other:

**Your answer:** 

> 

---
## Question 3 — A list of 56 name matches to confirm or reject
**What's going on.** The Zylbercweig text mentions thousands of organisations. For each one, we've been matching it against our master list of known organisations. Most matches are easy and we've done them automatically. The 56 cases below are the ones where the automatic matcher was **not confident enough** to decide on its own — either because a first name creates ambiguity, or because two surnames appear together, or because a 'state theatre' isn't tied to a known Yiddish city.

For each item you'll see:
- **What Zylbercweig says** — the Yiddish name as it appears in the source.
- **City** — the settlement Zylbercweig associates with it (if any).
- **What I think the answer is** — a proposal (please treat it only as a starting point).
- **Your reply** — a blank line for you to write on.

**How to reply on each row.** Use one of these short answers in the reply column:

- `YES` — agree with what I proposed.
- `NO` — reject the proposal; the row should be left as a new separate entity (or you can specify).
- `ALIGN <name>` — connect it to a specific existing entity by name (e.g. `ALIGN Riga Yiddish State Theatre`).
- `NEW` — this should be created as a new standalone entity.
- `NON-YIDDISH` — mark it as a non-Yiddish theatre (state institution, no alignment to a Yiddish entity).
- Free text is fine too — anything you write, I'll interpret.

---
### Sub-list A — 16 name matches where a first name creates doubt
**Pattern.** Zylbercweig mentions a specific person by first + last name (e.g. 'Boris Tomashevski'). Our database has a family troupe with only the surname ('Tomashevski's troupe'). Question: is the specific-named person the same as the family troupe, or is it a distinct entity?

These are **risky** — different first names inside the same family often meant different theatre operations (Boris vs. Pinchas vs. Mike Tomashevski were different people running different shows). Please judge case by case.

**A1.** Zylbercweig says: **`טרופּע פֿון הענרי לאַכטיגער`**  
First-name found in mention: `הענרי`  
Proposed match: the family troupe **טרופּע פֿון לאַכטיגער**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A2.** Zylbercweig says: **`יידישער טרופּע פון ש. הערשקאָוויטש`**  
First-name found in mention: `ש`  
Proposed match: the family troupe **הערשקאָוויטשס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A3.** Zylbercweig says: **`א'ד פֿון ש. הערשקאָוויטש`**  
City: `לובלין`  
First-name found in mention: `ש`  
Proposed match: the family troupe **הערשקאָוויטשס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A4.** Zylbercweig says: **`ביי חיים סאַנדלער`**  
First-name found in mention: `חיים`  
Proposed match: the family troupe **טרופּע פֿון סאַנדלער**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A5.** Zylbercweig says: **`אייראָפּעישן טורניי פֿון באָריס טאָמאַשעווסקי`**  
First-name found in mention: `באריס`  
Proposed match: the family troupe **טאָמאַשעווסקיס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A6.** Zylbercweig says: **`שלמה הערשקאָוויטשעס אַרטיקער טרופּע`**  
First-name found in mention: `שלמה`  
Proposed match: the family troupe **הערשקאָוויטשס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A7.** Zylbercweig says: **`לובלינער יידישער טרופּע פֿון שלמה הערשקאָוויטש`**  
City: `לובלין`  
First-name found in mention: `שלמה`  
Proposed match: the family troupe **הערשקאָוויטשס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A8.** Zylbercweig says: **`ג. בעקער`**  
First-name found in mention: `ג`  
Proposed match: the family troupe **בעקערס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A9.** Zylbercweig says: **`ביי באָריס טאָמאַשעווסקי`**  
City: `פֿילאדעלפיע`  
First-name found in mention: `באריס`  
Proposed match: the family troupe **טאָמאַשעווסקיס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A10.** Zylbercweig says: **`באַריס טאָמאַשעווסקי`**  
First-name found in mention: `באריס`  
Proposed match: the family troupe **טאָמאַשעווסקיס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A11.** Zylbercweig says: **`טרופּע אונטער דער דירעקציע פֿון נחום ליפּאָווסקי`**  
First-name found in mention: `נחום`  
Proposed match: the family troupe **ליפּאָווסקיס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A12.** Zylbercweig says: **`הורוויץ מיט אַ טרופּע`**  
City: `יאָסי`  
First-name found in mention: `א`  
Proposed match: the family troupe **טרופּע פֿון הורוויץ**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A13.** Zylbercweig says: **`טרופּע פֿון דוד שאַראַוונער`**  
City: `נאָוואָסעלץ`  
First-name found in mention: `דוד`  
Proposed match: the family troupe **שאַראָוונער'ס טרופּעס**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A14.** Zylbercweig says: **`מיט פּנחס טאָמאָשעווסקי`**  
First-name found in mention: `פנחס`  
Proposed match: the family troupe **טאָמאַשעווסקיס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A15.** Zylbercweig says: **`טרופּע פֿון פּנחס טאָמאָשעווסקי`**  
City: `שיקאָגאָ`  
First-name found in mention: `פנחס`  
Proposed match: the family troupe **טאָמאַשעווסקיס טרופּע**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

**A16.** Zylbercweig says: **`ל. ראָפּעל`**  
City: `וואַרשע`  
First-name found in mention: `ל`  
Proposed match: the family troupe **ראָפּעל**  
Sinai's guess: probably a *different* entity — leave separate (new).  
**Your reply:** 

> 

---
### Sub-list B — 3 matches with 'brothers'/joint wording
**Pattern.** Zylbercweig mentions 'the brothers X' or 'X with Y'. Our database has a single 'X's troupe'. Question: is the 'brothers' troupe the same family troupe (just described differently), or a distinct joint operation?

**B1.** Zylbercweig says: **`טרופּע פון די ברידער קאָריק`**  
Proposed match: **קאָריקס טרופּע**  
**Your reply:** 

> 

**B2.** Zylbercweig says: **`ברידער קאָכאַנסקי`**  
Proposed match: **טרופּע פֿון קאָכאַנסקי**  
**Your reply:** 

> 

**B3.** Zylbercweig says: **`פינקעל מיט אַדלערן`**  
Proposed match: **פינקעלס טרופּע**  
**Your reply:** 

> 

---
### Sub-list C — 1 match that could be a person or a duo
**C1.** Zylbercweig says: **`זשאָרזש בעקערס פּראָווינץ-טרופּע`** ('George Becker's province troupe')  
The database has three possibly-relevant entries:
- **George Beker Troupe** (a single person, George Becker, with his own troupe)
- **בעקערס טרופּע** ('Becker's troupe' — generic)
- **טרופּע זשאָרזש אוּן בּעקער** ('Troupe of George AND Becker' — a joint duo of two different people)

Which one is Zylbercweig referring to?

- [ ] The single-person George Becker's troupe
- [ ] The generic Becker's troupe
- [ ] The joint George + Becker duo
- [ ] A new separate entity

**Your reply:** 

> 

---
### Sub-list D — 1 Moscow state theatre (Maly Teatr)
Zylbercweig mentions **`מאָסקווער קליינעם מלוכה-טעאַטער (סאָמבאַטאָוו)`** — 'Moscow's small state theatre (Sombatov)'. This is Moscow's **Maly Teatr**, the famous Russian-language state theatre. It's already marked as 'Non-Yiddish Theatre'. Please confirm.

- [ ] Confirm: Non-Yiddish Theatre, no alignment to the Moscow Yiddish State Theatre (GOSET)
- [ ] No — actually it's the Yiddish one, please align to GOSET
- [ ] Other:

**Your reply:** 

> 

---
### Sub-list E — 35 state-theatre mentions to classify
**Pattern.** These all contain the Yiddish word `מלוכה` ('state') + theatre. Some clearly say `יידיש` ('Yiddish') and refer to Yiddish State Theatres in various cities (Warsaw, Łódź, Baku, Kharkov, etc.). Others say only `סאָוויעטיש` ('Soviet') or a country name without 'Yiddish' — and might be non-Yiddish state institutions Zylbercweig noted because Jewish artists passed through.

A quick primer on the database entries relevant here (so you don't need to look up any file):

| Yiddish State Theatre entries already in the database |
|---|
| Moscow, Kiev, Vilna, Riga — all present |
| Kharkov, Minsk, Bucharest, Birobidzhan, Vinnitsa, Odessa, Moldovan/Kishinev — all present |
| Warsaw, Łódź, Baku, Petrograd, Berdychiv, Tarnopol, Poltava, Homel, Kremenchug — **not yet in the database** (would need to be created if you decide they're Yiddish State Theatres) |

For each row, reply:
- `YES` = agree with the proposal shown
- `ALIGN <city>` = align to the existing Yiddish State Theatre for that city (e.g. `ALIGN Kharkov`)
- `NEW` = create as a new database entry
- `NON-YIDDISH` = mark as a non-Yiddish theatre, no alignment

**E1.** Zylbercweig says: **`מלוכה'ש דראַמאַטיש טעאַטער`**  
City: `זינאָוויאָווסק`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E2.** Zylbercweig says: **`פעדעראַלן מלוכה טעאַטער-פּראָיעקט`**  
City: `(no city given)`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E3.** Zylbercweig says: **`מלוכה-טעאַטער`**  
City: `קרעמענטשוג`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E4.** Zylbercweig says: **`מלוכה-טעאַטער אין גאַלאַץ`**  
City: `גאַלאַץ`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E5.** Zylbercweig says: **`יידישן מלוכהשן באַוועגלעכן טעאַטער פֿון דער אוקראיינישער סאָוועטישער רעפּובליק`**  
City: `(no city given)`  
Proposal: **(needs your judgment)**  
_Reasoning: Yiddish State Theatre, but city wasn't recognized. Is this a known GOSET branch (e.g. Belarusian SSR mobile, All-Ukrainian itinerant)? If yes, write db_id; otherwise NEW._  
**Your reply:** 

> 

**E6.** Zylbercweig says: **`מלוכה-טעאַטער „דער רויטער האַמער"`**  
City: `(no city given)`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E7.** Zylbercweig says: **`באַוועגלעכן מלוכה-טעאַטער פון אוקראינע`**  
City: `(no city given)`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E8.** Zylbercweig says: **`מלוכהשן יידישן באַוועגלעכן טעאַטער פֿון אוקראינע`**  
City: `(no city given)`  
Proposal: **(needs your judgment)**  
_Reasoning: Yiddish State Theatre, but city wasn't recognized. Is this a known GOSET branch (e.g. Belarusian SSR mobile, All-Ukrainian itinerant)? If yes, write db_id; otherwise NEW._  
**Your reply:** 

> 

**E9.** Zylbercweig says: **`דראַמאַטישע סטודיע ביים וואַרשעווער יידישן מלוכה-טעאַטער`**  
City: `וואַרשע`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in וואַרשע — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E10.** Zylbercweig says: **`יידיש באַוועגלעכן מלוכהטעאַטער`**  
City: `(no city given)`  
Proposal: **(needs your judgment)**  
_Reasoning: Yiddish State Theatre, but city wasn't recognized. Is this a known GOSET branch (e.g. Belarusian SSR mobile, All-Ukrainian itinerant)? If yes, write db_id; otherwise NEW._  
**Your reply:** 

> 

**E11.** Zylbercweig says: **`יידישן מלוכהטעאַטער אין פּוילן`**  
City: `נידערשלעזיע`  
Proposal: **(needs your judgment)**  
_Reasoning: Yiddish State Theatre, but city wasn't recognized. Is this a known GOSET branch (e.g. Belarusian SSR mobile, All-Ukrainian itinerant)? If yes, write db_id; otherwise NEW._  
**Your reply:** 

> 

**E12.** Zylbercweig says: **`יידישן מלוכה-טעאַטער`**  
City: `באַקו | באָקו`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in באַקו — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E13.** Zylbercweig says: **`יידישן מלוכה-טעאַטער`**  
City: `וואַרשע`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in וואַרשע — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E14.** Zylbercweig says: **`יידישן מלוכה-טעאַטער`**  
City: `פּאָלטאָווע`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in פּאָלטאָווע — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E15.** Zylbercweig says: **`יידישן מלוכה-טעאַטער`**  
City: `טאַרנאָפּאָל`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in טאַרנאָפּאָל — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E16.** Zylbercweig says: **`יידישן מלוכה-טעאַטער`**  
City: `בערדיטשעוו`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in בערדיטשעוו — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E17.** Zylbercweig says: **`יידישן מלוכה-טעאַטער`**  
City: `לאָדזש`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in לאָדזש — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E18.** Zylbercweig says: **`יידישן מלוכה-טעאַטער`**  
City: `(no city given)`  
Proposal: **(needs your judgment)**  
_Reasoning: Yiddish State Theatre, but city wasn't recognized. Is this a known GOSET branch (e.g. Belarusian SSR mobile, All-Ukrainian itinerant)? If yes, write db_id; otherwise NEW._  
**Your reply:** 

> 

**E19.** Zylbercweig says: **`יידישן מלוכה-טעאַטער`**  
City: `פּעטראָגראָד`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in פּעטראָגראָד — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E20.** Zylbercweig says: **`קעשנעווער מלוכה פּופּן-טעאַטער`**  
City: `קעשנעוו`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E21.** Zylbercweig says: **`באָברויסקער סאָוויעטישן מלוכה-טעאַטער`**  
City: `באָברויסק`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E22.** Zylbercweig says: **`באַוועגלעכן יידישן מלוכהטעאַטער פֿאַר די מערב-געביטן`**  
City: `(no city given)`  
Proposal: **(needs your judgment)**  
_Reasoning: Yiddish State Theatre, but city wasn't recognized. Is this a known GOSET branch (e.g. Belarusian SSR mobile, All-Ukrainian itinerant)? If yes, write db_id; otherwise NEW._  
**Your reply:** 

> 

**E23.** Zylbercweig says: **`אוקראַאינישן יידישן מלוכה-טעאַטער`**  
City: `כאַרקאָוו`  
Proposal: **ALIGN 478?**  
_Reasoning: State theatre with 'יידיש' marker — looks like Kharkov Yiddish State Theatre (db 478). Confirm ALIGN to db 478, or write a different db_id in ruthie_decision._  
**Your reply:** 

> 

**E24.** Zylbercweig says: **`„פּעדאַגאָגיש יידיש-אוקראיינישער מלוכה-טעאַטער פֿאַר קינדער“`**  
City: `קיעוו`  
Proposal: **ALIGN 515?**  
_Reasoning: State theatre with 'יידיש' marker — looks like Kiev Yiddish State Theatre (db 515). Confirm ALIGN to db 515, or write a different db_id in ruthie_decision._  
**Your reply:** 

> 

**E25.** Zylbercweig says: **`מלוכה-טעאַטער פאַרן יונגען צושויער`**  
City: `קיעוו`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E26.** Zylbercweig says: **`וויניצער זשיטאָמירער יידישן מלוכה-טעאַטער`**  
City: `וויניצע | זשיטאָמיר | יעקאַטערינאָסלאָוו`  
Proposal: **ALIGN 535?**  
_Reasoning: State theatre with 'יידיש' marker — looks like Vinnitsa Yiddish State Theatre (db 535). Confirm ALIGN to db 535, or write a different db_id in ruthie_decision._  
**Your reply:** 

> 

**E27.** Zylbercweig says: **`יידישן מלוכה-טעאַטער „געזקולט`**  
City: `(no city given)`  
Proposal: **(needs your judgment)**  
_Reasoning: Yiddish State Theatre, but city wasn't recognized. Is this a known GOSET branch (e.g. Belarusian SSR mobile, All-Ukrainian itinerant)? If yes, write db_id; otherwise NEW._  
**Your reply:** 

> 

**E28.** Zylbercweig says: **`יידישן מלוּכהטעאַטער פֿוּן ווייסרוסלאַנד`**  
City: `(no city given)`  
Proposal: **(needs your judgment)**  
_Reasoning: Yiddish State Theatre, but city wasn't recognized. Is this a known GOSET branch (e.g. Belarusian SSR mobile, All-Ukrainian itinerant)? If yes, write db_id; otherwise NEW._  
**Your reply:** 

> 

**E29.** Zylbercweig says: **`פּעדאַגאנישער יידיש-אוקראיינישער מלוכה-טעאַטער פֿאַר קינדער`**  
City: `קיעוו`  
Proposal: **ALIGN 515?**  
_Reasoning: State theatre with 'יידיש' marker — looks like Kiev Yiddish State Theatre (db 515). Confirm ALIGN to db 515, or write a different db_id in ruthie_decision._  
**Your reply:** 

> 

**E30.** Zylbercweig says: **`יידישער מלוכהשער קאָמער-טעאַטער`**  
City: `פּעטראָגראָד`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in פּעטראָגראָד — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E31.** Zylbercweig says: **`כאַרקאָווער מלוכהשן קינדער-טעאַטער`**  
City: `כאַרקאָוו`  
Proposal: **Non-Yiddish Theatre?**  
_Reasoning: State theatre WITHOUT 'יידיש' marker — may be a national-language state institution (Russian / Ukrainian / Belarusian / Polish state theatre). Consider type=Non-Yiddish Theatre with no ALIGN, OR confirm Yiddish + give db_id._  
**Your reply:** 

> 

**E32.** Zylbercweig says: **`1-טן יידישן מלוכה-טעאַטער-וואַרשטאָט`**  
City: `האָמעל`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in האָמעל — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E33.** Zylbercweig says: **`יידיש מלוכה-טעאַטער ביים פֿאָלקבילד`**  
City: `האָמעל`  
Proposal: **NEW?**  
_Reasoning: Yiddish State Theatre in האָמעל — no existing DB row found. Likely NEW (create new db row), or ALIGN if you know an equivalent. Write NEW or db_id._  
**Your reply:** 

> 

**E34.** Zylbercweig says: **`יידיש-אוקראַאינישן מלוכהשן באַוועגלעכן טעאַטער`**  
City: `(no city given)`  
Proposal: **(needs your judgment)**  
_Reasoning: Yiddish State Theatre, but city wasn't recognized. Is this a known GOSET branch (e.g. Belarusian SSR mobile, All-Ukrainian itinerant)? If yes, write db_id; otherwise NEW._  
**Your reply:** 

> 

**E35.** Zylbercweig says: **`ווייסרוסלענדיש-יידישער מלוכה-טעאַַטערסטודיע`**  
City: `מאָסקווע`  
Proposal: **ALIGN 543?**  
_Reasoning: State theatre with 'יידיש' marker — looks like Moscow Yiddish State Theatre (GOSET) (db 543). Confirm ALIGN to db 543, or write a different db_id in ruthie_decision._  
**Your reply:** 

> 

---
## When you're done
Just save this file with your answers in it. Sinai will apply everything to the database in one pass. Thank you!
