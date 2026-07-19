# YiDraCor — Status for Noa, 2026-06-14

Combined status of (a) the Blimele Q1–Q4 from the 2026-06-04 handoff, (b) the pipeline-rule questions you sent in chat on 2026-06-14.

**Status legend**

- `[resolved]` — decision applied to the data; no action needed from you.  
- `[confirmed in data]` — your edits in the 06-14 Transkribus pull already apply the rule on Ezra; the pipeline now matches your behavior.  
- `[Sinai confirmed]` — Sinai locked the rule into the pipeline on 2026-06-14.  
- `[open]` — decision needed before next pipeline run.

---

# Part 1 — Open questions (5)

Please answer these before the next pipeline run on the remaining plays.

## A4. p.64 `דער איינער` ("the one") — ensemble or new role?

Single occurrence on Blimele p.64. Two options:

- (a) add as `prefix_variant` of collective `eyner`  
- (b) coin a new body-only xmlid for this specific solo speaker

## B6. Post-act-header line → `stage{type:setting}`?

Should the line immediately following an act header (`ערשטער אקט`, …) or scene header — when that line is NOT a speaker turn and NOT parenthesized — be encoded as a whole-line `stage{type:setting}` describing the scene location?

**Concrete cases in Ezra+Blimele:**

- **Ezra p4** — `ערשטער אקט.` followed by `איינע וואלדגעגענד — רעכטס אַ הייזעל מיט א פענסטער…` (the only unparenthesized post-act-header setting line in either play; all other Ezra act openers — II/III/IV — are followed by a parenthesized stage direction which the existing rule already catches).  
- **Blimele p7** — `I אקט` followed by `(שטעלט פאר בייא ליעפען א סאלאן…)` — already parenthesized, already typed setting by existing rule; B6 doesn't change this.

**Tagging volume if yes:** 1 line in Ezra+Blimele; meaningful only if the same pattern recurs in other plays.

## B7. `(ביס)` triggers song mode \+ same-page backfill?

Every line containing a standalone `(ביס)` marker treated as in-song, AND prior eligible lines on the same page (back to the last heading or non-chorus speaker change) tagged as song (`l` / `lg_id`) under the same musical number?

**(ביס) occurrences in Ezra+Blimele:**

- **Ezra:** p35 (1 line). Total 1\.  
- **Blimele:** p10 (4), p14 (1), p15 (2), p16 (1), p37 (3), p38 (1), p39 (4), p56 (2), p60 (1), p61 (3), p62 (1), p65 (2). Total 25\.

If you say yes, these 26 ביס-bearing lines plus their same-page predecessors will be re-typed as song lines.

## B8. `(ביס)` cross-page backfill?

Should the `(ביס)` rule also reach BACKWARD across page breaks when the preceding page ends with lyric-like content?

**Concrete suspect cross-page bleeds in Blimele:** the ביס clusters span consecutive pages (p14→p15→p16, p37→p38→p39, p60→p61→p62) — songs likely start a page or two before the first (ביס) marker, so cross-page backfill would re-type the lyrics on the leading pages too.

## B9. Mixed rule scope — only for entrance/exit \+ action?

Should `mixed` be used ONLY for entrance/exit cues combined with other action — i.e. directions that combine non-movement functions (`set + emotion`, `business + delivery`, etc.) should continue to pick the dominant function rather than be retyped as `mixed`?

**Concrete cases to ground the decision** (a few stage directions in Ezra+Blimele that combine functions without an entrance/exit cue):

- Ezra p4 — `(לעגט וועג דיא האַרפֿע— ערשיינט)`: HAS entrance cue, already resolved (B2 below) as `mixed`. Not relevant to B9.  
- General class — directions like `(שטיל, פערקלעהרט)` (emotion adverbs stacked) or `(זינגט, טאנצט)` (delivery \+ business). Currently the pipeline picks the first function. Should they become `mixed` instead?

*(I haven't enumerated specific page refs for this — happy to if you want.)*

## **PART 1 — Ezra \+ Blimele (already annotated, your decision per page)**

Each entry: page • trigger word • proposed lyric-span length • opening text. Please confirm/adjust the lg boundaries on each page.

### **Ezra (1 (ביס), 4 זינגט)**

* **p5** \[זינגט, 2L\] — opens — 4 — (trigger: וואלעֶנטין (זינגט פֿון פֿערנע).)  
* **p19** \[זינגט, 2L\] — opens הערען וואס איהר קענט\! (trigger: עזרא (זינגט) טרויריג איזט דאס לעבען...)  
* **p26** \[זינגט, 17L\] — trigger: קאמפּף. (זינגט)  
* **p35** \[(ביס), 13L\] — trigger: נע ונד דאס איז דיין שיקזאל...  
* **p36** \[זינגט, 1L\] — קאהר: (זינגט) אַך וואָס איז דא...

### **Blimele (25 (ביס), 7 זינגט, 6 Nr./געזאנגס)**

* **p10** — FOUR distinct (ביס) spans on same page (yom-tov song with refrains): 6L \+ 2L \+ 2L \+ 2L  
* **p11** \[זינגט, 4L\] — Zelikel's improvised song trigger  
* **p14** \[(ביס), 5L\] — ביידע: יאָ לאמיר פּריווען...  
* **p15** — TWO (ביס) spans: 1L \+ 6L (chorus song)  
* **p16** \[(ביס), 7L\] — refrain about hope/return  
* **p24** — TWO זינגט spans: 1L \+ 2L (king \+ Daniel echo-singing)  
* **p33** \[Nr.1, 3L\] \+ \[זינגט, 21L\] — **first numbered song** (No. 1)  
* **p34** \[No.2, 31L\] — **No. 2**, large span  
* **p37** — THREE (ביס) spans: 5L \+ 5L \+ 4L (Graf victory songs)  
* **p38** \[(ביס), 6L\] — אלע: אונד מיט איהם (ביס) איזט דער זיעג  
* **p39** — FOUR (ביס) spans: 2L \+ 2L \+ 2L \+ 5L (revenge-plan ensemble)  
* **p54** \[זינגט, 1L\] — silent prayer with song direction  
* **p56** — FOUR spans: 5L זינגט \+ 6L (ביס) \+ 6L (ביס) \+ 1L זינגט (Berele/Pavel singing offstage)  
* **p60** \[(ביס), 11L\] — opens Act V masquerade ball  
* **p61** — THREE (ביס) spans: 4L \+ 6L \+ 6L (gypsy חליק duet, the "Elyen Amadiar" refrain)  
* **p62** \[(ביס), 7L\] — continuation of the gypsy song  
* **p65** — TWO (ביס) spans: 5L \+ 3L (yom-tov finale)

---

## **PART 1 — Ezra \+ Blimele (already annotated, your decision per page)**

## Each entry: page • trigger word • proposed lyric-span length • opening text. Please confirm/adjust the lg boundaries on each page.

### **Ezra (1 (ביס), 4 זינגט)**

* ## **p5** \[זינגט, 2L\] — opens — 4 — (trigger: וואלעֶנטין (זינגט פֿון פֿערנע).)

* ## **p19** \[זינגט, 2L\] — opens הערען וואס איהר קענט\! (trigger: עזרא (זינגט) טרויריג איזט דאס לעבען...)

* ## **p26** \[זינגט, 17L\] — trigger: קאמפּף. (זינגט)

* ## **p35** \[(ביס), 13L\] — trigger: נע ונד דאס איז דיין שיקזאל...

* ## **p36** \[זינגט, 1L\] — קאהר: (זינגט) אַך וואָס איז דא...

### **Blimele (25 (ביס), 7 זינגט, 6 Nr./געזאנגס)**

* ## **p10** — FOUR distinct (ביס) spans on same page (yom-tov song with refrains): 6L \+ 2L \+ 2L \+ 2L

* ## **p11** \[זינגט, 4L\] — Zelikel's improvised song trigger

* ## **p14** \[(ביס), 5L\] — ביידע: יאָ לאמיר פּריווען...

* ## **p15** — TWO (ביס) spans: 1L \+ 6L (chorus song)

* ## **p16** \[(ביס), 7L\] — refrain about hope/return

* ## **p24** — TWO זינגט spans: 1L \+ 2L (king \+ Daniel echo-singing)

* ## **p33** \[Nr.1, 3L\] \+ \[זינגט, 21L\] — **first numbered song** (No. 1)

* ## **p34** \[No.2, 31L\] — **No. 2**, large span

* ## **p37** — THREE (ביס) spans: 5L \+ 5L \+ 4L (Graf victory songs)

* ## **p38** \[(ביס), 6L\] — אלע: אונד מיט איהם (ביס) איזט דער זיעג

* ## **p39** — FOUR (ביס) spans: 2L \+ 2L \+ 2L \+ 5L (revenge-plan ensemble)

* ## **p54** \[זינגט, 1L\] — silent prayer with song direction

* ## **p56** — FOUR spans: 5L זינגט \+ 6L (ביס) \+ 6L (ביס) \+ 1L זינגט (Berele/Pavel singing offstage)

* ## **p60** \[(ביס), 11L\] — opens Act V masquerade ball

* ## **p61** — THREE (ביס) spans: 4L \+ 6L \+ 6L (gypsy חליק duet, the "Elyen Amadiar" refrain)

* ## **p62** \[(ביס), 7L\] — continuation of the gypsy song

* ## **p65** — TWO (ביס) spans: 5L \+ 3L (yom-tov finale)

## 

## **PART 2 — Remaining plays (proposed spans, please confirm)**

### **AlNaharotBavel — 3 proposed song spans**

* p23 \[זינגט, 2L\] — זרובבל: (זינגט).  
* p43 \[זינגט, 1L\] — זרובבל זינגט פון דרויסען סאלא...  
* p51 \[זינגט, 1L\] — דלילה: (מיט איין בעכער וויין זינגט).

### **DerManUnterTiff — 4 proposed song spans**

* p8 \[זינגט, 27L\] — Krakower opens with שלום עליכם then sings  
* p18 \[זינגט, 3L\] — Krakower again  
* p18 \[(ביס), 27L\] — ווי מיר קומען צום רבין צופארען (ביס)  
* p18 \[זינגט, 1L\] — Turniver's song "אוי טאטעניו"

### **Di Seder Nakht — 12 proposed song spans *(Noa fully RA-edited this play 2026-06-14 — already her authority)***

* p23 \[זינגט, 1L\]; p34 \[זינגט, 1L\]  
* p40 \[זינגט, 8L\] — about cantors in shul  
* p50 \[זינגט, 1L\] — synagogue opening  
* p55 \[Nr/געזאנגס, 2L\] \+ \[זינגט, 3L\] — געזאנגס-טעקסט header  
* p59 \[Nr. 3, 28L\]; p61 \[Nr. 4, 28L\] \+ \[Nr. 5, 6L\]  
* p62 \[Nr. 6, 31L\]; p68 \[Nr. 8, 3L\]; p70 \[Nr. 9, 5L\]

### **IshahRaah — 9 proposed song spans**

* p24 \[זינגט, 24L\] — (דער גאנצער אסאמבעל זינגט ביז צו עֶנֶדע, פֿייער...)  
* p25 \[(ביס), 14L\]  
* p27 \[(ביס), 7L\] — ביידע: פעראיינט בלייבען וויר  
* p28 — TWO spans: 3L \+ 9L (Shlomit's loyalty song)  
* p54 — FOUR (ביס) spans: 1L \+ 2L \+ 3L \+ 4L (chorus call-to-arms; Talmai's בעשטירמען דאס העברעער לאנד)

### **KidushHashem — 2 proposed song spans**

* p9 \[זינגט, 28L\] — long Sabbath teaching song  
* p58 \[זינגט, 19L\] — (איזאבעלא זינגט).

### **MishkeMashke**

* **No song triggers found.** Likely no songs in this play (or markers are absent from the OCR). Worth a manual sample-check.

### **Yudale der Blinder — 3 proposed song spans**

* p13 \[זינגט, 1L\] — Freydale's eyes-lowered direction  
* p21 \[(ביס), 12L\] — ביידע: אוי גוט אוי וואהל... ליעבע איז זא זיעס\!  
* p31 \[זינגט, 10L\]

### **Dos Yudishe Kind — 1 proposed song span**

* p7 \[זינגט, 22L\] — בען (זינגט): long lament

### **Lateiner Meshumed**

* **No page\_annotated/ or page/ dir.** Source not yet pulled to per-page XML — only consolidated TEI in text/raw/. Skip until bootstrapped.

---

Notes on the proposal logic:

* "Trigger" \= the line containing (ביס), זינגט in a stage span, or Nr.N/געזאנגסטעקסט opener.  
* Span length \= lines from the proposed lg start back to the last heading/speaker boundary, through to the next heading/speaker/new-stage boundary.  
* Multiple spans on the same page \= separate sub-songs (e.g. chorus \+ soloist alternation).  
* Where you see a span starting with (ביס) alone — that means the prior line was a speaker turn the algorithm respected as a hard boundary; if the lyric actually starts before the speaker line, the boundary needs widening.

---

# Part 2 — Resolved items (reference)

## A. Blimele speaker questions (originally from 2026-06-04 handoff)

### A1. `[resolved]` `בערל` (17×, pp.7–9)

**Decision:** colloquial short form of Berele. Added as `prefix_variants` of `berele` in cast\_dict. All 17 turns now resolve to `xmlid:berele`.

### A2. `[resolved]` `ער` / `זיא` as speaker labels (pp.13, 14, 61\)

**Decision:** these are gendered sung-duet labels for two characters dressed in disguise. Per-scene mapping:

- pp.13–14 (Zelikel/Tsierele duet): `ער` → `zelikel_mnagen`, `זיא` → `tsierele`  
- p.61 lines 4–6 (Zelikel/Tsierele as doves): same  
- p.61 lines 21–28 (Daniel/Blimele as gypsies): `ער` → `doktor_daniel`, `זיא` → `blimele`

Stored in `data/Blimele-AhronFaust1903/speaker_overrides.json`. Schema extended to support per-page-line-range scoping so future plays can declare the same pattern. New infrastructure also added to `auto_resolve_flags` to consult overrides before cast\_dict lookup.

### A3. `[resolved]` Joint / duet speaker labels (6 labels)

**Decision:** single `speaker` span carrying space-separated xmlids (downstream structurer expands to TEI `<sp who="#a #b">`). All 6 lines tagged:

- p.13 `דועט ביידע` → `zelikel_mnagen tsierele`  
- p.16 `דאניאל בליהמעלע דועט` → `doktor_daniel blimele`  
- p.39 `ליעפע דאניאל זעליקל` → `liepe doktor_daniel zelikel_mnagen`  
- p.39 `ליעפע זעליק` → `liepe zelikel_mnagen`  
- p.39 `מאקסים גראף` → `maksim graf_stanislav`  
- p.39 `דאניאל ליעפע זעליקל` → `doktor_daniel liepe zelikel_mnagen`

### A4. Ensemble members speaking solo — 2 of 3 resolved

- `[resolved]` p.61 `טויבען` → joint `zelikel_mnagen tsierele` (per stage direction, the doves ARE Zelikel \+ Tsierele in disguise).  
- `[resolved]` p.61 `ציגיינער` → joint `doktor_daniel blimele` (gypsies are Daniel \+ Blimele in disguise).  
- *(p.64 `דער איינער` deferred — see Part 1 above.)*

## B. Pipeline-rule questions (from 2026-06-14 chat)

### Stage-direction typing

1. `[confirmed in data]` Every occurrence of `ערשיינט` in a stage direction whose only verb is `ערשיינט` → `stage{type:entrance}`. (Ezra p5: your edit retyped `(ערשיינט)` business→entrance.)  
     
2. `[confirmed in data]` Every `ערשיינט` co-occurring with another action verb in the same direction (e.g. `(לעגט וועג דיא האַרפֿע— ערשיינט)`) → `stage{type:mixed}`. (Ezra p4.)  
     
3. `[confirmed in data]` Every `אב` co-occurring with another action word in the same direction (e.g. `אב, שטורם`) → `stage{type:mixed}`. (Ezra p9.)  
     
4. `[Sinai confirmed]` Bare exit (`(אב)` / `(<actor> אב)`) stays `exit`; modal-guarded "intent to leave" (`(<actor> וויל אב)`) stays `business`. The new mixed rule does NOT apply to these.

### Setting detection

5. `[Sinai confirmed]` Every standalone `פערווענלונג` / `פערוואנדלונג` / `פערוואנדעלונג` direction → `stage{type:setting}` (with or without parens/nikud).

---

## Summary

- **Open (5)**: A4 (`דער איינער`), B6, B7, B8, B9  
- **Resolved**: A1, A2, A3, A4 (partial — 2 of 3\)  
- **Confirmed by your past edits**: B1, B2, B3  
- **Sinai confirmed 2026-06-14**: B4, B5

\#\#\# Part 1 — Open Questions Decisions

\*\*A4. p.64 דער איינער ("the one") — ensemble or new role?\*\*  
\* \*\*Decision:\*\* (b) Coin a new body-only \`xml:id\` for this specific solo speaker. Treat it as a unique standalone occurrence on Blimele p.64, not as part of the collective.

\*\*B6. Post-act-header line → stage{type:setting}?\*\*  
\* \*\*Decision:\*\* Yes. If a line immediately follows an act or scene header and is NOT a speaker turn and NOT parenthesized, encode it as a whole-line \`stage{type:setting}\` describing the scene location.

\*\*B7. (ביס) triggers song mode \+ same-page backfill?\*\*  
\* \*\*Decision:\*\* Deferred. A separate detailed report/log specifying the exact page boundaries and structural adjustments for the \`(ביס)\` and \`זינגט\` spans will be provided. Do not apply automated same-page backfill across the board yet.

\*\*B8. (ביס) cross-page backfill?\*\*  
\* \*\*Decision:\*\* Deferred. This will be handled manually/individually via the upcoming separate report mapping out the specific song ranges.

\*\*B9. Mixed rule scope — only for entrance/exit \+ action?\*\*  
\* \*\*Decision:\*\* Keep the current behavior for regular action/emotion combinations (the pipeline picks the first dominant function). However, as a strict rule: if an entrance and an exit cue co-occur within the same stage direction (e.g., "Miriam exits and Shlomo enters"), this MUST be explicitly typed as \`stage{type:mixed}\`.  
