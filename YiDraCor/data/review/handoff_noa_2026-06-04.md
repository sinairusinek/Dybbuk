# YiDraCor — Handoff to Noa, 2026-06-04

Two plays have been fully re-annotated and pushed to Transkribus (collection 18874) after the latest round of your decisions.

## 1. Ezra (Emkroyt 1908) — doc 828481

**Status:** Fully annotated. **No open questions.**

All 39 pages have been:

- pulled from Transkribus,
- re-classified (titlePage / castList / body),
- body pages tagged with speakers, stages, and song structures,
- collective-speaker turns tagged (`alle`, `beyde`, `chor`, `eyner`),
- all OCR variants of cast names resolved to the canonical role.

Cast variants applied since the previous round include: `קעניג` → kazimir, `ווילניצקי` → graf_vilnitsky, `לוי` → doktor_levi, `מארפא` → marpa_tsutska, `פאזש` → zigizmund (the 13 court-page turns on pp.15–16), plus OCR variants `גרעגאר` → grenar, `זייגעסמונד` / `גיזיזמונד` → zigizmund, `סטאניסלאוי` → stanislav, `געזרא` / `ערא` → ezra, `לעבארא` → debora, `אוויצקי` → savitsky.

A new body-only role was coined for `סאלדאט` (soldier/bailiff in the trial scene, pp.26–27) per your confirmation.

**Action requested:** please review the whole play on Transkribus at your convenience.

---

## 2. Blimele (Ahron Faust 1903) — doc 828455

**Status:** Fully annotated. **4 open question batches** below.

All 72 pages have been:

- pulled, re-classified, body-tagged, collective-tagged,
- castList page 6 patched per your decisions: King August role extended to include "דער 3-טע", the 11-element ensemble on idx 11–12 split into individual roles, the location line "אָרט דער האנדלונג…" tagged as `stage type=setting`,
- cast variants applied since the previous round: `מאקסים` → maksim, `דאניאל` → doktor_daniel, `זעליקל` / `זעליק` → zelikel_mnagen, `בליהמעלע` → blimele, `ציערעלע` → tsierele, `פאויל` → berele, `מרים` → miriam, `ציערעלע זעליקל` → tsierele, plus OCR variants `מאקים` → maksim, `דיעפע` → liepe, `געליקל` / `עליק` → zelikel_mnagen, `גראס` / `גראפ` → graf_stanislav,
- new body-only roles coined for `באטע` (messenger, p.46) and `פאזשע` (court page, pp.60–61) per your confirmation,
- chorus lexicon fixed so the spelling `קאר` is now recognized (was previously only matching `קאהר` / `כאר`).

**43 residual flags** are left, all clustered into the four batches below.

### Q1. `בערל` — short form of Berele, or a different character?

The label `בערל:` appears as a speaker 17 times on pp.7–9. Sample turns:

- p.7: `בערל: וואס וואונדערסט דוא דיך אזוי?`
- p.8: `בערל: אבער זעליקעל ביזט דוא משוגע געווארן?`
- p.9: `בערל: יא דאניאל ר' ליפע שוחט'ס זוהן מיינסט?`

The castList has Berele (xmlid `berele`); you already confirmed פאויל is his post-conversion name. Is `בערל` simply the colloquial short form of Berele (treat as variant of `berele`), or is it a separate "Berl" character?

### Q2. `ער` / `זיא` used as speaker labels

`ער` ("he", 9 hits) and `זיא` ("she", 8 hits) appear in the speaker position on pp.13, 14, 61. Two possible readings:

1. **Continuation marker** — the previous turn's named speaker continues; the pronoun is a shorthand for "he/she continues". In that case we should attach the prior named speaker's xmlid.
2. **Stage-direction shorthand** — the pronoun introduces a quoted utterance inside a stage direction, not a true speaker change.

How should we encode these turns? (Same call needed for any future plays using this convention.)

### Q3. Joint / duet speaker labels

Six labels denote two or more cast members speaking together:

| Label | Page | Members |
| --- | --- | --- |
| דועט ביידע | 13 | (both) |
| דאניאל בליהמעלע דועט | 16 | doktor_daniel + blimele |
| ליעפע דאניאל זעליקל | 39 | liepe + doktor_daniel + zelikel_mnagen |
| ליעפע זעליק | 39 | liepe + zelikel_mnagen |
| מאקסים גראף | 39 | maksim + graf_stanislav |
| דאניאל ליעפע זעליקל | 39 | doktor_daniel + liepe + zelikel_mnagen |

How should joint turns be encoded? Options: (a) a single `speaker` span carrying multiple xmlids separated by space (TEI `@who="#a #b"` analogue); (b) splitting the line into multiple `speaker` spans; (c) coining a new collective xmlid per combination. Your call on the convention.

### Q4. Ensemble members speaking solo

Three labels match ensemble members from the castList but spoken solo, not as a group:

| Label | Page | Possible target |
| --- | --- | --- |
| טויבען | 61 | ensemble_toyben (the doves) |
| ציגיינער | 61 | ensemble_eyn_tseyginer_paar (the gypsy pair) |
| דער איינער | 64 | collective `eyner` ("the one") |

Add these as `prefix_variants` of the existing xmlids on the right, or coin a separate xmlid for each?

---

## Next step

Once you answer Q1–Q4, I'll add the variants / coin the roles / encode the joint speakers, re-run the resolver, and Blimele residuals should drop to zero.

## Where to look on Transkribus

- Ezra: collection 18874, document 828481.
- Blimele: collection 18874, document 828455.

Both plays are at status `IN_PROGRESS` with toolName `YiDraCor-annotation-pipeline` on the top layer.
