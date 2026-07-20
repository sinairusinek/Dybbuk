# YiDraCor — consolidated handoff for Noa, 2026-07-20

Everything still open across **all 15 printed plays**, in one place. Previous
rounds (06-14, 06-18, 06-24, 06-28) are closed or absorbed here.

**Reply format:** write your answer next to each item. Sinai batch-applies.

This list is only what no rule can decide. If you ever find your own
annotation missing, tell us — we can recover it from the layer history.

---

## Part 1 — Where your two answers disagree with each other

~~For these three you answered in prose on 06-21, then tagged the castList by
hand differently on 06-24.~~ **Resolved 2026-07-20 (Sinai): the annotation
stands in all three cases.** No action needed — cast_dict already matched your
annotation, so nothing had to change.

| | your 06-21 prose | your 06-24 annotation |
|---|---|---|
| **1.1 Bas Sheva** `שעפער שעפעריגען` | (a) ONE collective `shefer` | **two** separate role spans |
| **1.2 Sore Sheyndel** `משוררים און פאלק` | (a) THREE items (shabse + mshorerim + folk) | **two** — `משוררים און פאלק` fused into one role |
| **1.3 Dovid's Fidele** `דאוויד גייגער` | (a) role=`דאוויד`, roleDesc=`גייגער טוביה'ס ברודער` | role=`דַאוִויד גֵייגֶער`, roleDesc=`טוביה'ס ברודער` |

Your other ten 06-21 answers matched the data and are applied. Q2.2
(`יודען סאלדאטען` → one compound `yudn_soldatn`) is now applied too — your
prose and your annotation agreed there.

---

## Part 2 — The song-boundary report (oldest open item)

On 06-14 you deferred B7 and B8:

> "A separate detailed report/log specifying the exact page boundaries and
> structural adjustments for the `(ביס)` and `זינגט` spans will be provided.
> Do not apply automated same-page backfill across the board yet."

That report hasn't reached us. **Everything about song structure is waiting on
it** — roughly 70 proposed `lg` spans across 10 plays.

We have honoured the deferral as of 2026-07-19: the `(ביס)`-triggers-song-mode
rule had been running since 06-14 (it shipped before your answer arrived) and
is now **disabled**. Only `זינגט` and `Nr.`/`געזאנגס` open a song.

☐ **2.1** Is the report still coming, or would you rather we generate a
per-page proposal for you to correct? →

---

## Part 3 — Musical directions (decided; for your information)

`(ביס)` and `רעפריין` had never been given an encoding. They were being
handled four incompatible ways (`stage type="business"`, untagged, counted as
verse lines, and once as a *speaker* — which minted a junk cast role). We
researched TEI and DraCor practice and applied a convention. **Decided by
Sinai — nothing needed from you**, but flag anything that looks wrong. Full
reasoning in `docs/proposal_musical_directions_2026-07-19.md`.

Relevant findings: TEI's `stage/@type` list is explicitly open and contains no
musical value; TEI has no repeat-mark mechanism at all; and DraCor constrains
none of this and has **no song guidance whatsoever** — a *Posse mit Gesang*
like Nestroy's *Der Talisman* has 38 bare `<lg>` and zero `stage type=`. So
there is no house style to follow; we're setting the precedent.

**Applied (132 marks corpus-wide):**
- `(ביס)` / `(ביסס)` and the pointed `(בּיס)` / `(בּיסס)` → `stage{type:repeat}`
- `(ביס 2 מאל)` — repeat with a printed count — treated as a plain repeat; the
  number is not recorded *(Sinai)*
- `רעפריין` → `head`, enclosing block becomes `<lg type="refrain">`
- voice rubrics (`סאלא אלט`, `אַלט`, `סאפראן`, `טענאר`, `באס`) → `speaker`, §G.4
- **compound `(קאהר ביס)` ×9, `(קאהר - ביסס)`, `(כער ביס)`, `(אלע ביס)`** →
  one span over the whole parenthesis, `stage{type:repeat}` ascribed with
  `@who` → `<stage type="repeat" who="#kor">`. TEI's `<stage>` carries `@who`
  via att.ascribed, so this is standards-supported. `כער` treated as a spelling
  variant of `קאהר`, not OCR *(Sinai)* — the nine genuine `(קאהר ביס)` support
  the reading.

Also decided, for the record: the mark is placed where printed (scope not
recorded via `@target`); mid-line `(ביס)` is treated the same as end-of-line;
and `סאלא אלט` is resolved by §G.4.

---

## Part 4 — A rule of yours that now conflicts with a later decision

**B9 (2026-06-14):** "if an entrance and an exit cue co-occur within the same
stage direction, this MUST be explicitly typed `stage{type:mixed}`."

**Option C (2026-06-18, you + Sinai):** compound directions get space-separated
TEI tokens — `type="entrance business"` — and `mixed` is a single-value
fallback for when the functions *can't* be enumerated.

Entrance+exit **is** enumerable, so option C implies `type="exit entrance"`
(the 06-18 doc even lists that as an example).

**Resolved 2026-07-20 (Sinai): option C wins — `type="exit entrance"`.** The
rule is updated and the one span in the corpus that is genuinely entrance+exit
(Der Mann p.13 `(יאָכטשע אָבּ, אויפטריט סאבּעלע…)`) is retyped. Nothing needed
from you.

---

## Part 5 — Untyped stage directions

~~**5.1 — the five that fell through the cracks**~~ — **absorbed into 5.2
below.** Der Mann p.18 and Mishke Mashke p.16 are typed; Der Mann p.10 and
p.14 and Al Naharot p.9 are in the 5.2 list. Blimele p.27 was `ביס` (Part 3).

**5.2 — untyped `stage` spans.** This was reported to you as 60. The real
number is **16**: the rest were duplicate scan-set copies of the same page and
stale local files, not real work. Of the 16, 8 we have typed, 5 are our own
defects to fix, and **3 need you**:

☐ **5.2a** Der Mann untern Tisch p.14 `(קלערְט אבּייסיל)` — "ponders a bit".
`business` (an action) or `delivery` (it colours the speech that follows)? →

☐ **5.2b** Di Seder p.55 `(דאָס זינגט דאָס קאָהר אזוי ווי דער פֿאָרהאַנג געהט
אויף אים ערשטען אקט)` — "the chorus sings this as the curtain rises in act 1".
A cue note about who sings and when, not stage action. `business`, or does it
want its own musical treatment? →

☐ **5.2c** Di Seder p.69 `רעפריין, א. ז. וו.` — "refrain, etc." at the end of
a verse line, i.e. *repeat the refrain here*. M5 makes `רעפריין` a `head`, but
that is for a rubric heading a block; this is an inline instruction. Tag it
`stage{type:repeat}` instead? →

Our proposals for the other 8, with reasoning, are in
`data/review/stage_type_proposals_2026-07-20.csv` — correct any you disagree
with in the `noa_correction` column.

---

## Part 6 — New unknown speaker labels

Only genuinely new ones — everything you answered on 06-28 has been applied or
is queued as a code change on our side.

| Play | Page | Line | ☐ |
|---|---|---|---|
| Isha Raa | 65 | `פון: וואָס גֶעהט דִיך אָן אַבּנר'ס טאָכטער` | → |
| Sore Sheyndel | 12 | `קשיה: פאַר וואָס רוּפט מען שבּת הגדול` | → |
| Sore Sheyndel | 16 | `הער אויס: וֶוען חנְהלֶע מיִט אִיהר מוּטער` | → |
| Yudale der Blinder | 65 | `דבורה: לאָז מִיך פאַטער, אַזוי וִויל אוֹ` | → |
| Isha Raa | 6 | `אללע: יַא יַא עֶר לעבּע לאַנְג` | → |
| Bas Sheva | 15 | `אבנר, בנימין: (לויפ'ן ענטגעגען) צוּריק!` | → |

Options: (a) coin a role · (b) variant of an existing role · (c) OCR error →
correct form · (d) collective · or "do not tag as a role".

*Our guesses, for what they're worth: `קשיה` and `הער אויס` read like rubrics
rather than speakers ("question", "listen"); `דבורה` is probably `דבורה'לע`;
`אללע` you answered before as `מעדכען (נערות)` — it recurs on Isha Raa p.6 and
we'd rather confirm than assume. `אבנר, בנימין` is the joint speaker you already
ruled on ("tag each one as a separate character") — it stays flagged only because
the comma-split emitter doesn't exist yet; no action needed from you.*

---

## Part 7 — Cast entries that never speak

This was 14 roles; **it is now 2.** Back-filling role xmlids from cast_dict let
twelve of them match their speakers, which confirms they were our bug rather
than castList errors. The remaining 2 are in Das Yudishe Kind.

☐ **7.1** Nothing needed unless you want them named — say so and we'll list
them. →

---

## Part 8 — Two small things

~~**8.1 Mishke Mashke act numbering**~~ — **resolved, nothing needed.** Act 4
is on p.16 and always was; our heading rule didn't recognise the spelling
`פיערטער`, so it went untagged. Fixed 07-20.

☐ **8.2 Meshumed `ר' יאָכטשֶעֶ`** — you asked for the exact page of the
vocalized duplicates before ruling. That's owed to you by Sinai, not the other
way round; noted here so it isn't forgotten. (Meshumed is a manuscript and now
sits in a separate track, so this is low priority.)

---

## Part 9 — Das Yudishe Kind is now annotated

49 of its 60 pages had never been through the annotation pass at all (they were
never pulled from Transkribus, so the tool couldn't see them). Done on 07-20 —
the play went from 11 annotated pages to 60.

Two speaker labels there need you. Everything else it threw up is ours to fix
(`שפּר` / `שפּרנצע` / `שפּריעצע` are OCR variants of `שפּרינצע`; `זי` and `ער`
are the duet pronouns, handled per-scene as you ruled before).

☐ **9.1** `רב זינגט:` — is the speaker `רב`, with "זינגט" a stage/song rubric?
Or is the whole thing the label? →

☐ **9.2** `זַיי:` ("they") — coin a collective for it, or is it a variant of an
existing one? →

---

## For reference

`docs/annotation_conventions.md` is new — every ratified decision in one place,
superseding the older convention files. If anything there misstates a call of
yours, say so and it gets fixed at the source.
