# YiDraCor — consolidated handoff for Noa, 2026-07-20

Everything still open across **all 15 printed plays**, in one place. Previous
rounds (06-14, 06-18, 06-24, 06-28) are closed or absorbed here.

**Reply format:** write your answer next to each item. Sinai batch-applies.

This list is only what no rule can decide. If you ever find your own
annotation missing, tell us — we can recover it from the layer history.

---

## Part 1 — Where your two answers disagree with each other

For these three you answered in prose on 06-21, then tagged the castList by
hand in Transkribus on 06-24 and tagged it **differently**. The annotation is
later and you were looking at the image, so it may well supersede — but we
won't overwrite your written call on a guess. **Which stands?**

| | your 06-21 prose | your 06-24 annotation |
|---|---|---|
| **1.1 Bas Sheva** `שעפער שעפעריגען` | (a) ONE collective `shefer` | **two** separate role spans |
| **1.2 Sore Sheyndel** `משוררים און פאלק` | (a) THREE items (shabse + mshorerim + folk) | **two** — `משוררים און פאלק` fused into one role |
| **1.3 Dovid's Fidele** `דאוויד גייגער` | (a) role=`דאוויד`, roleDesc=`גייגער טוביה'ס ברודער` | role=`דַאוִויד גֵייגֶער`, roleDesc=`טוביה'ס ברודער` |

☐ 1.1 → ☐ 1.2 → ☐ 1.3 →

*(1.3 is the consequential one — `dvid_geyger` is referenced on 18 body pages.)*

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

## Part 3 — Musical directions: please ratify

`(ביס)` and `רעפריין` had never been given an encoding. They were being
handled four incompatible ways (`stage type="business"`, untagged, counted as
verse lines, and once as a *speaker* — which minted a junk cast role). We
researched TEI and DraCor practice and applied a convention; **it needs your
sign-off.** Full reasoning in `docs/proposal_musical_directions_2026-07-19.md`.

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

☐ **3.1** `(ביס)` → `stage{type:repeat}` — agreed? →
☐ **3.2** `רעפריין` → `head` in `<lg type="refrain">` — agreed? →
☐ **3.3** Compound `(קאהר ביס)` → `<stage type="repeat" who="#kor">` — agreed?
Or would you rather the chorus rubric were a separate span from the repeat
mark? →

*Decided by Sinai, no action needed from you: the mark is placed where printed
(scope not recorded via `@target`); mid-line `(ביס)` is treated the same as
end-of-line; and `סאלא אלט` — which you left blank on 06-28 — is resolved by
§G.4, ratified 07-02 after you answered.*

---

## Part 4 — A rule of yours that now conflicts with a later decision

**B9 (2026-06-14):** "if an entrance and an exit cue co-occur within the same
stage direction, this MUST be explicitly typed `stage{type:mixed}`."

**Option C (2026-06-18, you + Sinai):** compound directions get space-separated
TEI tokens — `type="entrance business"` — and `mixed` is a single-value
fallback for when the functions *can't* be enumerated.

Entrance+exit **is** enumerable, so option C implies `type="exit entrance"`
(the 06-18 doc even lists that as an example). The two rules collide and this
was never re-asked.

☐ **4.1** Entrance + exit in one direction → `type="exit entrance"` (option C),
or `type="mixed"` (B9)? →

---

## Part 5 — Untyped stage directions

**5.1 — the five that fell through the cracks.** These were asked on 06-24,
never answered, and dropped out of the 06-28 document. Choose:
`setting / entrance / exit / business / delivery / mixed` (or multi-token).

| Play | Page | ☐ |
|---|---|---|
| Der Mann untern Tisch | 10 | → |
| Der Mann untern Tisch | 14 | → |
| Der Mann untern Tisch | 18 | → |
| Al Naharot Bavel | 9 | → |
| Mishke Mashke | 16 | → |

*(Blimele p.27 was the sixth — it was `ביס`, now handled by Part 3.)*

**5.2 — 60 bare `stage` spans across the corpus** have no `@type` at all, and
no lexicon cue the rules can act on (the ones that *did* have a cue —
`(פארהאנג)`, `פערוואנדלונג` — are now typed `setting` automatically).

Al Naharot Bavel **20** · Kidush Hashem **15** · Di Seder **6** ·
Mishke Mashke **5** · Ezra **4** · Der Mann **3** · Dovid's Fidele **3** ·
Das Yudishe Kind **2** · Hinke Pinke / Sore Sheyndel **1** each.

These are the residue *after* auto-typing everything the lexicon could reach.
☐ **Shall we run an LLM pass over them and send you its proposals to correct,
rather than have you type all 60 from scratch?** →

Full list: `data/review/lint_flags_2026-07-20.csv`.

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
