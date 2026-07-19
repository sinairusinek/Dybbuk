# YiDraCor — consolidated handoff for Noa, 2026-07-19

Everything still open across **all 15 printed plays**, in one place. Previous
rounds (06-14, 06-18, 06-24, 06-28) are closed or absorbed here.

**Reply format:** write your answer next to each item. Sinai batch-applies.

Two notes before you start:

1. **We lost some of your answers for a while, and that's on us.** Your
   2026-06-21 castList answers sat in the Google Doc and were never copied back
   into the repo, so they read as unanswered for four weeks. And on 06-24 a
   pipeline push buried your castList annotations on Bas Sheva p.6 and Dovid's
   Fidele p.6 about 35 minutes after you made them. **Both are now recovered
   and applied** — nothing of yours was lost, and guards are in place so it
   can't recur. Sorry for the noise.
2. **This list is deliberately short.** A first pass produced 1,742 flags for
   you; ~1,630 of those turned out to be gaps in our own tag vocabulary
   (`unclear`, `actor` — *your* tags — and Transkribus-native spans lint-ing as
   errors). Fixed. What follows is the genuine residue.

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

**Applied (68 instances corpus-wide):**
- `(ביס)` / `(ביסס)` → `stage{type:repeat}`
- `רעפריין` → `head`, enclosing block becomes `<lg type="refrain">`
- voice rubrics (`סאלא אלט`, `אַלט`, `סאפראן`) → `speaker`, per §G.4

☐ **3.1** `(ביס)` → `stage{type:repeat}` — agreed? →
☐ **3.2** `רעפריין` → `head` in `<lg type="refrain">` — agreed? →
☐ **3.3** **`(כער ביס)`**, Hinke Pinke p.63:
`חִינְקֶע! קוּם צוּ דַיין פּנקען. א. ז. וו. (כער ביס)`. We think `כער` is an OCR
error for `כאר` (chorus), making this a compound voice-rubric + repeat — so we
left it alone rather than guess. Is that right, and how should it be tagged? →

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

**5.2 — 63 bare `stage` spans across the corpus** have no `@type` at all.
Concentrated in **Al Naharot Bavel (20)** and **Kidush Hashem (15)**, then
Di Seder (6), Ezra (5), Mishke Mashke (5), Der Mann (4), Hinke Pinke (3),
Dovid's Fidele (2), Bas Sheva / Blimele / Sore Sheyndel (1 each).

Most should be rule-resolvable. ☐ **Shall we auto-type what the lexicon can
reach and send you only the residue?** →

Full list: `data/review/lint_2026-07-19.csv`.

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

Options: (a) coin a role · (b) variant of an existing role · (c) OCR error →
correct form · (d) collective · or "do not tag as a role".

*Our guesses, for what they're worth: `קשיה` and `הער אויס` read like rubrics
rather than speakers ("question", "listen"); `דבורה` is probably `דבורה'לע`.*

---

## Part 7 — Cast entries that never speak

14 roles are declared in cast_dict but never used by any speaker span: **Bas
Sheva ×5, Hinke Pinke ×4, Dos Yudishe Herts, Dovid's Fidele, Sore Sheyndel,
Das Yudishe Kind ×2**.

Each is one of: a genuinely silent role (fine — keep), a speaker label we're
failing to match (our bug), or a mis-read castList entry.

☐ **7.1** Want the itemised list to check, or shall we investigate first and
only bring you the ones that look like real castList errors? →

---

## Part 8 — Two small things

☐ **8.1 Mishke Mashke act numbering** — acts run 1, 2, 3, **5**. Is act 4
missing from the print, mislabelled, or did we mis-tag a heading? →

☐ **8.2 Meshumed `ר' יאָכטשֶעֶ`** — you asked for the exact page of the
vocalized duplicates before ruling. That's owed to you by Sinai, not the other
way round; noted here so it isn't forgotten. (Meshumed is a manuscript and now
sits in a separate track, so this is low priority.)

---

## For reference

`docs/annotation_conventions.md` is new — every ratified decision in one place,
superseding the older convention files. If anything there misstates a call of
yours, say so and it gets fixed at the source.
