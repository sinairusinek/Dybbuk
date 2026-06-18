# YiDraCor — Two questions for Noa, 2026-06-18 (follow-up)

Two separate items left after the 2026-06-14 round. Independent — answer whichever you want first.

---

## 1. Yudale castList: name vs. roleDesc on three printed lines

We need to decide, line by line, where the proper-name ends and where the role-description begins. The choice determines:

- the `xml:id` of the character (one xmlid per role; from the bare proper name only),
- the `bare` string in `cast_dict.json` (drives speaker matching on body pages — any body-page utterance that begins with this bare string is routed to this xmlid),
- the `roleDesc` span (rendered as the descriptive label in the printed castList, never matched against body lines).

Three printed lines on the Yudale castList page (`page_annotated/0004_OTgwNjYwMDE.111007892.xml`):

### 1a. Line 65 — Yudale, the nephew, (blind)

> `יוּדאלע זיין נעפפע (בּלינד).`

Options (please pick one):

- **(a)** `role = יוּדאלע` ; `roleDesc = זיין נעפפע (בּלינד)` — the parenthetical is part of the descriptive label and travels with it on the page.
- **(b)** `role = יוּדאלע` ; `roleDesc = זיין נעפפע` ; `(בּלינד)` is encoded separately as a `stage{type:business}` standalone parenthetical.
- **(c)** Something else — please specify.

The downstream impact: in (a) the page reader sees "his nephew (blind)" as one phrase under Yudale; in (b) the "(blind)" hangs as a standalone parenthetical on the castList page.

### 1b. Line 107 — Iser the moneylender

> `איסר פּראָצענטניק.`

Note the *period* at the end — this is a standalone line on the castList, not a continuation.

Options:

- **(a)** `role = איסר` ; `roleDesc = פּראָצענטניק` — `איסר` is the proper name, `פּראָצענטניק` ("moneylender") is the descriptive title.
- **(b)** `role = איסר פּראָצענטניק` (no roleDesc) — Iser-the-Moneylender is treated as a compound proper name (so body-page speakers would only resolve if they say "Iser Procentnik" in full, not bare "Iser").
- **(c)** `form = איסר פּראָצענטניק` ; `bare = איסר` (compound surface, but bare speaker matching only on "Iser") — middle ground; bare speakers route, surface preserves the trade name.

### 1c. Line 114 — Professor Edelman, the eye doctor

> `פּראפעסאר עדעלמאן (אויגען דאָקטער).`

Options:

- **(a)** `role = פּראפעסאר עדעלמאן` ; `roleDesc = (אויגען דאָקטער)` — "Professor Edelman" is the proper name (with title attached, like "Mr. Smith"), the parenthetical eye-doctor is the descriptive label.
- **(b)** `role = עדעלמאן` ; `roleDesc = פּראפעסאר אויגען דאָקטער` — strip "Professor" out of the proper name; both the academic title and the medical title go into roleDesc.
- **(c)** `role = פּראפעסאר עדעלמאן (אויגען דאָקטער)` — keep the whole printed string as the role name.

The cross-cutting question, if you want to answer it at the pattern level instead of per-line:

> **In our castList encoding, is a profession/relation-modifier ("nephew", "moneylender", "eye doctor", "his wife") considered (i) part of `roleDesc` always, or (ii) part of `role` when it's a title or trade name and `roleDesc` only when it's a parenthetical aside?**

A pattern-level answer would settle 1a–1c (and several similar lines in other plays) in one shot.

---

## 2. Ezra p.4 stage-direction type — `mixed` or `entrance`?

Two things from the 2026-06-14 round are in tension and we need your call.

**What you RA-edited on 2026-06-14:** Ezra page 4, the stage direction
> `(לעגט וועג דיא האַרפֿע— ערשיינט)`
> "lays the harp aside — appears"

You retyped this from `business` → `mixed`. That edit established what we've been calling **rule B2**: when an entrance verb (`ערשיינט`) co-occurs with any other action verb in the same direction, the direction is `mixed`.

**What you wrote in chat on the same day** (Q B9):

> "Keep the current behavior for regular action/emotion combinations (the pipeline picks the first dominant function). However, as a strict rule: if an entrance and an exit cue co-occur within the same stage direction (e.g., 'Miriam exits and Shlomo enters'), this MUST be explicitly typed as `stage{type:mixed}`."

Reading this strictly: `mixed` is reserved for **entrance + exit** co-occurrence only. Under that reading, the Ezra p.4 line above should be `entrance` (the harp-aside is incidental business; the dominant action is appearing on stage), not `mixed`.

**Concrete impact across the corpus.** All 7 `type:mixed` tags currently in the codebase are in DerManUnterTiff:

| Page | Line text | Current | Under "entrance+exit only" |
|---|---|---|---|
| Ezra p.4 | `(לעגט וועג דיא האַרפֿע— ערשיינט)` | mixed | entrance |
| DerMan p.11 | `(זיי נעהמען זיך אַרוּם, יאכֿטשע ערשיינט)` — they embrace, Yakhtshe appears | mixed | entrance |
| DerMan p.11 | `(ער בעהאַלט זיך. אויפֿטריט סאבעלע)` — he hides, Sabele enters | mixed | entrance |
| DerMan p.18 | 4× multi-line stage paragraphs about dance/song (no entrance/exit cues) | mixed | business |
| DerMan p.18 | `פאָרהאַנג פאַלט.` — curtain falls | mixed | setting |

**The two clean options:**

- **(A) Keep B2 as you RA-edited it.** `mixed` fires for entrance + any other action verb (current behavior). B9's entrance+exit rule is ADDITIVE — also fires `mixed`. Nothing in the corpus changes; Ezra p.4 stays `mixed`. Interpret your chat answer as "keep the current behavior" *including* the entrance+business cases.
- **(B) Narrow `mixed` to entrance + exit co-occurrence only.** Ezra p.4 becomes `entrance`; the 7 corpus `mixed` tags get retyped accordingly. Your chat answer's "strict rule" takes precedence over the same-day RA edit.

We can't sit on the fence — the code currently implements B2 (option A), and we want to either confirm that or switch to B9 (option B).

Which is your real position?

---

*Background context for both questions: the 2026-06-14 castlist handoff had its DerMan and Meshumed sections content-swapped; you spotted that and added `[Note: Swapped/Corrected Title]` to your replies. Both files now have a correction header. Meshumed itself is being moved to a separate handwritten-plays track and won't be processed alongside the printed Lateiner corpus.*
