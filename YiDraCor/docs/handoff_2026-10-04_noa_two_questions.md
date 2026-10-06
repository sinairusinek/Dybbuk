# Two last questions — Emigration's Bilder, and one speaker label

**For Noa. 2026-10-04.**

These are the only two things still open on the manuscript track. Everything
else from the three earlier rounds is answered, applied, and live in
Transkribus — and as of today **all nine manuscript plays build as TEI
editions** with their act and Bild divisions in place (Judith settled those in
August). Each question below is self-contained: you should not need any of the
earlier documents to answer it.

---

## Question 1 — Emigration nach America: where are the 9 Bilder?

### The situation

Emigration's title page
([p.3](https://app.transkribus.org/collection/2372172/doc/1013131/detail/3))
declares the play to be

> `געזאנג דראממא אין 5 אקטען אין 9 בילדער`
> *a song-drama in 5 acts in 9 Bilder*

A **Bild** (tableau) is a scene division inside an act. In the TEI they become
`<div type="scene">` nested inside the act, so we need to know where each one
begins.

**The 5 acts are certain** — each has its own written heading:

| act | page |
| :-- | :-- |
| Act 1 | [p.5](https://app.transkribus.org/collection/2372172/doc/1013131/detail/5) |
| Act 2 | [p.26](https://app.transkribus.org/collection/2372172/doc/1013131/detail/26) |
| Act 3 | [p.48](https://app.transkribus.org/collection/2372172/doc/1013131/detail/48) |
| Act 4 | [p.64](https://app.transkribus.org/collection/2372172/doc/1013131/detail/64) |
| Act 5 | [p.94](https://app.transkribus.org/collection/2372172/doc/1013131/detail/94) |

**The 9 Bilder are the problem.** When we asked Judith in August where they
begin, her answer was: *"two more Bilder: p. 42, p.80"* — "two more" meaning in
addition to the scene-change lines we had already spotted at p.30 and p.58.

### What we found when we went to tag them

We searched every line of all 117 pages for any Bild-ish word. The notebook
contains exactly **three** scene-change markers, all reading `פערוואנדלונג`
("transformation" — the standard stage term for a scene change):

| page | the line as written |
| :-- | :-- |
| [p.30](https://app.transkribus.org/collection/2372172/doc/1013131/detail/30) | `פערוואנדלונג (ארם צוממער)` — *transformation (poor room)* |
| [p.58](https://app.transkribus.org/collection/2372172/doc/1013131/detail/58) | `N xx (פֿערוואנדלונג (פינסטערער קעלער` — *(transformation (dark cellar* |
| [p.80](https://app.transkribus.org/collection/2372172/doc/1013131/detail/80) | `פערוואנדלונג (העסטער סטריט)` — *transformation (Hester Street)* |

**p.80 is confirmed** and is now tagged.

**But [p.42](https://app.transkribus.org/collection/2372172/doc/1013131/detail/42)
has no marker of any kind.** That page is ordinary mid-scene dialogue — Sanye
and Yekel arguing about when to hold their wedding
(`אין אויף ווען זאללען מיר אבלעגען דיא חתינה!`) — with only the Regie cues
`I Ret` and `II Ret` in the margins, which are the copyist's musical-number
brackets, not scene divisions.

Everywhere else the root *bild* appears in this notebook it is the ordinary
Yiddish word inside spoken dialogue, not a stage division:

- `איינבילדונג` — *imagination* (pp.70, 78, 98)
- `טרוימבילד` — *dream-image* (p.98)
- `דאס בילד פאן מאריצען` — *the picture of Moritz* (p.102)

**So: three markers written, nine promised on the title page.**

### What we need

Tick whichever applies, or write in an answer.

- [ ] **p.42 was a slip** — the marker is really on page ______
      *(if you can see one we missed on p.42, quote the line and we will tag it)*
- [ ] **The other six Bilder are simply not written in the manuscript.**
      This is a perfectly good answer — the title page may be advertising a
      staging the copyist never notated. We would record "declared 9, marks 3"
      and move on.
- [ ] **The six should be inferred** from setting changes in the stage
      directions rather than from explicit markers. *(If so, we would need a
      pass over the stage directions — say the word and we will prepare one.)*
- [ ] Something else: _______________________________________________

**Comment:**

*(p.42 is left untagged for now rather than guessed at, so nothing is wrong in
the data while this is open.)*

---

## Question 2 — Yoysef in Egipten p.19: is `נאר` a character?

### The situation

This one is **not** about act structure — it is a speaker label, and it is the
**single last unresolved label** out of the 386 we started with in August.

It has been parked twice already: on the first questionnaire the answer was
*לא יודעת כרגע* ("I don't know at the moment"), and on the follow-up round the
box ticked was *"still unsure — leave it flagged."* So this is a genuine
puzzle, not an oversight — but it is now the only thing left, so it is worth
one more look.

### The evidence

On [p.19](https://app.transkribus.org/collection/2372172/doc/826832/detail/19)
**three** lines on the same page say almost the same thing — *"must die on the
spot"* — and the pipeline tagged a speaker on two of them:

| line id | tagged as a speaker? | the text as written |
| :-- | :-- | :-- |
| r2l2 | **no** — no speaker span at all | `מיז שטערבען אויף דעם ארט` |
| r2l6 | yes, `סע` | `סע מוז שטערבען אויף דעם ארט.` |
| r2l7 | yes, `נאר` | `נאר מוז שטערבען אויף דעם אַָרט` |

Two things follow from this.

First, on the earlier questionnaire **`סע` was marked "not a speaker —
mis-tagged"**, which is clearly right: `סע` is just the pronoun *"it"*.

Second — and this is new since we last asked — **the first of the three lines
carries no speaker tag at all**, even though it is the same sentence. So what
decided whether a prefix got tagged here was simply whether a short word
happened to lead the line, not whether anyone is named.

### Our reading

`נאר` is the ordinary Yiddish adverb *"only / but"*. All three lines look like
fragments of one speech by whoever is actually talking, and in two of them the
leading word was mistaken for a speech prefix. If that is right, `נאר` should
be dropped exactly as `סע` was, and r2l2 shows what the untagged version
correctly looks like.

(The stage direction `פארהאנג` — "curtain" — follows two lines later, so this
is the end of a scene, which fits a single closing speech rather than two or
three one-word characters.)

### What we need

- [ ] **Not a speaker either** — drop it, the same as `סע`. *(our recommendation)*
- [ ] **It is a character**, and the name is: _______________________
- [ ] **Leave it flagged** — this is fine; it blocks nothing, it is one span.

**Comment:**

---

## For context: what these two questions are holding up

Nothing urgent. Both are small and independent:

- **Emigration** builds fine as TEI today with its 5 acts and the 3 Bilder we
  can see. Answering Q1 only refines its scene divisions.
- **`נאר`** is one span in one play and blocks nothing at all.

Every other speaker label, cast-list entry, act division and Bild across the
nine notebooks is now resolved and encoded. Thank you — this was 386 spans and
121 labels when we started.
