# Proposal — encoding `(ביס)` and `רעפריין` (musical directions)

> **Update 2026-07-21 (Sinai):** `repeat` is retired. The `(ביס)` mark is
> now typed `delivery` like the other musical performance directions. All
> `type="repeat"` below now reads `delivery`; the 136 live spans were migrated.

**Date:** 2026-07-19 · **For:** Noa (decision) · **Prepared by:** Sinai + pipeline

Two printed markers have never had an encoding decision and are currently
handled three different ways across the corpus. This proposes one convention
for each, grounded in TEI and in what DraCor actually does.

Related: §G of `castlist_tagging_conventions_2026-06-18.md` (voice rubrics,
ratified 2026-07-02) — this extends it to the two markers §G does not cover.

---

## 1. They are two different things

The data makes this clear, and it is the key to the proposal.

| | `(ביס)` | `רעפריין:` |
|---|---|---|
| What it is | a **repeat instruction** — "sing that again" | a **structural rubric** naming the block that follows |
| Where it sits | end of a sung line, or alone on its own line | line-initial, with a colon, followed by the refrain text |
| Analogy | a compositor's/composer's mark | a heading |
| Count | ~49 | ~11 |

They should **not** get the same encoding. `רעפריין` labels a verse block;
`(ביס)` instructs that something be repeated.

## 2. Current state — inconsistent, and partly wrong

60 instances across 9 plays, presently encoded four incompatible ways:

| Treatment | Where | Verdict |
|---|---|---|
| `stage type="business"` | Blimele (24), Isha Raa (8), Dovid's Fidele (4), Ezra (1) | wrong — it is not stage business |
| **untagged entirely** | Hinke Pinke (8), Sore Sheyndel (2), Dos Yudishe Herts (2), Dos Yudishe Kind (2) | gap |
| `l` (a verse line) + `lg` opener | Di Seder pp.66–67, 69 | wrong — the rubric is counted as sung text |
| `speaker xmlid:refrn` | Dos Yudishe Herts p.55 | wrong — and it minted a junk cast role `refrn` |

That last one is the same defect as `etts` (`עטצ.`/"etc." coined as a role in
Bas Sheva): a non-character label promoted to a cast entry.

## 3. What TEI and DraCor actually say

Verified against primary sources (see footnotes).

- **`<stage>/@type` is an OPEN list.** The Guidelines state plainly: *"No closed
  set of values for the type attribute is therefore proposed at the present
  time, though some suggested values are indicated in the list below."* The nine
  suggested values are `setting, entrance, exit, business, novelistic, delivery,
  modifier, location, mixed`. **None is musical.** Adding a value is sanctioned,
  not a hack. [1]
- **`<lg>` is defined as grouping "a stanza, refrain, verse paragraph, etc."** —
  so a refrain is a canonical `<lg>`. But `<lg>/@type` has **no** suggested-value
  list anywhere in the Guidelines, so `type="refrain"` is legal and idiomatic
  **convention, not a named TEI recommendation**. [2]
- **TEI has no mechanism for a printed repeat mark.** No `<seg type="delivery">`
  precedent, nothing in the Primary Sources chapter. The closest fit is
  `<metamark>`: *"contains or describes any kind of graphic or written signal
  within a document the function of which is to determine how it should be read
  rather than forming part of the actual content."* That describes `(ביס)`
  exactly — but `function="repeat"` would be a coinage, and `<metamark>` belongs
  to the manuscript/genetic-transcription tradition. [3]
- **DraCor imposes nothing.** Its ODD has no `<elementSpec>` for `stage` at all,
  so `@type` is unconstrained; the strings `song`, `refrain`, `delivery` and
  `metamark` appear nowhere in it. Checked a *Posse mit Gesang* — Nestroy's *Der
  Talisman* — where all 38 `<lg>` are bare and there are **zero** `stage type=`
  attributes: DraCor does not distinguish sung from spoken text at all. [4]

**So: nothing we choose conflicts with DraCor, and there is no house style to
follow. On this point we are the precedent, not a follower.** No Yiddish or
Hebrew drama TEI precedent for ביס exists either.

## 4. Recommendation

### `(ביס)` → `<stage type="delivery">`

```xml
<lg type="refrain">
  <l>אַז דאָס קרִיעגֶעלֶע זאָל זַיין פִיל</l>
  <l>יאַ פִיל!</l>
</lg>
<stage type="delivery">ביס</stage>
```

**Why this over `<metamark function="repeat">`,** which is the better fit on pure
TEI semantics: the decisive constraint is the **annotation surface**. Noa and the
RAs tag in Transkribus, where a span is a custom tag from our tagset. `stage`
with a new `@type` token is a one-word addition to `STAGE_TOKENS`; `metamark`
would mean a new tag in the Transkribus tagset *and* in `schema.ALLOWED_TAGS`,
for a construct no drama tooling renders. Practical wins here.

*Honest cost:* stage-direction word counts get ~49 tokens of noise, and DraCor's
API computes `play_num_of_word_tokens_in_stage`. Negligible at this scale, and we
control our own build — but it is a real, checkable side effect, not nothing.

### `רעפריין:` → `head` span, and the block becomes `<lg type="refrain">`

```xml
<lg type="refrain">
  <head>רעפריין</head>
  <l>יוּדאלע נישט האָבּ קיין מורה</l>
  …
</lg>
```

`head` is **already in our tagset and already carries `lg_id`** — `annotate_songs`
emits exactly this shape for song headings. So this needs no new vocabulary at
all: it reclassifies `רעפריין` from `l`/`speaker`/`stage` to `head`, and the TEI
builder stamps `type="refrain"` on the enclosing `<lg>`.

This also removes the junk `refrn` cast role.

### Bonus — `סאלא אלט` is already answered

Noa left this blank on 06-28, but **§G.4 was ratified on 07-02, after she
answered**, and it decides the case: a solo *voice* rubric is a speaker
attribution, not a stage direction —
`<sp><speaker>סאלא אלט</speaker><lg>…</lg></sp>`, with `@who` resolved to the
named singer if identifiable, otherwise pointing at an abstract
`<person xml:id="alt">` in `particDesc`. Not added to the printed castList.

Distinguish it from `רעפריין`: **`סאלא אלט` says *who sings*; `רעפריין` says
*which part of the song it is*.** Different layers, different encodings.

## 5. Questions for Noa

1. **`(ביס)` → `stage type="delivery")`** — agreed? Or do you want the
   semantically-purer `<metamark function="repeat">` despite the workflow cost?
2. **`רעפריין:` → `head` inside `<lg type="refrain">`** — agreed?
3. **Scope of the repeat.** `(ביס)` sometimes ends a single line and sometimes
   stands alone after a block. Should we record *what* repeats (via `@target`
   pointing at the line or lg), or just place the mark where it is printed?
   Recording scope is more useful and more work; placing it is faithful and
   cheap. **Recommend: place it now, add `@target` later if wanted** — the
   information is not lost either way.
4. **`(ביס)` inside a line** (e.g. Hinke Pinke p.38 `אוֹי, היום היום (ביס) תּברכנוּ`,
   Dovid's Fidele p.38) — mid-line marks presumably repeat only the preceding
   phrase. Same treatment, or flag these separately for your song report?

## 6. If approved — implementation

1. `schema.py`: add `delivery` to `STAGE_TOKENS`.
2. `annotate_songs.py`: recognise `רעפריין` as a `head` (currently untreated);
   stop `(ביס)` from being typed `business`.
3. `auto_resolve_flags`: retype the 37 existing `stage type="business"` ביס spans
   → `delivery`; tag the 14 currently-untagged ones.
4. `build_tei.py`: stamp `type="refrain"` on an `<lg>` whose `head` is `רעפריין`.
5. Drop the junk `refrn` role from `DosYudisheHerts-1910/cast_dict.json`
   (and `etts` from `BasSheva` while we're there — same defect).
6. Re-run `check_who.py` on affected editions.

**Note:** this touches only the *marker* encoding. It does **not** decide song
*boundaries* — B7/B8 remain deferred pending Noa's page-boundary report, and the
`(ביס)`-triggers-song-mode rule stays under that deferral.

---

[1] TEI P5 ch. 7.2.5 Stage Directions — https://www.tei-c.org/release/doc/tei-p5-doc/en/html/DR.html · https://tei-c.org/release/doc/tei-p5-doc/en/html/ref-stage.html
[2] https://tei-c.org/release/doc/tei-p5-doc/en/html/ref-lg.html · ch. 6 Verse https://www.tei-c.org/release/doc/tei-p5-doc/en/html/VE.html
[3] https://tei-c.org/release/doc/tei-p5-doc/en/html/ref-metamark.html · ch. 12 https://www.tei-c.org/release/doc/tei-p5-doc/en/html/PH.html
[4] https://github.com/dracor-org/dracor-schema · https://dracor.org/doc/odd · https://dracor.org/api/v1/corpora/ger/plays/nestroy-der-talisman/tei
