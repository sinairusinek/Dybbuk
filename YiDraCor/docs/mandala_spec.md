# The Lateiner–Hurwitz Mandala — specification

**Status:** draft v0, built 2026-10-06. Live as a private artifact at
<https://claude.ai/code/artifact/cba3da8e-6481-4f9f-8e1a-c3ba22fb6733>.
**Destination:** `docs/Visualizations/` on the GitHub Pages site (main:/docs), linked
from `gate.html`.

**This file is where Sinai writes the spec.** Sections marked _"Sinai:"_ are yours to
fill — everything under **What is built today** is a description of the current state,
so you can write against something concrete rather than from scratch.

---

## 1. What is built today

### Data source

Everything comes from `YiDraCor/data/entity_graph.json` (692 nodes, 887 edges).
Nothing is hand-maintained. Counts as built: 277 works, 27 editions, 64 events,
225 songs, 37 persons, 50 orgs, 12 places.

### Layout

| ring | radius | content |
|---|---|---|
| centre | — | two portrait medallions, Lateiner (right half) and Hurwitz (left) |
| inner band | 352 | the **217 plays known only as a catalogue title**, drawn as radial ticks |
| outer wheel | 462 | the **60 plays with a surviving edition, staging or song**, drawn as discs |
| traces | 566 | gold diamonds = editions, open rings = performance events |

The two-lane split is the main design decision and the one most worth your judgement.
A single flat ring gave the well-documented plays a 5.7 px slot for a 30 px dot and
they collided into a smear. Splitting them also states something true: roughly
four-fifths of the repertoire survives as a name only.

### Encoding

- **Colour** = genre family, normalised from the free-text `tags` field into ten
  families (biblical, historical, operetta, gezang-drama, Tsaytbild/Lebnsbild,
  melodrama, drama, comedy, other, not catalogued). `genre` is 86 % empty and is used
  only as a fallback. Compound values (`"historical operetta; operetta"`) resolve to a
  single family by priority order.
- **Size** = `editions × 3 + events × 2 + min(songs, 14) × 0.55`. Weighted because an
  edition is a far rarer trace than a song title; on events + editions alone some 230
  plays would be identical minimum dots.
- **Opacity** — a play whose attribution is `false ascription` is drawn translucent.

### Interaction

Click a play → the wheel dims to ~5 %, the play lifts to a hub, an inner ring shows its
credits (author in gold, persons as circles, companies and venues as squares) and an
outer ring its stagings and editions; a dossier panel opens with the full lists.
Escape or a click on the canvas returns. Legend swatches isolate one genre. Drag to
pan, scroll to zoom.

### Portraits

Real, from the Zylbercweig *Leksikon* via Transkribus collection 227902 — Lateiner
from doc `807124` (vol. 2 p. 90), Hurwitz from doc `807402` (vol. 1 p. 312). Files and
exact crop boxes are recorded in `DybbukMedia/data/media_manifest.tsv` rows
`lexicon-0025` and `lexicon-0024`; images in `DybbukMedia/media/lexicon/`.
Both rows are still `PROPOSED`/`PENDING` — **not reviewed**.

---

## 2. Open questions for Sinai

_Sinai: answer inline, delete what you don't care about._

1. **Audience and venue.** Is this a research instrument for the team, or a public
   exhibit on the Pages site? That changes how much apparatus (counts, provenance,
   caveats) belongs on screen.
2. **Is the two-lane split right?** Alternatives: one ring with the title-only plays
   omitted entirely; one ring with size on a log scale; or lanes by date rather than
   by evidence.
3. **Songs.** 225 of them, currently only in the dossier. Should they be a visible
   tier, a filter, or stay where they are?
4. **Ordering within each author's arc.** Currently clustered by genre family, then by
   weight. Chronology is the obvious alternative but most works carry no date.
5. **Attribution.** `false ascription` works are shown translucent and still counted in
   the 277. Should they be excluded, or marked more explicitly?
6. **Hurwitz vs Lateiner asymmetry.** 152 vs 125 plays. Anything to say visually about
   the rivalry, or keep it neutral?

---

## 3. Sinai's specification

_Write freely here — prose is fine, I'll turn it into work items._

### 3.1 Must change

-

### 3.2 Would like

-

### 3.3 Explicitly not wanted

-

---

## 4. Known gaps and technical debt

- **The generator is not yet in the repo.** The page is currently assembled in a
  session scratchpad from `head.html` + `body.html` + an inlined `mandala.json` +
  `app.js`. Moving to Pages requires a committed script under `YiDraCor/code/`
  that writes into `docs/Visualizations/`. Note the two traps recorded in memory:
  all visualizations live in **one** directory so relative cross-links work, and
  generators have previously written to the wrong `docs/` path — an empty
  `git status` after a run means the path is wrong.
- **No date axis.** Most works carry no year, so chronology is not currently encodable.
- **Genre normalisation is lossy.** Ten families from free text; the mapping is in the
  build script and should arguably live in a reviewable file.
- **Portrait provenance is unreviewed.** See above — the two manifest rows need a
  `reviewer` stamp before anything downstream treats them as fact.
- **Only two portraits exist.** A general per-person portrait pipeline is deferred;
  the Album's 1 389 detected faces are tagged by caption order at 0.6 confidence and
  none is tied to either playwright.

---

## 5. Changelog

| date | change |
|---|---|
| 2026-10-06 | First build. Two-lane layout, genre colour, hub view, dossier. Portraits added from Transkribus. |
