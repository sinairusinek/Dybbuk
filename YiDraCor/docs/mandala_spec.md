# The Lateiner–Hurwitz Mandala — specification

**Status:** draft v1, built 2026-10-06. **On the Pages site** at
`docs/Visualizations/lateiner_hurwitz_mandala.html`, linked from `gate.html`.
Also live as a private artifact at
<https://claude.ai/code/artifact/cba3da8e-6481-4f9f-8e1a-c3ba22fb6733>.

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
| the band | 286 / 303 / 320 | **all 277 plays, one continuous band** on three sub-lanes, each on a line to its author. Documented and title-only plays sit side by side as one repertoire; a play's lane is its position in the arc, never its evidence |
| traces | 430 / 460 | one icon per surviving trace, on a line back to *its play* — **parting curtain** = performance event, **book** = printed edition, **notebook** = manuscript |

Lane assignment is a greedy packing rather than `i % 3`: a well-documented play needs
more arc than a 4 px title dot, so each play takes whichever lane has room at its
angle. Traces are laid out in one pass over all plays in angular order, so that
neighbouring plays' traces cannot pile into the same arc.

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

Clicking routes on what survives of the play (spec §3 A/B):

- **A play with no traces** → a side panel: its date of composition stated as precisely
  as the record allows (a year, a range, before/after, or "Date of composition
  unknown"), and the reference works that attest it.
- **A play with traces** → the play's collaborators and traces fan **outward**, away
  from the centre, continuing the direction the play already points, so the reading
  stays radial instead of doubling back over the band. The playwright is deliberately
  *not* drawn — he is already the central medallion the play is spoked to.
  **Every node in that fan is clickable**, and a click fills the side panel with that
  node's own record (a person's roles, an edition's printer and holding library, an
  event's date and venue) **without disturbing the drawing** — the viewer keeps their
  place. Only a click on bare canvas, or Escape, returns to the wheel.

Icons in that view: notebook = manuscript, book = print edition, parting curtains =
performance event, male/female bust = actor/actress, musical notes = composer, lyre =
lyricist, baton = arranger, dancing figure = choreographer, theatre front = venue,
press = publisher/printer, colonnade = holding library, hand = former owner.

Escape or a click on the canvas returns. Legend swatches isolate one genre. Drag to
pan, scroll to zoom.

### Dates and attestations

`Year of Composition (C)/Publication (PB)/Performance (PF)` is parsed into a structured
reading: bare years, ranges (`1877-78`), uncertainty (`1877?`), before/after, explicit
C/PB/PF tags, and a parenthetical kept separately as provenance ("acc. Gorin"). 110 of
277 works carry a date; none fail to parse. Note the openpyxl float artifact — `1890.0`
must be cut at the decimal, never `rstrip('.0')`, which would yield 189.

Attestations come from the presence columns JPRESS (102), Zylbercweig's *Leksikon* (82),
Berkovitsh (58) and Sieger (24).

**A quirk of the data worth knowing:** no title-only play has *both* a date and an
attestation — the two facts come from different source sheets and never co-occur. 90 of
the 217 have a date, 98 have attestations, and the two sets do not overlap.

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
A. two-lane split: the inner circle with the play titles should show colored circles rather than tics, and each circle should have a line connecting to the author. the second tier/circle should be of the 'traces' of plays, with a footprint icon, connect with a line not to the author but to the title of the play in the inner circle. 
The panel that opens when clicking on a circle in the inner circle that has no traces will include the title, date of composition if known or "date of composition unknown" if not, enable also range or after and before dates if they are the only ones known. write the attestations for these plays - where were they recorded? 
B. For plays that do have traces, instead of opening a side panel, center on them, and highlight all the linked persons. Notice not to include an additional playwright node for Lateiner/Hurwitz which is already there as the central author node - this is duplication. In this view use notebook icon for manuscripts, book icons for print editions, opening-theatre-curtain-like icon for performance events, male/female portrait icons for actors, and find an appropriate icon for other roles related to the editions or events. 

_Write freely here — prose is fine, I'll turn it into work items._

### 3.1 Must change

-

### 3.2 Would like

-

### 3.3 Explicitly not wanted

-

---

## 4. Known gaps and technical debt

- **The generator is still not in the repo.** `docs/Visualizations/lateiner_hurwitz_mandala.html`
  is committed and served, but it was assembled by hand in a session scratchpad from
  `head.html` + `body.html` + an inlined `mandala.json` + `app.js`. **Nobody but this
  session can currently rebuild it.** The fix is a committed script under
  `YiDraCor/code/` that reads `data/entity_graph.json` and writes the page into
  `docs/Visualizations/` — the same shape as `build_character_networks.py`. Until then,
  edits to the mandala cannot be reproduced. Note the two traps recorded in memory: all
  visualizations live in **one** directory so relative cross-links work, and generators
  have previously written to the wrong `docs/` path — an empty `git status` after a run
  means the path is wrong.
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
| 2026-10-07 | Tiers pulled in toward the author; the two play bands merged into one continuous three-lane band; footprints replaced by curtain / book / notebook icons with a legend key; focus fan moved outward and every node made clickable into the side panel. |
| 2026-10-06 | Spec §3 A/B applied: inner ring as coloured circles with author lines; footprint traces linked to their play; split interaction (panel for title-only, centred graph for traced); icon vocabulary; dates and attestations parsed. Published to the Pages site. |
