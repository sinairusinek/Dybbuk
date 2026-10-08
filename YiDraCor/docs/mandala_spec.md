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
| the band | 272 / 291 / 310 / 329 | **all 277 plays, one continuous band** on four sub-lanes, each on a line to its author. It is read from the inside out: the **217 title-only plays on the two inner lanes**, the **60 documented plays on the two outer lanes**, so the traces that spring from them already point away from the centre |
| traces | — | no ring of their own. Each play's traces **spring outward from the play itself**, continuing the author → play line: rows of up to three, stepping out. **Theatre front** = a house, with its **curtained stages** hanging off it; **book** = printed edition, **notebook** = manuscript |

A staging hangs off its **theatre**, not off the play: a house that gave a play four
nights is one venue node with four stagings under it. 61 of the 64 events name a venue,
across 25 houses — the Windsor alone accounts for 19 — so the intermediate node earns
its place. The 3 events with no recorded venue attach directly to the play.

The staging glyph is a scalloped valance over swept drapes, closed by a stage-floor
line. The scalloping is what stops it reading as a pair of columns at 19 px; several
plainer curtains were tried and all failed that test.

Lane assignment within each half of the band is a greedy packing rather than a
round-robin: a well-documented play needs more arc than a 4 px title dot.

The traces deliberately do **not** share a radius — a play with eleven stagings reaches
further out than one with a single edition, and that asymmetry is the point. They are
laid out as a short spray rather than a single-file chain, because one chain of eleven
reached 380 px beyond its play and more than doubled the drawing's extent, shrinking
the whole wheel to fit.

One ordering subtlety: the genre sort puts every documented play at the head of its
genre run, which made their sprays collide into a single tangle. Documented plays are
therefore spread evenly through their own genre run — the colour arcs are unchanged,
but no two sprays start side by side.

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

C. (2026-10-08) To accommodate visualizations we need alternative, **toggled views**:

0. **Only the plays band**, in one row, none enlarged. If possible showing the
   Yiddish titles, alternating (unless they can fit together outside the circle).
1. **See the music** — will show composers, scores, song info and recordings, with
   all info related to them. The composers should be located close to the authors in
   the circle and each linked to all the plays they composed for.
2. **Archival traces** — will show printed and ms editions and the cards from the
   Perlmutter archive.
3. **Performances** — theatres and events with all information related to them.

#### Data audit for C (done 2026-10-08, not yet built)

**View 0 is geometrically settled.** All 277 works have a Yiddish title (median 18
characters, max 92). With radial labels the limit is angular pitch against
line-height (~12.5 px), *not* title length — length only decides how much radius a
label eats. Pitch at one ring of 277: R=420 → 9.5 px, R=520 → 11.8 px,
**R=620 → 14.1 px, R=760 → 17.2 px**. So one row of Yiddish titles fits at
**R ≥ 620**; alternating is only needed below that.

**View 1 has material in the graph.** 18 composers / arrangers / lyricists over 46
play-links (Mogulesko 6, Wohl 5, Friedsel 5, Abramovitsh 4, Brody 4, Sendler 3,
Russotto 3, Perlmutter 3 …), the 225 songs, and 84 gramophone recordings across 52 of
them. Relevant edges: `composer` 43, `arranger` 3, `lyrics` 2, `choreographer` 1.
**Scores are not in the graph** — they are in DybbukMedia (352 `sheet_music` rows), so
this view needs the media join below.

**View 2 needs DybbukMedia; the graph alone is thin.** The graph holds only 27
editions and 27 `holds` edges. The richness is in `DybbukMedia/data/media_manifest.tsv`
(1 093 rows), which the mandala does **not** read at all today: 352 sheet_music,
303 manuscript, 127 program, 97 photograph, **74 card_catalog — the Perlmutter
cards** — 37 poster, 28 press_notice. The Perlmutter card catalogue (*affishn un
programen*) is 72 of those rows; 273 rows are Perlmutter-tagged overall.

**The join is the real work: 427 of the 1 093 rows are anchorable to a mandala work
today.** Only **28** reach a work through `expression_id`; **421** reach one through
`play_key` matching a work's label (904 rows carry a play_key, 201 distinct, but only
70 of those keys match). By type: sheet_music 175, manuscript 137, **card_catalog 36**,
poster 23, press_notice 22, print_edition 19, photograph 14. And `link_status` is
PROPOSED 910 / GAP 127 / LINKED 56 — most anchors are machine-proposed, so these views
must say so on the face of them, the way the song panel now does.

**View 3 is ready from the graph as it stands.** 64 events (`date`, `event_type`,
`work_id`), 144 `performed_at` edges over 29 venue orgs, 12 places, 9 `premiered_in`.
Stagings already hang off their theatre.

#### Questions this raises, for Sinai

- **View 1:** should a composer sit on a **ring of its own between the portraits and
  the play band**, or in the centre beside the portraits? "Close to the authors"
  admits both, and the choice decides whether composer→play lines cross the band.
- **Do the views filter one band or re-lay the wheel?** Filtering keeps the mental
  map — a play stays where it was, non-matching plays dim. Re-laying reads better per
  view but the viewer loses their place between toggles.
- **View 2 pulls a second dataset into the page.** 427 rows inline is fine; all 1 093
  with thumbnails is not.

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
| 2026-10-07 | Stagings regrouped under their theatre; the staging icon redrawn as a scalloped valance after the plain curtain read as two columns. |
| 2026-10-07 | Documented plays moved to the band's outer lanes; the traces' shared ring removed so each play's traces spring radially from it, keeping the asymmetry; documented plays interleaved through their genre run so the sprays do not collide. |
| 2026-10-07 | Tiers pulled in toward the author; the two play bands merged into one continuous three-lane band; footprints replaced by curtain / book / notebook icons with a legend key; focus fan moved outward and every node made clickable into the side panel. |
| 2026-10-07 | Songs given their full record in the side panel: both titles, lyricist, songbook + page, gramophone-recording count, NLI links. Three defects fixed in `work_layer.py` — a 14-row column shift in the songs sheet, 92 lost reading notes, and the list's forced RTL. |
| 2026-10-08 | Songs drawn in the wheel as a third tier of the hub fan. 34 of the 54 plays with songs have no other trace and had been opening onto an empty fan — 111 songs were stranded. |
| 2026-10-06 | Spec §3 A/B applied: inner ring as coloured circles with author lines; footprint traces linked to their play; split interaction (panel for title-only, centred graph for traced); icon vocabulary; dates and attestations parsed. Published to the Pages site. |
