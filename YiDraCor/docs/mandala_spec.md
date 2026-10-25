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
| composers | ~190–260, **by connection** | *music view only.* 18 musical contributors, each seated at the circular mean of the plays it wrote for and relaxed clear of its neighbours — **not a ring**, despite the radius range, which is only where they happen to land. See the 2026-10-25 amendment |
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

- **A composer** (music view) → its whole fan lights: the plays it wrote for, their
  titles, spokes and traces stand; its edges go to full strength; every other
  composer and edge recedes. The side card opens alongside, listing the plays with
  their genres. Added 2026-10-25 — before that a composer click opened the card
  only, which left no way to see *which* plays a composer wrote for. Clears on a
  second click, Escape, reset, or bare canvas.

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

#### Decided by Sinai, 2026-10-08 — items 1 and 2 AMENDED 2026-10-25, see below

1. ~~**Composers sit in the centre, beside the portraits** — not on a ring of their
   own.~~ **Superseded 2026-10-25.** They are placed by connection; see the
   amendment. The concern this decision was answering — that composer→play lines
   cross the band — turned out to be the lesser problem: a ring of their own made
   *every* line a chord, and position carried no information at all.
2. **The views filter one band in place; they do not re-lay the wheel.** Every play
   keeps its position in all four views. A view dims what it does not concern, lights
   what it does, and draws its own layer over the top. This keeps the viewer's place
   across toggles and keeps the 217 title-only plays visible as context in every view
   — the "most of this repertoire is lost" reading survives the toggle. The cost,
   accepted: a view's 24–58 lit plays are scattered around the ring rather than
   gathered into a shape.
   **Still true of the PLAYS, as of 2026-10-25** — they never move. What a view may
   now re-lay is its *own added layer* (the composers), not the band.
3. **Where the 1 093 media rows come from** — audited below before any build.

#### Amended by Sinai, 2026-10-25 — the music view

Prompted by a screenshot of the music view that showed faint hairlines on cream and
no legible network. Sinai: *"I don't see the links/edges and the network structure
that I would like seen. The composers for example — I would expect clicking them to
highlight both the linked nodes and edges that are relevant to them. And be located
according to their connections, not in a regular circle."*

Built in `887f190f0`. Four things changed, all in the composer layer of
`mandala/tail.html`:

1. **Composers are placed by connection, not on a ring.** Each seats at the
   *circular mean* of the plays it wrote for — unit vectors summed, never radians
   averaged, which wraps wrongly at ±π — pulled inward in proportion to its degree,
   then relaxed apart with a spring back toward its own angle.
   **The plays themselves do not move**, so decision 2 above still holds for the
   band: this re-lays only the layer the view adds.
   *Scope: the music view only.* Archival traces and Performances still place their
   added layer as before.
2. **All 18 musical contributors draw.** A `plays.size > 1` filter had been dropping
   the 7 with a single credit, and 7 of the 45 links with them. A reader cannot tell
   a dropped node from an absent one, so the filter was a silent falsehood about the
   data. Edges drawn: 38 → 46.
3. **Edges are visible.** `stroke-opacity .16` at `.6px` on this ground is below what
   the eye resolves; they are the subject of this view, so `.5` at `1.3px`,
   round-capped.
4. **Clicking a composer lights its fan.** `focusComposer()` lights that composer's
   plays, titles, spokes and traces, takes its edges to `.95` at `2px`, and drops the
   other composers to `.18` and their edges to `.04`. It clears on a second click,
   Escape, the reset button, or bare canvas — sharing the clear path with a play
   focus. Before this, a click opened the side card only.

**A property of the data to design around, not a layout fault:** only **20 of the
275 plays carry music**, and they sit in one arc of the ring. All 18 composers
therefore cluster in that direction. The crowding a viewer sees is the shape of the
repertoire; the relaxation spreads them for legibility without pretending otherwise.

#### Where the 1 093 come from (audited 2026-10-08)

`DybbukMedia/data/media_manifest.tsv`, six harvests, declared in
`DybbukMedia/data/sources.tsv`:

| source | rows | what it is | item types |
|---|---|---|---|
| `dorot` | **786** | NYPL Dorot *plus* other archival holdings — **despite the name it is not one collection**. Holding institutions: YIVO 319, NYPL 111, LOC 34, Polona 17, Kaminska Theatre Museum 14, YBC 9, Boston PL 9 … | sheet_music 352, manuscript 268, program 127, poster 25, photograph 14 |
| `album` | 114 | Zylbercweig, *Album fun yidishn teater* (1937); own KG in SQLite | photograph 83, page_scan 31 |
| `transkribus` | 112 | Transkribus collection 18874 — notices, posters, **the Perlmutter card catalogue** | card_catalog 74, manuscript 26, poster 12 |
| `edition` | 28 | the 28 editions in `YiDraCor/data/editions.json` | print_edition 19, manuscript 9 |
| `jpress` | 28 | JPRESS historical Jewish press notices | press_notice 28 |
| `lexicon` | 25 | Zylbercweig, *Leksikon fun yidishn teater* | page_scan 23, portrait 2 |

**The Perlmutter material** is 74 `card_catalog` rows from `transkribus` (the card
catalogue *affishn un programen*, 72 rows) plus scattered `dorot` rows citing the
Sholem Perlmutter Collection at YIVO by series and folder — 273 rows mention
Perlmutter in total.

**Two facts that constrain what views 1–2 can show:**

- **Almost nothing is an image on disk.** `local_path` is populated on **2 of 1 093
  rows** — the two portraits already in the mandala. Only 385 rows carry even a
  `source_url` (236 of the 786 `dorot` rows do). So these views can show **citations
  and counts, not thumbnails**, unless a harvest step comes first.
- **The anchoring is better than the first pass suggested, if the right matcher is
  used.** Exact `play_key`→label matching reaches 427 rows / 76 works. But
  **DybbukMedia already has a matcher — `translit_key()` in
  `DybbukMedia/code/common.py`, with tests in `test_matching.py` — and it reaches 707
  rows on 100 works with only 14 ambiguous**, once the index also covers the
  catalogue's three alternative-title fields (`Play title by Daniela`, `Translated
  Play title`, `שם אצל ברקוביץ`). **Use it; do not write a third matcher.**

By type, those 707: sheet_music 283, manuscript 270, **card_catalog 40**, program 30,
poster 28, press_notice 23, print_edition 19, photograph 14.

**202 rows / 76 play keys still miss**, and they are real corpus plays whose spelling
defeats the fold — *Blihmele*, *Shlekhte-Froy*, *Yakov un Esau*, *Soroh Sheyndel*,
*Yosef in Egypten*, *Di Seder Nacht*. These want a human pass, not a looser matcher;
`link_status` across the manifest is PROPOSED 910 / GAP 127 / LINKED 56, so **every
view built on this must show provenance on its face**, the way the song panel does.

#### Decided by Sinai, 2026-10-11

4. **A composer medallion is about 2/3 the size of Lateiner's and Hurwitz's.** The
   playwrights stay the largest thing at the centre; composers read as a second order
   of the same kind of node, not as equals and not as specks.
5. **Only composers with MORE THAN ONE play go inside.** A composer credited on a
   single play is placed outside, by that play. This is what makes the centre
   tractable: **11 composers inside, 7 outside** —

   | inside (>1 play) | plays | | outside (1 play) |
   |---|---|---|---|
   | Mogulesko, Zigmunt | 6 | | Rumshinsky, Yoysef |
   | Wohl, Herman (Tsvi) | 5 | | L. Y. Baraban (choreographer) |
   | Friedsel, Louis | 5 | | Shayevitsh, Yisroel |
   | Abramovitsh, Maks (Mendl) | 4 | | Sandler, Perets |
   | Brody, Yoysef | 4 | | Lyov, Leo (Moyshe-Leyb) |
   | Sendler, Yakov Kopl | 3 | | Shmulevitsh, Sholem (lyrics) |
   | Russotto, Henry | 3 | | Joachim Kurantman |
   | Perlmutter, Arnold | 3 | | |
   | Minkowski | 2 | | |
   | Gimpel, Adolf | 2 | | |
   | Shlosberg, Yitskhok | 2 | | |

6. **The toggle keeps the selected play open across views.** Switching view while a
   play is open re-draws that play in the new view rather than returning to the wheel
   — so one play can be watched changing aspect. This is the main thing filter-in-place
   buys, and it only works because a play never moves (decision 2).

#### Decided by Sinai, 2026-10-11 (second round)

7. **The 7 single-play composers go on the trace tier beside their play** — where the
   editions, stagings and songs of that play already sit — not on a ring of their own.
   They are near the one play they touch, and the fan machinery already places them.
8. **View 0 is the RESTING STATE, not a fourth view.** The bare band is what the
   mandala shows with no view selected, and views 1–3 filter it. So there are **three
   view layers to build**, over a resting band that is already most of what view 0
   asks for.

9. **The resting band expands to ONE ring at R≈660.** The four-lane band at
   R=272–329 cannot carry labels (6–7 px pitch against a ~12.5 px line-height), so
   "view 0 is the resting state" means the band itself becomes view 0. At R=660 over
   the usable arc (2π less the two author gaps) the pitch is **13.8 px** — labels fit.

   **Dots are uniform at rest**, which is what "none enlarged" asks for and is also
   what makes it possible: a 20 px evidence-sized dot collides at *any* practical
   radius, but a uniform ~9 px dot leaves the pitch to the label. Views 1–3 still size
   by evidence, and they can: each lights only 24–58 plays, so their pitch is 69–168 px.

   **What this costs, and it is not nothing:** the inner/outer split — 217 title-only
   plays inside, 60 documented outside — was the drawing's main argument, that most of
   this repertoire survives as a name. One ring gives that up. It has to be carried by
   dot treatment (fill vs. hollow) instead of by radius.

#### BUILT 2026-10-25 (`b790d3e85`, `6980a086d`)

All of §C is in. The resting ring is view 0; `#views` switches the three
lenses; the composers hold the centre.

- **58** plays carry music, **26** an edition, **24** a staging. A view keeps
  its plays at full strength and fades the rest, so the shape of the whole
  repertoire stays legible underneath.
- **The toggle keeps the selected play open** — switching view re-opens the
  same play under the new lens. Verified over 1100 play-opens (every play in
  every view), no errors.
- **11 composer medallions** at 2/3 the portrait radius on a ring at R=300,
  each lined to every play it wrote for; opening a play dims the composers who
  had no hand in it. The 7 single-play composers are on their play's trace
  tier.
- **The ring's titles clear while a play is open** — on the 660 band the fan
  crosses them, and a dimmed title still occupies its space.

#### Still open

- Whether the three view layers can share one `drawFocus`, or each needs its own
  fan builder. The song tier already strains the single fan, and none of the
  three views yet changes what the FAN draws — only which plays are lit.
- **DybbukMedia is still not joined.** Views 1–2 show only what the graph
  holds: no scores (352 `sheet_music` rows) and no Perlmutter cards (74
  `card_catalog` rows). The repo's own `translit_key()` reaches 707 of 1093
  rows on 100 works; that join is the next real piece of work.
- How the "most of it is lost" reading survives the move to one ring.

---

## 4. Known gaps and technical debt

- ~~**The generator is still not in the repo.**~~ **FIXED 2026-10-11** (`2715f530e`).
  The page is three committed pieces: `code/mandala/head.html`, the payload built by
  **`code/build_mandala.py`** from `data/entity_graph.json`, and `code/mandala/tail.html`.
  Head and tail are hand-written and stay hand-written — they are the design; only the
  payload is generated. `python3 code/build_mandala.py` rewrites the page;
  `--check` verifies it is current and writes nothing. Run it after every
  `build_entity_graph.py`. The masthead counts are filled from the payload, so they
  cannot drift from the wheel again.
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
