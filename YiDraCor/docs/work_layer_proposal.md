# Adding a `work` node (WEMI Work) to the entity graph

**Status:** proposal, not implemented. Raised by Sinai 2026-10-05.
**Touches:** `YiDraCor/code/build_entity_graph.py`, and downstream
`DybbukMedia/` anchoring.

## The defect

`entity_graph.json` has four node kinds — `edition`, `person`, `org`, `place` —
and **no work layer**. Editions stand in for plays. In WEMI terms the graph
jumps straight from Work to Item, so every work-level fact is attached to a
particular printed book.

That is not a tidiness complaint. It makes the graph assert false things:

**42 performance edges claim a performance of a book that did not yet exist.**

| play | performed | edition printed | gap |
|---|---|---|---|
| Mishke Mashke | 1889 | 1911 | 22 years |
| Ezra | 1891 | 1908 | 17 years |
| Yudale der Blinder | 1899 | 1908 | 9 years |
| Di Seder Nakht | 1903 | 1908 | 5 years |

The 1889 New York premiere belongs to the *play* Mishke Mashke. The graph
currently hangs it on the 1911 Warsaw printing, which is a different kind of
thing and did not exist in 1889.

**82% of all edges are work-level** (232 of 283):

| level | edges | rels |
|---|---|---|
| work | 232 | `performed_at` 117, `composer` 43, `wrote` 28, `actor` 19, `premiered_in` 10, `actress` 8, `arranger` 3, `lyrics` 2, `choreographer` 1 |
| item | 51 | `holds` 21, `published` 15, `printed_in` 15 |

Only those 51 genuinely belong to an edition. The authorship, the cast, the
score and every performance belong to the work.

## What it costs us right now

**252 of 277 catalogued plays are invisible to the graph.** The catalogue knows
277 Lateiner/Hurwitz plays (`Lateiner Plays` 152 + `Hurwitz Plays` 125, keyed by
`Expression ID`); only 25 have a surviving edition in our corpus. The other 252
cannot be represented at all, because the only available node kind is `edition`
and there is no edition to make.

That is not an abstract loss. In `DybbukMedia/data/media_manifest.tsv`:

- **525 of 1090 media rows (48%) name a play and anchor to nothing**, spanning
  **131 distinct titles** — posters, scores and press notices for real plays
  whose texts are lost.
- A work layer would rescue **338 of those 525 rows, attaching them to 65
  works**, using the matcher already in `DybbukMedia/code/common.py`.

Top orphaned titles by item count: Homon der zvayter (24), Malke Shvo (18),
Der kuzari (16), Hokhmas noshim (16), Der yud in Sabiesky tsayten (16),
Shloyme hamelekh (14), Yehuda un yisroel (14).

## The work layer already exists in the data

Nothing needs inventing. `Lateiner Plays` / `Hurwitz Plays` supply:

| column | use |
|---|---|
| `Expression ID` | **the work key.** 277 distinct, stable, already referenced by `editions.json.expression_id` |
| `English Name`, `Yiddish Name` | labels, both scripts |
| `author` | people_db db_id (`683.0`) — already how `wrote` resolves |
| `TAGS` | genre (gezang-drama, operetta, tsaytbild) |
| `certainty` | `certain` / `false ascription` — **attribution is already graded** |
| `expression` | adaptation lineage ("adaptation: Linetsky play", "adaptation from Goldfaden") |
| `do we have a copy` | survival: `perl`, `y`, archive.org URLs |
| `JPRESS` | **102 of 152 Lateiner plays flagged as having JPRESS coverage** |
| `Silberzweig`, `Berkovitsh`, `Sieger` | cross-references to the reference literature |

`editions.json.expression_id` joins **all 28 editions** to a catalogue work.

> An earlier draft of this document reported 26 of 28, with `MS_BenHaDor`
> (4010) and `MS_Emigration` (3830) missing from the sheets. That was a bug in
> my probe, not a gap in the catalogue: I normalised the float ids openpyxl
> returns with `str(v).rstrip('.0')`, which strips *every* trailing `.` and `0`,
> so `'4010.0'` became `'401'` and `'3830.0'` became `'383'`. **28 of the 277
> work ids end in zero** and were all corrupted. Use
> `str(int(float(v)))`. Ben Hador is `4010` in `Hurwitz Plays`;
> `Di emigratsyon nokh Amerike` is `3830` in `Lateiner Plays`.

## Proposed shape

```
work   --wrote-->       (no: person --wrote--> work)
person --wrote-->       work        # moved off edition
work   --realised_as--> edition     # new; the WEMI spine
work   --performed_at-> org         # moved off edition
work   --premiered_in-> place       # moved off edition
person --actor-->       work        # moved off edition
person --composer-->    work        # moved off edition
org    --published-->   edition     # unchanged, correctly item-level
edition --printed_in--> place       # unchanged
org    --holds-->       edition     # unchanged
```

So: **move the 232 work-level edges onto `work` nodes, keep the 51 item-level
edges on `edition`, and add `realised_as` to connect the two.**

A performance event is arguably its own node (it has a date, a venue, a cast and
a troupe), which would make the cast edges attach to the *event* rather than
the work. That is the fuller WEMI+event model and a larger change; the work node
is the prerequisite either way, so it should land first.

## Decisions taken (Sinai, 2026-10-05)

1. **Mint all 277 works** — including those with no surviving edition, so the
   252 text-less plays become addressable and the site can show a poster for a
   lost play.
2. **Mint falsely-ascribed works too, recording the attribution status as such.**
   A false ascription is evidence about reception history; the node says the
   attribution is disputed rather than omitting the play.
3. **Node id: `work:<Expression ID>`.** Verified safe — the 277 ids are globally
   unique, with **no id appearing in both sheets**.
4. **The 28-of-28 join**: resolved, see the note above. No catalogue gap exists.

## Questions that remain

### 1. `certainty` — DECIDED (Sinai, 2026-10-05)

**Carry the five raw values through verbatim** (`certain`, `uncertain`,
`false ascription`, `error`, `uncertain/false?`). No normalisation: `error` and
`false ascription` are different claims and collapsing them would destroy the
distinction between a misattribution (3841 *Di laykhtziniger* — the play exists,
by Friendsel) and a spurious title (3907 *Nekhemye kugl* — not an independent
work at all).

**The 125 Hurwitz works get `unknown`**, not a defaulted `certain` — the column
is empty for all of them, and nobody has checked. `unknown` says so.

A consumer wanting coarser buckets can group the raw values; a consumer wanting
the distinction still has it. The free-text `comments` column is where the
reason lives and should travel with the work.

### 2. The two sheets have different schemas — DECIDED: union

**Take the union of both schemas.** The asymmetry is between the two *sheets*,
not between the two playwrights — Hurwitz is of course a playwright, and the
sheets were simply maintained by different hands at different times.

**The 9 shared columns carry the whole work identity:**
`Expression ID`, `English Name`, `Yiddish Name`, `author`, `TAGS`, `Genre`,
`certainty`, `expression`, `comments`.

Everything playwright-specific is secondary:

| Lateiner-only | Hurwitz-only |
|---|---|
| `JPRESS` (102), `Silberzweig` (82), `Berkovitsh` (58), `Sieger` (24) — provenance flags | `Year of Composition` (110), `Translated Play title` (111), `Music available` (45), `Sources/Availability of Text` (37), `Composer` (28), `Draws on existing material` (24), `Success/Length of run` (12), `Plot summary` (3) |
| `do we have a copy` (68) | `Details/Notes` (47), `Play title by Daniela`, `tempsource` |

**A null in a union field means "unrecorded in this sheet", never "none
existed."** The clearest case: `Composer` is populated for 28 Hurwitz works and
absent from `Lateiner Plays` entirely — yet Mogulesco demonstrably composed for
Lateiner. The graph already knows this independently: its **44 `composer` edges
come from the roles sheets (source: Silberzweig), which cover both playwrights**,
not from `Hurwitz Plays.Composer`. So composer coverage does *not* actually
depend on the asymmetric column — the union's nulls are narrower than they look,
and the real composer data arrives by another route.

The builder should therefore record provenance per field (which sheet a value
came from) so a null is never misread as a negative claim. Checked and not
found: neither the `hafakot` (productions) nor `songs_Lateiner and Hurwitz`
sheets carry play-level composer data, so there is no third source to fill the
Lateiner side from.

### 3. Adaptation lineage — DEFERRED (Sinai, 2026-10-05)

Left as a research task. Carry the `expression` /
`Draws on existing material` text on the work as an attribute; do not attempt to
parse it into entities or `adapted_from` edges yet.

### 4. Performance event as its own node — DECIDED: yes (Sinai, 2026-10-05)

**With an explicit requirement: the model must support both granularities at
once.** Sinai: "especially in Zylbercweig, many cases we will know that an actor
played a role in a play without knowledge of the exact performance events, but
when we have the refined data we keep it."

So a cast fact must be recordable at whichever level the evidence supports:

```
person --actor--> work                  # Zylbercweig says X played Y; no event known
person --actor--> performance_event     # a dated playbill: X played Y, Thalia, 1897
performance_event --of_work--> work     # the event always names its work
```

This is a **coarse-to-fine** model, not two competing ones. The work-level edge
is not a placeholder to be deleted once an event turns up: the Leksikon's claim
that an actor played a role is itself evidence, with its own source, and it
survives alongside any later event-level attestation. A consumer asking "who
played in this play" must union both levels; one asking "who was on stage that
night" reads only the event level.

Consequences to design for:
- An event-level fact must not be double-counted as a separate work-level claim
  when both exist for the same person+role+work.
- The reverse inference is **invalid**: an event-level cast fact does imply the
  work-level one, but a work-level fact must never be promoted to an invented
  event.
- Each level keeps its own `source` (Silberzweig vs a specific playbill), so the
  provenance distinction stays visible.

### 5. What layer do the character networks and the DraCor export describe?

**Why this is a real question and not bookkeeping.** Today there are 24 DraCor
TEIs for 27 works, so the mapping looks 1:1 and either answer "works". That is a
coincidence of our current corpus, and it is the reason this is easy to get wrong
now and expensive to unpick later. The moment a second edition of one play is
encoded — a Warsaw and a New York printing of *Hinke Pinke*, say, or a printed
text alongside the manuscript — the two layers diverge and every consumer that
guessed wrong breaks.

**The substantive issue: a character network is a measurement of a text, not of
a play.** It is computed from who co-occurs in which scene, so it depends on
choices that belong to *one* edition:

- how many acts and scenes that printing has (our MS and print witnesses differ);
- which characters that witness names, and under which speech-prefix labels —
  the whole `speaker_overrides` apparatus exists because witnesses disagree;
- cuts, censorship and added musical numbers.

Two editions of one play therefore yield two *different, both-correct* networks.
Attaching the network to the work would force a false choice about which witness
represents the play.

**But the reader's expectation runs the other way.** Someone browsing the site
wants "the network for *Mishke Mashke*", not "the network for the 1911 Warsaw
Kultur printing". And for the 252 text-less works there is no network at all, so
a work-keyed network silently means "no data" for 90% of the plays.

**Recommendation: the artefact is edition-level, the entry point is
work-level.** Concretely:

- the network **belongs to the edition** (`edition --has_network--> …`), because
  that is what was measured;
- the work page **lists the networks of its editions**, labelled by witness, and
  shows one by default when there is only one;
- the DraCor export likewise stays **edition-keyed** — it already carries
  edition identity, not work identity: `Mishke-Mashke.xml` asserts
  `<idno>II 65.675</idno>` (the Biblioteka Narodowa shelf mark) and a
  `transkribus` idno pointing at doc 828537. Those identify **a specific
  physical copy**, so the file is already describing an edition whatever we
  call it.

**What this needs from the builder:** the DraCor TEIs should also carry the work
identifier (`<idno type="work">3787</idno>` beside the existing shelf mark), so
a consumer can group editions of one play without re-deriving the mapping from
filenames. That is a small addition to the TEI header and the honest way to
express "this witness realises that work".

**Still open for Sinai:** whether DraCor itself should receive one file per
edition or one per work. DraCor's own model is play-centric, so submitting two
witnesses of *Hinke Pinke* as two plays would misrepresent the corpus there,
while submitting one means choosing a base witness — an editorial decision, not
a technical one. That choice only binds the external submission, not our
internal graph.

## Answers to three questions raised before implementation (2026-10-05)

### Which works lack a TEI, and do we have their editions?

Matching TEIs to editions by **Transkribus docId** (filename matching is
unreliable — it wrongly flags Blimele and Emigration): **24 DraCor TEIs cover
25 of the 28 edition rows.** Three do not, each for a different reason, and
**none is a missing-edition problem**:

| folder | expr | why no TEI |
|---|---|---|
| `YIVO_ShimshonHagibor` | 3959 | **Deliberately excluded.** It is a part-book (מחברת תפקיד) carrying one role's lines and cues only, not a full playtext, so it cannot be encoded as a drama. Decision already recorded in the corpus sheet. 18 pp, fully transcribed. |
| `MS_YetsiasMitsrayim` | 4043 | **Blocked, not missing.** 67 pp pulled and transcribed (66 GT + 1 FINAL); no `cast_dict.json`, so the builder cannot emit a castList. Also notable: **the text is GERMAN in Latin script**, not Yiddish, and it carries Russian censorship stamps — banned for performance, St Petersburg, 29 Oct 1910. |
| `HurbanYerushalaim_820938_duplicate` | 3891 | **A duplicate**, not a work. Doc 820938 is a second scan of doc 838368 (the real `MS_KhurbnYerusholaim`), 56 pp with a single IN_PROGRESS transcript. A PI decision from 2026-06-24 on whether to keep both copies is still pending. |

So: 26 real editions, 24 encoded, 1 excluded by editorial decision, 1 blocked on
a `cast_dict.json`, plus 1 duplicate row that should not be counted as an
edition at all. **Nothing is missing that we hold and have not encoded.**

The earlier "24 TEIs for 27 works" phrasing was loose — it compared TEIs against
*edition* rows (28, including the duplicate), not works.

### Do we have two editions of one play?

**No — not one case.** Grouping the 28 rows by `expression_id` gives exactly one
collision, and it is a data bug rather than a second witness: **expression 3879
appears twice, both `Lateiner_Meshumed`, with identical fields except `notes`.**
A duplicate row, already flagged above for independent fixing.

This matters for the question-5 recommendation: the edition/work distinction is
**entirely latent today** — every work has at most one edition. That is precisely
why the layers must be separated now, while nothing depends on conflating them.
The first genuine second witness (a print text beside a manuscript) would
otherwise break every consumer that assumed 1:1.

### The songs table — and it needs a node too

`songs_Lateiner and Hurwitz`, **235 rows across 56 plays.** The graph currently
has **no song node**: songs survive only as a `counts.songs` integer on edition
nodes (175 in total). That is the same collapse as the work layer, one level
down.

**The songs table is work-level data, not edition-level.** 223 of 235 rows
resolve to exactly one work via the existing `translit_key` matcher (1 ambiguous,
11 unmatched). And its play keys include works with **no surviving edition** —
*Yafes toyer oder, Bilem haroshe* has **25 songs and no text at all**, the
single largest song group in the table. Keyed on editions, those 25 songs are
unrepresentable; keyed on works, they attach cleanly.

Top song groups: Yafes toyer (25), Ben Hador (19), Ezre der eybiker yid (19),
Khurbn Yerusholayim (16), Goles Rusland (11), Der kuzari (10).

Fields: `Song title in sources`, `Romanized title`, `YIVO transliteration`,
`כלל יידיש` (normalised Yiddish, populated for all 235), `Title`, `Play Key`,
`Author` (173), `Source` (235 — 12 distinct publications), page numbers,
`external source id`, `recordings acc. Shund on Shellac` (64), and
`Ruthie - elaboration` (88).

**Open decisions for the song node:**

1. **Identity.** `tempID` is explicitly temporary and not unique (216 distinct
   over 224 populated), so songs need **minted ids** — unlike works, which had
   `Expression ID` waiting. Suggest `song:<n>` from a new stable sequence,
   assigned once and never renumbered.
2. **Attachment level.** `song --in_work--> work` is right for the 223 that
   resolve. But a song also appears *in a printed edition* (the 175 counted on
   edition nodes come from the editions' own song lists) and *in a sheet-music
   item* (`Score-print-editions`, `Hurwitz music`, `VilneMusicArchive` in
   DybbukMedia). That is the same coarse-to-fine situation as the cast facts in
   §4: `in_work` when the attestation is a song list, `in_edition` when a
   particular printing carries it.
3. **Is a song a Work in its own right?** Several are attested independently of
   their play — 57 rows come from *Shund on Shellac* (recordings), and 64 rows
   carry recording counts. A song with its own recordings, its own sheet music
   and its own composer is arguably a WEMI Work that happens to be *part of*
   another work, not merely an attribute of it. If so the relation is
   `song_work --part_of--> play_work`, and the song can carry its own
   editions (sheet music) and performances (recordings).
4. **Composer/lyricist.** The `Author` column names 21 distinct people
   (Yozef Latayner 107, Hurwitz 15, שלמה שמולעוויטץ 6, לואיס קאפעלמאן 5,
   Goldfaden 3) but conflates roles — for a song, "author" may mean lyricist
   while the composer sits in `Hurwitz music.Composer` or the roles sheets.
   **12 rows say "Not known"/"Unkown"** and need the same `unknown` treatment
   agreed for Hurwitz `certainty`.

**Recommendation:** mint the song node in the same pass as the work node — the
two share the `translit_key` resolution path and the coarse-to-fine attachment
pattern, and leaving songs as an integer count would repeat the exact mistake
this proposal exists to fix. Decide §3 (song as Work vs attribute) before
building, since it changes the shape.

## Knock-on effects

- `DybbukMedia` should then anchor `play_key` to `work:*` rather than leaving
  527 rows at GAP. The `link_status` vocabulary and the matcher need no change.
- `seed_jpress.py` currently seeds 28 tasks from editions. The `JPRESS` column
  flags **102 Lateiner plays**, so the worklist should be driven off works —
  a much better seed than the one I built.
- Character networks and the DraCor export key on editions today; both would
  need to decide which layer they describe.
