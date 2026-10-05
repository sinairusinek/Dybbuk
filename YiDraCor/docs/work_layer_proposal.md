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

`editions.json.expression_id` joins **26 of 28 editions** to a catalogue work.

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

## Open decisions for Sinai

1. **Node id scheme.** `work:3787` (Expression ID) is stable and already the
   join key. Readable ids were made canonical elsewhere
   (see `project_yidracor_speaker_who_review`) — but play titles are not unique
   enough across 277 works to key on.
2. **The 2 editions that do not join**: `MS_BenHaDor` (expression_id 4010) and
   `MS_Emigration` (3830) carry ids absent from both Plays sheets. Bad ids, or
   works missing from the catalogue?
3. **Scope.** Mint all 277 works, or only those with an edition or surviving
   material? Minting all 277 makes the 252 text-less plays addressable, which is
   what lets the website show a poster for a lost play.
4. **`certainty: false ascription`** — should a falsely-ascribed play still get a
   work node (with the attribution marked), or be excluded? It is evidence about
   the reception history either way.
5. **`Goles Rusland` / expression 3879** appears on two `editions.json` rows that
   are otherwise identical (`Lateiner_Meshumed` twice, differing only in `notes`).
   **This looks like a duplicate row rather than two editions** — worth fixing
   independently of this proposal.

## Knock-on effects

- `DybbukMedia` should then anchor `play_key` to `work:*` rather than leaving
  527 rows at GAP. The `link_status` vocabulary and the matcher need no change.
- `seed_jpress.py` currently seeds 28 tasks from editions. The `JPRESS` column
  flags **102 Lateiner plays**, so the worklist should be driven off works —
  a much better seed than the one I built.
- Character networks and the DraCor export key on editions today; both would
  need to decide which layer they describe.
