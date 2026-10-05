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

### 1. `certainty` has five values, not two — and Hurwitz has none

`Lateiner Plays.certainty`: `certain` 100, `uncertain` 25,
**`false ascription` 14**, **`error` 5**, `uncertain/false?` 1.
`Hurwitz Plays.certainty`: **0 populated — the column is empty.**

So "save false ascription as such" needs a target vocabulary. `error` is not the
same claim as `false ascription`: the comments show `error` rows are mostly
*not plays at all* or not independent works —

| id | title | why flagged `error` |
|---|---|---|
| 3841 | Di laykhtziniger | "Play not by Lateiner, but by Friendsel" |
| 3907 | Nekhemye kugl | "appears only in Berkovitsh. It is not an indipend[ent]…" |
| 3908 | Nokhem gendzele | "It is not an ind[ependent]…" |
| 3818 | Der nayer stern | "Was staged at the People's theatre in Februar 1907. Play by …" |

3841 is a *misattribution* (the play exists, by someone else) — arguably the
same class as `false ascription`. 3907/3908 are *spurious titles* (not separate
works at all). Those are different facts and a reader will want them apart.

**Open:** do we (a) carry the five raw values through verbatim, (b) normalise to
a smaller vocabulary — say `certain` / `uncertain` / `misattributed` /
`spurious` — or (c) carry the raw value plus a normalised one? And what do the
125 Hurwitz works get, given the column is empty: `unknown`, or `certain` by
default? Defaulting to `certain` would assert something nobody checked.

### 2. The two sheets have different schemas

Hurwitz carries fields Lateiner lacks — `Composer` (28), `Year of Composition`
(110), `Draws on existing material` (24), `Success/Length of run` (12),
`Sources/Availability of Text` (37), `Plot summary` (3) — while Lateiner carries
`certainty`, `JPRESS` (102), `Silberzweig` (82), `Berkovitsh` (58), `Sieger` (24),
which Hurwitz lacks entirely.

**Open:** does the `work` node take the union of both schemas (most fields null
for one playwright), or only the intersection plus a per-sheet extras blob? The
union is simpler to query and honest about what is missing; it also makes the
asymmetry visible, which may itself be a finding worth surfacing.

### 3. Adaptation lineage points *outside* the corpus

30 works record a source (`expression` col: 13 Lateiner + 17 Hurwitz; plus
Hurwitz's 24 `Draws on existing material`). But the sources are overwhelmingly
**external**: Dumas (Monte Cristo), Strindberg, Goldfaden, Max Nordau's *Dr.
Kuhn*, Aaron Halle-Wolfsohn, a Linetsky play, "the German play *Olaf*".

So this is not a work→work edge inside our 277. **Open:** mint external source
works as nodes too (a fifth status beyond LINKED/PROPOSED/GAP, since they are
outside the project's scope), record the lineage as a free-text attribute on the
work, or model `adapted_from` pointing at a stub node? Note the values are prose
("adaptation: Katzeboim play, 'Eremit oyf armentera'"), often hedged with `??`,
so parsing them into entities is itself a research task, not a transform.

### 4. Does the performance event become its own node?

Deferred from the original proposal but worth re-asking now that works are
being minted. A performance has a date, a venue, a troupe and a cast; today
those facts collapse onto `performed_at` edge attributes. With a work layer,
`person --actor--> work` is *better* than today but still lossy: it cannot say
that Mogulesco played Mishke **in the 1889 Poole's Theatre production
specifically**. The 117 `performed_at` edges plus 75 catalogue productions are
enough material to justify the node — but it is a second, larger change.

### 5. What do the character networks and DraCor export describe?

Both key on editions today. A network is a property of the *text*, so it
arguably belongs to the edition (which text was encoded), while a reader will
expect to find it under the play. Needs a decision before the site links either.

## Knock-on effects

- `DybbukMedia` should then anchor `play_key` to `work:*` rather than leaving
  527 rows at GAP. The `link_status` vocabulary and the matcher need no change.
- `seed_jpress.py` currently seeds 28 tasks from editions. The `JPRESS` column
  flags **102 Lateiner plays**, so the worklist should be driven off works —
  a much better seed than the one I built.
- Character networks and the DraCor export key on editions today; both would
  need to decide which layer they describe.
