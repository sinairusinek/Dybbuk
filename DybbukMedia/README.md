# DybbukMedia

Visual material for the Lateiner/Hurwitz database — the images that will
accompany the data on the project website.

This folder is a **catalogue**, not an image dump. `data/media_manifest.tsv`
holds one row per visual item, and every row tries to anchor to an entity in
`YiDraCor/data/entity_graph.json`. That anchor is the point: it is what lets the
site put a poster next to the edition it advertises, or a portrait next to the
actor it depicts.

Binaries are **git-ignored** (`media/`). The manifest travels in git; the bits
are re-fetchable from each row's `source_url` / `fetch_hint`.

## Current state — 1090 items

| source      | items |
|-------------|-------|
| dorot       |   786 |
| album       |   114 |
| transkribus |   112 |
| edition     |    28 |
| jpress      |    28 |
| lexicon     |    22 |

537 of 1090 rows (49%) carry an entity anchor, reaching
53 distinct entities (27 edition, 21 person, 5 org).
**All 27 editions have visual material.**

### Link status

| status   | items |
|----------|-------|
| GAP      |   553 |
| PROPOSED |   481 |
| LINKED   |    56 |

`LINKED` is an identity join or a human decision, and must carry a `reviewer`
stamp. `PROPOSED` is the matcher's candidate and **is not a fact** — a person
promotes it and stamps it in the same write. `GAP` is an open question. Same
vocabulary as the entity graph, deliberately.

### Item types

| item type     | items |
|---------------|-------|
| sheet_music   |   352 |
| manuscript    |   303 |
| program       |   127 |
| photograph    |    97 |
| card_catalog  |    74 |
| page_scan     |    53 |
| poster        |    37 |
| press_notice  |    28 |
| print_edition |    19 |

## The six sources

See `data/sources.tsv` for the registry.

| source | how it arrives |
|---|---|
| `dorot` | NYPL Dorot posters/programs/scores plus the archival-holdings sheets (Hurwitz scores, Vilne Music Archive, LOC copyright deposits), from the catalogue workbook. |
| `album` | Zylbercweig's *Album fun yidishn teater* (1937). Harvested from the Album tagger's own SQLite KG. |
| `lexicon` | Zylbercweig's *Leksikon*. The structured TEIs anchor each entry to a page facsimile, so a person's entry page — and the portrait on it — is locatable. |
| `transkribus` | Collection 18874. Doc 821034 is 49 pp of YIVO theatre posters whose description is an item-level inventory; docs 899977 and 898723 are the YIVO card catalogue; RG8 folders are manuscript playscripts. |
| `edition` | The 28 editions themselves, print and manuscript, including those never prepared for TEI. |
| `jpress` | Press notices. **Manual**: JPRESS links are unstable and often miss the target, so each row is a search task carrying terms and a date window. |

### Workbook sheets deliberately NOT harvested

`PerlmutterMSScards` transcribes catalogue **cards** already covered as
Transkribus doc 899977 — those rows index the scans rather than naming new
objects, so harvesting them would double-count. The remaining ~26 sheets are
metadata (play-name lists, role tables, person records), not physical items.

## Scripts

Every harvester is **append-and-update-by-media_id**. A re-run refreshes
generated fields but will never overwrite a human-owned one
(`entity_ids`, `link_status`, `reviewer`, `local_path`, `status`, `crop_of`,
`notes`) — it reports how many it protected instead. Running them in any order
is safe; running them all refreshes the catalogue.

```
python3 DybbukMedia/code/harvest_catalogue.py     # Dorot + programs + scores + cards
python3 DybbukMedia/code/harvest_archives.py      # Hurwitz scores, Vilne, RG8, LOC
python3 DybbukMedia/code/harvest_album.py         # Album plates and photos
python3 DybbukMedia/code/harvest_lexicon.py       # Lexicon entry pages
python3 DybbukMedia/code/harvest_editions.py      # the editions themselves
python3 DybbukMedia/code/harvest_transkribus.py   # collection 18874  (--refresh to re-query)
python3 DybbukMedia/code/seed_jpress.py           # JPRESS search worklist

python3 DybbukMedia/code/test_matching.py         # matcher regression test
python3 DybbukMedia/code/verify_manifest.py       # manifest integrity checks
```

## Matching across romanisations

These sources spell the same play many ways — *Khinke-Pinke* / *Hinke Pinke*,
*Di seyder nakht* / *Di Seder Nakht*, *Ishe-roe* / *Isha Raa*, *Al Nahares
Bovl* / *Al Naharot Bavel*. `common.translit_key()` folds the systematic
alternations (kh/ch/h, ts/tz, ey/ei/e, all vowels to one class, word-final
tav→s, dropped conjunctions and articles) to a comparison skeleton.

`test_matching.py` pins 26 real source spellings against 12 control titles —
real Yiddish plays that are *not* in this db and must never be claimed. Run it
after touching the folds.

People are bridged differently: the Album names people in Latin script while
the entity graph holds mostly Yiddish `Surname, Given`, so names route through
`Zylbercweig/people/people_db.tsv`, which carries both forms under one `db_id`.

## Imprint: publisher and printer are both traversable

`build_entity_graph.py` reads the imprint and emits:

```
org     --published-->  edition      # the publisher
org     --printed_by--> edition      # the printer
edition --printed_in--> place        # where it was printed
work    --realised_as-> edition      # the WEMI spine
```

So **play → edition → publisher** and **play → edition → printer** both resolve.
`printed_by` was added 2026-10-06; before that the printer named on a title page
was a dead-end string. All 7 printers in the corpus link to core_db orgs that
already existed and were already typed `Printer`:

| edition | printer | core_db | publisher |
|---|---|---|---|
| Mishke Mashke | F. Baumritter | 157 | Farlag "Kultur" |
| Der Mann untern Tisch | Druk ha-Tsfira | 270 | "Teater bibliyotek" |
| Isha Raa | S.L. Deitscher | 66 | Verlag von Benjamin Munk |
| Hinke Pinke (1907) | N. Starowolski | 58 | "Teater bibliyotek" |
| Sore Sheyndel | B. Turš | 76 | "Di yudishe bihne" |
| Kidush Hashem | E. Salat | 70 | Verlag von D. Roth |
| **Khurbn Yerusholaim (1908)** | **N. Starowolski** | **58** | **none on the title page** |

The last row is why the edge earns its place: that title page reads
"Тип. Н. Старовольскаго, Варшава Гуся 18. 1908" and names no publisher, so the
printer is its only imprint actor. The same Warsaw printer appears in
consecutive years, once working for a named publisher and once alone.

Matching needed `roman_fold()`: the exact resolver returned GAP for every
printer because title pages and core_db romanise the same name differently
(Starowolski/Sṭarovolsḳi, Turš/Tursh, Deitscher/Deytsher).

**Still open for the org-alignment track**: core_db 70 reads `D. Salat` against
Yiddish `א. סאלאט` (A.) while the Kidush Hashem title page says *E.* Salat — one
of three initials is wrong. `הצפירה` 68 and `HaTsfira` 270 still look like a
duplicate pair, and 68 carries no `org_type`.

## Next steps

1. **Review the 481 PROPOSED anchors.** The matcher's candidates need a human
   pass; a promotion must carry a `reviewer` stamp.
2. **Page-level pass on the card catalogues** (899977, 1551 pp; 898723, 994 pp).
   The `cardCatalogAffishenProgramen` and `PerlmutterMSScards` sheets look like
   transcriptions of these scans — **verify that join** before treating the
   sheets as an index to them.
3. **JPRESS, together.** 28 search tasks seeded; links are unstable, so this is
   screen-by-screen work.
4. **Fetch the Dorot images** over IIIF for the rows that have permalinks.
5. **Crop the Album and Lexicon portraits** from the page scans already located.
6. **Add the `printed_by` edge** to `build_entity_graph.py` — the six printer
   orgs already exist and are already typed; only the edge is missing. See the
   modelling gap above.
7. **For the org-alignment track**: the probable `הצפירה` 68 / `HaTsfira` 270
   duplicate, db 68's empty `org_type`, and db 70's `D.`/`א.`/`E.` Salat
   initial conflict.
