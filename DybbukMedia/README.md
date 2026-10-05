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

## Known modelling gap: the printer edge is missing (the orgs are not)

`build_entity_graph.py:541-549` reads the imprint and emits two edges:

```
publisher         -> org node (org_role="publisher"), rel="published"
publication_place -> place node,                     rel="printed_in"
```

Note `printed_in` points at a **place**, not a printer. The `printer` field of
`editions.json` is **read by nothing** — so:

- **play -> edition -> publisher IS traversable**, for the 15 printed editions
  that name a publisher. The other 12 (the MS track) have no imprint, correctly.
- **play -> edition -> printer is NOT**, for any edition.

**The six printers already exist in core_db and are already typed `Printer`:**

| imprint on the edition | core_db | org_type |
|---|---|---|
| F. Baumritter | 157 `Baumritter` | `Printer` |
| Druk ha-Tsfira (Panska 40) | 68 `הצפירה` (addr. פייַנסקא 40 = Panska 40) | *(empty)* |
| Druck von S.L. Deitscher | 66 `ש. ל. דייטשער / Sh. L. Deytsher` | `Printer` |
| N. Starowolski | 58 `N. Sṭarovolsḳi` | `Printer` |
| B. Turš | 76 `B. Tursh` | `Printer` |
| Druck von E. Salat | 70 `D. Salat / א. סאלאט` | `Printer` |

So nothing needs minting. The fix is one block in `build_entity_graph.py`:
resolve `e["printer"]` the way `publisher` already is and emit
`rel="printed_by"`. The imprint strings carry a `Druck von` / `Druk` prefix and
a trailing `(address)` that must be stripped before resolving, and romanisation
differs (`Starowolski` vs `Sṭarovolsḳi`, `Turš` vs `Tursh`), so this needs the
variant-probing the entity-graph resolver already does — an exact match finds
none of them.

**`org_type` already has the vocabulary for the publisher/printer overlap**:
62 orgs are `Publisher`, 13 `Printer`, and **16 are already `Printer/Publisher`**.
It is a single-valued field — no `|`-separated values anywhere in 2196 rows — so
`Printer/Publisher` is the existing way to say an org did both, and the right
value for any of these six that also published. Note that `org_role` in the
entity graph is a *different* thing: it records the role the org played **in
this edition's imprint**, which is per-edge, not an org-level type.

Two data issues found while checking, both for the org-alignment track rather
than here:

- **db 68 `הצפירה` and db 270 `HaTsfira` look like a duplicate pair**, and 68
  carries no `org_type` despite being a printer with a matching address.
- **db 70 is internally inconsistent**: `D. Salat` against `א. סאלאט`
  (= *A.* Salat), while this edition's imprint reads *E.* Salat. One of the
  three initials is wrong.

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
