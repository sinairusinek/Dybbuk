# Visual-material manifest schema

One row per **visual item** (a poster, a photograph, a press notice, a page
scan, a PDF). `data/media_manifest.tsv`, tab-separated, UTF-8, LF.

The manifest is the tracked artefact. Binaries live under `media/<source>/`
and are **git-ignored** — anyone can re-fetch them from `source_url` /
`fetch_hint`. A row is useful even with no file on disk yet; `local_path`
empty + `status=PENDING` is the normal starting state.

## Columns

| column | meaning |
|---|---|
| `media_id` | stable id, `<source>-<nnnn>`. Never reused, never renumbered. |
| `source` | one of `jpress` `album` `lexicon` `dorot` `transkribus` `edition`. |
| `item_type` | `poster` `playbill` `program` `press_notice` `photograph` `sheet_music` `page_scan` `manuscript` `print_edition` `card_catalog`. |
| `title` | item title as the holding institution gives it. |
| `title_yiddish` | Yiddish title where known. |
| `date` | item date, `YYYY` / `YYYY-MM` / `YYYY-MM-DD`, or a range `YYYY-YYYY`. Empty if unknown. |
| `date_note` | free text for approximate/inferred dating ("after the 1896 premiere"). |
| `place` | place of production/publication, as written on the item. |
| `holding_institution` | NYPL, YIVO, LOC, NLI, … |
| `collection` | named collection within the institution. |
| `signature` | shelf mark / call number. |
| `source_url` | **permalink preferred.** JPRESS links are known-unstable — see `fetch_hint`. |
| `fetch_hint` | how to actually get the bits when `source_url` is not enough (JPRESS search terms, Transkribus docId/pageNr, Album page+region, PDF path). |
| `local_path` | path under `media/`, relative to `DybbukMedia/`. Empty until fetched. |
| `rights` | PD / PD-with-credit / permission-needed / unknown. |

## Entity anchoring — the point of the whole file

| column | meaning |
|---|---|
| `entity_ids` | `\|`-separated ids **from `YiDraCor/data/entity_graph.json`** (`edition:…`, `person:…`, `org:…`, `place:…`). This is what joins visual material to the db. |
| `play_key` | catalogue play key, when the item is about a specific play. |
| `expression_id` | catalogue expression id, when the item is about a specific *edition*. |
| `link_status` | `LINKED` (entity_ids confirmed) · `PROPOSED` (automatic guess, needs a human) · `GAP` (no anchor found). Same vocabulary as the entity graph — deliberately. |
| `reviewer` | who confirmed, + date. **Stamped on every write that sets a decision**, per project convention. |

## Workflow columns

| column | meaning |
|---|---|
| `status` | `PENDING` (catalogued, not fetched) · `FETCHED` · `CROPPED` · `PUBLISHABLE` · `REJECTED`. |
| `crop_of` | for a crop, the `media_id` of the full page it came from. |
| `notes` | free text. |

## Rules

1. **Never renumber `media_id`.** Appending is the only safe write.
2. `link_status=PROPOSED` is never treated as a fact. A human promotes it to
   `LINKED` and takes a `reviewer` stamp in the same write.
3. Harvest scripts are **append-and-update-by-media_id**, never
   truncate-and-rebuild — the manifest accumulates human decisions that no
   generator can reproduce.
4. Yiddish: normalise NFKD before stripping points when matching; probe
   spelling variants before concluding a name is absent.
