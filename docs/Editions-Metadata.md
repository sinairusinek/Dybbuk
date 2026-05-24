# Editions Metadata

This file documents how we track the print editions in YiDraCor: where the metadata lives, what each column means, and the current state of the corpus as of 2026-05-18.

## Source of truth

[`data/editions.csv`](../data/editions.csv) — **hand-maintained** index, one row per print edition. Source of truth for what we own locally, what is in Transkribus, and the vocalization-convention columns the RA fills.

Two derived files are built from `editions.csv` + the DiJeSt-style catalogue:

- [`data/editions.json`](../data/editions.json) — **nested per-edition records**, one entry per edition with full sub-arrays for productions, roles, songs, performance events, print-edition catalogue rows, and document items. Used to enrich TEI `<teiHeader>` blocks.
- [`data/editions_flat.csv`](../data/editions_flat.csv) — **flat one-row-per-edition** slice with the same columns as `editions.csv` plus catalogue/expression columns and per-edition counts (`n_productions`, `n_performance_events`, `n_premieres`, `n_shows`, `n_roles`, `n_songs`, …). Used for visualization (BI tools, plots).
- [`data/editions.md`](../data/editions.md) — **human-readable snapshot** (overview, publication info, vocalization conventions, enrichment counts). Auto-regenerated on every build.

Both derived files are produced by [`code/build_editions_dataset.py`](../code/build_editions_dataset.py). Re-run after editing `editions.csv` or the xlsx catalogue:

```bash
python3 code/build_editions_dataset.py
```

The per-edition `data/<folder>/metadata.xml` files are **Transkribus exports**, not our schema; they capture upload provenance only (`docId`, `uploader`, `nrOfPages`, collection list).

## Columns

| Column | Meaning |
|---|---|
| `title` | Display title of the play (English/transliterated). |
| `author` | Author (currently all Joseph Lateiner). |
| `year_written` | Year of composition, if known. |
| `year_printed` | Year of the print edition we are working from. |
| `publication_place` | Place named on the title page as the place of publication. |
| `publisher` | Publisher imprint as it appears on the title page. |
| `publisher_place` | City of the publisher (often the same as `publication_place` but may differ). |
| `printer` | Printer imprint (often distinct from publisher; e.g. *Druck von …*). |
| `printer_place` | City of the printer. |
| `transkribus_doc_id` | Transkribus document ID — the canonical key. |
| `transkribus_collection_id` | Collection ID on Transkribus (all currently `18874`, "Yiddish"). |
| `transkribus_url` | Direct link to the document on Transkribus. |
| `transkribus_ready` | `done` / `done?` / `partial` / `not_yet` — whether HTR has been finalized. |
| `folder` | Local directory under `data/` (empty if no local folder yet). |
| `rafe` | Edition convention: does it mark rafe (ֿ) on soft כ/פ? `yes` / `no`. |
| `vocalization_position` | `syllables` (niqqud under syllables) / `consonants` (under each consonant) / `mixed`. |
| `speakers_stage_vocalized` | `yes` / `no` / `vocalized_untagged` — vocalized in source, tagging status. |
| `notes` | Free-text. |

The three convention columns (`rafe`, `vocalization_position`, `speakers_stage_vocalized`) drive pipeline choices — see [Vocalization rafe rule](../../.claude/projects/-Users-sinairusinek-Documents-GitHub-YiDraCor/memory/vocalization_rafe_rule.md) and [DerMann conventions](../../.claude/projects/-Users-sinairusinek-Documents-GitHub-YiDraCor/memory/vocalization_dermann_conventions.md). They are filled by the RA, not derived automatically.

## Catalogue join (DiJeSt expression IDs)

The IDs in `edition metadata/DybbukCatalogue May2024.xlsx` come from the DiJeSt database and follow the **work / expression** schema. Each print edition we hold is one *manifestation* of an *expression* (the play as a textual creation); the catalogue identifies expressions with an `Expression ID` column on the **Lateiner Plays** sheet.

The integration script resolves `expression_id` per edition via this cascade:

1. **`transkribus_doc_id` → TranskribusPlayStatus → "Play" column** (canonical play name).
2. **"Play" name → Lateiner Plays.English Name / Yiddish Name → Expression ID**.
3. **Manual overrides** for editions missing from TranskribusPlayStatus or with non-matching names (`doc_to_eid_overrides` / `play_to_eid_overrides` in the script).
4. **Fallback** to our own `title` column against Lateiner Plays.

Once `expression_id` is known, the script fans out to:

| Sheet | Joined on | Result field |
|---|---|---|
| Lateiner Plays | Expression ID | `expression` (titles, TAGS, Genre, certainty, comments) |
| Lateiner hafakot | `expression` / Play KEY | `productions[]` (year, place, theatre, type) — *raw catalogue rows* |
| ProfessionalRoles- Lateiner | Play name | `roles[]` (person, role, source) |
| songs_Lateiner and Hurwitz | Play Key | `songs[]` (title, romanization, source) |
| Score-print-editions | `work id` | `print_edition_catalogue[]` (publisher, holding institution) |
| documentsitems | `work id` | `document_items[]` (programs, related artifacts) |
| **PerformanceEvents_Report_*.xlsx** (DB dump) | Roman Title | `performance_events[]` — *canonical event records (premiere/show, exact date, venue, actor, person)* |

The `performance_events[]` list is **the canonical source** for premiere and show events. Each event includes `event_type`, `date` (full date string from DB), `year`, `venue`, `actor_character`, `person_role`. Where the DB venue is missing but the hafakot row has one (matched by year + type), the hafakot venue is attached as `venue_alt` along with `source_catalogue`. `productions[]` is preserved alongside as the raw spreadsheet rows.

As of 2026-05-18: 15 of 16 editions resolved to an `expression_id`. **Ishe Raa (docId 820937)** has no matching entry in the Lateiner Plays sheet — flagged for catalogue follow-up. **Das Yudishe Kind (3867)** is marked `certainty: false ascription` in the catalogue (worth knowing before publishing it as a Lateiner work).

## Current corpus

The live, always-current snapshot lives in [`data/editions.md`](../data/editions.md) — generated on every build run, includes overview, publication info, vocalization conventions, and catalogue enrichment counts. Read that file instead of duplicating it here.

All editions are in Transkribus collection `18874` ("Yiddish").

## Local layout per edition

Conventions established by the existing plays (DerMann, Yudale):

```
data/<folder>/
  metadata.xml                          # Transkribus export, provenance
  page_transkribus_pulled_<YYYY-MM-DD>/ # raw PAGE-XML as pulled from Transkribus
    NNNN_<pageId>.xml
    _manifest.json
  page_annotated/                       # TEI-annotated pages (after annotation pipeline)
  page_final/                           # RA-vocalized gold pages (never overwritten by pipeline)
```

`page_final/` is the gold corpus — see [vocalization_gold.md](../../.claude/projects/-Users-sinairusinek-Documents-GitHub-YiDraCor/memory/vocalization_gold.md).

## How the scaffolds were created (2026-05-18)

For the 8 editions that had no local folder, the raw PAGE-XML was pulled with:

```bash
cd code
python -m transkribus.sync pull --col 18874 --doc <docId> --out <out-dir>
```

This wrote one `NNNN_<pageId>.xml` per page plus `_manifest.json` into `data/<folder>/page_transkribus_pulled_2026-05-18/`. Requires `TRANSKRIBUS_USER` / `TRANSKRIBUS_PASS` in the environment.

A spot-check of page 5 in each new pull showed that 7 of 8 have line segmentation but no transcribed Unicode (HTR not run), and IshahRaah has no TextLines at all. Downstream annotation can't proceed on these until HTR is finalized in Transkribus.

## Open questions

Tracked in [Editions-Open-Questions.md](Editions-Open-Questions.md) — list of per-edition gaps awaiting the DB expression report, RA inspection, or title-page imagework. Update as items get answered.

## Maintenance

- When a new edition is added on Transkribus, append a row to `editions.csv` with at minimum `title`, `transkribus_doc_id`, `transkribus_collection_id`, `transkribus_url`, then re-run `build_editions_dataset.py`.
- When the RA returns answers about an edition's vocalization conventions, update that row's `rafe` / `vocalization_position` / `speakers_stage_vocalized` cells. Don't scatter the information into separate notes files.
- When HTR is finalized on Transkribus, re-pull into a fresh `page_transkribus_pulled_<new-date>/` directory rather than overwriting the existing one.
- When the DiJeSt xlsx or the PerformanceEvents report is updated, just re-run `build_editions_dataset.py` — no manual edits to `editions.json` / `editions_flat.csv`. The script picks up the most recent `PerformanceEvents_Report_*.xlsx` by filename.
- If an edition fails to resolve an `expression_id` (script prints `UNMATCHED`), either add a row to the catalogue's Lateiner Plays sheet or add a `doc_to_eid_overrides` entry in the script.
