# pilot_2024 — the pre-Gemini pilot data, rescued from OpenRefine

In 2024–25, before the 2026 Gemini extraction of the Yiddish lexicon, the project ran two
hand-validated pilots and an org alignment with the Munich T-Migrants database. That work
existed only in Sinai's local OpenRefine workspace. An audit on 2026-11-08 compared all 156
Zylbercweig-related OpenRefine projects, cell by cell, against everything under
`Zylbercweig/`. The 2026-era projects were already covered. The pilot data was not, and
it is exported here.

These are **raw exports**, one TSV per OpenRefine project, as the project's rows stand.
Nothing has been normalised, deduplicated or merged into `people_db` / `core_db`.

## Folders

| folder | what it is | start with |
|---|---|---|
| `private/400entries_validation/` 🔒 (8 files) + `400entries_validation/` (4 public: places & org validation) | ~383 lexicon entries, worked from Steven Lasky's English translations (museumoffamilyhistory.com). An RA extracted and validated personal info, professional life, colleagues, family and migration, with fuzzy date ranges, place ids (DiJeSt `dbIDplace` / Wikidata), person and org ids (`Person DB ID`, `org id` = DiJeSt), and English comments. | `2025-07-06_…validation_8_xlsx…` (colleagues/troupes layer, newest). Personal info: `2024-12-29_…personal_information…`. Migration validation: `2025-01-01_…_5_xlsx…`. Places (public): `2025-01-28_…places_for_validation…` |
| `private/400entries_laski_layers/` 🔒 | The earlier per-layer cut of the same 400 entries (Colleagues, Professional life, Personal info, Family, Migration), plus the entry-id ↔ URL list. Some values exist only here. | the layer you need |
| `100entries_munich_tmigrants/` | 100 entries prepared for T-Migrants (Munich): the same layers plus T-Migrants ids (`AbrBin-742`, `USA-Bost-00`), Wikidata QIDs for places and countries, Ruthie's comments, "add as / DONT ADD" decisions, and external-source links for colleagues. Many near-duplicate versions; each one kept here holds something the others don't. | `private/…/2024-12-28_…Sept29_1_xlsx…` 🔒 (the 122-column merge); public:  `…Colleagues_2…`, `…Personal_Information_2…`, `…Professional_Life_1…`, `…places_1…` |
| `tmigrants_orgs/` | The T-Migrants organisation list (`IDOrganisation`, `inst_name`, `InstType`, `IDLocation`) aligned to DiJeSt org ids (`DiJeSt-ID`). 485 rows are starred. What the star meant was not recorded. | `2025-06-12_organisation_Tmig_zyl_merge_update…` |
| `dijest_db_export/` | The DiJeSt "Persons + Organisations Report" export of 2025-07-06: 436 birth dates, 150 death dates and 87 "Author of Work" lists that `people_db` lacks. | the one file |

`manifest.tsv` lists every file with its visibility, OpenRefine project id and name,
created/modified dates, row count and starred/flagged counts. 🔒 = under the git-ignored `private/`.

## Column conventions added by the export

- `<column>__recon`: the OpenRefine reconciliation match for that cell, as `QID|label|judgment`.
- `__or_starred` / `__or_flagged`: the OpenRefine row marks (1/0). Their meaning was never
  written down, so read them together with that project's comment columns.
- `ID-action`-style and `Column N` columns are the RAs' own working columns. They often hold
  comments, so keep them.

## Public vs private files

The repo is public, and Steven Lasky's English translations are not ours to republish.
So the export is complete (nothing blanked), but **any file containing that text is written
under `private/`, which is git-ignored**. It exists only on Sinai's machine, next to the
OpenRefine originals. That covers 15 files: the 400-entry pilot except its 4 places/org validation sheets, and
the Munich merge `2024-12-28_…Sept29_1_xlsx…` (only 11 Lasky cells, but enough to keep it
out). The other 36 files are public. `manifest.tsv` (public) lists all 51 with their
`visibility` and Lasky-cell count, so the private ones are documented even where they
can't be seen. Regenerating restores `private/` from OpenRefine.

## Ids you will meet

- **DiJeSt**: the project database. Ids appear bare (`698`) or as `id_name`
  (`373_Sabsey Troupe`). Person ids are the same as `people_db.db_id`, and org ids
  are the same as `core_db.db_id` (spot-checked: 131, 178, 244, 373).
  **Place ids are a separate DiJeSt place table** (`727 Odessa (Ukraine)`, `82 Warsaw`,
  in `dbIDplace` / `… validation` columns). They are *not* `core_db` ids, and that table is
  not in the repo (the repo's place authority is the zibn-shtern gazetteer). Most rows carry a Wikidata QID
  as well.
- **T-Migrants**: the Munich partner's ids. People are `AbrBin-742` (name prefix + DiJeSt id),
  places are `USA-NY-00` / `Rus-DiJeSt-127`, orgs are numeric `IDOrganisation`.
- Lasky entry slugs: `abramov/mary`, `yt/lex/A/abramov-mary.htm`, which match
  `people_db.external_source_id`.

## Regenerate

```
python3 export_from_openrefine.py     # reads ~/Library/Application Support/OpenRefine, read-only
```
`projects.py` holds the project list: for each family, the smallest set of projects that
together hold every value not found elsewhere in the repo (recomputed 2026-11-15).
