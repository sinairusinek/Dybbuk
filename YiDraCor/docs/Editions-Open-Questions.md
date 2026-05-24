# Open questions / data gaps for the 16 editions

Working list of things we don't know yet, organized so they can be answered piecewise — from the forthcoming DB expression report, from RA work, or from inspecting title-page images.

Last reviewed: 2026-05-19.

## 1. Awaiting the DB expression report

A DB-side report of **expressions** is supposed to carry the publication-level details for these editions (publisher, place, printer, exact print year). When it lands, the fields below should fill themselves.

For each edition: what's still blank in `data/editions.csv` after the 2026-05-19 title-page extraction pass.

| Edition | docId | eid | year_printed | publication_place | publisher | publisher_place | printer | printer_place |
|---|---|---|---|---|---|---|---|---|
| Yudale der Blinder | 828539 | 3927 | 1908 ✓ | — | — | — | — | — |
| Ezra | 828481 | 3875 | 1908 ✓ | — | — | — | — | — |
| Blimele (di Perle von Warsha) | 828455 | 3801 | 1903 ✓ | — | — | — | — | — |
| Bas Sheva | 828443 | 3798 | — | — | — | — | — | — |
| Hinke Pinke | 820969 | 3877 | — | — | — | — | — | — |
| Sore Sheyndel | 820964 | 3833 | — | — | — | — | — | — |
| Dovid's Fidele | 820845 | 3869 | 1904 ✓ | — | — | — | — | — |
| Dos Yudishe Herts | 820841 | 3866 | 1910 ✓ | — | — | — | — | — |
| Meshumed | 534187 | 3879 | — | — | — | — | — | — |

The 7 plays not in this table (Mishke Mashke, DerMann, Di Seder Nakht, Das Yudishe Kind, Al Naharot Bavel, Kidush Hashem, Isha Raa) have at least partial publication info filled — confirm against the DB report when it arrives.

## 2. Per-edition flags that need a human eye / DB cross-check

- **Kidush Hashem (3892):** title-page Yiddish year reads `תרפש` in the RA-corrected text — not a valid Hebrew year abbreviation. Most likely OCR/correction error for **תרס״ט = 1908/09**. Need to: (a) eyeball page 1 of the image and (b) check the DB.
- **Mishke Mashke (3838):** title page reads תרע״א (= 1910/11). I set `year_printed = 1911`. DB may have it as 1910 — pick one canonical year.
- **Der Mann untern Tisch (3817):** title page has *no publisher named*, only the printer (Druk ha-Tsfira, Panska 40, Warsaw). Confirm the DB doesn't list a separate publisher.
- **Das Yudishe Kind (3867):** catalogue Lateiner Plays sheet flags this as `certainty: false ascription`. Decide whether to keep in the Lateiner corpus or split off.
- **Isha Raa (3884):** not present in `TranskribusPlayStatus`; eid resolved manually via `doc_to_eid_overrides`. Add a TranskribusPlayStatus row so the join is automatic.
- **Yudale der Blinder (3927):** local `page_final/` starts at page 5 — no title page in the RA-corrected set. Need either (a) RA to also vocalize p.1-2 or (b) DB metadata directly.
- **Meshumed (534187):** the catalogue ties this docId to the play key *"Goles Rusland"* (a different title). Confirm whether the 1908-era printed Meshumed and the catalogue's *Goles Rusland* are the same expression or two distinct editions.

## 3. Systematic catalogue gaps

These are not per-play but corpus-wide; fixing them once would unlock multiple editions:

- **Score-print-editions / documentsitems sheets** in `DybbukCatalogue May2024.xlsx` have `work id` empty for all 16 of our docIds. If those rows exist in the DB with `work id` populated, the next xlsx export will join automatically. Otherwise add `work id` to the existing rows.
- **PerformanceEvents — venue discrepancies:**
  - Sore Sheyndel: DB has empty `Held at` for 3 of 4 events; the spreadsheet `Lateiner hafakot` has `Roumanian Opera House|Central Theatre`, `Vilna`, `Vilna`. Reconcile.
  - Meshumed: DB event venue empty; hafakot has `Roumanian Opera House|Central Theatre`. Reconcile.
- **PerformanceEvents — cast layer empty:** `Actor (Character)` 0/454 filled; `Person (Professional Role)` 1/454 filled. Either these columns are not yet populated in the DB or they live elsewhere.
- **People DB linkage:** `editions.json[*].expression.author_id` is `683` for all 16 (= Lateiner), but the catalogue `people` sheet has no `ID = 683` row — the column is empty, so author identification only works by name. We're holding off on persons until the live people DB is provided.

## 4. Catalogue-title alignment

Several of our editions have a printed title that differs from the catalogue's `English Name` / `Yiddish Name`:

| Our edition title | Catalogue title | eid |
|---|---|---|
| Mishke Mashke | Di grinhorns | 3838 |
| Yudale der Blinder | Yidele oder der emes un sheyker | 3927 |
| Hinke Pinke | Gabriel oder di libe fun a yidisher froy | 3877 |
| Sore Sheyndel | Di farblondzhete neshome | 3833 |
| Meshumed | Goles Rusland | 3879 |

These are not bugs — the print edition simply uses a subtitle or alternate title as its main title — but it's worth surfacing the catalogue's preferred title alongside ours in the TEI header (`<title type="alt">…</title>` style).

## 5. Workflow

When the DB expression report arrives:

1. Add the columns to `data/editions.csv` row-by-row (or write a loader in `code/build_editions_dataset.py` that consumes the report directly and merges by docId / expression_id).
2. Re-run `python3 code/build_editions_dataset.py` — `editions.json`, `editions_flat.csv`, and `editions.md` regenerate.
3. Cross-check each entry in §2 against the new data and tick off here.
