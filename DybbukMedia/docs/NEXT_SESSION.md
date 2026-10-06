# Next session — paste this into a fresh Claude Code session

> Repo: `~/Documents/GitHub/Dybbuk`. Written 2026-10-06 at the close of the
> work-layer session. Read `YiDraCor/docs/work_layer_proposal.md` for the model
> and `DybbukMedia/README.md` for the media catalogue before starting.

## Where things stand

The entity graph has a full WEMI spine and **`YiDraCor/data/entity_gaps.tsv` is
empty** — every entity resolves.

| | |
|---|---|
| nodes | 692 — work 277, song 225, event 64, org 50, person 37, edition 27, place 12 |
| statuses | LINKED 690, MERGED 2 (deliberate tombstones) |
| media | 1091 rows, **964 anchored (88%)**, 136 entities covered |

Relations: `work --realised_as--> edition`, `song --part_of--> work`,
`event --of_work--> work`, `org --published--> edition`,
`org --printed_by--> edition`, `edition --printed_in--> place`.
Cast facts carry a `level` field and may attach to either a work or an event —
coarse-to-fine, never promoted upward (see `work_layer.coarse_to_fine`).

Before changing anything, run both tests:

```
python3 YiDraCor/code/test_work_layer.py     # graph invariants
python3 DybbukMedia/code/test_matching.py    # 28 titles, 10 controls
python3 DybbukMedia/code/verify_manifest.py  # manifest integrity
```

## The work, in the order I'd do it

### 1. Review the 908 PROPOSED media anchors  ← biggest item

Every one is a matcher candidate, **not a fact**. A promotion to `LINKED` must
carry a `reviewer` stamp in the same write (`verify_manifest.py` enforces this).

| count | source | anchors to |
|---|---|---|
| 382 | dorot | work |
| 257 | dorot | edition |
| 111 | album | person |
| 44 | transkribus | edition |
| 44 | dorot | org |
| 42 | transkribus | work |
| 23 | lexicon | person |

Do **not** bulk-promote. Sample each source first: the dorot→work group comes
from title matching and is the most likely to contain errors, while the
lexicon→person group routes through `people_db` db_ids and is the most reliable.
Consider building a small review view rather than editing the TSV by hand —
findings belong in app surfaces, not side-channel docs.

### 2. The 32 unanchored play titles (104 rows)

Most look like near-misses the matcher should reach, not absent plays — a good
sign that a few more folds would pay off, but **check each against the catalogue
before widening anything**: this session twice proved an "absent" claim wrong.

```
 13  Blihmele                     ← almost certainly 3801 Blimele; spelling only
 10  Nebukhdanetser melekh babel  ← 3961 exists (Nebukhdanotser meylekh bvl)
  9  Shlekhte-Froy
  9  Soroh Sheyndel               ← 3833 Di farblondzhete neshome? cf. the
                                    confirmed alt-title Sore Sheyndel
  8  Di tsigaynerin               ← 3952 (3850 merged into it)
  6  Shlomo Gargel
  6  Atalyahu/Etliyahu
  4  Akeida / Don Yosef Abarbanel / Mamon der geldgot
```

Several rows are not titles at all — `Play title`, `(in a separate sheet!)`,
`kom` — and should be marked `REJECTED` rather than chased.

### 3. Fetch the images — nothing is downloaded yet

All 1091 rows are `status=PENDING` and `local_path` is empty everywhere.

- **Dorot**: NYPL permalinks are stable and serve IIIF by item UUID. This is
  the most automatable source — write a fetcher into `media/dorot/`.
- **Album / Lexicon**: the page scans are already local; these need *crops*.
  Each row records the plate or facsimile it comes from.
- **JPRESS**: 27 search tasks, deliberately manual — links are unstable and
  often miss the target notice. Sinai wants to do these together.
- **Rights**: 1035 rows are `unknown`. NYPL is PD-with-credit; the YIVO and LOC
  rows need checking before anything is published.

### 4. `KhurbnYerusholaim-BN1908` has no CONFIG block

Newly recognised as a real edition this session (the 1908 Biblioteka Narodowa
print, 56 pp, Polona 96221303), so `build_tei.py --play KhurbnYerusholaim-BN1908`
fails. It also has only one IN_PROGRESS transcript, so the text is not ready
either. Work 3891 is the corpus's **only work with two witnesses** — encoding
this one would make that visible in the TEI layer too.

### 5. Deferred research tasks

- **Adaptation lineage** — 30 works name a source in `adapted_from_raw`
  (Dumas, Strindberg, Goldfaden, Nordau, Halle-Wolfsohn). Values are hedged
  prose, so turning them into entities is research, not a transform.
- **Which layer DraCor itself receives.** Decided: one file per edition, and
  every TEI now carries `<idno type="work">`. Still open is whether DraCor
  should get one file per work instead — an editorial call about base witnesses.

### 6. For the org-alignment track (not this repo's graph)

- **core_db 70** reads `D. Salat` against Yiddish `א. סאלאט` (= *A.* Salat),
  while the Kidush Hashem title page says **E.** Salat. One of three initials
  is wrong.
- **`הצפירה` 68 and `HaTsfira` 270** look like a duplicate pair; 68 carries no
  `org_type` despite being a printer with a matching address (Panska 40).
- **No YIVO item ID for MS_Emigration** — `Perlmutter 628` is a collection-level
  reference, and the CJH record (000133864) carries no RG number.

## Things that will bite you

- **Never `rstrip('.0')` on openpyxl float ids** — it eats real trailing zeros
  (`4010.0` → `401`) and fabricates missing records. Use `str(int(float(v)))`.
- **Probe both scripts before saying a name is absent.** Exact matching produced
  a false "absent" claim three times in one session (printers, Ben Hador,
  Uriel Acosta). Romanisation differs between sources.
- **Verify a control is absent before using it in a test.** Most of the original
  matcher controls turned out to be real Lateiner works.
- **Harvesters never overwrite human-owned fields** (`entity_ids`,
  `link_status`, `reviewer`, `local_path`, `status`, `crop_of`, `notes`). They
  report how many they protected. Never truncate-and-rebuild the manifest.
- **`entity_ids` and `link_status` are one fact.** `reconcile_link_status()`
  keeps them consistent; a re-run can otherwise fill one and leave the other
  stale.
- Run the pipeline under **python3.11**, not the repo's `.venv` (3.9).
