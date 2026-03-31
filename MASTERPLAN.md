# Zylbercweig Lexicon — Master Plan: Organizations, People & Places

## Overview

Three parallel workstreams feed a single relational DB from the Zylbercweig Lexicon extraction (Colab `volume2JSON` notebooks → XML → JSON → TSV):

| Workstream | Repo | Status | Current priority |
|---|---|---|---|
| **A. Organizations** | Dybbuk | Ready for implementation | **Active** |
| **B. People/Prosopography** | Dybbuk | Data extracted, not organized | Next |
| **C. Places/Gazetteer** | zibn-shtern | Early — schema design | Parallel |

---

## Workstream A: Organizations

### Data landscape
- **Extraction**: `organizations2026-03-08.tsv` — org mentions from biographical entries (Yiddish, 7 volumes)
- **Existing DB**: ~400 orgs in relational DB (English/romanized). Report: `Organisations-Report-20260205-2154-xlsx (1).tsv`
- **Alignment work**: `Organisations_Alignmenet.xlsx` — Tab 1: dedicated org entries; Tab 2: DB report
- **OpenRefine clustering**: partially done, visible in `clustered organization` column
- **Source JSON**: `Zylbercweig_extraction/volume*IIIorg.json`

### Two populations
1. **Dedicated org entries** (~dozens): Organizations with their own Lexicon entries
2. **Mentioned orgs** (~hundreds to low thousands): Organizations referenced in people's biographies

### Phases

**Phase A1: Classify — Names vs. Descriptive Terms**
- Build Python classifier using heuristic signals (title field presence, generic patterns, possessives, plurals)
- Tag rows: `proper_name` / `descriptive_term` / `ambiguous`
- RA reviews ambiguous cases in spreadsheet

**Phase A2: Cluster — Deduplicate Proper Names**
- Yiddish-aware fuzzy clustering (normalize ו/וו, בּ/ב, etc.)
- Block by org_type + settlement; Levenshtein/Jaro-Winkler matching
- Merge with existing OpenRefine clusters
- Tag descriptive terms with `generic_category`

**Phase A3: Align — Reconcile with Existing DB**
- Transliterate → match against DB (exact + fuzzy + type mapping)
- Build Streamlit review app for RA (reuse TheAlbum patterns)
- Harmonize type taxonomies (extraction → DB)

**Phase A4: Enrich — Populate DB**
- Create new org entries; link person–org relationships; cross-reference dedicated entries

### Key decisions
- Descriptive terms are NOT authority records
- Dedicated org entries are the alignment anchor
- Bilingual canonical names (Yiddish + romanized)
- Non-theatre orgs (factory, workplace) preserved but flagged

### Type taxonomy mapping
| Extraction `org_type` | DB `Organization Type` |
|---|---|
| theatre | Theatre |
| troupe | Traveling Company / Company on Tour |
| school | (new type or Society/Union) |
| publisher | (new or Journals/Newspapers) |
| newspaper | Journals/Newspapers |
| factory | (non-theatre — flag separately) |
| group | Amateur / Society/Union |
| organization | Society/Union |
| union | Society/Union |
| radio | Media (Radio/Film) |
| workplace | (non-theatre — flag separately) |

---

## Workstream B: People / Prosopography

### Status
- Extraction done: `volume*IIIorg.json` — names, dates, family background, education, org relationships
- External sources not yet in repo (YIVO, Wikidata, other encyclopedias)
- Not yet systematically organized

### Phases (to be detailed)
- **B1**: Person entity resolution — dedup across volumes, handle name variants
- **B2**: External source alignment — Wikidata, YIVO, persistent identifiers
- **B3**: Relationship mapping — person–org, person–person, person–place, person–work
- **B4**: DB population — depends on Org A4 and Places C3

### Key files
- `Zylbercweig_extraction/volume*IIIorg.json`, `credited persons.xlsx`, `categorizations.xlsx`, `categorizing relations.xlsx`

---

## Workstream C: Places / Gazetteer (zibn-shtern repo)

### Status
- Early stage — gathering sources, defining schema
- Extraction: `Zylbercweig-Extraction2026-02-05-places.tsv` with partial Wikidata reconciliation
- Cemetery data: `Zylbercweig-Extraction2026-02-05cemeteries-tsv.xlsx`

### Phases (to be detailed)
- **C1**: Gazetteer schema (Yiddish name, romanized, coordinates, Q-ID, GeoNames, temporal validity)
- **C2**: Source integration — complete Wikidata reconciliation, integrate venue data from org extraction
- **C3**: Cross-reference with orgs and people

---

## Cross-Workstream Coordination

### Dependency map
```
INDEPENDENT (parallel now):        SEQUENTIAL:                CONVERGENCE:
  Org A1-A2 (classify/cluster)       Org A3 ← A2              Org A4  ──┐
  People B1 (person dedup)            People B2 ← B1           People B3-B4 ──┤→ DB
  Places C1 (schema)                  Places C2 ← C1           Places C3  ──┘
```

### Shared infrastructure
1. **Type taxonomies**: Org types, relation categories, place types — consistent across extraction and DB
2. **ID scheme**: Stable internal IDs for persons, orgs, places before DB population
3. **Transliteration conventions**: One romanization standard across all workstreams
4. **Streamlit review app**: Multi-view tool for org alignment, person dedup, place reconciliation

### Recommended work order
1. **Now**: Org Phases A1-A2 + Places C1
2. **Next**: Org Phase A3 (RA alignment) + People B1 (person dedup)
3. **Then**: Converge — Org A4 + People B3-B4 + Places C3 → DB population
