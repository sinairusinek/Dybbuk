# Prompt — validate the 1,137 "dodgy" place QIDs

Paste everything below the line into a fresh Claude Code session opened in
`~/Documents/GitHub/Dybbuk`.

---

## Task

Validate every place link in `Zylbercweig/zibn-shtern/data/working/places_unified_corrected.csv`
whose `qid_source` is **`dodgy`**: 1,137 attestation rows, **582 distinct QIDs** (1,106 `place`,
19 `country`, 7 `province`, 5 `settlement`). They came from the original extraction and were
never checked. Fix what Wikidata plus the Leksikon text can decide. Send only true dead ends to
the RA Maaty, through the Kimatch review queue. Then rebuild, push, and give me live links.

## Why: what the 2026-11-15 audit found

A Wikidata label check of the gazetteer found 85 suspect QIDs. 50 of the bad ones were
`dodgy` rows. Typical failures:
- **Wrong item, right label**: Poltava → a German lake, Łomża → "public health",
  Kolomyia → "baptistery", Land of Israel Q23793 → "bridge" (Palestine is Q23792; several are
  off by one or two digits). Chashniki → an album, Koidanov → a hill.
- **French communes for Eastern European towns**: Grodzisk → Gugney-aux-Aulx, Terespol →
  Saint-Carné, Komarno → Sombrin, etc.
- **Deleted Wikidata items** (`missing` in wbgetentities): Ostrołęka, Ciechanów, Brăila, Prešov…
- **Right QID, wrong English label** in our data (Olomouc labelled "La Pernelle"). This is a
  label fix only.
- **Kima's own WikiData_Id can be wrong** (Kima 20696 Turiisk → Tavriysk Q760227; 14894
  Dzyarzhynsk → a hill). So a Kima match is not proof.

Everything found so far is fixed and recorded. Read these first:
- `Zylbercweig/zibn-shtern/data/reference/place_qid_overrides.tsv` — the fix lever (62 rows).
  22 of the 582 dodgy QIDs are already covered; skip them.
- `Zylbercweig/zibn-shtern/data/reference/gazetteer_wikidata_label_audit_2026-11-15.tsv`
- Memory: `project_dybbuk_and_france_dossiers.md`, `feedback_kg_rebuild_hazards.md`,
  `project_zylbercweig_place_review_app.md`, `feedback_push_and_give_live_links.md`.

## Method

1. **Automatic pass (no Claude subagents; batch Wikidata API calls).** For each distinct QID:
   `wbgetentities` (labels en/yi, descriptions, P31, P17 country, P625 coords, redirects/missing).
   Use a generic User-Agent, batches of 50, ≥2 s between calls and backoff on HTTP 429 — Wikidata
   rate-limits hard. Use P279 subclass lookups where needed.
   Classify each QID as:
   - **OK**: P31 is a human settlement / admin unit / region / neighbourhood, AND the country
     (or coords) fits the attestation's Yiddish province/country (gubernia → modern country:
     Poland, Ukraine, Belarus, Lithuania, Latvia, Moldova, Romania…; America → US; etc.).
   - **LABEL**: right item, but our `wikidata_label_en` / `place_type` disagree → label fix.
   - **WRONG**: non-place P31 (lake, concept, album, galaxy, person, disambiguation…), or a place
     in an implausible country (France for a Galician shtetl), or a deleted item.
2. **Candidate search for WRONG.** Search `wbsearchentities` by our `wikidata_label_en` and by the
   Kima romanization, constrained by the province/country stated in the entry. Read the entry
   text around the spelling (`people/entry_texts.tsv`; attestations map via
   `toponyms_attestations.csv`, `P-<entry_id>`). Phrases like "near Uman", "Kalisz gubernia" or
   "Marmarosh county" usually decide it. Take a candidate only when its type, country and context
   all agree.
3. **Check Maaty's existing decisions first.** Fetch
   `kimatch_decisions_full.json` from the Kimatch repo's `data` branch:
   `gh api "repos/{owner}/{repo}/contents/data/zylbercweig/kimatch_decisions_full.json?ref=data" --jq .content | base64 -d`
   (run in `~/Documents/GitHub/Kimatch`). Never contradict a decision silently: resolve her Kima id
   to a QID. If Kima's WikiData_Id is the error, keep her place, fix the QID, and add the Kima record
   to `data/working/kima/kima_wikidata_corrections_*.tsv`.
4. **Write fixes** as rows in `place_qid_overrides.tsv` (cols
   `corpus record_id source_value qid label_en lat lon note`):
   - `qid  *  <wrong QID>  <right QID> …` remaps the QID everywhere.
   - `label  *  (blank)  <QID> …` is a label/coords fix.
   - `person  <entry_id>  <spelling>  <QID> …` is ONE record only (use it when the same spelling
     means different places in different entries, e.g. Strasburg/West Prussia vs Strasbourg).
   Put the evidence in `note` (what it was, the Yiddish spelling, the context phrase).
5. **Residue → Maaty**: true dead ends only (no Wikidata/Kima candidate the text supports).
   Append to `data/working/kima/audit_gazetteer_qid_residue_<date>.tsv` (same columns as the
   2026-11-15 file, with a `note` that asks the concrete question). Run
   `scripts/build_judgment_review_queue.py` (it reads that file). Check the Kimatch queue diff:
   only the new rows should be added, and no existing row (or its decision) dropped. Commit and
   push the Kimatch repo.

## Rebuild (exactly as on 2026-11-15)

- **Gazetteer spine — PIN the org review**, or ~134 unrelated attestations ride along:
  ```python
  # run from Zylbercweig/zibn-shtern
  import importlib.util; from pathlib import Path
  spec = importlib.util.spec_from_file_location("but", "scripts/build_unified_toponyms.py")
  m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
  m.ORG_REVIEW = Path("<scratch>/org_review_pinned.tsv")   # git show 117707299:Zylbercweig/organizations/org_alignment_review.tsv
  m.main()
  ```
  Snapshot `toponyms_attestations.csv` / `toponyms_gazetteer.csv` first. Diff by `attestation_id`:
  every changed row must trace to an override. Overrides fill missing coords and never overwrite them.
- **KG**: snapshot `Zylbercweig/plays/kg/*.tsv`, then `python3.11 build_kg.py --execute`. Diff the
  edges by content, ignoring `edge_id` and mapping minted `UP-/UO-/UPL-` ids to their labels.
  Expect only place re-targets.
- **Downstream**: `plays/build_kg_viz.py`; `organizations/build_person_org_graph.py` (check
  "edges by source" keeps eponym rows); `organizations/build_org_cluster_kg_backlog.py`;
  YiDraCor `code/build_entity_graph.py` + `build_entity_tree_data.py` + `build_schema_tab.py`;
  `plays/france_dossier/kg_slice.py` + `build_france_dossier.py`;
  `plays/person_dossier/build_person_network.py / build_person_itinerary_data.py /
  build_person_pages.py --config rumshinsky.json`.
- **Cited ids**: grep `docs/` for `E-\d{5}` and `UP-\d{4}`. Every hand-written page citation must
  still point at the same content. Regenerate generated pages and remap hand pages by content.
- Commit with explicit paths (paths with spaces: use `--pathspec-from-file`). `git fetch` first;
  another session may share this checkout. Push, then give me **live links**, never file paths.

## Deliverables

- Override rows + rebuilt spine/KG, pushed.
- An updated audit TSV (`…_dodgy_validation_<date>.tsv`) with a per-QID verdict
  (OK / LABEL / FIXED→Qx / MAATY / KIMA-ERROR) and the evidence.
- Kimatch queue rows for the residue (pushed), and Kima corrections appended.
- A short report: counts per verdict, notable cases, anything that needs my policy call.
- Update memory (`project_dybbuk_and_france_dossiers.md` → or a new
  `project_dodgy_qid_validation.md`) and the MEMORY.md line.
