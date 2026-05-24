# TODO — TEI structurer & DraCor pipeline

Tracking for the final pipeline stage: annotated PAGE-XML → presentable TEI →
DraCor. Created 2026-05-24 after building the structurer for Di Seder Nakht.
Companion: `data/HANDOFF_2026-05-21.md` (annotation state), memory
`structurer-build-tei`, `diseder-two-part-tei`.

## Status

- [x] **Structurer built** — `code/structure/build_tei.py` assembles
  `data/<play>/page_annotated/*.xml` + `cast_dict.json` + `editions.json` into
  one tei_all TEI. Run: `cd code && python3.11 -m structure.build_tei --play <folder>`.
- [x] **Di Seder Nakht** → `tei/Di-Seder-Nakht.xml` (4 acts, 542 sp, 235 stage,
  33 lg, 16 persons; well-formed; all `@who` valid).
- [x] **Conventions agreed** with PI (2026-05-24) after reviewing the conforming
  `../DybbukXMLeditions/A-Earliest1898.xml`: tei_all (not dracor schema); two-part
  body/back songs with `@corresp`; castList+listPerson from cast_dict; A-Earliest
  xml:id naming; editorial spans pass-through-only. See memory `structurer-build-tei`.
- [x] **Two data gaps flagged for Noa** — added to `data/review/annotation_flags_enriched.tsv`
  (collective `אלע` untagged ×14; song-singer rubrics; coarse appendix lg).

## We are still finalizing the TEIs — open work

### A. Close the Di Seder data gaps (Noa, via review list)
- [ ] **Chorus `אלע`** — add a collective role (`xmlid:alle`) to the castList and
  tag the 14 untagged turns (pp.5,6,9,11,19,31,32,42,43,48,54,63,64,65). Until
  then they merge into the preceding `<sp>` in the TEI.
- [ ] **Song supplement (pp.55-70)** — re-segment one-lg-per-song → per-stanza
  `<lg>` at `1)2)3)` / Roman / `רעפריין` markers; set `cont:yes` on continuation
  pages; add `songGroup` headings for the act-1 and act-4 song groups (only act-2
  p61 / act-3 p67 exist now, so `actSongs @corresp` covers only acts 2 & 3).
- [ ] Re-run `build_tei.py` after each RA pass; re-check `@who` report.

### B. Generalize the structurer to the other finished plays
- [ ] Add `CONFIG` blocks + run for **Der Mann untern Tisch** (already DraCor-shaped
  by hand — good cross-check) and **Mishke Mashke**.
- [ ] Decide per play: scene level? songs inline vs back? (Di Seder is two-part by
  its physical source; most plays are single-part → songs likely inline in body.)
- [ ] Factor out the Di-Seder-specific assumptions (body_last_page, songs routing)
  once a second play is wired, so onboarding a play is just a CONFIG block.

### C. DraCor-adapted version (separate target from TEI-Publisher)
- [ ] Add a `--dracor` mode (or a post-transform) that emits DraCor-strict TEI:
  swap the `<?xml-model?>` PI to `https://dracor.org/schema.rng`; `publisher
  xml:id="dracor"`; complete `teiHeader` metadata DraCor wants (genre/`textClass`,
  Wikidata/`idno` ids, full `bibl`/source, `particDesc` with `sex`/role where known).
- [ ] Validate against the DraCor RNG **and** Schematron (`dracor.sch`) — the
  TEI-Publisher build only checks well-formedness + tei_all today.
- [ ] Map our metadata to DraCor's expected fields (author Wikidata, premiere date
  from `performance_events[]` in `editions.json`, `idno type="wikidata"` per person
  where resolvable).
- [ ] Confirm DraCor's requirement on `<sp>`/`<speaker>`/`<stage>`/scene divs vs
  what we emit; decide whether to introduce a scene level.

### D. TEI-Publisher integration
- [ ] Load `tei/Di-Seder-Nakht.xml` into the YiDraCor app
  (`code/tei-publisher/YiDraCor/`) and confirm it renders (Hebrew RTL, castList,
  sp/speaker, stage, songs in back).
- [ ] Decide `pb/@facs` resolution — currently the Transkribus image filename;
  wire to deployed page images or drop if not served.
- [ ] Optional `<front>` title page from the p3 titlePage region (we skip it now;
  A-Earliest also omitted front matter).

### E. Pipeline hygiene
- [ ] `build_tei.py` validation is internal-only (`@who` ⊆ listPerson). Add an
  optional `--validate` that runs the tei_all RNG (jing/lxml RelaxNG) so the
  structurer fails loudly on schema drift.
- [ ] Document the structurer stage in `README.md` (the "TEI → DraCor transform"
  section still describes only the old Lateiner_Meshumed `|`-delimiter flow).
