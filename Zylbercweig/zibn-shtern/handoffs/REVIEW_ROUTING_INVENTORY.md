# Review-routing inventory — toponym review queues

Built 2026-05-22. Purpose: one index of every open review/audit queue, with **where
it should be worked** (OpenRefine/Sheets vs the Streamlit app) and — given the agreed
**engine-fixes-first** plan — whether it is worth working **now** or should **wait for
the post-fix re-run**.

## The key distinction: match-derived vs judgment

- **MATCH-DERIVED** — the file is an output of the *current* Kima/Wikidata match. The
  vocalization fix (skill handoff §B1) + systematic geo guard will **regenerate it**,
  recover currently-unlinked spellings, and remove much of the collision class.
  → **Do not hand-work now.** Re-run first, then route the (smaller) survivors.
- **JUDGMENT** — a corpus/PI/historical decision the engine fix will *not* resolve
  (which Williamsburg? which Newark? historical identity of an obscure shtetl).
  → **Workable now**, unaffected by the re-run.

Routing test for the survivors: *can the reviewer decide from the row alone?*
Yes → OpenRefine/Sheets. Needs the attestation text / map / sibling settlements →
Streamlit (settlement workbench: folium + cluster back-pointers + Research action).

---

## Workable NOW (judgment; survives the re-run)

| file | rows | decision | route | note |
|---|---|---|---|---|
| `kima/review_disambiguation.tsv` | 23 | which place (corpus-influenced) | **Streamlit** | Newark↔NY, Williamsburg (BK vs VA), Troy. Design-Q **D**. The natural Streamlit pilot — needs corpus context + map. PI call. |
| `kima/audit_translit_review_punchlist.tsv` | 17 | historical identity (B-grade) | **Streamlit / cluster-research** | Treysk, Neustadt→Novo Mesto, Rzheka, Szczezhets, Izhbits… genuine identity research; some may also be auto-resolved by §B1. Grades A/B/C inside. |
| `venues_unlinked.csv` | 2,982 | venue/institution → org match | separate **org track** | Not settlements; the org-blocking lever (see [[project_unified_toponyms]]). Independent of the place-match re-run. |

## WAIT for the post-fix re-run (match-derived; will be regenerated)

| file | rows | decision | route once re-run | dataset |
|---|---|---|---|---|
| `kima/review_1_confirm.tsv` | 53 | choose among Kima candidates | OpenRefine | Zylb |
| `kima/review_2_identify.tsv` | 107 | identify unknown spelling | OpenRefine (+Streamlit for context) | Zylb |
| `kima/review_3_triage.tsv` | 349 | triage weak/none | OpenRefine | Zylb |
| `kima/residual_A_autolink.tsv` | 116 | spot-check auto-links | OpenRefine | Zylb |
| `kima/residual_B_review.tsv` | 23 | confirm typed candidate | OpenRefine | Zylb |
| `kima/residual_C_review.tsv` | 486 | weak/none | OpenRefine | Zylb |
| `kima/audit_name_exact.tsv` | 438 | WD-disagree verdicts | OpenRefine | Zylb |
| `kima/audit_name_exact_ambiguous.tsv` | 22 | ambiguity pick | Streamlit | Zylb |
| `kima/audit_ambiguity_all.tsv` | 18 | ambiguity pick (all methods) | Streamlit | Zylb |
| `kima/audit_translit_mismatch.tsv` | 196 | wrong-city flags | OpenRefine | Zylb |
| `kima/verify_translit_strong.tsv` | 139 | WD verify of strong flags | OpenRefine | Zylb |
| `toponyms_unlinked.csv` | 630 | unmatched spellings | OpenRefine (residue) | Zylb |
| `yivo_yiddishland_kima.A_autolink.csv` | 151 | spot-check (geo-cleaned) | OpenRefine | YIVO |
| `yivo_yiddishland_kima.B_review.csv` | 38 | confirm candidate | OpenRefine | YIVO |
| `yivo_yiddishland_kima.C_review.csv` | 3,102 | bulk triage | OpenRefine | YIVO |
| Fischer gazetteer | 9,723 | (full run pending) | — | Fischer |

---

## What this implies for the plan

1. **Don't hand-work the ~6,000 match-derived rows now** — the re-run shrinks and
   reshapes them. The biggest single pile (YIVO `C_review`, 3,102) is squarely in this
   bucket.
2. **Pre-re-run, only three things are worth touching**, all judgment:
   `review_disambiguation` (23, Streamlit pilot), the translit punchlist (17), and the
   `venues_unlinked` org track (independent workstream).
3. **OpenRefine vs Streamlit splits cleanly by decision type, not dataset.** Every
   bulk candidate-pick / spelling-fix queue → OpenRefine (export via
   `export_review_for_openrefine.py`). The small context-dependent disambiguation set →
   Streamlit settlement workbench.
4. **Donations gate** (separate from review): export only audited-clean (variant→Kima)
   pairs from `kima_variants_export.tsv`, deduped across Zylbercweig/YIVO/Fischer by
   (kima_id, normalized variant). Best done *after* the re-run so the donated variants
   reflect the corrected links.

Sequencing agreed 2026-05-22: **engine fixes (§B1 vocalization + systematic geo guard)
→ re-run all 3 datasets → assemble review + curate donations once.**
