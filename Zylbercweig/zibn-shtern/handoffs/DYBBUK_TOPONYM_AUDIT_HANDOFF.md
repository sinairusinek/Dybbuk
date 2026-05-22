# Handoff → Dybbuk repo: applying all toponym-matching lessons to ALL the data

Source: same two sessions (2026-05-22). Question this answers: **did we apply every lesson to
all the data in this repo? No — here is exactly what remains.** See
[[project_unified_toponyms]] for full context. All paths under
`Zylbercweig/zibn-shtern/`.

## What WAS applied (this session)
- Audited & fixed the auto-linked `kima_name_exact` set: **437 of 1,837** distinct linked
  toponym spellings. 13 corrections total (`data/working/kima/matching_corrections_log.tsv`).
- Built reusable detectors: `scripts/audit_kima_name_exact.py` (Wikidata yi cross-check) and an
  inline ambiguity scan (spelling → >1 Kima place). Plus the residual resolver/punch-list
  scripts.
- New `link_method` provenance column on every attestation → audits are reproducible & scoped.

## UPDATE — session 2026-05-22 (continued): items 1 & 2 done, item 3 graded

The Kima reference data + phonetic venv live in `~/Documents/GitHub/Kimatch/`
(`20250126KimaPlacesCSVx.csv`, `Kima-Variants-20250929.tsv`, `.venv/bin/kimatch`).
Run the audit scripts with that venv's python.

**Item 1 (generalize audit to ALL link_methods) — DONE.**
`scripts/audit_all_links.py` audits every `link_status==linked` attestation (13,606)
regardless of `link_method`. Two passes: WD-independent **translit-mismatch** (pass A)
and **Kima-ambiguity** (pass B, rebuilt from the real Kima variant index).
`scripts/verify_flags_wikidata.py` is the generalized WD yi cross-check (any
(yiddish,qid) list, not just `kima_name_exact`).

**Item 2 (translit-mismatch detector) — DONE.** Built on the real Yiddish→IPA→DM
bridge. Key design fix: the oracle is **the linked place's OWN Kima names** (Hebrew
variants + Latin romanization), not the modern English label — so legit exonyms
(פּוילן→Poland, לעמבערג→Lviv) and historical renamings (יעקאַטערינאָסלאָוו→Dnipro) pass,
while a spelling matching none of the linked place's names is flagged.
Output: `data/working/kima/audit_translit_mismatch.tsv` (197 flags / 127 STRONG).

**Triage + fixes applied (14 distinct corrections, ~60 attestations) across 3 source files:**
- Class A — person-mention mis-resolution in `places_unified_corrected.csv` (a
  minority of attestations overrode the correct qid_map entry): רוסלאַנד→Russia,
  רומעניע→Romania, אונגאַרן→Hungary, פּוילן→Poland, קאָנאָדע→Canada, באָריסאָוו→Barysaw,
  בריסק ליטאָווסק→Brest(BY), טישמיעניץ→Tysmenytsia, סימפעראָפּאָל/סימפּעראָפּאל→Simferopol.
- Class B — `kima_name_exact` devocalized-collision FPs in `unlinked_confirmed.tsv`:
  טאָמסק→Tomsk (was Jericho), דניעפּראָפּיעטראָווסק→Dnipro (was Villeneuve-d'Ascq),
  האַרלעם→Harlem Q189074 (was Aleppo). Harlem also fixed in the **org collapse** file.
- All logged in `matching_corrections_log.tsv`; attestations rebuilt & re-verified
  (residual wrong-qid = 0). Every guessed QID was verified live against Wikidata
  first (this caught e.g. Tomsk≠Q970/Comoros, Simferopol≠Q3953/Kalmykia).
- `מעזריטש` (handoff flag) caught by pass B (links to the Ghetto, not the town).
  `סעלץ`→Frederick and `לונאַ`→Lonavala are `needs_review` (not linked) so they don't
  pollute linked data — left parked with their wrong suggestions.

**Item 3 — graded, not force-resolved.** New `audit_translit_review_punchlist.tsv`
(17 uncertain settlement→settlement flags w/ hypotheses + grades A/B/C). The pre-
existing review queues (`review_disambiguation.tsv`, `review_1_confirm.tsv`, …) are
genuine human/corpus-judgment calls (Williamsburg/Troy/Newark = design Q-D) and were
deliberately left for a human rather than guessed. The B-grade punchlist rows
(Treysk, Neustadt→Novo Mesto, Rzheka, Szczezhets, Izhbits…) remain for cluster-research.

## What was NOT applied — the gap (with real numbers) [original assessment, pre-update]
Of **1,837** linked toponym spellings, **1,400 were never audited** — everything not linked via
`kima_name_exact`/`audit_corrected`:

| bucket (link_method) | linked attestations | audited? |
|---|---|---|
| (blank) pre-existing / unified / Maaty | 6,086 | **NO** |
| qid_map (internal-index + country/region + grade-A WD) | 2,225 | grade-A type-verified; rest **NO** |
| guberniya_governorate | 384 | curated, not FP-audited |
| adjectival_er_strip | 55 | guarded, not FP-audited |
| kima_name_exact + audit_corrected | 5,603 | **yes** |

Residual FP risk in the 1,400 is **small but real**: a quick scan found **9 Kima-ambiguous** and
**8 very-short (≤3-letter, homograph-risk)** spellings, including likely errors:
`סעלץ → Frederick`, `לונאַ → Lonavala (India)`, `מעזריטש → which Międzyrzec?`.

## TODO for the next Dybbuk session (ordered)

1. **Run the existing audits across ALL linked spellings, not just `kima_name_exact`.**
   Generalize `scripts/audit_kima_name_exact.py` to take any `link_method` filter (or none) and
   re-run; same for the ambiguity scan. Start with the 9 ambiguous + 8 short flagged above —
   confirm/fix `סעלץ`, `לונאַ`, `מעזריטש` first.

2. **Build the transliteration-mismatch detector** (the wrong-city catcher Wikidata can't do —
   see Kimatch handoff A2). Compare each linked spelling's phonetic proxy to its resolved label /
   kima_rom proxy; flag big mismatches. Run over all 12,582 kima-linked attestations.

3. **Work the open review files** (already generated, graded by need-for-review):
   - `data/working/kima/review_disambiguation.tsv` — Newark-vs-NY (provisional Newark; close-read
     via `source_record_id` cluster back-pointers), Cuba/Quba, Troy, Williamsburg (corpus call).
   - `data/working/kima/audit_name_exact.tsv` + `audit_name_exact_ambiguous.tsv` — survivors:
     לידז (Łódź vs Leeds?), טשעלסי, מאָהילעוו, קעניגסבערג.
   - `review_1_confirm.tsv` (53), `review_2_identify.tsv` (107), `review_3_triage.tsv` (349) from
     the residual pass.

4. **After the Kimatch vocalization fix lands** (see KIMATCH_SKILL_HANDOFF.md §B1), **re-run the
   whole match** on the corpus. The de-vocalized-collision class (Troy/Tarai, Cuba/Quba) can only
   be *systematically* prevented engine-side; today's fixes are one-offs. Re-resolution will also
   recover spellings currently in `toponyms_unlinked.csv` that need vocalization to match.

5. **Deferred design questions** (PI to discuss — keep on the list):
   - (D) corpus-influenced disambiguation — bias choices by corpus (Williamsburg→Brooklyn in a
     Yiddish-theatre corpus, not Virginia).
   - (G) historical & **polygonal** locations — modeling places like Königsberg/Kaliningrad whose
     identity/extent shifts over time (and regions vs points generally).

6. **Other repos:** the Hasidigital repo (`~/Documents/GitHub/Hasidigital/`) has its own
   place-authority/matching DB. The same name_exact-ambiguity / homograph / vocalization lessons
   likely apply there — audit it separately. (Out of scope for the Dybbuk data but flagged.)

## Reusable assets in this repo
- `scripts/audit_kima_name_exact.py` — Wikidata yi cross-check, graded.
- `scripts/resolve_residual_wikidata.py` — internal-index + WD resolver, P31 type-verified, A/B/C.
- `scripts/build_residual_punchlists.py` — graded review-list generator.
- `data/working/kima/matching_corrections_log.tsv` — error registry w/ root-cause taxonomy (also
  feeds the Kima donation/contribution task).
- `unlinked_confirmed.tsv` `method` column + attestation `link_method` — the audit/provenance spine.
