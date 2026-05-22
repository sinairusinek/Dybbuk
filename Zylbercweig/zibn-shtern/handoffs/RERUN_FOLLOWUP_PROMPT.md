# Re-run FOLLOW-UP prompt — finish the unfinished steps

The first re-run attempt (see RERUN_SESSION_PROMPT.md) only completed the **Fischer**
track. This prompt covers what's still outstanding. Open a session in the **Dybbuk repo**
and paste the block below.

## State as of this writing (verified across both repos)
- **Engine fix: DONE** in `~/Documents/GitHub/Kimatch` (2026-05-22): vocalization-aware
  Hebrew/Yiddish translit + A/B/C grading + safety guards (`7b0ca01`), geo-plausibility
  guard (`ff5ecfa`, `d563cd6`), CSV-header union fix (`53a33cb`). Live via the editable venv.
- **Fischer: DONE** in Dybbuk (`e4c5c1a`, `bfb4003`): match, per-UID consolidation,
  conflict resolution, variant + external-ID donations under `data/working/kima/fischer/`.
- **NOT done:** Zylbercweig re-run+rebuild, YIVO re-match, geo-guard job config, the
  validation audits, and cross-dataset donation dedup.

## IMPORTANT correction to the original prompt
The geo guard shipped as **Signal 1 only** — coordinate distance, enabled by a single
job key `thresholds.max_plausible_km` (a number, or `true` → 300 km default). It flags
`geo_implausible` and demotes only when **both** the input row and the chosen Kima place
have coords. The handoff's **Signal 2** (country allow-list / `region_countries` /
`neighbors`) was **not implemented** — do NOT add a `geo` block; it has no effect.
Consequence: the guard helps coord-bearing gazetteers (YIVO ~90% of rows) but does
little for coord-less inputs (most Zylbercweig text mentions; check Fischer).

---

```
Finish the post-engine-fix re-run. The Kimatch engine fix is live via
~/Documents/GitHub/Kimatch/.venv (run `.venv/bin/kimatch doctor` to confirm). Fischer is
already done; do the rest. Work in this Dybbuk repo, on a branch.

READ FIRST: handoffs/RERUN_FOLLOWUP_PROMPT.md (this file, incl. the geo Signal-1-only
correction), handoffs/REVIEW_ROUTING_INVENTORY.md, handoffs/DYBBUK_TOPONYM_AUDIT_HANDOFF.md.

STEP 0 — confirm the fix is live (Kimatch venv python), else STOP:
  טראָי→Troy (≠Tarai), קובאַ→Cuba (≠Quba), שול→not-a-place; and a far exact-name hit
  (אַפּט→Apt France) flags geo_implausible when coords are present.

STEP 1 — geo config (Signal 1 only). In ~/.claude/skills/kimatch/jobs/yivo_yiddishland.json
  set thresholds.max_plausible_km: 300. Do NOT add region_countries/neighbors (unimplemented).
  Leave fischer_gazetteer.json as-is unless its input rows carry coords (then set it too).

STEP 2a — Zylbercweig re-run + rebuild (the dataset the vocalization fix most helps):
  scripts/kimatch_match.py --full (read its docstring), then scripts/build_unified_toponyms.py.
  PRESERVE the hand-curated unlinked_confirmed.tsv + places_unified_corrected.csv (they hold
  the 14 prior one-off corrections). After rebuild, reconcile: corrections the engine now
  handles systematically can be retired from unlinked_confirmed.tsv, but verify case-by-case.

STEP 2b — YIVO re-match: re-run yivo_yiddishland_gazetteer.enriched.csv via the kimatch
  skill + yivo_yiddishland.json; regenerate yivo_yiddishland_kima.{A_autolink,B_review,
  C_review}.csv. The geo guard should now auto-demote the ~15 implausible A-grade hits that
  were hand-cleaned last time (אַפּט→Apt France, נאָבל→Nabeul, קריטי→Crete…); confirm it did.

STEP 2c — Fischer spot-check (don't redo): confirm fischer_matched.* was produced by the
  FIXED engine (commits post-date 7b0ca01 on 2026-05-22, so likely yes) and note that its
  geo guard was Signal-1-limited. If input rows have coords and max_plausible_km was unset,
  consider a guarded re-run; otherwise leave it.

STEP 3 — VALIDATE (the missing point of the whole re-run):
  Re-run scripts/audit_all_links.py + scripts/verify_flags_wikidata.py on the rebuilt
  Zylbercweig attestations. Success criteria:
    - translit-mismatch STRONG count well below the prior 127 baseline,
    - devocalized-collision class (Troy/Tarai, Cuba/Quba) gone,
    - geo_implausible demotions present in the YIVO grades.
  Diff new vs pre-re-run attestations; spot-check that no previously-correct link regressed.
  Record before/after numbers in the commit message.

STEP 4 — cross-dataset donations (Fischer donations already exist; unify):
  Build the gated, deduped donation export from kima_variants_export.tsv + the Fischer
  donation files: include only audit-clean (variant→Kima) pairs, dedup across
  Zylbercweig/YIVO/Fischer by (kima_id, normalized variant), merge attestation provenance.
  Confirm Kima's contribution format via ~/Documents/GitHub/Kimatch/skills/kimatch/references/kima-api.md.

When done: commit per step, merge to main, push.
```

---

## Baseline numbers to beat
- translit-mismatch: 196 flags / **127 STRONG** (pre-re-run) → expect a large drop.
- devocalized-collision class → **zero** (now handled engine-side).
- YIVO A_autolink: ~15/171 were geo-implausible FPs (hand-cleaned last round) → the Signal-1
  guard should now demote them automatically.
