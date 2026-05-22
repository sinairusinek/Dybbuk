# Re-run session prompt — three datasets against the fixed Kimatch engine

Open a session in the **Dybbuk repo** and paste the prompt below. This is toponym
handoff **item 4** (re-run after the engine fix lands). Prereq: the Kimatch engine
fixes (§B1 vocalization-aware Hebrew/Yiddish + systematic geo guard) are merged in
`~/Documents/GitHub/Kimatch` and live via its editable venv.

---

```
The Kimatch engine fixes (§B1 vocalization-aware Hebrew/Yiddish + systematic geo
guard) have landed in ~/Documents/GitHub/Kimatch and are live via its editable venv.
Re-run all three datasets against Kima with the fixed engine, validate that the
collision class is gone, then route survivors per the inventory.

READ FIRST:
  Zylbercweig/zibn-shtern/handoffs/KIMATCH_SKILL_HANDOFF.md          (§B1, §A audits)
  Zylbercweig/zibn-shtern/handoffs/KIMATCH_GEO_PLAUSIBILITY_HANDOFF.md (geo config surface)
  Zylbercweig/zibn-shtern/handoffs/DYBBUK_TOPONYM_AUDIT_HANDOFF.md    (item 4 = this re-run)
  Zylbercweig/zibn-shtern/handoffs/REVIEW_ROUTING_INVENTORY.md       (where survivors go)
All work happens in this Dybbuk repo; use the Kimatch venv:
  ~/Documents/GitHub/Kimatch/.venv/bin/kimatch  (run `doctor` first to confirm the
  vocalization fix + geo guard are present).

STEP 0 — sanity-check the engine fix on fixtures (Kimatch venv python):
  Confirm טראָי→Troy (≠Tarai), קובאַ→Cuba (≠Quba), שול→not-a-place resolve correctly
  and that a far-off exact-name hit (e.g. אַפּט→Apt France) now flags geo_implausible.
  If these don't behave, STOP — the venv isn't picking up the fix; resolve before re-running.

STEP 1 — configure the geo guard in the job JSONs (~/.claude/skills/kimatch/jobs/):
  Add thresholds.max_plausible_km=300 + the geo.region_countries / neighbors block from
  KIMATCH_GEO_PLAUSIBILITY_HANDOFF.md to yivo_yiddishland.json. For fischer_gazetteer.json
  use Signal-2 (country allow-list) only — it has no coords-vs-Kima distance. Both signals
  must no-op when unconfigured.

STEP 2 — re-run each dataset:
  a) Zylbercweig: scripts/kimatch_match.py --full (read its docstring), then rebuild with
     scripts/build_unified_toponyms.py. NOTE: build also reads the hand-curated
     unlinked_confirmed.tsv + places_unified_corrected.csv, which carry the 14 one-off
     corrections from the prior session — do NOT clobber them; the rebuild should preserve
     them. Many may now be redundant (engine handles the collision systematically) —
     reconcile, don't blindly trust either side.
  b) YIVO Yiddishland: re-match yivo_yiddishland_gazetteer.enriched.csv via the kimatch
     skill + yivo_yiddishland.json; regenerate the A/B/C grade splits (geo guard now
     demotes the implausible A-grade hits that were the one-off cleanup).
  c) Fischer: first full run, fischer_gazetteer.json on fischer_gazetteer.csv. Then the
     per-UID consistency analysis that's still pending (see project_fischer_gazetteer memory).

STEP 3 — validate (the point of the re-run):
  Re-run scripts/audit_all_links.py + scripts/verify_flags_wikidata.py on the rebuilt
  Zylbercweig attestations. Success = the translit-mismatch STRONG count drops well below
  the prior 127 baseline, the devocalized-collision class is gone, and geo_implausible
  demotions appear. Diff the new attestations vs the pre-re-run version; spot-check that
  no previously-correct links regressed.

STEP 4 — route + donations (per REVIEW_ROUTING_INVENTORY.md):
  Regenerate the review queues; they should be smaller. Then build the gated, deduped
  donation export from kima_variants_export.tsv: include only audit-clean (variant→Kima)
  pairs, deduped across Zylbercweig/YIVO/Fischer by (kima_id, normalized variant), merging
  attestation provenance. Confirm Kima's contribution format via the kima-api.md reference.

Work on a branch; commit per dataset/step. When done, merge to main and push (the
re-run outputs are inputs other sessions depend on).
```

---

## Why STEP 0 matters
The re-run touches 13k+ Zylbercweig attestations plus ~13k gazetteer rows. If the venv
hasn't actually picked up the engine fix, you'd reprocess everything against the old
behavior and not know it. The three collision fixtures + one geo fixture are the cheap
proof the fix is live before committing to the full run.

## Baseline numbers to beat (from the prior session)
- translit-mismatch: 196 flags / **127 STRONG** → expect a large drop.
- devocalized-collision class (Troy/Tarai, Cuba/Quba) → expect **zero**, handled engine-side.
- YIVO A_autolink had ~15/171 geo-implausible FPs (one-off cleaned) → the guard should now
  catch these automatically and demote them to C.
