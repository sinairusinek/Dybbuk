# Kimatch place-review pipeline — full state & handoff

Built across sessions 2026-05-22/23. This is the end-to-end loop for reviewing
Zylbercweig place links in the **Kimatch app** and applying the decisions back to
the canonical Zylbercweig data. Open a new session with this file for context.

## The two repos

- **Kimatch** (`/Users/sinairusinek/Documents/GitHub/Kimatch`) — the review app +
  generic review engine. Deployed on Streamlit Cloud, installed **editable**
  (`requirements.txt` = `-e .`), so `kimatch/**` changes deploy on push without a
  version bump. Decisions persist to the repo's **`data` branch**
  (`data/zylbercweig/kimatch_decisions_full.json`, gitignored on main; fetched/
  pushed via the GitHub contents API).
- **Dybbuk** (`/Users/sinairusinek/Documents/GitHub/Dybbuk`) — `Zylbercweig/zibn-shtern`
  holds the queue builder, the apply script, and the unified-toponym pipeline.

## The loop

1. **Build the review queue** → `scripts/build_judgment_review_queue.py`
   Writes `<Kimatch>/data/zylbercweig/kimatch_review_full.tsv`. Bakes per row:
   `fuzzy_candidates` (Kima candidate cards), `contexts` (Yiddish lexicon text),
   and a `mentions` column (per-mention breakdown grouped by source_record_id,
   each with entry `head` + windowed ctx + `full` entry text). Sources: the
   disambiguation/punchlist/ambiguity-audit TSVs under `data/working/kima/` +
   the attestation spine + the Lexicon XML + `organizations_clustered.tsv`.

2. **Review in the app** → Kimatch `ui/pages/zylbercweig_review.py`
   (backend `kimatch/review/zylbercweig_backend.py`, generic page
   `kimatch/review/page.py`). Filters: Suggested-match(fuzzy)/No-Match/Ambiguous/
   Decided. Features: Kima-style candidate **cards** (green when selected; in
   per-mention mode pressing a card applies to all mentions), one-click
   **confirm-QID** for no-match items, **per-mention** decisions with apply-to-all
   + **✂️ Split** + **🚫 Not a place (unlink)**, header **Wikidata link + description
   + ✏️ Change-Wikidata-entity / clear-link** popover, **Decided** = read-only table
   (Place/Decision/Decider/Comment). Decisions → `data` branch JSON.
   Reviewers drill here; ~149 decisions as of 2026-05-23.

3. **Apply decisions to canonical data + rebuild** → `scripts/rebuild_corrected.py`
   (the orchestrator — see workflow.md §4). It chains:
   `auto_reclassify.py` (regenerates `places_unified_corrected.csv`) →
   `apply_translit_audit_fixes.py` → `apply_kimatch_review_decisions.py --apply` →
   `build_unified_toponyms.py`. **Run this, not auto_reclassify alone** —
   auto_reclassify overwrites corrections, so they must be re-applied after.
   `--no-regen` skips step 1 (re-apply + rebuild only).

## apply_kimatch_review_decisions.py — routing

Fetches decisions from the `data` branch via `gh` (private repo). Routes each:
- **person** corpus → rewrites/clears `qid` in `places_unified_corrected.csv`
  (per-mention by `entry_id`, flat by `source_value`; `qid_source=kimatch_review_…`).
- **org** corpus → `kima/review_applied_org_qids.tsv` (4-col: source_value,
  record_id, qid, **action**[link|unlink]) — org QIDs aren't stored in the unified
  CSV; `build_unified_toponyms.py` consumes this handoff (highest-priority org
  resolution; `unlink` forces link_status=unlinked).
- **split** → `kima/review_split_punchlist.tsv` (route to the QID-exploder).
- **unlink** → clears the qid (region / not-a-place; e.g. דרום-רוסלאַנד).
- `no_match_found`/`skip`/`ambiguous` → recorded only (no_match KEEPS a valid WD link).
- `map_to:Q…` (a QID typed as a Kima id) → treated as `wikidata:` (Maidanek).
- Audit log: `kima/kimatch_review_apply_log.tsv` (fresh each run; separate from the
  translit `matching_corrections_log.tsv`).

## Current state (2026-05-23, all merged to main)

- Review queue: 171 rows (125 pre-existing + 46 judgment items; 22 are multi-mention).
- Applied: 86 person qid rewrites + Maidanek + דרום-רוסלאַנד **unlink/cleared**;
  org handoff = 141 rows; spine rebuilt (linked 13592 / unlinked 4431 / misresolved
  19; 178 kimatch_review links).
- **Skipped (no resolvable QID):** 4 Kima places that have no Wikidata link —
  יאָבלאָניצע (75464), סאָקילען (84142), ליאָכאָוויטש & לעכעוויטש (22491).

## Open items / next steps

- **Org-side via handoff is durable**; person-side is "corrections on top" and only
  survives if `rebuild_corrected.py` (not bare auto_reclassify) is used. A parallel
  session's auto_reclassify run wiped it once — now reconciled.
- The 4 no-WD Kima places can't get a QID; decide whether to keep them Kima-only.
- Consider migrating `auto_reclassify.py` to call the corrections itself (vs the
  orchestrator) if reviewers keep running it directly.
- Bulk match-derived queues (review_1/2/3, residuals, YIVO C, etc.) still route to
  **OpenRefine**, not the app — see `handoffs/REVIEW_ROUTING_INVENTORY.md`.

## Deploy gotcha

Kimatch is `pip install -e .` on Streamlit Cloud — `kimatch/**` changes deploy on
push. If a deploy ever serves stale code, **Reboot app** from Manage app. `ui/pages`
and `data/` files refresh without a reinstall.
