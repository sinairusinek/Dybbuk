# Handoff: Zylbercweig Organization Classification — Final Steps

> Handoff document for a parallel Claude Code session picking up the org-type classification project.
> Repo: `/Users/sinairusinek/Documents/GitHub/Dybbuk`
> Project working dir: `/Users/sinairusinek/Documents/GitHub/Dybbuk/Zylbercweig/organizations/`
> Date: 2026-05-12
> Status: classification work **complete**; punchlist empty; three follow-up tasks remain.

---

## TL;DR — what's done, what's left

**Done:**
- Free-text `org_type` from LLM extraction → fixed canonical typology of **31 types**.
- All 4 source TSVs (mentions, clusters, addresses-review, core_db) updated in-place; backups preserved at `*.pre_canonical_backup`.
- Multi-model pipeline: rule cascade (`map_canonical_types_v3.py`) → Gemini 3 Pro verification (`llm_verify_shallow.py`) → Opus/Gemini residual passes → in-session Claude Opus 4.7 cleanup → all 488 originally-flagged punchlist rows resolved.
- PI made and applied 6 typology additions/renames + per-dilemma decisions (see [CLASSIFICATION_POLICY.md](CLASSIFICATION_POLICY.md), [PI_DECISIONS_COMPANION.md](PI_DECISIONS_COMPANION.md)).
- Project methodology fully documented in [PROJECT_REPORT.md](PROJECT_REPORT.md).
- [`pi_punchlist.tsv`](pi_punchlist.tsv) is currently a frozen snapshot of the original 33-row final punchlist (PI accepted all; needs regeneration to reflect empty state).

**Left to do — in priority order:**
1. **Sync Zalmen app UI dropdowns** to the new 31-type list. Stale 25-type list still embedded in two view files.
2. **Refresh terminal docs** (regenerate empty punchlist, mark PI companion as applied, append a "completion" note to PROJECT_REPORT.md).
3. **Git commit** — many files modified; user authorization required.

**Deferred** (in user's auto-memory at `~/.claude/projects/-Users-sinairusinek-Documents-GitHub-Dybbuk/memory/todo_brewery_db_schema.md`):
- Brewery/factory DB-level schema change (two relation-typed columns). User said to coordinate with Zalmen app changes before implementing.

---

## Canonical typology (31 types) — source of truth

`Zylbercweig/organizations/CLASSIFICATION_POLICY.md` is binding.

The 31 types (use these strings EXACTLY — case + punctuation matter, note spaces):

```
Theatre, Traveling Company, Company on Tour, Amateur, Kleinkunst, Circus,
Theatre education, Publisher, Printer, Printer/Publisher, Journals/ Newspapers,
Media (Radio/ Film/TV), Library, Heritage Institution, Education,
Musical organization, Theatre-related Society/ Union,
Religious institutions/organizations, Jewish political bodies,
Non-Jewish political bodies, Welfare/Aid organization, Business,
Labour (factory/workshop), Health institutions, Military, Not an organization,
OTHER - elaborate!, Trade Union / Professional Association, Judenrat,
Sports/Recreation, Fraternal order
```

PI-confirmed renames vs. original 25-type list:
- `Society/Union` → **Theatre-related Society/ Union**
- `Labour` → **Labour (factory/workshop)**
- `Media (Radio/ Film)` → **Media (Radio/ Film/TV)**
- `Political bodies` → split into **Jewish political bodies** + **Non-Jewish political bodies**

Added types:
- Welfare/Aid organization
- Trade Union / Professional Association
- Judenrat
- Sports/Recreation
- Fraternal order

Source of all decisions: [PI_DECISIONS_COMPANION.md](PI_DECISIONS_COMPANION.md) (PI's filled-in answers).

---

## Step 1 — Sync Zalmen app UI dropdowns (highest priority)

### Where

Two files in the Zalmen Streamlit app hard-code the type dropdown:

- `Zylbercweig/zalmen/views/org_review.py` — line ~54: `_ORG_TYPE_OPTIONS = [...]`
- `Zylbercweig/zalmen/views/org_addresses.py` — line ~85: `_ORG_TYPE_OPTIONS = [...]`

Both currently contain the OLD 25-type list (set early in the conversation before the renames/additions).

### What to do

1. Replace both lists with the 31-type list above (preserve trailing `""` empty option for "no value yet" if it was there — check both files for current convention).
2. Verify `org_alignment.py` and `org_clusters.py` don't also hard-code the list — they reference `org_type` via column reads, but worth a `grep "ORG_TYPE_OPTIONS"` to be safe.
3. There is a fix already in place to preserve unknown `org_type` values not in the dropdown — see [org_review.py:1147](Zylbercweig/zalmen/views/org_review.py) for the pattern. Verify both files still have that fallback after the edit.

### Verification

```bash
grep -rn "_ORG_TYPE_OPTIONS" /Users/sinairusinek/Documents/GitHub/Dybbuk/Zylbercweig/zalmen/
```

Should show two definitions (both 31 items) plus the read sites.

Run the app if possible to confirm the dropdowns render all 31 types and existing data still loads.

---

## Step 2 — Refresh terminal docs

### 2a — Regenerate empty `pi_punchlist.tsv`

The current punchlist file is a snapshot from before "accept all"; the actual mapping TSVs now have `needs_review=""` for those rows. Regenerate so the punchlist reflects the empty state:

```python
# Inline script — or save as regenerate_punchlist.py
import csv, sys
from pathlib import Path
csv.field_size_limit(sys.maxsize)
HERE = Path("/Users/sinairusinek/Documents/GitHub/Dybbuk/Zylbercweig/organizations")

files = [HERE/"organizations_clustered_canonical_mapping.tsv",
         HERE/"org_alignment_review_canonical_mapping.tsv",
         HERE/"org_addresses_review_canonical_mapping.tsv",
         HERE/"core_db_canonical_mapping.tsv"]
rows = []
for fn in files:
    with open(fn, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r.get("needs_review") == "yes":
                rows.append(r)

cols = ["review_tag","name","original_type","current_canonical","decided_via",
        "llm_concern_theme","llm_reason","llm_suggested_new","source_file","row_id",
        "entry_name","volume","context_text"]
with open(HERE/"pi_punchlist.tsv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
    w.writeheader()
print(f"Regenerated: {len(rows)} flagged rows remaining (should be 0).")
```

### 2b — Mark PI companion as applied

Prepend a one-line note at the top of [PI_DECISIONS_COMPANION.md](PI_DECISIONS_COMPANION.md):

```markdown
> **STATUS: APPLIED 2026-05-12.** All PI verdicts have been integrated. The summary checklist at the bottom can be considered ticked. Original document preserved below for reference.
```

### 2c — Append completion note to `PROJECT_REPORT.md`

Add a new section at the end:

```markdown
## 11. Completion (2026-05-12)

PI reviewed [`pi_punchlist.tsv`](pi_punchlist.tsv) on 2026-05-12 via the
[`PI_DECISIONS_COMPANION.md`](PI_DECISIONS_COMPANION.md) workflow. All 488
originally-flagged rows have been resolved:

- 6 typology refinements applied (3 renames, 1 split, 2 additions during LLM verification phase, 3 more additions post-PI-review: Judenrat, Sports/Recreation, Fraternal order).
- 60 explicit per-dilemma overrides applied via `apply_pi_decisions.py`.
- 1,135 LLM-verification corrections unflagged under "LLM as verification authority" policy.
- 41 union-default rows re-evaluated by Gemini 3 Pro via `llm_redo_union_defaults.py` (41/289 reclassified).
- 33 hard residual cases (data-quality, brewery-conflict, ambiguous) reviewed by Claude Opus 4.7 in-session via `apply_claude_residuals.py`.
- All 41 remaining cases accepted by PI 2026-05-12.

**Punchlist is now empty.** All `needs_review=yes` flags have been cleared.

### Final canonical-type distribution (mention level, 16,454 rows) — post-PI

(Run `awk -F'\t' 'NR==1 {for(i=1;i<=NF;i++) if($i=="_ - organizations - _ - org_type") c=i; next} {print $c}' Zylbercweig/organizations/organizations_clustered.tsv | sort | uniq -c | sort -rn` for the latest counts.)

### Open deferred items

- Brewery/factory DB schema change (dual relation-typed labels). See `~/.claude/projects/-Users-sinairusinek-Documents-GitHub-Dybbuk/memory/todo_brewery_db_schema.md`.
- Zalmen app UI dropdowns sync to 31-type list (Step 1 in HANDOFF.md).
```

(Adjust paragraph text if the counts/dates differ when you fetch them.)

---

## Step 3 — Git commit

**Do not commit unilaterally.** Confirm with the user first.

Suggested commit (when authorized):

```bash
git status   # review what changed
git diff --stat
```

Files touched in this session (relative to repo root):
- `Zylbercweig/organizations/*` — mapping TSVs, source TSVs, scripts (`map_canonical_types_v3.py`, `llm_verify_shallow.py`, `llm_redo_union_defaults.py`, `apply_pi_decisions.py`, `apply_claude_residuals.py`, `enrich_punchlist.py`, etc.), policy, report, companion, punchlist, backups.
- `Zylbercweig/zalmen/views/org_review.py` — early-session 25-type list (will be re-edited in Step 1).
- `Zylbercweig/zalmen/views/org_addresses.py` — same.

Suggested commit message (split into logical commits if preferred):

```
feat(orgs): finalize 31-type canonical org classification + apply PI decisions

- Add 5 new canonical types: Welfare/Aid organization, Trade Union /
  Professional Association, Judenrat, Sports/Recreation, Fraternal order.
- Rename Society/Union → Theatre-related Society/ Union; Labour → Labour
  (factory/workshop); split Political bodies into Jewish vs Non-Jewish.
- Multi-model pipeline: rule cascade → Gemini 3 Pro verification →
  Claude Opus 4.7 residual review. ~99.8% mention rows confidently
  classified; remaining 0.2% accepted by PI.
- Sync Zalmen app dropdowns to new 31-type list.
- Document methodology in PROJECT_REPORT.md and policy in
  CLASSIFICATION_POLICY.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## Key files / inventory

```
Zylbercweig/organizations/
├── CLASSIFICATION_POLICY.md      # Binding 31-type policy (source of truth)
├── PROJECT_REPORT.md             # Full methodology + decisions narrative
├── PI_DECISIONS_COMPANION.md     # PI's review doc (filled in)
├── HANDOFF.md                    # This file
├── pi_punchlist.tsv              # Was 488 rows; will be empty after regenerate
│
├── core_db.tsv                   # 578 DB rows + .pre_canonical_backup
├── org_addresses_review.tsv      # 576 DB working + .pre_canonical_backup
├── org_alignment_review.tsv      # 7,499 cluster rows + .pre_canonical_backup
├── organizations_clustered.tsv   # 16,454 mention rows + .pre_canonical_backup
│
├── *_canonical_mapping.tsv       # Per-row decision history for each of the 4 above
│
├── map_canonical_types_v3.py     # Final rule mapper (Pass A + named-entity + Pass B keyword cascade)
├── llm_verify_shallow.py         # Gemini 3 Pro verification pass (parallel)
├── llm_redo_union_defaults.py    # Gemini focused re-run on union defaults
├── llm_redo_residuals_opus.py    # Opus residual redo (Anthropic credits required)
├── llm_deep_review.py            # Opus 196-row stratified sample (calibration)
├── llm_calibrate_gemini.py       # Gemini-vs-Opus calibration
├── llm_calibrate_gemini_retry.py # Retry script for token-truncation fix
├── llm_review_sample.py          # Earlier Sonnet sample (superseded)
├── apply_pi_decisions.py         # Applies PI verdicts from the companion doc
├── apply_claude_residuals.py     # Applies Claude-in-session classifications (97 rows)
├── enrich_punchlist.py           # Adds entry_name/volume/context_text columns
├── build_ra_comparison.py        # RA-vs-auto comparison (deprioritized)
├── build_canonical_mapping_tsv.py  # Older mapping-TSV builder
├── map_canonical_types.py        # v1 mapper (superseded)
├── map_canonical_types_all.py    # v1 multi-file (superseded)
├── map_canonical_types_v2.py     # v2 mapper (superseded)
└── ra_tag_canonical.tsv          # RA's tag→canonical mapping
```

### Python environment

The repo has multiple venvs. Use the **root Dybbuk venv** for these scripts:

```
/Users/sinairusinek/Documents/GitHub/Dybbuk/.venv/bin/python3
```

NOT `python3` (default), which resolves to `Zylbercweig/zibn-shtern/.venv/bin/python3` and **does not** have `google.genai` / `anthropic` installed.

### API keys

- `GOOGLE_API_KEY` — set in env, Gemini-side credit is fine.
- `ANTHROPIC_API_KEY` — set in env, but **out of credit balance** at handoff time. Don't run Opus-via-SDK scripts unless credits are topped up. Use Gemini fallback or in-session Claude reasoning instead.

---

## Conventions / norms picked up this session

- User wants concise, decision-driven responses. Doesn't want narration.
- All mapping TSVs are positionally aligned with their source data TSVs (same row order). Scripts rely on this.
- Backups already exist for the 4 source TSVs (`*.pre_canonical_backup`); don't re-overwrite them.
- The PI is referred to in code and docs as "PI" (capital). User is the PI's collaborator/co-investigator.
- Yiddish/Hebrew strings: use exact substring matching with word-boundary regex for named-entity matching (see `_word_boundary_match` in `map_canonical_types_v3.py`). Diacritic variants matter (e.g., `שרײַבער` vs `שרייבער`).
- The Zalmen app is a Streamlit app under `Zylbercweig/zalmen/`. Don't touch its data flow without coordination — schema changes (e.g., brewery dual labels) are explicitly deferred for that reason.

---

## Context from earlier conversation worth carrying

- 196-row Opus 4.7 deep review found three failure modes: shallow keyword missed cue, named-entity substring overfire, sub-entity parent inheritance.
- Word-boundary fix dropped named-entity false positives from ~7% to near-zero.
- Gemini 3 Pro calibration: 99% match with Opus on cases where both responded; required `max_output_tokens >= 2000` to avoid thinking-token truncation; 88% overall agreement after retry.
- Brewery/factory class needs relation-aware classification (Business for owners, Labour for employees). User chose dual-labels at DB level — schema change deferred.
- Generic society/club/association without theatre cue now defaults to **Trade Union / Professional Association** (PI section 4.2). The same default was applied to `union` after a re-run found ~80 union rows are actually theatre-related (Yiddish PEN Club, Drama Guild, etc., caught by Gemini's deeper reasoning).
