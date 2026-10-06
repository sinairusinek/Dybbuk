# Prompt: reconcile 42 local commits with origin/main and push

Paste everything below into a **fresh session** in `~/Documents/GitHub/Dybbuk`.

---

You are reconciling a long-stale local `main` with `origin/main` and pushing.
Work in two phases: **plan first, confirm with me, then execute.** Do not push
anything until I approve the plan.

## The situation (verified 2026-10-06 07:30, re-verify before acting)

- Local `main` is **42 commits ahead** and **~423 behind** `origin/main`.
- The gap grows continuously: the Zalmen Streamlit app **auto-pushes to main**.
  Treat `origin/main` as live and moving. Re-fetch at every phase boundary.
- All 42 local commits are Claude-assisted work authored as Sinai, spanning
  several sessions: troupe review, speaker labels, act structure, the YiDraCor
  entity graph, DybbukMedia, and the work/song layer.
- A **parallel session is editing this repo right now**. `YiDraCor/data/editions.csv`
  was modified minutes ago by someone else (it reclassifies the
  `HurbanYerushalaim_820938_duplicate` row as a real 1908 print edition).
  **Do not commit, revert or rebuild files you did not change.** Check `stat`
  mtimes before touching anything under `YiDraCor/data/`.

## The one thing that must not go wrong: colliding org ids

While this work was local, the app minted its own organisations. **Org db_ids
2256–2259 now mean different things on each side:**

| db_id | LOCAL (mine, uncommitted upstream) | REMOTE (app, already live) |
| --- | --- | --- |
| 2256 | Biblioteka Narodowa (Polona) | אַרבעטער-טעאַטער-פֿאַרבאַנד |
| 2257 | The International Biblioteque | יידישער אַרטיסטן-פאַריין |
| 2258 | American Jewish Archives (Cincinnati) | אַרטיסטן און פֿריינט |
| 2259 | St Petersburg State Theatre Library | אינדעפּענדענט היברו עקטאָרס ליג |

Remote `core_db.tsv` max db_id is **2314**. **The remote ids win** — they are
already referenced by live app decisions. My four must be **renumbered to
2315+**, and every reference to them updated in lockstep:

- `Zylbercweig/organizations/core_db.tsv` — the four rows
- `Zylbercweig/organizations/activity_log.tsv` — I appended **5** rows with
  `view` = `yidracor_entity_graph`: four MINT (2256, 2257, 2258, 2259) and one
  MERGE (target 1077 → 27). Only the four MINT rows need renumbering; each
  carries the id twice, in `target_id` and in the JSON `extra` column.
- `YiDraCor/code/build_entity_graph.py` — the `ORG_DECISION` table maps
  `"diyudishebihne"` etc. to ids; also check `2256`/`2257` elsewhere in that file
- `YiDraCor/data/entity_graph.json` and `entity_gaps.tsv` — regenerate, do not
  hand-edit
- `YiDraCor/data/editions.csv` — the `library` column is matched **by name**,
  not id, so it needs no change; verify this rather than assume

**people_db ids 3822–3826 do NOT collide** — remote max is 3821, so my five
minted people (Minkowski, Simovitch, Baraban, Kurantman, Kamińska) keep their
ids. Confirm this still holds at execution time.

## What is already upstream

`git cherry -v origin/main HEAD` marks **2 commits as already applied** by
patch-id:

- `201a8a4c9` Org review: surface GENERIC in the UI
- `6672be2ea` DB Audit: add a "Joint names" section for the serial-list rows

Two more share a **subject line** with an upstream commit but differ in content
— the same work landed by another route, probably re-committed by the app:

- `01e0b2e4d` Split 9 multi-place clusters by settlement (upstream `a12660e3e`)
- `ee90973c6` Education: apply the 2026-10-04 generic-institution rule
  (upstream `f4574a793`)

**Diff those two pairs before deciding.** If the upstream version supersedes
mine, drop mine rather than re-applying it over newer app decisions.
Re-run `git cherry -v origin/main HEAD` at execution time — more may have
landed since.

## Files both sides changed

Conflicts are expected in exactly these, and each has a different correct
resolution:

1. **`Zylbercweig/organizations/core_db.tsv`** — both sides appended rows at the
   tail. One conflict hunk. Resolution: **keep both sides' rows**, then
   renumber mine to 2315+. Never drop a remote row.
2. **`Zylbercweig/organizations/activity_log.tsv`** — genuinely append-only:
   2613 rows shared, remote added 125, I added 5. Resolution: **union, sorted by
   `ts`**. Note this file has **mixed line endings** — append with `\n`.
3. **`Zylbercweig/organizations/org_alignment_review.tsv`**,
   **`db_joint_name_decisions.tsv`**, **`education_titular_review_punchlist.tsv`**
   — app-written decision ledgers. **The remote is authoritative for any row the
   app decided.** Per `feedback_dybbuk_ingest_prs_field_level`: reconcile
   field-level, never by textual merge. If a row differs, prefer the side with a
   `reviewer` stamp and the later `reviewed_at`.
4. **`Zylbercweig/zalmen/views/org_review.py`**, **`views/db_audit.py`** — app
   code the remote has moved on. Read both versions; my local changes here are
   older. Prefer remote unless a local change is clearly additive and still needed.
5. **18 TEI files** under `YiDraCor/tei/dracor/` and `YiDraCor/tei/ms/` —
   add/add conflicts, both sides generated. **Do not hand-merge XML.** Take
   either side, then regenerate: `cd YiDraCor/code && python3.11 -m
   structure.build_tei --play <folder>` for each affected play, and validate
   with `jing` against tei_all (NOT xmllint). A schema copy may still be at
   `/private/tmp/claude-501/.../scratchpad/tei_all.rng`; else fetch from
   `https://www.tei-c.org/release/xml/tei/custom/schema/relaxng/tei_all.rng`
   (follow the redirect — the http URL returns a 169-byte stub).

## Hard constraints

- **CRLF**: `core_db.tsv`, `people_db.tsv` and `editions.csv` are **strict
  CRLF**. Read with `io.StringIO(path.read_text())`, write with
  `lineterminator="\r\n"` and `write_bytes`. Verify after every write:
  `python3 -c "d=open(P,'rb').read(); print(d.count(b'\r\n'), d.count(b'\n')-d.count(b'\r\n'))"`
  — the second number must be 0. A whole-file line-ending flip has already
  happened once in this work and was caught only by diffing.
- **Commit with explicit paths**: `git commit -m "..." -- <paths>` (the `--`
  goes after `-m`). Never `git add -A`, never commit a directory.
- **Never `--force`** to `main`.
- Leave untracked files alone: `.claude/`, the three `db_*_punchlist.tsv`,
  `troupe_tags_export.csv`, `troupe_tags_sheet.csv`,
  `Zylbercweig/organizations/meeting/` (those four `.md` are byte-identical to
  the remote and will be restored by the merge — if they block the merge, verify
  identity by md5 against `origin/main`, remove, merge, and they come back).
  Also leave `Zylbercweig/zibn-shtern/data/working/venues_unlinked.csv`, which
  was already modified before this work began.

## Phase 1 — plan (do this first, then stop and show me)

1. `git fetch origin`; report exact ahead/behind.
2. Confirm or correct every fact above: the 2256–2259 collision, remote max org
   id, people_db 3822–3826 still free, which 2 commits are already upstream,
   and the current conflict set.
3. Choose **merge** or **rebase** and justify it in two sentences. Consider that
   rebase replays 39 commits and may conflict repeatedly on the same appended
   ledger rows; a single merge commit conflicts once per file. Consider also
   whether a **branch + PR** is safer than touching `main` at all, given the app
   pushes to `main` continuously.
4. List, in order, the commits/steps you will make and what each contains.
5. State how you will verify success **before** pushing — at minimum: all 4 org
   rows renumbered and referenced consistently; no remote row lost from any
   ledger; `build_entity_graph.py` runs clean with 0 dangling edges; TEIs
   validate; CRLF intact; `git diff origin/main --stat` reviewed and every
   changed file explicable.

**Stop here and show me the plan. Do not execute it.**

## Phase 2 — execute (only after I approve)

- Work on a branch first, even if the plan is to land on `main`.
- Re-fetch before starting; if `origin/main` moved, re-check the id collision.
- After each conflict resolution, show me the resolved hunk for the three
  decision ledgers before continuing.
- Rebuild rather than hand-edit any generated file (`entity_graph.json`,
  `entity_gaps.tsv`, the TEIs, `docs/Visualizations/*.html`).
- Push, then confirm: `git log --oneline origin/main -3` and
  `git status` clean.
- Finally, report what landed and anything you deliberately left out.

## Useful context

- Session memory: `project_yidracor_entity_graph.md`,
  `feedback_dybbuk_local_main_is_always_stale`,
  `feedback_dybbuk_ingest_prs_field_level`,
  `feedback_core_db_link_authority`, `feedback_git_commit_path_scope`,
  `feedback_check_staged_before_commit`.
- The work being pushed is described in the commit messages themselves; read
  `git log origin/main..HEAD` before planning.
