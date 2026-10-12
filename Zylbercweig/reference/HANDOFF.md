# Galron press matching — handoff

State as of 2026-10-10. Read this plus `README.md` before touching anything.

**Review doc (the form):** <https://claude.ai/code/artifact/e99c9e25-2546-4589-afd7-8a91388c71f7>
It carries only undecided material; decided tiers were removed, with counts
kept in its intro.

---

## What this is

Joseph Galron-Goldschläger's **Union List of Digitized Jewish Historic
Newspapers** (<https://go.osu.edu/jpress>) parsed into
`galron_union_list.tsv` — 2,424 rows / 2,013 distinct titles / 519 Yiddish —
and matched against Zylbercweig's press organizations.

**Scope decision (Sinai, 10-10): the Zylbercweig side only.** Galron's list is
a reference, not a source of organizations. Nothing of Galron's gets minted.
"Named after" is not recorded as a relation.

## Where the work stands

| | |
|---|---|
| Decisions recorded | **160** — 24 CONFIRM, 127 REJECT, 6 RESEARCH_FURTHER, 3 NOT_A_NEWSPAPER |
| Applied to core_db | **5 address cells** (`8e4f29721`). Nothing else. |
| Open on the form | **57 tier rows + 28 unlinked titles** (down from 81: the two matcher defects below are now fixed, which cleared 24) |
| Never reviewed | 98 STRONG band, 164 WEAK band |

Decided and closed: A1, A2 (73 rejected on place conflict), B, D, F (6 of 7),
C1a (7 of 8 groups, on location).

---

## Open work, in Sinai's order

### 0. Finish the form (80 + 28 decisions)

| Section | Open | Note |
|---|---|---|
| Tier A | 8 | new rows surfaced by the widened blocking; not the decided A1/A2 |
| Tier C | 29 | C1a's קולטור group is the one location could not settle; C1b's unlisted rows are in the TSV, not the doc |
| Tier E | 19 | was 43 — the language gate cleared 24. Expect mostly REJECT still |
| Tier F | 1 | `ORG-C01891` *Yidishe Folksbibliotek* vs *Yidishes Folksblatt* |
| Unlinked (Zylbercweig) | 28 of 29 | each pre-judged in `unlinked_judgements.tsv`: 22 are real titles, 2 generic, 2 books, 3 unclear |

6 rows are parked at `RESEARCH_FURTHER`, not open. `ORG-C07330` was closed as
CONFIRM this session — the research had answered it (Landsberg's
*Lager-Cajtung* renamed itself *Jidisze Cajtung* in 1946).

The form itself still shows the pre-fix tables. Regenerating Tier C and E
against the current run is the first thing worth doing next session; the row
*sets* shrank, so some rows on the form no longer exist.

### 1. Finalise cluster merging and alignment

The C1a work is the lever. 71 Tier C rows resolved to 35 Galron papers, and 8
papers were each claimed by 3–9 Zylbercweig orgs. Those claimant sets are
candidate **within-corpus merges** — if nine orgs all match one paper, some are
probably each other. The `entity-matching` skill's within-corpus cascade
applies; its auto-merge gate needs `signal == trigram_full`, which none of
these have, so everything routes to review.

### 2. Mint the remaining newspapers

Zylbercweig papers with no core_db row of their own — **not** Galron's.
Mint ids against **origin's** high-water mark at apply time: the app pushes
core_db on every save, and it moved 2,288 → 2,597 rows during this work alone.

### 3. Enrich every newspaper entry

A short comment plus links. Ready to draw on:
- `galron_title_families_research.md` — 12 titles, verdicts, sources
- `folksshtime_research.md` — the Lodz Folks-shtime case
- `c1a_location_evidence.md` — per-group location evidence
- `galron_union_list.tsv` — years, depository, scan URL per title
- `unlinked_judgements.tsv` — translit, gloss, is-it-a-title

**Open question:** core_db has no comment column. Decide between
`name_variants`, a new column (the app's save path must learn it), or a sidecar
TSV keyed by db_id. The sidecar is the safer default.

### 4. Research the non-Jewish newspapers

6 are tagged in `unlinked_judgements.tsv` with `NON_JEWISH`: *Kievlyanin*,
*Odesski Vestnik*, *Odesskiya Novosti*, *Pravda*, *Pinski Listok*,
*Bessarabskoya Zhizn*. Their absence from Galron is explained — **he indexes
the Jewish press** — so they need other sources: Russian Wikipedia, Wikidata,
the National Library of Russia, Libraria for Ukraine. Also worth deciding
whether `org_type` should mark them at all.

---

## Two matcher defects — both FIXED this session

**`language` is now consulted** (`press_match_key.language_compatible`).
Galron records each title's language and the matcher ignored it, which let
Zylbercweig's פּראָגרעס — a Warsaw *publisher* — match New York's **Ladino**
איל פרוגריסו, both skeletons reducing to `frgrs`. Now a title with no
Yiddish/Hebrew component demotes one band.

Deliberately narrow, after a first attempt over-reached. A mixed entry
("רוסית/יידיש", "Russian / Hebrew / English") counts as compatible, and the
penalty applies **only when the titles do not already agree outright** —
because a Yiddish paper is routinely catalogued under an English or Russian
name. The first pass demoted 13 correct pairs including `db249`
פֿאַרווערטס → *Forward* and both *Russkii Evrei* rows Sinai had confirmed.

**Publisher→newspaper matches are now gated** (`FIRM_TYPE_RE` + the
`zyl_is_firm` column). A Zylbercweig org typed Publisher or Printer matching a
newspaper title is a type mismatch and demotes one band. Not a veto: a firm and
the paper it printed often shared a name.

Effect: Tier E 43 → 19, total open 81 → 57, STRONG 98 → 95. Two new columns in
the review TSV, `language_ok` and `zyl_is_firm`, say which rule fired.

## Traps that cost time here

- **`years_covered` is digitisation coverage.** Never date a paper from it, in
  code *or* in prose. Slipped once anyway (see `folksshtime_research.md`).
- **core_db is CRLF.** A writer emitting LF rewrites all 2,598 lines and buries
  the real change; `git numstat` then reports 2598 for 5 changed cells. Verify
  with a cell-level diff against a backup, not numstat.
- **The doc's row order differs from the TSV's.** Tier D decisions filed by TSV
  position would have inverted four of six. Read row→entity pairings back from
  the doc.
- **`translit_yiddish_to_latin.py` is pointed-only** — it mangles 11 of 14
  unpointed words silently. Use `press_match_key.py` instead, and always with
  the phonetic channel (244 of 530 candidates existed only because of it).
- **Local main is always stale** and the parallel session is live in
  `organizations/`. Fetch first; commit with explicit paths.

## Files

| File | What |
|---|---|
| `galron_union_list.tsv` | the parsed union list (2,424 rows) |
| `parse_galron_union_list.py` | rebuilds it from the two saved HTML sheets |
| `press_match_key.py` | cross-script skeleton + DM phonetics + `place_agree` |
| `match_press_to_galron.py` | the matcher → `galron_press_match_review.tsv` |
| `galron_press_match_review.tsv` | **499** candidates, banded and tiered (496 + 3 from the parallel session's new publisher rows) |
| `galron_decisions_round1.tsv` | **the decision ledger — 160 rows** |
| `apply_galron_round1.py` | applies the ledger to core_db (idempotent, dry-run default) |
| `galron_mint_proposal.tsv` | 45 NOT_MINTED + 5 APPLIED, kept for the record |
| `unmatched_press_orgs_handoff.tsv` | 71 publishers → publisher audit, 29 printers → Zalmen |
| `unlinked_judgements.tsv` | the 29 unlinked titles, judged |
| `NEXT_STEPS.md` | the same four stages, written before this handoff |

## Handed off elsewhere

`unmatched_press_orgs_handoff.tsv` — 100 unmatched publishers and printers.
None is a mistyped periodical (0 of 100 read as newspapers; 90 are personal
names), so they were routed, not reclassified: 71 publishers to the publisher
audit running in the parallel session (36 already among its 157 decisions),
29 printers to Zalmen (26 untouched).

---

## Live hazard: the parallel session feeds rows back into this matcher

While this handoff was being written, the parallel publishers session committed
`b650b17c3` ("Mint Bletnitski, work the press handoff, and propose a dynasty
model") — acting on `unmatched_press_orgs_handoff.tsv`. It minted publisher
rows **db47 Kultur, db57 Progres, db63 Amkroyt un Fraynd**, and re-running
`match_press_to_galron.py` immediately picked them up as three new candidates
(496 → 499 rows).

Two consequences:

1. **Re-running the matcher is not idempotent against a moving core_db.** The
   row set grows as the parallel session mints. That is correct behaviour, not
   a bug, but it means the review TSV and the ledger can drift apart. Match the
   ledger on `entity` rather than `(entity, title)` — Galron title strings vary
   between runs (`Russkii Evrei` vs `Russkii Evrei-`), which already caused two
   rows to look open when they were decided.

2. **db57 Progres is the case already rejected.** `ORG-C01415` פּראָגרעס was
   rejected as a Warsaw *publisher* wrongly matched to New York's Ladino
   *El Progreso*. It is now a minted publisher row and the matcher proposes it
   again. The `language` defect above is what lets that recur — fix it before
   the next matcher run, or this pair will keep coming back.

The newly matched rows are left in the review TSV, undecided, so the next
session sees them rather than inheriting a silently filtered file.

---

## Publishing history — why it took three attempts

*Resolved; see "MERGED into main" below. Kept because the reasons recur.*

Through `178cc1954` everything was committed but unpushed, for two reasons,
both about other people's work rather than this:

1. **The branch carries 32 commits ahead of origin, only 20 of them this
   session's.** The rest are the parallel publishers session and some YiDraCor
   work. Pushing sends everything, which is not this session's call to make.

2. **The parallel session is live.** At the moment of writing, `core_db.tsv` and
   `activity_log.tsv` had been modified 43 seconds earlier, and five files in
   `organizations/` hold its uncommitted work. `git rebase onto origin/main`
   refuses while those are dirty, and stashing them to force it through is
   exactly how committed work has been clobbered in this repo before.

Local main was also **1,621 commits behind** origin — the app's auto-saves.
That is normal here. It is also why the id collisions kept recurring: a local
`core_db` that far behind gives a stale high-water mark to mint from.

### MERGED into main — 2026-10-12

Done. The branch `galron-press-handoff` was merged to `origin/main` through a
PR, after a third id renumbering. Nothing is pending.

**The merge was a union, not either side.** The earlier advice in this file —
`git checkout --ours` on `core_db.tsv` and `activity_log.tsv` — was **wrong**
and is kept here only as a warning: `--ours` takes origin's whole row and would
have silently discarded the publishers session's additions, which filled 15
empty `org_type` cells and added `linked_cluster_ids` on five rows. These TSVs
are append-only and **each side filled cells the other left empty**, so the
resolution is per cell, against the merge base: where one side still holds the
base value, the other side's edit wins.

**The ids moved a third time: 2809–2820 → 2846–2857.** The deployed app mints
on every Save, so it had taken 2809–2820 for theatres and amateur groups while
this work sat unmerged — overtaking the `P162` renumber just as `P162` had
overtaken the original 2661–2669 mints. See §8 of
`organizations/PUBLISHING_DYNASTY_MODEL_PROPOSAL.md` for the full chain and the
final id table (`P173`–`P175`).

Two traps worth carrying forward:

- **Renumber by column, never by text.** In `eponym_candidates.tsv`, `2809` is
  also a *person* hub id (`HUB-D2809`) and 1809–1820 are person db ids. A
  search-and-replace would have corrupted them.
- **A renumbering sweep can catch rows that are not yours.** `P162` dragged
  nine of Noa's `NEW` rows along with the publishers; origin had already minted
  those societies at 2718–2726, and both sides' `core_db` already agreed, so
  only `org_alignment_review.tsv`'s column was stale. Restored under `P174`.

**The standing lesson:** any id minted locally against a stale `core_db` is a
guess, because the app is minting at the same time. Mint from `origin/main`'s
high-water mark fetched at that moment and merge the same day — or mint no id
and let the app assign it.

The `activity_log` mint rows still record the **original** ids (2661–2669,
2805–2807). The log is append-only history and was left as written.
