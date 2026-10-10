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
| Decisions recorded | **160** — 23 CONFIRM, 127 REJECT, 7 RESEARCH_FURTHER, 3 NOT_A_NEWSPAPER |
| Applied to core_db | **5 address cells** (`8e4f29721`). Nothing else. |
| Open on the form | **81 tier rows + 28 unlinked titles** (was 80; the parallel session added one — see the hazard at the end) |
| Never reviewed | 98 STRONG band, 164 WEAK band |

Decided and closed: A1, A2 (73 rejected on place conflict), B, D, F (6 of 7),
C1a (7 of 8 groups, on location).

---

## Open work, in Sinai's order

### 0. Finish the form (80 + 28 decisions)

| Section | Open | Note |
|---|---|---|
| Tier C | 37 of 75 | C1a's קולטור group is the one location could not settle; C1b's 17 unlisted rows are in the TSV, not the doc |
| Tier E | 43 of 43 | untouched; expect mostly REJECT (one row is db63, newly minted next door) |
| Tier F | 1 | `ORG-C01891` *Yidishe Folksbibliotek* vs *Yidishes Folksblatt* |
| Unlinked (Zylbercweig) | 28 of 29 | each pre-judged in `unlinked_judgements.tsv`: 22 are real titles, 2 generic, 2 books, 3 unclear |

7 rows are parked at `RESEARCH_FURTHER`, not open. **One of them is already
answered:** `ORG-C07330` — Landsberg's *Lager-Cajtung* renamed itself *Jidisze
Cajtung* in 1946, so it is the same paper and confirmable as it stands.

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

## Two known defects in the matcher

**`language` is never consulted.** It is a column in the union list and the
matcher ignores it. This produced the `ORG-C01415` error: Zylbercweig's
פּראָגרעס (a Warsaw *publisher*) matched Galron's איל פרוגריסו (a New York
**Ladino** paper) because both skeletons reduce to `frgrs`. A Yiddish-corpus
org matching a Ladino, German or Hungarian title should be penalised
automatically. **Fixing this first would likely clear noise from Tier E.**

**Publishers match newspapers.** 4 Tier E rows pair a Zylbercweig publisher
with a paper — Kletskin's publishing house → *Vilner Togblat*, two Warsaw
publishing committees → *Di Abend Tsaytung*. The org_type is known on the
Zylbercweig side and could gate this.

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
