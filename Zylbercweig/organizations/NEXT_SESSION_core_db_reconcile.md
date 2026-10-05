# Next session: reconcile core_db.tsv between local `main` and `origin/main`

Paste the "Prompt" section below into a fresh session in
`/Users/sinairusinek/Documents/GitHub/Dybbuk`.

---

## What the problem actually is (plain version)

Two things wrote new organizations into `core_db.tsv` at the same time, without
knowing about each other:

1. **The deployed Zalmen app.** Every time someone clicks Save in a review
   view, the app commits and pushes the file itself. Those are the hundreds of
   `chore: save ...` / `chore: log ...` commits on GitHub. They are real work —
   Ruthie's and Maty's decisions.
2. **This local clone**, in a session on 2026-09-30/10-04, which added 16 new
   orgs from Ruthie's troupe-questions doc plus one library record.

A new org gets its `db_id` by taking the **highest existing id + 1**. Both sides
started from the same number, so **both sides minted ids 2240–2256 — for
completely different organizations.**

| | rows |
|---|---|
| common ancestor | 2180 |
| local `main` | 2197 |
| `origin/main` | 2212 |

So `db2240` is `פֿרידמאַנס טרופּע` locally and `אָריענטאַל טעאַטער` on the
server. Sixteen such pairs. A plain `git merge` cannot fix this: there is no
text conflict to resolve — both files are internally valid, they just disagree
about what those sixteen numbers mean.

**The good news, already verified: zero rows were edited on both sides.** The
only collision is the newly minted ids. Nothing has to be hand-merged field by
field.

**`origin/main` is the authority.** It holds the live app's saves and is what
every deployed tool reads. The local 16 must be **re-minted at new ids above the
server's high-water mark**, not forced over the top.

## Current state

- Local `main`: **14 ahead, 259 behind** `origin/main`. All 259 are Sinai's own
  (overwhelmingly app `chore:` saves) — no colleague's work at risk.
- Working tree: **~62 uncommitted files**, nearly all **YiDraCor** (a parallel
  session's work in progress). **Do not commit, stash or revert those** without
  asking — they are not part of this job.
- Already deployed and done, do not redo: the DB Audit **"Joint names"** section
  (PR #15, merge `93f9a00a2`) is on `origin/main`.
- Of the 14 local commits, only **3 touch `core_db.tsv`**:
  - `40262f272` Ruthie's 32 clean answers (16 orgs minted, 13 cluster links, 2 merges, 3 deprecated, 12 bundle rows retired)
  - `90addf009` Sinai's 5 follow-ups (2 merges: db583→db459, db579→db254; 1 link)
  - `76263cc1f` Biblioteka Narodowa minted as 2256

## The decisions already made, which must survive

Re-applying is better than merging, because the inputs are all still on disk:

- `ruthie_doc_answers_2026-10-04.tsv` — the transcribed answers (source of truth)
- `apply_ruthie_doc_answers.py` — applies them; **idempotent**, CREATE guarded by
  an existence check on the normalized Yiddish name
- `bundle_resolve_candidates.tsv`, `build_bundle_resolve_table.py` — the evidence

Content that must end up present, whatever the ids:

- **16 new orgs** for the bundle parts Ruthie marked "create"
- **14 cluster links** onto existing orgs she marked "link" (incl. q7 → db459)
- **2 merges**: db583 → db459, db579 → db254 (convention: `merged_into=<target>`,
  `deprecated=true`, clusters moved to target, source `linked_cluster_ids`
  cleared, source spelling kept as a `name_variant`)
- **3 deprecated**: db1354, db1717, db1735
- **12 bundle rows retired** (`deprecated`). **db1076 must stay live** — its q4 is
  still unanswered.
- **1 library org** (Biblioteka Narodowa / Polona)
- Still open: **q4** — Ruthie chose "create new" for `איציקל גאָלדענבערג` though
  db1329 `איציקל גאָלדענבערג און קאָנער` exists. Do not decide it; it is queued
  in DB Audit → Joint names (db1076).

## Traps that cost time in the last session

- **`build_core_db.py` is non-idempotent** — regenerating drops app-appended
  rows. Never "just rebuild" the file.
- **`linked_cluster_ids` has no single authority** — REMOVE decisions,
  `merged_into` chains and umbrella parents all override `aligned_db_id`. Read
  the `feedback_core_db_link_authority` memory before repairing links.
- **Yiddish matching**: normalize **NFKD before stripping points**. The corpus
  stores the precomposed ligature `U+FB4E (פֿ)` where a typed string has
  `פ + rafe`; stripping points alone matches nothing and *looks* like a clean
  absence. Strip points from the **pattern too**, not just the text.
- **A substring search on one spelling is not an existence check.** Last session
  I reported "no Ziegler entry exists"; it was there as `זייגלער` (double yod)
  while I only probed `זיגלער`. Probe variants (yod doubling, ג/נ, ז/ס) before
  concluding a name is absent.
- **Possessive after a final letter form**: `גאָלדפאַדען + ס` is
  `גאָלדפאַדענס`, not `גאָלדפאַדעןס`.
- **Commit with explicit paths**: `git commit -m "..." -- <paths>`; the `--` goes
  *after* `-m`. Never commit a whole directory.
- The app's Python is **anaconda 3.11** (`/opt/anaconda3/bin/python3`), not the
  repo `.venv` (3.9).

## Suggested shape (not binding)

1. Re-verify the diagnosis from scratch; do not trust this document.
2. Decide and state the id policy — most likely: re-mint the local 16 above
   `origin/main`'s highest id, keeping the server's 2240–2256 untouched.
3. Rebase or cherry-pick the 3 commits' *intent* by re-running
   `apply_ruthie_doc_answers.py` against a tree based on `origin/main`, rather
   than merging the file.
4. Check nothing else in the 14 local commits is lost (11 touch other files).
5. Dry-run, verify independently (fresh read, not the pattern that did the
   edit), then one commit with explicit paths, then PR.

Leave the YiDraCor files alone throughout.

---

## Prompt

> `core_db.tsv` has diverged between my local `main` and `origin/main` and I need
> it reconciled. Read
> `Zylbercweig/organizations/NEXT_SESSION_core_db_reconcile.md` first — it has the
> diagnosis from the session that caused it, but **verify everything yourself
> before acting; do not trust that write-up.**
>
> Short version: the deployed Zalmen app pushes `core_db.tsv` itself on every
> save (those are the `chore:` commits), and a local session also added 16 new
> orgs. Both sides minted db_ids 2240–2256 for different organizations. No row
> was edited on both sides, so this is an id-collision problem, not a
> field-merge one.
>
> `origin/main` is the authority — it has the live app's saves. My local 16 need
> re-minting above the server's highest id. The inputs are all still on disk
> (`ruthie_doc_answers_2026-10-04.tsv` + the idempotent
> `apply_ruthie_doc_answers.py`), so re-applying the decisions onto a tree based
> on `origin/main` is probably cleaner than merging the file — but tell me what
> you'd do before you do it.
>
> Constraints:
> - Don't touch the ~62 uncommitted YiDraCor files; they're another session's
>   work in progress.
> - Don't force-push; the 259 remote commits are real decisions by Ruthie and
>   Maty.
> - Don't re-decide q4 (`איציקל גאָלדענבערג`); it's queued in DB Audit → Joint
>   names and is Ruthie's call.
> - Don't regenerate with `build_core_db.py` — it drops app-appended rows.
>
> Give me the plan first, then apply it, dry-run before writing, and verify with
> a fresh read rather than the pattern that made the edit.
