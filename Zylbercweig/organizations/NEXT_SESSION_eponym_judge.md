# Next session: the eponym judge, and whether it graduates

Paste the **Prompt** at the bottom into a fresh session in
`/Users/sinairusinek/Documents/GitHub/Dybbuk`.

Full writeup, with a pipeline diagram:
https://claude.ai/code/artifact/2994fe4f-a925-46ac-95d6-f8a6b93adfe8

---

## Where things stand (2026-10-14)

> **Superseded 2026-10-25:** `recover-session` was merged into `main` as
> `05a2ab20c` and deleted (tip kept as tag `archive/2026-10-25/recover-session`).
> Work on `main`. The branch notes below are history; the current state is in
> `HANDOFF_eponym_2026_10_25.md`.

**Branch `recover-session`, 16 commits ahead of origin/main, UNMERGED.** A
concurrent session orphaned four commits earlier in the day; they were
cherry-picked back and both sessions' work coexists. `main` does not contain
that other session's reconstruct commit either, so merging the two lines is
Sinai's call — do not do it unasked.

- **134 eponym edges** live across 118 orgs (`edge_source=eponym`)
- **156 decisions / 134 CONFIRM** in `eponym_decisions.tsv`, all stamped `Sinai`
- **`eponym_RA_workbook.xlsx`: 506 rows, ready for Ruthie**, with 90 machine
  proposals sorted to the top, each carrying the quote that supports it
- Both calibration gates passed at **1.000 precision**, 0 grounding failures

Ruthie has never worked this queue. Nothing blocks her starting.

## What to do first

Nothing is broken and nothing is half-finished. Pick from the open items.

### 1. The 51 recovered names (smallest, highest value)

`eponym_recall_candidates.tsv`, `outcome=recovered`. These came from the
judge's `free_text_name` — when it abstains it often still names the person
the sentences point to, WITH the given name the span lacked
(`לארגמאנ` → `ליאַ לאָרגמאַן`). 51 resolve to exactly one hub on surname AND
given name together. They are not in the workbook because they are spans where
no candidate was offered, so they need either a separate sheet or injection as
proposals. **Ask Sinai which.**

**Never match on surname alone.** This run proved why: `ליאַ לאָרגמאַן` matches
`ברין, אַליאַ` and `זיגמונד טורקאָוו` matches `זיגמונד שוואַרץ`. Different
people.

### 2. The 134 "absent" people

Same file, `outcome=absent`. Organisations named after someone with no usable
hub — the judge read the name from the sentences and nothing in
`person_hub.tsv` matches. **This is a people-side finding, not an eponym
answer**: theatre figures the lexicon mentions but never gave an entry.
Possibly more valuable than the eponym links. Belongs to whoever owns the
person track; ask before routing it.

### 3. Does the engine graduate to Zalmen org alignment?

That was the pilot's actual purpose. `eponym_dossier.py` was written to be
imported for exactly this. Before scaling to thousands of cases:
- the system prompt is resent per call and is ~half the input → wire up
  Gemini's caching
- the `absent` result suggests a systematic person-data gap org alignment
  would hit too

### 4. Five scripts that are broken and are not this track's

They name the retired `gemini-3-pro-preview` as a live `MODEL` constant and
will 404 silently inside their own retry loops:
`llm_calibrate_gemini.py`, `llm_verify_shallow.py`,
`llm_calibrate_gemini_retry.py`, `llm_redo_union_defaults.py`,
`YiDraCor/code/vocalization/phonotactic_check.py`.
Live id: `gemini-3.1-pro-preview`.

## How to run the pipeline

```
python3 build_eponym_candidates.py                      # candidates
python3 eponym_dossier.py --show 641                    # inspect one case
python3 eponym_llm_judge.py --gold --workers 8          # calibrate (gated)
python3 eponym_llm_hardeval.py --workers 8              # adversarial
python3 eponym_llm_judge.py --open --workers 8          # proposals
python3 build_eponym_ra_workbook.py                     # the workbook
python3 ingest_eponym_ra_workbook.py <filled.xlsx>      # dry-run by default
python3 apply_eponym_decisions.py --execute             # edges
python3 eponym_llm_report.py                            # read the numbers
```

Use `/opt/anaconda3/bin/python3` (not the repo .venv). `GOOGLE_API_KEY` and
`ANTHROPIC_API_KEY` are both set in the environment.

## Traps that cost real time — read before touching the data

- **`core_db.tsv` has embedded newlines in quoted fields.** awk/grep mis-parse
  it and report ~2,757 rows for a 2,744-line file. Python csv with
  `field_size_limit(10**9)`.
- **A comma is NOT a reliable surname separator.** 660 of 2,897 hub headings
  write `surname given` with no comma. Use `split_heading()` from
  `build_eponym_candidates.py` — never split on the comma yourself. This bug
  hid 22% of people and made a real person look absent from the corpus.
- **`entry_person_ids` is PIPE-separated** when someone has entries in two
  volumes (168 people do). Split on `[|,]`.
- **`person_hub.tsv`'s `non_person` flag is `0` for every row.** Useless as a
  filter; exclude institutions by a positive test.
- **NFKD before stripping Hebrew points**, and fold final letters on BOTH
  sides. Use `sm()` / `key()`.
- **The judge's confidence signal is worthless** — all 92 answered cases rated
  themselves `high`. Do not triage by it.
- **Verify concurrency by looking at the process**, not the flag:
  `lsof -p <pid> | grep -c ESTABLISHED`.
- Only **4 entries in the whole corpus** are headed by a company name, so
  "read the troupe's own entry" is not available for most orgs.

## Read first

Memories: `project_eponym_dossier_judge_pilot`,
`feedback_llm_judge_pipeline_hazards`, `project_eponym_person_org_links`,
`feedback_dybbuk_local_main_is_always_stale`,
`feedback_concurrent_session_clobbers_worktree`.

---

## Prompt

> Continuing the eponym-link work in the Zylbercweig data, on `main` (the
> `recover-session` branch was merged into main on 2026-10-25 and no longer
> exists).
>
> Read `Zylbercweig/organizations/NEXT_SESSION_eponym_judge.md` first for the
> state and the data traps — but verify its numbers yourself rather than
> trusting the write-up.
>
> Where it stands: 134 eponym edges are live, and a 506-row Excel workbook
> with 90 machine proposals is ready for Ruthie. An LLM judge reads a dossier
> assembled by id-joins (no embeddings) and must quote its evidence or
> abstain; it calibrated at 1.000 precision both against my own 148 decisions
> and against an adversarial test that hides the right person among real
> same-surname namesakes.
>
> Three things are open, none blocking. (1) 51 names the judge recovered from
> its own free-text output that aren't in the workbook yet — ask me whether
> they go in a separate sheet or as proposals. (2) 134 organisations named
> after people with no hub at all, which is a people-side gap, not an eponym
> answer. (3) Whether the same engine should now draft the Zalmen
> org-alignment and minting queues, which was the point of the pilot.
>
> Tell me which you'd start with and why before doing it.
>
> Two things not to do: don't match a person on surname alone (it gives
> `ליאַ לאָרגמאַן` → `ברין, אַליאַ`, different people), and don't split a hub
> heading on the comma — 22% of them have none, which is the bug that made
> Joseph Kessler look absent from the corpus.
>
> Dry-run before writing, verify with a fresh read rather than the pattern
> that made the edit, and commit with explicit paths.
