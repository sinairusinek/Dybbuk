# Push note

## CLOSED — merged into main, 2026-10-12

The branch was merged to `origin/main` through a PR. The conflict map below is
**historical**, and its central recommendation turned out to be wrong in one
respect worth flagging: resolving `core_db.tsv` and `activity_log.tsv` with
`git checkout --ours` would have discarded the publishers session's additions.
The merge was done per cell against the merge base instead — union, not either
side, exactly as the "Recommended approach" section below concludes.

A third id collision also had to be cleared first: the app had taken
**2809–2820** while this sat unmerged, so the publishers moved to
**2846–2857**. See §8 of `organizations/PUBLISHING_DYNASTY_MODEL_PROPOSAL.md`
(`P173`–`P175`) and the "MERGED into main" section of `HANDOFF.md`.

---

## Superseded — pushed as a branch, 2026-10-11

Everything is on GitHub: **`galron-press-handoff`** at `bcb821940`, 38 commits,
verified matching the local HEAD.

<https://github.com/sinairusinek/Dybbuk/pull/new/galron-press-handoff>

`main` was left alone — still 38 ahead / ~2,400 behind, unrebased. The merge
into `main` is deliberately left to a PR, where the conflicts below can be
reviewed properly instead of resolved blind in a terminal. The rest of this
note is the conflict map for whoever does that merge.

---

**Original task:** rebase this branch onto `origin/main` and push.

**State:** `main` at `22430e8f7`, **37 commits ahead, ~2,400 behind**. Tree
clean except untracked files and `venues_unlinked.csv` (a regenerated output,
dirty since before this work; origin never touches it).

**Safety net:** branch `PRE_REBASE_BACKUP_2026_10_11` == `22430e8f7`, the exact
pre-attempt state. `git rebase --abort` returns there. Verified twice today:
abort restores HEAD, the tree, and `venues_unlinked.csv` byte-for-byte.

## Why it isn't done

Two attempts (2026-10-11). The rebase replays 37 commits and conflicts
repeatedly in the same three append-only TSVs, because both sides appended rows
while the app committed ~2,400 times upstream. It is mechanical, not hard —
but it needs a judgement call per conflict, and the later ones touch **the
publishers session's data, not this session's**, which is why I stopped rather
than resolve them.

## The conflicts, in the order they come

**Commit 11/37** — `862412e1a` "Audit the publisher rows against the
publisher-like clusters" — `core_db.tsv` + `activity_log.tsv`.
Benign: origin appended rows 2661–2715, the incoming side of the hunk is empty.
Keep origin's:
```
git checkout --ours -- Zylbercweig/organizations/core_db.tsv Zylbercweig/organizations/activity_log.tsv
git add Zylbercweig/organizations/core_db.tsv Zylbercweig/organizations/activity_log.tsv
GIT_EDITOR=true git rebase --continue
```
(Verified: afterwards 0 conflict markers, all 55 of rows 2661–2715 present.)

**Commit 15/37** — `ea767ed3c` "Apply the accepted publisher-audit decisions to
the cluster ledger" — `core_db.tsv` + `activity_log.tsv` +
`org_alignment_review.tsv` (20 hunks).
**NOT benign — do not blanket-keep either side.** Inspected cell by cell: the
incoming side writes `org_type = Publisher` into 15 empty cells (db 47, 48, 49,
50, 52, 53, 54, 55, 56, 57, 61, 62, 63, 64, 65) and adds
`linked_cluster_ids` on five (db55 `ORG-C05292`; db57 gains
`ORG-C00937 | ORG-C02255 | ORG-C05576`; db61 `ORG-C00612 | ORG-C05936`;
db63 `ORG-C04218 | ORG-C06894`; db64 `ORG-C06527`). **Every one is an addition
into an empty cell — no competing values.** So the merge is a union, and
`--ours` here would silently discard the publisher audit's classifications.

Expect more of the same shape in the remaining 22 commits.

## Recommended approach

Rather than hand-merging ~20 hunks across three files, consider:

```
git rebase --abort                      # start clean
git checkout -b galron-press-handoff    # keep main untouched
git push origin galron-press-handoff    # push the branch as-is, no rebase
```
A branch push needs no rebase at all, puts everything safely on GitHub, and
leaves the merge to a PR where the diff can be reviewed properly — which suits
data this contested better than a terminal conflict resolution.

If a rebase onto `main` is still wanted, the rule throughout is **union, not
either side**: these files are append-only and each side filled cells the other
left empty.

## What is in the 37 commits

20 are the Galron press matching work (all in `Zylbercweig/reference/`, plus 5
`address` cells in core_db). The rest are the publishers session and some
YiDraCor commits that were already on this branch. Entry point for the press
work: `Zylbercweig/reference/HANDOFF.md`.
