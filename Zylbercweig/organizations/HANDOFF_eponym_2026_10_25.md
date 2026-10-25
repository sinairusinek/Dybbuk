# Eponym track — handoff from the 10-14/10-15 session

Written 2026-10-25 by the session that did the 10-14/10-15 work, as it closes.
The track is now owned by another session (it holds the uncommitted
`ingest_eponym_ra_workbook.py` edit for Ruthie's 10-21 workbook revision).

**Read `NEXT_SESSION_eponym_judge.md` too** — it still describes the judge
pipeline correctly. This file covers only what changed after it, plus what is
open.

---

## RESOLVED 2026-10-25, later the same day: the branch is merged

**This section's original warning is obsolete — do not go looking for
`recover-session`, it no longer exists.** Kept here, corrected, because the rest
of the file was written while it was still true.

A consolidation session merged `recover-session` into `main` as `05a2ab20c` and
deleted it. The checkout is on `main` at `origin/main`, nothing ahead or behind.
The branch tip is preserved as tag **`archive/2026-10-25/recover-session`**
(`a07aea804`, pushed to origin; 31 such tags in total, one per retired branch and
stash). Restore with
`git branch recover-session archive/2026-10-25/recover-session`.

Verified independently after the merge, not taken on trust:

- All ten key commits are ancestors of `main` (`9a4927395`, `3a76e27ab`,
  `35409eaa2`, `c11918d50`, `a27f4bd35`, `ed64cbb6c`, `d1f22725b`, and the three
  handoff commits).
- **No ledger rows were lost, and the data grew**: 414 decisions (375 CONFIRM /
  39 REJECT; Sinai 156, Ruthie 258), **302 eponym edges** over 232 orgs, 0
  duplicate pairs, 0 duplicate decision rows.
- `people_db` 3,027 rows with the 24 live mints intact and 3829/3841 still
  merged away; the README consistency check PASSES.
- Both 10-15 surface detaches held: `אידא קאַמינסקאַ` has NOT returned to
  Regina's `HUB-D3612`, and `חיהלע פֿרידמאַן` has NOT returned to the Ash hub.
  A careless merge would have restored both.
- `HUB-D383` still reads `שלום-עליכם` with its 239 mention links.
- Line endings survived: `people_db` and `people_extracted` CRLF,
  `eponym_decisions` LF.

The `core_db` staleness this section used to warn about is gone with the merge —
the checkout now has main's org table. Re-measure before assuming anything about
candidate yield.

---

## State as of 2026-10-25 (verified by fresh read, not remembered)

Includes the 10-25 commit `22b47de6c` by the incoming session (Ruthie's 10-21
Drive answers + her two reject vocabularies).

| | value |
| :-- | :-- |
| `eponym_decisions.tsv` | 404 rows — 365 CONFIRM, 39 REJECT; Sinai 156, Ruthie 248 |
| eponym edges | 295, over 228 orgs, 0 duplicate (db_id, person_hub_id) pairs |
| `eponym_RA_workbook.xlsx` (repo) | 374 troupes / 230 links / 2592 persons — **stale vs Drive AND vs the ledger; deliberately not rebuilt** |
| `eponym_recall_candidates.tsv` | 54 recovered · 1 ambiguous · 58 attested · 73 absent |
| `ra_comment_identifications.tsv` | 18 rows — 5 resolved, 5 attested, 8 unresolved |
| `names_not_minted.tsv` | 12 rows |
| minted people | `people_db` 3829–3854, less 3829+3841 merged away = **24 live** |

The Drive workbook (`1kPuKA8E4EnYnbf_fbFSVHyckEHI0TuDd`, modified 2026-10-21) is
NEWER than the repo copy and is the one to ingest.

**The repo workbook is OUTPUT ONLY.** Only `build_eponym_ra_workbook.py` writes
it (`-o` defaults to that path) and nothing reads it as input — the one other
mention, in `build_eponym_persons_csv.py`, is a docstring. Refresh it from the
ledger by re-running the builder; never hand-edit it. Its `links` sheet is
regenerated from `eponym_decisions.tsv`, so it reappears on every build.

---

## THE OPEN BLOCKER: a decided person who can never be applied

ONE blocker, two faces. State it as: **an eponym edge requires `person_hub_id`;
`person_hub.tsv` is built by `people/build_person_hub.py` from ENTRIES ONLY; so a
minted-but-unheaded person can be decided but never applied.**

That covers **24 of the 26 mints** (3829–3854 less the two merged away) plus at
least three of Ruthie's named people. Having no entry is the very reason they
were minted, so this is structural, not an oversight. Nobody has designed how
such a person gets a hub. This is the main unfinished piece of the track.

### Bug 6: the ingest DROPS free-text identifications

Found by the session taking over, 10-25, in code I wrote (`c11918d50`).
`ingest_eponym_ra_workbook.py` consults the comments column **only when its
content matches a reject token**:

```python
if not answer and cmt_col:
    note = ...
    if note.lower() in NONE_TOKENS or note.lower() in OUT_OF_SCOPE_TOKENS:
        answer = note
```

A free-text NAME there is skipped entirely. So all 101 CONFIRMs in `22b47de6c`
came from the `person_name` dropdown and **28 of Ruthie's comment answers never
reached the ledger** — the ones where she named someone the generator offered no
candidate for: Leo Largman (db804, db825), Fischel Kanapoff, Kokhanski, Vera
Kanievska, Harlamp (db851, db1049), Kalmen-Moyshe Ebel (db545, db1034), Chaim
Sandler (db814, db1068), Dr. Rosenthal, Korsh, Eskin, Krylov, Moshe Zilberman,
Byelayev, the duo splits db858 דזשיגאן/שומאכער, db883, db889, and db864
אידא/קאמינסקי.

`ingest_ra_comments.py` harvests those to `ra_comment_identifications.tsv`, but
that is a separate ledger — it does not produce decisions or edges.

**DO NOT fix this with a naive name→hub fallback.** Here is why, concretely:

**db864 Ida Kaminska.** Ruthie answered `אידא קאמינסקי` on both the `אידא` and
`קאמינסקי` spans. That is correct and agrees with the 10-15 analysis. Ida is
`people_db 3832` and has **NO hub**. A fallback that resolves her label against
hub surfaces or headings can land on her sister Regina's `HUB-D3612` and silently
restore the exact misattribution that `9a4927395` removed — Regina died
1913-09-28, Ida lived 1899–1980, and 56 of Ida's attestations had been credited
to her dead sister. The only reason it did not happen on 10-25 is that the label
was never resolved at all.

So the 28 are blocked on the hub question, not on parsing. Of them,
`אידא קאמינסקי`=3832, `לייזער בערנשטיין`=3833 and `אַלעקס שטיין`=3837 are db rows
with no hub; `שמעון דזיגאן`, `ישראל שומאכער` and `יידל גאלדפאדען` have neither
hub nor db row and need minting first. Solve the hub question, then wire the 28.

---

## Open decisions (all four still Sinai's)

1. **~~The 54 recovered names~~ — DONE.** 29 injected as proposals (`35409eaa2`);
   43 of the 54 were already decided and 0 contradicted a human decision.
   `db1482 ראטבאום` excluded by id as a bad match.
2. **The attested names → minting.** 26 minted 10-15, approved by Sinai. The
   remaining question is the hub problem above, not the people.
3. **Hyphenated headings — NOT STARTED.** `שלום-עליכם` is one hyphenated token,
   so `split_heading` keys it whole as the surname with NO given tokens and it
   can never match. **15 orgs are named after him** and the generator produces a
   candidate for only ONE, at `low`. Decide: fix the few headings as data, or
   teach `split_heading` the pattern (survey how many hubs are written that way
   first).
4. **Does the engine graduate to Zalmen org alignment?** The pilot's actual
   purpose. Prerequisites: wire up Gemini prompt caching (the system prompt is
   resent per call, ~half the input), and diagnose the person-data gap the
   `attested` result exposed, which org alignment would hit too.

## Two hub-level defects, neither fixed

- **`HUB-D1345` is a chimera.** It merges `בלומענטאַל, נייטאַן` (b.1870, vol 1,
  administrator) with `בלומענפעלד טורקאָוו, דיאַנאַ` (b.1906, vol 4, performer).
  Its heading says Blumental, its `db_id` 1345 is Diana, its top surface is
  `דיאַנאַ בלומענפעלד` ×18. Needs splitting before any edge uses it.
  Diana's person id IS `people_db 1345` — link db1482 to 1345 and nothing else.
- **Ida needs a hub** (above).

## Named-but-unresolved people

- `חיהלע פֿרידמאַן` — detached from `חיהלע אָש (פֿורמאַן)` on 10-15 (Furman is not
  Friedman; Ash was born 1920, Friedman ran a collective in 1927). Never minted,
  so still a real gap.
- `ברידער קאָכאַנסקי` — the Leksikon never names them; Latin and Yiddish search
  found nothing. Mint the org, defer the people.
- Two mints carry **CHECK BEFORE LINKING** in `new_mints_evidence.open_questions`
  and were merged on Sinai's ruling: `3829 → db2282` (Libert) and
  `3841 → db2394` (Morgules).

---

## Six pipeline bugs — five fixed, bug 6 OPEN (above)

All of them were LOSING WORK silently.

1. **The 90th proposal vanished.** The workbook labelled a proposal
   `heading [person_db_id]` and fell back to blank when that id was empty. 301 of
   2,897 hubs are facsimile-derived (`HUB-P-*`) with no `people_db` row, so the
   guard sat in front of all of them. The summary line counted verdicts, not
   labels written, so it printed 90 while shipping 89. (`f4a377884`)
2. **A bracketed letter inside a surname hid Boris Tomashevsky.**
   `טאָמאַשעוו[פ]סקי` became two tokens; a man with his own entry (HUB-D693) and
   122 mentions came back `absent` for three orgs. `heading_variants()` now
   indexes both readings. (`d3b6d8b87`)
3. **`absent` never meant "not in the corpus."** It meant "no entry heading
   matches." `mentions_all.tsv` (38,269 mentions) was never consulted; 58 of 134
   `absent` names are in the corpus as mentions. New `attested` outcome.
   (`3ed24f963`)
4. **Decisions were keyed per person, not per span.** db1335 names Esther-Rachel
   Kaminski across four spans; only the first was recorded, so a rebuild
   **re-asked 31 questions Ruthie had already answered.** Now keyed on the
   `(db_id, hub_id, span)` triple, with a matching dedup in
   `apply_eponym_decisions.py` so extra spans don't become extra edges.
   (`56ebac0fa`)
5. **Her comments column was read by nothing** — 19 of 22 identifications and 11
   span rejections never reached the ledger. New `ingest_ra_comments.py`.
   Also: a hubless REJECT never closed its span, so "this span is not a name"
   was asked forever. (`c11918d50`)

6. **The ingest drops free-text identifications** — see THE OPEN BLOCKER above.
   28 of Ruthie's answers still unlanded. **OPEN.**

The pattern: **five of six were discarding a reviewer's answers, and the summary
lines kept reporting success.** Count what was WRITTEN, never what was intended.
Every one of them was found by reconciling the data against the reviewer's own
sheet, never by reading a log.

### Harden this BEFORE the hub question

The incoming session's recommendation, and it is the right order of work: the
summary line is the thing that lies. **Make `0 unmatched` actually mean nobody
was dropped** — add a third counter for *answers seen but not resolved*, so a
cell holding content that no branch consumed is reported rather than counted as
blank. Every one of the six bugs would have surfaced on its first run against
such a counter. Without it the next reviewer round loses answers the same way and
nobody notices for a week.

---

## Data traps that cost real time

- **`people_db.tsv` no longer holds the Hebrew-stylistics rows.** 478 moved out
  to `people_db_hebrew_stylistics.tsv` (`ed64cbb6c`, 10-14). Any note saying "its
  first rows are modern Israeli writers" is STALE.
- **Hebrew is stored PRECOMPOSED.** `person_hub_members` has `פֿ` as the single
  codepoint `U+FB4E`; a literal typed in a script is `U+05E4 + U+05BF`. `==` is
  False and a detach silently does nothing — this bit the 10-15 fix on its first
  run. Normalise through NFC. Same family as the `core_db` `U+FB2E` bug.
- **Line endings differ per file.** `people_db.tsv`, `core_db.tsv` and
  `people_extracted.tsv` are CRLF; `new_mints_evidence.tsv`,
  `eponym_decisions.tsv`, `names_not_minted.tsv` and `activity_log.tsv` are LF.
  Detect per file; a split-on-`\n` rewrite of `people_extracted.tsv` would have
  rewritten all 3,085 line endings.
- **Check a name against hub SURFACES, not just headings.** Four "new" people
  already existed under a different spelling (`אייווען אַבראַמסאָן`=735,
  `אַלעקסאנדער מאַגיידזאָן`=2371, `סידי טאָל`=3070 `טהאל`,
  `חיהלע פֿרידמאַן`). The recall index keys headings only — that is why it also
  listed 7 names Ruthie had already resolved.
- **Never match a person on surname alone.** `ליאַ לאָרגמאַן` matches
  `ברין, אַליאַ`; `זיגמונד טורקאָוו` matches `זיגמונד שוואַרץ`. Different people.
- **`מינע` in `מינע און דיאַנאַ בלומעפעלד` is not a name** — it is `מיינע`,
  "my", in Jonas Turkow's first-person narration (`בעת מיינע אונ דיאנא
  בלומענפעלדס גאסטראלנ`, recurring in four entries). One eponym, not two.
- **The judge's `confidence` is worthless** — all 92 answered cases rated
  themselves `high`.
- `core_db.tsv` has embedded newlines in quoted fields; use Python `csv` with
  `field_size_limit(10**9)`, never awk/grep.
- **Build column positions from `HEAD`, not literals.** Inserting "Ruthie's
  comments" as column J shifted four hard-coded positions (`row[8]`, the
  `I2:I...` dropdown range, a letter-keyed width map, wrap/RTL index lists).

## The spot-check queue — 153 settled links worth a second read

`eponym_spotcheck_queue.tsv`, generated 10-25 from the ledger. Every CONFIRM
that is either `low` tier or whose SPAN does not appear in the chosen person's
heading. 153 rows (was 94 before the 10-21 answers landed), of which **43 are
span-not-in-heading** — the stronger signal. By reviewer: Ruthie 145, Sinai 8.

This is the test that caught **db1047** `זאָלאָטאַרעווסקיס טרופּע` answered as
`זאַסלאַווסקי` — a row slip onto a different surname, where that hub was also
Sinai's own CONFIRM for a different org. Read the org name against the person.

Most flags are benign: a possessive `ס` on the org name, or an OCR variant the
reviewer corrected by eye (`מעערסאן` → `מייערסאָן`, `זייגלער` → `זיגלער`). Two
were raised with Sinai on 10-15 and are **still unresolved**:

- **db287** `איציק מענדל בערמאָןס טרופּע` → `בערגמאַן, איציק-מענדל`. Berman vs
  Bergman: surname differs by a letter, but the given name matches exactly and
  the entry corroborates. Probably hers is right and the ORG name is the OCR
  error — same shape as the db1047 slip, so worth a deliberate ruling.
- **db1068** `אָפּערעטן-טרופּע פון סאָנדלער, ליבערט און קאָמפּאַנעיעץ` →
  `קאָמפּאַנעעץ, אַבא`. Fine in itself (Sinai REJECTed Max Kompaneyets first),
  but note the org is a THREE-partner troupe and `סאנדלער` + `ליבערט` had no
  decision as of 10-15 — check whether the 10-21 round closed them.

Regenerate it after any ingest; it is derived, not a ledger.

## ~~The unmerged branch~~ — MERGED, see the top of this file

Superseded: `recover-session` was merged into `main` as `05a2ab20c` and deleted
on 2026-10-25, tip archived as tag `archive/2026-10-25/recover-session`. The
verification is at the top. What follows is kept only because the merge-hygiene
rule still applies to these files in any future reconciliation.

What was at stake, and is now on `main`:
`ed64cbb6c` (478 Hebrew-stylistics rows split out of people_db), `d1f22725b`
(Sholem Aleichem 3492 merged into 383), `a27f4bd35` (his reversed heading fixed
+ 239 mentions linked), `3a76e27ab` (26 people minted), `9a4927395` (Ida
separated from Regina), and the whole judge pilot from `4bc15b2fd` onward.

The data files are the part that matters: `people_db.tsv`,
`new_mints_evidence.tsv`, `person_hub.tsv`, `person_hub_members.tsv`,
`eponym_decisions.tsv`, `person_org_edges.tsv`, `names_not_minted.tsv`. Per
Sinai's standing rule these are append-only ledgers and must be merged
**field-level, never with `git merge --ours/--theirs`** — each side fills cells
the other left empty, and line endings differ per file (see the traps).

That rule still stands for any future reconciliation of these files, and the
10-25 merge respected it — verified above: no rows lost, both surface detaches
held, no duplicate decision rows.

## Review doc

<https://claude.ai/code/artifact/f590847d-1d22-4638-86c5-5595338a5040> — the
10-15 review doc for Sinai, written so he could review before passing the
workbook to Ruthie.

Partly stale now: its "four open decisions" item 1 is done, and its spot-check
table is superseded by `eponym_spotcheck_queue.tsv` (94 rows there, 153 now).
What it still holds that is NOT in this file: the reconciliation of Ruthie's
original 136 answers (25 agreeing with existing CONFIRMs, 25 off-candidate, 0
conflicts), her 22 comment identifications in their three kinds, and the db1047
correction written out with its evidence. Worth opening once rather than
treating as superseded wholesale.

Also stale in it: one truncated org name — it shows db1068 as
`אָפּערעטן-טרופּע פון סאָנדלער, ליב`, which is `ליבערט` cut at 34 characters,
not a given name. Do not read a person out of that.
