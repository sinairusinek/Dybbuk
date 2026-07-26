# Lateiner/Hurwitz knowledge graph — what we need from you

*Prepared 2026-07-26. Everything mechanical has already been auto-resolved
(rules + drafter, spot-checked). Below is ONLY what needs a human judgment.
Answers go into the named TSV columns — no separate document needed.*

---

## PI — one policy decision + two single calls (≈ 30 min)

### 1. Approve (or reject) the author-swap punchlist — the one decision that matters
**File:** `eval/attribution_resolution.tsv`, the **64 rows** with
`resolution = lexicon_contradicts_db`.

The play lists in `people_db.tsv` (`created_expressions`) assign these 64
titles to one playwright, but the title appears **only in the OTHER
playwright's own lexicon entry**, and the DybbukCatalogue agrees with the
entry. Example: עזרא sits in Hurwitz's list, but only Lateiner's entry (and
the catalogue, and the 1908 print edition) has it.

**Question: may we move these 64 titles to the other author in people_db?**
- If yes to all: reply "approved" (we apply them in one script run; each row's
  `recommendation` column says exactly what moves where).
- If you want to check first: spot-check any 5–10 rows; the `matched_segment`
  column shows the title as found in the other entry.

*Side effect of approving: 4 of the 6 flagged play↔Transkribus-edition links
(יידעלע, עזרא, ציון/על נהרות בבל, דער מאן אונטערן טיש) become consistent
automatically.*

### 2. משפּט שלמה — one play or two? (row `PL-0223` in the same file)
The title appears in **both** playwrights' entries. Decide: two distinct
same-titled plays (keep both nodes, status `disputed`) or one play (tell us
which author). Write the answer in that row's `recommendation` column.

### 3. קידוש השם — who is the author of the printed edition's play?
**File:** `eval/eval_findings.tsv`, filter `aspect = attribution`, play
קידוש השם (plays `PL-0257`/`PL-0258`).
The lexicon list AND the catalogue works sheet say **Hurwitz**; the
catalogue's print-edition record (Transkribus doc 820939, premiere 1896
Windsor Theatre) says **Lateiner**. Possibly both wrote one. Write your call
in the `adjudication` column of those rows.

---

## RA — small, concrete checks (≈ 1–2 hours)

Fill the named column in each file; leave a short note where asked.

### 1. Two undecidable entity links
**File:** `kg_link_review.tsv` — the only **2 rows with an empty `decision`**:
- play surface **שמשון הגבור** — is this mention Hurwitz's play (`PL-0272`) or
  another author's Samson play? (Context: a 1909 immigrant actor under Moshe
  Richter's influence.)
- venue surface **מאַלווינאַ לאָבעלס ראָיאָל טעאַטער** — same as cluster
  `ORG-C04687` or a different theatre?
Write `ALIGN` + the link, or `REJECT`, in `decision`/`decided_link`.

### 2. Nine date/venue disagreements with the newspaper catalogue
**File:** `eval/eval_findings.tsv`, rows `F-0365, F-0377, F-0402, F-0411,
F-0412, F-0413, F-0452, F-0453` (+ any row with `aspect = premiere_year`).
For each: read the quoted lexicon sentence (`evidence` column) and say in
`adjudication` which is right — `lexicon_error`, `extraction_error`, or
`catalogue_error`. These are exactly the "lexicon vs. newspapers" cases the
evaluation was built to surface; a real lexicon error is a finding worth
keeping, not a bug.

### 3. One impossible date
Entry `P-1-facs_135_tr_1740521022` produced a block of events dated
**1854-10-09** (before Yiddish theater existed). Open that entry, find the
date near the „אלמנה" cast list, and tell us what it actually says (likely a
misread or a Hebrew-calendar date). One-line answer is enough (note it in
`eval/eval_findings.tsv` on any `production_before_premiere` row for that
entry).

### 4. Optional QA sample (only if time permits)
Pick ~20 random rows with `decision` starting `GEMINI_` in
`kg_link_review.tsv` and mark disagreements in `reviewer_notes`. This
calibrates how far we can trust auto-adjudication before scaling to all
playwrights. Skip if pressed for time — the graph already marks these as
machine-decided.

---

## Explicitly NOT needed from you
- The other ~969 link-review rows (auto-resolved; machine-labeled in `decision`).
- The 56 `unresolved` attribution rows (title absent from both entries) — these
  wait for a source pass (JPRESS/Sieger), not for you.
- The 11 known homonym false-positives and recall gaps — pipeline fixes,
  already queued in `eval/eval_notes.md`.
