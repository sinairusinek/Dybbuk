# Handoff: two core_db data defects + matcher recall improvements

> For a separate session. Repo: `/Users/sinairusinek/Documents/GitHub/Dybbuk`
> Dir: `Zylbercweig/organizations/`
> Date: 2026-05-27
> Author of this note: investigation triggered by "off-candidate manual alignments" audit.

---

## Background — how we got here

We audited the 24 `ALIGN` rows in `org_alignment_review.tsv` where the reviewer
aligned a cluster to an **existing DB entity that the auto-matcher never proposed**
(`aligned_db_id` absent from `candidate_db_ids`). Diagnosing *why the matcher missed*
surfaced (a) real matcher gaps — now partly fixed — and (b) **two core_db data defects**
that masqueraded as "needs external knowledge." This note covers both, who/what caused
them, the matcher fixes already made, and how to **find more cases like them**.

`cluster_pairs_review.tsv` was also checked: it is a pre-filtered candidate queue
(every row, incl. all 315 MERGEs, sits at similarity 0.8–1.0), so it has **no**
off-candidate manual merges by construction. A side-finding: true merges below the
0.8 similarity floor are structurally invisible in that workflow.

---

## Defect 1 — db42 `name_yiddish` is contaminated with another entity's name

| field | value |
|---|---|
| db42 `name` | `גאָלדפאדעןס טרופּע -Avraham Goldfaden Troupe` |
| db42 `name_yiddish` | `טרופּע ציפקוס` ← **wrong entity** |
| db490 `name` | `טרופּע ציפקוס` (the *real* Tsipkus troupe, linked ORG-C00050) |

`טרופּע ציפקוס` belongs to db490; it does not belong on db42 (which is the Goldfaden troupe).

**Root cause (NOT an RA hand-edit):**
1. Cluster **ORG-C00047** (canonical `טרופּע ציפקוס`) is aligned to **db42** — but that
   alignment row has a **blank `reviewer` and blank `reviewed_at`**. It is an
   unattributed seed/import alignment, never reviewed in the Zalmen app. It is almost
   certainly wrong (should point to db490).
2. `backfill_name_yiddish_from_alignments.py` fills an *empty* `core_db.name_yiddish`
   from the aligned cluster's `canonical_yiddish`. db42's `name_yiddish` was empty until
   commit `d26cc2d` (2026-05-17, "Phase 3+4 … recall + SPLIT pipeline"), where the
   backfill ran and stamped `טרופּע ציפקוס` onto db42 from the wrong ORG-C00047 alignment.

So the contamination = **an automated backfill propagating a pre-existing, unattributed
mis-alignment**. No named RA caused it.

**Fixes to apply (after human eyeball):**
- Re-point ORG-C00047 alignment: db42 → **db490**.
- Clear/repair db42 `name_yiddish`: should be `גאָלדפֿאַדענס טרופּע`, not `טרופּע ציפקוס`.
- Re-run `backfill_name_yiddish_from_alignments.py` only after fixing the alignment,
  or it will re-stamp the wrong value (it only fills empty cells, so clear it AND fix
  the alignment in the same pass).

## Defect 2 — duplicate Guzik-troupe DB entities (264 ↔ 486), plus a garbage bucket

| db | `name` | `name_yiddish` | linked |
|---|---|---|---|
| 264 | `Guzik Troupe` | `גוזיקס טרופּע` | (unlinked) |
| 486 | `טרופּע פֿון גוזיק` | `גוזיק` | ORG-C00034 |

Same entity, two rows — **present in the original source import (commit 187af20)**; this
is a source-data duplicate, not an RA error. In fact RA **Bella** *flagged* it:
ORG-C00751 → db264, reviewer note *"יש שני מזהים - השני מיותר"* ("there are two
identifiers — the second is redundant").

Worse, db486 has become a **catch-all bucket** — clusters aligned to it:
```
db486 ← ORG-C00033 גוזיק            ✓ Guzik
        ORG-C01591 גוזיקן           ✓ Guzik
        ORG-C05624 גוזיקס אַרופּע     ✓ Guzik (OCR typo for טרופּע)
        ORG-C00034 אָפּערע-טעאַטער אין פּערם   ✗ Perm opera theatre
        ORG-C02330 טרופּע פֿון קאָריק   ✗ Korik troupe
        ORG-C06413 טרופּע פֿון זשוקאָוו ✗ Zhukov troupe
```
…while the genuinely-Guzik ORG-C00751 (`גוזיקס טרופּע`) sits on db264.

**Fixes to apply (after human eyeball):**
- Merge db264 ↔ db486 (dedup; keep one canonical id, re-point alignments).
- Investigate the three non-Guzik clusters parked on db486 (Korik/Zhukov/Perm) — pull
  their mention contexts; they likely need their own entities.

---

## Attribution summary (the "which RA" question)

**Neither defect was caused by an RA's manual mistake.**
- Defect 1: automated `backfill_name_yiddish_from_alignments.py` propagated an
  **unattributed** (reviewer-blank) seed mis-alignment (ORG-C00047→db42).
- Defect 2: **source-data duplicate** present at first import; RA Bella *flagged* it.

Implication: git blame on `core_db.tsv` is useless for attribution — the file is
bulk-rewritten by the app's `chore: save core DB` commits (all authored "Sinai" via
github_sync). To find similar problems, target the **backfill output and the source
data**, not RA review logs.

---

## Matcher improvements already made (this session)

Two changes to candidate generation, both motivated by the off-candidate misses:

### 1. Cross-script: surface embedded Yiddish runs from mixed `name` fields
`prepare_alignment.py::split_name_variants` now extracts top-level Yiddish-script runs
from each name part (previously only inside parentheticals). A mixed DB name like
`גאָלדפאדעןס טרופּע -Avraham Goldfaden Troupe` (en-dash not space-delimited, so the
` - ` split didn't separate scripts) now yields the clean variant `גאָלדפאדעןס טרופּע`,
which a Yiddish cluster can match.

### 2. Token-set similarity (order-independent, morphology-stripped)
New `org_normalize.py::token_key_set` / `token_set_similarity`, wired into
`prepare_alignment.py` as a `token_set` method (floor 0.60, requires a shared content
token of len≥3). It drops generic head-nouns (טרופּע/טעאַטער/…), of-tokens (פֿון/of),
articles, strips possessive/plural `ס`, maps city-adjectives (ניו-יאָרקער→ניו-יאָרק) and
strips an unmapped adjectival `ער`. So:
- `טרופּעס פֿון מאָגולעסקאָ` ↔ `מאָגולעסקאָס טרופּע`  → {מאָגולעסקאָ} (1.0)
- `אוניווערזיטעט פון בערלין` ↔ `בערלינער אוניווערזיטעט` → {אוניווערזיטעט, בערלינ} (1.0)
- `גאָלדפֿאַדענס טרופּע` ↔ `גאָלדפאדעןס טרופּע` → {גאָלדפאדען} (1.0)

**Recall against the 24 off-candidate ALIGN cases: 0/24 → 6/24 newly in candidates**
(re-ran `prepare_alignment.py`, re-checked `aligned_db_id ∈ candidate_db_ids`):

| cluster | →db | rank | via | attributable to |
|---|---|---|---|---|
| ORG-C05160 | 42 | 1 | exact | Change 1 (Yiddish-run extraction → clean `גאָלדפאדעןס טרופּע`) |
| ORG-C06643 | 42 | 1 | ipa_phonetic | Change 1 |
| ORG-C04833 | 42 | 3 | ipa_phonetic | Change 1 |
| ORG-C05812 | 561 | 1 | token_set | Change 2 (`בערלינער` ↔ `פון בערלין`) |
| ORG-C06095 | 337 | 1 | token_set | Change 2 (`טרופּעס פֿון מאָגולעסקאָ` ↔ `מאָגולעסקאָס טרופּע`) |
| ORG-C02149_Q01 | 125 | 1 | ipa_phonetic | incidental (accumulated pipeline state) |

So the whole Goldfaden family (Defect-1's db42) is now reachable, and the two
word-order/morphology cases fall out cleanly from token_set.

**Environment note:** `jellyfish` is NOT installed (system Python or `.venv`), so the
DM-soundex `phonetic` method is currently a no-op everywhere — the baseline candidate
lists were `ipa_phonetic`-only, so this measurement is apples-to-apples. Installing
`jellyfish` is a cheap additional lever for the Latin-named misses below (Lipzin/Thalia/
Vikt) — DM soundex over Latin forms.

### The 18 still-missing — residual buckets (for the type-wall / cross-script next pass)
- **Cross-script, English DB name, ipa ranks a decoy above target:** db299 Keni Lipzin
  (`ליפּצין-טעאַטער`), db130 Thalia (`טאָליאַ`), db482 Handelskamer, db0 Am Olam, db433 Vikt
  (empty candidate list — acronym `וויק'ט`). Better cross-script normalization /
  jellyfish would help here.
- **Spelling-divergent Yiddish (OCR/orthographic):** db485 Kompaneyets
  (`קאָמפּאָנעעץ` vs `קאָמפּאַניעעצ`, ×2), db257 Gimpel, db488 Ararat, db474.
- **DB-duplicate pick (not a real matcher miss):** db486 Guzik (top=264, the dup twin) —
  resolved by Defect-2 dedup, not by matching.
- **Toponym synonymy:** db525 (`לעמבערגער` ↔ Lwów-named `לוואָווער`; top=544 is itself a
  DB dup of 525). Feed settlement aliases into matching.
- **Type-wall:** several of the above are also cluster=Theatre vs DB=Traveling-Company;
  the deferred between-related-types peeking pass would recover them.

### Deferred per PI (do NOT enable yet)
- **org_type wall peeking.** Many misses are cluster=Theatre vs DB=Company-on-Tour /
  Traveling-Company for the same entity. Dropping the type block would raise recall but
  PI is worried about false-positive flooding. Plan: enable peeking **only between
  specific related type pairs**, and do it as a **one-off offline run** outside the
  Zalmen app — not in the live pipeline.

---

## How to find MORE cases like these (next session's main task)

### Class A — contaminated `name_yiddish` (Defect-1 family)
Detection queries over `core_db.tsv`:
1. **Cross-row name collision (smoking gun):** any db row whose `name_yiddish` exactly
   equals another db row's `name` (or `name_yiddish`). db42.name_yiddish == db490.name
   is exactly this. Cheap exact-string scan.
2. **Internal name/name_yiddish mismatch:** for each row with both fields populated,
   compute `token_set_similarity(name, name_yiddish)` (and/or cross-script IPA). Low
   similarity ⇒ the two fields likely describe different entities ⇒ candidate
   contamination. Rank ascending, hand off the bottom.
3. **Backfill provenance:** cross-reference rows whose `name_yiddish` was filled by
   `backfill_name_yiddish_from_alignments.py` against alignments with **blank reviewer**
   — those are unverified seed alignments that may have stamped wrong Yiddish.

### Class B — duplicate DB entities (Defect-2 family)
1. Run the **improved matcher DB-against-itself**: for every pair of core_db rows,
   compute exact/token_set/IPA similarity; surface high-scoring pairs as dup candidates
   (264↔486 would top the list). Mind transliteration: a Latin row and its Yiddish twin.
2. **Garbage-bucket detector:** for each db row with ≥3 aligned clusters, compute
   pairwise name similarity among those clusters' canonicals; a row whose aligned
   clusters do NOT mutually match (like db486's Guzik+Korik+Zhukov+Perm) is a bucket —
   flag for splitting.

Both classes want a small reviewable punchlist (TSV) for Maati, not an auto-fix.

---

## Files touched this session
- `prepare_alignment.py` — Change 1 (Yiddish-run extraction) + Change 2 (token_set wiring).
- `org_normalize.py` — new `token_key_set`, `token_set_similarity`.
- `org_alignment_review.tsv` — regenerated candidates (decisions preserved).
  Backup: `org_alignment_review.tsv.pre_recall_backup`.

Not committed — needs user authorization.
