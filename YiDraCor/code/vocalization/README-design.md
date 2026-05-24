# Vocalization design log

Non-obvious decisions behind the pipeline, with the evidence we have for
each. Read this when a future change feels like it should be obvious but
isn't.

## Rule inventory

Mined empirically from page 6 of *Yudale der Blinder* (the only fully
hand-vocalized reference at the time). All numbers refer to that page.

| # | Rule | Evidence | Confidence |
|---|---|---|---|
| A | Speaker name + stage directions in `(…)` are not vocalized | 17/17 speakers bare | 100% |
| B | Word-final `ן`, `ם` never carry nikkud | 98/98 + 15/15 | 100% |
| C | Yod (single or in `יי`) never carries nikkud | 157+52+52 bare | 100% |
| D | Cluster-first consonant → sheva (esp. `ר`/`ל`/`נ`) | 60 sheva / 88 vocalized cluster-first | ~95% |
| E | Consonant before `ע` → segol | 85/88 | 97% |
| F | Consonant before single `י` → hiriq (rare holam for /oy/) | 52/58 hiriq | 90% |
| G | Consonant before `יי` → patah (/ay/) or tsere (/ey/); lexical | 19+12 / 31 vocalized | 100% inventory, lexical choice |
| H | `ב` carrying a vowel → dagesh (בּ) | 24/24 | 100% on vocalized |
| I | `ש` in a vocalized word → shin-dot (שׁ) | 17/19 | 90% |
| J | Second `ו` of `וו` digraph: usually bare | mostly | **superseded** — see decision 13 |

For details and the analysis script see `analyze_rules.py`. Updates from the
5-page hand-corrected gold (pages 5-9) are recorded in decisions 12-14 below;
running totals come from `eval_vs_gold.py`.

## Decision log

### 1. We process line-level `<Unicode>` only, not region-level
PAGE-XML stores text twice: once per `<TextLine>`, and once concatenated
at `<TextRegion>` level. Transkribus regenerates the region-level copy via
a separate (often older) OCR pass, so it can disagree with the line-level
text — `ירוהם` appeared in the region-level copy where line-level had the
correct `ירוחם`. The line-level text is what the RA actually corrects;
the region-level copy is stale OCR. We always iterate line-level only.

### 2. We don't use layer-disagreement as an OCR signal
For the same reason as (1): the region-level text reflects a state
*before* the RA's corrections, so disagreements between layers only point
back at errors the RA already fixed. Useless for catching the residual
errors that are our actual target.

### 3. `י↔ו` removed from the confusable-letter set
און/אין, די/דו, אייך/אויך are all real Yiddish orthographic variants, not
OCR errors. Including `יו` in `CONFUSABLES` produced noise on every page.
Kept: `הח`, `נג`, `םס`, `בכ`, `רד` — all corpus-validated as real OCR
error classes.

### 4. Speaker spans are stripped of nikkud unconditionally
The RA's convention is bare speaker names (`יאכנע:`, not `יאַכנע:`). The
source occasionally has nikkud on a speaker; we strip it to match the
reference convention. `SPEAKER_RE` includes nikkud in the captured span
(`[א-ת][א-ת֑-ׇ]*`) so already-vocalized speakers are recognized.

### 5. Claude round-trip rejects consonant changes
The vocalization prompt is constrained: the model must add only nikkud,
not change letters. Any output whose stripped-nikkud form differs from
the input is rejected as a hallucination and written to a sidecar JSON
for human review. The exception we saw — `ר → ר'` (adding geresh to
mark *Reb* abbreviation) — is correct but classified as a consonant
change by our filter; we accept that conservative behavior.

### 6. Claude vs. Gemini play different roles
Two contradictory contracts can't share one prompt:
- *Vocalization* says: never change letters.
- *OCR detection* says: tell me which letters are wrong.
We keep them as two stages with two prompts. Claude vocalizes (one trusted
voice is enough), Gemini phonotactic-checks (with optional Claude Sonnet
parallel for high-recall calibration).

### 7. We picked Sonnet 4.6 over Opus 4.7 for the `--phono both` pairing
Per-page cost difference between Sonnet and Opus is ~5×, with comparable
recall on this task. At corpus scale that's real money; per-page it's
negligible. The case for Sonnet rests on being cheap enough that
multi-shot consensus stays affordable if we ever need it.

### 8. `--phono gemini` is the default, not `both`
Earlier we considered defaulting to `both` for higher recall on
human-review workflows. We backed off after discovering that:
- LLM phonotactic flagging is genuinely stochastic — different runs
  return different sets, even with identical inputs.
- The expensive obvious-OCR catches (`התן`, `זאנט`, `נאט`) get caught
  reliably by the **deterministic confusable-swap scanner** (`ocr_flags.
  confusable_swap_scan`) for free, before any LLM call.
- The LLM step now mostly surfaces borderline/contextual cases (e.g.
  `לאטישע מאַהלער → אַלטישע מיידעל`), where one model is fine and the
  marginal benefit of a second doesn't justify 2× cost on average.

### 9. The deterministic confusable-swap scanner runs first, free
For every token: try swapping each instance of a known confusable letter;
if the swapped form occurs more often on the same page, flag the
original. Catches the systematic OCR error classes (`ה↔ח`, `נ↔ג`) with
zero API cost. The LLM step picks up only what this misses.

### 10. We don't auto-apply Claude's suggested OCR fixes
Even when Claude proposes a clearly-correct consonant change (e.g.
`ר → ר'` to restore an abbreviation geresh), we write it to the suspects
JSON, not into the XML. Auto-applying letter changes across an unreviewed
corpus is a one-way mistake; the RA stays the gatekeeper.

### 11. Stage directions in `(…)` are not vocalized
Same skip treatment as speakers, by RA convention. Both round and square
brackets, with both ASCII and full-width variants, are handled
(`bracket_spans()` in `rules.py`).

### 12. Loshn-koydesh words *are* vocalized, via the dict (revised 2026-05-17)
Original take: skip loshn-koydesh; trust source nikkud. The 5-page gold
overturned this — the RA explicitly vocalized `חתן → חָתָן` (4×),
`דבורה → דְבוֹרֶה` (3×), `מלא → מָלֵא`, etc. With the dict now built
from all of `page_final/` (decision 14), these words are covered
automatically; no Hebrew lexicon shim needed.

### 13. The `וו` (double-vav) digraph rule (J) was wrong
Original Rule J: "second `ו` of `וו` digraph: usually bare." The 5-page
gold shows that when the digraph carries a following vowel, nikkud goes
on the **second** vav: `ווֶער` (7×), `ווִי` (6×), `וֶועט` / `ווֶעט` (4×
each). Bare only when no following vowel in the syllable. We did not
patch `rules.py` — picking the digraph vowel quality from position alone
isn't reliable, and all high-frequency digraph words are in the dict
now, so the rule engine doesn't need to handle them.

### 14. The dictionary is built from `page_final/`, NFC-normalized
Originally built from page 6 alone (~136 keys). Now built by merging
every XML in `data/{project}/page_final/` (~390 keys across pages 5-9),
NFC-normalized so that visual duplicates like precomposed `שׁ` vs
`ש`+combining-shin-dot collapse into one entry. Leave-one-out eval
(`eval_vs_gold.py`, 2026-05-17): rules+dict alone hits **78.0%** of
tokens-needing-vocalization across pages 5-9 — 97.7% on page 6 and
90.6% on page 9, 60-62% on the harder pages 7-8. The Claude stage
should be tuned to target the residual on 7/8, not re-do what dict
already covers.

**Caveat:** `page_final/` is also the default *output* directory of
`pipeline.py` (`final_dir` in pipeline.py). Re-running the pipeline on
pages 5-9 will overwrite the gold. Redirect `final_dir` or back up
`page_final/` before any re-run on those pages. Pages 10+ are safe.

### 15. The RA is internally inconsistent on ~17% of vocabulary keys
66 of 390 keys in `page_final/` have more than one vocalization. Notable:
`איהר` 14× `אִיהר` vs 10× `אִיהְר`; `וואס` in 4 variants; `אין` 15×
hiriq vs 2× tsere (real semantic split: "in" vs "one"); `יאכנע` 7× bare
vs 1× vocalized (rule A inconsistency). `most_common(1)` is the right
default, but it means held-out eval has a hard ceiling — some "misses"
are RA disagreement with themselves, not pipeline errors. Worth
surfacing the multi-variant list to the RA for canonicalization.

## Files

```
pipeline.py                 orchestrator (single-page, all stages)
rules.py                    rule definitions and helpers
vocalize_from_reference.py  stage 1: rules + dict
claude_vocalize.py          stage 2: LLM fill
phonotactic_check.py        stage 3: phonotactic OCR check
ocr_flags.py                deterministic OCR signals (confusable swap, intra-doc variants)
unclear_tags.py             stage 4: write Transkribus <unclear> annotations
make_eval_html.py           build an HTML evaluation view
analyze_rules.py            rule-mining utility (one-shot, not part of pipeline)
eval_vs_gold.py             leave-one-out eval of rules+dict against page_final/
```
