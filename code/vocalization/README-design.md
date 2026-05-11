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
| J | Second `ו` of `וו` digraph: usually bare | mostly | strong but not categorical |

For details and the analysis script see `analyze_rules.py`.

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

### 12. Loshn-koydesh words are *not* given a separate skip pass
The source already carries hand-vocalized nikkud on most loshn-koydesh
words; my Yiddish-native rules over-vocalize the rest (`חתן → חָתָן`).
We could add a stem list, but the RA confirmed source nikkud is the
authority — easier to let the source pass through than build a Hebrew
lexicon shim. Comparison with the RA reference showed this gap is
small in practice.

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
```
