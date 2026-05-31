# RA-correction analysis (Transkribus, 2026-05-31)

This report compares, per page, the layer most recently pushed by our
annotation pipeline (`YiDraCor-annotation-pipeline` / `apply_pi_decisions` /
`apply_collective_speakers` / `auto_resolve_flags`, all by `sinai.rusinek@…`)
against the latest RA-edited transcript on top of it. Inputs were pulled live
via `code/transkribus/pull_layers.py`; diffs were produced by
`code/transkribus/diff_ra_layers.py`. Rule precision was evaluated by
`code/transkribus/eval_rules.py`.

All artefacts live under `data/review/`:
- `ra_corrections_summary_2026-05-31.tsv` — one row per play
- `ra_stage_changes_2026-05-31.tsv` — every stage diff (n=33)
- `ra_speaker_changes_2026-05-31.tsv` — every speaker diff (n=15)
- `ra_heading_changes_2026-05-31.tsv` — empty (no RA heading edits)
- `ra_stage_type_lexicon_2026-05-31.tsv` — token frequencies per new_type
- `ra_final_stage_corpus_2026-05-31.tsv` — every stage tag on RA-touched pages, for rule eval
- `ra_rule_precision_2026-05-31.tsv` — precision of each candidate rule
- per-play `data/<folder>/layer_diff_2026-05-31/` — raw `pipeline.xml` / `final.xml` pairs + `_manifest.json`

## 1. Coverage

| Play | Pages with an RA layer | RAs (count) |
| --- | --- | --- |
| MishkeMashke-Kultur1910 | 4 / 24 | judith ×3, sinai ×1 |
| DerManUnterTiff | 5 / 20 | noashur ×5 |
| Yudale_der_blinder | 17 / 70 | noashur ×12, judith ×3, sinai ×2 |
| Di_seyder_nakht | 12 / 72 | sinai ×5, noashur ×4, judith ×3 |
| Dos_yudishe_kind | 3 / 60 | sinai ×2, judith ×1 |
| AlNaharotBavel | 11 / 68 | judith ×10, sinai ×1 |
| KidushHashem | 7 / 80 | judith ×6, sinai ×1 |
| **Total** | **59 / 394** | judith / noashur / sinai |

Active RAs are `judithl1@mail.tau.ac.il` and `noashur@mail.tau.ac.il`; a handful
of pages were also hand-edited by Sinai through the web client.

## 2. Headline numbers

Across the 59 RA-edited pages (1,493 lines in common between layers):

| Category | Count |
| --- | --- |
| Lines with text changes (vocalization/OCR) | 165 |
| Stage tags added | 9 |
| Stage tags deleted | 6 |
| Stage tags retyped (`type_changed`) | 13 |
| Stage tags span re-anchored | 5 |
| Speaker tags added | 8 |
| Speaker tags deleted | 4 |
| Speaker tags re-anchored (`span_changed`) | 5 |
| Speaker xmlid re-assigned | 1 |
| Heading tags changed | 0 |
| Trailer tags added | 0 |

The text-change count is dominated by RA vocalization corrections (already
documented in the `feedback_ra_vocalization_corrections` memory note: the
pipeline systematically under-vocalises; OCR confusions ג↔נ / ך↔ד; dagesh is
the least reliable mark). Nothing new there — the focus of this report is the
structural-annotation diffs.

## 3. Stage-type corrections

### 3.1 Patterns

Every RA stage diff falls into one of seven lexically obvious buckets:

1. **`(אב)` → exit.** Three explicit retyings (`exit` from `business` or
   untyped); plus four more cases in the broader corpus where the pipeline
   labelled `(אב …)` as `business`. The single token `אב` (or vocalised
   `אָבּ`, `אבּ`) is the standard Yiddish theatre shorthand for "exit".
2. **`(אויפטריט / אויפטרעטען …)` → entrance.** Yudale p23
   `(אויפטרעטען ירוחם, פריידעלע, יאכנע…)` retyped `business → entrance`;
   DerManUnterTiff p10 `ניִווער אָבּ)` flipped from untyped to `exit` — and
   another pipeline-side `business` instance left untouched would also flip.
3. **`(קומט אריין)` / `(פאלט אריין)` → entrance.** Di_seyder p18
   `(קומט אריין)` retyped to entrance; Yudale p9 `(פישעל פאלט אריין)` retyped
   to entrance.
4. **פערוואַנדלונג / פאָרהאַנג / "א צימער ביי X" → setting.** Yudale pp.5,
   16, 22 — three independent `business → setting` retypings on the canonical
   scene-cue phrases, plus two added `setting` tags on bare `פערוואַנדלונג.`
   lines that the pipeline had not tagged at all.
5. **(emotion-adverb) → delivery.** Six retypings cover `(בּייז)`, `(שפעטיש)`,
   `(שרייט)`, `(ברוגז)`. All were typed `business` by the pipeline; RA chose
   `delivery`. The broader corpus shows 16 more such single-word
   emotion/manner directions still typed `business` (12× `(אפארט)`, 3×
   `(בעגייסטערט)`, 1× `(קוקט … פערקלעהרט)`) — a substantial backlog the rule
   would catch.
6. **`(ענדע פון … אקט)` → trailer.** Yudale p25 retyped from
   `stage{type:business}` → `trailer`. There are two more such instances in
   the corpus still mistyped `business`. Schema and `auto_resolve_flags`
   already know `ענדע …` → trailer, but the lexicon only fires for *untyped*
   stage spans; when the LLM has already supplied `business`, the lexicon
   defers and the RA has to retype manually. This is the single most-fixable
   class.
7. **Song lyrics deleted from stage.** Yudale p25 lines `r…1221l11`,
   `…l10`, `line_1648547840812_1271` — three consecutive stage tags removed
   by the RA. The substrings are clearly sung verse content (`ליעבּער גָאט
   זעֶה אָ זעֶה / עֶס זאָל נישט זיין קיין חלום`). Our pipeline tagged them
   `stage{type:business}`; they should be `l`/`lg` inside a song group.

### 3.2 Proposed rules (precision evaluated on the 20-tag *RA-touched* corpus,
plus impact estimated on the 224-tag pipeline-side corpus over the same pages)

The first table is the *decisive* test: when the RA touched a stage tag whose
substring matches the rule predicate, did they choose the rule's predicted
type? The second column ("would-correct") is the count of pipeline-side stage
tags whose current `type` (mostly `business`/`mixed`/untyped) the rule would
*flip* — the deployment impact.

| Rule | Predicted type | Precision (RA-touched) | Would-correct (pipeline-side `business`/`mixed`/untyped) |
| --- | --- | --- | --- |
| `\bענדע\b.*\bאקט\b`                                  | trailer  | 1/1 (100%) | 2 business |
| `^\s*\(?\s*ענדע`                                     | trailer  | 1/1 (100%) | (subset of above) |
| `פערוואנדלונג` ∨ `פאָרהאַנג` ∨ `פארהאנג`            | setting  | 2/2 (100%) | 1 business + 1 mixed + 1 untyped |
| token `צימער` in stripped-paren content              | setting  | 2/2 (100%) | 3 business |
| token `אב` in stripped-paren content                 | exit     | 3/3 (100%) | 2 business + 1 untyped |
| `אויפטריט` / `אויפטרעטען` / `אויפטרעטן` substring   | entrance | 1/2 (50%, see note) | 1 business + 2 mixed |
| `קומ[טען]…אריין`                                    | entrance | 1/1 (100%) | 1 business |
| `\bפאלט\b.*\bאריין\b`                               | entrance | 1/1 (100%) | 1 business |
| Token in {בייז, שרייט, ברוגז, שפעטיש, זיגנענד, אפארט, בעגייסטערט, פערקלעהרט/פערקלערט, פערשעמט, לאכענד, ערנסט, שטיל} | delivery | 4/4 (100%) | **16 business** |

**Notes / caveats**

- The `אויפטריט` rule's lone "wrong" case is DerManUnterTiff p11
  `אונטער'ן טיש. אויפטריט טורניווער אוּן זעצט זיך צוּם טיש, יאָכֿטשע` — a
  three-event compound direction. The RA retyped it `mixed → business` rather
  than `entrance`. Guarding the rule with "no other action verb in the same
  span" (e.g. no `זעצט`, `גיט`, `נעמט`, …) preserves ≥90% precision while
  keeping the typical "(אויפטרעטען X, Y, Z)" cast-call.
- All rule predicates run on the *nikud-stripped* substring of the stage span,
  with `( ) [ ] . , : ; ׃ ־` stripped from the boundary (mirroring
  `auto_resolve_flags.stage_lexicon`).
- All rules are **safe to apply over an existing `business` type**, not just
  untyped spans — that's the lever that unlocks the 16 single-word-emotion
  retyings. This is the single biggest change.

## 4. Speaker corrections

### 4.1 Multi-word speakers truncated to the first token

`ben_kaspi` in *Al Naharot Bavel* was tagged five times by the pipeline as
`speaker{offset:0; length:4; xmlid:ben_kaspi}` — i.e. covering only `בֶּן`.
Every time the RA deleted that span and replaced it with one covering the full
`בֶּן כַּסְפִּי`. The cast_dict has the correct `bare: "בן כספי"` and
`build_name_matcher` (in `auto_annotate.py`) already builds a regex spanning
both words. So this came from the LLM annotator path (`annotate_pages.py`)
producing the short tag without consulting the matcher. **The
`auto_resolve_flags` `resolve_line` step should re-anchor any speaker span
whose covered text is a strict prefix of a multi-word `bare`/`form` in
cast_dict.**

### 4.2 Collective speaker `ביידע` not auto-added

Three lines in Yudale (p14 l214, p15 r1l4, p15 l336) start with `ביידע:` and
the RA added a missing `speaker{xmlid:beyde}`. `ביידע` is already in
`schema.KNOWN_COLLECTIVE`, so the collective machinery knows about it — but
the auto-adder (`apply_collective_speakers.py`) only fires when the label is
*not* a known cast member AND something else also looks like a speaker line.
**Lower the bar: any line of form `<known-collective>:` at the start should
unconditionally get a `speaker` tag, with `xmlid` = sanitized collective
label.**

### 4.3 Speaker spans shifted by vocalization edits

Five `speaker.span_changed` events (yakhne in Yudale ×2, tobyas in KidushHashem
×2, etc.) reflect the RA *removing* a stray nikud inside the speaker label —
e.g. `יאַכנע → יאכנע`, `טאבִּיאס → טאביאס`. The xmlid is unchanged. These
will auto-resolve as soon as we fix the vocalization-noise issue noted in
`feedback_ra_vocalization_corrections`; no separate action.

### 4.4 cast_dict additions surfaced

No new roles. The only "added" speakers were collective (`ביידע`) and the
re-anchoring of `ben_kaspi` (above). `noashur` flagged `יהודית'ל` once in Al
Naharot — that's a diminutive of `יהודית` already in cast; the RA correctly
deleted our wrong tag (`ben_kaspi` somehow assigned to it via offset-overlap
chaos on a heavily-edited page) rather than adding a new role.

## 5. Other

- **No heading edits** at all. `heading{type:act|scene|epilog}` is being
  produced correctly across the seven plays.
- **No `trailer` edits beyond the one retyping of `(ענדע פון ערשטען אקט)`** —
  consistent with #4 above (we know about the cue word but defer to LLM-set
  types).
- **Three song-lyric lines were deleted** from stage in Yudale p25 (above).
  No corresponding `l`/`lg` was added in the RA layer — they just deleted our
  wrong tag and presumably plan a separate pass to song-tag them. We should
  add a pre-LLM guard: if a `TextRegion` is already song-classified by the
  cast/song detector, don't let the LLM emit `stage` inside it. This is one
  observation; would want to confirm against more pages before coding.

## 6. Recommended pipeline changes (prioritised)

In priority order — each item lists the concrete code touch-point.

### P0 — Lexicon overrides win over LLM-assigned stage type

File: `code/annotation/auto_resolve_flags.py`, function `resolve_line`,
around the `if tag == "stage":` block.

Today: if `a.get("type") in STAGE_TYPES`, we keep the LLM's type unchanged and
the lexicon never gets to fire. Change: always consult `stage_lexicon(text)`
for the current line, and also a new `stage_lexicon_span(substring)` for the
span content; if it returns a type, override the LLM's choice. Specifically:

- `^\s*\(?\s*ענדע` → drop the stage tag and add a `trailer` span instead
  (the existing code already does this when the LLM omitted a type; extend it
  to *also* fire when the LLM gave `business`).
- substring contains `פערוואנדלונג` / `פארהאנג` / `פאָרהאַנג` → set
  `type:setting` regardless of prior LLM choice.
- substring tokenises to `אב` (single-token or "אב מיט X") → `type:exit`.
- substring contains `קומט אריין`, `קומען אריין`, `פאלט אריין`,
  `אויפטרעטען` (with no further action verb in the same span) → `type:entrance`.
- substring is a single emotion/manner adverb in the closed list above →
  `type:delivery`.

Estimated impact on the 7-play sample: 25+ pipeline-side stage tags would be
auto-retyped to the value the RAs are picking by hand. Higher across the full
26-play corpus.

### P1 — Speaker span extension to multi-word cast forms

File: `code/annotation/auto_resolve_flags.py` (new check in `resolve_line`);
or alternatively `code/annotation/annotate_pages.py` post-LLM normaliser.

For every `speaker{offset, length, xmlid}` tag: look up `xmlid` in
`cast_dict.roles`. If the `bare` form contains a space and the currently
tagged substring is a strict prefix (modulo nikud) of that bare form, extend
`length` to cover the full multi-word match starting at `offset`. Fixes the 5
ben_kaspi tags (and similar future cases like any "בֶּן …", "ר׳ …",
"דאָקטאָר …", "פראָפעסאָר …" composite names).

### P2 — Collective speakers fire unconditionally on `<label>:` prefix

File: `code/annotation/apply_collective_speakers.py`.

For any line whose stripped-nikud prefix matches `<known_collective>:`, add a
`speaker{xmlid:<sanitised collective>}` covering offset 0…label-length, *with*
trailing colon, regardless of context. Today the script is more cautious.
Fixes the three Yudale `ביידע:` lines and will pre-empt the same correction
across the wider corpus.

### P3 — `stage` span containing `(ענדע …` is a trailer, not a stage

Already partly implemented in P0; calling it out separately because the schema
side (`schema.STAGE_TYPES` does not include `trailer`, and `trailer` is a
top-level tag in `ALLOWED_TAGS`) means the change is *delete the stage span
and emit a trailer span at the same offsets*. Make sure the rewrite preserves
`offset`/`length`. No schema change.

### P4 — `prompts.py` lexicon entries

File: `code/annotation/prompts.py` (LLM system/user prompts).

Even with the post-processor rules above, it is cheaper and more legible to
also teach the LLM the same shortlist. Add a "stage @type lexicon" appendix
to the prompt enumerating the same triggers and target types listed in 3.2.
Worth doing once the post-processor is in (so we measure their independent
contributions).

### P5 — Song-lyric guard

File: `code/annotation/annotate_pages.py`.

If the cast/song-detection step has classified a region as song-bearing (or
the line is already inside an `lg`), don't accept LLM `stage` annotations on
its lines. Lower priority — only 3 cases in the sample — but easy when we get
to it.

### Non-changes

- **Schema**: no changes required. `STAGE_TYPES` already covers the predicted
  set; `trailer` and `heading{type:epilog}` are already valid.
- **Cast_dict**: no new roles surfaced. The `ben_kaspi` `prefix_variants`
  field should *not* be populated with `בֶּן` alone — that would mis-resolve
  any "בֶּן …" speaker (Ben Asher, Ben Yochanan, etc.). The span-extension
  rule (P1) is the right fix.

## Surprises / things to flag

1. **All seven plays produced zero RA heading edits.** Our act/scene/epilog
   detector is the single best-behaved part of the pipeline on the
   RA-reviewed pages. Worth not breaking when we tweak prompts.
2. **The biggest single backlog is delivery vs. business for single-word
   manner adverbs.** 12 of the 16 mis-typed `business` directions matching
   the delivery rule are the single token `(אפארט)` in KidushHashem. One
   ~50-token dictionary commit would resolve that play class-wide.
3. **`ben_kaspi` speaker truncation is upstream of `auto_resolve_flags`** —
   the matcher already builds the correct regex. The bug is that the LLM
   path bypasses the matcher and only the resolver sees the result. Either
   teach the resolver to re-anchor (P1) or stop the LLM from emitting
   speaker spans at all when the matcher already has a hit.
4. **One RA edit on Yudale p25 reveals our pipeline tags entire song
   stanzas as `stage{business}`.** This is rare in the sample but a known
   class of error worth a guard once we get to it.
