# YiDraCor — Status for Noa, 2026-06-14

Combined status of (a) the Blimele Q1–Q4 from the 2026-06-04 handoff,
(b) the pipeline-rule questions you sent in chat on 2026-06-14.

**Status legend**
- `[resolved]` — decision applied to the data; no action needed from you.
- `[confirmed in data]` — your edits in the 06-14 Transkribus pull already
  apply the rule on Ezra; the pipeline now matches your behavior.
- `[Sinai confirmed]` — Sinai locked the rule into the pipeline on 2026-06-14.
- `[open]` — decision needed before next pipeline run.

---

# Part 1 — Open questions (5)

Please answer these before the next pipeline run on the remaining plays.

## A4. p.64 `דער איינער` ("the one") — ensemble or new role?

Single occurrence on Blimele p.64. Two options:
- (a) add as `prefix_variant` of collective `eyner`
- (b) coin a new body-only xmlid for this specific solo speaker

## B6. Post-act-header line → `stage{type:setting}`?

Should the line immediately following an act header (`ערשטער אקט`, …) or
scene header — when that line is NOT a speaker turn and NOT parenthesized
— be encoded as a whole-line `stage{type:setting}` describing the scene
location?

**Concrete cases in Ezra+Blimele:**
- **Ezra p4** — `ערשטער אקט.` followed by `איינע וואלדגעגענד — רעכטס אַ הייזעל מיט א פענסטער…` (the only unparenthesized post-act-header setting line in either play; all other Ezra act openers — II/III/IV — are followed by a parenthesized stage direction which the existing rule already catches).
- **Blimele p7** — `I אקט` followed by `(שטעלט פאר בייא ליעפען א סאלאן…)` — already parenthesized, already typed setting by existing rule; B6 doesn't change this.

**Tagging volume if yes:** 1 line in Ezra+Blimele; meaningful only if the
same pattern recurs in other plays.

## B7. `(ביס)` triggers song mode + same-page backfill?

Every line containing a standalone `(ביס)` marker treated as in-song, AND
prior eligible lines on the same page (back to the last heading or
non-chorus speaker change) tagged as song (`l` / `lg_id`) under the same
musical number?

**(ביס) occurrences in Ezra+Blimele:**
- **Ezra:** p35 (1 line). Total 1.
- **Blimele:** p10 (4), p14 (1), p15 (2), p16 (1), p37 (3), p38 (1), p39 (4), p56 (2), p60 (1), p61 (3), p62 (1), p65 (2). Total 25.

If you say yes, these 26 ביס-bearing lines plus their same-page
predecessors will be re-typed as song lines.

## B8. `(ביס)` cross-page backfill?

Should the `(ביס)` rule also reach BACKWARD across page breaks when the
preceding page ends with lyric-like content?

**Concrete suspect cross-page bleeds in Blimele:** the ביס clusters span
consecutive pages (p14→p15→p16, p37→p38→p39, p60→p61→p62) — songs likely
start a page or two before the first (ביס) marker, so cross-page backfill
would re-type the lyrics on the leading pages too.

## B9. Mixed rule scope — only for entrance/exit + action?

Should `mixed` be used ONLY for entrance/exit cues combined with other
action — i.e. directions that combine non-movement functions
(`set + emotion`, `business + delivery`, etc.) should continue to pick
the dominant function rather than be retyped as `mixed`?

**Concrete cases to ground the decision** (a few stage directions in
Ezra+Blimele that combine functions without an entrance/exit cue):
- Ezra p4 — `(לעגט וועג דיא האַרפֿע— ערשיינט)`: HAS entrance cue, already
  resolved (B2 below) as `mixed`. Not relevant to B9.
- General class — directions like `(שטיל, פערקלעהרט)` (emotion adverbs
  stacked) or `(זינגט, טאנצט)` (delivery + business). Currently the
  pipeline picks the first function. Should they become `mixed` instead?

*(I haven't enumerated specific page refs for this — happy to if you want.)*

---

# Part 2 — Resolved items (reference)

## A. Blimele speaker questions (originally from 2026-06-04 handoff)

### A1. `[resolved]` `בערל` (17×, pp.7–9)

**Decision:** colloquial short form of Berele. Added as `prefix_variants` of
`berele` in cast_dict. All 17 turns now resolve to `xmlid:berele`.

### A2. `[resolved]` `ער` / `זיא` as speaker labels (pp.13, 14, 61)

**Decision:** these are gendered sung-duet labels for two characters dressed
in disguise. Per-scene mapping:
- pp.13–14 (Zelikel/Tsierele duet): `ער` → `zelikel_mnagen`, `זיא` → `tsierele`
- p.61 lines 4–6 (Zelikel/Tsierele as doves): same
- p.61 lines 21–28 (Daniel/Blimele as gypsies): `ער` → `doktor_daniel`, `זיא` → `blimele`

Stored in `data/Blimele-AhronFaust1903/speaker_overrides.json`. Schema
extended to support per-page-line-range scoping so future plays can
declare the same pattern. New infrastructure also added to
`auto_resolve_flags` to consult overrides before cast_dict lookup.

### A3. `[resolved]` Joint / duet speaker labels (6 labels)

**Decision:** single `speaker` span carrying space-separated xmlids
(downstream structurer expands to TEI `<sp who="#a #b">`). All 6 lines
tagged:
- p.13 `דועט ביידע` → `zelikel_mnagen tsierele`
- p.16 `דאניאל בליהמעלע דועט` → `doktor_daniel blimele`
- p.39 `ליעפע דאניאל זעליקל` → `liepe doktor_daniel zelikel_mnagen`
- p.39 `ליעפע זעליק` → `liepe zelikel_mnagen`
- p.39 `מאקסים גראף` → `maksim graf_stanislav`
- p.39 `דאניאל ליעפע זעליקל` → `doktor_daniel liepe zelikel_mnagen`

### A4. Ensemble members speaking solo — 2 of 3 resolved

- `[resolved]` p.61 `טויבען` → joint `zelikel_mnagen tsierele` (per stage
  direction, the doves ARE Zelikel + Tsierele in disguise).
- `[resolved]` p.61 `ציגיינער` → joint `doktor_daniel blimele` (gypsies are
  Daniel + Blimele in disguise).
- *(p.64 `דער איינער` deferred — see Part 1 above.)*

## B. Pipeline-rule questions (from 2026-06-14 chat)

### Stage-direction typing

1. `[confirmed in data]` Every occurrence of `ערשיינט` in a stage direction
   whose only verb is `ערשיינט` → `stage{type:entrance}`. (Ezra p5: your edit
   retyped `(ערשיינט)` business→entrance.)

2. `[confirmed in data]` Every `ערשיינט` co-occurring with another action verb
   in the same direction (e.g. `(לעגט וועג דיא האַרפֿע— ערשיינט)`) →
   `stage{type:mixed}`. (Ezra p4.)

3. `[confirmed in data]` Every `אב` co-occurring with another action word in
   the same direction (e.g. `אב, שטורם`) → `stage{type:mixed}`. (Ezra p9.)

4. `[Sinai confirmed]` Bare exit (`(אב)` / `(<actor> אב)`) stays `exit`;
   modal-guarded "intent to leave" (`(<actor> וויל אב)`) stays `business`.
   The new mixed rule does NOT apply to these.

### Setting detection

5. `[Sinai confirmed]` Every standalone `פערווענלונג` / `פערוואנדלונג` /
   `פערוואנדעלונג` direction → `stage{type:setting}` (with or without
   parens/nikud).

---

## Summary

- **Open (5)**: A4 (`דער איינער`), B6, B7, B8, B9
- **Resolved**: A1, A2, A3, A4 (partial — 2 of 3)
- **Confirmed by your past edits**: B1, B2, B3
- **Sinai confirmed 2026-06-14**: B4, B5
