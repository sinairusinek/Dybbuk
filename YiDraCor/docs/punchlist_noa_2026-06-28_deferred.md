# YiDraCor — deferred items after applying Noa's 2026-06-28 decisions (2026-07-02)

Noa's 06-28 handoff was applied and pushed to Transkribus (149 auto-edits / 74
pages). This is the punch-list of things that were **not** fully auto-applied —
things we may want to fix later.

## Needs code / schema work
1. **`אבנר, בנימין` joint speaker (Bas Sheva p.16).** Noa: "tag each one as a
   separate character." Both roles exist (`bnr`, `bnimin`). The resolver has no
   path to split one `label:` turn into two `who` refs. Left as a
   `speaker missing xmlid` / unknown flag. Needs a comma-split → `who="#a #b"`
   emitter (TEI joint-speaker convention already supported downstream by the
   structurer).

2. **`זעזעמיר):` (Das Yudishe Kind p.45).** Noa: "Stage → Delivering." This is
   a stray-`)` case (name + trailing paren, no opening paren), so it does NOT
   match the new standalone-paren-stage detector (that only fires on a fully
   parenthesized cue like `(שרייט):`). Needs a one-off manual stage/delivery tag
   on that line, or a detector for `<name>):` continuation artifacts.

3. **`רעפריין` "Musical Direction" (Di Seder Nakht, p.69).** Noa: tag as musical
   direction. For now added to `non_speaker_labels` so it stops being flagged as
   a speaker, but it is NOT yet tagged as a musical/stage direction. Proper
   handling belongs in the song-annotation pass (`annotate_songs.py`).

## Applied but worth a second look
4. **Pronoun / single-char overrides are whole-page, not line-scoped.**
   `speaker_overrides.json` entries added:
   - Das Yudishe Kind pp.20,26 — `ער`→vladislav, `זי`→henele
   - Isha Raa p.47 — `ה`→milkah
   - Di Seder Nakht p.32 — `דו`→dovid_kahn
   These fire for the WHOLE page (line-scoped rules aren't plumbed through
   `resolve_line` yet — see [[project-yidracor-speaker-overrides]]). If any of
   these pages host a second scene where the same pronoun means someone else,
   the mapping will be wrong. Spot-check these four pages.

5. **`kind_2` role (Di Seder Nakht p.65).** Coined per Noa's "(a) coin role" for
   `2) קינד`. The pre-existing speaker span on that line was mis-anchored
   (`offset:1;length:6` → captured `) קינד` incl. the stray paren); resolved by
   adding `) קינד` as a variant. Anchoring is cosmetically off (includes the
   `)`); fine for `who` resolution but re-anchor if it matters for display.

6. **`kinder_kor` collective (Hinke Pinke).** Coined as a normal cast role
   (not via the `KNOWN_COLLECTIVE` machinery). `קינד` (p.50 solo child) was
   added as a variant per Noa ("part of the collective") — this maps a singular
   child line to the chorus entity. Reasonable but review if a distinct solo
   child should exist.

## Non-annotation follow-up
7. **Dovid's Fidele p.71 is a promotional/ad page**, not play text. Noa flagged
   `בר ככנא`, `טהיילע)`, `בריינדיל קאזאק` as "promotional material,
   non-theatrical." Two of them added to `non_speaker_labels`; the whole page
   should probably be excluded from the body / marked back-matter. No exclusion
   mechanism applied yet.

8. **`סאלא אלט` (Di Seder Nakht p.56)** — Noa left the call blank (undecided).
   Not touched. Likely a voice rubric (solo alt); needs her decision.

9. **OCR corrections for Judith.** Many of Noa's `(c) OCR →` calls were routed
   to `cast_dict` `prefix_variants` (so the speaker resolves), per
   [[feedback-yidracor-ocr-vs-variants]]. The underlying page text is still
   OCR-wrong on those lines and could be queued for Judith to fix the transcript
   itself (e.g. `אנטינגוס`→`אנטיגנוס`, `שלומיר`→`שלומית`, `בנילין`→`בנימין`).

## Code changes shipped this round
- `auto_resolve_flags.py`: now consumes `cast_dict.non_speaker_labels`
  (previously inert — this also retroactively honors SoreSheyndel's suppressions)
  and detects standalone parenthesized cues (`(שרייט):`) as stage directions,
  typed via the span lexicon (`שרייט`→delivery).
