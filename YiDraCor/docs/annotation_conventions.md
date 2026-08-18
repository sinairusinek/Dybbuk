# YiDraCor — Annotation Conventions

**Single source of truth. Last updated 2026-07-20.**

Supersedes and absorbs `castlist_tagging_conventions_2026-06-18.md`. Where an
older handoff contradicts this file, **this file wins** — superseded rules are
recorded in §9 so the history stays legible.

Scope: the 15 **printed** Lateiner plays. Manuscript plays are a separate track
(§10).

---

## 1. Span mechanics

Annotation lives in the Transkribus `custom` attribute on each `TextLine`:
`tag {k:v; k:v;}`. Parsed by `schema.parse_custom` / `serialize_custom`.

- `offset`/`length` are **character (code-point) indices** into the line's
  `Unicode` text.
- Spans **never cross line boundaries**. A direction spanning three lines gets
  three spans.
- Different tags may overlap on a line; **same-tag spans must not**.
- **Dedup:** for each `(tag, offset, length)` triple, the **last** occurrence
  wins. Transkribus *layers* spans on re-push rather than replacing them, so
  without this, re-pushes accumulate duplicates. `schema.dedup_entries`.

### The tagset (`schema.ALLOWED_TAGS` — authoritative)

| tag | attributes | notes |
|---|---|---|
| `speaker` | `xmlid` **required** | name only; space-separated ids for joint turns |
| `stage` | `type` **required**, `continued`, `xmlid` | see §3; `xmlid` → TEI `<stage who="#id">` (att.ascribed) |
| `trailer` | `type` optional | `ענדע דער X אקט` — **not** a stage direction |
| `heading` | `type`, `n`, `subtype` | `type` ∈ {act, scene, epilog} |
| `role` | `xmlid` **required** | castList pages only |
| `roleDesc` | — | castList pages only |
| `actor` | — | TEI `<actor>`: performer's name beside a role |
| `l` | `lg_id`, `continued` | verse line |
| `head` | `lg_id`, `unit-type` | song/section heading |
| `lg` | `n`, `cont`, `continued`, `type` | one per stanza — **never per line** |
| `fw` | `type` **required** | forme work; page numbers are `type:pageNum` |
| `metamark` | `function` **required**, `role` **required**, `n`, `spanto`, `corresp` | Regie/prompter cue notation, MS track only — see §11 |

**Editorial tags** passed through to TEI: `unclear`, `sic`, `corr`, `orig`,
`reg`, `abbr`, `expan`, `supplied`, `add`, `del`, `gap`, `note`, `foreign`,
`hi`.

**Ignored** (Transkribus-native, not ours): `readingOrder`, `textStyle`,
`Header`, `Footer`, `structure`.

Page types: `titlePage` (may span 1–3 pages; do **not** tag titles/authors/
publishers), `castList`, `body`.

---

## 2. Speakers

**S1.** The speaker span covers the **name only** — never the trailing colon,
comma or whitespace.

**S2.** `xmlid` is required and must point at a castList `role` xml:id.

**S3.** OCR variants still get the **canonical** xmlid (`אכטשעי` →
`xmlid:yokhtshe`). The page text stays as printed; the transcript fix is
queued separately for Judith — see §7.

**S4. Joint / duet turns = ONE span with space-separated xmlids** → TEI
`<sp who="#a #b">`. *Noa 2026-06-14.*
Blimele: `דועט ביידע`→`zelikel_mnagen tsierele`; `דאניאל בליהמעלע דועט`→
`doktor_daniel blimele`; `ליעפע דאניאל זעליקל`→`liepe doktor_daniel
zelikel_mnagen`; `ליעפע זעליק`→`liepe zelikel_mnagen`; `מאקסים גראף`→
`maksim graf_stanislav`; `דאניאל ליעפע זעליקל`→`doktor_daniel liepe
zelikel_mnagen`.

**S5. Disguise / gendered duet pronouns resolve per scene** via
`data/<play>/speaker_overrides.json`. Blimele pp.13–14 and p.61 ll.4–6:
`ער`→`zelikel_mnagen`, `זיא`→`tsierele`; p.61 ll.21–28 `ער`→`doktor_daniel`,
`זיא`→`blimele`. p.61 `טויבען`→joint `zelikel_mnagen tsierele`;
`ציגיינער`→joint `doktor_daniel blimele`.
⚠️ *Known gap:* overrides are applied **page-scoped**, not line-scoped —
`resolve_line` never plumbed the line ranges through.

**S6.** `דער איינער` (Blimele p.64) → a **new body-only xmlid**, not a variant
of the collective `eyner`.

**S7. Known collectives get no individual cast entry** and must never be
flagged "missing cast member". Matched on the nikud-stripped consonant
skeleton. List: `אלע`, `שטימען`, `ביידע`, `מענער`, `מעדכען`, `קאהר`/`כאר`/
`קאר`, `דועט`, `איינער`, `דאמען`, `קינדער`, plus voice rubrics `סאפראן`,
`אלט`, `באס`, `טענאר`.

**S8. Do NOT pre-seed chorus collectives.** Explicitly **No** for Al Naharot
Bavel, Di Seder Nakht and Kidush Hashem. Tag chorus lines only where they
actually occur in the body.

**S9. Per-play name routing** *(Noa 2026-06-14)*:
- **Al Naharot Bavel** — `זמרי` is one persistent role even post-renegade;
  `דער קעניג`/`קעניג בלשצר`/bare `בלשצר` → `belshatsar`; `דלילה` single role
  (`favoritin` is a roleDesc); bare `בן כספי` canonical.
- **Der Mann untern Tisch** — `יוסף` and `אסיפ` → one id `yosef_osip`. Full
  `ליובאוו אבאולאוונא` is the role string; bare first name routes to it.
- **Di Seder Nakht** — bare `ריזוואן` → the **last-spoken brother**, not
  always Karl.
- **Kidush Hashem** — `שבת גוי` → `andre` **only as a speaker label**;
  descriptive vocatives inside dialogue are not routed. `איזראעל`/
  `דאן איזראעל` → `dan_yisroel`.
- **Mishke Mashke** — `מאשקע`/`מישקע` distinct ids, **tight matching, no
  fuzzy match**; `שאַרלאָטאַ'ס` → prefix_variant of `sharlata`; `מאשקע` is male.
- **Dos Yudishe Kind** — `דער קעניג`→`kenig_zigmund`;
  `דער מייסטער`→`kerker_meyster`.

**S10.** `non_speaker_labels` suppresses labels that look like speakers but
aren't (consumed by `auto_resolve_flags` since 2026-07-02).

---

## 3. Stage directions

**ST1.** `type` is **required** on every `stage` span. No bare stage.

**ST2. Vocabulary:** `setting`, `entrance`, `exit`, `business`, `delivery`,
`location`, `costume`, `novelistic`, `modifier`, plus the fallback
`mixed`.

**ST3. Multi-token `@type` for compound directions** *(Sinai + Noa 2026-06-18,
"option C")*. Space-separated tokens; **`mixed`, if used, must be the only
value**. `(לעגט וועג דיא האַרפֿע— ערשיינט)` → `type="entrance business"`.
Other shipped retypes: Der Mann p.11 `business entrance`; p.18 dance/song
lines `business delivery`; `פאָרהאַנג פאַלט.` → `setting`.

**ST3b. Exit + entry of *different* persons → `mixed`** *(Noa via Sinai
2026-08-02)*. When one direction has X leaving and Y arriving —
`(יאכטשע אב, אויפטריט סאבעלע)`, `(אַלע, אב, עזרא … ערשיינט)` — the type is
`mixed`, not `exit entrance`. Multi-token stays for compounds of ONE
person's movement (`exit business`, `entrance business`).

**ST4.** `ערשיינט` (and infinitive `ערשיינען`) alone → `entrance`.

**ST5.** Bare `(אב)` / `(<actor> אב)` → `exit`. Modal intent
`(<actor> וויל אב)` → `business`.

**ST6. Setting cues.** `פערווענלונג` / `פערוואנדלונג` / `פערוואנדעלונג` →
`setting`, with or without parens or nikud. `פאָרהאַנג` / `פאָרהאַנג פאַלט`
(curtain) → `setting`, **not** `business`.

**ST7. Post-header setting line.** A line immediately after an act/scene
header that is neither a speaker turn nor parenthesized → whole-line
`stage{type:setting}`.

**ST7b. The parenthesized act-opening direction → `setting`** *(Sinai
2026-07-20)*. ST7 covers the *unparenthesized* line after a header; this is its
parenthesized counterpart — the opening tableau, `(גאַרטען, לינקס צדוק'ס
הויז…)`. `stage_lexicon` is purely lexical and only fires on a cue word (ST6),
and an opening tableau has no cue, so the corpus had split by accident of
wording: 22 openers `setting`, 15 `business`. Position identifies these, not
vocabulary.
- The **whole parenthesis is one `setting`**, even where it describes people in
  motion — the established reading, not a new call: Al Naharot p.51
  `(דער קעניג זיצט אויף דעם טראהן…)` and p.38 `(… מעדכען זינגענדיג)` are both
  plain `setting`. No compound `setting business` here.
- Carries across continuation lines while the parenthesis stays open (ST9).
- Only retypes `business` or untyped. **`entrance` and `delivery` openers are
  left for a human** — Kidush Hashem p.47 `(קאהר ווי אנפאנג פונ'ם צווייטען אקט)`
  sits in opening position but is a musical instruction.
- `annotation.auto_resolve_flags --sweep-openings`.
- *(Noa via Sinai 2026-08-02)* the trigger is not only an act/scene header:
  the same rule fires **directly after a `פערוואנדלונג`** (mid-act
  transformation) — the description lines that follow are `setting` even
  when they enumerate the people on stage (Yudale p.38). `location` spans
  in that position retype too. Excluded: song-supplement context notes and
  music cues (`Ritt.` / `Nr.`), which are not scene descriptions.

**ST8.** `delivery` **requires parentheses** in the source. Never assign it to
an unparenthesized line.

**ST9.** Multi-line directions: tag each line's portion separately, including a
continuation line containing only `)`.

**ST10.** Unbalanced OCR parens: still tag the visible parenthesized text.

**ST11.** Unparenthesized third-person narration with no preceding speaker →
`stage`.

**ST12.** `trailer` is **not** a stage direction. `ענדע דער X אקט`,
`ענדע פונ'ם X אקט`, `ענדע דער פיעססע`, bare `ענדע` → whole line `trailer`.

**ST13a. `stage{type:trailer}` is wrong** — `trailer` is a TAG, not a stage
`@type`. `(ענדע פון ערשטען אקט)` is `<trailer>`, per ST12.

**ST13. Emotion adverbs → `delivery`** (`וויינט`, `לאכט`, `וויינענד`, …). The
rule-based lexicon in `auto_resolve_flags` **overrides** the LLM annotator.

---

## 4. castList and roles

**Global A — setting lines at the bottom of a castList.** Lines beginning
`אָרט דער האַנדלונג…` or `דיא געשיכטע האנדעלט זיך…` are **not** roles → whole
line `stage{type:setting}`.

**Continuation lines carry the rule.** The statement usually runs over two
lines and the second carries no cue of its own (`נאך חרבן בית ראשון.`). Every
following line gets its own whole-line `stage{type:setting}` — spans never
cross line boundaries (ST9) — until a line bearing a `role` span.

⚠️ **Tagging these `roleDesc` is a silent error.** In the TEI a `roleDesc`
with no `role` attaches to the preceding castItem, so the locus of the action
is read as part of the last character's description. It was invisible to every
check until 2026-07-20: lint flagged only castList lines with *no* role/roleDesc
span, so a MIS-tagged line counted as tagged. Now `auto_resolve_flags`
converts them (page-level `apply_global_a`) and lint reports
`mis-tagged setting line`. Global-C brace labels (`זיינע קינדער`) are a
legitimate roleDesc-without-role and never match a Global-A prefix, so they
are untouched.

**Global B — library shelfmarks stay completely untagged** (`II 43.144`,
`ע63.390`, BN catalogue ids). No stage, no castItem.

**Global C — brace-group siblings → N separate `role` castItems sharing ONE
`roleDesc`.** Don't fuse; don't drop the shared description.

**Global D — bare functional roles keep their indefinite article.**
`א וועכטיר`, `אַ דִיענֶער`, `אַ גַייסט` — **never strip the `אַ`/`א`**.

**Global E — final-line collective enumerations → one collective xml:id per
comma-separated token.** No catch-all id. Literary "etc." markers (`אאז"ו`,
`עטצ.`, `אַ. זַ. וו.`) remain in the surface text but are **never spanned** as
characters.

**Global F — profession/relation modifiers → `roleDesc` always; fused titles
stay in `role`.**
- → roleDesc: `זיין נעפפע`, `פּראָצענטניק`, `(אויגען דאָקטער)`, `זיין ווייב`,
  `איהר פעטער`.
- stay in role: `פּראפעסאר עדעלמאן`, `דאן איזראעל`, `קעניג בלשצר`,
  `ר' יאָכטשעֶ`, `פֿעטער משה`.
- **Test:** if dropping the modifier wouldn't change *who* the speaker is, it
  belongs in roleDesc.

**Global G — particDesc / castList / voice encoding** *(DraCor alignment,
2026-07-02; verify with `python3.11 -m structure.check_who tei/<Play>.xml`)*:
1. `particDesc/listPerson` is the machine-readable master; `castList` is
   documentary. Every role gets a listPerson entry so every `@who` resolves.
   castList is never padded with voice parts or non-printed collectives.
2. `"collective": true` → `<personGrp><name>`, not `<person><persName>`.
3. Joint turns → space-separated `@who`, each id validated and `#`-prefixed.
4. Song-supplement voice rubrics are **speaker attributions** — see §5.

**Role mechanics.** `role`/`roleDesc`/`actor` appear on castList pages only.
`role` requires `xmlid` (lowercase ASCII translit, stable). `roleDesc` covers
the **entire** description, starting after the name+separator, excluding
trailing whitespace and a final `.`.

**Per-play locks.** *Dos Yudishe Kind:* keep `⸗` verbatim (`הויפ⸗נאר`); leave
`שׁמֶערְל`/`שׁפרִינְצֶע` roleDesc empty — do not infer. *Der Mann:* the
strikethrough role is **kept** (editorial cancellation). *Kidush Hashem:*
`אֵינקוויטאָר` is roleDesc, not part of the name. *Di Seder:* `קאטינקא` desc
empty for now.

**Junk labels are never cast roles.** `עטצ.` ("etc.") and `רעפריין` were both
wrongly minted as roles (`etts`, `refrn`) and removed 2026-07-19. Watch for
this class.

**Hygiene.** `bare`/`form` must carry **no trailing punctuation or
whitespace**. `build_name_matcher` appends `[:׃]` to the bare form, so a bare
of `חינקע:` compiles to a pattern requiring two colons and can never match.
Normalized corpus-wide 2026-07-19.

---

## 5. Songs and musical directions

**M1.** `lg` = one group marker per stanza/song. **Per-line `lg` spans are
wrong** — lines are `l {lg_id}`.

**M2.** `lg.cont` is optional and only ever `yes` (stanza continues from the
previous page). Legacy equivalent: `continued:true; type:cont`.

**M3. Song openers:** `זינגט` (inside a stage span) and header-style
`Nr. N` / `געזאנגס-טעקסט`. *(B1–B3, confirmed by Noa's 06-07 edits.)*
⚠️ `(ביס)` is **not** an opener — see §8.

**M4. The repeat mark → `stage{type:delivery}`.** *(Sinai 2026-07-21.)* The
printed `(ביס)` instruction to sing a line again is a musical performance
direction, so it is typed `delivery` like the other performance instructions
(vocal manner, `(זינגט)`), not stage business.
> Was `type="repeat"` from 2026-07-19 to 2026-07-21. `repeat` is now retired
> from the vocabulary; the 136 live spans were migrated with
> `annotation.migrate_repeat_to_delivery`. See §9.

**Spellings covered** — all are the same mark: `(ביס)`, `(ביסס)` (doubled ס),
the **pointed** `(בּיס)` / `(בּיסס)`, and the **zayin** spelling `(ביז)` /
`(קאהר ביז)` *(Sinai 2026-07-20, Yudale p31/p32)*. Match nikud-insensitively.
⚠️ `ביז` is homographic with the preposition "until", so the zayin form is
matched **only** when anchored — parenthesised, whole-line, or after a chorus
name — never as a bare substring. Two independent misses here (nikud-blind in
the first pass = 29; zayin = 3) are the reminder to verify coverage with a
pattern INDEPENDENT of the matcher.

**Printed without parentheses** *(Sinai 2026-07-20)*. Three plays print the
mark bare on its own line — Bas Sheva (9), Ezra (6), Blimele (3). Match only
when the mark is the **whole line** (trailing punctuation allowed): that
anchoring is what makes it safe without parens, where a substring match would
hit `ביסלעך`, `ביסינג` and the preposition `ביס` ("until").
⚠️ The fetch list was itself built from the paren-anchored pattern, so Bas
Sheva never entered it and no pass of any kind touched the play. Rebuild the
target list from every marker pattern the tool knows
(`retag_musical_directions --rebuild-targets`), never from one of them.

**Repeat with a count** — `(ביס 2 מאל)`, `(ביס 4 מאהל)` — is tagged as a plain
`delivery`; **the number is not recorded** *(Sinai 2026-07-19)*. The mark is
placed **where printed**; scope is not recorded via `@target`.

**M4b. Compound `(קאהר ביס)` → `stage{type:delivery; xmlid:kor}`** → TEI
`<stage type="delivery" who="#kor">`. A collective named together with the
repeat instruction ("chorus, repeat"). *Sinai 2026-07-19; retyped delivery
2026-07-21.*
- The **whole parenthesis** is one span. It is one editorial unit and one
  instruction; we don't split character names out of `(ער גייט אב)` either.
- `<stage>` carries `@who` through **att.ascribed** (verified against the TEI
  spec, `ref-stage.html`). This is standards-supported, not a coinage.
- `stage.xmlid` uses the same space-separated convention as `speaker.xmlid`.
- **`כער` is a spelling variant of `כאר`/`קאהר`, not an OCR error** *(Sinai)* —
  nine genuine `(קאהר ביס)` in the corpus support the reading.
- Attested: `(קאהר ביס)` ×9, `(קאהר - ביסס)`, `(כער ביס)` → `#kor`;
  `(אלע ביס)` → `#alle`.

**False friends — never tag as repeat:** `(אויפטריט ביסינג)` ("enter Bising",
a character in Mishke Mashke) and `(ערוואכט צו ביסלעך)` ("awakens bit by bit").

*Applied corpus-wide: **136** marks, 13 ascribed, all now `delivery`.* **Ratified by Sinai; Noa informed, no sign-off
required.**

**M5. `רעפריין` → `head`** (keeping `lg_id`); the enclosing block becomes
`<lg type="refrain">`. It is a structural rubric, not a verse line, not a
speaker, not a stage direction.

**M6. Voice rubrics → `speaker`** (§G.4). A printed rubric before sung lines
(`קאָהר:`, `סאלא אלט:`, `סאפראן:`, `דועט קארל און ראשעל:`) opens
`<sp><speaker>…</speaker>` with the sung text in `<lg>/<l>`. `@who` ladder:
1. a named play-role sings → that role's id;
2. a solo rubric that identifiably **is** a character → keep the printed rubric
   in `<speaker>`, resolve `@who` to the character;
3. a duet of two named singers → space-separated `@who`;
4. a genuinely abstract voice → `<personGrp>` (group) or `<person>` (solo) in
   listPerson, flagged **`"printed": false`** in cast_dict so `build_tei` keeps
   it out of the printed castList while still resolving `@who`.

> `סאלא אלט` says **who sings**; `רעפריין` says **which part of the song** it
> is. Different layers, different tags.

**M7. Idempotency.** `annotate_songs` re-runs strip `l`/`lg` and song-derived
`head` spans — but **preserve** any `head` covering a refrain rubric or
carrying no `lg_id` (castList `פערזאנען`, act headings).

**M9. An `l` covers the spoken text only — never the speaker label** *(Sinai
2026-07-20)*. In TEI `<speaker>` and `<l>` are siblings inside `<sp>`; the
printed label is not part of the verse line. So on
`עָזריה: יא טיעף אין זיעזעס…` the `l` starts at offset 8, after the label and
its colon — not at 0.

Corpus was split 119 wrong / 139 right; the 119 were rescoped 2026-07-20 with
`auto_resolve_flags --sweep-l-scope`. A line that is nothing but a label loses
its `l` entirely.

This is the same rule §G.4/M6 already applied to voice rubrics ("shrink the
`l` to the sung tail"); it had simply never been applied to named speakers.

⚠️ The published TEI was never wrong: `build_tei.speaker_slice` re-splits the
line by the SPEAKER span's length and emits only the remainder as `<l>`,
ignoring the `l` offsets for content. The fix is to the annotation layer, so
the offsets mean what they say for any consumer that reads the PAGE-XML
directly. Trim the same leading punctuation `speaker_slice` does, or the two
disagree.

**M8. A whole-line stage direction is not a verse line** *(Sinai 2026-07-19)*.
When a `stage` span covers the entire line, drop any `l` on it — a bare
`(ביסס)` or `(טאנץ).` on its own line inside a song was being marked as verse.
**Narrow by design:** of 111 lines carrying both `stage` and `l`, 94 are
genuine sung lines with an *inline* `(ביס)`, and dropping `l` there would
destroy the song encoding. Require an actual whole-line `stage` span — testing
"widest stage span ≥ line length − 1" with no stage span present is true for
any single-character line, which silently deleted the `l` from nine lone `—`
placeholders filling Blimele p.27's song columns.

---

## 6. Page furniture and act/scene headings

`fw` covers printed page numbers, running heads, catchwords, signatures.
`type` required; `FW_TYPES = {pageNum, header, footer, catch, sig}`. Page
numbers are `type:pageNum`. Per-play quirks (spread scans, bare-number plays)
are handled in `annotation.tag_pagenums_collectives`.

**H1. What counts as an act heading** *(Sinai 2026-07-20)*. One matcher,
`schema.parse_act_heading` / `parse_scene_heading`, used by both annotators.
Accepts, with or without nikud:
- a Hebrew word ordinal before the word — `ערשטער אקט`, and the variant
  spelling `פיערטער` (Mishke Mashke p.16, Bas Sheva p.54);
- a **Roman numeral on either side** — `I. אַקט`, `אַקט .II`, `V. אַקט.`,
  `III אַקט` (the numeral-after forms are how the period is stored in RTL);
- **trailing content after the heading** — Isha Raa p.5
  `I. אַקְט (I. Rittornetto)`.

Before this the matcher lived in two copies, took only a Hebrew word ordinal,
and anchored to end-of-line: 26 headings tagged corpus-wide, **25 untagged**.

**H2. Never an act heading:** `אקט` followed by another Hebrew letter — every
title page reads `אין 4 אקטען` ("in 4 acts"). Enforced by lookahead. Act *ends*
are `trailer` (ST12), never `heading`.

**H3.** The matcher runs at annotate time, so fixing it does nothing for pages
already annotated — sweep them with
`annotation.auto_resolve_flags --sweep-headings` (adds a `heading` span only to
a line that has none, skipping `trailer`).

---

## 7. Editorial / OCR policy

**Text-wrong vs. cast-needs-mapping.** These are different queues:
- the **printed text is wrong** (OCR error) → Judith's transcript queue;
- the **text is right but the label isn't in cast** → cast_dict
  `prefix_variants`.

`prefix_variants` captures both, so a speaker resolves immediately while the
underlying transcript fix proceeds independently. Noa's `(c) OCR →` answers are
applied as variants and do **not** silently rewrite the page text.

---

## 8. Deferred — do not implement

**B7 / B8 — `(ביס)` as a song-mode trigger and same-page / cross-page
backfill.** *Noa 2026-06-14:* "A separate detailed report/log specifying the
exact page boundaries … will be provided. **Do not apply automated same-page
backfill across the board yet.**"

The rule shipped anyway on 06-14 (commit `7610fcb4`), before her answer
arrived, and ran for five weeks with a comment misattributing it to her as
approved. **Disabled 2026-07-19** (commit `edb79aef`). Re-enable only against
her report.

⚠️ **Code and data currently disagree.** Existing ביס-derived spans remain in
the corpus (~2,217 `l` spans). Blimele holds 388 `l` spans with no `lg_id`;
since grouping is driven by `lg` markers rather than `lg_id`, those emit as a
few undifferentiated `<lg>` blocks with no `@n` — stanza structure lost, though
**not** invalid TEI. Reconciling requires a corpus-wide re-run, which would
rewrite Noa's Di Seder work; deferred to her report.

---

## 9. Superseded rules

| Rule | Superseded by |
|---|---|
| B2/B3: `ערשיינט`+verb → `mixed`; `אב`+action → `mixed` | ST3 multi-token typing (2026-06-18). `mixed` is now a fallback only. |
| Global D as originally *asked* ("strip `אַ` from bare") | Noa: "Never strip the article." Keep it. |
| `(ביס)` opens song mode | §8 — deferred, disabled 2026-07-19 |
| `רעפריין` as a speaker / verse line | M5 — it is a `head` |
| Voice rubrics as `stage type="delivery"` or inline verse | M6 / §G.4 |
| `l` spanning the speaker label as well as the speech | M9 — spoken text only (2026-07-20) |
| Act-opening parenthetical typed `business` (no cue word) | ST7b — position, not vocabulary (2026-07-20) |
| `stage{type:repeat}` for the `(ביס)` mark (2026-07-19) | M4 — retyped `delivery`, `repeat` retired (2026-07-21) |
| Act heading = Hebrew word ordinal only, anchored to line end | H1 — Roman numerals, either side, trailing content allowed (2026-07-20) |

**B9 vs ST3 — resolved 2026-07-20 (Sinai): ST3/option C wins.** Entrance+exit
in one direction is `type="exit entrance"`, not `mixed`. B9 (2026-06-24) had
required literal `mixed`, but option C reserves `mixed` for functions that
CANNOT be enumerated, and entrance+exit plainly can — the 06-18 document lists
`exit entrance` as its own example. `auto_resolve_flags` updated.

⚠️ Only ONE span in the corpus was genuinely entrance+exit (Der Mann p.13).
The other **21** `mixed` spans stand in for other combinations — mostly
entrance+business and exit+business, several of them continuation lines of a
multi-line direction (ST9), and at least one that is really a `setting`
(Yudale p.51, an act-opening tableau). They each need their own enumeration
and are NOT covered by this decision.

---

## 10. Out of scope

Manuscript / handwritten plays (Meshumed, Yaakov-Esav, the YIVO group,
Emigration, …) are a **separate track**. The whole pipeline — stage lexicons,
`(ביס)` conventions, orthography assumptions — is calibrated on printed OCR.
Manuscripts bring editorial layers, strike-throughs, scribal `<unclear>` and
mixed-language insertions. Do not bootstrap them as one-offs.

---

## 11. Regie cue notation (manuscript track)

*Sinai 2026-08-18. Decoded from MS_Emigration + Lateiner_Meshumed; inventory in
`data/review/ms_cue_notation_inventory.tsv`.*

The handwritten witnesses carry Latin-script marks in the Regie/prompter hand:
`RI`, `RII`, `IR`, `IIR`, `R.I`, `SI`, `SII`, `N4`, `No 4.`, `Returnell:`,
`Chor`, `Marsch`, `Terzett`. They are **signs about the performance, not part
of the play's text**, so they are `metamark`, never `stage`.

**C1. The grammar.** Three orthogonal slots, confirmed by MS_BenHaDor's fully
spelled-out forms `II Returnell: No 4.` and `II Return: N=3` — i.e. *the second
ritornello of number 4*:

| slot | answers | forms |
|---|---|---|
| number | *which* number | `N`, `No 4.`, `N=3`, `N2`…`N12` |
| boundary | *where* it starts/stops | `RI`/`RII`, `IR`/`IIR`, `R.I`, `1 Returnell:`, `II Return:`, bare `I`/`II` (BasKoyen) |
| genre | *what kind* of number | `Chor`, `Marsch`, `Tanz`, `Terzett`, `Quarted`, `Duet`, `arya`, `Refrein` |

**C2. The letter is the family; the numeral is a state.** `R` = Returnel
(ritornello, the pit's vamp), `S` = Szene. The Roman numeral is **in/out, not a
serial** — which is why `RIII` and `SIII` never occur anywhere in the corpus.
`S II` falls on `פֿארהאנג` and `פֿערוואנדלונג`; Meshumed p.37 has `R II S II`
together at the Act IV curtain — music out *and* scene out at one moment.

**C3. Tagging.** `metamark {function; role}`, both **required**
(`schema.METAMARK_FUNCTIONS` / `METAMARK_ROLES`):

- `function` ∈ {`musicCue`, `sceneCue`} — the family.
- `role` ∈ {`in`, `out`, `genre`, `number`} — the slot. `in`/`out` are shared
  across families, so `RI` and `SI` both take `role:in`.
- `n` — the **normalised** number (`No 4.` → `n:4`). Required on `role:number`.
- `spanto` / `corresp` — see C5.

→ TEI `<metamark function="musicCue" ana="#cue-in" place="inline" n="4">`,
against the `prod-cues` taxonomy `build_tei` writes into `encodingDesc`. The
**span text keeps the diplomatic reading**; the normalisation lives in the
attributes. That is what lets `RI`, `IR` and `1 Returnell:` be one queryable
class without flattening what the scribe actually wrote.

**C4. `R` and `Returnell` are the same mark; the genre words are not.**
`R` *is* the abbreviation of `Returnell`, so `RI`, `IR`, `R.I`, `.R.I`,
`1 Returnell:` and BasKoyen's bare `I` all take `function:musicCue; role:in` —
the spelling variation is scribal, not semantic. The numeral-first forms
(`IR`, `IIR`, `II R`) are **RTL/bidi rendering flips of the same code**, not a
separate notation; do not tag them as a variant.
⚠️ But `Terzett` carries no in/out state — it says the number is a trio.
Genre words take `role:genre`. Collapsing them into `role:in`/`out` destroys
both the score index and the ability to ask where the music runs.

**C5. `S` additionally justifies structure.** `R` records something the TEI
does not otherwise hold (what the pit does); `S` marks a boundary TEI already
models. So a scene cue carries `corresp` to the `<div>` it delimits, and the
Meshumed `S` marks are the documentary evidence for where that play's
divisions fall. Where an in/out pair is identified, the opening mark carries
`spanto` → an `<anchor>` at the closing one; that makes the bracket reading
falsifiable by query rather than by assertion.

**C6. The boundary rule — function, not word.** *Is it part of the play, or a
sign about the play?*
- `פֿערוואנדלונג`, `פֿארהאנג`, `Vorhang`, `Verwandlung`, `Auftritt` → part of
  the text: `stage` (act ends stay `trailer`, ST12).
- `קאהר` heading a sung line → part of the text: `speaker` (§M6).
- `Chor` beside `N=3` → a sign about the text: `metamark`.

⚠️ **MS_YoysefInEgipten's 23 `קאהר` hits are speaker labels, not cues.** Same
word, opposite treatment.

**C6a. Two tests decide a chorus word**, applied in this order by
`annotation.tag_ms_cues`:
1. **Already tagged `speaker` or `role`?** Then it is a label and never a cue —
   the RA has decided, and it resolves to an xml:id that `<sp who>` depends on.
   Overriding it would break speaker attribution. 3 cases, incl. Emigration
   p.5 `RII N קאהר דער שעהנער זאממער`, where `RII` and `N` are still tagged so
   the musical information survives.
2. **Is it the agent of its stage direction?** If the enclosing `stage` span
   still holds substantive text once every cue is removed, the chorus is the
   party doing something — `2 מערדער קאהר זינגט פריער מארש`,
   `אופטריט עלמת רחומה קאהר.`, `hintergrund chor on` — and tagging it
   `metamark` would strip the subject out of the direction. 5 cases.

**C6b. The old annotation comes off.** A cue span carries `metamark` and
nothing else: the covering `stage` **and `heading`** spans are subtracted
(union of all cues on the line, in one pass — `II Return: N=3` is two cues
under one span). A residue with no substantive text is dropped rather than
left as a stage direction consisting of a full stop; a residue on both sides
becomes two spans, which §1 allows. *Sinai 2026-08-18: "I prefer
consistency."* The Meshumed marks were tagged `heading`, so `R II S II` was
being published as an act heading — 63 such spans dropped, which also cleared
62 corpus lint flags.

**C6c. A bare I/II beside another cue is a cue** with the letter elided —
`R I II` marks both R I and R II; `N3    II` is number 3 plus its out-bracket.
Requiring another cue on the same line is what keeps this off Khurbn's act
numbers (C7), which stand alone and run to V.

**C7. Bare numerals are play-specific — never sweep them corpus-wide.**
- **MS_BasKoyen**: bare `I`/`II` ARE cues (they sit on `קאהר` and
  `(קאהר אב מיט געזאנג)`), with the letter dropped. 24 of them.
- **MS_KhurbnYerusholaim**: bare numerals run I–V. IV and V cannot be an
  in/out bracket, so those are act numbers. Leave them to `heading`.

**C8. DraCor.** `<metamark>` is tei_all but outside the DraCor profile —
nothing in their pipeline reads it. `build_tei` **strips** it from the
`tei/dracor/` variant (it strips rather than whitelists, unlike
`dracor_transform`, so every new element needs an explicit line there). The
`prod-cues` taxonomy is kept in both: it documents the witness at no cost.

⚠️ `data/review/heading_notation_questions_2026-08-16.tsv` predates this and
still carries the stale guess `German (S=Scene, R=?)` in its `family` column;
it also only covers marks already tagged `heading`/`stage`, so it misses every
spelled-out musical cue. Work from the inventory, not from that sheet.

---

## Reference

Tools: `annotation.auto_annotate` · `annotation.auto_resolve_flags` ·
`annotation.lint_pages --all` · `annotation.retag_musical_directions` ·
`annotation.tag_pagenums_collectives` · `transkribus.refresh_page_annotated` ·
`structure.build_tei` · `structure.check_who`

**Run the pipeline under `python3.11`, not the 3.9 venv.**

**Before any push:** check the page's current top Transkribus layer. Build the
edit on top of it, chain `parent_tsid`, preserve `status`. Never regenerate a
page from a stale local mirror — that is how Noa's 06-24 castLists were buried.
