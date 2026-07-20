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
`location`, `costume`, `novelistic`, `modifier`, `repeat`, plus the fallback
`mixed`.

**ST3. Multi-token `@type` for compound directions** *(Sinai + Noa 2026-06-18,
"option C")*. Space-separated tokens; **`mixed`, if used, must be the only
value**. `(לעגט וועג דיא האַרפֿע— ערשיינט)` → `type="entrance business"`.
Other shipped retypes: Der Mann p.11 `business entrance`; p.18 dance/song
lines `business delivery`; `פאָרהאַנג פאַלט.` → `setting`.

**ST4.** `ערשיינט` (and infinitive `ערשיינען`) alone → `entrance`.

**ST5.** Bare `(אב)` / `(<actor> אב)` → `exit`. Modal intent
`(<actor> וויל אב)` → `business`.

**ST6. Setting cues.** `פערווענלונג` / `פערוואנדלונג` / `פערוואנדעלונג` →
`setting`, with or without parens or nikud. `פאָרהאַנג` / `פאָרהאַנג פאַלט`
(curtain) → `setting`, **not** `business`.

**ST7. Post-header setting line.** A line immediately after an act/scene
header that is neither a speaker turn nor parenthesized → whole-line
`stage{type:setting}`.

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

**M4. The repeat mark → `stage{type:repeat}`.** The printed instruction to
sing a line again. Not stage business — it is a musical instruction. TEI's
`stage/@type` list is explicitly open and contains nothing musical, so this is
a sanctioned extension. Chosen over `<metamark function="repeat">` (purer, but
would need a new tag in both the Transkribus tagset and `ALLOWED_TAGS`).

**Spellings covered** — all are the same mark: `(ביס)`, `(ביסס)` (doubled ס),
and the **pointed** forms `(בּיס)` / `(בּיסס)`. Match nikud-insensitively; a
nikud-blind pattern missed 29 instances in the first pass.

**Repeat with a count** — `(ביס 2 מאל)`, `(ביס 4 מאהל)` — is tagged as a plain
`repeat`; **the number is not recorded** *(Sinai 2026-07-19)*. The mark is
placed **where printed**; repeat *scope* is not recorded via `@target`.

**M4b. Compound `(קאהר ביס)` → `stage{type:repeat; xmlid:kor}`** → TEI
`<stage type="repeat" who="#kor">`. A collective named together with the
repeat instruction ("chorus, repeat"). *Sinai 2026-07-19.*
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

*Applied corpus-wide 2026-07-19: 114 marks, 12 of them ascribed.*
***Awaiting Noa's ratification.***

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

---

## 6. Page furniture

`fw` covers printed page numbers, running heads, catchwords, signatures.
`type` required; `FW_TYPES = {pageNum, header, footer, catch, sig}`. Page
numbers are `type:pageNum`. Per-play quirks (spread scans, bare-number plays)
are handled in `annotation.tag_pagenums_collectives`.

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

**Open contradiction — B9 vs ST3.** Noa's B9 ruling says entrance+exit
co-occurring "MUST be explicitly typed `stage{type:mixed}`". But entrance+exit
is exactly enumerable, so ST3 says `type="exit entrance"` — and the 06-18
follow-up itself lists `exit entrance` as an example. **Never re-asked. Open
question for Noa.**

---

## 10. Out of scope

Manuscript / handwritten plays (Meshumed, Yaakov-Esav, the YIVO group,
Emigration, …) are a **separate track**. The whole pipeline — stage lexicons,
`(ביס)` conventions, orthography assumptions — is calibrated on printed OCR.
Manuscripts bring editorial layers, strike-throughs, scribal `<unclear>` and
mixed-language insertions. Do not bootstrap them as one-offs.

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
