# CastList tagging conventions (post-Noa 2026-06-14 replies)

Reference for the RA workflow that hand-tags `role` / `roleDesc` spans on castList pages in Transkribus. Codifies Globals A–E from `handoff_2026-06-14_castlists_remaining_plays.md`. These are **conventions for human (or LLM-assisted) tagging** — the cast_dict generator (`extract_cast_dict.py`) is a pure harvester; it only reflects what was tagged.

## A. Setting lines at the bottom of a castList

Lines beginning `אָרט דער האַנדלונג…` or `דיא געשיכטע האנדעלט זיך…` (or close variants) are **not** roles. Tag the whole line as `stage{type:setting}`. The auto-resolver lexicon now also catches these by phrase (`auto_resolve_flags.stage_lexicon`).

## B. Library shelfmarks

Lines like `II 43.144`, `1943 D12`, `ע63.390`, BN catalog IDs: **leave untagged**. They are not part of the dramatic text.

## C. Brace-group siblings

A printed brace ` } זייערע קינדער` covering N stacked names → N separate `role` `castItem` spans, each sharing the same `roleDesc` (`זייערע קינדער` / `זיינע קינדער`). Don't fuse the siblings into one item; don't drop the shared roleDesc.

## D. Bare-functional roles with indefinite article

`א וועכטיר`, `אַ דִיענֶער`, `אַ גַייסט`, `פּרעפעקט`, `אַ ריכטיר…`: tag as individual roles. **Keep the `אַ` / `א` in the bare form** — do NOT strip it. (This reverses an earlier normalization in our cast_dict generator's `bare`-field handling. The harvester preserves whatever the role span contains, so the convention drives the data.)

## F. Profession / relation modifiers → roleDesc always (Noa 2026-06-18)

A profession-noun or relation-noun appearing *alongside* the proper name — "his nephew" (`זיין נעפפע`), "moneylender" (`פּראָצענטניק`), "eye doctor" (`(אויגען דאָקטער)`), "his wife" (`זיין ווייב`), "her uncle" (`איהר פעטער`), "his second wife" (`זיין צוויטע פרוי`) — goes in `roleDesc`, **always**. The `role` span covers only the proper-name portion.

Fused titles that bind to the name as part of the printed identifier — `פּראפעסאר עדעלמאן` (Professor Edelman), `דאן איזראעל` (Don Yisroel), `קעניג בלשצר` (King Belshatsar), `ר' יאָכטשעֶ` (Reb Yokhtshe) — **stay inside the role span**, with the post/parenthetical descriptor going in roleDesc.

Quick test: if the modifier could be dropped without affecting *who* the speaker is, it belongs in `roleDesc`. "Edelman" alone identifies the character; "Professor Edelman" is the same character. "Iser" alone identifies him; "Iser the moneylender" is the same character.

Applied 2026-06-18 to Yudale castList page (lines 65 / 107 / 114). Same pattern should be audited and applied on every other play's castList page going forward; existing entries are mostly correct because the harvester picks up whatever's in the `role` span, but a sweep would catch role spans that currently spill into modifier text.

## E. Final-line collective enumerations

A castList trailer like `יוּדען, יוּדענעס, געסטע, קאָהר` or `דיענער, מאטראזען, חינעזער, פאסאזשירען`: split by comma, one collective `xml:id` per token (e.g. `yudn`, `yudines`, `geste`, `kor`). Do **not** create one catch-all xmlid. Do **not** tag literary "etc." markers (`אאז"ו`, `עטצ.`) as characters — leave them in surface text but unspanned.

## G. particDesc / castList / song-voice encoding (DraCor alignment, 2026-07-02)

Ratified after checking our practice against DraCor's Schematron (which validates
against `particDesc`, not `castList`). Implemented in `structure/build_tei.py`;
verify any edition with `python3.11 -m structure.check_who tei/<Play>.xml`.

1. **`particDesc/listPerson` is the machine-readable master; `castList` is documentary.**
   Every role gets a `listPerson` entry (so every `@who` resolves); the printed
   dramatis-personae is mirrored in `castList` and is **not** padded with voice
   parts or non-printed collectives. This was already our practice — unchanged.

2. **Collective / chorus speakers are `<personGrp xml:id="…">`, not `<person>`.**
   Any cast_dict role flagged `"collective": true` (`אלע`/alle, `קאהר`/chorus,
   `שטימען`, `דאמען`, `קינדער`, …) now emits `<personGrp><name>` instead of
   `<person><persName>`, matching DraCor corpora so network tooling treats them
   as group nodes. Same `xml:id`, so `who="#kor"` still resolves. **Data gap to
   audit:** a chorus role must actually carry `"collective": true` in cast_dict
   to become a personGrp — e.g. Di Seder's `kor` is currently a plain person and
   should be flagged collective.

3. **Joint / duet turns use space-separated `@who`.** A speaker span whose
   `xmlid` is two-or-more space-separated ids (`xmlid:karl_rizvan rashel`,
   Noa 2026-06-14) now emits `who="#karl_rizvan #rashel"` — each id validated and
   `#`-prefixed independently (previously it mis-emitted `who="#karl_rizvan rashel"`
   and was wrongly flagged as an unknown role).

4. **Song-supplement voice parts are speaker attributions, not stage directions.**
   A printed rubric before sung lines in the `<back>` supplement (`קאָהר:`,
   `סאלא אלט:`, `סאפראן:`, `דועט קארל און ראשעל:`) opens an `<sp><speaker>…</speaker>`
   whose sung text goes in an `<lg>/<l>` — **not** a `<stage type="delivery">`, and
   no longer baked inline into the verse `<l>`. Resolution policy for its `@who`:
   - **A named play-role sings** → `@who` points to that role's existing id
     (`karl_rizvan`, `rashel`). Trivial and keeps songs queryable per character.
   - **A solo voice rubric that identifiably *is* a character** (the "Sopran" of
     an operetta duet = the heroine) → keep the printed rubric in `<speaker>` but
     resolve `@who="#mirele"`. Transcription and interpretation in their proper
     layers.
   - **A duet/pair of two named singers** → space-separated `@who` (case 3).
   - **A genuinely abstract voice** (unattributable chorus/solo) → give it a
     `listPerson` entry — `<personGrp xml:id="chor">` for a group, `<person
     xml:id="sopran">` for an abstract solo — and point `@who` there.
   Do **not** add Sopran/Alt/Chor to the printed `castList`; if a human-visible
   listing is wanted, use a separate editorial `<castGroup>` (not mixed among the
   printed roles). An un-resolved voice label surfaces as an `<sp>` with no `@who`
   in `check_who.py` check [2] — that is the RA's to-do list, not an error.

## Per-play notes locked in by Noa

- **AlNaharotBavel** — `זמרי` is one persistent role; `דער קעניג`/`קעניג בלשצר`/bare `בלשצר` all → `belshatsar`; `דלילה` is a single role (`favoritin` is a roleDesc, not an ensemble label); `בן כספי` bare canonical short. **Do NOT pre-seed a `kor` collective** — tag chorus lines only if they appear in the body.
- **DerManUnterTiff** — `יוסף` and `אסיפ` both → `yosef_osip`; brace officers (`איוואן` / `גרעגואר מארקסענעל` / `וואסקא א גלח`) are three siblings sharing `אפֿיציערען`; four functional roles individually; full `ליובאוו אבאולאוונא` is the role string with bare→full routing; strikethrough role is **kept** (editorial cancellation, not removed).
- **Di Seder Nakht** — closing history line → setting; functional roles individual; `ריזוואן` ambiguous → resolves to last-spoken brother (Galed-style heuristic); `קאטינקא` desc empty for now. **No pre-seeded kor.**
- **KidushHashem** — shelfmarks untagged; `שבת גוי` as a speaker label → `andre`; `אֵינקוויטאָר` is `roleDesc`, not part of name; bare `איזראעל` / `דאן איזראעל` → `dan_yisroel`. **No pre-seeded kor/hoyf.**
- **Lateiner_Meshumed** — page-XML split: now bootstrappable; 44-page pull from coll 18874 lives in `data/Lateiner_Meshumed/page_pulled_18874_2026-06-18/`. Ages stay in `roleDesc` as free text. Two `אורחים` are individual roles (`orekh_krakever`, `orekh_turniover`) sharing `roleDesc=צוויי אורחים`. Single role `sabele`. (Q5 was a workflow-glitch hallucination — see correction note in main handoff.)
- **MishkeMashke** — four collectives: `dinens`, `matrozn`, `khinezer`, `pasazhirn` (not one catch-all); `מאשקע`/`מישקע` distinct ids, no fuzzy match; `שאַרלאָטאַ'ס` possessive → prefix_variant of `sharlata`; `מאשקע` is gendered male.
- **Yudale der Blinder** — braced siblings (Convention C); 4 distinct collectives from final line; `(בלינד)` is part of `roleDesc` for `yudale`; `פּראָצענטניק` and `(אויגען דאָקטער)` are roleDescs, not part of name.
- **Dos Yudishe Kind** — **KEEP `⸗` verbatim** in bare form (`הויפ⸗נאר` stays as-is; do NOT strip). **KEEP `אַ` article** in bare form (Convention D). Two collectives (`yudn`, `grafn`); literary `אאז"ו` left as surface text. `שׁמֶערְל`/`שׁפרִינְצֶע` roleDesc empty. Long roleDesc with cross-role reference (`בּײַ גראַף פּעטעפי`) preserved verbatim.
