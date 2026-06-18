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

## E. Final-line collective enumerations

A castList trailer like `יוּדען, יוּדענעס, געסטע, קאָהר` or `דיענער, מאטראזען, חינעזער, פאסאזשירען`: split by comma, one collective `xml:id` per token (e.g. `yudn`, `yudines`, `geste`, `kor`). Do **not** create one catch-all xmlid. Do **not** tag literary "etc." markers (`אאז"ו`, `עטצ.`) as characters — leave them in surface text but unspanned.

## Per-play notes locked in by Noa

- **AlNaharotBavel** — `זמרי` is one persistent role; `דער קעניג`/`קעניג בלשצר`/bare `בלשצר` all → `belshatsar`; `דלילה` is a single role (`favoritin` is a roleDesc, not an ensemble label); `בן כספי` bare canonical short. **Do NOT pre-seed a `kor` collective** — tag chorus lines only if they appear in the body.
- **DerManUnterTiff** — `יוסף` and `אסיפ` both → `yosef_osip`; brace officers (`איוואן` / `גרעגואר מארקסענעל` / `וואסקא א גלח`) are three siblings sharing `אפֿיציערען`; four functional roles individually; full `ליובאוו אבאולאוונא` is the role string with bare→full routing; strikethrough role is **kept** (editorial cancellation, not removed).
- **Di Seder Nakht** — closing history line → setting; functional roles individual; `ריזוואן` ambiguous → resolves to last-spoken brother (Galed-style heuristic); `קאטינקא` desc empty for now. **No pre-seeded kor.**
- **KidushHashem** — shelfmarks untagged; `שבת גוי` as a speaker label → `andre`; `אֵינקוויטאָר` is `roleDesc`, not part of name; bare `איזראעל` / `דאן איזראעל` → `dan_yisroel`. **No pre-seeded kor/hoyf.**
- **Lateiner_Meshumed** — page-XML split: now bootstrappable; 44-page pull from coll 18874 lives in `data/Lateiner_Meshumed/page_pulled_18874_2026-06-18/`. Ages stay in `roleDesc` as free text. Two `אורחים` are individual roles (`orekh_krakever`, `orekh_turniover`) sharing `roleDesc=צוויי אורחים`. Single role `sabele`. (Q5 was a workflow-glitch hallucination — see correction note in main handoff.)
- **MishkeMashke** — four collectives: `dinens`, `matrozn`, `khinezer`, `pasazhirn` (not one catch-all); `מאשקע`/`מישקע` distinct ids, no fuzzy match; `שאַרלאָטאַ'ס` possessive → prefix_variant of `sharlata`; `מאשקע` is gendered male.
- **Yudale der Blinder** — braced siblings (Convention C); 4 distinct collectives from final line; `(בלינד)` is part of `roleDesc` for `yudale`; `פּראָצענטניק` and `(אויגען דאָקטער)` are roleDescs, not part of name.
- **Dos Yudishe Kind** — **KEEP `⸗` verbatim** in bare form (`הויפ⸗נאר` stays as-is; do NOT strip). **KEEP `אַ` article** in bare form (Convention D). Two collectives (`yudn`, `grafn`); literary `אאז"ו` left as surface text. `שׁמֶערְל`/`שׁפרִינְצֶע` roleDesc empty. Long roleDesc with cross-role reference (`בּײַ גראַף פּעטעפי`) preserved verbatim.
