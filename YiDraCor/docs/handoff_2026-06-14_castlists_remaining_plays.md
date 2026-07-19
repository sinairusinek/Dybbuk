# YiDraCor — castList questions for Noa, 2026-06-14 (remaining plays)

Survey of castList pages for the seven plays we have not yet RA-reviewed. Each question is yes/no; "yes" locks the call into the cast\_dict / annotation pipeline. Questions are ordered from most uncertain downward, capped at six per play. Anything you say "no" to, please add a one-line note so we can refine before re-running.

Same conventions as `handoff_noa_2026-06-14_questions.md`:

- `[new behavior]` — no precedent in pulled XML; please confirm.  
- `[confirmed in data]` — your earlier edits already imply this; flag only to OVERRIDE.

---

## AlNaharotBavel (Amkreut & Freund, 1909\)

castList page: `page_annotated/0006_...Page_06.xml`. cast\_dict has 12 roles.

1. `[new behavior]` Should the closing lines `אָרט דער האַנדלונג באַבילאָן, אין דעם 70-טען יאהר / נאך חרבן בית ראשון` be tagged as `stage{type:setting}` (place \+ time of action) and NOT coined as a role? (They currently sit in the castList region but are prose, not a castItem.)  
2. `[new behavior]` `זמרי, אַ יודישער רענעגאט` — should body-page mentions of זמרי be treated as one persistent role even when post-renegade he is referenced descriptively (parallel to Berele→פאויל)?  
3. `[new behavior]` `בלשצר, קעניג פון בבל` — when the body uses `דער קעניג` / `קעניג בלשצר` / bare `בלשצר`, all to the same xml:id `belshatsar`, yes?  
4. `[new behavior]` `דלילה, פאוואריטין דעם קעניגס` — single role `delila`, no separate `kenig_favoritin` ensemble label, yes?  
5. The cast has no chorus/collective explicitly listed, but the play is a "biblishes muzikdrama" and body pages are likely to carry יוּדען / כאר lines. Should we pre-seed a `kor` collective xml:id even though it is not in the printed castList?  
6. `בן כספּי, א רייכער יוד אין בבל` — is bare `בן כספּי` (without `דוד זיין זוהן` underneath collapsing into it) the canonical short form, yes?

---

## DerManUnterTiff (Lateiner, "Der Mann unter dem Tisch")

castList page: `page_annotated/0004_30557022.xml`. **No `cast_dict.json` exists yet** — castList here is in mixed German/Yiddish orthography and contains several heavy OCR-noise lines.

1. `[new behavior]` Should we auto-coin a cast\_dict for this play from page 4 even though the cast names are unusually German-flavored (`גראַף פּעטעפי`\-style transliteration), or do you want to hand-curate the dict first?  
2. `[new behavior]` `יעקב אייזענשטיין` vs the inline alias `יוסף  זיין זוהן  (אסיפ דער משמד)` — body pages alternate `יוסף` ↔ `אסיפ`. Confirm both surface forms map to one xml:id `yosef_osip` (parallel to the Berele→פאויל rule)?  
3. `[new behavior]` Lines 9–11 are a brace-group on the page (`איוואן / גרעגואר מארקסענעל } אפֿיציערען / וואסקא א גלח`). Should the brace be encoded as three sibling castItems all carrying `roleDesc = אפֿיציערען`, with `וואסקא א גלח` as its own separate item even though it sits inside the same brace?  
4. `[new behavior]` `א געהיים פאליציסט`, `א ריכטיר פֿון געהיימס געריכט`, `א שרייביר ביי געריכט`, `א וועכטיר` — four bare-functional roles with no proper name. Coin them as individual roles (`gehaym_politsist`, `rikhter`, `shrayber`, `vekhter`) rather than a single `gerikht` collective, yes?  
5. `[new behavior]` `ליובאוו אבאולאוונא. אסיפס געליבטע` — should this be one role with bare form `ליובאוו` (Russian patronymic dropped for xml:id purposes) and the patronymic kept only in `form`?  
6. The strikethrough `<hi style="text-decoration: line-through;">גרעגואר מארקסענעל</hi>` on line 10 — was this deliberately struck in the printed source? If yes we drop it; if it is an editorial cancellation by the transcriber, we keep it. Which?

---

## Di seyder nakht (Emkroyt, 1908\)

castList page: `page_annotated/0004_OTgwNjMyNTk.111102484.xml`. cast\_dict present. Cast block is a clean Kohn-family ensemble.

1. `[new behavior]` Closing lines `דיא געשיכטע האנדעלט זיך אין גאלאטץ אין דער מאלדוויא אים יאהרע 1859` → `stage{type:setting}`, not a role, yes?  
2. `[new behavior]` `אֵיין אוּנְטֶערְזוּכוּנְגְס רִיכְטֶער` and `פְּרֶעפֶעקְט` — two bare-functional roles, no proper name. Coin both as individual roles, yes?  
3. `[new behavior]` `קַארְל רִיזְוַואן` \+ `דוּמִיטְרִיע רִיזְוַואן, זיין בּרוּדער` — when body pages refer to `ריזוואן` alone, should the short form route to whichever brother spoke last (Galed-style heuristic) or always default to Karl as the senior?  
4. `[new behavior]` `קַאטִינְקא` has no `roleDesc` in the castList. Should we leave `desc` empty in cast\_dict, or back-fill from body context once we tag?  
5. Should we expect a chorus/wedding-guests collective in body pages that is NOT in the printed castList (operetta convention)? If yes, pre-seed `kor`?

---

## KidushHashem (Lateiner, 1909\)

castList page: `page_annotated/0006_...Page_06.xml`. cast\_dict present. Spanish-Inquisition setting; the castList is followed by library shelfmarks (II 43.144, etc.) — those are clearly noise, not roles.

1. `[new behavior]` Should the lines `II 43.144 / II 63.390 / ע63.390. / 1943 D12 / 9ux 5 42` (library shelfmarks beneath the castList) be left untagged (no stage, no castItem, no setting), yes?  
2. `[new behavior]` `אַנְדְרֶעע אַ שבּת־גוֹי` — body pages may use `שבת גוי` as a descriptive vocative for Andre. Confirm all such mentions route to xml:id `andre`?  
3. `[new behavior]` This castList has NO explicit `אָרט דער האַנדלונג` line. Should we look on page 7 for an "in Spain, year XXX" line and retro-tag it as `stage{type:setting}`?  
4. `[new behavior]` `מֶעַנְדֶעס אֵינקוויטאָר` — is `אֵינקוויטאָר` ("inquisitor") part of the role name or a roleDesc? cast\_dict currently treats the whole string as the form.  
5. `[new behavior]` Operatic chorus/court-attendants are almost certain in a 5-act Inquisition operetta but are not in the printed castList. Pre-seed a `kor` and/or `hoyf` collective?  
6. `דָאן אִיזְרַאעֶל זַיין נעפֿע` — body shorthand will be `איזראעל` / `דאן איזראעל`. Both → xml:id `dan_yisroel`, yes?

---

## Lateiner\_Meshumed

**No `page_annotated/` exists** — only `text/raw/Lateiner_Meshumed.xml` (consolidated TEI, not page-XML). The cast block is in lines starting at `פעֶרְזאנעֶן`. Pipeline is upstream of page-XML splitting here.

1. `[new behavior]` Before we extend annotation to this play, do you want a one-off page-XML split of the consolidated TEI so it joins the rest of the corpus, or do we treat Meshumed as TEI-only and skip the page-annotated pipeline?  
2. `[new behavior]` Cast block contains explicit ages (`אַלְט 45 יָאהר`, `״ 35 ״`, `״ 18 ״`, `״ 45 ״`). Should ages live in `roleDesc` as free text, or in a separate `age` attribute on `<person>`?  
3. `[new behavior]` `צְוֵויי אורְחיִם / קראָקעֶוועֶר ״ 18 ״ / טוּרְניִווער ״ 45 ״` — two guests labeled by hometown (Krakower, Turniover). Coin as `orekh_krakever` \+ `orekh_turniover` under a shared `orkhim` group, yes?  
4. `[new behavior]` `סאָבּעֶלעֶ זַיין צווייֵטעֶ פְרוֹי` — explicit "second wife": no prior wife is named, so single role. Confirm?  
5. `[new behavior]` `ר' יאָכטשֶעֶ` appears twice (vocalized variants on consecutive lines). Confirm this is OCR/typesetting duplication and one role, not two characters with the same name.

---

## MishkeMashke (Kultur, 1910\)

castList page: `page_annotated/0004_NTU4MTU1.558055.xml`. cast\_dict has 10 roles. This is the printed castList that ends with `דִיענֶעֶר, מאַטראָזעֶן, חינעֶזעֶר, פּאַסאַזשירעֶן.` — a four-collective ensemble line.

1. `[new behavior]` The final line lists FOUR collectives: `דיענער` (servants), `מאטראזען` (sailors), `חינעזער` (Chinese), `פאסאזשירען` (passengers). Coin one collective xml:id per group (`dinens`, `matrozn`, `khinezer`, `pasazhirn`) rather than one catch-all? The play involves a sea voyage so they probably appear in different scenes.  
2. `[new behavior]` `מאשקע` (Mashke) vs `מישקע` (Mishke) — two characters whose names differ by ONE vowel (`א`/`י`). Body pages risk OCR confusion. Confirm we keep them as distinct xml:ids with tight speaker-string matching (no fuzzy match between the two)?  
3. `[new behavior]` `ביסינג: בּאַנקיִר` — roleDesc is just `בּאַנקיִר`. `ליעבערמאן : אֵיין אַמעֲריקאַנישעֶר בַּאַנְקיִר` — also a banker. When body uses bare `דער באנקיר`, default to ביסינג (first listed, primary) or flag as ambiguous?  
4. `[new behavior]` `שארלאטא : זַײַן גֶעלִיִעבְּטֶעֶ` (Karl's beloved) and `מינא: שאַרלאָטאַ'ס אַ גוטעֶ פרײַנדיִן` — possessive `שאַרלאָטאַ'ס` inside a roleDesc. Should the apostrophe-S form be added as a `prefix_variant` of שארלאטא so speaker matcher can route `שאַרלאָטאַ'ס` to xml:id `sharlata` if it ever appears as a speaker?  
5. `[new behavior]` `מאשקע: אִיהר פעֶטעֶר` — "her uncle" referring to Sharlata. Confirm Mashke is the gendered male character and not a misread of "מאַשקע" the feminine; otherwise the cross-references in body will not resolve.

---

## Yudale der blinder (Emkroyt, 1908\)

castList page: `page_annotated/0004_OTgwNjYwMDE.111007892.xml`. cast\_dict has 12 roles. Cast includes printed braces grouping siblings.

1. `[new behavior]` `ראזא / אַלטעריל` are braced under `זיינע קינדער` (Hertsl Valdman's children) — two castItems, shared `roleDesc=זיינע קינדער`, yes?  
2. `[new behavior]` `דבורה'לע } / פריידאלע } זייערע קינדער / בּערמאן }` — THREE sibling castItems braced under one `זייערע קינדער` (Yerukhem \+ Yakhne's children). Confirm all three coined as individual roles (`dvorele`, `freydale`, `berman`) with shared roleDesc?  
3. `[new behavior]` Final line `קאָהר געסטע, יוּדען, יוּדענעס עטצ.` — FOUR collectives: `קאָהר` (chorus), `געסטע`, `יוּדען`, `יוּדענעס`. Coin all four, or collapse `יוּדען`\+`יוּדענעס` into one mixed-gender `yudn` collective?  
4. `[new behavior]` `יוּדאלע זיין נעפפע (בּלינד).` — the parenthetical `(בּלינד)` is a permanent character attribute (he is the title role and blind throughout). Confirm `(בּלינד)` is the roleDesc and NOT a stage-direction-style note?  
5. `[new behavior]` `איסר פּראָצענטניק` — `פּראָצענטניק` (moneylender) is part of the name or a roleDesc? cast\_dict treats whole as form.  
6. `[new behavior]` `פּראפעסאר עדעלמאן (אויגען דאָקטער)` — same question: is the parenthetical the roleDesc, yes?

---

## דאס יידישע קינד (Dos Yudishe Kind, a komishe operete)

castList page: `page_annotated/0002_OTYyMjEzMDA.97285719.xml`. cast\_dict has 15 roles. Mixed Polish-Jewish nobility cast.

1. `[new behavior]` `הוִיפ⸗נאָר` ("Hofnarr" \= court jester) — the `⸗` (double oblique hyphen) survives in cast\_dict bare form as `הויפ⸗נאר`. Strip the hyphen for the bare form so body matches `הויפנאר` / `הופנאר` route correctly, yes?  
2. `[new behavior]` `שׁמֶערְל` and `שׁפרִינְצֶע` appear as consecutive bare names with no roleDesc — are these Jewish characters in the rabbi's household? roleDesc inferred from context, or leave empty?  
3. `[new behavior]` Final line `יוּדֶען, גראפֶען אַ. זַ. וו.` — `יוּדן` \+ `גראפן` \+ `אאז"ו` (etc.). Coin TWO collectives (`yudn`, `grafn`), drop the `אאז"ו` as a literary etc. marker, yes?  
4. `[new behavior]` `אַ דִיענֶער` and `אַ גַייסט` ("a servant", "a ghost") — bare-functional roles with `אַ` indefinite article. Coin as individual roles, yes (parallel to DerMan's bare officers)?  
5. `[new behavior]` `קֶענִיג זיגמוּנד` (King Sigismund) and `קֶערְקֶער⸗מַייסטֶער` (jailer/Kerkermeister) — both use German compound forms. Confirm body-pages will see `דער קעניג` / `דער מייסטער` and these route to the right xml:ids without further speaker variants?  
6. `[new behavior]` `מארטהא אַ גוּבּערנאַנטין בּײַ גראַף פּעטעפי` — long roleDesc with cross-role reference (`בּײַ גראַף פּעטעפי`). Keep full roleDesc verbatim, yes?

---

## Cross-play patterns to confirm globally

These recur in 5+ plays above; if you answer once, we apply everywhere:

A. `[new behavior]` `אָרט דער האַנדלונג ...` / `דיא געשיכטע האנדעלט זיך ...` lines at the bottom of a castList page → always `stage{type:setting}`, never a castItem. Apply to all plays?

B. `[new behavior]` Library shelfmarks (`II 43.144`, `1943 D12`, `ע63.390`, BN catalog ids) under a castList → leave untagged. Apply to all plays?

C. `[new behavior]` Brace-group siblings (`} זייערע קינדער` style) → N sibling castItems sharing one `roleDesc`. Apply to all plays?

D. `[new behavior]` Bare functional roles (`א וועכטיר`, `אַ דִיענֶער`, `אַ גַייסט`, `פּרעפעקט`) with indefinite article → individual castItems with the `א` preserved in `form` but stripped from `bare`. Apply to all plays?

E. `[new behavior]` Final-line collective enumerations (`יוּדען, יוּדענעס, געסטע, קאָהר`, etc.) → one collective xml:id per comma-separated token; drop `אאז"ו` / `עטצ.` as literary etc. markers. Apply to all plays?

Markdown

\#\#\# AlNaharotBavel (Amkreut & Freund, 1909\)  
1\. **\*\*\[new behavior\]\*\*** Yes. Tag the closing lines (\`אָרט דער האַנדלונג...\`) as \`stage{type:setting}\` (place \+ time of action) and do NOT coin them as a role.  
2\. **\*\*\[new behavior\]\*\*** Yes. Treat body-page mentions of זמרי as one persistent role (\`xml:id="zimri"\`) even when referenced descriptively post-renegade.  
3\. **\*\*\[new behavior\]\*\*** Yes. Route \`דער קעניג\`, \`קעניג בלשצר\`, and bare \`בלשצר\` to the same \`xml:id="belshatsar"\`.  
4\. **\*\*\[new behavior\]\*\*** Yes. It is a single role (\`delila\`); do not create a separate \`kenig\_favoritin\` ensemble label. This is a descriptive roleDesc.  
5\. No. Do not pre-seed a \`kor\` collective. Tag any \`כאר\` / \`קאָהר\` lines directly as a speaker if and when they appear in the body pages.  
6\. Yes. Bare \`בן כספּי\` is the canonical short form; do not collapse \`דוד זיין זוהן\` into it.

\#\#\# DerManUnterTiff (Lateiner) — \[Note: Swapped/Corrected Title\]  
1\. **\*\*\[new behavior\]\*\*** Yes. Auto-coin the \`cast\_dict\` from page 4 using the German-flavored transliterations; let the machine attempt to build it first.  
2\. **\*\*\[new behavior\]\*\*** Yes. Both surface forms \`יוסף\` and \`אסיפ\` map to a single unified \`xml:id="yosef\_osip"\`.  
3\. **\*\*\[new behavior\]\*\*** Yes. Encode the brace-group as three sibling \`castItem\` elements sharing \`roleDesc="אפֿיציערען"\`, with \`וואסקא א גלח\` as its own item.  
4\. **\*\*\[new behavior\]\*\*** Yes. Coin them as four individual functional roles (\`gehaym\_politsist\`, \`rikhter\`, \`shrayber\`, \`vekhter\`) rather than a single collective.  
5\. **\*\*\[new behavior\]\*\*** The full string \`ליובאוו אבאולאוונא\` is the role; tag the entire phrase as the character, and then route the bare first name to this full form.  
6\. This is an editorial cancellation/transcriber note, so keep the character; do not drop it.

\#\#\# Di seyder nakht (Emkroyt, 1908\)  
1\. **\*\*\[new behavior\]\*\*** Yes. Tag the closing history/setting lines as \`stage{type:setting}\`, not a role.  
2\. **\*\*\[new behavior\]\*\*** Yes. Coin both bare-functional roles (\`אֵיין אוּנְטֶערְזוּכוּנְגְס רִיכְטֶער\` and \`פְּרֶעפֶעקְט\`) as individual roles.  
3\. **\*\*\[new behavior\]\*\*** Route to whichever brother spoke last (Galed-style heuristic); do not default permanently to Karl.  
4\. **\*\*\[new behavior\]\*\*** Leave \`desc\` empty in \`cast\_dict\` for \`קַאטִינְקא\` for now; we will back-fill from context later.  
5\. No. Do not pre-seed a \`kor\` collective. Tag \`קאָהר\` / \`כאר\` lines directly as a speaker if they appear.

\#\#\# KidushHashem (Lateiner, 1909\)  
1\. **\*\*\[new behavior\]\*\*** Yes. Leave the library shelfmarks (\`II 43.144\`, etc.) completely untagged (no stage, no castItem).  
2\. **\*\*\[new behavior\]\*\*** Do not route descriptive vocatives within dialogue lines. However, if \`שבת גוי\` appears explicitly as a speaker label on a body page, route it to \`xml:id="andre"\`.  
3\. **\*\*\[new behavior\]\*\*** No. Do not scan page 7 to retro-tag an implicit setting line.  
4\. **\*\*\[new behavior\]\*\*** \`אֵינקוויטאָר\` is a title/roleDesc, not part of the character's proper name. Correct \`cast\_dict\` accordingly.  
5\. No. Do not pre-seed \`kor\` or \`hoyf\` collectives.  
6\. Yes. Both \`איזראעל\` and \`דאן איזראעל\` shorthand forms route to \`xml:id="dan\_yisroel"\`.

\#\#\# Lateiner*\_Meshumed — \[Note: Swapped/Corrected Title\]*  
*1\. **\*\*\[new behavior\]\*\*** This is a question for Sini; do not execute the page-XML split or choose TEI-only yet.*  
*2\. **\*\*\[new behavior\]\*\*** Keep explicit ages in \`roleDesc\` as free text; do not put them in a separate digital \`age\` attribute on \`\<person\>\`.*  
*3\. **\*\*\[new behavior\]\*\*** Tag both as separate individual characters (\`orekh\_*krakever\` and \`orekh*\_turniover\`) but assign them both the shared \`roleDesc="צווי אורחים"\`.*  
*4\. **\*\*\[new behavior\]\*\*** Yes. Confirm this is a single, unique role (\`sabele\`) with no prior wife entity needed.*  
*5\. \[Noa requested the exact page number for the vocalized duplicates of ר' יאָכטשֶעֶ to verify before locking this rule\].*

*\#\#\# MishkeMashke (Kultur, 1910\)*  
*1\. **\*\*\[new behavior\]\*\*** Yes. Coin each group as a distinct, individual collective \`xml:id\` (\`dinens\`, \`matrozn\`, \`khinezer\`, \`pasazhirn\`) rather than one single catch-all.*  
*2\. **\*\*\[new behavior\]\*\*** Yes. Keep \`מאשקע\` and \`מישקע\` as completely separate \`xml:ids\` with tight speaker-string matching (no fuzzy matching).*  
*3\. According to our analysis, this specific bare case does not occur in the text, so this scenario is not applicable.*  
*4\. Yes. Add the possessive form \`שאַרלאָטאַ'ס\` as a \`prefix\_*variant\` of \`שארלאטא\` so the speaker matcher routes it correctly.  
5\. Yes. Confirmed that \`מאשקע\` in this context is a gendered male character ("her uncle").

\#\#\# Yudale der blinder (Emkroyt, 1908\)  
1+2. **\*\*\[new behavior\]\*\*** Yes. Encode braced siblings as individual \`castItem\` elements sharing the same family/group \`roleDesc\` (e.g., \`זיינע קינדער\` and \`זייערע קינדער\`).  
3\. Note that the original line is not a merged phrase; it is an enumeration ("קאָהר, געסטע..."). Coin separate \`xml:ids\` for all individual collective tokens.  
4\. **\*\*\[new behavior\]\*\*** The entire phrase \`זיינעפפע (בלינד)\` is treated as the permanent \`roleDesc\`.  
5+6. Both the parenthetical \`(אויגען דאָקטער)\` and the modifier \`פּראָצענטניק\` function as titles/roleDesc, not as proper names.

\#\#\# דאס יידישע קינד (Dos Yudishe Kind)  
1\. **\*\*\[new behavior\]\*\*** No. Do not strip or alter the double oblique hyphen (\`⸗\`) in the bare form; keep it verbatim as printed.  
2\. Leave the \`roleDesc\` empty for \`שׁמֶערְל\` and \`שׁפרִינְצֶע\` for now; do not infer context.  
3\. Yes. Coin two separate collective tags (\`yudn\`, \`grafn\`).  
4\. **\*\*\[new behavior\]\*\*** Do not strip the indefinite article \`אַ\` from the bare form; keep it in the form and treat them as individual roles.  
5\. Yes. Body pages using \`דער קעניג\` or \`דער מייסטער\` should automatically route to these respective \`xml:ids\`.  
6\. Yes. Keep the full \`roleDesc\` verbatim, including the cross-role reference (\`בּײַ גראַף פּעטעפי\`).

\---

\#\#\# Global Patterns (Cross-Play Applied Behaviors)

A. **\*\*\[new behavior\] Setting Lines:\*\*** Always tag \`אָרט דער האַנדלונג...\` / \`דיא געשיכטע האנדעלט זיך...\` lines as \`stage{type:setting}\`, never as a \`castItem\`.  
B. **\*\*\[new behavior\] Shelfmarks:\*\*** Always leave library shelfmarks and BN catalog IDs completely untagged.  
C. **\*\*\[new behavior\] Braced Siblings:\*\*** Always split brace-groups into N individual sibling \`castItem\` tags sharing one joint \`roleDesc\`.  
D. **\*\*\[new behavior\] Indefinite Articles:\*\*** No. Never strip the article \`אַ\` / \`א\` from the bare form.  
E. **\*\*\[new behavior\] Collective Lists:\*\*** Yes. Create a collective \`xml:id\` per comma-separated token. Do not delete literary "etc." markers (\`עטצ.\`, \`אַ. זַ. וו.\`), but do not tag them as characters/roles.

