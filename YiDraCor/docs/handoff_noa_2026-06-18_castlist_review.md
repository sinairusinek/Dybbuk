# YiDraCor — castList review for Noa, 2026-06-18

Five newly-bootstrapped Lateiner plays. Each castList page was auto-tagged with role/roleDesc spans applying the Globals A–F (esp. **F**: profession/relation modifiers → `roleDesc` always; fused titles like Reb/Doctor/Professor stay inside `role`). Most decisions were unambiguous and should be fine. **The questions below surface only the genuinely uncertain calls** — please pick A / B / C or write a different answer.

Each play has a Transkribus link to its castList page; you can verify against the image while answering.

---

## 1. BasSheva (page 6) — [Transkribus](https://app.transkribus.org/collection/2372172/doc/828443/page/6)

The castList has 10 named characters (Tsadok, Bas Sheva, Shloymiel, Azarya, Lemekh, Avner, Binyomin, Ester, Tayfel) plus a closing collective enumeration: `שׁעפֿער שׁעפֿעריגען, מיליטער, טייפֿעל, בּוֹיען, עטצ.` ("shepherds, military, devils, builders, etc.").

### Q1.1 — Is `שׁעפֿער שׁעפֿעריגען` one collective or two?

`שעפער` = shepherds (masc.), `שעפעריגען` = shepherdesses (fem.). Currently tagged as **one** collective `xmlid=shefer`.

- **(a)** Keep as one (`shefer`) — Recommended; gendered pair often acts as a single chorus.
- **(b)** Split into two — `shefer` (masc.) + `shefirigen` (fem.).
- **(c)** Other (please specify)

### Q1.2 — `tayfel` (the character on line 10) vs `tayflen` (the collective on line 11)?

The proper name "Tayfel" (a wealthy character "from the gold mines") shares the bare form with the collective noun "devils" (plural). Currently separated as two distinct xmlids: `tayfel` (proper name) and `tayflen` (collective).

- **(a)** Keep separate (different xmlids) — Recommended; they're different entities.
- **(b)** Merge into one `tayfel` (treat the collective as the character's gang).
- **(c)** Other.

---

## 2. HinkePinke (page 4) — [Transkribus](https://app.transkribus.org/collection/2372172/doc/820969/page/4)

12 named characters + closing collective enumeration: `יעֶנעֶר, בּויערעֶן, יוּדעֶן סאָלדאַטען.`

### Q2.1 — `יענער` as a collective?

`יענער` literally means "that" / "those" in Yiddish — could be a demonstrative pronoun rather than a noun. Currently tagged as a collective `xmlid=yener`.

- **(a)** Yes, it's a collective (some group label that just survives un-translated).
- **(b)** Drop it — it's a printing artifact / demonstrative pronoun, not a cast item.
- **(c)** Other.

### Q2.2 — `יוּדעֶן סאָלדאַטען` — one collective or two?

Two ways to read: `יודן סאלדאטן` = "Jews [and] soldiers" (2 collectives), or `יודן סאלדאטן` = "Jewish soldiers" (1 collective, compound noun). Currently tagged as **two** (`yudn` + `soldatn`).

- **(a)** Two separate collectives (yudn + soldatn) — Recommended; matches the pattern of the rest of the list.
- **(b)** One collective `yudn_soldatn` (Jewish soldiers).
- **(c)** Other.

---

## 3. SoreSheyndel (page 7) — [Transkribus](https://app.transkribus.org/collection/2372172/doc/820964/page/7)

10 named characters + a closing line containing both a single character and a collective enumeration.

### Q3.1 — `רֶב יוחנצי דין` — what's the role, what's the roleDesc?

Three plausible readings of "Reb Yontsi/Yokhantsi Din":

- **(a)** role = `רֶב יוחנצי`, roleDesc = `דין` (judge / dayan) — Recommended; "Reb" is a fused title, "Din" is the profession-modifier (Global F).
- **(b)** role = `רֶב יוחנצי דין` (whole as compound proper name, "Reb Yontsi-Din" as surname-style).
- **(c)** role = `יוחנצי`, roleDesc = `רב דין` (both Reb and Din as descriptors).
- **(d)** Other.

### Q3.2 — Brace-group siblings: `אברהמעלע / זייערע קינדער / באבעלע` — siblings of which parents?

This brace structure mirrors the Yudale pattern (children of multiple named parents). In Sore Sheyndel the bracketed children sit between Reb Yontsi + Sore Sheyndel's entry above and Dzeyk's entry below. Currently both Avromele and Babele are tagged with shared roleDesc `זייערע קינדער` ("their children").

- **(a)** Yes, both are Reb Yontsi + Sore Sheyndel's children — Recommended.
- **(b)** No — they're someone else's children (please specify whose).
- **(c)** Other.

### Q3.3 — `שׁאַפּסע שׁמשׁ. משׁוררים אוּן פֿאָלק.` — how many cast items on this line?

Currently tagged as **three** separate role spans on the same line: `shabse` (with roleDesc `שמש`), then collective `mshorerim`, then collective `folk`.

- **(a)** Three (shabse + mshorerim + folk) — Recommended; the period after `שׁמשׁ` cleanly separates Shabse from the collectives.
- **(b)** One character (shabse, with the rest in roleDesc).
- **(c)** Other.

---

## 4. Dovid's Fidele (page 6) — [Transkribus](https://app.transkribus.org/collection/2372172/doc/820845/page/6)

12 named characters, dash-delimited castList. Mostly clean.

### Q4.1 — `דַאוִויד גֵייגֶער` — Geyger as profession or surname?

The play is *Dovid's Fidele* (David's Violin). `Geyger` is Yiddish for "fiddler / violinist". Two readings:

- **(a)** Profession → roleDesc. role = `דַאוִויד`, roleDesc = `גֵייגֶער טוביה'ס ברודער` (Geyger, Toviya's brother) — Recommended per Global F; this is the title character.
- **(b)** Surname (German *Geiger*) → part of role. role = `דַאוִויד גֵייגֶער`, roleDesc = `טוביה'ס ברודער`.
- **(c)** Other.

### Q4.2 — `אִיצֶעלֶע פִּיפֶּעק` — Pipek as surname or descriptor?

`פיפעק` doesn't match a common Yiddish profession word; could be a surname or a humorous nickname. Currently tagged as part of role: `role = אִיצֶעלֶע פִּיפֶּעק`.

- **(a)** Part of role (treat as surname/full-name compound) — Recommended.
- **(b)** Move to roleDesc.
- **(c)** Other.

---

## 5. Dos Yudishe Herts (page 4) — [Transkribus](https://app.transkribus.org/collection/2372172/doc/820841/page/4)

12 named characters + brace group + closing setting line (Global A).

### Q5.1 — `פֿעטער משה` — Feter as fused title or relation-modifier?

"Feter" = "Uncle" in Yiddish. Two readings:

- **(a)** Fused title (like Reb / Doctor / Professor) → stays inside `role`. role = `פֿעטער משה`, roleDesc = `פֿוּן בּיִטשוטש סעֶרקעֶלעֶס בּרוּדעֶר` (from Bytshutsh, Serkele's brother) — Recommended; "Feter Moyshe" appears as a stable name-form throughout the play.
- **(b)** Relation-modifier (like "his nephew", per Global F) → goes in roleDesc. role = `משה`, roleDesc = `פֿעטער פֿוּן בּיִטשוטש סעֶרקעֶלעֶס בּרוּדעֶר`.
- **(c)** Other.

### Q5.2 — `יעקב שטערען אַמאַלעֶר איהר חתן` — boundary?

Three plausible readings:

- **(a)** role = `יעקב שטערען` (Yankev Shtern, surname compound), roleDesc = `אַמאַלעֶר איהר חתן` ("a painter, her fiancé") — Recommended; Shtern is a surname, Maler is a profession.
- **(b)** role = `יעקב`, roleDesc = `שטערען אַמאַלעֶר איהר חתן` (treat Shtern as a descriptor too).
- **(c)** role = `יעקב שטערען`, roleDesc = `אַמאַלעֶר`, and treat `איהר חתן` as ALSO part of role (i.e. two separate roles or hyphenated)?
- **(d)** Other.

### Q5.3 — `למְך אַקְרעֶטשמעֶר` — Kretshmer as profession?

"Kretshmer" = "tavern-keeper". Two readings:

- **(a)** Profession → roleDesc. role = `למְך`, roleDesc = `אַקְרעֶטשמעֶר` — Recommended per Global F.
- **(b)** Bound compound surname-form ("Lemekh the Tavernkeeper" as full name).
- **(c)** Other.

### Q5.4 — Brace group: `וויקטאָר / איהרע קינדער / לידאַ` — siblings of which parent?

Brace label `איהרע קינדער` ("HER children") — but the previous line is Paula Popeska (a wealthy boyar woman). So Viktor + Lida are Paula's children?

- **(a)** Yes, Paula's children — Recommended.
- **(b)** No (please specify whose).
- **(c)** Other.

---

## After Noa returns

If any answer ≠ (a) "Recommended", we'll fix the castList page tagging and regenerate the cast_dict. Then push to Transkribus.

Once castLists are confirmed, body-page annotation can run for each play:
- `auto_annotate` resolves speakers using cast_dict bare forms + prefix_variants.
- LLM annotator types stage directions (with the new TEI multi-token convention).
- `auto_resolve_flags` cleans up + pushes.

Estimated time per play after castList confirmation: ~30 minutes of pipeline + push, then queued for RA review.
