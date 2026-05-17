# Organization-Type Classification Policy

This document is the canonical specification for mapping the free-text `org_type` values produced by LLM extraction into a fixed canonical typology. It is the source of truth for `map_canonical_types_v2.py` (keyword rules) and `llm_review_sample.py` (LLM prompt). Update both when this doc changes.

## Canonical types (31)

The list below is exhaustive. Any classification not on this list is invalid.

### Theatre & performance
1. **Theatre** — an established theatre institution with a venue (or named theatre house).
2. **Traveling Company** — a company whose core operational identity is touring (no permanent home stage). Yiddish theatre troupes named "X's troupe" / "Vilner Trupe" / "Lodzer Trupe" etc. fall here.
3. **Company on Tour** — a company that has a home venue and is *currently* on tour. Touring is incidental, not constitutive. Use only when the text makes this explicit.
4. **Amateur** — amateur theatre groups, drama circles, theatre clubs, union drama circles.
5. **Kleinkunst** — vaudeville, cabaret, variety theatre.
6. **Circus** — circus troupes and circus venues.
7. **Theatre education** — drama schools, theatre studios, acting studios, drama academies. The studio/institute is for *training performers*, not performing.

### Publishing / Print / Media
8. **Publisher** — book publishers, lexicons, monographs, almanacs.
9. **Printer** — print shops (`דרוקעריי`), printers as printing trade.
10. **Printer/Publisher** — combined operation.
11. **Journals/ Newspapers** — newspapers, magazines, periodicals, miscellanies that are serial publications.
12. **Media (Radio/ Film/TV)** — radio stations, film companies, film studios, cinema studios, TV stations, broadcasters, news agencies, film publishers.

### Knowledge / Memory institutions
13. **Library** — public, communal, or institutional libraries proper.
14. **Heritage Institution** — archives, museums, galleries (when functioning as museum/exhibition space), Yiddish cultural-research institutes (YIVO, Kultur-Lige, Sholem-Aleichem Institute, IKUF, Leivick House), exhibitions when treated as standing institutions.
15. **Education** — schools, universities, gymnasia, non-theatre research institutes, secular educational/recreational camps (Camp Boiberik, Camp Kinderland, Camp Lakeland), youth-educational orgs (when primarily educational).

### Music
16. **Musical organization** — orchestras, choirs (secular), choral societies, music ensembles, music associations (`מוזיק-פאַראיין`, `געזאַנגס-פאַריין`), `Hazomir`/`הזמיר`, philharmonics, bands. **Excludes** synagogue choirs (→ Religious).

### Theatre-industry membership bodies
17. **Theatre-related Society/Union** *(renamed from Society/Union)* — bodies organized around the theatre/arts industry: actors unions, artists unions, writers unions, theatre associations, theatre societies, dramatic societies, theatre committees, theatrical alliances. **Trigger word "Society" or "Union" alone does NOT qualify** — must have theatre/arts/writers/musicians cue.

### Sports and recreation *(new — added 2026-05-12 by PI decision)*
30. **Sports/Recreation** — Jewish (and general) sports clubs, athletic associations, gymnastics and physical-culture clubs, sports federations. Examples: **Maccabi** (`מכבי`) and all Maccabi branches (Kovner Maccabi, etc.), other Jewish sports organizations. Maccabi was previously routed to Education with a PI-dilemma flag because it has a triple identity (sports + Zionist + youth-education); PI chose to acknowledge the sports identity as primary with its own canonical type.

### Fraternal orders *(new — added 2026-05-12 by PI decision)*
31. **Fraternal order** — Lodges, fraternal/benevolent orders, mutual-aid fraternal societies that are NOT specifically Jewish-labour-political (those go to **Jewish political bodies** as the Arbeter Ring class). Examples: Knights of Pythias (`נייטס אָוו פּיטיס`), Masonic lodges (`מעסאניק לאָדזש`), B'nai B'rith, Independent Order Brith Abraham. Previously routed to Welfare/Aid with a PI-dilemma flag; PI chose to give fraternal orders their own type since their primary identity is fraternal-ceremonial-social, not welfare per se.
    - **Note (PI 2026-05-12)**: **Grand Street Boys** (`גרענד סטריט-באָיס`) is an *informal street-based youth association*, not a formal fraternal order. Route to **Education** instead.

### General trade unions and professional associations *(added 2026-05-12 after Gemini verification surfaced 80+ rows with no canonical home)*
28. **Trade Union / Professional Association** *(added 2026-05-12)* — occupational unions, trade unions, professional associations, employer associations, scientific/professional societies, guilds, and chambers that are NOT theatre-industry. Examples: printers' unions, garment-trade unions, cigarmakers' union, tailors' union, fur workers union, shoe workers union, butchers union (Yiddish: `געווערקשאַפט`, `דרוקער-פאַריין`, `אַרבעטער-פֿאַריין` when context is a specific trade), State Bar of Michigan and other bar associations, gardeners' associations, handelskammer (chambers of commerce — note: when explicitly a *business* chamber, Business also acceptable), professional scientific societies, employer trade associations.
    - **Excludes** theatre/arts unions (those go to **Theatre-related Society/ Union**).
    - **Excludes** workers' fraternal-political organizations like Arbeter Ring, Arbeter-Farband, Bund (those are **Jewish political bodies** with PI dilemma flag).
    - **Excludes** factories themselves (those are **Labour (factory/workshop)**).
    - The line between this and Theatre-related Society/Union is the *trade* identity: a printers' union and a stage-actors' union are both unions, but the printers' union is a Trade Union / Professional Association, while the stage-actors' union is Theatre-related Society/Union.

### Religion
18. **Religious institutions/organizations** — synagogues, yeshivot, hasidic courts, religious choirs, churches, mosques, religious courts, kehiloth (when functioning as religious-communal body).

### Political bodies
19. **Jewish political bodies** — Jewish national / Zionist / Bundist / Labor-Zionist / religious-political organizations and the funds/foundations that operate as their political arms. Includes:
    - Zionist parties & movements: Poale Zion, Mizrachi, Hashomer Hatzair, Hatzohar/Revisionist, Agudath Israel, etc.
    - Jewish socialist/labour-political bodies: Bund, Algemeyner Yidisher Arbeter Bund, Jewish socialist territorialists.
    - Israeli political bodies (post-1948): Knesset, Israeli political parties, Histadrut (labour federation that is fundamentally political).
    - Zionist funds & foundations: Keren Hayesod, Keren Kayemet / Jewish National Fund / נאַציאָנאַל-פֿאָנד, Israel Bonds, יידישן נאַציאָנאַל פאַנד, etc.
    - Jewish national congresses & world bodies: World Jewish Congress, Yiddish Cultural Congress (when political-organizational, not cultural society).
    - Workmen's fraternal-political societies *when their primary identity is political-ideological*: Arbeter Ring / Workmen's Circle, Yidish-Natsionaler Arbeter-Farband. **Always flag for PI** (these are dual-identity organizations — cultural-fraternal AND labour-political).
    - São Paulo Yiddish Society (`סאַן פּאָולאַ יידישער געזעלשאַפט`) — PI assigned 2026-05-12.

29. **Judenrat** *(new — added 2026-05-12 by PI decision)* — Jewish councils imposed by Nazi authorities in occupied territories, 1939–1945. Given their own canonical type rather than collapsing into Jewish political bodies, reflecting their distinct historical character (coerced administrative bodies operating under occupation). Includes: `יודענראַט`, `יידנראָט`, `Warsaw Judenrat`, `Łódź Judenrat`, branches of these.
20. **Non-Jewish political bodies** *(renamed from Political bodies)* — non-Jewish governments, parliaments, parties, councils, ministries, courts, commissariats, embassies, congresses, conferences, executive committees, municipal councils. Examples: Polish Sejm, Soviet commissariats, US Department of X, Communist Party of Poland (multi-ethnic), Polish Socialist Party.

### Welfare & mutual aid *(new)*
21. **Welfare/Aid organization** *(new)* — Jewish and general welfare, philanthropic, mutual aid, immigration aid, relief, philanthropic societies, communal welfare bodies. Examples: HIAS / האַיאַס / היאָס, JDC / דזשאינט, ORT / אָרט, UNRRA / אונראַ, UJA / United Jewish Appeal, Jewish Welfare Board, Hadassah, WIZO, ADL, mutual aid burial societies (חסד של אמת), old age homes (מושב זקנים), social self-help organizations (יידישער סאָציאַלער אַליינהילף, יידישער אַליינהילף), refugee aid, settlement houses.
    - **WIZO and Hadassah** lean welfare (women's welfare/education + hospitals) rather than political-Zionist; route here, not Jewish political.
    - **UJA / Federation** = welfare fundraising → here, not Jewish political.

### Business & labour & health
22. **Business** — commercial enterprises with a profit motive: banks, insurance companies, hotels, restaurants, cafés, saloons, taverns, shops, stores, firms, law firms, brewery (when ownership-relation), trading companies, manufacturing companies, motor companies, telegraph companies, financial corporations, booking offices, galleries (when commercial art-dealer), pharmacies.
23. **Labour (factory/workshop)** *(renamed from Labour)* — **places of physical labour**. Factories, sweatshops, workshops, breweries (when employment-relation), tailor shops, bakeries, workplaces. **Excludes** labour movements and workers' fraternal/political organizations (those go to Jewish political bodies or Non-Jewish political bodies).
24. **Health institutions** — hospitals, clinics, sanatoria, medical institutions, infirmaries. Red Cross / רויטן קרייץ falls here.

### Military
25. **Military** — armies, military units, military organizations, self-defense organizations (Jewish Legion, Jewish self-defense / יידישן זעלבסטשוץ), partisans. Examples: Polish Army / פּוילישער אַרמיי, Red Army / רויטער אַרמעע, American Army / אַמעריקאַנער אַרמעע.

### Sentinels
26. **Not an organization** — places mis-tagged as orgs: ghettos, concentration camps, labor camps, refugee camps, residences, parks (Seaside Park, etc.), colonies, geographic regions, books and lexicons (when the row really refers to a publication rather than the publisher).
27. **OTHER - elaborate!** — fallback for entities that genuinely fit no canonical type. Must include reviewer notes describing what the entity is.

---

## Disambiguation rules (the hard cases)

### Tag `union` / `Union` / `יוניע` / `פאַריין`
- Theatre / arts / writers / musicians cue in name or sentence → **Theatre-related Society/Union**
- Factory / sweatshop / workers / printers / tailors / bakers context → **Labour (factory/workshop)** *only if it is a place; if it is a movement → political*. But trade unions like "Vilner Drukers-Farein" — these are occupational unions, not factories. Route to **Theatre-related Society/Union** only if theatre context; otherwise flag (the canonical doesn't have a "general trade union" bucket — PI decision).
- Jewish labour-political fraternal: Arbeter Ring, Arbeter-Farband, Bund — **Jewish political bodies + flag for PI**.
- Otherwise unresolved → default **Theatre-related Society/Union** with context_weak flag (Zalmen is a theatre lexicon — theatre-relatedness is the prior).

### Tag `company`
- Theatre / drama / troupe / players / ensemble cue → **Traveling Company**.
- Film / cinema / pictures / radio cue → **Media (Radio/ Film/TV)**.
- Business markers (`& Co`, `Inc`, `Ltd`, `Corp`, `קאָמפּ`, `קאָמפּאַני`, `קאָרפּאָר`, `פֿירמע`, `אינשורענס`, `באַנק`, `האָטעל`, etc.) → **Business**.
- Otherwise → **Business** (most "X company" mentions in this lexicon are commercial), flagged.

### Tag `brewery` / `factory` / `workshop` / `bakery` / `tailor shop` / `farm` / `shop` / `store` / `firm`
Use the relation column (mention-level only):
- `Leadership_Ownership` → **Business**
- `Employment_Performance` → **Labour (factory/workshop)**
- `Production_Distribution` → **Business** (likely a supplier)
- Other / missing → flag for review
At cluster/DB level, the same physical entity can carry both labels across different mentions. **Until the PI decides the schema policy (one-type-per-entity vs. relation-typed labels), default to majority-relation at the cluster level and flag conflicts.**

### Tag `society` / `געזעלשאַפט` / `club` / `association` / `community`
- Theatre / writers cue → **Theatre-related Society/Union**.
- Religious cue → **Religious institutions/organizations**.
- Zionist / political-Jewish cue → **Jewish political bodies**.
- Political (non-Jewish) cue → **Non-Jewish political bodies**.
- Music cue → **Musical organization**.
- Welfare / aid / philanthropic cue (aid, relief, hilf, philanthropic, charity, welfare, mutual aid, immigrant) → **Welfare/Aid organization**.
- Default unresolved → **Theatre-related Society/Union** (Zalmen prior) with context_weak flag.

### Tag `organization` / `org` / `אָרגאַניזאַציע`
- Apply the same rule cascade as `society`, but default unresolved → **OTHER - elaborate!** with flag (organization is too generic to assume theatre-relatedness without any cue).

### Tag `synagogue choir`
- **Religious institutions/organizations** (per canon definition: religious choirs are religious orgs, not musical orgs).

### `Camp`, `concentration camp`, `labor camp`, `refugee_camp`
- **Not an organization** (places).
- Exception: **Educational/recreational camps** (Camp Boiberik, Camp Kinderland — Yiddish summer camps with cultural-educational mission) → **Education**.

### `Park`, `colony`, `ghetto`, `residence`, `home` (when meaning dwelling)
- **Not an organization**.

### Specific named entities (always)
- **Judenrat / יודענראַט / יידנראָט** → **Jewish political bodies + flag PI** (`pi_dilemma:judenrat`). Reason: Nazi-imposed Jewish administrative councils; historiographically contested whether to treat as legitimate Jewish self-administration or coerced collaboration. PI to settle the policy.
- **Arbeter Ring / Workmen's Circle / אַרבעטער-רינג** → **Jewish political bodies + flag PI** (`pi_dilemma:fraternal_political_dual_identity`). Reason: simultaneously a workers' fraternal-mutual-aid society, a Yiddish-socialist political-cultural movement, and a sponsor of Yiddish theatres/choirs/schools. No single bucket captures all three.
- **Yidish-Natsionaler Arbeter-Farband / יידיש-נאַציאָנאַלן אַרבעטער-פֿאַרבאַנד** → **Jewish political bodies + flag PI** (`pi_dilemma:fraternal_political_dual_identity`). Reason: Labor-Zionist fraternal-mutual-aid; same dual identity as Arbeter Ring.
- **HIAS, JDC, ORT, UNRRA, UJA, ADL** → **Welfare/Aid organization** (no flag).
- **Hadassah / הדסה** → **Welfare/Aid organization + flag PI** (`pi_dilemma:zionist_welfare_dual_identity`). Reason: Women's Zionist Organization of America. Primary outputs are hospitals + welfare (Welfare/Aid fits), but founding identity is Zionist (Jewish political also defensible). PI to choose.
- **WIZO / וויצאָ** → **Welfare/Aid organization + flag PI** (`pi_dilemma:zionist_welfare_dual_identity`). Reason: Women's International Zionist Organization. Same dual identity as Hadassah — Zionist political + welfare/education/health.
- **Keren Hayesod, JNF / Keren Kayemet / נאַציאָנאַל-פֿאָנד, Israel Bonds, Histadrut** → **Jewish political bodies**.
- **YIVO, Kultur-Lige, IKUF, Sholem-Aleichem Institute, Leivick House** → **Heritage Institution**.
- **Maccabi / מכבי** → **Education + flag PI** (`pi_dilemma:sports_zionist_youth_triple_identity`). Reason: Jewish-Zionist sports-and-youth movement. Sports (no canonical category) + Zionist political ideology + youth education. Default routes to Education for the youth-education aspect, but the canonical typology may need a new `Sports/Recreation` type if many rows accumulate.
- **Polish Army, Red Army, American Army, Jewish Legion, Jewish Self-defense** → **Military**.
- **Red Cross / רויטן קרייץ** → **Health institutions**.
- **Hazomir / הזמיר** → **Musical organization**.

### Generic `lodge` / `fraternal`
- Always **Welfare/Aid + flag PI** (`pi_dilemma:fraternal_welfare_or_other`). Reason: Jewish fraternal orders (B'nai B'rith branches, Independent Order Brith Abraham, etc.) blended mutual aid, social/ceremonial activity, and sometimes political functions. Need PI judgement per case.

---

## Flags

Every flagged row in the output mapping TSVs must include a `review_reason` value. Standard values:

- `pi_dilemma:fraternal_political_dual_identity` — Arbeter Ring class (cultural-fraternal + labour-political + theatre-patron).
- `pi_dilemma:judenrat` — Judenrats (Nazi-imposed Jewish councils).
- `pi_dilemma:zionist_welfare_dual_identity` — Hadassah, WIZO (Zionist + welfare/health).
- `pi_dilemma:sports_zionist_youth_triple_identity` — Maccabi (sports + Zionist + youth-education; missing Sports category).
- `pi_dilemma:fraternal_welfare_or_other` — generic lodges/fraternal orders.
- `pi_dilemma:trade_union_no_theatre_cue` — generic trade unions (printers/tailors/etc.) with no theatre cue.
- `pi_dilemma:brewery_relation_conflict` — same entity Business in one mention, Labour in another.
- `pi_dilemma:ostrovski_institute_theatre_or_general` — core_db row 484: Ostrowski Institute, unclear if theatre-focused.
- `pi_dilemma:vilna_printers_union_trade_or_theatre` — core_db row 508: not theatre but doesn't fit Labour either.
- `pi_dilemma:sao_paulo_yiddish_society_scope` — core_db row 493: society scope unclear.
- `context_weak:union_default_society` — union with no theatre/labour cue, defaulted to Theatre-related Society/Union.
- `context_weak:company_no_cue` — company with no theatre/film/business cue, defaulted to Business.
- `context_weak:society_default_theatre` — society/club/etc. with no cue, defaulted to Theatre-related Society/Union.
- `context_weak:institute_default_education` — institute with no theatre/research cue, defaulted to Education.
- `context_weak:academy_default_education` — academy with no theatre/music cue.
- `context_weak:youth_default_education` — youth_organization with no political cue.
- `context_weak:production_default_business` — production with no theatre/film cue.
- `context_weak:camp_default_not_org` — camp not matched by named-entity rule (could be educational).
- `context_weak:lodge_default_welfare` — superseded by `pi_dilemma:fraternal_welfare_or_other` in v3.
- `unresolved` — Pass B couldn't decide; original value kept.

---

## Out of scope

- Whether to allow per-relation type labels at DB level (current decision: one type per entity, majority-relation, flag conflicts).
- Whether to add `Sports/Recreation` as a new canonical type (insufficient row volume so far).
- Whether to add `Fund/Foundation` as a separate type (subsumed into Jewish political bodies per PI guidance).
