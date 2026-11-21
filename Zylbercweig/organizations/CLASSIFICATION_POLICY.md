# Organization-Type Classification Policy

This document is the canonical specification for mapping the free-text `org_type` values produced by LLM extraction into a fixed canonical typology. It is the source of truth for the rule cascade — now `map_canonical_types_v3.py` — and for `llm_review_sample.py` (LLM prompt). Update both when this doc changes.

> **Consumers to check when the list changes.** The live cascade is `map_canonical_types_v3.py`; `map_canonical_types_v2.py` is superseded and referenced only by docs. The app's dropdowns are `_ORG_TYPE_OPTIONS` in **both** `zalmen/views/org_review.py` and `zalmen/views/org_addresses.py`, and `org_review.py` also holds `_ORG_TYPE_CANON` / `_canonical_org_type()`, the casefold→canonical-spelling map that keeps a stored value from being re-lowercased on save. `map_canonical_types_v2.py` does **not** know `Non-Yiddish Theatre`; v2 is dead. `llm_review_sample.py`'s prompt learned it on 2026-11-01.

## Canonical types (35)

The list below is exhaustive. Any classification not on this list is invalid.

Types are matched **exactly**, including case and internal spacing. A value that differs only in case is a defect, not a variant: on 2026-10-25 some 2,096 rows carried lowercase spellings (`traveling company` ×575, `education` ×221, …) because the app's type selectboxes lowercased the stored value before matching it against this list, then wrote the lowercased form back. Repaired in `5d8fa3f4d` and `4f88074d8`.

### Theatre & performance
1. **Theatre** — an established theatre institution with a venue (or named theatre house). Implicitly a *Yiddish* theatre; see 1a.
1a. **Non-Yiddish Theatre** — added 2026-06-14 as a sibling of **Theatre**, not a subtype. Latvian / Russian / Ukrainian / Polish / Belarusian / Romanian national and state theatres that Yiddish actors passed *through*. They belong in the DB because they carry biographical mentions, but must never be aligned to a Yiddish state theatre. Motivation: the melukha audit found ~44 such entries. **Scope: theatres only** — publishers, conservatories, schools and philharmonics with the same problem keep their existing non-Theatre types. Auto-applied by Rule 4's ethnonym pass (a `Theatre` cluster whose name carries a national-language token — רוסישער / לעטיש / אוקראיניש / פוילישער / ווייסרוסיש / ראומעניש — plus `מלוכה-טעאַטער`). That check must run **before** the GOSET short-form align, or a non-Yiddish state theatre gets mis-aligned to the Moscow/Kiev Yiddish State Theatre; Maly Teatr was caught doing exactly that in an early pass.
2. **Traveling Company** — a company whose core operational identity is touring (no permanent home stage). Yiddish theatre troupes named "X's troupe" / "Vilner Trupe" / "Lodzer Trupe" etc. fall here.
3. **Company on Tour** — a company that has a home venue and is *currently* on tour. Touring is incidental, not constitutive. Use only when the text makes this explicit.
4. **Amateur** — a *performing* group of non-professionals: drama circles (`דראַמקרייז`), dramatic sections of a body (`דראַמ.-סעקציע`), `ליבהאָבער` groups, dramatic clubs (`דראַמאַטיק קלאָב`), and a `דראַמאַטישע געזעלשאַפֿט` / `פֿאַראיין` / club that **stages its own plays** (Rule 4, 2026-11-15). Members may learn by doing; "learned to act in the circle" does not make it a school. A "studio" that is only a club's performing group, with no pupils or teachers in the text, is Amateur (Rule 2 exception — e.g. db 3735, 4360). **A circle *at* a host** (`ביים X`, `ביי דער X`, `פֿון X`) is its own record with `parent_db_id` = the host; the host keeps its own type and is never typed Amateur because it had a drama circle (Rule 3). **Caveats:** `ליבהאָבער-ראָלן` means *lover roles* (amoureux), not amateur roles (db 2278 was a professional troupe); `קונסט-ליבהאָבער` / `מוזיק-ליבהאָבער` are art/music *lovers* (patrons); a troupe that tours as professionals goes to a company type (Rule 5). Bare labels ("דראַמאַטישן קרייז", "גרופּע ליבהאָבער") that collect unrelated groups are GENERIC (Rule 6). See `theatre_education_amateur_audit/`.
5. **Kleinkunst** — vaudeville, cabaret, variety theatre.
6. **Circus** — circus troupes and circus venues.
7. **Theatre education** — drama schools, theatre studios, acting studios, drama academies. The studio/institute is for *training performers*, not performing. **Rule 1 (2026-11-15):** any school, studio, course or institute that trains actors, directors or stage performers is Theatre education, **not Education** — including music-drama schools and drama classes at a conservatory (96 drama schools moved out of Education that day). Student productions do not change the type. **Rule 2:** a *studio* is Theatre education by default, because the word means pupils and teachers; exceptions: (a) a performing-only "studio" with no pupils or teachers in the text → Amateur; (b) when a studio becomes a company (Granovsky's studio → GOSET) there are **two institutions, as with Gimpel** — the studio (Theatre education) and the theatre it became (typed by its own evidence), linked by a dated "became" relation, each mention going to one of them by date and content. Children's theatre studios are Theatre education (Mendl Elkin's, db 331). Dance/ballet schools: **open question** (db 1319 stays Education until decided). Not Theatre education: art/painting/sculpture studios (→ Education), film studios (→ Media), studio journals (→ Journals).

### Publishing / Print / Media
8. **Publisher** — book publishers, lexicons, monographs, almanacs.
9. **Printer** — print shops (`דרוקעריי`), printers as printing trade.
10. **Printer/Publisher** — combined operation.
11. **Journals/ Newspapers** — newspapers, magazines, periodicals, miscellanies that are serial publications.
12. **Media (Radio/ Film/TV)** — radio stations, film companies, film studios, cinema studios, TV stations, broadcasters, news agencies, film publishers.

### Knowledge / Memory institutions
13. **Library** — public, communal, or institutional libraries proper.
14. **Heritage Institution** — archives, museums, galleries (when functioning as museum/exhibition space), Yiddish cultural-research institutes (YIVO, Kultur-Lige, Sholem-Aleichem Institute, IKUF, Leivick House), exhibitions when treated as standing institutions.
15. **Education** — schools, universities, gymnasia, non-theatre research institutes, children's educational camps (Camp Boiberik, Camp Kinderland); adult vacation camps (Lakeland, Nit Gedayget, Unity) are **Hospitality** (2026-11-21), youth-educational orgs (when primarily educational). **Drama training is not Education** (→ Theatre education, Rule 1). General schools stay Education even when their pupils put on a play (the Zhitomir Rabbinical School, Workmen's Circle schools, the Peretz school, the Vilna teachers' seminary); so do art and sculpture schools (Bezalel, the Kiev art school, Exter's studio) and conservatories without a drama programme. A drama circle *at* a school is a child record (Rule 3), not a reason to retype the school.

### Music
16. **Musical organization** — orchestras, choirs (secular), choral societies, music ensembles, music associations (`מוזיק-פאַראיין`, `געזאַנגס-פאַריין`), `Hazomir`/`הזמיר`, philharmonics, bands. **Excludes** synagogue choirs (→ Religious).

### Theatre-industry membership bodies
17. **Theatre-related Society/ Union** *(renamed from Society/Union)* — bodies organized around the theatre/arts industry: actors unions, artists unions, writers unions, theatre associations, theatre societies, theatre committees, theatrical alliances. **A dramatic society that stages its own plays is Amateur, not this type** (Rule 4, 2026-11-15): a society belongs here only when it supports, coordinates or represents others — a central office for drama circles, a patrons' society, an actors' union. If the text shows the members were paid professionals, it goes to a company type. **Trigger word "Society" or "Union" alone does NOT qualify** — must have theatre/arts/writers/musicians cue.

### Sports and recreation *(new — added 2026-05-12 by PI decision)*
30. **Sports/Recreation** — Jewish (and general) sports clubs, athletic associations, gymnastics and physical-culture clubs, sports federations. Examples: **Maccabi** (`מכבי`) and all Maccabi branches (Kovner Maccabi, etc.), other Jewish sports organizations. Maccabi was previously routed to Education with a PI-dilemma flag because it has a triple identity (sports + Zionist + youth-education); PI chose to acknowledge the sports identity as primary with its own canonical type.

### Fraternal orders *(new — added 2026-05-12 by PI decision)*
31. **Fraternal order** — Lodges, fraternal/benevolent orders, mutual-aid fraternal societies that are NOT specifically Jewish-labour-political (those go to **Jewish political bodies** as the Arbeter Ring class). Examples: Knights of Pythias (`נייטס אָוו פּיטיס`), Masonic lodges (`מעסאניק לאָדזש`), B'nai B'rith, Independent Order Brith Abraham. Previously routed to Welfare/Aid with a PI-dilemma flag; PI chose to give fraternal orders their own type since their primary identity is fraternal-ceremonial-social, not welfare per se.
    - **Note (PI 2026-05-12)**: **Grand Street Boys** (`גרענד סטריט-באָיס`) is an *informal street-based youth association*, not a formal fraternal order. Route to **Education** instead.

### Cultural and workers' clubs *(new — added 2026-11-15 by PI decision)*
33. **Cultural/workers' club** — a general culture club, workers' club, culture house or culture palace, or culture society (`קולטור-קלוב`, `אַרבעטער-קלוב`, `קולטור-הויז`, `קולטור-פּאַלאַץ`, `קולטור-פֿאַראיין`, `פֿאָלקסהויז`, Perets-fareyn, literary club) whose identity is a social-cultural centre with several activities (lectures, choir, library, drama circle). Examples: the An-ski culture club (914), the Zhitomir/Kharkov/Minsk workers' clubs, Spartak in Buenos Aires, the metalworkers' club named after Tomsky, Bildung in Kraków. **It is a host**: its drama circle or section is a separate **Amateur** child (Rule 3), never a reason to type the club Amateur. **Not this type:** an American *dramatic club* (`דראַמאַטיק קלאָב` — Kessler, Gordin, Progressive Dramatic Club), which is the performing group itself → **Amateur** (Sinai 2026-11-15: "club" as an Amateur subtype means these, not the hosting clubs); a union's own local → Trade Union; a party branch → Jewish political bodies; Kultur-Lige-type cultural-research institutes → Heritage Institution.

### General trade unions and professional associations *(added 2026-05-12 after Gemini verification surfaced 80+ rows with no canonical home)*
28. **Trade Union / Professional Association** *(added 2026-05-12)* — occupational unions, trade unions, professional associations, employer associations, scientific/professional societies, guilds, and chambers that are NOT theatre-industry. Examples: printers' unions, garment-trade unions, cigarmakers' union, tailors' union, fur workers union, shoe workers union, butchers union (Yiddish: `געווערקשאַפט`, `דרוקער-פאַריין`, `אַרבעטער-פֿאַריין` when context is a specific trade), State Bar of Michigan and other bar associations, gardeners' associations, handelskammer (chambers of commerce — note: when explicitly a *business* chamber, Business also acceptable), professional scientific societies, employer trade associations.
    - **Excludes** theatre/arts unions (those go to **Theatre-related Society/ Union**).
    - **Excludes** workers' fraternal-political organizations like Arbeter Ring, Arbeter-Farband, Bund (those are **Jewish political bodies** with PI dilemma flag).
    - **Excludes** factories themselves (those are **Labour (factory/workshop)**).
    - The line between this and Theatre-related Society/ Union is the *trade* identity: a printers' union and a stage-actors' union are both unions, but the printers' union is a Trade Union / Professional Association, while the stage-actors' union is Theatre-related Society/ Union.

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
22. **Business** — **commerce and services** with a profit motive: banks, insurance companies, shops, stores, trading companies, law firms, telegraph and telephone companies, railways, financial corporations, galleries (when commercial art-dealer), pharmacies, publicity bureaus. Hotels, restaurants, cafés, taverns and vacation camps → **Hospitality**; theatre and concert bureaus → **Theatre/concert agency** (both 2026-11-21).

### Hospitality and agencies *(new — added 2026-11-21 by PI decision)*
34. **Hospitality** — hotels, resorts, taverns, cafés, restaurants, wine cellars, and adult vacation camps (Lakeland, Nit Gedayget, Unity; Unzer Kemp, 4946, parent Farband). A venue where troupes played keeps this type; the performances are carried by the mention. Children's educational camps stay **Education**.
35. **Theatre/concert agency** — theatre bureaus, concert bureaus, booking and impresario agencies that place performers (`טעאַטער-ביוראָ`, `קאָנצערט-ביוראָ`, `טעאַטער-אַגענטור`). A publicity bureau is **Business**; a manager's touring company is a company type.
23. **Labour (factory/workshop)** *(renamed from Labour)* — **places of production**: factories, mills, sweatshops, workshops, breweries, bakeries, tailor shops, farms and orchards, printing works when not typed Printer. A manufacturing *firm* is typed here by what it makes, even when the text calls it a פֿירמע. **Excludes** labour movements and workers' fraternal/political organizations (those go to Jewish political bodies or Non-Jewish political bodies).

> **The type describes the organization, never a person's relation to it** *(PI 2026-11-01)*. Until then a brewery was Business "when ownership-relation" and Labour "when employment-relation", which would split one brewery into two rows as soon as it was mentioned both ways. Whether someone owned, managed or worked at a place is carried by the **relation** on the mention (`Leadership_Ownership`, `Employment_Performance`, …) and so by the edge in the person–org graph and the KG — the org type does not repeat it. Production site → Labour; commerce and services → Business.
24. **Health institutions** — hospitals, clinics, sanatoria, medical institutions, infirmaries. Red Cross / רויטן קרייץ falls here.

### Military
25. **Military** — armies, military units, military organizations, self-defense organizations (Jewish Legion, Jewish self-defense / יידישן זעלבסטשוץ), partisans. Examples: Polish Army / פּוילישער אַרמיי, Red Army / רויטער אַרמעע, American Army / אַמעריקאַנער אַרמעע.

### Sentinels
26. **Not an organization** — places mis-tagged as orgs: ghettos, concentration camps, labor camps, refugee camps, residences, parks (Seaside Park, etc.), colonies, geographic regions, books and lexicons (when the row really refers to a publication rather than the publisher).
27. **OTHER - elaborate!** — fallback for entities that genuinely fit no canonical type. Must include reviewer notes describing what the entity is.

---

## Disambiguation rules (the hard cases)

### Tag `union` / `Union` / `יוניע` / `פאַריין`
- Theatre / arts / writers / musicians cue in name or sentence → **Theatre-related Society/ Union**
- Factory / sweatshop / workers / printers / tailors / bakers context → **Labour (factory/workshop)** *only if it is a place; if it is a movement → political*. But trade unions like "Vilner Drukers-Farein" — these are occupational unions, not factories. Route to **Theatre-related Society/ Union** only if theatre context; otherwise flag (the canonical doesn't have a "general trade union" bucket — PI decision).
- Jewish labour-political fraternal: Arbeter Ring, Arbeter-Farband, Bund — **Jewish political bodies + flag for PI**.
- Otherwise unresolved → default **Theatre-related Society/ Union** with context_weak flag (Zalmen is a theatre lexicon — theatre-relatedness is the prior).

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
- Theatre / writers cue → **Theatre-related Society/ Union**.
- Religious cue → **Religious institutions/organizations**.
- Zionist / political-Jewish cue → **Jewish political bodies**.
- Political (non-Jewish) cue → **Non-Jewish political bodies**.
- Music cue → **Musical organization**.
- Welfare / aid / philanthropic cue (aid, relief, hilf, philanthropic, charity, welfare, mutual aid, immigrant) → **Welfare/Aid organization**.
- Default unresolved → **Theatre-related Society/ Union** (Zalmen prior) with context_weak flag.

### Tag `organization` / `org` / `אָרגאַניזאַציע`
- Apply the same rule cascade as `society`, but default unresolved → **OTHER - elaborate!** with flag (organization is too generic to assume theatre-relatedness without any cue).

### Tag `synagogue choir`
- **Religious institutions/organizations** (per canon definition: religious choirs are religious orgs, not musical orgs).

### `Camp`, `concentration camp`, `labor camp`, `refugee_camp`
- **Not an organization** (places).
- Exception: **children's educational camps** (Camp Boiberik, Camp Kinderland — Yiddish summer camps with a cultural-educational mission) → **Education**; **adult vacation camps** (Lakeland, Nit Gedayget, Unity) → **Hospitality** (2026-11-21).

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
- `context_weak:union_default_society` — union with no theatre/labour cue, defaulted to Theatre-related Society/ Union.
- `context_weak:company_no_cue` — company with no theatre/film/business cue, defaulted to Business.
- `context_weak:society_default_theatre` — society/club/etc. with no cue, defaulted to Theatre-related Society/ Union.
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
