# Merge / Alignment rules — for PI confirmation
Date: 2026-06-14
Sample: 7 core_db merges + 98 ALIGNs + 151 NEWs decided between 2026-06-04 and 2026-06-14 (242 changes in `org_alignment_review.tsv`).

The rules below are inferred from the user/reviewer pattern observed in that window. Each is stated narrowly enough to apply globally as an auto-suggester (still requiring human confirm), with the **trigger** (when to fire), the **evidence** (recent clusters that follow the rule), and the **safety guard** (when NOT to apply).

---

## Rule 1 — Family-name canonical-troupe absorption

**Trigger.** A cluster whose `canonical_yiddish` reduces to a known surname X (any of the surface forms below), and there exists a single non-deprecated core_db row of type `Traveling Company` whose canonical Yiddish is `X-ס טרופּע` / `X Troupe`.

Surface forms that should ALIGN to that row:
- bare surname: `X`, `X-ן`, `X-ס`
- possessive-troupe: `X-ס טרופּע`, `X׳ס טרופּע`, `X's troupe`
- of-troupe: `טרופּע פֿון X`, `טרופּע X`
- regional adjective: `X-ער טרופּע`, `X פּראָווינץ-טרופּע`, `X וואַנדער/וואָנדער-טרופּע`, `X-ער קיעווער/וואַרשעווער/לאָדזשער טרופּע`
- theatre-form (director→theatre): `X-טעאַטער`, `X-ס טעאַטער`
- shows: `X-ס פֿאָרשטעלונגען`
- genre variant: `אָפּערעטן-טרופּע פֿון X`, `יידישער טרופּע פֿון X`
- director-prefixed: `טרופּע פֿון דירעקטאָר X`, `טרופּע פֿון גאַסטראָלירנדיקן X`
- city-shortform: `X אין <city>` (place + person stub)
- spelling variants of X (the spelling cascade we already use for fuzzy match)

**Evidence (recent).**
| db | canonical | absorbed clusters in last 10d |
|---|---|---|
| 302 Kompaneyets | קאָמפּאַניעיעצ׳ס טרופּע | C01045, C01285, C01595, C03815, C04097, C04990, C05568, C05690, C05702 (×9) |
| 545 Ebel | עבעלס טרופּע | C00188, C02768, C03056, C05654 |
| 385 Shomer | Shomer Troupe | C00444, C00518, C07076 |
| 198 Hart | בער האַרטס טרופּע | C00868, C02609, C05404 |
| 338 Segalesco | Segalesco Troupe | C01934, C05532, C06845 |
| 531 Kadish-Khash | טרופּע פֿון קאַדיש-כאַש | C02839, C02886, C03581 |
| 490 Tsipkus | טרופּע ציפקוס | C03390, C05432, C05615 |
| 330 Meerson | מעערסאָנס טרופּע | C05750, C05797, C06775 |
| 180 Axelrad | Axelrad Troupe | C00187, C02997 |
| 533 Meltser | מעלצערס טרופּע | C04799, C05938 |
| 610 Becker | בעקערס טרופּע | C04667, C05943 |
| 790 Liebert | ליבערטס טרופּע | C02819, C05795 |
| 839 Fishzon | מישע פישזאָנס טרופּע | C02129, C04324 |
| 373 Sobye / שמ״ר | סאָביים טרופּע | C06395, C06469 |

Plus the 4 core_db merges done today (198/610/302/490 absorbing 269/824/485/810) — same pattern at the DB level.

**Safety guard.**
- **Do NOT fire on joint-named troupes** `X און Y` / `X-Y`. Joint troupes are their own entity (see Rule 3).
- If multiple Traveling Company rows share surname X (e.g. two distinct families named "פֿישזאָן"), do NOT auto-align — surface to user.
- "X-טעאַטער" form only when DB row already documents X as a director/troupe-owner; otherwise it may be a Theatre (venue) not a Company.

---

## Rule 2 — First-name tolerance within a family troupe

**Trigger.** Two clusters reference the same surname X with different first names (e.g. `מישע גענפֿער`, `סאַמואיל גענפֿער`), and the existing canonical row is family-named (`X-ס טרופּע`, not first-name-qualified).

**Evidence.** db 254 (Genfer) absorbed both Misha Genfer (C05096) and Samuel Genfer (C06301). Brothers/relatives running one family troupe.

**Safety guard.** If the canonical row itself is first-name-qualified (`Avraham Goldfaden Troupe`), require first-name match.

**This one is the riskiest of the four.** PI confirmation strongly advised before global apply.

---

## Rule 3 — Joint troupe = own entity

**Trigger.** Cluster `canonical_yiddish` is `X און Y` / `X-Y` / `X און Y טרופּע`.

**Action.** Align to existing joint-named DB row if one exists; otherwise create NEW. Do NOT collapse into the X-troupe or Y-troupe.

**Evidence.**
- ALIGNed to existing joint: C01141, C06815 → db 304 (Krause-Spivakovski); C02826, C05297 → db 864 (Kaminska-Turkov).
- Created NEW: C00731 (ציפקוס און ליפּאָווסקי), C01044 (Borisov+Rus), C01073 (Schwartz+Rosenfeld), C01236 (Guzik+Tsuker) — all NEW.

**Safety guard.** Treat `X-Y` and `Y-X` as equivalent (order-insensitive). Strip role tags (`דיר.`, `דירעקטאָר`) before matching.

---

## Rule 4 — Yiddish State Theatre short-form

**Trigger.** Cluster `canonical_yiddish` matches `<city-adj>ער מלוכה-טעאַטער` with **no national-language ethnonym** (לעטיש / רוסיש / אוקראיניש / פּויליש / ווייסרוסיש / ראומענישן / בולגאַריש / ליטוויש), AND city ∈ {Moscow, Kiev, Vilna, Riga, Kharkov, Odessa, Minsk, Birobidzhan, Vitebsk, Zhitomir, Vinnitsa, Białystok, Baku, Kovno, Bucharest, Kishinev, Chernivtsi, Lvov, Warsaw}.

**Action.** ALIGN to the city's Yiddish State Theatre db row (500 Riga, 515 Kiev, 519 Vilna, 543 Moscow GOSET, …).

**Evidence.** ORG-C01943 (Riga) → 500; ORG-C02388 (Moscow GOSET studio Mikhoels) → 543 — both applied today from melukha audit.
Also today's 3 core_db merges 573→500, 666→519, 668→515 — exactly this pattern at the DB level.

**Inverse.** If a national-language ethnonym IS attached, set `org_type = Non-Yiddish Theatre` (Rule 0 — already shipped this morning).

---

## Rule 5 — Strict NEW for distinct entities (negative rule)

Patterns that should NOT auto-align, even with surname overlap:
- Director's eponymous troupe with no DB match → NEW (e.g. C00224 סידי טאָל, C00229 בעני אַדלער, C00230 יעקב רעכצייט, C00680 דאָראַ ווייסמאַן און אנשל שאָר). The recent NEW stream confirms reviewers create rather than force-fit.
- QID-split clusters where each settlement is a separate institution (ליריק-טעאַטער Q02–Q07 → 4 new rows, one per city).

---

## Suggested rollout

1. **Rule 1 (family-name absorption)** — highest precision, ~50 ALIGNs validated in last 10d. Propose: run an auto-suggester over all open clusters of type Traveling Company; surface top-1 family-name match with confidence ≥ 0.85; PI confirms in batch.
2. **Rule 3 (joint-troupe)** — clean classifier; safe to gate cluster creation.
3. **Rule 4 (state-theatre)** — bounded by curated city list; safe to auto-suggest.
4. **Rule 2 (first-name tolerance)** — needs PI green-light per surname; do NOT enable globally without curating a "family troupes only" allow-list of canonical db_ids.
5. **Rule 5** — already implicit in current reviewer behaviour; no code action.

If PI confirms 1/3/4: a global pass over the ~1700 still-open clusters (decision blank) could yield several hundred safe ALIGNs in one sweep, leaving the residual to human review.
