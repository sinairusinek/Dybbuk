# מלוכה (State-Theatre) Audit — Yiddish vs. Non-Yiddish

Run: 2026-06-02. Scope: every entity in core_db.tsv + organizations_clustered.tsv whose name contains `מלוכה`.

## Headline
- **Total touched:** 49 core_db rows + 111 clusters = 160 entities
- **Auto-passed as Yiddish** (name contains `יידיש`/`אידיש`): 40 db + 62 clusters = **102**
- **Flagged for review:** 9 db + 49 clusters = **58**

The triage of the 58 flagged entities is below. Three categories emerge.

---

## Category A — Clearly NON-Yiddish (national-language state institutions)

These have a non-Yiddish ethnonym in the name (Latvian / Russian / Ukrainian / Polish / Belarusian / Romanian / etc.) and no Yiddish marker. They are referenced by Yiddish biographees only because Jewish actors/musicians passed through them. **They should remain outside the Yiddish org DB.**

| Cluster / db | Name | Settlement | Verdict |
|---|---|---|---|
| db 594 | אַלאוקראַאינישן מלוכה-טעאַטער | — | All-Ukrainian State Theatre |
| db 618, 619 | לעטישן מלוכה-טעאַטער | Riga | Latvian National Theatre (confirmed; "אין לעטיש") |
| ORG-C07378 | לעטישן מלוכה-טעאַטער | Riga | Same as 618 |
| ORG-C00209 | אוקראינישן מלוכהטעאַטער „געזבולט" | — | Ukrainian state |
| ORG-C00435 | אוקראינישן באַוועגלעכן מלוכה-טעאַטער „רויטער האָמער" | — | Ukrainian itinerant state |
| ORG-C03547 | אַלאוקראיינישן מלוכה-טעאַטער „געז-קולט" | — | All-Ukrainian state |
| ORG-C04267 | אַלאוקראַאינישן מלוכה-טעאַטער „געזקולט" | Kharkov | Same family as above |
| ORG-C00713 | אוקראַאינישן מלוכה-פֿאַרלאָג פֿאַר נאַציאָנאַלע מינדערהייטן | Berdychiv | Ukrainian state publisher for minorities (published Yiddish works, but is a Ukrainian institution) |
| ORG-C04787 | אוקראַאינישער מלוכה-פאַרלאָג | Kharkov | Ukrainian state publisher |
| ORG-C03427 | ווייסרוסישן מלוכה-אינסטיטוט פֿאַר טעאַטראַלער קונסט | — | Belarusian SSR theatre institute |
| ORG-C04575 | ווייסרוסישער מלוכה[שער] אינסטיטוט פֿאַר טעאַטראַלער קונסט | Moscow | Belarusian SSR theatre institute |
| ORG-C03050 | פּוילישער מלוכהשער קאָנסערוואַטאָריע | — | Polish State Conservatory |
| ORG-C04952 | מלוכהשע רוסישע רעאַל-שול | Warsaw | Russian-language state Realschule |
| ORG-C05225 | רוסיש מלוכה-טעאַטער | — | Russian state theatre |
| ORG-C05829 | מאָסקווער קליינעם מלוכה-טעאַטער (סאָמבאַטאָוו) | — | **Maly Teatr** Moscow — Russian state theatre |

**Recommended action:** confirm DECISION=NEW (or a "non-Yiddish reference" disposition if such exists) — do **not** ALIGN to any Yiddish db row.

---

## Category B — Likely NON-Yiddish (Soviet/other state institutions attended by Jews)

No ethnonym in the name, but the institution is structurally a non-Yiddish state body: conservatory, opera, gymnasium, philharmonic, state factory, state publishing house, government pension. Mention context = Jewish biographee studied/worked there.

Cluster | Name | Settlement | Type
|---|---|---|---|
| db 734 + ORG-C00313 | פֿרונזער (קירגיזיע) מלוכהשן אָקאַדעמישן אָפּערעטעאַטער | Frunze, Kirgizia | Operetta theatre |
| ORG-C00233 | אוזבעקישער מלוכהשער פילהאַרמאָניע | — | Uzbek Philharmonic |
| ORG-C00344 | מלוכהשער אוקראינישער אָפּערעטע | Donbas | Ukrainian operetta |
| ORG-C01242 | פעדעראַלן מלוכה טעאַטער-פּראָיעקט | — | US **Federal Theatre Project** (English-language WPA program) |
| ORG-C01553_Q01 | מלוכה-קאָנסערוואַטאָריע אין כאַרקאָוו | Kharkov | State conservatory |
| ORG-C01553_Q02 | מלוכה-קאָנסערוואַטאָריע | Petersburg | Petersburg Conservatory |
| ORG-C01567_Q01/Q03 | מלוכה-טעאַטער (Vilnius / Sofia / Galați) | various | Lithuanian / Bulgarian / Romanian state theatres |
| ORG-C01611 | מלוכהשע קאָנסערוואַטאָריע פֿאַר דראַמאַטישער קונסט | — | State drama conservatory |
| ORG-C02218 | קיעווער מלוכהשער מוזיקאַלישער שול | Kyiv | Kyiv music school |
| ORG-C02219 | מלוכהפֿאַרלאַג פֿון סאָוועטנפֿאַרבאַנד | — | Soviet state publishing |
| ORG-C02383 | אוקראיינישע מלוכהשע פילהאַרמוניע | Kharkov | Ukrainian Philharmonic |
| ORG-C02846 | מלוכהשן לערער-סעמינאַר | Lemberg | State teachers seminary |
| ORG-C02979 | קעשנעווער מלוכה פּופּן-טעאַטער | Kishinev | State puppet theatre |
| ORG-C02983 | ווילנער מלוכהשע קאָנסערוואַטאָריע | Vilna | Vilnius Conservatory |
| ORG-C03139 | מלוכהשן אינסטיטוט פֿאַר טעאַטער-קונסט | Warsaw | State theatre institute |
| ORG-C03258 | מלוכה-פאַבריק | Kovno | **State factory** — not a cultural org at all |
| ORG-C03634 | מלוכה-טעאַטער פאַרן יונגען צושויער | Kyiv | Kyiv TYUZ (children's theatre) |
| ORG-C03641 | מלוכהשע דראָמאַטישע שול | Warsaw | State drama school (Polish PIST) |
| ORG-C03958 | מינסקער ווייסרוסלענדישן מלוכה-פֿאַרלאָג | Minsk | Belarusian state publisher |
| ORG-C04207 | מלוכה-גימנאַזיע | — | State gymnasium |
| ORG-C04268 | קיעווער 3-יאָריקע מלוכה-קורסן פֿאַר פרעמדע לשונות | Kyiv | Kyiv state foreign-language courses |
| ORG-C04716 | מלוכה-קאָלעקטיוו | Sebezh | State collective |
| ORG-C04782 | מלוכה-פילהאַרמאָניע | — | State philharmonic |
| ORG-C04848 | כאַרקאַווער מלוכה-אָפּערע | Kharkov | Kharkov State Opera |
| ORG-C04849 | כאַרקאָווער מלוכהשן קינדער-טעאַטער | Kharkov | Kharkov children's theatre |
| ORG-C05104 | ראַטנפֿאַרבאַנד-מלוכה | Moscow | **Soviet state** (re: pension) — not an org |
| ORG-C06436 | מלוכהשער קונסט-שול | Odessa | Odessa state art school |
| ORG-C01593 | מלוכה-טעאַטער „דער רויטער האַמער" | — | Ukrainian itinerant state theatre |
| ORG-C01600 | באַוועגלעכן מלוכה-טעאַטער פון אוקראינע | — | Itinerant Ukrainian state theatre |
| ORG-C00728 | מלוכה'ש דראַמאַטיש טעאַטער | Zinovyevsk | Soviet provincial state drama |
| ORG-C01567_Q01 | מלוכה-טעאַטער | Vilna/Sofia | mixed Lithuanian + Bulgarian state |
| ORG-C03207 | באָברויסקער סאָוויעטישן מלוכה-טעאַטער | Bobruisk | **AMBIGUOUS** — could be Belarusian SSR Yiddish branch; check mentions |

**Recommended action:** mark as NON_YIDDISH_REFERENCE (whatever the equivalent decision is). For ORG-C03207 (Bobruisk Soviet State Theatre) — pull mentions and decide; some Bobruisk theatres were Yiddish.

---

## Category C — Yiddish, missed by auto-pass (false negatives)

The pattern: name uses `מלוכה` *without* the `יידיש` modifier, but the entity is clearly the Yiddish State Theatre lineage in context.

| Cluster / db | Name | Settlement | Should align to |
|---|---|---|---|
| **ORG-C01943** | **ריגער מלוכה-טעאַטער** | **Riga (4 mentions)** | **db 500** (Riga Yiddish State Theatre) |
| db 573 (already merged) | ריגער מלוכה-טעאַטער | Riga | merged_into=500 ✓ |
| db 666 (already merged) | ווילנער מלוכה-טעאַטער | Vilna | merged_into=519 ✓ |
| db 668 (already merged) | קיעווער מלוכה-טעאַטער | Kyiv | merged_into=515 ✓ |
| **ORG-C02388** | **מאָסקווער מלוכה-סטודיע פֿון שלמה מיכאַעלס** | **Moscow** | **db 543** (Moscow GOSET) — Mikhoels-founded studio |
| ORG-C03207 | באָברויסקער סאָוויעטישן מלוכה-טעאַטער | Bobruisk | **NEEDS MENTION CHECK** before deciding |

The first three core_db rows (573/666/668) are already cleanly merged. The two big actionable items are:
- **ORG-C01943** → align to db 500 (4 Riga mentions; "ריגער מלוכה-טעאַטער" without יידיש is just a short form, identical to db 573 which is already merged into 500)
- **ORG-C02388** → align to db 543 (Mikhoels = GOSET founder; "Moscow State Studio of Solomon Mikhoels" is the GOSET training studio)

---

## Edge case — Russian-language Jewish education

- **ORG-C02666** `4-קלאַסיקע רוסישע מלוכה-שול פֿאַר יידן` — "Russian state 4-class school *for Jews*". Russian-medium but explicitly Jewish-targeted (probably a Tsarist-era *kazyonnoe yevreyskoye uchilishche* / state Jewish school). Belongs in a Jewish-education category, but not the Yiddish theatre/cultural-org DB. Disposition: NEW with type=Education, no Yiddish-org merge.

---

## Summary of recommended actions

1. **2 alignments to write** (Category C false negatives):
   - ORG-C01943 → ALIGN db 500
   - ORG-C02388 → ALIGN db 543
2. **1 cluster to investigate** (Category B ambiguous):
   - ORG-C03207 (Bobruisk Soviet State) — pull mentions before deciding
3. **~50 clusters to dispose as non-Yiddish** (Categories A + B):
   - All correctly outside the Yiddish org DB; need a clean disposition (NEW with type-tag, or a new NON_YIDDISH_REFERENCE decision if you want them excluded explicitly from the DB)
4. **1 special case**: ORG-C02666 — Russian state Jewish school, separate education category

**Rule reinforcement:** the `מלוכה` lexical pattern is heavily polluted by non-Yiddish national/Soviet state institutions. Any future automated rule keyed on `מלוכה-טעאַטער` MUST require either:
- `יידיש`/`אידיש` modifier in the name, OR
- explicit settlement match against the curated **Yiddish State Theatre city list** (Moscow, Kiev, Minsk, Kharkov, Odessa, Vinnitsa, Vitebsk, Zhitomir, Białystok, Birobidzhan, Baku, Riga–Skolas-iela-6, Kovno, Bucharest, Kishinev, Chernivtsi, Lvov, Warsaw, Vilna).

Without that guard, naive matching will fold Maly Teatr, the Federal Theatre Project, and Kharkov Opera into the Yiddish State Theatre family.
