# Bootstrap pull + draft castList tagging, 2026-06-18

> **Update 2026-06-18 (evening, Claude):** the auto-tagger draft was cleaned up by a per-line decision pass with anchored matching, hand-curated xmlids, brace-group handling, collective splitting per Global E, and Global F (profession/relation modifier → roleDesc) applied. Open judgment calls are now in `handoff_noa_2026-06-18_castlist_review.md` — 10 yes/no/choice questions for Noa. Quality is no longer "draft" — it's "ready for RA verification on the surfaced uncertainties."

5 printed Lateiner plays pulled from Transkribus and given **draft** castList tagging via auto-tagger. **NOT yet pushed to TK** — quality needs review before pushing.

| Play | Doc | Pages pulled | castList page | Roles tagged | Notes |
|---|---|---|---|---|---|
| BasSheva | 828443 | 74 | 0006 | 10 | Names+actors line; final collective enumeration not split (Global E) |
| HinkePinke | 820969 | 68 | 0004 | 13 | Final line `יעֶנעֶר, בּויערעֶן, יוּדעֶן סאָלדאַטען` should be 4 collectives (Global E) |
| SoreSheyndel | 820964 | 68 | 0007 | 9 | `רב יוחנצי דין` mis-split (only "רב" landed as role); braced children pattern not captured |
| DovidsFidele | 820845 | 74 | 0006 | 12 | Cleanest — dash-delimited; mostly OK |
| DosYudisheHerts | 820841 | 76 | 0004 | 12 | Setting line `(אָרט דער האַנדלוּנג דוּמעניען).` caught; multi-line desc on Roza/Paula over-included |

## Known quality issues — needs RA / LLM-assist cleanup

### Bad xmlids (crude transliteration)
- `vvyktar` → should be `viktor` (ויקטאָר)
- `tvbyh` → should be `toviya` (טוֹביה)
- `gympel` → should be `gimpel` (גימפּעל)
- `dr_abrhm` → should be `dr_avrohom` (ד"ר אברהם)
- Many more — TRANSLIT table in `/tmp/bootstrap_castlists.py` is rough; needs proper IPA-style Yiddish→Latin map.

### Split errors (role boundary wrong)
- **SoreSheyndel** `רב יוחנצי דין` — only `רב` tagged as role; "יוחנצי דין" went to roleDesc. Should be role=`רב יוחנצי`, roleDesc=`דין` (or no descr).
- **DosYudisheHerts** `ראָזאַ סעֶרקעֶלעֶס טאָכֿטעֶר פוּן עֶרְשׁטעֶן מאַן` — whole string tagged as role. Should be role=`ראָזאַ`, roleDesc=rest.
- **BasSheva** final line `שׁעפֿער שׁעפֿעריגען, מיליטער, טייפֿעל, בּוֹיען, עטצ.` — collective enumeration not split per Global E.
- **HinkePinke** final line `יעֶנעֶר, בּויערעֶן, יוּדעֶן סאָלדאַטען.` — collective enumeration not split.

### Brace-group siblings not captured (Global C)
- **SoreSheyndel** has `אברהמעלע / זייערע קינדער / באבעלע` brace structure — auto-tagger flattens it.
- **DosYudisheHerts** has `וויקטאָר / איהרע קינדער / לידאַ` brace structure — auto-tagger flattens it.

### Profession/relation modifier separation (Global F, Noa 2026-06-18 rule)
The split delimiter ":" / "." / "," mostly produces a clean role + roleDesc; the rule (modifier → roleDesc always) is honored where the delimiter is unambiguous. Where the proper name has no delimiter from the descriptor (SoreSheyndel space-delimited), boundaries are guessed and often wrong.

## Path forward

These 5 plays are now **bootstrapped at the infrastructure level** — page-XML pulled, draft castList tagged, cast_dict.json present. To finish:

1. **Cleanup pass** (per play): fix bad xmlids, split miscombined roles, capture brace groups, split collective enumerations. Either RA-led or LLM-assist.
2. **Apply Global F roleDesc pattern** (profession/relation modifiers → roleDesc) consistently — auto-tagger gets this mostly right when delimiter is clear.
3. **Push castList back to TK** as a new layer (with `tool_name="YiDraCor-castlist-bootstrap-draft"` so it's identifiable).
4. **Body-page auto_annotate** + LLM stage typing + push.

## Files written

- `data/<play>/page/*.xml` — full page pull from TK (read-only mirror; do not edit).
- `data/<play>/page_annotated/<castList>.xml` — draft tagged castList.
- `data/<play>/cast_dict.json` — derived dictionary (matches the draft castList).

Bootstrap script: `/tmp/bootstrap_castlists.py` (one-shot; not part of the pipeline).
