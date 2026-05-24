# Annotation / vocalization progress tracker

Living tracker for the GT-restore + full-annotation effort (plan: restore masked RA vocalization & fully annotate all editions). Collection 18874. RA userIds: Judith=357543, Noa=397914. Updated per play.

**Legend** — Masked: RA layer (GT/IN_PROGRESS) hidden under our sparser push. Restored: GT ingested→gold, reanchored, pushed FINAL. Rest: non-GT pages vocalized+annotated, pushed IN_PROGRESS.

| Edition | docId | Pages | Survey done | Masked pages | Restored→FINAL | Lexicon harvested | Rest done | Notes |
|---|---|---|---|---|---|---|---|---|
| Di Seder Nakht | 828503 | 72 | ✅ | 3,4,5,6,7 | ✅ FINAL + page-type | partial | ☐ | pp.3–7 restored FINAL + page-type. act-4 p50 pushed. **Appendix pp.55–70 re-segmented lg=stanza + songGroup headings, pushed IN_PROGRESS. p67 done from your corrected server version (lg{n:7;cont:yes}+songGroup; lg numbering vs neighbors flagged for RA).** cross-page cont flagged for RA. 2-part TEI in [[diseder-two-part-tei]]. Gold p5 has RA `continued:true` to convert. |
| Der Mann untern Tisch | 817462 | 20 | ✅ | none masked | ✅ Noa ingested (pp.8,9,18 pushed IN_PROGRESS) | ☐ | ☐ | **2026-05-21**: Noa stopped; ingested her authoritative pp.8-11,18 → gold+annotated; converted per-line `lg{continued}`→group `lg{n:1}` on p8(23l)/p9(12l)/p18; pushed p8,9,18 (dens 0.24-0.25, body, no continued). pp.10,11 = her content (local synced, already on server). p3 = Judith GT skip-dup, left. **Conflicts for Noa** (`annotation_conflicts_DerMann_2026-05-21.json`): p10 ×2 untyped stages look like speech; p18 untyped stage + I/II/III stanza split. |
| Yudale der Blinder | 828539 | 70 | ✅ | 5,7,8,9 (Judith FINAL) | ✅ (288302586–602) | ☐ | ☐ | **RESTORED 2026-05-21**: fetched Judith FINAL→gold, reanchored tags (0 drops), stripped speaker/stage, pushed FINAL. dens 0.18/0.27/0.27/0.25. cast_dict exists. RA-touched 64/70. |
| Das Yudishe Kind | 828424 | 60 | ✅ | 1,3 (Judith GT/FINAL) | ✅ (288304877,879) | ☐ | ☐ | **RESTORED 2026-05-21** (NO strip — speakers vocalized): p1 titlePage 0.22, p3 body 0.25, page-type re-applied. RA-touched 54/60. |
| Al Naharot Bavel | 820975 | ~68 | ✅ (partial) | none on server | n/a | ☐ | ☐ | Server clean (pipeline-vs-pipeline). Local page_annotated pp.26–63 stale vs local gold — refresh local to avoid future mask. |
| Kidush Hashem | 820939 | 75 | ✅ (partial) | none | n/a | ☐ | ☐ | Clean (p65 blip under threshold). **lg converted per-line→group+cont 2026-05-21**: 18 song pp. re-pushed IN_PROGRESS (tsIds 288296xxx), 33 synthesized cont:yes flagged (`lg_cont_flags_2026-05-21.json`) + 6 pre-existing cont to review. |
| Mishke Mashke | 828537 | ? | ☐ | ? | ☐ | ☐ | ☐ | Only front-matter annotated (4 pp). Gold p9 has RA `continued:true` to convert. |
| Ezra | 828481 | ? | ☐ | ? | ☐ | ☐ | ☐ | Not started. |
| Blimele | 828455 | ? | ☐ | ? | ☐ | ☐ | ☐ | Not started. |
| Bas Sheva | 828443 | ? | ☐ | ? | ☐ | ☐ | ☐ | Not started. |
| Isha Raa | 820937 | ? | ☐ | ? | ☐ | ☐ | ☐ | Not started. |
| Hinke Pinke | 820969 | ? | ☐ | ? | ☐ | ☐ | ☐ | Not started. |
| Sore Sheyndel | 820964 | ? | ☐ | ? | ☐ | ☐ | ☐ | Not started. |
| Dovid's Fidele | 820845 | ? | ☐ | ? | ☐ | ☐ | ☐ | Not started. |
| Dos Yudishe Herts | 820841 | ? | ☐ | ? | ☐ | ☐ | ☐ | Not started. |
| Meshumed | 534187 | ? | ☐ | ? | ☐ | ☐ | ☐ | Not started (partial in DraCor). Attribution note: [[dos_yudishe_kind_attribution]] is a different play. |

## Conventions to honor (per [[editions-conventions-2026-05-19]])
- DerMann: rafe=yes, syllable position, strip segol/hiriq from stage dirs.
- Yudale: rafe=no, consonant position, speakers/stage not vocalized.
- Di Seder: rafe=no, mixed position.
- Das Yudishe Kind: rafe=yes, syllable position, **speakers vocalized but untagged — don't strip**.
- Kidush Hashem / Al Naharot: rafe=no, consonant position.

## Stage `type` allowed set (Phase 0.5, per TEI Drama)
`setting, entrance, exit, business, delivery, location, costume, novelistic` — every `stage` must have one; **no `mixed`**.
