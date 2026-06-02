# Kaminski / Kaminska entity merge — split options for PI review

**Context.** RA reported that the Kaminski/Kaminska organisation entries in the Zylbercweig `core_db` should be merged. A first-pass merge was drafted that would fold six rows into one canonical entity (db_id 147), per the working principle that theatre-family entities (founder + spouse-led troupe + later state theatre + family venue) should be preserved as one continuous organisational identity. On review, the merge was **not applied to `core_db.tsv`** — we want PI input on whether the full merge is too aggressive before changing the DB.

## The six entities currently merged into 147

| db_id | Name | Historical reading |
|---|---|---|
| 147 | Kaminski's Theatre (Ulica Obozna 1–3, Warsaw) | Pre-1918 family venue — A. Y. Kaminski's fixed theatre in Warsaw |
| 809 | טרופּע פֿון א. י. קאַמינסקי | A. Y. Kaminski's touring troupe (the patriarch, d. 1918) |
| 239 | Ester Rukhl Kaminska Troupe | E-R Kaminska's own touring company (she d. 1925) |
| 297 | Kaminska Troupe | Generic — could be E-R Kaminska's troupe or daughter Ida Kaminska's |
| 477 | יידישן באַוועגלעכן טעאַטערקאָלעקטיוו אויפן נאָמען פון אסתר-רחל קאמינסקאַ | "Named after" — commemorative mobile theatre, post-1925 |
| 599 | יידישער מלוכה-טעאַטער אויפֿן נאָמען פֿון אסתר-רחל קאמינסקאַ | Polish State Yiddish Theatre (1950–present, Ida Kaminska era) — postwar institutional reconstitution |

Row 374 ("Sam Adler and Kaminski Troupe") is **not** part of this discussion — conjunction entities are kept separate as a matter of policy.

## Splitting options (most → least aggressive merge)

### Option A — keep fully merged (current staged state)
One Kaminski-family canonical at 147; all six rows fold into it. Maximises family continuity.

### Option B — split off the postwar State Theatre (599) only
- 147 ← {239, 297, 477, 809}: pre-WWII family theatre + troupes
- 599 stands alone: the postwar Polish State Yiddish Theatre is an organisationally distinct institution (state-funded, communist Poland, founded 1950, still operating today as Teatr Żydowski w Warszawie).
- **Rationale:** family continuity ≠ institutional identity. The State Theatre was *named after* E-R Kaminska, not run by her. Different funding, governance, era.

### Option C — split both commemorative "named after" entities (477 + 599) off
- 147 ← {239, 297, 809}: the actual Kaminski family operation
- 477 stands alone: commemorative mobile collective
- 599 stands alone: State Theatre
- **Rationale:** the Yiddish "אויפן נאָמען פון" ("named after") prefix is a strong signal that 477 and 599 are commemorative successor institutions honouring E-R Kaminska after her 1925 death, not continuations she led.

### Option D — split by founder/person
- 809 (A. Y. Kaminski) + 147 (his Warsaw venue) → canonical "A. Y. Kaminski Theatre"
- 239 + 297 → "E-R Kaminska Troupe"
- 477 stands alone
- 599 stands alone
- **Rationale:** treats each person's primary company as a distinct entity, with the venue tied to the patriarch.

### Option E — split venue from troupes
- 147 alone (the fixed Warsaw venue)
- 809 + 239 + 297 + 477 merged (all family touring companies)
- 599 alone (State Theatre)

### Option F — full unmerge
Restore all six as separate entities (i.e. do not commit the staged merge).

## Recommendation

Option **B** or **C**. The "אויפן נאָמען פון" prefix is a strong textual signal that 477 and 599 are commemorative successor institutions; 599 in particular is unambiguously a different organisation from the prewar family business. Option B is the conservative minimum (separates only the State Theatre, which is hardest to justify as continuous); Option C extends the same logic to 477.

## Mention evidence (do the source mentions cluster as cleanly as the entities?)

Yes — the textual evidence in the corpus already does most of the disambiguation work:

| Cluster | Size | Canonical mention text | Aligned db_id | Verdict |
|---|---|---|---|---|
| ORG-C00014 | 1 | יידישן באַוועגלעכן ט"ק אויפן נאָמען פון אסתר-רחל קאמינסקאַ | 477 | clean — exclusively the commemorative mobile collective |
| ORG-C00584 | 4 | טרופּע פֿון א. י. קאַמינסקי / א. י. קאַמינסקיס טרופּע | 809 | clean — all 4 mentions explicitly name **A. Y.** Kaminski |
| ORG-C02264 | 1 | יידישער מלוכה-טעאַטער אויפֿן נאָמען פֿון אסתר-רחל קאמינסקאַ | 599 | clean — explicitly "מלוכה-טעאַטער" (State Theatre) |
| ORG-C06877 | 1 | קאַמינסקאַ | 297 | ambiguous bare surname — neutral under any split option |

- 147 (Warsaw venue) and 239 (E-R Kaminska Troupe) have **no corpus mentions** — they come from the Lexicon biographical entries / address backfill.
- Total of **7 mentions** across the 4 clusters — a small surface area, so the decision matters more for downstream entity-modelling consistency than for any large reclassification cost.
- The Yiddish naming patterns (`אויפן נאָמען פון`, `מלוכה-טעאַטער`, `א. י.`) cleanly separate the commemorative, institutional and patriarch-troupe mentions; the splits in Options B/C/D are well grounded in the source text.

## Status

**Left open pending PI review.** No changes have been applied to [core_db.tsv](core_db.tsv) for the Kaminski/Kaminska entities; no follow-up changes will be made until PI selects an option.

## Follow-up: examine other "super-org" family clusters

Whatever option is chosen here should become a precedent for the analogous family-theatre clusters in the DB. We suggest a systematic pass over the candidates below, applying the same questions — *founder vs. spouse-led troupe vs. fixed venue vs. successor / "named after" institution vs. postwar state theatre* — and producing a parallel PI memo for each before committing any merges.

Likely super-org family clusters to examine:

- **Adler** (Jacob Adler / Adler troupes / Sara Adler / Stella Adler / Adler venues, plus conjunctions like 374 "Sam Adler and Kaminski Troupe" which stay separate)
- **Thomashefsky** (Boris / Bessie / Thomashefsky theatre)
- **Goldfaden** (founder + early itinerant troupe + successor companies named after him)
- **Schwartz / Yiddish Art Theatre** (Maurice Schwartz the impresario vs. the Yiddish Art Theatre institution)
- **Fishzon** dynasty
- **Spivakovsky** dynasty
- **Kompaneyets** dynasty

The mention-evidence pattern observed here — that the Yiddish source text often disambiguates founder / spouse / commemorative / state-theatre forms by explicit prefixes — should generalise, and is worth checking first for each family before deciding how aggressively to merge.

## What we need from PI

1. A choice between A–F for the Kaminski/Kaminska cluster.
2. Confirmation that the same review procedure (entity table + split options + mention-evidence check) should be applied to the other family clusters listed above, with a separate memo per family.
