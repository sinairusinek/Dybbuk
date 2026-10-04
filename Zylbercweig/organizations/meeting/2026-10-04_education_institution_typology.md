# Education institutions: a typology for the minting rule

Written 2026-10-04 after the team meeting, to make the new rule operational.
Companion to the status and questions documents.

## The rule as agreed

> All the education institution types raised in the questions document are **generic** — they name a *kind* of school, not an individuated institution — and are not mintable. **Universities are exempt.**

Refinement agreed in follow-up: in the tertiary grey zone (conservatories, institutes, polytechnics, academies, colleges, seminaries), a name is exempt where it is **titular** — where there was only one institution of that title.

## The operative distinction

The rule is not really about institution *kind*. It is about whether the name **individuates**. "University" is exempt not because universities matter more, but because a city had one, so the name picks out a referent. The same logic exempts "Riga Polytechnic" and refuses "the local conservatory."

Three tiers follow:

### Tier 1 — Never mintable (generic kind)

A kind-noun, alone or with a non-individuating adjective: *local, municipal, state, government, Jewish, Polish, Russian, German, folk, general, elementary, evening, commercial, technical, higher.*

Examples: `קאָנסערוואַטאָריע` · `אָרטיקער קאָנסערוואַטאָריע` · `שטאָטישע שול` · `רעאַל-שול` · `יידישע פֿאָלקסשול` · `חדר` · `תלמוד-תורה`

These assert only "both were at *a* school of this kind in this city" — which is no stronger than "both lived in this city." **131 rows stamped `GENERIC` automatically.**

### Tier 2 — Titular, therefore exempt

The name picks out one institution, by any of:

- **University or faculty** — a city had one. `פֿרייבורגער אוניווערזיטעט`, `יורידישן פֿאַקולטעט`. *(14 rows, left as-is.)*
- **A person** — founder, director, patron. `ד"ר ווייכערטס יידישער דראַמאַטישער שול`, `פּריוואַטער דראַמאַטישער שול פֿון מאַדאַם מאָרגענראָט`, `סעמינאָר פֿון פּראָפעסאָר שפּאַן`
- **A movement or named body** — `שלום עליכם פאָלקס אינסטיטוט`, `תרבות`, `צווישאָ`
- **A unique institutional title** — `ניו ענגלאַנד קאָנסערוואַטאָריע`, `ריגער פּאָליטעכניקום`, `ראָיאל קאָלעדזש`
- **Attachment to a named host** — `דראַמאַטישע סטודיע פֿון מאָסקווער קונסט-טעאַטער`

### Tier 3 — Needs a human titular check

**Kind + city adjective**, the genuinely ambiguous pattern:

- `ווינער קאָנסערוואַטאָריע` — probably titular, *but* Wikidata's "Vienna Conservatory" is a **disambiguation page**: there was more than one.
- `וואַרשעווער גימנאַזיע` — certainly not titular; Warsaw had dozens.

The test is historical, not linguistic: **did that city have one institution of that title, or many?** As a rule of thumb, the rarer and more capital-intensive the kind, the likelier it is titular — one conservatory or polytechnic per city is common; one gymnasium or kheyder is not.

## What was applied, and what was not

| | Rows | Action |
|---|---:|---|
| Tier 1 — bare kind + generic adjective | 131 | stamped `GENERIC` |
| Tier 2 — university / faculty | 14 | left `DEFER`, exempt |
| Tier 3 — needs titular check | 414 | left `DEFER`, written to punchlist |

The 414 are in `education_titular_review_punchlist.tsv` with blank `titular?` and `wikidata_qid` columns to fill in. By kind: School 181 · Yeshiva 35 · Gymnasium 34 · Conservatory 20 · Studio 18 · Institute 17 · Academy 10 · Courses 9 · College 6 · Talmud Torah 5 · Seminary 4 · Polytechnic 4 · Lycée 1 · other 70.

**Why only 131 were auto-closed.** The classifier is a whitelist: it closes a row only when every token is a recognised kind-noun or generic adjective. Anything unrecognised — a surname, a city, an initialism — goes to review. A blacklist was tried first and swept in real institutions (`ד"ר ווייכערטס...שול`, `קלאַראַ מאָרגענשטערענס פּריוואַטע פאָלקס שול`, `פּראָפעסאָר האַקסיס סקול אָוו מיוזיק`). The two errors are not symmetric: a missed generic stays in a queue and costs a minute, while a wrongly closed institution disappears silently.

**Wikidata is not a reliable gate here.** Its search API returns a disambiguation page for "Vienna Conservatory" and nothing at all for the Royal College of Music or the Leipzig Conservatory. Useful for confirming a specific hunch; not for deciding 414 rows.

## Recommended next step

Work the punchlist by kind, not row order — the 20 conservatories and 4 polytechnics are mostly titular and will resolve quickly, while the 181 schools and 34 gymnasia are mostly generic. Two passes at opposite ends would close most of the 414 with little deliberation.

## Open question this surfaces

The same rule should apply to the other unfinished types — a `שטאָטישער טעאַטער` mentioned once is the same problem as a `שטאָטישע שול` mentioned once. Theatre alone has 1,394 singleton clusters. Worth confirming before anyone mints there.
