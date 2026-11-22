# Session prompt — how should generic organization clusters be modelled?

## STATUS 2026-11-22 — DECIDED and IMPLEMENTED (read this first; the 10-04 text below is history)

**Decision (Sinai 11-22): kind as a property, not a node.** Neither (a) nor (b) as posed:
- A GENERIC cluster never becomes a node of any kind (the KG has done this since `e716b891e`, 11-01).
- Its mentions keep **kind + place**. `Zylbercweig/organizations/generic_kind.py` gives each
  GENERIC row a kind: a fine kind for Education (yeshiva, kheyder, talmud-torah, gymnasium,
  realschule, high-school, government-school, conservatory, drama-school, ...), the org_type
  slug otherwise (theatre, traveling-company, military, press, ...). Corrections go in
  `generic_kind_overrides.tsv` (cluster_id, kind, note), which wins over the rules.
- `kg_orgrel.py` writes every GENERIC mention to `plays/kg/generic_mentions.tsv` (person, kind,
  relation, settlement, QID, dates, sentence) and, when the settlement resolves, a
  person -> place edge `generic_org_in` (role_detail = kind). There are no type nodes, so no
  person -> person path can run through a kind. The place says "same town" and no more.
- The city goes on the mention/edge, never on a (type, city) node.
- Troupe tags: two schemes, by design. Troupe tags describe MINTED troupes; a generic troupe
  cluster gets kind `traveling-company` (its org_type) and no tags.
- First build: 1,878 GENERIC mentions, 1,170 people, 869 placed (e.g. yeshiva: 60 mentions,
  55 people, 31 placed). Counts have grown since 10-04: 912 GENERIC rows, not 374.

**Arbeter Ring / Tarbut / TsIShO defect: FIXED 11-22** (`apply_network_parents_2026_11_22.py`).
Rule: a network sub-unit that does not individuate gets ALIGN to the network parent; one that
does gets its own row with `parent_db_id` = the network. 11 GENERIC rows -> ALIGN (AR 189,
TsIShO 1111, Tarbut minted 5077; Tarbut school Otaci 5078). The main AR cluster `ORG-C00198_Q01`
(26 mentions, wrongly place-split and swept) was un-split -> ALIGN 189. 15 sub-units were
parented. AR 189 went from 0 to 41 KG edges.

**Still open** (flagged, nothing applied):
- `ORG-C00069` 'צווישאָ' (Zhitnitsky, president): probably the Argentine TsVISHO, not 1111.
- Sholem Aleichem school network is the same shape as AR: 'שלום עליכם שול 3' (numbered),
  'שלום עליכם פאָלקשולן', 'שלום-עליכם-מיטלשול', and 'שלום עליכם פאָלקס אינסטיטוט' itself, all GENERIC.
- 'ברייטענער שול 4' (numbered) and 'שולמית' x2 (perhaps the Shulamit conservatory, Tel Aviv) are GENERIC.
- core_db 3073 / 3184: probably one AR old-age home, twice.

---

*Original prompt (2026-10-04):*

Paste this into a new session. Everything below is current as of 2026-10-04.

## The question

374 clusters in `Zylbercweig/organizations/org_alignment_review.tsv` are marked `GENERIC`: their name denotes a *kind* of institution, not an individuated one — `גימנאַזיע`, `אָרטיקער קאָנסערוואַטאָריע`, `בריסקער ישיבה`, `יידישע פֿאָלקסשול`. They are not mintable as organizations: two people who attended "a folk school in Warsaw" share a city, not an institution.

They still hold **856 mentions**. The question is what to do with them in the KG:

- **(a) Unclustert** — drop them; the mentions carry no org link.
- **(b) Cluster to type** — each generic cluster points at a type node (`generic:gymnasium`, `generic:yeshiva`, …), so the corpus can answer "how many biographies mention an unnamed yeshiva?"
- **(c) Something else** — e.g. keep the cluster but mark it unlinkable, or attach the mention to the *place* instead of any org.

Sinai's instinct is (b). The task is to sketch it concretely enough to implement, or to argue him out of it.

## Why (b) is attractive, and the trap

Type-clustering preserves a real finding: *which kinds of institution the Leksikon's subjects name precisely, and which they leave vague.* Current spread of the 374:

| kind | rows | | kind | rows |
|---|---:|---|---|---:|
| school | 149 | | conservatory | 12 |
| (other) | 56 | | courses | 7 |
| troupe | 41 | | institute | 6 |
| yeshiva | 32 | | kheyder | 5 |
| gymnasium | 32 | | studio | 5 |
| talmud-torah | 17 | | polytechnic / seminary / academy / university / library / camp / college | 1–3 each |

**The trap:** if a type node is modelled as an ordinary organization, you have recreated exactly the false link the generic rule exists to prevent — everyone at `generic:yeshiva` appears to share an institution. A type node must be a *distinct node kind* that the graph never treats as an org, and no person-to-person edge may traverse it.

Secondary point to settle: the city. "A yeshiva in Odessa" is worth keeping as place evidence. Does the city stay on the mention, on a `(type, city)` pair, or nowhere? A `(type, city)` node risks re-asserting "the Odessa yeshiva" as a thing.

## The precedent: the troupe typology

This has been done once already, and the new scheme should extend it rather than invent a parallel one.

`Zylbercweig/organizations/troupe_tags.tsv` (on the **`zalmen-data`** branch, not `main`) tags **683 troupe entities** with a **closed 16-tag vocabulary**, **multi-label** (1–5 tags; 288 entities carry 2):

> Impresario Company 536 · Star Company 173 · Ad Hoc Company 164 · Family Company 89 · Operetta/Opera 55 · Cooperative 47 · Non-Jewish 37 · Kleinkunst/Revue/Cabaret 35 · Not a Troupe 22 · Ensemble 20 · Amateur 12 · Institutional 11 · German-Jewish 8 · Hebrew-Language 7 · Children's 6 · Marionette/Puppet 1

Rules: `Zylbercweig/organizations/TROUPE_TAGGING_RULES.md`. Review UI: the "Troupe-tag review" view in the Zalmen app. Columns: `db_id, tags, other_tags, comment, reviewer_notes, reviewer, reviewed_at`.

**Note the key difference:** troupe tags describe **minted entities** (`db_id`) that already exist. Generic clusters have **no entity** and must not get one. So this is not the same operation — it is the mirror image, and the sketch has to say how the two relate. Does a generic cluster carry a *tag* from a parallel vocabulary? Does `Not a Troupe` (22 entities) already encode something like `NOT_AN_ORG`?

## Current state of the decision vocabulary

Reworked 2026-10-04, all pushed to `main`:

| decision | count | meaning |
|---|---:|---|
| (undecided) | 4,327 | in the queue |
| NEW | 2,155 | minted as a DB org |
| ALIGN | 1,184 | aligned to an existing DB org |
| **GENERIC** | **374** | names a kind, not an institution — **the subject of this prompt** |
| SPLIT | 42 | needs splitting; basis not in the record (see punchlist) |
| SPLIT_DONE | 16 | split into children, parent retired |
| **NOT_AN_ORG** | 7 | not an organization name at all (extraction artifact, event, city, common noun) |
| DISCUSS | 6 | |
| UNCLUSTER | 3 | |

`DEFER` and `DESCRIPTIVE` were both retired on 2026-10-04. DESCRIPTIVE was folded into GENERIC after it turned out the two were the same judgement under different names — five names had been filed both ways.

## The generic rule as it now stands

A name is **generic** when it does not individuate:

1. **Bare kind-noun**, alone or with a descriptive adjective — `קאָנסערוואַטאָריע`, `אָרטיקער קאָנסערוואַטאָריע`, `יידישע פֿאָלקסשול`, `שטאָטישע שול`. Ethnolinguistic and civic adjectives (Jewish, Russian, Polish, municipal, state, folk, modern) do **not** individuate.
2. **City + a kind a city had many of** — `וואַרשעווער גימנאַזיע`, `בריסקער ישיבה`, `לעמבערגער פאָלקס-שול`. Gymnasia, schools, yeshives and kheyders ran to dozens per city.
3. **Benefactor networks** — `באַראַן הירש-שול` is attested in Vienna, Sosów, East Broadway (NYC) and Vilna. Like "Rothschild hospital": the name picks out a funder, not an institution.

It is **not** generic when:

- a **person is the school's subject** (not its sponsor) — `ד"ר ווייכערטס דראַמאַטישער שול`, `כהןס גימנאַזיע`, `גימנאַזיע פֿון לעווין`
- it is a **famous single institution known by its town** — Volozhin, Mir, Telz, Slabodka, Rameyles
- a **rare kind** a city normally had one of — conservatory, polytechnic, university, academy. (These stay in the queue for a human, *not* auto-aligned: Wikidata lists "Vienna Conservatory" as a **disambiguation page**, so even a rare kind is not proof.)
- a **district narrows the city** — `קיעווער פּעטשערסקער גימנאָזיע` (two place adjectives)
- there is a **dedication or quoted title** — `ישיבה ר' יצחק אַלחנן`, `ישיבה פֿון חפץ חיים`

## Known defect to fix, whichever option is chosen

**אַרבעטער-רינג was wrongly swept as a benefactor network.** The Workmen's Circle was a membership organization that *ran its own institutions* — unlike Hirsch, who funded other people's. The team had already minted 6 of its sub-units: `db1982` Teachers' Seminary, `db1994` Pittsburgh school, `db2086` Camp Kinder Ring, `db2087` South Haven camp, `db2175` Williamsburg school, `db1978` London radical school.

7 cluster rows were wrongly closed as GENERIC, including `אַרבעטער רינג קעמפּ (באָסטאָן)` — while its sibling `אַרבעטער רינג קעמפּ (סאוט העיווען)` is minted as db2087. Same shape, opposite treatment.

The right model is a **parent org with named sub-units** (compare the Yudenrat parent pattern and the Forverts umbrella in `PI_DECISIONS.md`): `ברענטש 6`, `ברענטש 597`, `סאַנאַטאָריע`, `יוגנט צענטער`, `קינדער ביבליאָטעק` all individuate — a branch number is as good as a street address. Only bare `אַרבעטער רינג-שול` with no city or number is generic.

**Tarbut and TsIShO are in the same position** — school systems, not funders — and were swept the same way. Only Hirsch, ICA and ORT are true funder-networks. Re-check those before any further sweeping.

## What a good answer looks like

1. A recommendation on (a)/(b)/(c), with the reasoning.
2. If (b): the node shape, the edge semantics, and an explicit statement of what prevents a person→person inference through a type node.
3. Where the city goes.
4. How this relates to the troupe tag vocabulary — one scheme or two, and why.
5. The migration: which file changes, what the Zalmen app shows, whether `kg/build_kg.py` needs a new node kind.

## Where things are

- Review file: `Zylbercweig/organizations/org_alignment_review.tsv` (8,114 rows)
- Core DB: `Zylbercweig/organizations/core_db.tsv` (2,210 rows; 17 deprecated 2026-10-04)
- Troupe tags: `troupe_tags.tsv` on the **`zalmen-data`** branch
- Sweep scripts (all idempotent, all with `--apply`): `apply_education_generic_rule.py`, `apply_defer_sweep.py`, `apply_titular_refinement.py`, `apply_generic_adjective_gap.py`, `unmint_generic_schools.py`, `generic_school_queue_sweep.py`, `fold_descriptive_into_generic.py`, `split_remaining_clusters.py`
- Shared helpers: `place_norm.py` (Yiddish settlement folding), and `sm()` in `unmint_generic_schools.py` (strip points + fold final letters)

**A warning that cost this session five rounds of correction:** Yiddish name matching fails silently on Unicode. Points compose and decompose differently between sources, and final letters (ךםןףץ) differ from their medial forms — `וואָלאָזשין` ends in final nun but appears medially inside `וואָלאָזשינער`, so Volozhin first classified as a generic town yeshiva. **Always normalise with `sm()` before comparing names**, and spot-check the result rather than trusting the count.
