# Publishing dynasties — how to model successive owners

**Status:** PROPOSAL, now **grounded in a survey of outside practice** (§5,
done 2026-10-10). Nothing in `core_db.tsv` has been restructured for it. The
publisher audit of 2026-10-10 minted and aligned under the *current*
conventions.

**The survey changed the answer.** Rules 1–4 stand, but **Rule 1's mechanism
does not**: putting the proprietor sequence in `name_variants` is the one choice
every practice surveyed identifies as the lossy one. See §4.2–4.3. The survey
also found that **the Romm press is already split across db 495 and db 953 by
accident** (§5.7) — a live defect, and the natural first test of the rule.

**For:** PI + Sinai.
**Companion to:** `CLASSIFICATION_POLICY.md` (the canonical typology) and
`TROUPE_TYPOLOGY_PROPOSAL.md` — which §5.9 argues is **the same schema question
as this one** and should be decided with it.
**Raised by:** Sinai, 2026-10-10, on the Lebenzohn finding below.
**Surveyed by:** Claude, 2026-10-10 (§5; six strands, web research).

---

## 1. The problem, in one case

Friedberg's *תולדות הדפוס העברי בפולניה* (pp. 111–112) documents the Warsaw
Lebenzohn press across four generations:

| Proprietor | From | Evidence |
|---|---|---|
| **Avigdor** b. Yoel ha-Kohen Lebenzohn | press founded תקע״ח / **1818** | typesetter for Nosonovitsh, then his own press; partners with him to 1821 |
| **Yoel**, his son | תרי״ב / **1852** | Avigdor hands it over on leaving for Eretz Yisrael; prints to 1873 |
| **Yosef**, Yoel's son | after Yoel's death, to ~**1900** | from תרל״ח with David Fridman, then alone |
| *(parallel)* **Gershon** and **Pesach**, Yoel's sons | תרט״ז / **1856** | open a *further* press |

`core_db` holds **db 24 = יואל לעבענזאהן** — the middle proprietor only. The
corpus separately mentions `דפוס של ר" יוסף לעבענזאָהן`. Neither the same
person nor unrelated: father and son, one continuing house.

The question generalises. The audit met the same shape in the Baumritter,
Goldfarb, Bletnitski, Katsenelbogn, Morgenstern and Jakobson–Goldberg firms,
and it will recur in every printing family in the corpus.

**Why it matters beyond tidiness.** The alignment pipeline attaches each
cluster to exactly one `db_id`. If a dynasty is one row, a mention of *Yosef*
aligns to a row named *Yoel* and the name no longer matches the evidence. If it
is three rows, a bare `לעבענזאהן` mention cannot be aligned at all without
guessing a date. Either way the modelling decision is upstream of the
alignment, which is why the audit stopped rather than guessing.

---

## 2. What the DB already does

Four precedents exist and the proposal below is assembled from them, not
invented.

**a. Family continuity in theatre — already ruled on.** PI guidance of
2026-06-02 on Kaminski/Kaminska: *"though Kaminski is the father and Kaminska
wife and then daughter, and it was a theater as well as troupe, we would like
to preserve the continuity of the family in the entity."* Six rows (147, 239,
297, 477, 599, 809) were merged into **db 147**. Recorded in
`feedback_family_continuity_merges`.

**b. Conjunction entities stay separate.** From the same ruling: "Sam Adler and
Kaminski Troupe" (374) is a partnership, not a continuation, and keeps its own
row.

**c. `parent_db_id` exists, and is used only for simultaneous branches.** All
10 rows that use it are places, in **three** groups: Forverts New York /
Chicago / Philadelphia / Detroit under db 249 (692–695); Hashomer Hatzair
Vilnius / Toronto / Chełm under db 602 (696–698); and Unzer Vinkl Kharkov /
Kyiv / Minsk under db 427 (1785–1787). None expresses a succession in time.

**d. Merges use `deprecated=true` + `merged_into`,** with the absorbed row's
`linked_cluster_ids` folded into the survivor and its names into
`name_variants`. 56 rows currently do this.

---

## 3. Proposal

### Rule 1 — Succession is ONE organization

A press passing from father to son is the Kaminski shape: one continuing
concern under changing proprietors. Model it as a single row.

- Canonical row keeps the **earliest** `db_id` already in the DB.
- Each proprietor's name form goes in `name_variants`.
- The succession itself — who held it when — goes in the notes, with its
  source.

So Lebenzohn is **db 24**, carrying Avigdor / Yoel / Yosef, even though the row
is currently named for Yoel alone.

*Against:* the row's `name` then names one proprietor while the entity spans
three. *For:* the alternative fragments one imprint across three ids and makes
every undated mention unalignable. §4 asks whether the detail should be
recoverable rather than flattened.

### Rule 2 — Concurrent relatives are SEPARATE organizations

The test is **chronological overlap, not surname**.

Sh./S./Shelomoh Goldfarb runs 1900–1933; M. Goldfarb runs 1924–1933. They
overlap for a decade, so they were two businesses, however closely related.
Two rows: db 50 and the newly minted db 2861 (`P156`; 2669 before the §8 renumber).

### Rule 3 — A partnership *phase* is not a new organization

Baumritter founded his press in 1864, took in his son-in-law Ganshar in 1891,
and separated from him in 1895. `בוימריטטער וחתנו גאנשאר` is **db 157 during
those four years**, not a fourth org — the firm continues either side of the
partnership.

This sits beside precedent (b), and the distinction is: a *standing* joint
name ("X and Y Troupe") is its own entity; a *phase* of one continuing firm is
not.

### Rule 4 — A firm that MOVED is one row; simultaneous branches use `parent_db_id`

The Bletnitski Brothers were an Odessa bookshop that had books printed in
Vilna — one row (db 2853; 2661 before the §8 renumber), seat in `address`, earlier seat in the notes. Had
they run shops in both cities *at once*, that would be the Forverts shape and
`parent_db_id` would apply.

---

## 4. The open question — ANSWERED by the survey

> This section originally recommended adopting Rules 1–4 and deferring the
> per-proprietor question. The survey in §5 (done 2026-10-10) changes the
> reasoning and sharpens the recommendation. The original framing is kept below
> as §4.1 because the dilemma it states is correct; §4.2 is the answer.

### 4.1 The dilemma as originally stated

Rule 1 flattens the dynasty into `name_variants`. That is cheap and needs no
schema change, but it is **lossy**: you cannot then ask *who ran this press in
1880?*, or attach a particular edition's imprint to the proprietor who actually
issued it — which the Lateiner/Hurwitz edition work (see
`YiDraCor/data/editions.csv`, which records publisher and printer per edition)
will eventually want.

The alternative is a **row per proprietor**, each pointing at the house with
`parent_db_id`, with dates on each. That is queryable and honest to the history,
but it extends `parent_db_id` from "branch in another city" to "proprietor in
another decade"; it needs date fields the schema does not have; and it
multiplies rows a matcher must choose between, which is the problem Rule 1 was
avoiding.

### 4.2 What the survey settles

**The dichotomy was a false one.** Both horns are attested and both fail in the
way §4.1 predicts — ISNI holds Plantin-Moretus as one flattened record and
Elzevier as three unlinked ones, and the survey's own verdict is that this is
"not a rule but an artifact of what the contributing library did" (§5.5). The
flattened form loses succession *structure*; the split form loses *identity*.
Neither is recoverable from the other.

What every modern model does instead is **keep one entity for the house and hang
the proprietors off it as dated relations** — not as name strings, and not as
sibling rows. W3C ORG draws our exact line: a change of proprietor is *not* an
`org:ChangeEvent`, because that is reserved for when "the resulting organization
is sufficiently distinct … that it has a distinct identity and distinct URI"
(§5.6). Wikidata is the countable proof: ~120 publishing houses use dated
`P127 owned by` on a single item, and a sweep for per-proprietor succession items
among publishers returns **zero** father-to-son cases (§5.5a).

So:

- **Rule 1's conclusion is right and is now externally corroborated.** One row
  per house. Keep it.
- **Rule 1's mechanism is wrong.** `name_variants` is the one thing the survey
  unanimously identifies as the lossy choice — CERL's live Moretus record shows
  "Witwe und Söhne" and "heirs" sitting as untyped strings (§5.2); Wikidata's
  trade names survive only as undated aliases (§5.5a); and MARC's own threshold
  says a changed proprietor's given name is a *name change*, explicitly not a
  variant (§5.1). It is also unprecedented here: the Kaminski merge it cites left
  `name_variants` empty (§5.7).
- **The per-proprietor alternative is not needed to get the queryability**, and
  the survey gives a cheaper route to it (§4.3).

### 4.3 Recommendation

**Adopt Rules 1–4, with Rule 1's mechanism replaced, and do not change the
schema yet.** Specifically:

1. **One row per house** — Rule 1 as written, canonical row keeps the earliest
   `db_id`. Lebenzohn is db 24.
2. **The proprietor sequence does not go in `name_variants`.** It stays where it
   already is and already works: the decision ledger
   (`publisher_audit_decisions.tsv`), which holds dated prose *with its source*
   — which is more than any `core_db` column could carry today, and more than
   CERL's or Wikidata's flattened forms retain. `core_db` has no notes column at
   all (§5.7), so this is not a choice between ledger and row; the row cannot
   hold it.
   Put in `name_variants` only what a *matcher* needs — the surface forms that
   should align to this row.
3. **Do not extend `parent_db_id` to proprietors.** Keep it meaning
   "simultaneous branch", as all 10 rows in three groups now use it (§5.7). The
   ontologies agree this is a different relation from succession.
4. **Reserve succession links for genuine discontinuity**, following RiC-O and
   `org:ChangeEvent` — i.e. when a house ceases to be itself, not when it changes
   hands. We have `deprecated`/`merged_into` for the merge case already.

**Why not build the dated model now, even though it is the right destination:**
the typed-succession vocabulary that would justify it serves **about six
printing mentions**, one of which ("heirs of the author") is not a firm
succession at all (§5.8). Building relation machinery for six rows is the wrong
order of work. The ledger holds the facts losslessly in the meantime, and
RiC-O's property-chain design (§5.6) is the standing proof that an undated
assertion can be upgraded to a dated one later without rework.

**What to do instead, now, cheaply:**

- **Fix Romm — but as a decision for Sinai, not a cleanup.** It is a live
  defect independent of any rule: db 495 and db 953 are one Vilna house, one of
  them with no `org_type`, with the "brothers" phase (1839–1862) attached to the
  *Farlag* row (§5.7). *Nothing has been changed* — merging them would be the
  first application of Rule 1 and therefore exactly the kind of accidental
  decision §6 was right to avoid. The proposed merge, for approval: survivor
  **db 495** (earliest id, per Rule 1), which also needs its `org_type` set;
  db 953 deprecated with `merged_into=495`; its two clusters folded in.
  **Counter-argument to weigh first:** a *Farlag* and a *Drukeray* can be
  genuinely distinct concerns — publishing house vs. printing shop — and Romm
  was both. If the PI reads them as two functions of one house, Rule 1 merges
  them; if as two businesses, Rule 2's overlap test splits them, and the 1887
  *Farlag* vs. 1839–1862 *brothers' press* dates do not settle it either way.
  Reconcile to Wikidata **Q7193734** and NLI J9U **987007477320405171** /
  GND **1131194608** whichever way it goes.
- **Record the succession type where the imprint states it**, as text, in the
  ledger: `וחתנו` (son-in-law) for Baumritter, `אלמנה`/`אחים` for Romm. Not a new
  column — just don't discard a typed relation the source handed us (§5.4).
- **Leave Lebenzohn db 24 unaligned**, as §6 did. Under this recommendation the
  single undated Yosef mention aligns to db 24, but note it is one mention
  against a four-generation Friedberg account; aligning it decides nothing and
  can wait for Sinai.

**The one decision to escalate:** whether `core_db` should gain a time dimension
at all. Do not take it for dynasties alone. `TROUPE_TYPOLOGY_PROPOSAL.md` hits
the identical wall from the other side and reaches the same words — "a
per-mention rather than per-entity model and a substantially larger change"
(§5.9). The mention layer already carries dates on **61% of printing/publishing
mentions** (§5.7), so the raw material exists and the question is only whether
`core_db` should receive it. That is one schema decision serving both proposals,
and it should be taken once, with the PI, on both agendas together.
**§10 states that question in the form a decision can be taken on.**

---

## 5. Outside practice — surveyed 2026-10-10

The brief in this section asked six questions and said not to reason from first
principles. Answers below, with what each practice keeps, what it does with
dates, and what it loses. **The survey changed the recommendation in §4**; the
revision is recorded there and the reasoning here.

### 5.0 The headline

Two traditions answer the §1 question in opposite directions, and the split is
not a disagreement about history — it is a difference in what each tradition is
able to express.

| | unit | succession | why |
|---|---|---|---|
| **Cataloguing** (MARC/RDA, CERL, BBTI, SBTI, STCN) | one record **per proprietorship** | a link between records | records pre-date reification; a record has one name, so a new name needs a new record |
| **Ontologies** (RiC-O, CIDOC-CRM, W3C ORG, Wikidata) | one entity **per house** | a *dated relation* hanging off it | can attach dates to a relation, so identity and chronology need not compete |

So "one row or three rows?" is the wrong question to inherit from the
catalogues. Their N-records answer is a **workaround for a schema that cannot
date a relationship** — which is exactly the limitation `core_db` currently has.
That makes the catalogues the closest match to our present capability and the
ontologies the closest match to where we would go if we fixed it.

### 5.1 MARC/RDA — several entities, and the §5 question is a non-question

A real name change produces a **new authority record**, linked by `510` with
`$w a` (earlier) / `$w b` (later), or in RDA-era form `510 $w r $i Predecessor:`.
SHM H 460 states it flatly: "**When any of these types of name change occurs,
separate name authority records are created for each of the corporate body's
various names.**"

The brief asked whether practice treats the press under Yosef as a *later name*
or a *successor body*. **RDA refuses the distinction**: its glossary files
name-change predecessors under *related corporate bodies* — the same bucket as
subsidiaries. Both route to identical structure. The question cannot be
answered because cataloguing does not ask it; it only asks whether the name
changed.

Two consequences land directly on Rule 1:

- The real threshold is **variant vs. name change** (LC-PCC PS 11.2.2.5), where
  "minor" is limited to spelling, abbreviation, punctuation, articles and
  prepositions. **A changed proprietor's given name is not minor.** By this
  standard Yoel → Yosef is a name change, *not* a variant — the direct opposite
  of Rule 1's `name_variants` mechanism.
- There is a **one-way ratchet**: once split, PS 11.2.2.5 forbids re-collapsing
  without explicit evidence. Early guesses harden permanently. An argument for
  deciding deliberately rather than by accident — see the Romm finding in §5.7.

On the person/firm question that `דפוס של ר" יוסף לעבענזאהן` raises: rare-books
practice (Beinecke, DCRMR) defaults a one-man press to a **personal** name with
an `$e printer` relator, and makes a `(Firm)` corporate record only on evidence
of a stable entity. Both may coexist, and **nothing machine-readably links the
man to the firm.**

*Dates:* RDA 11.4.3/11.4.4 → MARC `046 $q`/`$r`, but **core only when needed to
disambiguate**, and scoped per record — so they date *the name*, not the
business. You cannot say "firm 1818–1900, under Yosef 1873–1900".

*Loses:* business continuity; the reason for transfer (`$w a`/`b` is pure
direction — inheritance, sale and death encode identically); overlap and gradual
handover; person↔firm identity.

### 5.2 CERL Thesaurus — the closest precedent, and it rules against Rule 1's mechanism

CERL is built for exactly our problem and its rule is explicit:

> "If a printing business … is handed over from one owner to another, **a new
> record is created** and a reference (successor/predecessor) is made from the
> earlier entity to the later and vice-versa."

Stronger than MARC: **a new owner means a new record even when the name does not
change.** Record types are `cni` (imprint name — printers/publishers/
booksellers), `cnp` (person), `cnc` (corporate body), `cnl` (place); note the
deliberate `cni`/`cnp` split, where one human gets a person record for his
intellectual life *and* a separate imprint record for his trade identity
(Plantin has `cnp01428077` plus two `cni`s). Succession is field `510`, with a
closed four-value vocabulary — `ex:hasPredecessor`, `ex:hasSuccessor`,
`ex:hasCollaborator`, `ex:hasRelatedEntity` — carrying `$3` target id, `$z`
relationship-specific dates, `$n` note, `$s` source.

**But there is no field for the firm itself**, and this is where Rule 1's
failure mode is visible in the wild. Record `cni00047960` (Joannes Moretus,
Plantin's son-in-law successor) carries:

```
variantForm descriptive: Moretus, Joannes, I, Witwe und Söhne
variantForm descriptive: Moretus, Joannes, I, dziedzice      [= heirs]
variantForm descriptive: Moretus, Joannes, II.
```

"Widow and sons" and "heirs" sit as **untyped descriptive strings**,
multilingual and unparsed, with Joannes II collapsed into Joannes I's record.
You cannot ask which presses were widow-run, or distinguish "widow alone" from
"widow with sons" from "sons after the widow". **This is precisely what Rule 1
proposes to do to us.**

*Dates — the part worth copying almost verbatim:* field `340`, indicator 1
distinguishing biographical (`0`) from **dates of activity** (`1`); `$a` free
human text ("ca. 390 – 320 v. Chr"); `$x` a parsed 10-char form `µBBBBµEEEE`
where µ is `a` (AD), `b` (BC) or **`u` (unknown)**, giving open-ended ranges
(`$xa1737u####`); **repeatable for contradictory statements**. Field `350 $z`
dates a specific role.

*Coverage, honestly:* our population is essentially absent. Baumritter 0
records; Lebensohn 4 records, all the Vilna *literati* (person records, no
imprint record, no dates); Orgelbrand present as **two unmerged duplicates**.
We would be minting, not matching.

*API trap worth recording:* `data.cerl.org/thesaurus/_search` now sits behind an
**Anubis proof-of-work bot wall** that returns a challenge page — which looks
exactly like zero results. The **SRU 1.2 endpoint** (`/thesaurus/_sru`) is not
gated and works. Same genre of failure as the gated basemap tiles.

### 5.3 Book-trade indexes — several entities, and only CERL has a real edge

| resource | entities | dates | succession recorded as |
|---|---|---|---|
| **BBTI** | several; **firm separate from founder, deliberately unlinked** | trade vs. biographical axes, admitted to conflict | **free-text notes** + `son John` / `widow` in the forename field |
| **SBTI** | several (persons and firms in one namespace) | **per-address chronological ranges** | prose biographical section |
| **STCN** | several, one per person/place | kmc 740/741/742–3 | **typed code** kmc 009 pos. 4: `w` weduwe, `e` erven, `c` compagnie + Roman numerals |
| **CERL** | several **by rule** | 340 activity + `510 $z` per relationship | typed, dated, directional edge |

BBTI is the cautionary one. Its practice is "**list known businesses separately
from the founder as an individual**", and it then *refuses to link*: "unless two
(or more) records are known without any doubt to refer to the same person, they
have not been conflated" — applied to family relations explicitly. Father, son,
widow and firm become four unlinked rows. Its FAQ even names our hazard:
"confusion may arise between founders and their businesses, which **sometimes
outlived their creators**."

STCN contributes the one cheap borrowable primitive: succession *type* as a
**queryable code** rather than prose, which is what makes "every widow-run
press" answerable.

### 5.4 Hebrew and Yiddish practice — the relation type is already in the imprint

Hebrew bibliography has the **imprint convention but not the data model**, and
the convention is richer than anything above: succession is written *into the
name*.

`דפוס ראם` → **`דפוס האלמנה והאחים ראם`** ("Press of the Widow and Brothers
Romm") — renamed after David Romm's death in 1862, on title pages from 1871,
later a joint-stock company. Also `בני ראם` (sons), `יורשי` (heirs), `וחתנו`
(and his son-in-law).

So the succession-type vocabulary that STCN encodes as `w`/`e`/`c`, our sources
hand us **as language**. Our own `בוימריטטער וחתנו גאנשאר` is a *typed* relation
— "and his son-in-law" — that Rule 3 currently discards into a name variant.

*But see §5.8 for the size of this.* The vocabulary is real and it is in our
corpus; it is also very small in the printing domain, and that bears on what is
worth building.

What Hebrew practice loses: everything the Dutch code and the CERL edge capture.
The type is recoverable only by parsing the string, there is no directional
link, and the dynasty is invisible as a unit — the Vilna Shas (1880–86) and an
1850s Romm imprint look like unrelated publishers.

Infrastructure note: NLI **does** hold an authority record
`Defus ha-almanah ṿeha-aḥim Rom (Vilnius)` — J9U `987007477320405171`, GND
`1131194608`. Both `nli.org.il` and `viaf.org` return 403 to automated fetches,
so whether NLI holds one record or two for pre- and post-1863 Romm is
unconfirmed. **Actionable regardless:** our Romm rows can carry these external ids.

### 5.5 VIAF and ISNI — the natural experiment

The most useful empirical result in the survey: **the same system produces both
horns of our dilemma, depending only on what a cataloguer did.**

**Elzevier — three unmerged ISNIs, zero relations between them:**

| ISNI | name |
|---|---|
| `0000000119456401` | Officina Elzeviriana (variant "Elzevir Press (1593–1712)") |
| `0000000110907878` | Johannes Elzevier Witwe und Erben |
| `0000000083379263` | Vidua & haeredes Joannis Elzevirii |

The last two are the *same* widow-and-heirs firm in German and Latin, unmerged,
sitting beside a house-wide record.

**Plantin-Moretus — one ISNI** (`0000000123372753`) holding the generations as
name variants: "Officina Plantiniana Raphelengii", "…Balthasaris Moreti (II,
Anvers)", "Joannes Moretus Witwe und Söhne", "Moretussen".

> ISNI's behaviour here is **not a rule but an artifact of what the contributing
> library did.**

Read that against §4: the flattened case (Plantin) loses succession
**structure** — sequence and dates are unrecoverable; the split case (Elzevier)
loses **identity**. **You cannot get both without a continuant for the firm.**
Rules 1 and the per-proprietor alternative are these two outcomes, and the
survey says each fails in the predicted way.

Mechanics: VIAF's clustering unit is the *contributed authority record*, not the
historical entity, and it **does not follow `510 $w a`/`b` as merge
instructions** — its guidelines file 5XX as "associated names (cross-references)",
a different category from the 4XX variants that drive clustering. It also
*penalises* merging: "VIAF tries not to include more than one record from each
source in a cluster. Duplicates often result in clusters being split." A library
holding two records for two phases therefore guarantees two clusters.

ISNI's unit is the **public identity**, and in principle one ISNI *can* span a
dynasty. The gap is admitted verbatim by OCLC Research (2016): "ISNI and LC/NACO
differ in their treatment of name changes that are not associated with a change
in the role and function of an organization… ISNI needs to determine a way to
accommodate multiple VIAF/LC/NACO identifiers… and to define a clear workflow
and relationship designators." Appendices titled "Dealing With an Organization's
Many Name Changes", "Organizational Restructuring", "…Mergers" — unsolved since
2016. ISNI *has* the relations (NISO I2 `supersedes`/`isSupersededBy`,
`formerName`); `organisationRelation` was **empty in every historical printer
record retrieved**.

*Coverage: empty for our population.* Romm — 59 ISNI records, one organisation,
a Québec marine-mammal network; `דפוס ראם` returns nothing. Lebensohn 5 records,
0 organisations. Baumritter 5/0, where the only Jewish-Warsaw hit is Gustaw
Baumritter absorbed as a **name variant of a lyricist** — our exact
person-swallows-firm hazard. Goldfarb 236/9, all modern. **Neither system can be
our authority; both are external-id attachments where a hit exists.**

*Status 2026, for anyone planning to script this:* VIAF is operating but
degraded (Bargioni, *ITAL*, Dec 2025) — `justlinks.json` dead since May 2024 and
never replaced, most endpoints broken (`viaf/{id}`, SRU and AutoSuggest still
work), newest dump still dated 2024-08-04, and the **URI Registry is gone**, so
a deprecated cluster id now returns a bare 404 indistinguishable from
never-issued. `viaf.org` 403s all scripted access. **ISNI's SRU endpoint
(`isni.oclc.org/sru`) is the only one of the two currently scriptable.**

### 5.5a Wikidata — the decisive count

Wikidata is the easiest comparison and gives the clearest verdict, because it
can be counted rather than characterised.

**The pattern is one item for the firm, with dated `P127 owned by`.** About
**120** publishing-house items carry two or more differently-dated `P127`
statements. The cleanest is **Hartleben's Verlag (Q278706)**: P127 → Q1034192
[1802–1863] → Q55844114 [1863–1903] → Q55674803 [1869–1918] → Q94779966
[1918–1947]. Founder to son to in-laws, four proprietorships, **one item**.
Also Garnier Frères (Q733801), Marquis Who's Who (Q1040912, five owners
1899–present), Ullstein (Q451462, five P127 whose end-dates record the
Aryanization). The inverse exists in bulk from the person side: printers with
dated `P1830 owner of` (e.g. Charles Broster Q29756860 → North Wales Gazette
[1817-02 – 1827-06]).

**The per-proprietor-item model has no instantiation at all.** A SPARQL sweep of
publisher-subclass items carrying `P1365`/`P1366` returned only *corporate*
replacements — Nixdorf → Siemens Nixdorf, École Libre des Sciences Politiques →
Sciences Po — every one a dissolution-and-refoundation of a legal entity.
**Zero father-to-son publishing cases.**

The property definitions say why, and they answer §5's question directly.
`P1365 replaces` instructs: "Use 'follows' (P155) if the previous item was not
replaced or **predecessor and successor are identical**." A firm that merely
changed proprietor falls in that gap — so the guidance is **don't mint a second
item**. Meanwhile `P127`'s allowed-qualifiers constraint whitelists `P580`,
`P582`, `P585` **and `P1365`/`P1366` as qualifiers**, while `P155` is not
whitelisted. Wikidata expects succession *inside* an owned-by statement, not as a
parallel chain of firm items.

**Romm is in Wikidata as one item — Q7193734** (`דפוס ראם`, "publisher of Jewish
religious literature (1788–1941)"), P571 = 1788, P576 = 1941, and it uses dated
`P159` qualifiers for the Grodno → Vilna move [1788–1799; 1799–1941]. But
**`P127` is absent entirely**: the Widow-and-Brothers phase is not modelled, and
Devorah Romm (Q137999556) carries `P31=Q5` and *nothing else* — her description
says she managed the press, no statement does. The dynasty is severed. Our two
Romm rows (§5.7) can nonetheless be reconciled to Q7193734.

Two further cautions from real items. **Plantin (Q5254948)** flattens the
son-in-law succession into a *second* `P112 founded by` (both Plantin and Jan
Moretus), which erases that one succeeded the other 35 years later — succession
becomes an unordered set; it also mis-uses `P156` to point at the *museum*, and
has an **uncaught duplicate (Q42168553)** for the same Antwerp press, unlinked by
`P1889`/`P460`. **Elzevier (Q602323)** is `P31` **family *and* company
simultaneously** — the family/firm conflation unresolved in the item itself. Both
are hazards we can name in advance.

*What the dated-`P127` model loses*, and this is the one that matters for us:
**the trade name.** "Widow and Brothers Romm" / "Moretus Press" survives only as
an undated *alias* string. A dated `P127` cannot say *the imprint changed in
1860*. For bibliographic work where the title page is the evidence, you cannot
ask "what was this house called in 1878." Phase-scoped facts suffer too — one
item means one `P31`, so a firm that printed under the father and only published
under the son cannot date that change.

### 5.6 Ontologies — one entity, dated relations, and they draw our line for us

RiC-O 1.1, CIDOC-CRM 7.1.3 and W3C ORG converge: **one persistent id for the
house, plus reified dated relations for who held it when.** None mints a new
entity on a change of name or owner.

**W3C ORG is the lightest (~30 terms) and states our boundary explicitly.**
`org:ChangeEvent` is gated to cases where "the resulting organization is
sufficiently distinct from the original organizations that it has a **distinct
identity and distinct URI**". So a father-to-son handover is *not* a change
event — it is a new dated holding on the **same** organization id. A change
event is for when the house ceases to be itself. **That validates Rule 1's
conclusion while refuting its mechanism**: one entity, yes — but the proprietors
are dated *relations*, not name strings. Minimal pattern:
`org:FormalOrganization` + `org:Post`/`org:Membership` + `org:memberDuring` →
`time:Interval`. Limits: `ChangeEvent` has no date property of its own, no
merger/split/rename vocabulary, and ORG has **no ownership relation** at all.

**RiC-O 1.1** (released 2025-05-22) offers the most relevant single fact for an
incremental path: its binary shortcuts (`rico:hasOrHadOwner`,
`hasOrHadLeader`) are derived from the reified relations by **OWL 2 property-chain
axioms**, so you may assert a plain undated triple now and reify **only when you
have dates**. Its `CorporateBody` scope note explicitly covers a one-man press:
"a sole trader or sole proprietor may be recognized as a corporate body, even
when the economic enterprise does not have additional members." `rico:Position`
"exists independently of the Person that holds the Position" — our case exactly.
Succession is reserved for genuine discontinuity (`rico:hasSuccessor`, with
subproperties `wasMergedInto` / `wasSplitInto`).

*Three corrections to this document's own §5 brief:* `rico:isOrWasSuccessorOf`
**does not exist** (the real terms are `rico:hasSuccessor` / `isSuccessorOf`);
`rico:Identity` **is not a class** in RiC-O 1.1 (agent/name separation is carried
by `rico:AgentName` + `hasOrHadAgentName`); and in CIDOC-CRM's bibliographic
extension, `F51 Name Use Activity` **was removed** in LRMoo 1.0, replaced by
`F12 Nomen`.

**CIDOC-CRM** is heaviest and only worth it if we ever model the printed objects
event-centrically: `E74 Group` persists by construction (membership change is an
`E85`/`E86` event *on* the group; ending requires an explicit `E68 Dissolution`),
a proprietorship interval is `E7 Activity` typed "proprietorship" + `P14 carried
out by` + `P4 has time-span` → `E52`, and `E8 Acquisition` (`P22`/`P23`/`P24`)
models the handover moment. **PROV-O is the wrong tool** — `specializationOf` /
`alternateOf` are abstraction levels, not temporal succession.

The borrowed lesson, stated by the survey and worth keeping: *for a family press
the **imprint** changes far more often than the **firm** does, and conflating the
two is the usual modelling error.*

### 5.7 What the survey found in our own data

Two things that change the stakes, both verified in `core_db.tsv` and
`organizations_clustered.tsv` on 2026-10-10.

**a. The Romm press is already split — by accident, in both directions at once.**
The canonical Hebrew printing dynasty is in our DB twice:

| row | name | org_type | clusters |
|---|---|---|---|
| **db 495** | `דרוקעריי פון ראָם` | *(empty)* | ORG-C00071 |
| **db 953** | `ראָם-פֿאַרלאַג` | Publisher | ORG-C00940, ORG-C04980 |

and the mention evidence shows all three clusters are one Vilna house:
`דרוקעריי פון ראָם` (undated), `ראָם-פֿאַרלאַג` (**1887**), and
`דרוקעריי פֿון די ברידער ראָם` — "press of the Romm **brothers**", dated
**1839–1862**. So db 953 holds two phases 25 years apart while db 495 holds the
undated generic form and carries **no `org_type` at all**. The undated bare-name
mention that §4 says would be "unalignable without guessing a date" has
**already been aligned** — to a row of its own. The question is not hypothetical
and it is not open: it has been answered twice, inconsistently, without anyone
deciding. Romm is therefore the right test case for whatever rule is adopted,
and it is also a live defect regardless of the rule.

**b. The mention layer already carries dates.** `organizations_clustered.tsv`
has `relations.date_start` / `date_end`, `locations.datefrom` / `dateto` and
`founding_date`. Of **590** mentions typed printer/publisher, **365 (61%) carry
at least one date**; 355 have `date_start`. §4's objection that a per-proprietor
model "needs date fields the schema does not have" is true of **`core_db` only** —
the raw material for a dated model is already upstream, and the gap is purely
that `core_db` has nowhere to receive it.

Two smaller corrections to this document: `core_db.tsv` has **no notes column**
(and no date columns), so §6's "with the Friedberg succession in its note" is
wrong about location — the succession is in `publisher_audit_decisions.tsv`
(`P058` rationale), and `core_db` *cannot* hold it. And `parent_db_id` now has
**10 rows in three groups**, not two: §2(c) misses Unzer Vinkl
(1785–1787 → 427), still all simultaneous city branches, so the precedent holds.

Also: Rule 1 says each proprietor's name goes in `name_variants`, citing the
Kaminski merge as precedent — but **db 147 has an empty `name_variants`**. Only
Rule 1's *conclusion* is precedented; its *mechanism* is new. Of the dynasty rows,
only db 2853 (Bletnitski) models variants well; db 2861 was minted bare, with no
variants and no address, and db 24 is a three-field row.

### 5.8 One finding that argues against building anything yet

§5.4 makes the typed-succession vocabulary look attractive. Scanning all 16,454
org mentions for אלמנה / אחים / בני / יורשי / חתן / שותפים says otherwise **for
presses**:

| form | mentions | of which printer/publisher |
|---|---|---|
| `אלמנה` widow | 1 | 1 — `דרוקעריי פון דער אלמנה און יתומים פּרופּס` (widow **and orphans** — a third type) |
| `יורשי` heirs | 1 | 1 — `הוצאת יורשי המחבר` (heirs of **the author**, not a firm succession at all) |
| `וחתנו` son-in-law | 1 | 1 — `בוימריטטער וחתנו גאנשאר` |
| `אחים`/`ברידער` | 19 | 3 |
| `בני`/`און זון` | 10 | **0** |
| `שותפים`/`& Co` | 0 | 0 |

So the vocabulary is real, and in the printing domain it is **about six
mentions**, one of which is not a firm succession. A typed succession relation
would be linguistically well-founded and would serve almost nothing today.

Note where the forms *are* frequent: `ברידער` and `און זון` are overwhelmingly
**troupes** (`געברידער שטיין`, `ברידער טשיסטאָגורסקי`, `טרופּע פֿון אַברהם
פישזאָן און זון`). If a succession vocabulary is ever built it should be built
**once, for troupes and presses together** — which is the same conclusion §5.9
reaches from the other direction.

### 5.9 This is one schema question, not two

`TROUPE_TYPOLOGY_PROPOSAL.md` reaches the identical wall independently. Its §2
observes that "`core_db.tsv` has **no subtype or attribute columns** … every
distinction a cataloguer might want to draw has to be squeezed into the single
`org_type` string", and its open question 5 asks whether the facet set is
time-invariant, concluding: "**If period-specific values matter, that is a
per-mention rather than per-entity model and a substantially larger change.**"
Its open question 3 already flags double-encoding risk against the same
family-continuity merges this document builds on.

Dynasty succession and troupe facets are **the same gap**: `core_db` models
entities with no time dimension, while the mention layer has dates. They should
be decided together, which is what §4's recommendation now says.

---

## 6. What the audit did in the meantime

Nothing irreversible. Under current conventions:

- **Lebenzohn** (db 24) stays as it is, flagged `P058`, with the Friedberg
  succession recorded in the **decision ledger** — `publisher_audit_decisions.tsv`,
  the `P058` rationale. (Not "in its note": `core_db.tsv` has no notes column,
  so the row *cannot* carry it. db 24 is a three-field row — name, `org_type`,
  nothing else. See §5.7.) **No cluster was linked to it** — `דפוס של ר" יוסף
  לעבענזאָהן` is deliberately left unaligned, because aligning it would decide
  this question by accident.
- **Baumritter** (db 157) took the partnership cluster under Rule 3 (`P057`).
- **Goldfarb** was split under Rule 2 (`P059`), minting a row for M. —
  now **db 2861** after the `P173` renumber (§8).
- **Bletnitski** was minted as one Odessa row under Rule 4 (`P148`) —
  now **db 2853** after the `P173` renumber (§8).

Reversing any of these is a cluster-link edit, not a data loss.

**Superseded in part:** see §7 for what was applied on 2026-10-11, including the
id renumber that moved every row this audit minted.

---

## 7. Applied 2026-10-11 (recommendation accepted)

Sinai accepted §4.3. Applied, with the ledger entries that record each:

| | what | where |
|---|---|---|
| **`P162`** | **ID collision fixed.** All 9 publishers this audit minted (2661–2669) collided with **9 real theatres already on `origin/main`**. Local main was 2106 commits behind, so the audit minted from a stale high-water mark of 2660; origin's true max is **2717**. Renumbered **2661–2669 → 2718–2726** in `core_db.tsv` (9 rows) and `org_alignment_review.tsv` (16 `aligned_db_id` rows). Verified: 0 remaining collisions, 0 other rows altered. Nothing was lost — the publishers were local-only, the theatres origin-only. | `core_db.tsv`, `org_alignment_review.tsv` |
| **`P163`** | db 495 `דרוקעריי פון ראָם` had an **empty `org_type`** → set to `Printer` (its mention reads *drukeray*). True either way the Romm question goes. | `core_db.tsv` |
| **`P164`** | db 157 Baumritter had an empty `address` while its own aligned mention states one → `רחוב ביעליינסקע 16, Warsaw`. | `core_db.tsv` |
| — | db 24 Lebenzohn `name_variants` → attested **surface forms only** (`לעבענזאהן`, `לעבענזאָהן`, `Lebenzohn`, `Lebensohn`), per §4.3(2): variants carry what a *matcher* needs, **not** the proprietor sequence. | `core_db.tsv` |

**Deliberately not done:** the typology casing sweep (`P166`) — below. The Romm
merge (`P165`) was deferred to Sinai and has since been **approved and applied**
as `P168`; see below.

### Still open, and why

- **`P165` — RESOLVED 2026-10-11.** Sinai chose to merge; applied as **`P168`**
  (db 953 → **db 495**, typed `Printer/Publisher`, all three clusters on the
  survivor). This was the **first application of Rule 1** and it held: the rule
  decided the survivor (earliest id), and the evidence — not the rule — decided
  that the three name-forms are one house. Full evidence and what was applied is in
  **[`ROMM_DECISION_MEMO.md`](ROMM_DECISION_MEMO.md)** (every attestation, how the
  split happened, and what the dates really mean). db 495 and db 953 are one
  Vilna house (§5.7) and the proposed merge is survivor **db 495**, db 953
  deprecated with `merged_into=495`. But a *farlag* and a *drukeray* may be two
  functions of one house (→ Rule 1 merges) or two businesses (→ Rule 2 splits),
  and the dates — 1887 for the *Farlag*, 1839–1862 for the brothers' press — do
  not settle it. Merging unasked would be precisely the accidental decision §6
  was right to avoid, so **nothing was changed**.
- **`P166` — off-typology `org_type` casing is a pre-existing, repo-wide defect:**
  **1,380 rows** across 34 non-canonical spellings (`traveling company` 573,
  `education` 218, `jewish political bodies` 147, …, `publisher` 3, `printer` 2).
  Mostly lowercasing, some Title-Case. Far outside this audit's scope, and a blind
  case-fold risks colliding with the app's unknown-value dropdown fallback.
  `Non-Yiddish Theatre` is a **legitimate** type, not a casing error. Needs its own pass.
- **The schema question** (§4.3, last paragraph) — whether `core_db` gains a time
  dimension — remains for the PI, jointly with `TROUPE_TYPOLOGY_PROPOSAL.md`.

---

## 8. Renumbered again, and merged — 2026-10-12

| | what | where |
|---|---|---|
| **`P173`** | **ID collision, third round.** The deployed app mints on every Save, so it had taken **2809–2820** for theatres and amateur groups while this work sat unmerged — overtaking the `P162` renumber (§7) exactly as `P162` had overtaken the original mints. Renumbered **2809–2820 → 2853–2864**. The first attempt placed them at 2846–2860, clear of origin's then-maximum of 2845 — and the app minted 2846–2849 during the merge itself, so a **fourth** move was needed before anything could land. Remapped the `db_id` column only, in `core_db.tsv` (12 rows), `eponym_candidates.tsv` (14 rows) and YiDraCor's `edition_org_links.tsv` (3 rows, `P171`/`P172`). A textual replace would have corrupted `eponym_candidates.tsv`, where `2809` is also a **person** hub id (`HUB-D2809`) and 1809–1820 are person db ids. | `core_db.tsv`, `eponym_candidates.tsv`, `edition_org_links.tsv` |
| **`P174`** | **`P162`'s destination was wrong for nine rows.** In `org_alignment_review.tsv`, 2809–2817 was carrying *two* sets of rows: Sinai's 19 Publisher `ALIGN` cells and **Noa's 9 `NEW` rows**, which the `P162` sweep had dragged along by mistake. Origin had already minted Noa's nine societies at **2718–2726** and both sides' `core_db` already agreed on them — only this column was stale. So the `ALIGN` cells follow `P173` to 2853–2864 while Noa's `NEW` rows were **restored to 2718–2726**. | `org_alignment_review.tsv` |
| **`P175`** | **Merged `origin/main`** as a union, not either side — these files are append-only and each side had filled cells the other left empty, so `--ours`/`--theirs` would have discarded one party's decisions. `core_db` 2794 rows = origin's 2782 + the 12 publishers. One true conflict, `ORG-C00594` (בראַנקס אַרט טעאַטער): both sides' `core_db` put that cluster at db **2805**, so origin's `aligned_db_id` stood and the branch's 2807 — collateral from an earlier sweep — was dropped. `activity_log` took both sides' appends with 502 shared upstream rows deduplicated. | all three |

**The ids this audit minted, final:**

| P | entity | db |
|---|---|---|
| `P148` | Gebrider Bletnitski (Odessa) | **2853** |
| `P138` | פאַרלאַג ת. יאָקובסאָן און מ. גאָלדבערג | **2854** |
| `P140` | פאַרלאַג ב. שימין | **2855** |
| `P142` | פאַרלאַג ש. שרעבערק | **2856** |
| `P143` | פֿאַרלאַג יאָטשקאָווסקי | **2857** |
| `P145` | י. אַלאַפּין | **2858** |
| `P150` | ל. מאָרגענשטערן | **2859** |
| `P153` | אברהם הירש קאַצענעלבאָגן | **2860** |
| `P156` | פערלאַג מ. גאָלדפֿאַרב | **2861** |
| `P169` | L. Goldfarb | **2862** |
| `P171` | Farlag "Di Yudishe Bihne" | **2863** |
| `P172` | Farlag "Kultur" (Warsaw) | **2864** |

**The standing lesson.** Four renumberings in three days is not bad luck: any
id minted locally against a stale `core_db` is a guess, because the deployed app
is minting at the same time and pushes on every Save. The app took four ids in
the forty minutes this merge was being prepared, which is why the gap has to be
re-checked immediately before the merge lands, not when the renumber is written.
Mint from `origin/main`'s high-water mark fetched at that moment and merge the
same minute — or, better, mint no id at all and let the app assign it. The `activity_log`
mint rows still record the **original** ids (2661–2669, 2805–2807); the log is
append-only history and was deliberately left as written.

---

## 9. Sources for §5

### Cataloguing standards
- MARC 21 Authority: [110](https://www.loc.gov/marc/authority/ad110.html) · [410](https://www.loc.gov/marc/authority/ad410.html) · [510](https://www.loc.gov/marc/authority/ad510.html) · [`$w` control subfield](https://www.loc.gov/marc/authority/adtracing.html)
- [Subject Headings Manual H 460 — Name Changes in Non-Jurisdictional Corporate Bodies](https://www.loc.gov/aba/publications/FreeSHM/H0460.pdf) (the "separate name authority records" rule)
- [LC-PCC Policy Statements, ch. 11](https://original.rdatoolkit.org/document.php?id=lcpschp11.pdf) (PS 11.2.2.5 variant-vs-name-change; the no-recollapse ratchet)
- [CONSER Cataloging Manual Module 18](https://www.loc.gov/aba/pcc/conser/CCM/Module18.pdf) · [LC RDA Training, Authorities II](https://www.loc.gov/catworkshop/RDA%20training%20materials/LC%20RDA%20Training/LC_RDA_Authorities%20II_Trainees%20Manual_08182012.pdf) · [LC-PCC Metadata Guidance: Corporate Body](https://loc.gov/aba/rda/mgd/corporateBody/mg-corporateBody.pdf)
- [Beinecke Rare Book Cataloging Manual — imprint tracings](https://rbc-cataloging-manual.beinecke.library.yale.edu/imprint-tracings-marc-700710-and-752) · [DCRMR 5.22 name of publisher](https://bsc.rbms.info/DCRMR/ppdm/Name-of-publisher/) (person-vs-firm default)
- *Not consulted:* the subscription RDA Toolkit, so RDA 11.2.2.6 is cited via LC's PS and training restatement, not verbatim.

### CERL Thesaurus
- [Format — list of fields](https://www.cerl.org/resources/cerl_thesaurus/editing/format) · [001 record identifier / prefixes](https://www.cerl.org/resources/cerl_thesaurus/editing/format/001) · [210](https://www.cerl.org/resources/cerl_thesaurus/editing/format/210) · [340 dates](https://www.cerl.org/resources/cerl_thesaurus/editing/format/340) · [350 activity](https://www.cerl.org/resources/cerl_thesaurus/editing/format/350) · [510 related imprint name](https://www.cerl.org/resources/cerl_thesaurus/editing/format/510)
- [**Guidelines on Editing Printers' Records**](https://www.cerl.org/resources/cerl_thesaurus/editing/printers) — the "new owner, new record" rule
- [Guide to editing / search indexes](https://www.cerl.org/resources/cerl_thesaurus/editing/newinterface) · [Indexing](https://www.cerl.org/resources/cerl_thesaurus/editing/indexing) · [SRU interface](https://www.cerl.org/resources/cerl_thesaurus/sru) · [CTAS 1.1 spec](https://www.cerl.org/_media/resources/cerl_thesaurus/ctas_v11.pdf) · [`cerl` PyPI](https://pypi.org/project/cerl)
- Records retrieved via SRU: `cni00047960` (Moretus, the untyped-variant evidence), `cni00047911`/`cni00051029` (Orgelbrand duplicates), `cnp01400267`/`cnp01272662`/`cnp02490340`/`cnp01378809` (Lebensohn)
- **Access note:** `data.cerl.org/thesaurus/_search` is behind an Anubis proof-of-work wall; use `/thesaurus/_sru`.

### Book-trade indexes
- [BBTI, Bodleian (offline holding page)](https://bbti.bodleian.ox.ac.uk/) · [Grub Street mirror](https://bbti.grubstreetproject.net/) · [search fields](https://bbti.grubstreetproject.net/search.php) · [**FAQ** — "businesses separately from the founder"](https://hcmc.uvic.ca/project/bbti/faq.html) · [Oxford ORA deposit](https://ora.ox.ac.uk/objects/uuid:19b63441-9497-4b07-903b-ec5742c44806)
- [SBTI dataset docs (data.nls.uk)](https://data.nls.uk/data/metadata-collections/scottish-book-trade-index/) · [NLS catalogue page](https://www.nls.uk/catalogues/scottish-book-trade-index/)
- [STCN Handleiding — drukkersthesaurus](https://www.kb.nl/kbhtml/stcnhandleiding/drukkersthes.html) (the `w`/`e`/`c` code) · [impressum kmc 4040](https://www.kb.nl/kbhtml/stcnhandleiding/4040.html) · [STCN Verantwoording](https://cerl.org/resources/stcn/stcn_ne/verantwoording) · [GGC richtlijnen](http://support.oclc.org/ggc/richtlijnen/php/showPresentation.php?id=13&ln=nl&par=p-thesaurus_drukkers_algemeen)
- Bibliopolis is **dead** — bibliopolis.nl redirects to [nederlandseboekgeschiedenis.nl](https://www.nederlandseboekgeschiedenis.nl/), which does not mention the old databases.

### Hebrew / Jewish printing
- [Bibliography of the Hebrew Book 1473–1960 (NLI)](https://www.nli.org.il/en/items/NNL_ALEPH002488716/NLI) · [NLI bibliographic databases](https://www.nli.org.il/en/research-and-teach/catalogs/bibliographic-databases) · [Vinograd, *Otsar ha-sefer ha-ivri*](https://catalog.hathitrust.org/Record/002815418) (arranged by city of press) · [AJL evaluation of Vinograd](https://ajlpublishing.org/index.php/jl/article/download/151/141/139)
- [NLI authority: *Defus ha-almanah ṿeha-aḥim Rom (Vilnius)*](https://www.nli.org.il/en/a-topic/987007477320405171) — J9U `987007477320405171`, GND `1131194608` (**403s automated fetches**)
- [Romm Family, YIVO Encyclopedia](https://encyclopedia.yivo.org/article/2210) · [Romm publishing house (Wikipedia)](https://en.wikipedia.org/wiki/Romm_publishing_house)

### VIAF / ISNI
- [VIAF Guidelines (OCLC)](https://www.oclc.org/content/dam/oclc/viaf/VIAF%20Guidelines.pdf) (4XX vs 5XX; duplicates split clusters) · [Hickey & Young, description of the VIAF dataset](https://www.semantic-web-journal.net/sites/default/files/swj294.pdf) · [Managing Ambiguity in VIAF](https://www.oclc.org/research/publications/2014/managing-ambiguity-viaf.html)
- [**Bargioni, "VIAF Governance Concerns about the Refurbished VIAF Web and API Interfaces," *ITAL*, Dec 2025**](https://ital.corejournals.org/index.php/ital/article/view/17384) — the 2026 degradation, incl. loss of the URI Registry
- [**Addressing the Challenges with Organizational Identifiers and ISNI** (OCLC Research, 2016)](https://www.oclc.org/content/dam/research/publications/2016/oclcresearch-organizational-identifiers-and-isni-2016-a4.pdf) — the admitted ISNI/NACO name-change gap
- [ISNI FAQs](https://isni.org/page/faqs/) · [ISNI Linked Data](https://isni.org/page/linked-data/) · [BISG on ISNI](https://www.bisg.org/isni) · [BookNet: imprint & publisher name in ONIX](https://www.booknetcanada.ca/blog/2019/1/17/imprint-and-publisher-name-opportunities-in-onix)
- ISNI records retrieved via SRU (`isni.oclc.org/sru`): Elzevier `0000000119456401` / `0000000110907878` / `0000000083379263`; Plantin `0000000123372753`
- **Access note:** `viaf.org` 403s all scripted access (Cloudflare); ISNI's SRU works.

### Ontologies
- [RiC-O 1.1 specification](https://www.ica.org/standards/RiC/RiC-O_1-1.html) · [**RiC-O 1.1 OWL file**](https://raw.githubusercontent.com/ICA-EGAD/RiC-O/master/ontology/current-version/RiC-O_1-1.rdf) (authoritative; term names verified against this) · [RiC-O GitHub pages](https://ica-egad.github.io/RiC-O/) · [RiC-O: a Semantic Framework](https://ceur-ws.org/Vol-3019/LinkedArchives_2021_paper_13.pdf)
- [CIDOC-CRM 7.1.3](https://cidoc-crm.org/html/cidoc_crm_v7.1.3.html) · [LRMoo 1.0](https://cidoc-crm.org/extensions/lrmoo/html/LRMoo_v1.0.html) (F51 removed, F12 Nomen) · [CIDOC Issue 371 — E74 Group](https://cidoc-crm.org/Issue/ID-371-e74-group-from-lrmoo-discussions)
- [**W3C Organization Ontology**](https://www.w3.org/TR/vocab-org/) (the `org:ChangeEvent` identity test) · [W3C PROV-O](https://www.w3.org/TR/prov-o/) (ruled out) · [schema.org/Organization](https://schema.org/Organization)

### Wikidata
- Properties: [P1365 replaces](https://www.wikidata.org/wiki/Property:P1365) · [P1366](https://www.wikidata.org/wiki/Property:P1366) · [P155 follows](https://www.wikidata.org/wiki/Property:P155) · [P156](https://www.wikidata.org/wiki/Property:P156) · [P127 owned by](https://www.wikidata.org/wiki/Property:P127) · [P1830 owner of](https://www.wikidata.org/wiki/Property:P1830) · [Property talk:P1365](https://www.wikidata.org/wiki/Property_talk:P1365) · [WikiProject Companies talk](https://www.wikidata.org/wiki/Wikidata_talk:WikiProject_Companies)
- Items: [**Q7193734 Romm**](https://www.wikidata.org/wiki/Q7193734) · [Q137999556 Devorah Romm](https://www.wikidata.org/wiki/Q137999556) · [Q2906565 Vilna Shas](https://www.wikidata.org/wiki/Q2906565) · [Q5254948 Plantin Press](https://www.wikidata.org/wiki/Q5254948) · [Q42168553 (its uncaught duplicate)](https://www.wikidata.org/wiki/Q42168553) · [Q602323 House of Elzevir](https://www.wikidata.org/wiki/Q602323) · [**Q278706 Hartleben's Verlag**](https://www.wikidata.org/wiki/Q278706) (the four-proprietorship exemplar) · [Q733801 Garnier Frères](https://www.wikidata.org/wiki/Q733801) · [Q1040912 Marquis Who's Who](https://www.wikidata.org/wiki/Q1040912) · [Q451462 Ullstein](https://www.wikidata.org/wiki/Q451462) · [Q634246 Breitkopf & Härtel](https://www.wikidata.org/wiki/Q634246) · [Q29756860 Charles Broster](https://www.wikidata.org/wiki/Q29756860)
- Queried via [query.wikidata.org](https://query.wikidata.org) (SPARQL) and the [MediaWiki API](https://www.wikidata.org/w/api.php). *Could not retrieve:* the original P1365/P1366 property proposal; `Help:Modeling/Organizations` does not exist.

### In-repo evidence for §5.7–5.9
- `core_db.tsv` (schema, db 24 / 495 / 953 / 147 / 2853 / 2861, `parent_db_id` rows)
- `organizations_clustered.tsv` (mention dates; ORG-C00071 / C00940 / C04980 Romm; the succession-vocabulary counts)
- `publisher_audit_decisions.tsv` (`P057`, `P058`, `P059`, `P148`, `P160`)
- `TROUPE_TYPOLOGY_PROPOSAL.md` §2 and open questions 3, 5

---

## 10. For the PI: should `core_db` carry time?

One question, serving this proposal and `TROUPE_TYPOLOGY_PROPOSAL.md` together.
Both are blocked on it and neither should answer it alone. Nothing here needs a
decision today; it is written so one *can* be taken.

### 10.1 What is actually blocked

- **Dynasties (§4).** Rules 1–4 are adopted and Romm is merged, but the
  succession itself — who held the press when — lives only as prose in
  `publisher_audit_decisions.tsv`. You cannot ask *who ran this press in 1880?*,
  and an edition's imprint cannot be attached to the proprietor who issued it,
  which `YiDraCor/data/editions.csv` will eventually want.
- **Troupe facets** (that doc's open question 5). A troupe goes amateur →
  professional, resident → itinerant. Facets as proposed record the
  *predominant* character, so a company that changed is recorded as whichever
  it was longest.
- Both reduce to the same sentence: **`core_db` models entities with no time
  dimension, while the mention layer has dates.**

### 10.2 What the data would support (measured 2026-10-25)

Of 16,454 org mentions, **7,323 (44%) carry at least one date** —
`relations.date_start` / `date_end`, `locations.datefrom` / `dateto`, or
`founding_date`.

| domain | mentions | dated |
|---|---|---|
| printer / publisher | 590 | 365 (61%) |
| theatre | 6,938 | 3,577 (51%) |
| troupe / travelling company | 3,358 | 1,437 (42%) |

Projected onto the **3,361 live `core_db` rows**:

| | rows |
|---|---|
| would gain ≥1 year from their mentions | **1,686 (50%)** |
| would gain ≥2 distinct years (a real range) | 725 |
| would gain ≥5 distinct years (a span, not a point) | 183 |

So a date dimension is **half-populated on arrival** — enough to be useful, not
enough to be trusted as complete. Any consumer must treat "no date" as *unknown*,
never as *not then*.

### 10.3 The three options

**A. Do nothing.** Keep dates in the mention layer; keep succession as sourced
prose in the decision ledgers.
*For:* no schema change, no migration, nothing to backfill, and the ledger
already holds more than any single column could — dated prose *with its source*.
*Against:* the two blocked questions stay blocked, and every future audit that
wants "when" re-derives it by hand.

**B. Two columns on `core_db`** — `date_from` / `date_to` on the entity.
*For:* cheapest thing that answers "when was this firm active?"; populates for
1,686 rows immediately; conventional (MARC `046 $q`/`$r`, Wikidata `P571`/`P576`,
both surveyed in §5).
*Against:* it answers the *entity's* span, **not** §4's actual question. One
range per row still cannot say who held the press in 1880, so Rule 1's loss is
unfixed. It also invites the error the survey warns about (§5.1): a per-record
date ends up dating *the name* rather than the business.

**C. A dated relation table** — one row per (entity, role-holder, from, to),
which is what every modern model does (§5.6): W3C ORG `org:Post` +
`org:memberDuring`, RiC-O `OwnershipRelation` + `relationHasDate`, Wikidata's
dated `P127`.
*For:* the only option that answers both blocked questions, and it generalises —
proprietors, troupe facets, and the city-branch case `parent_db_id` now carries.
*Against:* a new file and a new writer in a repo that already has
multi-producer hazards; needs a UI to be maintainable; and §5.8's warning stands —
the typed-succession vocabulary it would serve is about **six** printing mentions
today.

### 10.4 Recommendation

**B is the tempting answer and the wrong one.** It costs a migration and leaves
both motivating questions unanswered, while adding a column whose meaning
(entity span vs. name span vs. proprietorship) the survey shows is routinely
confused.

**Take A now, with C as the stated destination.** Concretely:

1. Keep succession and facet history in the decision ledgers, which already
   record them with sources.
2. Decide **C's shape** now even if it is not built — one dated-relation table,
   not per-domain columns — so the next audit that needs it extends one design
   instead of inventing a second.
3. Build C when a consumer actually demands it. The likeliest trigger is the
   Lateiner/Hurwitz edition work, which records publisher and printer per
   edition and will eventually want the proprietor, not just the house.
4. Do **not** extend `parent_db_id` to proprietors in the meantime (§4.3, item 3).
   Keep it meaning "simultaneous branch"; C subsumes it later.

RiC-O's property-chain design (§5.6) is the standing argument that this order
costs nothing: an undated assertion can be upgraded to a dated one later without
rework.

### 10.5 If the PI prefers B anyway

Then please also decide, because the survey says these get confused and the
answer changes what the columns mean:

1. Do `date_from`/`date_to` describe **the firm's existence**, or **the span the
   sources attest**? Half the rows have no date at all, so "unknown" must be
   representable and distinct from "not then".
2. What happens to the **dynasty** case — is a four-generation press one row with
   1818–1900, accepting that the proprietors remain unqueryable?
3. Who **writes** them? If the app does, add the merge-aware write path
   (`save_core_db` now merges rather than overwrites, `c8d84f006`); if a script
   does, it needs the multi-producer guard that `person_org_edges.tsv` just
   learned the hard way.