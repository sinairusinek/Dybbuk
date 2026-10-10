# Publishing dynasties — how to model successive owners

**Status:** PROPOSAL. Nothing in `core_db.tsv` has been restructured for it.
The publisher audit of 2026-10-10 minted and aligned under the *current*
conventions and left every dynasty question open.

**For:** PI + Sinai.
**Companion to:** `CLASSIFICATION_POLICY.md` (the canonical typology) and
`TROUPE_TYPOLOGY_PROPOSAL.md` (the same genre of question, for troupes).
**Raised by:** Sinai, 2026-10-10, on the Lebenzohn finding below.

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
10 rows that use it are places: Forverts New York / Chicago / Philadelphia /
Detroit under db 249; Hashomer Hatzair Vilnius / Toronto / Chełm under db 602.
None expresses a succession in time.

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
Two rows: db 50 and the newly minted db 2669.

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
Vilna — one row (db 2661), seat in `address`, earlier seat in the notes. Had
they run shops in both cities *at once*, that would be the Forverts shape and
`parent_db_id` would apply.

---

## 4. The open question

Rule 1 flattens the dynasty into `name_variants`. That is cheap and needs no
schema change, but it is **lossy**: you cannot then ask *who ran this press in
1880?*, or attach a particular edition's imprint to the proprietor who actually
issued it — which the Lateiner/Hurwitz edition work (see
`YiDraCor/data/editions.csv`, which records publisher and printer per edition)
will eventually want.

The alternative is a **row per proprietor**, each pointing at the house with
`parent_db_id`, with dates on each. That is queryable and honest to the
history, but:

- it extends `parent_db_id` from "branch in another city" to "proprietor in
  another decade" — a genuinely new semantics for the column;
- it needs date fields the schema does not have (`date_from` / `date_to` are
  not in `core_db`);
- it multiplies rows a matcher must choose between, which is the problem Rule 1
  was avoiding.

**Recommendation:** adopt Rules 1–4 now, since they only formalise what the DB
and the PI already do, and treat the per-proprietor question as a separate
decision — ideally taken together with whether `core_db` should carry activity
dates at all, which several other audits have wanted.

---

## 5. Before deciding: survey outside practice

This is a solved problem in bibliography and in authority control, and the
project should not invent a scheme without looking. **Whoever picks this up
should research the following and report back into this document**, rather than
reasoning from first principles:

1. **MARC/RDA and name authorities.** How do national libraries model a firm
   that changes proprietor? Look at the `110`/`410` corporate-name practice and
   the "earlier/later name" linking entries (`510` with `$w a`/`b`), which
   exist precisely for successive corporate bodies. Key question: does cataloguing
   practice treat "Lebenzohn press under Yosef" as a *later name* of the same
   body, or a *successor* body?
2. **VIAF and ISNI** for printer/publisher firms — whether they hold one
   cluster per firm across proprietors, and how they handle the Warsaw presses
   specifically.
3. **CERL Thesaurus** (`data.cerl.org`) — the standard authority file for the
   European book trade before 1830, built around exactly this problem:
   imprint names, firm succession, and place changes. The closest existing
   model to what we need, and it has an API.
4. **Book-trade indexes with explicit succession modelling** — the British Book
   Trade Index (BBTI), the Scottish Book Trade Index, and
   Bibliopolis/Netherlands. These record a person's trade, dates, address *and*
   successor, which is the shape Rule 1 flattens.
5. **CIDOC-CRM / records-in-contexts (RiC-O)**, for how archival description
   models an organisation's continuity across name and governance change
   (`rico:Agent`, with `rico:isOrWasSuccessorOf`). Relevant if the project ever
   exports to a graph.
6. **Wikidata's own practice**: `P1365 replaces` / `P1366 replaced by`,
   `P127 owned by` with qualifiers, and whether publisher items there are split
   per proprietor. Directly checkable and the easiest comparison, since several
   of these Warsaw firms may already have items.

Report for each: whether it keeps one entity or several, what it does with
dates, and what it loses. Then come back to §4 with a recommendation grounded
in practice rather than taste.

---

## 6. What the audit did in the meantime

Nothing irreversible. Under current conventions:

- **Lebenzohn** (db 24) stays as it is, flagged `P058`, with the Friedberg
  succession in its note. **No cluster was linked to it** — `דפוס של ר" יוסף
  לעבענזאָהן` is deliberately left unaligned, because aligning it would decide
  this question by accident.
- **Baumritter** (db 157) took the partnership cluster under Rule 3 (`P057`).
- **Goldfarb** was split under Rule 2 (`P059`), minting db 2669 for M.
- **Bletnitski** was minted as one Odessa row under Rule 4 (db 2661, `P148`).

Reversing any of these is a cluster-link edit, not a data loss.
