# Romm — every attestation in our data, for a merge/split decision

**For:** Sinai. **Prepared:** 2026-10-11. **Decision needed:** does the Romm
press become one row (db 495) or stay two (db 495 + db 953)?
**Companion to:** `PUBLISHING_DYNASTY_MODEL_PROPOSAL.md` §7 (`P165`).
**Nothing has been changed.**

---

## 1. There are exactly THREE attestations

A search for the Romm name across every name-bearing column of
`organizations_clustered.tsv` (16,454 mentions) returns **3 genuine mentions**.
Worth stating because a naive search returns 235: the Yiddish string for
"dramatic" (`דראָמאַטיש`) and `רעפאָרם` / `פאָרם` contain the same letters, and
all 232 others are that noise (`היפּאָדראָם`, `מאַסאַדראם`, `ראָמבערג`,
`ראָמאַן-צייטונג`, `פֿראָממער`…). The three real ones:

| # | name as attested | cluster | → row | settlement | dates on the mention |
|---|---|---|---|---|---|
| **1** | `דרוקעריי פון ראָם` | ORG-C00071 | **db 495** | ווילנע | *none* |
| **2** | `ראָם-פֿאַרלאַג` | ORG-C00940 | **db 953** | ווילנע | start **1887** |
| **3** | `דרוקעריי פֿון די ברידער ראָם` | ORG-C04980 | **db 953** | ווילנער | **1839–1862** |

All three are `cluster_size = 1` singletons, `confidence = medium`,
`name_type = proper_name`, and all three are **Vilna**.

---

## 2. Each attestation in full

### #1 — `דרוקעריי פון ראָם` → db 495

- **Source:** vol. 7, `facs_21_tr_1756831761`, entry **כץ, חבל** (Khevl Kats,
  b. 10 May 1902 Vilna – d. 8 March 1940)
- **Sentence:** `געוואָרן אַ מאַטריצירער אין דער דרוקעריי פֿון ראָם`
  — "became a **matrix-maker** in the Romm print shop"
- **Relation:** `Employment_Performance` / `employee`; `role_title` = `מאַטריצירער`
- **Venue:** `דרוקעריי` (print shop). **Province:** פּויליש-ליטע
- **Date:** none. But the biography dates it implicitly — Kats was born 1902,
  started work "very young", and emigrated to Argentina in **May 1930**. So this
  is a **c. 1915–1930** observation.
- **Alignment:** `decision=ALIGN`, `aligned_db_id=495`, **reviewer = Bella,
  2026-04-13T09:55:12Z**. Top candidate scored **1.000 (exact)**.

**This is the only one of the three that is a human-reviewed alignment,** and the
only one describing the press as a *workplace* rather than as an imprint.

### #2 — `ראָם-פֿאַרלאַג` → db 953

- **Source:** vol. 3, `facs_250_r`, entry **שמ"ר [שייקעוויטש, נחום מאיר]**
  (Shomer, the famous Yiddish novelist)
- **Sentence:** `באַזעצט זיך ש. אין ווילנע, וואו ער שרייבט ראָמאַנען אויסשליסלעך
  פֿאַרן ראָם-פֿאַרלאַג` — "Sh. settles in Vilna, where he writes novels
  **exclusively for the Romm farlag**"
- **Relation:** `Production_Distribution` / `published_by`; `role_title` = `מחבר` (author)
- **Date:** `date_start = 1887`
- **Alignment:** `decision=NEW` → db 953. **No reviewer. No timestamp.**
  Top candidates were db 185 (Am Olam, 0.500) and db 270 (HaTsfira, 0.400) —
  both wrong, so "NEW" was reasonable *given those candidates*. **db 495 was not
  offered as a candidate at all.**

Note this is the one attestation that genuinely uses *farlag* (publishing house)
rather than *drukeray* (print shop) — and it is about **commissioning an author**,
which is a publisher's function, not a printer's.

### #3 — `דרוקעריי פֿון די ברידער ראָם` → db 953

- **Source:** vol. 2, `facs_314_tr_1744365768`, entry **נאָרפלוס, יוסף**
  (b. 1894 Warsaw, actor; went to Argentina 1920)
- **Sentence:** `איבערגעדרוקט געוואָרן... אין דער ווילנער דרוקעריי פֿון די ברידער
  ראָם` — "**reprinted** … at the Vilna print shop **of the Romm brothers**"
- **Relation:** `Production_Distribution` / `published_by`
- **Dates:** `1839–1862`
- **Alignment:** `decision=NEW` → db 953. **No reviewer. No timestamp.**
  Top candidate was **db 495, scoring 0.615** — i.e. **the matcher proposed the
  right row and the decision overrode it.**

#### ⚠ The 1839–1862 dates do NOT describe the press

This matters for the decision, so it is worth being exact. The Norfluss entry
drifts, mid-entry, away from his biography into a **philological discussion of a
Yiddish adaptation of the Hebrew play *Yiftah* (Jephthah)** — comparing character
names in the Hebrew original against the Yiddish version (יפתח, חגלה, חומץ,
פּלטיה, זקן ושרי גלעד …). The printer mentions belong to **editions of that play
text**, not to Norfluss and not to the Romm firm's lifespan.

The proof is the sibling row from the *same* `xml:id`:

| org | date | sentence |
|---|---|---|
| `דרוק מאָנעם און זימעל` | **1831** | `ווילנע, דרוק מאָנעם און זימעל` |
| `דרוקעריי פֿון די ברידער ראָם` | **1839–1862** | `איבערגעדרוקט געוואָרן...` |

Two Vilna printers, two imprint dates, one bibliographic passage. These are
**editions of a playtext**, i.e. `1839` and `1862` are two printings — which is
exactly what `איבערגעדרוקט` ("reprinted") says.

**So the "1839–1862 vs 1887" date gap that looked like evidence of two different
firms is not evidence of anything about the firms.** It is one playtext's print
history against one novelist's contract date.

(Also telling: `דרוק מאָנעם און זימעל` — the immediate sibling imprint — was
left **unaligned with no core_db row at all**, while the Romm imprint from the
same passage got one. The inconsistency is in the handling, not the history.)

---

## 3. How the split actually happened

Not by a judgment that these were two firms. By three unrelated passes:

1. **Bella** reviewed ORG-C00071 and aligned it to the pre-existing **db 495** ✔
2. ORG-C00940 (`farlag`, 1887) was auto-marked `NEW`; db 495 **was never a
   candidate**, so **db 953 was minted**.
3. ORG-C04980 (`brothers`, reprint dates) was auto-marked `NEW` **despite db 495
   scoring 0.615 as top candidate**, and was attached to the just-minted db 953.

Neither step 2 nor step 3 carries a reviewer or a timestamp — both are machine
decisions that were never reviewed. **db 953 exists only because of them**; it
has no name in `name`, only `name_yiddish`.

This is a known defect class, not a Romm-specific slip: across
`org_alignment_review.tsv` (8,114 rows) there are **2,520** `NEW` decisions, of
which **174 have no reviewer**, and **668** `NEW` rows had a top candidate
scoring ≥0.60 — i.e. the matcher proposed an existing row and was overridden.

---

## 4. What the two rows are now

| | db 495 | db 953 |
|---|---|---|
| `name` | `דרוקעריי פון ראָם` | *(empty)* |
| `name_yiddish` | `דרוקעריי פון ראָם` | `ראָם-פֿאַרלאַג` |
| `org_type` | `Printer` *(set 2026-10-11, `P163`; was empty)* | `Publisher` |
| `address` | — | — |
| clusters | ORG-C00071 | ORG-C00940, ORG-C04980 |
| provenance | human (Bella) | machine, unreviewed |

No other core_db row is a candidate: the only other Vilna book-trade rows are
db 508 `ווילנער דרוקער-פאַריין` (the printers' **union**) and db 952
`ווילנער פֿאַרלאָג פֿון ב. קלעצקין` (Kletskin — a different firm).

---

## 5. External evidence (from the §5 survey)

The Romm press is the canonical Hebrew printing dynasty: Grodno 1788 → Vilna
1799, to 1940. Succession is written into the imprint — `דפוס ראם` became
**`דפוס האלמנה והאחים ראם`** ("press of the **widow and brothers** Romm") after
David Romm's death in 1862, appearing on title pages from 1871; later a
joint-stock company. It printed the Vilna Shas (1880–86).

- **Wikidata Q7193734** — **ONE** item, `דפוס ראם`, "publisher of Jewish
  religious literature (**1788–1941**)", using dated location qualifiers for the
  Grodno→Vilna move. No separate item per proprietor.
- **NLI** holds an authority record *Defus ha-almanah ṿeha-aḥim Rom (Vilnius)* —
  J9U `987007477320405171`, GND `1131194608`.
- **ISNI / VIAF** — no record of the firm at all (Romm returns one organisation:
  a Québec marine-mammal network).

Note what the external record says about our question: the house was **both** a
*drukeray* and a *farlag*, and `דרוקעריי פֿון די ברידער ראָם` is a **real
historical name of it** — the brothers' phase — not a different business.

---

## 6. The decision

### Option A — MERGE into db 495 *(recommended)*

Survivor **db 495** (earliest id, per Rule 1); db 953 `deprecated=true`,
`merged_into=495`; its two clusters folded into db 495's
`linked_cluster_ids`; `ראָם-פֿאַרלאַג` and `דרוקעריי פֿון די ברידער ראָם` into
`name_variants`.

**For:**
- All three attestations are **Vilna**, and all three name the **same house** —
  confirmed by the external record, where "brothers Romm" is a documented phase
  of the one firm.
- The date gap that appeared to separate them **isn't about the firms** (§2 #3).
- The split was never a decision — two machine rows with no reviewer, one
  overriding its own matcher's correct suggestion (§3).
- Matches the adopted model (one row per house) and Wikidata's single item.
- `org_type` on db 495 is now `Printer` and on db 953 `Publisher`; the firm was
  genuinely both, so a single row typed **`Printer/Publisher`** (a canonical type)
  describes it better than either row alone.

**Against:** loses the ability to say which attestation was the *farlag* function
vs the *drukeray* function — though under the adopted model that distinction
belongs in dated relations we are not yet building, and the mention layer retains
it regardless.

### Option B — keep two rows

**For:** a *farlag* and a *drukeray* are, in principle, distinct concerns, and
Rule 2 splits businesses that overlap in time.

**Against:** Rule 2's test is **chronological overlap of two businesses**, and we
have no evidence of two businesses — only one house performing two functions. The
1887 *farlag* and the 1839–1862 reprint dates do not overlap and do not refer to
competing firms. Keeping the split also preserves db 953, a row created by an
unreviewed machine decision against its own matcher's advice.

### If Option A, also worth doing
- Set `org_type` = **`Printer/Publisher`** on db 495.
- Add external ids if the schema gains a column for them: Wikidata **Q7193734**,
  NLI J9U **987007477320405171**, GND **1131194608**.
- Consider whether `דרוק מאָנעם און זימעל` (ORG-C04979, Vilna 1831, currently
  unaligned, no row) should be minted — it is the same *kind* of imprint mention
  and was handled the opposite way.

---

## 7. One caveat on all of it

Every one of the three attestations is a **singleton cluster at `medium`
confidence**, and two of the three are *incidental* references — a man's job, a
novelist's publisher, a playtext's imprint. None is an entry *about* the Romm
firm. The corpus is Zylbercweig's theatre lexicon, so the press appears only
where it touches a theatre person's life. Whatever is decided rests on three
passing mentions plus the external record, not on rich in-corpus evidence.
