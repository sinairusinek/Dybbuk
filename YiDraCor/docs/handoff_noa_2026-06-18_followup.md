# YiDraCor — Two questions for Noa, 2026-06-18 (follow-up)

> **Update 2026-06-18 (afternoon, Sinai):** Section 1 (Yudale roleDesc) resolved. Section 2 (Ezra p.4 `mixed` vs `entrance`) reframed with TEI-spec evidence — see below; the substantive question for you is narrowed.

---

## 1. Yudale castList: name vs. roleDesc — RESOLVED

**Decision (Sinai 2026-06-18):** profession/relation modifiers ("his nephew", "moneylender", "eye doctor", "his wife") go in `roleDesc` always. Fused titles like *Professor* / *Don* / *King* / *Reb* that bind to the name stay inside `role`.

**Applied to Yudale page 4:**

| Line | Text | role | roleDesc |
|---|---|---|---|
| 65 | `יוּדאלע זיין נעפפע (בּלינד).` | `יוּדאלע` (length 7) | `זיין נעפפע (בּלינד)` |
| 107 | `איסר פּראָצענטניק.` | `איסר` (length 4) | `פּראָצענטניק` |
| 114 | `פּראפעסאר עדעלמאן (אויגען דאָקטער).` | `פּראפעסאר עדעלמאן` (length 17) | `(אויגען דאָקטער)` |

cast_dict regenerated; conventions doc updated (`docs/castlist_tagging_conventions_2026-06-18.md`, new convention F). Same pattern to be audited across other plays' castList pages.

---

## 2. Ezra p.4 stage-direction type — reframed with TEI-spec evidence

We searched the TEI spec and the question's actual answer is **better than either of the two options I gave you earlier**. Quick summary, then your call.

### What the TEI Guidelines say

The TEI P5 reference for `<stage>` defines nine suggested `@type` values: `setting`, `entrance`, `exit`, `business`, `novelistic`, `delivery`, `modifier`, `location`, `mixed`. And — verbatim, from the spec:

> *"If the value `mixed` is used, it must be the only value. Multiple values may however be supplied if a single stage direction performs multiple functions, for example is both an entrance and a modifier."*

Canonical example from the spec:

```xml
<stage type="entrance modifier">Enter Latrocinio disguised as an empiric</stage>
```

So the TEI-blessed way to encode a stage direction that combines an entrance with another action is **space-separated multi-token `type`** — e.g. `type="entrance business"`, `type="exit entrance"`, `type="delivery modifier"`. The literal value `mixed` is reserved as a *fallback escape hatch* for when the constituent functions can't be enumerated. It is not the spec-preferred form when we *can* enumerate.

DraCor (our downstream) inherits this — their ODD/schema does not restrict `@type` values; published DraCor corpora use single tokens almost exclusively, but multi-token values are schema-valid.

Sources:
- TEI `<stage>` reference: https://www.tei-c.org/release/doc/tei-p5-doc/en/html/ref-stage.html
- TEI Performance Texts chapter: https://www.tei-c.org/release/doc/tei-p5-doc/en/html/DR.html
- DraCor schema: https://github.com/dracor-org/dracor-schema

### What this means for the Ezra p.4 case

`(לעגט וועג דיא האַרפֿע— ערשיינט)` — "lays the harp aside — appears". Per the TEI spec, the principled type is **`type="entrance business"`**, not `mixed` (since we *can* enumerate: the line carries both an entrance and an incidental physical business).

This also handles the other 7 current `mixed` tags in the corpus cleanly:

| Page | Line | Current | TEI-principled |
|---|---|---|---|
| Ezra p.4 | `(לעגט וועג דיא האַרפֿע— ערשיינט)` | mixed | `entrance business` |
| DerMan p.11 | `(זיי נעהמען זיך אַרוּם, יאכֿטשע ערשיינט)` | mixed | `business entrance` |
| DerMan p.11 | `(ער בעהאַלט זיך. אויפֿטריט סאבעלע)` | mixed | `business entrance` |
| DerMan p.18 | 4× dance/song multi-line | mixed | `business delivery` (or just `business`) |
| DerMan p.18 | `פאָרהאַנג פאַלט.` | mixed | `setting` |

### Your decision

Three options now, in order of TEI-spec alignment:

- **(C) — TEI-principled — multi-token `@type`** (Recommended): adopt space-separated multi-token typing for compound directions. Ezra p.4 → `type="entrance business"`. All 7 corpus `mixed` tags get retyped to their actual function pair (table above). The literal `mixed` value drops out of our codebase. Cleanest, future-proof, matches DraCor.
- **(A) — Keep B2 as-is**: status quo. `mixed` continues to fire for entrance + any other action verb. Diverges from TEI's recommended form but is locally consistent.
- **(B) — Narrow `mixed` to entrance+exit only**: drop B2, keep `mixed` for the rare entrance/exit co-occurrence case. Other compound directions pick the dominant function. Doesn't take advantage of multi-token typing either.

Your B9 chat reply's intent (strict rule for entrance+exit) is closest to (B), but (C) supersedes the question entirely — it's what the TEI spec actually prescribes.

If you say **(C)**, we update `auto_resolve_flags` to emit multi-token `@type` values and run a one-time retype sweep on the 7 corpus pages. If **(A)** or **(B)**, we stay inside our current vocabulary.

---

*Background reminders: the 2026-06-14 castlist handoff had its DerMan↔Meshumed sections swapped (you spotted it, both files now have correction headers). Meshumed is parked for a future handwritten-plays track; we won't process it alongside the printed Lateiner corpus.*
