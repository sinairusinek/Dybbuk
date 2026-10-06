# Next session: link the eponym of an org to that org

Paste the **Prompt** at the bottom into a fresh session in
`/Users/sinairusinek/Documents/GitHub/Dybbuk`.

**Wait for dybbuk-27's reconciliation PR to land first** — see "Blocked on" below.

---

## Sinai's ask (2026-10-05, verbatim)

> "I would like to make sure that ALL such organizations in the zylbercweig data
> which are made of names, one or more, also imply automatically that the person
> is linked to the organization."

So: `לערמאַנס טרופּע` should link **Lerman the person**; the trio
`ראָמעס, שוואַרץ און שאָר` should link **three** people.

## The gap — verified, not assumed

`organizations/graph/person_org_edges.tsv` holds **16,881 edges of exactly one
kind**: `host_heading`, the biography each org mention was *extracted from*.
That encodes "this person's entry mentions this org", which is a **different
relation** from "this org is named after this person". Two proofs:

- **db1062 `ראָמעס, שוואַרץ און שאָר`** (a trio) → 1 edge, and it is the trio's
  own entry. Romes, Shvarts and Shor as people: not linked.
- **db641 `לערמאַנס טרופּע`** → 14 performer edges, but **not Lerman**, the man
  it is named for.

`people/people_db.tsv` has a `professional_role_org` column that is **empty for
all 3,482 rows** — possibly the intended home for this, worth asking Sinai
before choosing where edges live.

## Scale (measured on live, non-deprecated, non-merged rows)

**~377 of 2,127 live orgs (~18%)** carry person name(s) in the name:

| pattern | count | example |
|---|---|---|
| `טרופּע פֿון X` | 137 | `טרופּע פֿון הורוויץ` |
| possessive `Xס טרופּע` | 131 | `גראָדנערס טרופּע` |
| comma / `און` name list | 109 | `Finkel, Feinman and Mogulesko's Troupe` |

**The third bucket is NOT clean.** The same comma sweep also catches
`Olimpic Theatre (St. Louis, MO)` and `Ḳ. Leṿenshṭeyn , ק. לעווענשטיין`. A
blanket rule over it would be wrong.

## Feasibility test already run

`people/people_db.tsv`: **3,482 people**, own `db_id` space, 3,260 distinct
surnames. Matching `Xס טרופּע` against it:

- **14 unique matches, 0 ambiguous** where the org name carries **given name +
  surname** — `סעם אַדלערס טרופּע` → `סעם אַדלער`,
  `יאָנאַס טורקאָווס טרופּע` → `יאָנאַס טוּרקאָוו`,
  `משה סקולניקס טרופּע` → `משה ליפּמאַן`-style clean hits.
- **99 "no match"** — but that is an artifact of my probe keying on the whole
  `hebname` ("surname, given") instead of the surname alone. Most are bare
  surnames like `גראָדנערס`. **Recall is understated; a surname index should do
  much better.** Re-measure before designing.

## Design notes

- **Three confidence tiers, not one rule.** Given+surname possessive → high.
  `טרופּע פֿון X` → high. Bare surname and multi-name lists → review queue: a
  surname alone is ambiguous across 3,260 surnames.
- **A multi-name org yields SEVERAL eponym edges** (the trio = 3 people). It is
  never a reason to split the org — the Joint names round (2026-10-05) settled
  all 16 such rows as ONE company each, deliberately.
- **`קאַליך רומשינסקי` is a management partnership, not one person** — see the
  `feedback_duo_surnames_not_variants` memory. Two surnames adjacent ≠ one
  compound person, and ≠ a name variant.
- **The edge needs its own type.** Don't overload `host_heading`; use something
  like `edge_source=eponym` in `person_org_edges.tsv`, or a new file. The two
  relations must stay distinguishable.
- **Person ids and org ids are separate id spaces.** Never mix them.
- Being named after someone does not mean they founded or ran it — the edge
  should claim eponymy, not a role, unless a mention supplies one.

## Yiddish matching traps (each cost real time in earlier sessions)

- **NFKD before stripping points.** The corpus stores the precomposed ligature
  `U+FB4E (פֿ)` where a typed string has `פ + rafe`. Stripping points alone
  matches nothing and *looks like a clean absence*.
- **Strip points from the PATTERN too**, not just the text — dagesh sits inside
  `טרופּעס`. Stripping one side matched 0 of 14 rows and read as a true negative.
- **A substring search on one spelling is not an existence check.** A previous
  session reported "no Ziegler org exists"; it was there as `זייגלער` (double
  yod) while only `זיגלער` was probed. Probe variants: yod doubling, ג/נ, ז/ס,
  final forms.
- **Possessive after a final letter form**: `גאָלדפאַדען + ס` →
  `גאָלדפאַדענס`, not `גאָלדפאַדעןס` (ן→נ ם→מ ך→כ ף→פ ץ→צ).

## Blocked on

**dybbuk-27 is reconciling `core_db.tsv` right now** (local main 44 ahead / 423
behind origin/main) and is **renumbering org ids 2240–2259 → 2315–2334**. Eponym
edges key on org `db_id`, so building them before that PR lands means building
on ids that are about to move. **Confirm the PR is merged and pull before
starting.** Also still true: `build_core_db.py` is non-idempotent — never
"just regenerate".

## Read first

Memories: `project_eponym_person_org_links` (this work),
`project_person_org_provenance_graph` (the existing layer to extend),
`project_alignment_sources` ("where actual alignments live" — read before any
people-matcher work), `project_people_matcher_rethink`,
`feedback_duo_surnames_not_variants`, `feedback_core_db_link_authority`.

Also `Zylbercweig/people/people_common.py` — `mentions_all.tsv` must be read
only via `load_mentions_with_host()`.

---

## Prompt

> New piece of work in the Zylbercweig data: organizations named after people
> should link to those people, and currently they don't.
>
> Sinai's ask: "ALL such organizations in the zylbercweig data which are made of
> names, one or more, also imply automatically that the person is linked to the
> organization." So `לערמאַנס טרופּע` should link Lerman; the trio
> `ראָמעס, שוואַרץ און שאָר` should link all three people.
>
> Read `Zylbercweig/organizations/NEXT_SESSION_eponym_person_links.md` first for
> the scoping and the Yiddish-matching traps — but **verify its numbers yourself;
> do not trust the write-up.**
>
> What's already established: `graph/person_org_edges.tsv` has 16,881 edges but
> all of one kind (the biography a mention was extracted from), which is a
> different relation from eponymy. Roughly 377 of 2,127 live orgs have person
> names in the name. `people/people_db.tsv` has 3,482 people to link to, and a
> `professional_role_org` column that is empty for every row.
>
> First check whether dybbuk-27's core_db reconciliation PR has landed and pull
> if so — it renumbers org ids 2240–2259, which eponym edges would key on.
>
> Then: measure the real patterns, propose where the edges should live (new edge
> type in `person_org_edges.tsv`, a new file, or that empty `professional_role_org`
> column — ask Sinai, don't assume), and tell me the plan with its confidence
> tiers before building anything.
>
> Two things not to do: don't apply one blanket rule across all 377 — the
> comma-list bucket catches things like `Olimpic Theatre (St. Louis, MO)` that
> contain no person; and don't treat a multi-name org as a reason to split it,
> because those 16 rows were all deliberately decided as ONE company each on
> 2026-10-05.
>
> Dry-run before writing, verify with a fresh read rather than the pattern that
> made the edit, and commit with explicit paths.
