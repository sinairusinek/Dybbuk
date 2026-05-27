# Matching — Orgs service (Dybbuk / Zylbercweig)

Dybbuk is the **Organizations** matching service in the shared matching
architecture. The org aligners live in
[`Zylbercweig/organizations/`](Zylbercweig/organizations/).

## Reference & Explanation live upstream

Anything **true regardless of entity type** — normalization rules, the similarity
primitive, the cascade *shape*, identifier parsing, review-band routing — is
documented and (increasingly) implemented in the shared repos. Read those first;
do not re-derive or fork them here.

- **Reference / Explanation / ADRs / cross-vetting ledger:**
  [entity-matching-skill](https://github.com/sinairusinek/entity-matching-skill)
  — start with
  [`architecture/matching-core-and-services.md`](https://github.com/sinairusinek/entity-matching-skill/blob/main/architecture/matching-core-and-services.md).
- **Shared code (single source of truth):**
  [matching-core](https://github.com/sinairusinek/matching-core).
- **Core/domain boundary (binding):**
  [ADR-0002](https://github.com/sinairusinek/entity-matching-skill/blob/main/decisions/0002-shared-core-extraction.md).

## What stays here (Orgs domain layer)

Domain logic specific to organizations — **it does not belong in core**:

- **Org-token stripping & troupe typology** — [`org_normalize.py`](Zylbercweig/organizations/org_normalize.py)
  (double-vov collapse, final forms, head-nouns טרופּע/טעאַטער/קאָמפּאַניע, possessives, articles).
- **Latin→Yiddish transliteration for blocking** — [`translit_latin_to_yiddish.py`](Zylbercweig/organizations/translit_latin_to_yiddish.py)
  (blocking-only; never canonical). *Note: the Polish romanization digraphs added here
  (`sz/szcz/cz/rz`) are entity-agnostic and are logged for core in the cross-vetting ledger.*
- **Location-aware clustering** — [`cluster_orgs.py`](Zylbercweig/organizations/cluster_orgs.py)
  (0.92 auto-merge / 0.70 review band).
- **Candidate generation / alignment** — [`prepare_alignment.py`](Zylbercweig/organizations/prepare_alignment.py)
  (blocking by org-type, exact/fuzzy/IPA-phonetic passes, head-noun-stripped alias for
  person-named troupes). *The surname-stripping technique is logged for core; the org
  tail-word list stays domain.*

## How-to / Tutorial (Orgs-specific)

These workflows differ per entity type and live with the service:

- Running the org alignment pipeline: `prepare_alignment.py` reads
  `organizations_clustered.tsv` + `core_db.tsv`, writes `org_alignment_review.tsv`.
- Re-running translit blocking: `translit_latin_to_yiddish.py --overwrite` after new
  DB rows are added.
- Review decisions are surfaced/saved through the Zalmen app
  ([`Zylbercweig/zalmen/`](Zylbercweig/zalmen/)).

## Decision-preservation invariant

Regenerating `org_alignment_review.tsv` must carry human decisions forward by the
stable `cluster_id`, **not** by a content-derived semantic key — see
[ADR-0003](https://github.com/sinairusinek/entity-matching-skill/blob/main/decisions/0003-stable-id-decision-preservation.md).
Guard: post-regen decision count must never drop below the pre-regen count.

## Cross-vetting

When you change behaviour that other services could want (normalization,
similarity, cascade, thresholds, identifier parsing, a new general technique),
follow [the cross-vetting process](https://github.com/sinairusinek/entity-matching-skill/blob/main/process/cross-vetting.md):
add a row to [`matching-core/LEDGER.md`](https://github.com/sinairusinek/matching-core/blob/main/LEDGER.md)
with the Places/People/Orgs disposition. Purely Orgs-domain changes (a new
org-token, a troupe-type rule) stay in this service's history only.
