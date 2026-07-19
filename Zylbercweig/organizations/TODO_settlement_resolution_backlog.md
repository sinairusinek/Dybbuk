# TODO — settlement resolution backlog

Opened 2026-07-19, from the audit that produced commit `fd9c5fa1` (exonym
aliases + borough containment + 3 Brooklyn reassignments).

**Context.** Sinai noticed Brooklyn orgs appearing under New York in the
settlement view. The mis-filing was real but small (3 rows). The audit behind
it turned up a much larger problem: **~42% of settlement values were not
resolving at all**, because `SettlementResolver` keys on Wikidata's *current
official* label while the review data uses historical/colloquial forms.
`fd9c5fa1` took `org_addresses_review.tsv` from 58% → 85.4% of occurrences
resolved. Items below are what that audit surfaced and did **not** fix.

Numbers are as of 2026-07-19 and will drift — re-measure before acting.

---

## Data defects

### 1. db_id 332 — coordinates land in Suffolk County
`Brownsville Metropolitan Singer Hall` has `lat/lon = 40.921376, -72.6633`.
Pitkin Avenue, Brownsville is ~`40.665, -73.91`. Almost certainly a geocoder
hit on a different "Pitkin".

This is the exact failure mode `TODO_settlement_vs_address_geocoding.md`
warns about — address-level lat/lon is a **manual, reviewer-confirmed** track
and a Nominatim hit is a starting point only. Worth sweeping all
`org_addresses_review.tsv` pins for distance-from-settlement-centroid outliers
rather than fixing this one row; a >50 km gap between a row's confirmed
address pin and its settlement QID centroid is a strong defect signal.

### 2. kimatch tags five cities as `neighborhood`
`resolved_category: neighborhood` on Vienna Q1741, Leipzig Q2079, Prague
Q1085, Bremen Q24879, Mainz Q1720.

Harmless **today** only because `settlement_resolver._SETTLEMENT_CATEGORIES`
accepts `{"settlement", "neighborhood"}` alike. The trap: that set is also the
root cause of boroughs arriving as top-level peers, so the obvious "fix" —
narrowing it to `{"settlement"}` — would silently drop Vienna's 16 rows out of
the lens. **Fix the categorisation upstream in kimatch before touching that
set.** kimatch has only 11 `neighborhood` entries total, so this is a small
manual correction.

### 3. Bronx and Harlem resolve to `None`
Q18426 (Bronx) and Q189074 (Harlem) are in
`settlement_variant_collapse_audit_2026-05-20.tsv` with `source: punchlist`,
but that file is **not** one of the resolver's two sources
(`places_unified_corrected.csv`, `kimatch_matched_full.tsv`). So `בראַנקס`,
`בראָנקס`, `בראַנקסער`, `האַרלעם`, `האָרלעם` all fail.

Their parent rows are already in `settlement_parents.tsv` and sit inert until
this is fixed. Either fold punchlist variants into a resolver source, or add
the punchlist as a third source.

### 4. Places absent from the gazetteer entirely
`Danzig`, `Baku`, `Birobidzhan`, `Mykolaiv`, `Lwów (Lemberg)` — ~16 rows.

Deliberately **not** patched with hardcoded QIDs in `settlement_resolver.py`:
that file's alias table maps to existing gazetteer keys, so a gazetteer change
degrades to "unresolved" and never to a *wrong* QID. Hardcoding QIDs would
break that property. Fix via a kimatch pass instead.

---

## TSV hygiene

### 5. Street addresses leaked into settlement fields
~40 distinct Hebrew values in `confirmed_settlement_yiddish` are full
addresses, e.g. `819 אַרטש סטריט, פֿילאַדעלפֿיע`,
`נעווסקי פּראָספּעקט 56, לענינגראַד / סאַנקט פּעטערבורג`.

This is why the **Hebrew** failure rate (52% of distinct values) is nearly
double the Latin one (29%) — it is a data-entry problem, not a resolver
problem. Two options: normalise at write time to the trailing city token, or
give `resolve()` an address-parsing tier that retries on the last
comma-separated segment. Prefer the former; the resolver should stay a lookup.

### 6. Sentinel and venue names in settlement fields
`(unknown)` (4 rows), `Grand Opera House`, `Gradina Lieblich Jigniza`. Venue
names belong in `extracted_venues`.

### 7. Codify the CRLF invariant
`org_addresses_review.tsv` is **CRLF**. Writing it with Python's default
`lineterminator='\n'` rewrites all 1290 lines, which guarantees a merge
conflict against Zalmen auto-saves. Correct form:

```python
with open(P, newline='') as f:          # read
    rd = csv.DictReader(f, delimiter='\t')
with open(P, 'w', newline='') as f:     # write
    w = csv.DictWriter(f, fieldnames=cols, delimiter='\t', lineterminator='\r\n')
```

This bit during `fd9c5fa1` and was caught only by checking `git diff --stat`
(1290/1290 instead of 3/3). Worth a shared writer helper so it stops depending
on whoever is at the keyboard remembering. See
`feedback_zalmen_tsv_conflict_patterns`.

---

## Cluster side

### 8. 715 of 1037 cluster settlement values unresolved (69%)
Far worse than the DB side. Dominated by the pipe-joined multi-variant format
in `org_alignment_review.tsv`: `ניו יאָרק | ניו-יאָרק` (46 rows),
`וואַרשע | וואַרשעווער` (12), `לאָדזש | לאָדזש` (10).

That last one is **the same string twice** and still fails — a pure format bug.
`_resolve_cluster_settlements` does split on `|` and `;`, so the fault is
upstream in `_cluster_settlement_strings`, which returns the pre-joined string
when JSON parsing fails. Probably the single largest remaining resolution win.

### 9. Adjectival `-er`, Latin side
`resolve()` has a Hebrew adjectival-suffix fallback (`ישער`/`ישע`/`ער` plus
vowel tails) with **no Latin counterpart**. This is Gap 1 from the older
settlement-collapse memo and is still open.

---

## Judgment calls — need PI/Sinai, not a mechanical fix

### 10. db_id 144 Kessler's Lyric Theatre — one venue or two?
Left at `New York` in `fd9c5fa1` on purpose. `reviewer_notes` (Maaty) read:
*"Zylbercweig lexicon: Kessler's Lyric Theatre (David Kessler), New York.
Address 35-37 Second Avenue, Manhattan. Likely SAME venue as Second Avenue
Theatre."* Coords `40.7252, -73.9893` confirm East Village. But
`extracted_addresses` (unreviewed machine output) says `Siegel Street and
Broadway, Brooklyn, NY`, and a Brooklyn venue of that name did exist.

So this is plausibly **two venues conflated into one row**, not a mis-filed
settlement. Reassigning it to Brooklyn would override a considered human call
on weaker evidence. Candidate for the `cluster-research` skill.

### 11. Are ghettos settlements?
`וואַרשעווער געטאָ` (20 rows) and `לאָדזשער געטאָ` (4) have no QID and do not
resolve. Modelling question: a ghetto is not a settlement, but it is not an
ordinary neighborhood either — it is time-bounded and its extent changed.

Decide before adding QIDs, because it determines whether they roll up into
Warsaw/Łódź via `settlement_parents.tsv` or need a separate mechanism with
date scoping. Related: `parent_db_id` already exists on `core_db.tsv` for
org-parentage and is **not** the right vehicle for geography.

---

## Notes on what was already decided

- **Storage stays flat; containment applies at query time.** A Brownsville org
  keeps its Brownsville QID and its own bucket. See `settlement_parents.tsv`
  and `SettlementIndex.buckets_in_city(include_children=True)`. This is what
  makes it safe to reassign an org to a borough — it still counts under NYC.
- **The parent table is hand-curated on purpose.** Observed scope is ~66 rows /
  6 QIDs, 95% NYC. Do not over-build a hierarchy. To generalise later, harvest
  P131 — `zibn-shtern/src/zibn_shtern/wikidata_client.py:103` already extracts
  it and currently discards it.
- **Latin values mostly DO resolve.** Warsaw, London, Vienna, Vilnius,
  Bucharest, Philadelphia, Kraków, Chicago, Paris, Berlin, Buenos Aires,
  Moscow, Riga, Minsk, Boston, Tel Aviv all work, case-insensitively. Do not
  assume "Latin is broken" — check the specific string against the gazetteer
  key first.
