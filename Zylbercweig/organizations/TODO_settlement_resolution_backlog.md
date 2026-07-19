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

### 3. Bronx and Harlem resolve to `None` — ✅ DONE 2026-07-19
Fixed by adding `settlement_variant_collapse_audit_2026-05-20.tsv` as a third,
**last-loaded** resolver source (so it can only ever fill gaps, never override
the gazetteers). All four boroughs are now live in the lens: Brooklyn 232,
Bronx 168, Brownsville 41, Harlem 45 mentions. NYC rollup went 3076 → 3293.

Original text kept below for context.


Q18426 (Bronx) and Q189074 (Harlem) are in
`settlement_variant_collapse_audit_2026-05-20.tsv` with `source: punchlist`,
but that file is **not** one of the resolver's two sources
(`places_unified_corrected.csv`, `kimatch_matched_full.tsv`). So `בראַנקס`,
`בראָנקס`, `בראַנקסער`, `האַרלעם`, `האָרלעם` all fail.

Their parent rows are already in `settlement_parents.tsv` and sit inert until
this is fixed. Either fold punchlist variants into a resolver source, or add
the punchlist as a third source.

### 3b. kimatch false positive: נאָוואָ-מינסק → a French hamlet
`נאָוואָ-מינסק` / `נאָוואָמינסק` (Novo-Minsk, i.e. **Mińsk Mazowiecki**,
Poland) matched to **Q198480 Gugney-aux-Aulx**, a French commune of ~50
people. Reached the resolver through *both* kimatch and the collapse map.

Suppressed via `settlement_resolver._EXCLUDE_QIDS`, which is enforced in
`_add()` across all sources — guarding only the new fallback layer left the
bad match live through kimatch. **That is a band-aid**: fix the match upstream
in kimatch and give Novo-Minsk its correct QID, then delete the entry.

Worth a sweep for siblings: a Yiddish name matching a tiny Western-European
commune is a recognisable signature of this failure mode, and the kimatch
geo-implausibility guard should have caught it.

### 4. Places absent from the gazetteer entirely
Partly resolved 2026-07-19 — the collapse-map source picked up **Baku**,
Poltava, Milwaukee, Homel and several Warsaw/Łódź spellings (207 occurrences
recovered in total).

Still absent, ~60 occurrences: `Danzig`, `Birobidzhan`, `Mykolaiv` /
`ניקאָלאַיעוו`, `Lwów (Lemberg)`, `וויניפּעג` (Winnipeg), `ליבאַווע` (Liepāja),
`מעקסיקאָ`, `מינכען` (Munich), `מעץ` (Metz), `אַנטווערפן` (Antwerp),
`וואַשינגטאָן`, `מעליטאָפּאָל`, `מאָהילעוו` (Mogilev).

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

### 5b. `org_type` casing splits every bucket in the lens
16 `org_type` values in `core_db.tsv` differ only by case, so the lens keys
`(qid, org_type)` produce two sections for the same type in every city:

| variants | counts |
|---|---|
| `Traveling Company` / `traveling company` | 125 / **557** |
| `Theatre` / `theatre` | 286 / 31 |
| `Publisher` / `publisher` | 55 / 3 |
| `Education` / `education` | 160 / 2 |
| `Jewish political bodies` / `jewish political bodies` / `Jewish Political Bodies` | 5 / 1 / 4 |
| `Theatre-related Society/ Union` (3 casings) | 1 / 3 / 15 |

…plus Amateur, Business, Circus, Health Institutions, Journals/ Newspapers,
Library, Media, Musical organization, Printer, Religious Institutions.

**Not** an itinerant-filter bug: `pre_explode_clusters.is_itinerant()` lowercases
before comparing, so the 557 `traveling company` rows are still correctly
excluded. The damage is confined to bucket fragmentation — an RA reviewing
"Theatre in Warsaw" sees two sections and can miss half the worklist.

Fix is a one-time normalisation pass over `core_db.tsv:org_type` against the
canonical typology, plus enforcing case at write time in the Zalmen `save_*`
path (see `feedback_zalmen_stale_headers` for the analogous header problem).

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

### 8. ~~Cluster-side pipe-format bug~~ — ❌ MISDIAGNOSED, no such bug

**This item was wrong. Correcting it rather than deleting it, so the bad
number does not get re-derived.**

It originally claimed 715/1037 cluster values (69%) failed because of a
pipe-joined-format bug, citing `לאָדזש | לאָדזש` failing as "the same string
twice". Three errors:

1. The measurement was taken **before** the exonym aliases landed. `ניו יאָרק`
   was the single biggest blocker and now resolves.
2. It counted **distinct values**, not rows — the long tail of one-off
   spellings dominates distinct counts and badly overstates row impact.
3. There is no format bug. All 5158 non-empty values are plain pipe-joined
   strings, and `_resolve_cluster_settlements` splits on both `|` and `;`
   already. `לאָדזש | לאָדזש` failed simply because that *spelling* of Łódź
   was not a key.

Actual measured state: cluster rows went **88.9% → 90.6%** resolved with the
collapse-map source added. The DB side is at 86.7%. What remains is a genuine
long tail (420 distinct tokens / 623 occurrences), not one systemic bug — see
items 4 and 8b.

### 8b. Countries and regions used as settlements
A real category the earlier audit missed. `אַמעריקע` / `אָמעריקע` / `אמעריקע`
(America, ~26 occurrences), `אָרץ-ישראל` (Land of Israel), `פּראָווינץ`
("province"), `רוסלאַנד און בעסאַראַביע` ("Russia and Bessarabia").

These are **correctly** unresolved — the resolver deliberately excludes
countries and provinces. But they are silently dropped, so an org whose only
recorded location is "America" vanishes from the lens with no trace. Decide
whether to surface them as an explicit "country-level only" bucket or flag
them for RA re-coding. Note the collapse map *does* map אַמעריקע → Q30, which
is why `_EXCLUDE_QIDS` exists.

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
