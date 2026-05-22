# Handoff → Kimatch engine: add a geographic-plausibility guard

Source: Dybbuk session 2026-05-22, matching the YIVO Yiddishland gazetteer (3,291 places)
against Kima. See [[project_unified_toponyms]] for the dataset.

## Why (the evidence)
The SKILL.md already warns that **geographic implausibility is not auto-guarded** ("a
spelling matching a far-off place … is *not* yet auto-flagged"). On this run it bit the
**A_autolink** tier — the tier the workflow says is "safe to auto-link":

- **15 of 171 A-grade hits (~9%) were geographically implausible**, most outright wrong:
  `אַפּט → Apt, France` (should be Opatów, Poland), `נאָבל → Nabeul, Tunisia`,
  `קריטי → Crete, Greece`, `לאָווין → Loveno, Italy`, `קאָלק → Kalk/Cologne, Germany`,
  `קראָן → Kranj, Slovenia`, `קאַלין → Kolín, Czech Rep.`, `טורץ → Turets, Belarus`.
- All were `name_exact` at 0.95 and sound-consistent, so neither the ambiguity guard nor
  the phonetic-mismatch check caught them — they are pure **homograph-across-borders**
  false positives. Only geography separates them.

These are silent false positives at exactly the point a human stops looking. A guard here
is the highest-leverage precision win left in the cascade.

## Recommended design — two signals, coordinate-first

`MatchResult` already declares the flag name (`models.py:82`: `"geo_implausible"`) and
already computes `distance_km` (`matcher.py:78,95`). So most of the plumbing exists.

### Signal 1 (primary): coordinate distance — nearly free, dataset-agnostic
When the **input row has coords** and the **chosen Kima place has coords**, `distance_km`
is already populated. Add to `_finalize` (right after `_check_sound`, before grading):

```python
# in _finalize(), after _check_sound(result, db)
_check_geo(result, max_km=db.max_plausible_km)   # see below
```

```python
def _check_geo(result, max_km: float) -> None:
    """Flag a single-place match whose chosen place sits implausibly far from the
    input's own coordinates. Only meaningful when BOTH have coords."""
    if result.kima_place is None or result.distance_km is None:
        return
    if max_km and result.distance_km > max_km and "geo_implausible" not in result.flags:
        result.flags.append("geo_implausible")
```

- `max_km` is **job-configurable** (`thresholds.max_plausible_km`, default ~300). 300 km
  comfortably covers historical border drift (interwar Poland → modern Ukraine/Belarus)
  while catching France/Tunisia/Greece/Italy. Make it None to disable.
- This is the robust signal: no string parsing, works for every script/dataset, and reuses
  a value already on the result. On Yiddishland, 2,962 / 3,291 rows have coords, so it
  covers ~90% directly.

### Signal 2 (fallback): country allow-list — for the coord-less rows
`KimaPlace` has **no structured country field** (`models.py`), so country must come from
the `primary_rom` parenthetical (`"Bar (Vinnyt︠s︡ʹka oblastʹ, Ukraine)"` → `Ukraine`).
That's fragile (the paren may be an oblast/voivodeship), so use it only as a fallback when
coords are absent, and **substring-match** rather than equality:

```python
# pseudo: only if result.distance_km is None (no coord signal available)
allowed = db.region_countries            # job config: ["Ukraine","Poland",...]
neighbors = db.region_neighbors          # historical drift pairs, see below
kima_country = _country_from_label(result.kima_place.primary_rom)  # last (...) token
row_country  = result.input_place.extra.get(db.country_field, "")
if allowed and kima_country and kima_country not in allowed \
   and (row_country, kima_country) not in neighbors:
    result.flags.append("geo_implausible")
```

Historical-neighbor whitelist that worked on this dataset (interwar→modern drift):
```
Poland↔Ukraine, Poland↔Belarus, Poland↔Lithuania, Poland↔Slovakia,
Romania→Moldova, Romania→Ukraine, Hungary→Slovakia/Ukraine/Romania,
Czech Republic→Ukraine, Slovakia→Ukraine, Lithuania↔Belarus, Ukraine↔Moldova
```
Better long-term: derive country server-side from Kima coords (reverse-geocode the dump
once, cache a `country` field on `KimaPlace`) so Signal 2 stops depending on label parsing.

### Grading change
In `grade_result` (`matcher.py:311`), treat `geo_implausible` like `phonetic_mismatch` but
**stronger** — a far-off exact-name hit is more likely wrong than a sound miss:

```python
geo = "geo_implausible" in result.flags
...
if st == MatchResult.NAME_EXACT:
    if geo:
        return MatchResult.GRADE_C        # demote out of auto-link; needs a human
    if result.match_method in ("name_exact","wikidata") and not mismatch:
        return MatchResult.GRADE_A
    ...
```
Demote to **C** (not B): on this run every geo-flagged A-grade was wrong or needed real
research, so it belongs in the human queue, not the glance queue.

## Config surface (job JSON)
```json
"thresholds": { "fuzzy": 0.6, "phonetic": 0.55, "max_plausible_km": 300 },
"geo": {
  "country_field": "country",
  "region_countries": ["Ukraine","Poland","Belarus","Lithuania","Romania",
                       "Hungary","Czech Republic","Slovakia","Latvia","Estonia"],
  "neighbors": [["Romania","Moldova"],["Poland","Ukraine"], "..."]
}
```
Both signals must be **opt-in / no-op when unconfigured** so existing jobs (e.g. the
parallel `fischer_gazetteer.json`, which has no coords-vs-Kima distance and no region list)
are unaffected.

## Test fixtures (regression)
Lock these in — all are real A-grade FPs from this run that must end up `geo_implausible`/C:
| input (yi) | row country | wrongly chosen | correct |
|---|---|---|---|
| אַפּט | Poland | Apt, France (#10855) | Opatów, PL |
| נאָבל | Ukraine | Nabeul, Tunisia (#15787) | Volhynian shtetl |
| קריטי | Ukraine | Crete, Greece (#8648) | — |
| קאָלק | Ukraine | Kalk/Cologne, Germany (#19669) | Kolky, UA |
| קראָן | Lithuania | Kranj, Slovenia (#17827) | — |
And these must STAY grade A (historical drift, not implausible):
`ראַווע→Rava-Rusʹka (UA)`, `סטאַניסלאָוו→Stanislav (UA)`, `קעשענעוו→Chișinău (MD)`,
`באַר→Bar (Vinnytsʹka oblastʹ, UA)`.

## Related
- The one-off cleaned A-grade for this dataset (15 demotions) is already applied in the
  Dybbuk repo: `data/working/yivo_yiddishland_kima.A_autolink.csv` (re-graded) +
  `review_geographic.tsv`. This handoff is to make that systematic engine-side.
- Companion fix already landed this session: `_write_rows` key-union header (`cli.py:201`)
  so `--split-by-grade` + hierarchy no longer crashes on `_parent_resolved`.
