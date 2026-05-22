# Fischer gazetteer → Kima match + donations (2026-05-22)

Match of the **Fischer gazetteer** (`Expanded-Gaz-TENTATIVE.xlsx`, Sheet1 → 22,932 Hebrew
spellings across 9,723 places/UIDs) against the Kima Historical Gazetteer, via the `kimatch`
skill/engine, then coordinate-arbitrated resolution and Stage-2 donation export.

## Pipeline (scripts in ../../../scripts/)
1. `kimatch match` (engine, A/B/C grading + safety guards) → `fischer_matched.csv` (+ grade splits).
2. `resolve_fischer_all.py` → `fischer_resolved.tsv`: one row per spelling, resolved by Fischer's
   own lat/lon (the correct Kima place is the candidate nearest Fischer's coords). This single pass
   recovers fuzzy/ambiguous rows, geo-verifies exact matches (catches cross-border homograph FPs —
   Wayne NJ for Vienna, Sydney NS for Sydney), and applies a name-aware large-entity rule (a Kima
   name == Fischer EngName allows country-scale distance up to 1500km, so polygonal countries like
   Argentina/Brazil aren't false-rejected, while transcontinental homographs still are).
3. `export_fischer_donations.py` → the three donation files below.

## Files
- **fischer_matched.csv** (+ `.A_autolink/.B_review/.C_review.csv`) — raw engine output per spelling.
- **fischer_resolved.tsv** — unified resolution per spelling. Key cols: `resolved_kima_id`,
  `dist_km`, `method` (exact_geoverified / coord_rearbitrated / none), `verdict`
  (KEEP ≤50km or name-confirmed large entity / REVIEW 50–300km / REJECT >300km /
  NO_CANDIDATE / NO_COORD), `candidates` (alternatives — the per-spelling ambiguity record).
- **fischer_donations.tsv** / **.json** — NEW HebName variants Kima lacks for confirmed (KEEP)
  places. **10,885 variants across 3,369 places.** (Includes epithets/calques like Austria +=
  עיר הדמים — Kima wants these; future TODO is to label `variant_type`.)
- **fischer_external_id_donations.tsv** — Fischer's KaganID / JGenID / USBGN / YS_id for confirmed
  places. **9,944 ids** (kagan 2,579 · jewishgen 3,650 · us_bgn 3,636 · ys 79). The live Kima API
  (2026-05-22) tracks only MAZAL/NAF/VIAF/GeoNames/WikiData, so these are *proposed new id-types*.
  ⚠ ~3,618 us_bgn values are negative (likely a sign/encoding artifact in the source) — flagged.
- **fischer_confirmed_decisions.tsv** — confirmed (UID, spelling)→kima_id; reusable as
  `kimatch match --prior-resolutions`.

## Headline numbers
13,814 confident (KEEP) rows → **4,552 Kima places / 4,733 UIDs anchored**. Coord arbitration
recovered or corrected 3,640 rows where the nearest candidate beat the engine's pick. 71 UIDs had
KEEP rows disagreeing on the place (anchor = majority). REJECT/NO_CANDIDATE rows (~7,400 incl.
4,558 genuinely absent from Kima) are out of scope here — candidates for new Kima places.
