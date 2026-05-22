# Fischer gazetteer → Kima match (baseline run, 2026-05-22)

Match of the **Fischer gazetteer** (`Expanded-Gaz-TENTATIVE.xlsx`, Sheet1 → 22,932 Hebrew
spellings across 9,723 places/UIDs) against the Kima Historical Gazetteer, via the `kimatch`
skill/engine. No priors; A/B/C grading + safety guards on.

## Files

- **fischer_matched.csv** — one row per input spelling, with the engine's verdict columns
  (`_match_status`, `_grade`, `_kima_id`, `_sound_match`, `_flags`, `_distance_km`,
  `_candidates`). The `.A_autolink.csv` / `.B_review.csv` / `.C_review.csv` are the same rows
  split into triage queues by grade (only A is auto-link-safe).

- **fischer_matched.by_uid.tsv** — regrouped to one row per place (UID): the anchor Kima id, an
  all-spellings-agree flag, the alternative candidate ids seen across the UID's spellings
  (future-ambiguity record), and donatable extra HebName variants.

- **fischer_matched.uid_conflicts.tsv** — the 126 UIDs whose confident spellings disagreed on
  which Kima place they are (≥1 spelling mismatched).

- **fischer_matched.conflicts_resolved.tsv** — those 126 conflicts arbitrated by Fischer's own
  lat/lon (the correct Kima place is the candidate nearest the gazetteer's coordinates).
  `resolution_quality`: `RESOLVED_near` (105, true match — the loser was a duplicate Kima record
  or a wrong homograph), `RESOLVED_mid` (7), `NO_GOOD_MATCH` (13, no candidate is near → manual
  search / candidate new place), `NO_COORD` (1).

- **fischer_matched.phonetic_resolved.tsv** — the 136 `phonetic_mismatch` rows (matched by
  spelling but don't *sound* like the chosen place — the wrong-city homograph class), verdict by
  distance from Fischer's coords: `REJECT` (60, >300km — clear wrong city, drop the link),
  `CHECK` (40, 50–300km), `KEEP` (30, ≤50km — sound-check false alarm, the match is fine),
  `NO_COORD` (6).

## Scripts (in ../../../scripts/)
- `consolidate_fischer_by_uid.py` — builds the `.by_uid` + `.uid_conflicts` views.
- `resolve_fischer_conflicts.py` — coord-arbitrates conflicts + phonetic_mismatch rows.
