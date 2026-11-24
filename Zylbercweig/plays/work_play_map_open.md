# work_play_map.tsv — the 6 rows that do not join (2026-11-25)

`work_play_map.tsv` persists the catalogue-work → `plays_db` join (271 OK).
Six rows do not resolve. They are **not** defects in the map; they are places
where the catalogue and `plays_db` genuinely disagree about how many plays exist.
Nothing downstream should join them until they are decided.

Regenerate / verify:

```
cd Zylbercweig/plays
python3.11 build_work_play_map.py          # rewrite
python3.11 build_work_play_map.py --check  # fail if stale
```

## AMBIGUOUS (4 rows = 2 pairs) — one title, two catalogue works, one play

In both pairs the catalogue has **two distinct works with the same Yiddish
title, one ascribed to Lateiner and one to Hurwitz**, while `plays_db` has a
single play. The old title-matching join picked whichever play matched and threw
the other work away, which silently erased a Lateiner/Hurwitz distinction the
catalogue draws deliberately.

| expr | title | catalogue playwright | catalogue attribution | plays_db |
| --- | --- | --- | --- | --- |
| 3868 | דוד בן ישי | Lateiner | certain | PL-0068, `author_db_id` 684 (Hurwitz) |
| 3960 | דוד בן ישי | Hurwitz | unknown | " |
| 3925 | יהודית און האָלאָפֿערנעס | Lateiner | certain | PL-0193, `author_db_id` 683 (Lateiner) |
| 4061 | יהודית און האָלאָפֿערנעס | Hurwitz | unknown | " |

**The question (for Ruthie):** is each pair one play or two? Three possibilities,
and the evidence differs per pair:

1. **Two different plays that share a title** — then `plays_db` is missing one,
   and a second play row should be minted.
2. **One play, ascribed to both at different times** — then one catalogue work is
   a duplicate and should be `merged_into` the other.
3. **One play, and one of the two ascriptions is wrong** — then the losing
   catalogue work keeps its row with `attribution = false ascription`.

Note both pairs follow the same shape: the Lateiner work is `certain`, the
Hurwitz one `unknown`. For PL-0068 `plays_db` says Hurwitz while the `certain`
catalogue work says Lateiner, so that pair is also one of the 17 author
disagreements.

## UNMAPPED (2 rows) — catalogued, but no play in plays_db

| expr | title | playwright |
| --- | --- | --- |
| 3881 | הוגענאָטן (Hugenotn) | Lateiner |
| 3966 | די עקדה אָדער פרינץ פון בית לחם | Hurwitz |

**The question:** should `plays_db` gain a play for each, or is the catalogue row
itself doubtful? These were already reported as "not in the Plays KG play list"
before this change; persisting the map just stops them being re-derived each run.

## Why this matters for the L/H subsumption

Phase 3 imports the small graph into the lexicon KG keyed on this join. Importing
an `AMBIGUOUS` row would attach a Lateiner work's evidence to a play recorded as
Hurwitz's — exactly the kind of silent mis-ascription the "both claims stand"
decision exists to prevent. See the memory note `project_lh_subsumption_plan`.
