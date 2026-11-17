# TODO: mentions that can't reach their person hub (found 2026-11-15)

## The problem

A mention attaches to a person hub only in two ways:
1. through the hub's **validated surfaces**: spellings an RA confirmed, which come from
   `derived_mention_alignments.tsv` via `build_person_hub.py`;
2. for bare-surname mentions, through the surname resolver (`mention_surname_resolutions.tsv`).

A full-name mention whose spelling was never validated has no route to its hub, even when
the hub exists. **1,113 of 2,900 hubs (38%) have no validated surface at all**, and 1,110 of
those are lexicon-entry hubs. This is a coverage gap of the validation work, not a bug in
the builder. But it means that, for over a third of the people, the full-name mentions
float free.

The problem surfaced while linking the 387 Wikidata-reconciled mention names
(`people_wikidata_qids.tsv`; that QID work is **parked**). 236 of them came out `no_hub`.
That label means "no hub lists this spelling", not "this person has no hub". They fall into
four situations:

| category | names | mentions | meaning | examples |
|---|---|---|---|---|
| A — hub exists, unreachable | 18 | 87 | in people_db and has a hub; the spelling isn't a surface, or the hub has none | Ida Kamińska `HUB-D3832` (no surfaces, 56 mentions; plus a likely duplicate DB row 3826), Witler `HUB-D3272`, Lateiner `HUB-D683` (other spelling) |
| B — in DB, no hub | 3 | 8 | DB row exists, no hub carries it | Regina Zuckerberg, Hillel Zeitlin |
| C — surname matches a hub | 71 | 144 | same person under another spelling, or a namesake: needs review | Shternberg, Spivak, Rachel Auerbach, Bialik |
| D — no trace | 144 | 315 | mention-only people: no entry, no DB row, so no hub by design | Melech Ravitch (30), Itzik Manger, the Zemachs, Kulbak, Victor Hugo |

These 236 are only the names that went through Wikidata. The real extent is set by the
1,113 surface-less hubs.

## To do

- [ ] **Size it properly.** `hub_reachability_empty_surface_hubs.tsv` gives a lower bound
  of 328 missed mentions, from exact-name matching of each surface-less hub's own names
  against `mentions_all`. Top of the list: Peretz Sandler (19), Witler (12), Max Pearlman
  (8), Ida Kaminska (8). Spelling variants will add many more. Decide whether to run the
  resolver/matcher over full-name mentions for these hubs.
- [ ] **Category A (18):** give these hubs their surfaces (validation, or a resolver pass).
  Check the Ida Kamińska DB duplicate 3826 / 3832.
- [ ] **Category B (3):** why does the DB row have no hub? Check against the Hebrew-stylistics
  split and the db-only-hub rules.
- [ ] **Category C (71):** review queue: same person or namesake?
- [ ] **Category D (144): policy decision (Sinai).** Should prominent people who are only
  mentioned (Ravitch, Manger, Kulbak …) get hubs? Today only the documented special cases
  do (`dbonly_hubs.tsv`, eponym mints).

## Files

- `audit_hub_reachability.py`: read-only. Re-run after any `build_person_hub.py` run.
- `hub_reachability_empty_surface_hubs.tsv`: surface-less hubs, with an estimate of the mentions each misses.
- `hub_reachability_reconciled_names.tsv`: the 236, with category, db_ids and candidate hubs.

## 2026-11-17: a full `build_person_hub.py` rerun is NOT safe yet (drift found)

Rerunning derive + build today, after adding `mention_surface_rejections.tsv`, changed
11 hub rows beyond the intended fix. They were NOT committed; only the HUB-D1247 row was
applied, surgically. Review these before the next full rebuild:
- **`אידא קאַמינסקאַ` (×56) would attach to HUB-D3612 = Regina Kaminska.** This is wrong. Ida's own hub HUB-D3832 still has no surfaces (group A above).
- `מאיר אייזענבערג` now matches two entries (P-1-facs_42… and P-6-facs_123…), so HUB-D1554 would lose its surface.
- HUB-P-5-facs_604… would gain `חיהלע פֿרידמאַן`.

New mechanism: `people/mention_surface_rejections.tsv` (surface + as_heading + reason)
is honoured by `derive_mention_alignments.py` (db_status=rejected, no matched ids), so a
bad RA-validated surface can be dropped without editing the RA sheets. First entry:
ביעלאָצערקאָוו (= Bila Tserkva) wrongly aligned to Tsile Bel, HUB-D1247 (Sinai, 11-17).
