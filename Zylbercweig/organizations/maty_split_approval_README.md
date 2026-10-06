# Split approval form — Maty, 2026-10-04

`maty_split_approval_2026-10-04.tsv` — 28 rows, one per proposed child organization.

## What happened

Nine clusters that you (or Bella/Sinai) marked **SPLIT** were attested in genuinely different settlements. Each has been split into one child per settlement, and the children are back in the **undecided** queue. Each child now needs a name and a disposition — that is what this form collects.

Splitting by place was applied **only** where it is actually the right basis:

- **Excluded: 26 `auto_drafter` SPLITs.** Those are LLM proposals, not decisions, and need a human to confirm the split before anything is created.
- **Excluded: 25 human SPLITs that are not place-splits.** They are plural-NAME splits (`אוניווערזיטעטן פֿון קעניגסבערג און…` = "the universities of Königsberg *and*…", `בען באַנוס טרופּעס` = "troupe**s**") or source-level partitions. Splitting those by city would invent wrong entities; they need the underlying passages.
- **Spelling variants do not count as different places.** `ווין`/`ווינער`, `ניו יאָרק`/`ניו-יאָרק`, `לעמבער`/`לעמבערג`, `סטאַניסלאָוו`/`סטאָניסלאָוואָוו` were folded to one settlement each.

## Columns to fill

| Column | What to put |
|---|---|
| `decision` | `LINK` · `CREATE` · `DROP` · `HOLD` (see below) |
| `target_db_id` | for `LINK` only — the core DB id it joins |
| `suggested_name` | for `CREATE` — the name the new org should carry |
| `note` | anything you want recorded |

- **`LINK`** — this place-child *is* an organization already in the DB. Put its id in `target_db_id`. The `candidate_names` column lists the current candidates with their Yiddish names and addresses; they come from the parent cluster, so they are suggestions only and are often wrong for a specific city.
- **`CREATE`** — a real organization not yet in the DB. Give it a name in `suggested_name`. Please make the name individuating (include the city) rather than a bare kind-noun, per the 2026-10-04 generic rule: `וואַרשעווער פֿאָלקס-שול`, not `פֿאָלקס-שול`.
- **`DROP`** — not a real organization in this city (bad extraction, a passing mention, a duplicate of another child).
- **`HOLD`** — needs discussion; say why in `note`.

## A caution on the names

Every child currently carries the **parent's** name, which is usually generic — six children of `ORG-C00238` are all called `פֿאָלקס-שול`. That is exactly the generic-name problem from the meeting: six "folk schools" in six cities are six different institutions. The `suggested_name` column is where that gets fixed, so please treat it as the main thing this form is asking for.

Nothing is minted until you return this and it is applied.
