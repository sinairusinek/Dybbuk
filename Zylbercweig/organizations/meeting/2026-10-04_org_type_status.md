# Organization alignment — status by type

Prepared 2026-09-30 for the meeting of Sunday 4 October 2026.
Source: `Zylbercweig/organizations/org_alignment_review.tsv` (8,072 cluster rows), as of commit on `origin/main`.

## Headline

| | |
|---|---|
| Cluster rows total | 8,072 |
| Decided | 4,025 (50%) |
| Undecided | 4,047 (50%) |
| Types fully closed | 10 of 33 |

The halfway point is real, but it is not evenly spread: **Theatre alone accounts for 1,710 of the 4,047 undecided rows (42%)**.

## Types that are finished

| Type | Rows | Closed mainly by |
|---|---:|---|
| Traveling Company | 1,338 | Ruthie |
| Education | 1,222 | Maaty |
| Theatre education | 17 | Ruthie, Judith |
| Library | 14 | Ruthie |
| Company on Tour | 7 | Judith, Sinai |
| Judenrat | 7 | Ruthie |
| Circus | 4 | |
| Sports/Recreation | 4 | |
| Fraternal order | 3 | |
| Printer / Printer-Publisher | 2 | |

One caveat on Education: it is at 100% *decided*, but **559 of its 1,222 rows (46%) are DEFER** — a deliberate "do not mint" judgement, not unfinished work. See the companion document for why, and for what needs confirming with Maaty.

## Types that remain

Sorted by how much is left. "Singletons" = clusters attested by a single mention, which by construction can link nothing.

| Type | Undecided | Total | Done | Singletons (of undecided) |
|---|---:|---:|---:|---:|
| Theatre | 1,710 | 2,492 | 31% | 1,394 (82%) |
| Journals / Newspapers | 510 | 661 | 23% | 385 (75%) |
| Publisher | 305 | 398 | 23% | 267 (88%) |
| Jewish political bodies | 261 | 274 | 5% | 214 (82%) |
| Theatre-related Society / Union | 224 | 289 | 22% | 184 (82%) |
| Welfare / Aid organization | 157 | 175 | 10% | 144 (92%) |
| Musical organization | 111 | 129 | 14% | 102 (92%) |
| Non-Jewish political bodies | 97 | 104 | 7% | 89 (92%) |
| Religious institutions | 87 | 117 | 26% | 79 (91%) |
| Business | 79 | 83 | 5% | 77 (97%) |
| Media (Radio / Film / TV) | 75 | 80 | 6% | 64 (85%) |
| Heritage Institution | 67 | 80 | 16% | 53 (79%) |
| Trade Union / Professional Assoc. | 64 | 71 | 10% | 59 (92%) |
| Amateur | 61 | 74 | 18% | 56 (92%) |
| Non-Yiddish Theatre | 57 | 59 | 3% | 55 (96%) |
| Military | 48 | 48 | 0% | 31 (65%) |
| Not an organization | 46 | 48 | 4% | 41 (89%) |
| Labour (factory / workshop) | 38 | 45 | 16% | 34 (89%) |
| Health institutions | 18 | 25 | 28% | 17 (94%) |
| Kleinkunst | 18 | 19 | 5% | 17 (94%) |
| OTHER — elaborate! | 13 | 25 | 48% | 13 (100%) |

**Military (48 rows) is the only type nobody has touched at all.** It is small enough to close in one sitting.

## The singleton problem

**3,375 of the 4,047 undecided rows (83%) are singleton clusters** — one mention, one person, no second attestation to link to.

This matters for how we plan the remaining work. A singleton cluster cannot produce a link between two people; minting it creates a database entity that will never connect anything. The question for each type is therefore not only "how many rows are left" but "how many rows could possibly earn their place in the graph."

If the Education precedent holds — defer the singletons, mint the individuated multi-mention cases — then the *decision-making* load on the remaining 4,047 rows is much smaller than the row count suggests, but it still requires a human pass to separate the two.

## Theatre: can the rest be auto-minted?

**Tested and the answer is no.** The proposal was that the team has already worked the larger cities at place level, so most remaining theatres might be the only theatre in their town and could be minted automatically.

Of the 1,710 undecided Theatre clusters:

| | | |
|---|---:|---:|
| No settlement recorded at all | 620 | 36% |
| Exactly one settlement | 996 | 58% |
| Two or more settlements | 94 | 6% |

Of the 996 with exactly one settlement:

| | | |
|---|---:|---:|
| Only theatre cluster in that settlement | 115 | 12% |
| Shares its settlement with other theatre clusters | 881 | 88% |

So **at most 115 rows (7% of undecided Theatre) qualify as "alone in their location"** — and 594 of the remainder sit in towns holding 20+ theatre clusters. Normalising Yiddish spelling variants (`ניו-יאָרק` / `ניו יאָרק`, `בוענאָס איירעס` / `בוענאָס-איירעס`) changes 115 to 112, so the finding is not an artefact of spelling.

Two further reasons the 115 are not a safe auto-mint batch:

1. **103 of the 115 (90%) are singletons** — one mention each. Minting them adds entities that link nothing.
2. **Nearly all carry candidate DB matches already.** Rows like `רוסישן טעאַטער אין קאַזאָן` list candidates `711 | 712 | 39 | …`. "Only one in its settlement" does not mean "not already in the DB" — it may be an alignment, not a new mint.

The premise about the big cities also does not hold: New York is only 19–31% decided, Warsaw 35%. The large cities are **not** finished.

**Recommendation:** no auto-minting for Theatre. The 115 sole-occupants are worth a manual pass as a named batch, but they are a small fraction, not a shortcut through the 1,710.

## Suggested allocation

- **Military (48)** — unclaimed, small, closable immediately.
- **Theatre (1,710)** — the main job; already shared across Maaty, Bella, Arne, Sinai. Needs a deliberate split, probably by city.
- **Theatre-related Society / Union (224)** — Noa's assigned type; she has 3 decisions recorded, so this continues rather than restarts.
- **Company on Tour** — finished, so Judith needs a new assignment.
- **Journals/Newspapers (510) and Publisher (305)** — the next-largest blocks, currently unassigned.

## A note on the figures

These counts come from the `decision` column of the alignment TSV. They measure *decisions recorded in the app*, which is the only thing visible in the repository. Work done and not saved, or recorded elsewhere, does not appear here.
