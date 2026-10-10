# Galron press matching — next steps

Agreed with Sinai 2026-10-10, after round 1 was applied (`8e4f29721`).

## Still on the form

The review doc now carries only undecided material — about 80 decisions, most
of them group decisions:

| Section | Decisions |
|---|---|
| Tier C1a | 8 — one Galron paper claimed by 3–9 Zylbercweig orgs |
| Tier C1b | 13 listed, 17 more in the TSV |
| Tier C2 | 4 — bridged across scripts |
| Tier E | 8 groups + 22 singletons |
| Unlinked (Zylbercweig) | 28 — is it a title? |

Tiers A1, A2, B, D and F are decided (113 rows) and removed from the doc.
7 Tier B rows are parked at `RESEARCH_FURTHER`, not open.

## Then, in order

1. **Finalise cluster merging and alignment.** The C1a finding is the lever: 71
   Tier C rows resolve to 35 Galron papers, and 8 papers are claimed by 3+
   Zylbercweig orgs. Those claimant sets are candidate *within-corpus* merges —
   if five orgs all match one paper, some of them are probably each other. The
   entity-matching skill's within-corpus cascade applies, and auto-merge there
   needs `signal == trigram_full`, which none of these have.

2. **Mint the remaining newspapers.** Not Galron's (settled: Zylbercweig side
   only) but the Zylbercweig papers that have no core_db row of their own.
   Mint ids against **origin's** high-water mark at apply time — the app pushes
   core_db on every save, and it moved 2,288 → 2,597 during this work.

3. **Enrich every newspaper entry.** A short comment plus links, from what the
   research turned up. Already available to paste in:
   - `galron_title_families_research.md` — 12 titles with verdicts, sources
   - `galron_union_list.tsv` — years covered, depository, scan URL per title
   - `unlinked_judgements.tsv` — transliteration, gloss, is-it-a-title verdict
   core_db has no comment column; decide whether this goes in `name_variants`,
   a new column, or a sidecar TSV keyed by db_id.

4. **Research the non-Jewish newspapers.** 6 are tagged in the unlinked table:
   Kievlyanin, Odesski Vestnik, Odesskiya Novosti, Pravda, Pinski Listok,
   Bessarabskoya Zhizn. Their absence from Galron is explained — he indexes the
   **Jewish** press — so they need a different source. Likely: Wikidata,
   Russian Wikipedia, the National Library of Russia, Libraria for Ukraine.
   Worth checking whether `org_type` should distinguish them at all.

## Handed off, not ours

`unmatched_press_orgs_handoff.tsv` — 71 publishers to the publisher audit
running in the parallel session (36 already covered by its 157 decisions),
29 printers to Zalmen (26 untouched). None is a mistyped periodical: 0 of 100
read as newspapers by name, and 90 are personal names.
