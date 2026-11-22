# Theatre education ↔ Amateur audit (2026-11-08)

Proposals only — nothing written to core_db. Review surfaces (Drive folder 1WsImdrjGxWbp4ujkRBCKZxrTYZ8NGbTl):
- Doc: https://docs.google.com/document/d/1-gXCSoEGT2OVIli27Bp8NqyaBQMew3F-R9yRorY8_VE/edit
- Sheet: https://docs.google.com/spreadsheets/d/1a6I79xPgNe66IRMK3GpwHuTPRUEIO8w5d1Ede_Q6qsA/edit

Pipeline (run in a scratch dir holding `core_db.tsv` and `clustered.tsv` = organizations_clustered.tsv, both via `git show origin/main:`):
1. `build.py` → universe.json (records typed Amateur/Theatre education or with a drama/studio/amateur name cue)
2. host scan (inline in the session; sentence cue over host types) → universe2.json — see sentence regex in judge_helpers.py
3. `judge.py` (Gemini 3.1 Pro, prompt.txt, quote grounding, resumable) → verdicts.jsonl
4. `tables.py` → records.tsv + mentions.tsv (per-mention labels kept, for splits / child records); overrides: camps → Education (policy), 3735 = Maaty's Amateur decision
5. `xlsx.py` → review workbook

## 2026-11-15 — decisions applied
Sinai answered the Doc's open questions on 2026-11-15 (Rules 1–6 accepted; child records "do and document").
6. `apply_decisions_2026_11_15.py --apply` → 144 retypes, 13 child records (db 4750–4762), 2 moves into existing
   units (913, 797), 9 parent links; every write logged in `activity_log.tsv`. Report: `applied_2026_11_15.json`.
   Then `../sync_address_cards.py`.
7. `decisions_2026_11_15.tsv` — status of all 397 records: APPLIED_* / FOR_NOA (cases in Noa's review Doc) /
   PENDING_SINAI (culture & workers' clubs — answer 4 "subtype of amateur" contradicts Rule 3) / NO_CHANGE.
Human type decisions were re-checked against the rules, not accepted automatically (3735 consistent; 2179 and 2277 contradict).

## 2026-11-22 — Noa's answers to Part C applied
Noa answered C1–C20 in the review Doc (https://docs.google.com/document/d/1uP0p68o3nOwzMynFTxMITeWP81N0abOjsAsITcAfJsg/edit).
8. `apply_noa_answers_2026_11_22.py --apply` → 24 mints (db 5079–5102), 15 retypes, 4 merges (1976/1977/3678 → 800, 1951 → 2211),
   7 bare labels retired (2276, 4316, 4604, 4607, 4609, 1188, 3047; leftover mentions GENERIC), 35 mention moves. Report:
   `applied_noa_answers_2026_11_22.json`. Then `../sync_address_cards.py`.
   Where the entry text contradicted a proposal, the text won (stamp Sinai, marked TEXT in the script): C7 Ruskin's studio is
   New York; C15 795 Kiev+Moscow is one studio (only Warsaw → 5064 and Kovno → 5084 split off); C15 Kaminski's united troupe = 239;
   C16 1951 = Yung-Argentine school 2211; 1188's heading mentions = Weichert's studio 2037.
   Policy: ballet/dance schools → Theatre education; DP-camp studios → Amateur; university line (C6) — CLASSIFICATION_POLICY type 7.
