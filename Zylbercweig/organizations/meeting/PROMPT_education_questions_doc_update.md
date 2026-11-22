# Prompt: close out the Education questions and slim the "Questions for Maaty" Doc

Paste everything below the line into a fresh Claude Code session opened in
`~/Documents/GitHub/Dybbuk`.

---

## Task

Maty has finished the Education org type. Resolve what is still inconsistent in it, then
rewrite the Google Doc
[2026-10-04 Questions for Maaty — Education and the minting rule](https://docs.google.com/document/d/1Z_TL_oR9eMAI1iZGepFcS1jqmu-rY0NrY6Tenv0DR1w/edit)
so it holds only what remains to be done. Maty is a man (he/him).

## Where things stand (checked 2026-11-22 on origin/main)

`Zylbercweig/organizations/org_alignment_review.tsv`, `org_type == "Education"`: 1,085 rows,
0 undecided. NEW 536, GENERIC 372, DEFER 74, ALIGN 66, SPLIT 23 (open), NOT_AN_ORG 7,
SPLIT_DONE 7.

What the Doc's five questions became:
- **Q1 and Q4** (meaning of DEFER / DESCRIPTIVE): settled at the 10-04 meeting. GENERIC and
  NOT_AN_ORG replaced DESCRIPTIVE. **DEFER now means "discuss with Ruthie"**; reviewers use it only
  when really necessary. Do not treat DEFER rows as backlog, do not sweep or redecide them, and do
  not put them in the Doc as questions for Maty.
- **Q5** (rule as project policy): done. All types together have 159 undecided rows left.
- **Q3** (11 multi-mention deferrals): all closed by the `rule_2026_10_04` sweep. One looks wrong:
  the **Berlin conservatory, ORG-C00001_Q03** (3 mentions) was swept GENERIC, although the generic
  rule treats a conservatory as a rare kind that a person decides. Its Warsaw and Vienna siblings are
  minted (db 1857, db 1962).
- **Q2** (same name minted in one place and generic in another): still open. 19 Education names
  have both NEW and GENERIC rows (normalise `canonical_yiddish` by stripping points), for example
  קאָנסערוואַטאָריע 6 NEW / 4 GENERIC, רעאַל-שול 2 / 8, אָרטיקער קאָנסערוואַטאָריע 2 / 3,
  האַנדלס-שול 1 / 4. Nobody has checked whether each NEW row is individuated by its source passage.

## Read first

- Memory: `project_org_decision_vocabulary.md` (the generic rule: what individuates and what does
  not), `project_org_type_status_2026_10_04.md`, `reference_maty_work_surfaces.md`,
  `feedback_ruthie_not_on_org_alignment.md` (DEFER exception), `feedback_zalmen_no_side_channels.md`,
  `feedback_text_resolvable_questions_not_for_pi.md`, `feedback_cli_scripts_must_log_decisions.md`,
  `feedback_zalmen_stamp_reviewer_on_every_decision_write.md`,
  `feedback_dybbuk_local_main_is_always_stale.md`, `feedback_always_link_google_docs.md`.
- The October versions: `Zylbercweig/organizations/meeting/2026-10-04_questions_for_maty.md`
  and `2026-10-04_education_institution_typology.md`.

## Steps

1. `git fetch` and read the review file from `origin/main`; the Zalmen app pushes all day.
2. **Q2:** for each of the 19 names, read the source passage of every NEW and every GENERIC row:
   cluster members through `organizations_clustered.tsv`, entry text from
   `Zylbercweig/people/entry_texts.tsv`. Apply the generic rule. Classify each row as
   *consistent* (the passage individuates the NEW one, or the GENERIC one is a bare kind) or
   *inconsistent*, with the quoted words that decide it. If the text decides it, it is our call,
   not Maty's.
3. **Berlin conservatory, ORG-C00001_Q03:** read its 3 passages. Say whether it is one
   institution, whether it matches an existing core_db org (search by name and Berlin), or
   whether it should be minted.
4. **Open SPLITs:** list the 23 Education rows still on SPLIT (18 Maty, 5 auto_drafter) with
   what each needs.
5. Show me the proposed changes as a table and **wait for my OK** before writing anything to
   `org_alignment_review.tsv` or core_db. When writing: one script, dry-run first, every row
   stamped with a reviewer and date, decisions logged; then commit with explicit paths and push.
6. **Rewrite the Doc** (Google Docs connector; load the google-workspace skill first). Replace
   the body with a short dated status, "Education: what remains (2026-11-xx)":
   - one line saying Education is done and Q1, Q3, Q4 and Q5 are settled (the old text stays in
     the Doc's version history);
   - only the questions that still need Maty, each with the cluster id, the passage quote and
     the concrete choice he has to make;
   - nothing about DEFER rows; those go to Ruthie.

   Keep the title. Read the Doc back after the edit, and give me its link.
7. Update memory: `reference_maty_work_surfaces.md` (Education section) and the MEMORY.md line.
