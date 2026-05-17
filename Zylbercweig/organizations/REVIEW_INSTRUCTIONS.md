# Organization Review: Step-by-Step Guide

You are reviewing rows from the Zylbercweig Lexicon — a Yiddish biographical encyclopedia of theatre people. Each row is a mention of an organization (a theatre, troupe, school, etc.) extracted from someone's biographical entry.

The computer has tried to classify each mention into one of these categories:

- **proper_name** — a specific named organization (e.g., "Lemberg Yiddish Theatre")
- **descriptive_term** — a generic description, not a name (e.g., "a school," "wandering troupe")
- **ambiguous** — could be either; not enough information to decide
- **not_an_organization** — not an organization at all (empty row, or a reference to a person)

Your job is to check whether the computer got it right, and correct it where it didn't.

---
https://972-and-local-call.streamlit.app/

## Before you start

Open `review_sample_main.tsv` in Google Sheets or Excel. You will fill in three columns:

| Column | What to write |
|---|---|
| `reviewer_correct` | `TRUE`, `FALSE`, or `UNCERTAIN` |
| `reviewer_suggested_type` | Only if you wrote `FALSE` — write the correct type |
| `reviewer_notes` | Any comment (optional, but helpful) |

For `reviewer_suggested_type`, use exactly one of: `proper_name`, `descriptive_term`, `ambiguous`, `not_an_organization`.

---

## For each row, follow these steps

### Step 1 — Read the context

There are two columns that give you the surrounding text. The column headers are very long — scroll right until you find them.

First, look for **`_ - organizations - _ - relations - _ - original_sentence`** (it is roughly in the middle of the spreadsheet, after the location columns). This is the sentence from the entry where the organization was mentioned. It may be blank for some rows.

If that column is blank, look instead at **`_ - span`** (near the left side of the spreadsheet). This contains the full biographical entry text — longer, but still useful.

Read whichever is available. Example:
> אויפֿגעפֿירט געוואָרן אין לעמבערגער יידישן טעאַטער פ'ס פּיעסע „שמשון הגבור"
> *(performed at the Lemberg Yiddish Theatre)*

### Step 2 — Read the name

Look at these three columns together:

- `_ - organizations - _ - title` — the org's name as extracted
- `_ - organizations - _ - descriptive_name` — a description of the org from the entry
- `clustered organization` — a cleaned-up version of the name (often the clearest)

Usually one of these will have the organization's name or description. Sometimes all three are empty — that means the row has no content.

### Step 3 — Decide

Ask yourself: **Is this a specific named organization, or a generic description?**

Use the table below:

| What you see | Classification |
|---|---|
| A name you could look up — a specific theatre, troupe, school with an identity | `proper_name` |
| A generic phrase — "a school," "wandering troupe," "theatres in New York" | `descriptive_term` |
| Ambiguous — could be a name or a description, you genuinely can't tell | `ambiguous` |
| All fields are empty, or it refers to a person rather than an organization | `not_an_organization` |

### Step 4 — Fill in the columns

- If the computer's `name_type` matches your decision → write `TRUE` in `reviewer_correct`. Done.
- If it doesn't match → write `FALSE` in `reviewer_correct`, and write the correct type in `reviewer_suggested_type`.
- If you really can't tell → write `UNCERTAIN` in `reviewer_correct`. Leave `reviewer_suggested_type` blank.

---

## Examples

**Example 1**
- title: `לעמבערגער יידישן טעאַטער`
- sentence: *performed at the Lemberg Yiddish Theatre*
- computer says: `proper_name`
- → This is clearly a named theatre. Write **TRUE**.

**Example 2**
- title: `שול`
- sentence: *studied at a school*
- computer says: `descriptive_term`
- → "A school" is generic — no specific name. Write **TRUE**.

**Example 3**
- title: `שטאָטישע שול`  *(city school)*
- sentence: *performed at the city school*
- computer says: `proper_name`
- → "City school" is a generic description, not a proper name. Write **FALSE**, `reviewer_suggested_type` = `descriptive_term`.

**Example 4**
- title: `ביי אַברהם אַקסעלראָד`  *(at Abraham Axelrod's)*
- sentence: *performed at Abraham Axelrod's troupe*
- computer says: `proper_name`
- → This refers to a person, not an organization name. Write **FALSE**, `reviewer_suggested_type` = `not_an_organization`. Add a note: *person reference — Axelrod's troupe*.

**Example 5**
- All fields empty, sentence empty
- computer says: `not_an_organization`
- → Correct — nothing here. Write **TRUE**.

**Example 6**
- descriptive_name: `טעאַטערס אין ניו יאָרק`  *(theatres in New York)*
- computer says: `descriptive_term`
- → This is a general reference to theatres in New York, not a specific named theatre. Write **TRUE**.

---

## Things to watch for

**A city name + institution type = usually a proper name.**
*לעמבערגער טעאַטער* (Lemberg Theatre), *וואַרשעווער פֿאַראיין* (Warsaw Society) — these are specific named organizations, even if they sound generic in translation.

**An adjective + generic noun = usually a descriptive term.**
*שטאָטישע שול* (city school), *יידישע טרופּע* (Jewish troupe), *פּוילישן שטאָט-טעאַטער* (Polish state theatre) — these describe a type of organization, not a specific named one.

**A person's name in the title = probably not an organization.**
If the title is just a person's surname (*גליקמאַן*) or a phrase like *ביי [name]* (at [name]'s), the row is referring to a person, not an organization. Mark as `not_an_organization` and note the person's name.

**When in doubt, write `UNCERTAIN`.**
It is better to say you don't know than to guess. If you write `UNCERTAIN`, add a note explaining what confused you — that information is useful.

---

## File 2: `review_sample_person_ref.tsv`

After you finish File 1, open this second file. It contains 85 rows that the computer flagged as possible person references — rows where the name looks like a person rather than an organization (e.g., a bare surname, or a phrase starting with *ביי/מיט*).

You will fill in four columns:

| Column | What to write |
|---|---|
| `reviewer_is_person_ref` | `TRUE` if it really is a person reference; `FALSE` if it is actually an organization |
| `reviewer_suggested_type` | Only if you wrote `FALSE` — write: `proper_name`, `descriptive_term`, or `ambiguous` |
| `reviewer_org_candidate` | Only if you wrote `FALSE` — write the organization's name as you would spell it |
| `reviewer_notes` | Any comment — especially: who is the person being referred to? |

### Step by step

1. Read the `original_sentence` column first.
2. Look at the `title` and `descriptive_name` columns.
3. Ask: **Is this the name of a person, or the name of an organization?**
   - If the name is a person (bare surname, or "at [person]'s troupe") → write `TRUE`.
   - If it turns out to be an organization name that was misidentified → write `FALSE` and fill in the other columns.
4. Either way: if you can identify the person being mentioned, write their name in `reviewer_notes`. This information will be used later to link people and organizations.

### Examples

**Row**: title = `גליקמאַן`, org_type = `theatre`
- This is just a surname. The computer flagged it correctly.
- → Write `TRUE`. Note in `reviewer_notes`: *likely Glickman's theatre — identify which Glickman*.

**Row**: title = `ביי אַברהם אַקסעלראָד`, org_type = `troupe`
- "At Abraham Axelrod's" — person reference.
- → Write `TRUE`. Note: *Abraham Axelrod's troupe — person reference*.

**Row**: title = `אַקטיאָרן-קלוב`, org_type = `organization`  *(Actors Club)*
- The flag misfired — this is an organization name.
- → Write `FALSE`, `reviewer_suggested_type` = `proper_name`, `reviewer_org_candidate` = `אַקטיאָרן-קלוב`.
