# Zylbercweig Lexicon datasets: a guide for outside users

This page explains the datasets we extracted from the *Lexicon of the Yiddish
Theatre*. It is written for researchers who have never seen this project and do not
read Yiddish. It covers what the data is, where each file is, what the columns mean,
how far to trust each part, and how the files connect.

Everything is in one GitHub repository that we use as a working space, so most of
it is not relevant to you. **Use only the files listed here.**

---

## 1. What the source is

Zalmen Zylbercweig's *Leksikon fun yidishn teater* (Lexicon of the Yiddish
Theatre) is a printed biographical encyclopaedia in seven volumes, published from
1931 onwards. It has entries on actors, playwrights, directors, composers,
troupes and theatres of the Yiddish stage worldwide: Eastern Europe, the Americas,
Western Europe, Palestine/Israel, South Africa, Australia. Each entry is a few
lines to several pages of biography. The entries mention thousands of other
people, organisations and places.

**How we turned it into data:**

1. **Text recognition.** The printed pages were run through Transkribus with a
   model we trained on the Lexicon's typeface. The text therefore contains some
   recognition errors.
2. **Extraction by a language model.** Each entry was given to a large language
   model (Google Gemini). It returned the people, organisations and places
   mentioned, the relation to the entry's subject, and the sentence the mention
   came from.
3. **Grouping and review.** Scripts grouped the mentions that refer to the same
   thing. Research assistants and the project lead then reviewed part of this by
   hand. Section 6 says which parts.

The data covers **3,032 entries from all seven volumes**. That is a subset of the
Lexicon, not the whole of it.

---

## 2. What you need to know about Yiddish (no reading knowledge required)

Yiddish is written in the **Hebrew alphabet, right to left**. Almost all names and
places in these files are in that script. Yiddish uses its own spelling
conventions, which differ from Hebrew's. Several features affect entity matching
directly:

- **Vowel marks.** Standard Yiddish spelling adds small marks under some letters:
  אַ is *a*, אָ is *o*, פּ is *p* and פֿ is *f*. Many sources, including Wikidata's
  Yiddish labels, leave them out. So `וואַרשע` (in the Lexicon) and `ווארשע` (on
  Wikidata) are the same word, *Varshe*, which is Warsaw.
- **Unicode.** The same marked letter can be stored either as one character (for
  example U+FB2E for אַ) or as a letter plus a separate mark (U+05D0 U+05B7).
  **Apply Unicode NFC normalisation to every string before comparing.** If you
  don't, identical-looking strings won't match.
- **Spelling variation.** One place often has several spellings in the corpus.
  Boston appears 5 ways: `באַסטאָן`, `באָסטאַן`, `באָסטאָן`, … New York is both
  `ניו יאָרק` and `ניו-יאָרק`.
- **Endings on names.** Yiddish adds endings to names in some grammatical
  positions. `גימפּעלן` is "Gimpel" with an ending. `לאָדזשער` means "of Łódź",
  as in "the Łódź troupe".
- **Abbreviations and initials.** `גוב.` stands for *guberniya*, a province of the
  Russian Empire. Within an entry, the subject is often referred to only by an
  initial, such as `ב.`.
- **Hebrew words.** Some words are Hebrew rather than Yiddish and are spelled the
  Hebrew way. For example, `בית עולם` means "cemetery".
- **Words that look like place names.** Some ordinary words match obscure place
  names character for character. `שול` (synagogue) matches Šiauliai, and `קהלה`
  (community) matches Kahla. Our place links have been audited for this; see §5.

You don't need to read the script to use the data. Every linked place has English,
Latin-script and Hebrew names, and many people and organisations have English names.

---

## 3. The files at a glance

Paths are relative to this folder.

| # | File | One row is… | Rows | Language of names |
|---|---|---|---|---|
| **A** | [`place_mentions_with_context.tsv`](place_mentions_with_context.tsv) | a place mentioned in a sentence, with the sentence and its Wikidata/Kima link | 41,339 | Yiddish + English/Hebrew/Latin for linked places |
| B | [`../zibn-shtern/data/working/toponyms_gazetteer.csv`](../zibn-shtern/data/working/toponyms_gazetteer.csv) | a real-world place, with all its spellings in the corpus | 949 | Yiddish + English, Kima Latin and Hebrew |
| C | [`../zibn-shtern/data/working/toponyms_attestations.csv`](../zibn-shtern/data/working/toponyms_attestations.csv) | a place spelling linked to the outside world (the source of A's links) | 19,330 | Yiddish + English |
| D | [`../zibn-shtern/data/working/toponyms_unlinked.csv`](../zibn-shtern/data/working/toponyms_unlinked.csv) | a place spelling we could **not** resolve | 748 | Yiddish |
| E | [`../people/mentions_all.tsv`](../people/mentions_all.tsv) | a person mentioned in an entry, with the sentence | 38,269 | Yiddish |
| F | [`../people/entry_mentions_slim.tsv`](../people/entry_mentions_slim.tsv) | the same as E, slimmed down and with the entry ID filled on every row | 38,269 | Yiddish |
| G | [`../people/entry_texts.tsv`](../people/entry_texts.tsv) | one full Lexicon entry (the complete text) | 3,032 | Yiddish |
| H | [`../people/person_hub.tsv`](../people/person_hub.tsv) | one real person, with every way of identifying them | 2,900 | Yiddish |
| I | [`../people/person_hub_members.tsv`](../people/person_hub_members.tsv) | one piece of evidence belonging to a person (an entry, a database row, a spelling) | 9,099 | Yiddish + English |
| J | [`../people/people_db.tsv`](../people/people_db.tsv) | a person in the research group's database, with an English name | 3,027 | Hebrew-script + English |
| K | [`../people/mention_resolution_decisions.tsv`](../people/mention_resolution_decisions.tsv) | a human decision about who a mention refers to | 1,338 | Yiddish |
| L | [`../people/derived_mention_alignments.tsv`](../people/derived_mention_alignments.tsv) | a name spelling tied to the entry of the person it refers to | 3,446 | Yiddish |
| M | [`../organizations/organizations_clustered.tsv`](../organizations/organizations_clustered.tsv) | an organisation mentioned in an entry, with the sentence and a cluster ID | 16,454 | Yiddish |
| N | [`../organizations/core_db.tsv`](../organizations/core_db.tsv) | one real organisation, with its English name | 3,632 | English + Yiddish |
| O | [`../organizations/org_addresses_review.tsv`](../organizations/org_addresses_review.tsv) | an organisation's location as checked by a person, with coordinates | 3,233 | Yiddish + English |
| P | [`../zibn-shtern/data/working/kima/matching_corrections_log.tsv`](../zibn-shtern/data/working/kima/matching_corrections_log.tsv) | a wrong place link we caught, with its cause | small | Yiddish + English |

All files are UTF-8. The `.tsv` files are tab-separated. The `.csv` files are
comma-separated and some start with a byte-order mark, so read them with
`encoding="utf-8-sig"`. A few TSV cells are very large (whole entries), so raise
your CSV reader's field-size limit.

---

## 4. How the files connect

```
                    entry_id  (e.g. P-1-facs_100_tr_1740521002 = volume 1, one entry)
                        │
   ┌────────────────────┼──────────────────────┬─────────────────────────┐
   │                    │                      │                         │
 G entry_texts     E/F person mentions     M org mentions          A place mentions
 (full text)       (sentence each)         (sentence each)         (sentence each)
                        │                      │                         │
                 K/L/I: who is it?        cluster_id ─► N core_db    wikidata_qid ─► B gazetteer
                        ▼                 (one real org,             kima_id      ─► Kima gazetteer
                 H person_hub ─► J people_db   English name)          (+ Wikidata online)
                 (one real person)  (English name, some
                                     NLI / Wikidata IDs)
```

- **`entry_id`** (`P-<volume>-<page/line id>`) is the key that ties everything to a
  Lexicon entry. In E it is called `host_person_id` (via F) or `host_xml_id`
  (without the `P-`). In M it is built from `File` (volume) + `_ - xml:id`. In C it
  is `source_record_id` (without the `P-`).
- **People.** A *hub* (H) is one real person. Hub IDs look like `HUB-D1000`, where
  1000 is their ID in J.
- **Organisations.** A *cluster* (`ORG-C00123`, in M) is a group of mentions that
  were judged to be the same organisation. N lists the real organisations, and its
  `linked_cluster_ids` column says which clusters belong to each.
- **Places.** Every linked place has a **Wikidata ID** (`Q270` = Warsaw) and most
  have a **Kima ID**. Kima is a historical gazetteer of place names in Hebrew
  script (including Yiddish spellings).

---

## 5. Places in detail

**External links exist only for places.** People and organisations link to the
research group's own databases (J and N), plus a small number of National Library
of Israel and Wikidata IDs for people.

### A. `place_mentions_with_context.tsv` (start here)

We built this file for outside users. It joins the place links (C) back to the
text, so every place mention has its sentence next to it. It is produced by
[`build_place_mentions_with_context.py`](build_place_mentions_with_context.py).

The rows come from three sources (`source_kind`):

| `source_kind` | What it is | Rows | Link quality |
|---|---|---|---|
| `vital_place` | where the entry's subject was born, died or was buried | 7,167 | linked by our place pipeline; some reviewed |
| `org_relation_place` | a place tied to an organisation in a sentence ("played at the Central Theatre in Warsaw") | 16,677 | linked by our place pipeline; some reviewed |
| `person_relation_place` | a place tied to a relation between two people ("toured with X in Romania") | 17,495 | **linked only by exact spelling, not reviewed** (see `link_status`) |

| Column | Meaning |
|---|---|
| `place_mention_id` | Row ID (`PM000001`…). Stable within this snapshot only. |
| `entry_id`, `entry_heading` | The Lexicon entry, and its heading (the subject's name in Yiddish). |
| `event_or_relation` | `birth`/`death`/`burial` for vital places; otherwise the relation type, e.g. `Employment_Performance`, `Professional_Collaboration`. |
| `related_name`, `related_id` | The organisation or person the place is attached to: an org cluster ID (+ `db` ID in N), or a person mention ID in E. |
| `place_field` | Kind of place as extracted: `settlement` (city or town), `country`, `province`, `venue` (a building or theatre), `address`, `place` (for vital places: town or cemetery). |
| `place_as_written` | The place exactly as the model extracted it, in Yiddish. |
| `context` | The text around the mention. |
| `context_kind` | `extracted_sentence` = the sentence the language model gave for the relation. `entry_text_window` = for vital places, about 150 characters either side of the place in the full entry. |
| `place_in_context` | `yes` if the place's spelling appears word-for-word in `context`. Often it is **`no`**, and that isn't an error: the model often took the place from the wider entry. A sentence about "the Kovno drama studio", for instance, gets the place "Lithuania". Use `entry_id` to read the full entry (G). |
| `link_status` | `linked`; `needs_review` (linked but flagged); `misresolved` (known wrong); `linked_by_spelling` (person-relation places only: the spelling matched **exactly one** place elsewhere in C, not reviewed); `ambiguous` (the spelling matched several places; candidates are in `review_flags`); `unlinked`. |
| `link_method` | How the link was made, for example `kima_name_exact` (exact match against Kima's name variants), `qid_map`, `guberniya_governorate`, `audit_corrected`, `spine_spelling_lookup`. Empty = the original Wikidata reconciliation. |
| `wikidata_qid`, `name_en`, `place_type` | Wikidata ID, English label, Wikidata type. |
| `kima_id`, `kima_name_latin`, `kima_name_hebrew` | Kima ID and its Latin and Hebrew names. |
| `lat`, `lon` | Coordinates, where known. |
| `review_flags` | Quality warnings, e.g. `place_province_mismatch`. |
| `attestation_id` | The row in C the link came from. |

Many venues (theatre buildings, halls) are unlinked. They mostly have no Wikidata
entry and need their own work.

### B. `toponyms_gazetteer.csv`

One row per real place (949), keyed by Wikidata ID. The useful columns for
cross-script matching are `label_en`, `label_yi` (Wikidata's Yiddish label, without
vowel marks), `kima_rom`, `kima_heb`, and **`variants`**: every Yiddish spelling
attested in the corpus, separated by `;`. 552 places have a Kima ID. Other columns:
`n_attestations` (how often it occurs), `category` (settlement, country, province,
cemetery, …).

### C. `toponyms_attestations.csv`

The source of A's links. Each row is a place spelling from a person's
vital-records data or an organisation cluster, with its link. Note that its
`context` column holds the **event type** (birth/death/burial), not text, and that
organisation places appear once per cluster and spelling. Both are why we built A.
Columns worth knowing:

- `maaty_qid` and `maaty_qid_conflict`: a **second, independent** resolution of
  the vital places, done by a research assistant. The two disagree on about 545
  mentions, which makes them useful as an annotator-agreement set.
- `rejected_qid`: a link we considered and rejected.
- `suggested_qid`: a candidate for an unlinked place.

### D. `toponyms_unlinked.csv`

748 spellings we could not resolve, with occurrence counts and the rows in C where
they appear. `is_descriptor = True` marks phrases that aren't places, such as "at
the front" or "in a small town". These make useful hard negatives and hard cases.

### O. `org_addresses_review.tsv`

For each organisation (by its N `db_id`), the locations a research assistant
**confirmed by hand**: city, street address in Yiddish and in Latin script, and
coordinates (377 confirmed settlements, 185 with coordinates). Also kept: what was
extracted (`extracted_*`), the reviewer's notes, reviewer and date. These are
hand-entered fields, so information is sometimes in a neighbouring column.

### P. Known errors (useful for error analysis)

[`matching_corrections_log.tsv`](../zibn-shtern/data/working/kima/matching_corrections_log.tsv)
and
[`audit_name_exact_ambiguous.tsv`](../zibn-shtern/data/working/kima/audit_name_exact_ambiguous.tsv)
list wrong links we caught, each with its cause. The second file is the list of
spellings that match more than one Kima place:

- one spelling matched to several places (Winnipeg → Edmonton, Cleveland → Seattle,
  Tarnów → Kielce);
- context needed to choose between places (Williamsburg in Brooklyn, not Virginia);
- removing vowel marks made two names identical (Cuba = Quba, Troy = Tarai).

---

## 6. People and organisations in detail

### E. `mentions_all.tsv`: person mentions

One row per person mentioned in an entry. Key columns:

| Column | Meaning |
|---|---|
| `mention_id` | Unique ID of the mention. |
| `mention_name` | The name as written. **A name ending in `*`** is not a name but a stand-in for an unnamed relative, e.g. `פֿאָטער*` = "father*". |
| `relation_sentence` | The sentence the mention comes from (84% of rows). |
| `relation_category`, `relation_specific` | How the person relates to the entry's subject: `Family` (with e.g. `parent`, `sibling`), `Professional_Collaboration`, `Professional_Hierarchy` (hired by, directed by), `Creative_Intellectual`, `Social_Personal`, `Other`. |
| `gender`, `person_description` | As stated or inferred by the model; the description is in Yiddish, e.g. "a merchant". |
| `extraction_issue` | A flag on what kind of name it is: `single-word name` (usually a surname alone, the hardest case), `initials`, `Unspecific` (a relative with no name). |
| `rel_place_*`, `rel_date_start/end` | Where and when the relation took place. The places are in A as `person_relation_place`. |
| `host_heading_fill` | The heading of the entry the mention is in, on every row. |

> **Trap:** `host_xml_id`, `host_heading` and `host_entry_text` are filled in only
> on the **first** row of each entry, as in a spreadsheet where the value is left
> blank below. Do not group rows by `host_xml_id` without carrying the value
> down. **The easy way: use F (`entry_mentions_slim.tsv`)**, where the entry ID
> (`host_person_id`) is on every row, and get full text from G.

### Which person a mention refers to: H, I, K, L

- **H `person_hub.tsv`**: one row per real person: `canonical_heading`, birth and
  death dates, which entries and database rows belong to them, and `top_surfaces`
  (the most common ways they are named in the corpus). `evidence` says how the hub
  was formed: `ra_align` (a research assistant matched the entry to the database),
  `human_align` (the project lead did), `singleton` (an entry with no database
  match).
- **I `person_hub_members.tsv`**: the membership list. `member_kind` = `entry` (a
  Lexicon entry), `db` (a row in J; `detail` holds the English name) or `surface`
  (a spelling of the name checked against this person, e.g. the surname alone, or
  the surname with a grammatical ending).
- **K `mention_resolution_decisions.tsv`**: 1,338 cases where **a person decided**
  who a specific mention (`mention_id`) refers to. `decision_kind`: `hub` (it is
  person `resolved_hub_id`), `family` (an unnamed relative), `other`.
  `resolver_verdict` is what our automatic resolver had said beforehand; mostly
  `AMBIGUOUS`, which is why a person was asked. Use this as **hand-checked gold
  data for hard cases**.
- **L `derived_mention_alignments.tsv`**: a name spelling (`mention_surface`) tied
  to the entry of the person it refers to (`matched_person_ids`), derived from
  research-assistant review sheets. `source_sheet` = `full` (full name),
  `surnames`, `initials`. Applies to the spelling as a whole, not to individual
  mentions.

### J. `people_db.tsv`: cross-script names for people

The research group's existing database of Yiddish theatre people. For
multilingual work, the important columns are `hebname` (name in Hebrew script) and
**`english`** (romanised name), plus `name_variants`, birth and death dates, and
`external_source` / `external_source_id`. These hold a Wikidata ID for 94 people,
a National Library of Israel authority ID for 60, and for most people a reference
to the Lexicon itself. `probably_not_zylbercweig = 1` marks people who probably
have no Lexicon entry.

### M. `organizations_clustered.tsv`: organisation mentions

One row per organisation mention. The column names are long (they reflect the
nested structure the model returned). The important ones are:

| Column | Meaning |
|---|---|
| `_ - organizations - _ - title` | The organisation's name as written. |
| `_ - organizations - _ - relations - _ - original_sentence` | The sentence it comes from (99%). |
| `_ - organizations - _ - org_type` | The model's guess at the type: Theatre, Traveling Company, Education, … |
| `_ - organizations - _ - relations - _ - category` / `specific_relation` / `role_title` | How the entry's subject relates to it: `Employment_Performance`, `Leadership_Ownership`, `Education_Formation`, `Production_Distribution`, `Affiliation_Membership`; e.g. *member*, *director*, *student*. |
| `_ - organizations - _ - locations - _ - *` | Where. These places are in A as `org_relation_place`. |
| `_ - heading`, `_ - xml:id`, `File`, `_ - entry` | The host entry (`_ - entry` is its text, cut short). |
| `cluster_id`, `canonical_yiddish`, `cluster_size` | Which group of same-organisation mentions this row belongs to. |
| `auto_or_review` | How the cluster was formed: `auto` (by a script), `singleton`, `qid_exploded` (one generic name, e.g. "the conservatory", split into one cluster per city), `manual_split_…` (split by a person). |

### N. `core_db.tsv`: organisations

3,632 real organisations. Columns: `name` (**English**), `name_yiddish`,
`org_type`, `address`, `linked_cluster_ids` (links to M), `parent_db_id`. Rows
with `deprecated` or `merged_into` filled are retired; follow `merged_into`.
Ignore `name_yiddish_translit`: it is an automatic Yiddish-script rendering of
the English name that we use only to find candidate matches, not a real attested
name.

### How much is checked by a person

| Data | Status |
|---|---|
| Extraction (all mentions, sentences, relation types) | automatic (language model); not checked row by row |
| Place links in A for `vital_place` / `org_relation_place` | automatic, with targeted audits and some review; flags in `review_flags` |
| Place links in A for `person_relation_place` | automatic exact-spelling match; **not checked** |
| O confirmed organisation locations | checked by a person |
| H/I person hubs (`ra_align`, `human_align`) | checked by a person (an entry matched to a database row) |
| K mention decisions | checked by a person, mention by mention |
| M organisation clusters | mixed; see `auto_or_review` |

---

## 7. Getting the files

You are viewing a **fixed snapshot** of the repository. The working branch keeps
changing; this snapshot won't.

- **Single files:** open a link above and use GitHub's *Download raw file* button.
  The larger files (E, G, M are 28–46 MB; A is 23 MB) are too big for GitHub to
  display, but the download works.
- **Everything, without the rest of the repository** (which is large):

  ```bash
  git clone --depth 1 --filter=blob:none --sparse \
      --branch datasets-2026-10-30 https://github.com/sinairusinek/Dybbuk.git
  cd Dybbuk
  git sparse-checkout set --no-cone \
      /Zylbercweig/datasets \
      /Zylbercweig/zibn-shtern/data/working/toponyms_gazetteer.csv \
      /Zylbercweig/zibn-shtern/data/working/toponyms_attestations.csv \
      /Zylbercweig/zibn-shtern/data/working/toponyms_unlinked.csv \
      /Zylbercweig/zibn-shtern/data/working/kima/matching_corrections_log.tsv \
      /Zylbercweig/zibn-shtern/data/working/kima/audit_name_exact_ambiguous.tsv \
      /Zylbercweig/people/mentions_all.tsv \
      /Zylbercweig/people/entry_mentions_slim.tsv \
      /Zylbercweig/people/entry_texts.tsv \
      /Zylbercweig/people/person_hub.tsv \
      /Zylbercweig/people/person_hub_members.tsv \
      /Zylbercweig/people/people_db.tsv \
      /Zylbercweig/people/mention_resolution_decisions.tsv \
      /Zylbercweig/people/derived_mention_alignments.tsv \
      /Zylbercweig/organizations/organizations_clustered.tsv \
      /Zylbercweig/organizations/core_db.tsv \
      /Zylbercweig/organizations/org_addresses_review.tsv
  ```

The other folders in the repository are working material: scripts, review queues,
backups and the review app. You don't need them, and they change without notice.

---

## 8. Using and citing

The entry texts and sentences come from the printed Lexicon. Before you
redistribute the full texts (G, or the `host_entry_text` / `_ - entry` columns),
ask the project. Using the extracted mentions, IDs and links for research is what
this release is for. Please cite the *Leksikon fun yidishn teater* (Zalmen
Zylbercweig) as the source, and this repository at the snapshot tag
`datasets-2026-10-30`.

Questions: Sinai Rusinek.
