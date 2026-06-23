# Arne — Source Guide for German-Archive City Audits

Goal: enrich Zylbercweig org clusters with QIDs, historic + current addresses,
duplicate/typology flags. Same workflow that worked for Vienna+Berlin.

## What you get

Four TSVs per batch. **Do not rename or reorder columns.** Add your edits in
place; leave blank what you can't resolve.

| file | purpose |
| --- | --- |
| `<batch>_audit_for_RA.tsv` | main worklist — one row per org cluster/DB row |
| `<batch>_dup_merge_template.tsv` | duplicate-merge instructions (v2 schema, see `dup_schema.md`) |
| `<batch>_typology_review_template.tsv` | wrong-org-type flags |
| `<batch>_questions_template.tsv` | anything you want to ask before deciding |

The first non-header row in each TSV is a worked example beginning with
`# EXAMPLE` — **delete it before returning.**

## What to fill in the main worklist

- **name_latin** — canonical Latin/German/Polish name as the entity called itself
  in its period. Use **Lemberg** not Lviv for pre-1918 entities; **Pressburg** not
  Bratislava pre-1919; etc. Modern name goes in `current_address` only.
- **QID** — Wikidata Q-id. Required if you flag `duplicate=yes`. Verify it's
  the *same entity*, not just same building/same name.
- **historic_address** — street + city as known in the entity's active period.
- **current_address** — modern street + postcode if you can find it.
- **duplicate** — `yes` if this cluster is the same entity as another cluster
  in this batch. Then **also fill a row in `dup_merge_template.tsv`** (see
  `dup_schema.md` — that file is what actually executes the merge).
- **miscls_theatre** — `yes` if `org_type=Theatre` is wrong (e.g. it's a
  scenery company, a university institute, an actor's name). Then fill
  `typology_review_template.tsv`.
- **comments** — free text. Use `?`, `Maybe`, `Probably`, or `Perhaps` if you
  want it routed to the questions file.

You do **not** need to touch any Yiddish-script field. Those carry through
automatically.

## Per-city archive map

Use sources roughly in the listed order. Always cross-check at least two before
asserting a QID.

### Czernowitz (Cernăuți / Chernivtsi)
1. **ANNO** (Austrian newspapers): https://anno.onb.ac.at — period-correct names
2. **Wikipedia-DE / Wikidata** — many Bukovina theatres + cultural orgs have entries
3. **Corbea-Hoișie, *Jüdisches Czernowitz*** — German-language scholarship
4. **YIVO Encyclopedia** (English): https://yivoencyclopedia.org — Bukovina entries

### Prag (Praha)
1. **Kramerius** (Czech National Library): https://kramerius5.nkp.cz
2. **Wikipedia-DE / Wikipedia-CS** + Wikidata
3. **Židovské muzeum v Praze** (Jewish Museum Prague) — institutional registers
4. **LBI Online Archives** (Leo Baeck Institute) — German-Jewish institutions

### Pressburg / Bratislava
1. **ANNO** + Slovak digital libraries (DigitalLibrary.sk)
2. **Wikipedia-DE / Wikipedia-SK** + Wikidata
3. **Múzeum židovskej kultúry** (Bratislava) — institutional context

### Brünn / Brno
1. **Kramerius** + Moravian Land Library digital collections
2. **Wikipedia-DE / Wikipedia-CS** + Wikidata

### Munich (München)
1. **Wikipedia-DE** + Wikidata + **GND** (DNB authority file)
2. **Stadtarchiv München** online catalog
3. **LBI** — Bavarian Jewish institutions
4. **Compact Memory** (jüdische Periodika): https://sammlungen.ub.uni-frankfurt.de/cm

### Hamburg
1. **Wikipedia-DE / Wikidata / GND**
2. **Staatsarchiv Hamburg** online finding aids
3. **LBI** + **Institut für die Geschichte der deutschen Juden**

### Frankfurt am Main
1. **Wikipedia-DE / Wikidata / GND**
2. **Institut für Stadtgeschichte Frankfurt** online catalog
3. **Compact Memory** (hosted at Frankfurt UB)
4. **LBI**

### Breslau (Wrocław)
1. **Wikipedia-DE / Wikipedia-PL / Wikidata**
2. **Polona** (Polish National Library): https://polona.pl
3. **LBI** + Silesian regional archives

### Königsberg (Kaliningrad)
1. **Wikipedia-DE** + Wikidata
2. **Geheimes Staatsarchiv Preußischer Kulturbesitz** (online finding aids)
3. **LBI** for Jewish institutions

### Danzig (Gdańsk)
1. **Wikipedia-DE / Wikipedia-PL / Wikidata**
2. **Polona** + Danzig-specific digital collections
3. **LBI** + Danzig research network

### Leipzig / Dresden (if in batch)
1. **Wikipedia-DE / Wikidata / GND**
2. **SLUB Dresden** / **UB Leipzig** digital collections
3. **Compact Memory**

## Always-on sources (any city)

- **Wikidata** — primary source of QIDs; use the search box, filter by city
- **GND** (Deutsche Nationalbibliothek): https://d-nb.info/gnd — German authority IDs
- **Compact Memory** — full-text of jüdische Periodika 18–20c
- **LBI Online Archives** — German-Jewish institutional records
- **YIVO finding aids** (English) — Eastern European Jewish institutions

## Canonical-name rule

The period name wins. Vienna 1910 = "Wien", Lemberg 1880 = "Lemberg",
Pressburg 1900 = "Pressburg", Königsberg 1930 = "Königsberg". Put the modern
city in `current_address` only.

## QID red flags — when *not* to assign one

- The Wikidata entry is for a *building* but the cluster is a *troupe* that
  performed there (or vice versa). Leave QID blank, note in comments.
- Same name, different entity (e.g. two different "Volksbühne" troupes in
  different decades). Leave QID blank.
- Modern reincarnation of a closed institution — only link if Wikidata
  treats them as the same entity.
- If in doubt → leave blank, write a `?` comment.

## Returning the batch

- Save all four TSVs as **UTF-8** (not Latin-1). In Excel: "Save As" → choose
  "CSV UTF-8" if exporting; or keep as TSV and verify a Yiddish/German
  diacritic survives a round-trip.
- Email the four files back. Sinai handles ingest.
