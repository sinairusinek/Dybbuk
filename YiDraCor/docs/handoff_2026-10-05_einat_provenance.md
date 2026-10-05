# Where do these eight editions live, and what is Meshumed's imprint?

**For Einat. 2026-10-05.**

We have just finished linking every person, venue, publisher, library and place
named across the 27 Lateiner and Hurwitz editions to our databases — all of
them now resolve to a database id. What is left open is not identification but
**provenance**: for eight editions we cannot say which institution holds the
copy we transcribed, and for one we cannot say who printed it.

These are catalogue questions, not editorial ones. Each is self-contained; you
should not need any other document.

---

## Part 1 — Six manuscripts with no holding institution recorded

### What we already have

Of our twelve manuscript-track editions, **six** record a YIVO shelfmark — but
only in the free-text notes field, not in a catalogue column. We have just
linked those to YIVO (now org 27 in our database, after merging a duplicate
record). They read:

| play | shelfmark |
| :-- | :-- |
| Yoysef in Egipten | YIVO rg8-1-f4179 |
| Khurbn Yerusholaim | YIVO rg8-1-f4176 |
| Bas Koyen | YIVO RG8-f4140 |
| Di Tsvey Tnoim | YIVO RG8-f4141 |
| Shimshon Hagibor | YIVO RG8-f4142A |

(The sixth is the duplicate record discussed in Part 3, which repeats
rg8-f4176.)

All are in **Record Group 8**, and the folio numbers cluster tightly —
f4140, f4141, f4142A, f4176, f4179 — which suggests the remaining six may sit
in the same run.

### The six with nothing recorded

For these we have the Transkribus transcription but no shelfmark, no record
group, and no holding institution in the catalogue:

| # | play | author | pp. | Transkribus |
| :-- | :-- | :-- | :-- | :-- |
| 1 | **Yaakov-Esav** (יעקב ועשו) | Hurwitz *(attributed)* | 60 | [doc 494907](https://app.transkribus.org/collection/2372172/doc/494907) |
| 2 | **Yetsi'as Mitsrayim** (יציאת מצרים) | Hurwitz *(certain)* | 67 | [doc 715163](https://app.transkribus.org/collection/2372172/doc/715163) |
| 3 | **Ben HaDor** (בן הדור) | Hurwitz *(attributed)* | 36 | [doc 826910](https://app.transkribus.org/collection/2372172/doc/826910) |
| 4 | **Tissa-Essler** (טיססא עסלער) | Hurwitz *(certain, 1892)* | 40 | [doc 905289](https://app.transkribus.org/collection/2372172/doc/905289) |
| 5 | **Emigration nach America** (עמיגראציאן נאך אמעריקע) | Lateiner *(certain, 1884)* | 117 | [doc 1013131](https://app.transkribus.org/collection/2372172/doc/1013131) |
| 6 | **Meshumed** (דער משומד) | Lateiner *(certain)* | 44 | [doc 534187](https://app.transkribus.org/collection/2372172/doc/534187) |

### Question 1

**For each of the six: which institution holds it, and under what shelfmark?**

If they are YIVO RG8 like the others, the folio number is what we need. If any
came from somewhere else — a different YIVO record group, another archive, a
private hand — that matters more, because we currently have no record of it at
all.

### Question 2 — a narrower version, if the above is a long job

Two of the six have a detail that may make them easier to place:

- **Yaakov-Esav** — the manuscript itself carries an archival catalogue label.
  If that label identifies the repository, reading it off the first pages would
  answer the question without any catalogue lookup.
- **Yetsi'as Mitsrayim** — the text is in **German in Latin script**, not
  Yiddish, its title page is in German and Russian (*"Von Profesor M.
  Horowitz, Musik von Perlmutter und Wohl"*), and it carries **Russian
  censorship stamps banning performance, St Petersburg, 29 October 1910**.
  A censored German-language Hurwitz manuscript almost certainly reached its
  repository by a different route than the Yiddish ones, so its provenance is
  worth knowing on its own account — and the censor's stamp may itself name
  the archive or collection it passed through.

**Is either of those quicker to answer than the full six?**

---

## Part 2 — Meshumed: who printed it, and when?

### The situation

**Meshumed** (דער משומד), Lateiner, is the only edition in the corpus where we
have a **printed** copy recorded but no imprint whatsoever:

| field | value |
| :-- | :-- |
| publisher | *(empty)* |
| publication place | *(empty)* |
| year printed | *(empty)* |
| holding library | *(empty)* |
| expression id | 3879 |

Authorship is not in doubt — the title page (p.3) reads
**`יוזף לאטייניר`** — and the play is well attested: we have 11 song/score
rows for it and a production record. It is the *imprint* that is blank.

Everything else in our print run has a full imprint, which is why this one
stands out:

| play | publisher | place | year | library |
| :-- | :-- | :-- | :-- | :-- |
| Mishke Mashke | Farlag "Kultur" (Dzika 13) | Warsaw | 1911 | Polona, II 65.675 |
| Ezra | Amkroyt un Fraynd | Przemyśl | 1908 | Polona, II 63.171 |
| Blimele | A. Faust | Podgórze | 1903 | Polona, II 63.206 |
| Al Naharot Bavel | Amkraut & Freund | Przemyśl | 1909 | Polona, II 63.436 |

### Question 3

**Is there a printed edition of Meshumed, and if so — publisher, place, year,
and which library holds it?**

Specifically: should this be a Polona record like the other fourteen, in which
case the signature would complete it — or is Meshumed **manuscript-only**, with
no print edition at all?

This matters because our catalogue currently lists Meshumed in **two rows**,
both pointing at the same Transkribus document (534187) and the same expression
(3879): one marked `partial` and one marked `manuscript_track` (44 pp, 39
transcribed). Both are therefore describing a manuscript.

So the question resolves to: **is there a printed Meshumed at all?** If not,
the duplicate row should go and the empty imprint fields are correct rather
than missing. If there is one, we need its imprint and a Polona signature —
and the print copy is a separate witness from the manuscript.

---

## Part 3 — One duplicate to confirm (quick)

`HurbanYerushalaim_820938_duplicate` ([doc 820938](https://app.transkribus.org/collection/2372172/doc/820938),
56 pp) is flagged in our catalogue as a duplicate of
**Khurbn Yerusholaim** ([doc 838368](https://app.transkribus.org/collection/2372172/doc/838368),
YIVO rg8-f4176, 1916), which is fully transcribed.

The duplicate has only one page transcribed, by an uploader we cannot identify
(`uninecessity`), and the catalogue notes say the decision is pending.

### Question 4

**Can doc 820938 be retired as a duplicate of 838368?**

If the two are genuinely the same manuscript, retiring it would remove the last
spurious record from the corpus. If they are *different* copies of the same
play — two witnesses rather than one — that is a more interesting finding and
both should stay, with distinct shelfmarks.

---

## Why these eight, and not others

For completeness, the remaining gaps in the corpus are not things you can
answer, and we are not asking about them:

- **The Hurwitz editions' performance events** were missing from our dataset
  because the PerformanceEvents report covers Lateiner only (129 of its 131
  titles). They are now derived from the catalogue's `Hurwitz hafakot` sheet
  instead — 40 events across the seven plays, each keeping its newspaper
  citation. Nothing needed from you.
- **The manuscripts have no publisher or print year** because they are
  manuscripts. That is correct, not missing.

---

## What happens to your answers

Shelfmarks go into `editions.csv` (`library`, `library_signature`), which
regenerates the holding-library links in the entity graph — each becomes a
`holds` edge carrying the shelfmark, so the corpus can state where every
witness physically lives. The Meshumed imprint, if there is one, would link
to a publisher org the way the other fourteen do.

Current state for reference: every person, venue, publisher, library and place
in the corpus is linked to a database id. These eight are the only open items
that need a human with the catalogue.
