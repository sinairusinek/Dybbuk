# Handoff → Kimatch repo: skill & engine improvements

Source: two Dybbuk/Zylbercweig toponym-matching sessions (2026-05-22) that ran the
`kimatch` skill at scale (~18k attestations, ~4.3k unlinked Yiddish spellings) against
the Kima gazetteer + Wikidata. This documents the lessons that belong **in the Kimatch
repo** (engine + skill), split into general (A) and per-language/script (B).

Carry this to a session in `~/Documents/GitHub/Kimatch`. Each item notes the file to touch.

---

## A. General engine / skill improvements

### A1. `name_exact` is NOT safe to auto-confirm — it produces false positives 3 ways
`match_place` (kimatch/core/matcher.py) returns NAME_EXACT at conf 0.95 and the skill
(SKILL.md) calls it "usually auto-confirmable, spot-check." At scale that's wrong. Three
failure modes we hit, all silent:

1. **Ambiguity** — a spelling matches a *variant on >1 distinct Kima place* and the matcher
   picks one (via first hit / `_pick_best_by_coords`). Real errors: נואַרק matched variants
   on BOTH New York and Newark → picked NY; טאַרנאָוו picked Kielce over Tarnów; דראָהאָביטש
   picked Dorogobuzh (Russia) over Drohobych (Ukraine).
   **Fix:** when `db.search_by_name(name)` returns >1 distinct place, do NOT early-return a
   single NAME_EXACT. Emit a new status `NAME_AMBIGUOUS` carrying all candidates, so the
   skill routes it to disambiguation instead of auto-confirm. (`match_place`, steps 2 & 1b.)
   A cheap detector to add as a `kimatch audit` subcommand: flag every confirmed link whose
   spelling still resolves to >1 place.

2. **Homographs** — a common word equals an obscure place variant. שול ("synagogue") matched
   *Šiauliai Ghetto*; קהלה ("community") matched *Kahla, Germany*; טראָי vs "Tarai".
   **Fix:** an optional per-job stop-list / common-word guard, and (see B) vocalization-aware
   matching that keeps שול ≠ Šiauliai.

3. **Geographic implausibility** — a Yiddish/European-corpus spelling matched far-flung places
   (לונאַ→Lonavala India; סעלץ→Frederick). **Fix:** an optional per-job *region prior*
   (bounding box / country allow-list); flag matches outside it as low-confidence.

### A2. Add a transliteration-mismatch check (catches what a Wikidata cross-check cannot)
We tried validating links by re-resolving each spelling through Wikidata's yi-label search.
It catches some errors but **misses the wrong-city class** (Winnipeg→Edmonton) because Wikidata
also lacks Yiddish labels — those sit in a "no WD place" bucket looking benign.
**The reliable signal:** compare the *input spelling's phonetic proxy* to the *matched Kima
place's romanized-name proxy*; a large mismatch = wrong pick. (Winnipeg-spelling vs
"Edmonton"-rom = mismatch.) Add as a confidence penalty in `match_place` and/or a
`kimatch audit` report. Both proxies already exist via `phonetics.py` + DM-input.

### A3. Wikidata-lookup hygiene (for `lookup --live` and any reconciliation the skill suggests)
- **User-Agent**: the default `Python-urllib` UA is **blocked by Wikimedia** (returns 0 results
  silently). Always send a descriptive UA. Bake this into any API helper the skill ships.
- **Type-verify every QID via P31** (`wbgetentities`) before trusting it. Top `wbsearchentities`
  hits are often the wrong type — we saw אַמעריקאַ→*US Dollar* (Q4917), ליטוויש→*Lithuanian
  language* (Q9083). Maintain a place allow-list / hard reject-list (currency/language/person/
  taxon/film/album/sports).
- **Never assert a QID from memory** — verify it. In these sessions multiple "known" QIDs were
  wrong (Q202032=politician not Volhynia; Q193563=BnF not Russian-Poland; Q1338261=footballer;
  Q2256=Birmingham not Winnipeg).

### A4. Ship the confidence-grading + graded-output pattern
We found A/B/C grading invaluable: **A_autolink** (exact label + type-verified + unambiguous),
**B_review** (typed candidate, loose/ambiguous), **C_review** (weak/none). Consider making
`kimatch match` emit this grading and a `--autolink-threshold`, plus per-grade output files.
Reference implementations to port: `scripts/resolve_residual_wikidata.py` and
`scripts/build_residual_punchlists.py` in the Dybbuk repo.

### A5. Corpus-prior ("internal index") resolution as a first-class signal
Resolving a spelling against QIDs *already validated elsewhere in the same corpus* (normalized
match) was safer than API guessing and collapsed spelling variants for free (e.g. resolved 76%
of the country head). Consider a `--prior-resolutions <file>` option feeding `find_candidates`
a high-precision signal ranked above fuzzy/phonetic. CAVEAT we learned: "QID exists in the
corpus" does NOT prove *this spelling* means it (כאָר→Kharkiv, וואַרשאָו→Warsaw Ghetto) — only
use exact-normalized spelling identity, never mere QID co-occurrence.

### A6. Fuzzy is review-evidence only
Document explicitly in SKILL.md: never auto-link `fuzzy` status; surface candidates for human
disambiguation. (We hit fuzzy + even name_exact false positives like טראָי→Tarai.)

---

## B. Per-language / per-script improvements + orchestration

### B1. ROOT CAUSE: Hebrew/Yiddish vocalization is discarded, never expanded
Empirically (kimatch/core/normalizers.py `normalize_name` + the dybbuk yiddish→IPA bridge):
- `normalize_name` does NFD + strips ALL combining marks → טראָי (Troy, *kamatz-aleph*) and
  טראי both become `טראי`. The vowel that disambiguates is deleted.
- The yiddish→IPA bridge **also ignores vocalization**: טראָי and טראי BOTH produce IPA `trʔj`
  — the kamatz-aleph (אָ) is rendered as a glottal stop, NOT as /o/. So "Troy" collapses to
  `tr_j` and collides with Tarai/Trai. קובאַ/קובא both → `kubʔ` (Cuba ≡ Azerbaijani Quba).

We are neither **expanding** matres-lectionis vowels (for defective spellings) NOR **honoring**
the vocalization that is present. Both directions matter.

**Fix (Yiddish/Hebrew strategy only):** a vocalization-aware transliterator:
- read pointing: *kamatz/holam-aleph* אָ → /o/, *pasekh-aleph* אַ → /a/, etc.
- Yiddish digraph orthography: וו→/v/, יי→/ey/, וי→/oy/, ע→/e/, final ה, etc.
- treat aleph as a **vowel carrier** (mater lectionis), not a default glottal stop.
- for *unvocalized* Hebrew input, optionally **expand** plausible vowels (the "invisible vowel"
  case) to generate candidate readings rather than one defective skeleton.

### B2. Differentiate Yiddish-style vs Hebrew-style spellings
A Hebrew-script string can be Yiddish (uses אַ/אָ/ע/וו/יי matres systematically) or Hebrew
(more defective). Detecting which lets the matcher apply the right transliteration and avoids
cross-language homograph collisions (the Troy/Tarai problem is partly a Yiddish-read vs
Hebrew-read problem). Add a `detect_orthography(hebrew_string) -> {yiddish|hebrew|ambiguous}`
heuristic feeding `strategy_for`.

### B3. Orchestration — keep language steps OUT of the general pipeline
`strategy_for(script, language)` (phonetics.py) already gates phonetic engines by script
(Latin → DM+Beider-Morse; hebrew/yiddish → bridge; arabic/cyrillic → graceful stub). **Extend
the same pattern**, do not bolt Yiddish logic onto the shared path:
- the new vocalization-aware transliterator + orthography detector live ONLY in the
  hebrew/yiddish strategy; the Latin/European path stays identity-normalize + DM/Beider-Morse.
- expose them through the strategy object (like `proxy`/`phonetic_engines`) so a non-Hebrew job
  never loads or runs them.
- keep `kimatch doctor` reporting per-script readiness (it already does) so missing Yiddish
  tooling degrades gracefully instead of silently mismatching.
- the expensive Beider-Morse step is already "candidates only, borderline only" — apply the same
  cost discipline to any expanded-vowel candidate generation (cap fan-out).

### B4. Per-script test fixtures
Add regression cases from these sessions so fixes are pinned: טראָי→Troy (≠Tarai),
קובאַ→Cuba (≠Quba), שול→(not a place), נואַרק→Newark (≠NY), ארץ-ישראל→Land of Israel region.

---

## Quick repro / evidence (run in the Kimatch venv)
```python
from kimatch.core import tools
from kimatch.core.normalizers import normalize_name
for w in ["טראָי","טראי","קובאַ","קובא"]:
    print(w, normalize_name(w), tools.yiddish_to_ipa(w), tools.ipa_to_dm_input(tools.yiddish_to_ipa(w)))
# טראָי and טראי both -> 'טראי' / 'trʔj' / 'trj'  ← the bug
```
