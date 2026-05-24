# YiDraCor

Pipeline for preparing Yiddish theatrical texts for [DraCor](https://dracor.org/).
Two independent strands of work live here:

1. **TEI → DraCor transform** (`code/dracor_transform.py`, `code/preprocessing.py`,
   `code/util/`) — the original Lateiner_Meshumed flow: turns a `|`-delimited
   intermediate into `<sp>`-wrapped TEI. Superseded for annotated plays by the
   structurer below; kept for that one play.
1b. **Structurer** (`code/structure/build_tei.py`) — the final stage for plays
   that went through the annotation pipeline. Assembles
   `data/<play>/page_annotated/*.xml` (with their `custom` spans) +
   `cast_dict.json` + `editions.json` into one tei_all TEI for the TEI-Publisher
   app: body acts as `<sp>`/`<stage>`/`<div>`, song supplement in `<back>` with
   `@corresp`, castList+listPerson from the cast dict. Run:
   `cd code && python3.11 -m structure.build_tei --play <folder>`.
   Open work + the DraCor-strict variant are tracked in `docs/TODO-tei-pipeline.md`.
2. **Yiddish vocalization** (`code/vocalization/`) — adds nikkud (Hebrew vowel
   points) to bare Yiddish text by learning the orthographic conventions of a
   single hand-vocalized reference page and propagating them across the rest
   of a play. Also flags suspected OCR errors as Transkribus `<unclear>`
   annotations for RA review.

## Repo layout

```
code/
  preprocessing.py          stage 1: raw Transkribus TEI → cleaned intermediate
  dracor_transform.py       stage 2: intermediate → DraCor-shaped TEI
  util/                     small XML utilities used by the above
  tei-publisher/            eXist-db / TEI-Publisher app for browsing
  vocalization/             see code/vocalization/README-design.md
data/
  Lateiner_Meshumed/        first play (pre-vocalization era)
  Yudale_der_blinder,_Emkroyt1908/
    page/                   Transkribus PAGE-XML, line-level corrected by RA
    page_final/             pipeline output, ready to upload back to Transkribus
    11.5-...                RA's authoritative vocalized export (read-only ref)
```

## Vocalization pipeline (TL;DR)

Single command, one page at a time:

```
cd code/vocalization
python pipeline.py --page 7                       # full pipeline
python pipeline.py --page 8 --ref 6               # use page 6 as reference
python pipeline.py --page 8 --no-llm              # rules+dict only, no API
python pipeline.py --page 8 --no-flags            # skip phonotactic check
python pipeline.py --page 8 --phono both          # dual-model calibration
```

Stages:

1. **rules + page-N dictionary** (`vocalize_from_reference.py`)
   — applies the empirically-mined rule inventory, then looks up
   consonantal forms against a reference page that's already vocalized.
2. **Claude fill** (`claude_vocalize.py`) — vocalizes tokens neither rules
   nor dictionary covered. Round-trip checks reject any consonant change.
3. **Phonotactic check** (`phonotactic_check.py`) — Gemini 3 Pro by default
   identifies tokens whose consonants look implausible (OCR errors the RA
   may have missed). `--phono both` uses Claude Sonnet 4.6 in parallel.
4. **Unclear-tag insertion** (`unclear_tags.py`) — converts the flagged
   tokens into Transkribus `unclear {offset:O; length:L;}` annotations on
   the appropriate `<TextLine custom="…">`.

Output: `data/{project}/page_final/{filename}.xml`, ready to drop into
Transkribus.

## Environment

```
pip install lxml acdh-tei-pyutils anthropic google-genai
export ANTHROPIC_API_KEY=...
export GOOGLE_API_KEY=...        # or GEMINI_API_KEY
```

## Status

The vocalization pipeline has been validated on pages 5, 7, 8, 9 of
*Yudale der Blinder* (1908). Outputs were compared against the RA's
hand-corrected reference: most lines are nikkud-only differences; the
two genuine OCR errors the RA fixed (`חען→חזן`, `פרענט→פרעגט`) were
correctly surfaced by the phonotactic check.

See `code/vocalization/README-design.md` for the design log and the
non-obvious decisions behind the pipeline.
