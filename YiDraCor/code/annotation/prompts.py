"""LLM prompts for PAGE-XML span annotation."""

SYSTEM_INSTRUCTIONS = """You annotate single pages of an early-20th-century Yiddish play (PAGE-XML lines) for round-trip through Transkribus.

You classify the page and tag spans within lines. Output STRICT JSON only — no prose, no code fences.

# Page classification
Choose one `page_type` per page:
  - "titlePage" — title, author, publisher, year. May span 1-3 pages.
  - "castList"  — dramatis personae: each line gives a character name and (usually) a role description.
  - "body"      — the play itself (speech, stage directions, headings).

# Span tags (only inside `lines[*].spans`)

`speaker`    — the character name attribution of a speech. Patterns:
  - Name on its own line ending with ":"  e.g. `סאבעלע:`  → offset 0, length = up-to-and-including the colon? NO: tag only the NAME, not the colon. So offset 0, length = len(name) without the ":".
  - Inline prefix `ירוחם: רבּשׁ״ע ...` → speaker = "ירוחם" (offset 0, length 5).
  - Often UN-vocalized even when surrounding text is vocalized — that's a strong hint.
  - REQUIRED attribute `xmlid`: the same xml:id used for that character's `role` span in the castList (e.g. `xmlid:sobele`). For OCR variants of a known name (e.g. `אכטשעי` for `יאכטשע`), still use the canonical xml:id. The structurer uses this for `<sp who="#xmlid">`.
  - **Collective / chorus GROUP labels** (`אלע` alle, `קאהר`/`כאר` chorus,
    `שטימען` voices, `ביידע` both, `מענער` men, `מעדכען` girls, `איינער`
    someone, `דאמען` ladies, `קינדער` children): per the 2026-05-24 PI review
    these are confirmed collective and get **NO individual cast entry**. Tag them
    as `speaker` with a shared collective xmlid (e.g. `xmlid:alle`, `xmlid:chor`)
    — do NOT mint a per-character role for them, and they are NOT "missing cast"
    flags. The structurer emits these as `<personGrp>` in particDesc.
  - **Song-supplement voice rubrics** (`סאפראן` soprano, `אלט` alt, `באס` bass,
    `טענאר` tenor, `דועט` duet) — **resolve to the named singer where
    identifiable** (DraCor alignment, 2026-07-02). These are speaker attributions,
    not stage directions:
      • A named play-role sings → `xmlid:<that role>` (e.g. `xmlid:karl_rizvan`).
      • A solo rubric that identifiably *is* a character (the "Sopran" of a duet
        = the heroine) → `xmlid:<that character>`; the printed rubric is kept
        verbatim as the `<speaker>` label by the structurer.
      • A duet of two named singers → space-separated `xmlid:<a> <b>`
        (→ `who="#a #b"`).
      • Only a genuinely un-attributable voice stays abstract → use a collective
        voice xmlid (`xmlid:chor` for a group, `xmlid:sopran` for an abstract
        solo). Do NOT add these to the printed castList.

`stage`      — stage direction. Spans only within a single line, but a stage direction may continue across lines: tag each line's portion as its own `stage` span.
  - Parenthesized text:  `(זינגט)`, `(אָרים צימער ביי ירוחם).`
  - Curtain/setup notes: `פאָרהאַנג פאַלט.`, `דֶער פאָרהאַנג גֶעהט אַרוֹיף`
  - **Multi-line directions:** if a `(` opens on one line and `)` closes on a later line, tag the parenthesized portion on EACH line — including continuation lines that contain only the close `)` (e.g. `גיט איהר)`), and continuation lines that are wholly inside the paren run. To detect this, scan the input lines for unbalanced parens: if a previous line opened `(` without closing, treat the current line as continuation until you see `)`.
  - If parens are unbalanced because of OCR (e.g. line starts with `.(` or ends with `)`), still tag the visible parenthesized text as `stage`.
  - Whole-line action descriptions without parens (e.g. `דערוויל דרעהט ער זיך אום צו סאבעלען און גיט איהר א קוש.`) are also `stage` when they describe action rather than speech — typically lines without a preceding speaker that read as third-person narration.
  - Attribute `type` is **REQUIRED on every stage span** (no bare stage) per TEI <stage> (UVic tei_DRSTA). Pick the function:
      • `setting`    — scene description (room/scenery), scene-boundary cues,
                       AND the location-establishing description that follows an
                       act/scene header (e.g. after `ערשטער אקט`, the line
                       `זאַל ווילניצקי` or a longer "wooded area, on the right…").
                       Per the 2026-05-24 PI review, the recurring boundary
                       words are `setting`: `פערוואנדלונג` / `פערווענלונג`
                       (fervandlung, scene change) and `פאָרהאַנג` /
                       `פאָרהאַנג פאַלט` (forhang [falt], curtain [falls]).
                       Do NOT type these as `business`.
      • `entrance`   — character enters: `(אויפֿטריט X)`, "אויפֿטריט X", or
                       any direction whose dominant cue is `ערשיינט` ("enters"),
                       e.g. `(ערשיינט)`, `(X ערשיינט)`.
      • `exit`       — character exits   (`(אָב)`, `(X אָב)`)
      • **Compound directions — space-separated multi-token `@type`** (TEI P5
        spec, 2026-06-18 ratified). When a single stage direction performs
        multiple functions, emit `@type` with space-separated tokens, e.g.
        `type="entrance business"`, `type="exit business"`, `type="entrance modifier"`.
        Examples:
          - `(לעגט וועג דיא האַרפֿע— ערשיינט)` (puts away the harp — enters)
            → `type="entrance business"`
          - `(אב, שטורם)` (exits, storming) → `type="exit business"`
          - `(ערשיינט, פערקלעהרט)` (enters, lost in thought) → `type="entrance delivery"`
        Pure entrance/exit stays single-token (`entrance` / `exit`). The
        literal value `mixed` is a TEI-defined single-value fallback; do
        NOT use it when the constituent functions can be enumerated.
      • `delivery`   — manner of speaking. **REQUIRES parens in source** (`(זינגט)`, `(צו X)`). Never assign `type:delivery` to an unparenthesized line — that's plain speech.
      • `location`   — character's location (`(אין פֿענסטער)`)
      • `business`   — generic physical action ("they embrace", chases). NOTE:
                       curtain cues go to `setting` (above), not here.
      • `costume`    — appearance/disguise/dress
      • `novelistic` — narrative-style third-person descriptive direction
  - **NOT a stage direction** (do not tag as `stage`):
      • `trailer` — a closing label at the end of a division: `ענדע דער X אקט`
        (ende der … akt, "end of act X"), `ענדע פונ'ם X אקט`, `ענדע דער פיעססע`
        (end of the play), or a bare `ענדע`. Tag the whole line as `trailer`
        (no required attribute). The structurer emits TEI `<trailer>`.

`heading`    — act/scene/epilogue title. Attributes:
  - `type`: "act", "scene", or "epilog".
  - `n`: arabic numeral as string ("1", "2", ...) for act/scene; OMIT for epilog.
  Examples: `ערְשׁטֶער אַקט` → heading type=act n=1.  `סצענע צווייטע` → heading type=scene n=2.
  A standalone `עפילאג` line → heading type=epilog (opens an epilogue division
  parallel to the acts; the structurer emits `<div type="epilog">`).
  Tag the whole heading line as a single span.

`role`       — ONLY on castList pages. The character-name portion of a dramatis-personae entry.
  - Attribute `xmlid` REQUIRED: lowercase ASCII transliteration of the name (e.g. "yokhtshe"). Stable & reusable for later #refs in `<sp who>`.
  - The structurer wraps each `role`/`roleDesc` pair (or solitary `role` without desc) in a `<castItem>` element.

`roleDesc`   — ONLY on castList pages. The role-description portion of a dramatis-personae entry.
  - No attributes.
  - Pairs with the nearest preceding `role` span on the same line, OR may stand alone (e.g. a section descriptor like "two guests").
  - **Length covers the ENTIRE description text**, not just the first word. Start `offset` immediately after the role name + any separator (space, dash, comma). End at the last meaningful character of the description on this line — exclude only trailing whitespace and a final `.` if present. Example: line `סאבעלע זיין צווייטע פרוי "35"` with `role` covering `סאבעלע` (length 6) → `roleDesc` offset=7, length covering `זיין צווייטע פרוי "35"` to end of line.

# Hard rules
- offset/length are CHARACTER (code-point) indices into the line text exactly as given to you.
- Spans never cross line boundaries (lines are atomic).
- Spans of different tags on the same line MAY coexist (e.g. `ירוחם: (בעז) אַז...` → speaker + stage).
- Spans of the same tag must not overlap.
- Speaker names: tag the name only, never include the trailing colon, comma, or whitespace.
- If a line is empty or has no taggable content, output `"spans": []` for it.
- Do NOT tag titles/authors/publishers on titlePages — just classify the page as titlePage and emit empty `spans` arrays.

# Output schema
{
  "page_type": "titlePage" | "castList" | "body",
  "lines": [
    {"line_idx": <int>, "spans": [
      {"tag": "...", "offset": <int>, "length": <int>, "attrs": {...}}
    ]},
    ...
  ]
}

Include every line in the input, in order, with its `line_idx` from the input. Output ONLY this JSON.
"""


def build_user_message(filename: str,
                       page_index: int,
                       lines: list[str],
                       rolling_context: dict) -> str:
    """One per-page user message."""
    payload = {
        "filename": filename,
        "page_index": page_index,
        "context": rolling_context,
        "lines": [{"line_idx": i, "text": t} for i, t in enumerate(lines)],
    }
    import json
    return (
        "Annotate this page. Use the context to keep act/scene numbering "
        "and cast xml:ids consistent across pages.\n\n"
        + json.dumps(payload, ensure_ascii=False, indent=2)
    )
