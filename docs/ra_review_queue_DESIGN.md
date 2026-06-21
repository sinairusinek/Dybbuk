# RA Review Queue — design sketch (2026-06-21)

A shared async queue that lets Claude (CLI), Noa/Maati (RAs), and Sinai (PI) collaborate on per-line review items without the round-trip-via-Sinai bottleneck. Lives at the **repo root** (not under YiDraCor) because the same protocol serves YiDraCor + Kimatch + Zylbercweig RA work.

## Why this shape (recap)

- Real-time three-way chat with Claude isn't a thing Anthropic ships today; build-your-own would be expensive overkill.
- An **async queue + thin Streamlit UI** gives the "three of us touch the same backlog" feeling.
- Generalizes — one protocol, multiple topics (YiDraCor: speaker / stage type / castList span; Kimatch: place QID pick; etc.).
- Same shape you already use for Zylbercweig org-review — proven pattern.

## File layout

```
data/review/
  queue.jsonl              # append-only event log (the source of truth)
  apply_handlers/          # per-topic application logic (Claude-side)
    yidracor_speaker.py
    yidracor_stage_type.py
    yidracor_castlist_span.py
    kimatch_place_match.py
  ui/
    streamlit_app.py       # the RA-facing UI
    requirements.txt
```

## Queue protocol (jsonl, append-only)

Each line is **one event**. Items have a stable `id`; the latest event per `id` is the current state. State machine:

```
asked → answered → applied
      ↘ dismissed
```

`asked` is the only event that creates a new item; subsequent events update it.

### Event schema

```jsonl
{
  "id": "yidracor_BasSheva_p23_l4_speaker_a1b2c3",
  "event": "asked",                          # asked | answered | applied | dismissed
  "at": "2026-06-21T14:23:01Z",
  "by": "claude",                            # claude | noa | maati | sinai
  "project": "yidracor",                     # yidracor | kimatch | zylbercweig | ...
  "topic": "speaker_resolution",             # for apply-handler dispatch
  "assigned_to": "noa",                      # noa | maati | any | sinai

  # Locator — wherever in the source data this question is about
  "locator": {
    "play": "BasSheva",
    "doc_id": 828443,
    "page": 23,
    "line_id": "r2l4",
    "tk_url": "https://app.transkribus.org/collection/2372172/doc/828443/page/23"
  },

  # The substance
  "context_lines": ["...line above...", "TARGET LINE", "...line below..."],
  "question": "Speaker label ראָזא — match to existing roza or coin new?",
  "options": [
    {"key": "a", "label": "Match to existing roza", "recommended": true},
    {"key": "b", "label": "Coin new — roza_<scene>"},
    {"key": "c", "label": "Other (please write below)"}
  ],
  "allow_freetext": true,
  "tags": ["castList-confirmed", "post-act-IV"]   # filters in UI
}
```

`answered` event adds `answer: {key: "a", freetext: null, note: "RA note", by: "noa", at: "..."}`.

`applied` event adds `applied_action: "set speaker xmlid=roza, pushed parent_tsid=292…"` and `applied_at`.

`dismissed` event adds `dismiss_reason: "duplicate of <other_id>"`.

### Why JSONL append-only
- Git-friendly: diffs are clean, every change shows who/when/what.
- No locking — concurrent writes are fine (open-append-close per line).
- Easy to replay or audit.
- Easy for Claude (CLI) and Streamlit to read without DB.

## UI (Streamlit) — RA-facing

One page, three filter dropdowns at top:
- **Assignee** (Noa / Maati / Any)
- **Project** (YiDraCor / Kimatch / All)
- **State** (Pending / All / Answered / Applied)

Below: list of cards, newest-first. Per card:

```
┌─────────────────────────────────────────────────────────────┐
│ YiDraCor · BasSheva · p23 · r2l4 · speaker_resolution       │
│ Asked 2 hours ago by Claude                                 │
│                                                              │
│ Context:                                                     │
│   …זיין טאָכטער. מיצי הארפעי                                  │
│   ראָזא: ניין, איך וועל ניט געהן                              │  ← target highlighted
│   טוביה: שׂרה'לע…                                            │
│                                                              │
│ ▸ Open in Transkribus                                       │
│                                                              │
│ Q: Speaker label ראָזא — match to existing roza or coin new? │
│                                                              │
│ ○ (a) Match to existing roza  ★ recommended                 │
│ ○ (b) Coin new — roza_<scene>                                │
│ ○ (c) Other:                                                │
│                                                              │
│ Note (optional): [______________________________________]   │
│                                                              │
│        [ Submit ]   [ Skip for now ]   [ Dismiss ]          │
└─────────────────────────────────────────────────────────────┘
```

Image embed: a small crop of the actual TextLine bounding box from the TK image (the coords are in PAGE-XML; we already pull them). Saves Noa the click-out for most items; the link is there for full context.

Submit → appends one `answered` event to `queue.jsonl`, refreshes the list.

## Claude-side workflow (CLI)

Three operations Claude performs:

### 1. Ask
When Claude (this session) hits an ambiguity it can't resolve confidently, it appends an `asked` event:

```python
# pseudocode
queue.ask(
    project="yidracor",
    topic="speaker_resolution",
    assigned_to="noa",
    locator={...},
    question="...",
    options=[...],
)
```

A new skill `/queue ask <topic>` makes this ergonomic.

### 2. Drain (apply answered items)
At session start or on demand (`/queue drain`):
1. Read `queue.jsonl`, build latest-state-per-id map.
2. For each item in `answered` state assigned to this project: load its apply-handler (`apply_handlers/<topic>.py`), call `apply(item, answer)`.
3. Handler returns a description of what it did. Append `applied` event with that description.
4. Handler is also responsible for the actual side effect (XML edit, push to TK, etc.).

### 3. Audit
`/queue status` — show pending/answered/applied counts per topic, assignee, project.

## Apply handlers (per topic)

Each handler is a small Python module:

```python
# apply_handlers/yidracor_speaker.py

def apply(item: dict, answer: dict) -> str:
    """Apply a speaker-resolution answer. Returns description of action taken."""
    play = item["locator"]["play"]
    page = item["locator"]["page"]
    line_id = item["locator"]["line_id"]
    if answer["key"] == "a":
        # match to existing xmlid (use the recommended option's label)
        xmlid = parse_xmlid_from_option(item, "a")
        return surgical_speaker_push(play, page, line_id, xmlid)
    elif answer["key"] == "b":
        # coin new — needs more info (scene context); freetext field carries the new xmlid
        new_xmlid = answer["freetext"]
        add_to_cast_dict(play, new_xmlid, declared="body-only", note=answer.get("note"))
        return surgical_speaker_push(play, page, line_id, new_xmlid)
    elif answer["key"] == "c":
        # freetext escape hatch — surface to Sinai
        raise RequiresSinai(answer["freetext"])
```

Each handler is small and self-contained. Adding a topic = adding one file.

## Sinai-side oversight

- `git log -p data/review/queue.jsonl` shows the full audit trail.
- `git blame` shows who answered what when.
- For items that escape to Sinai (RequiresSinai exception), the `applied` event records `applied_by: "claude", applied_action: "escalated to sinai"` and Sinai can post a follow-up `answered` event with `by: "sinai"` that re-triggers apply.
- Sinai can also append `dismissed` events to reject answers Noa got wrong (rare).

## Implementation phases

**MVP (v0):**
- `queue.jsonl` protocol.
- One apply handler: `yidracor_speaker.py` (the most common ambiguity during body-page annotation).
- Streamlit UI: list pending items, show context + TK link (text only, no image embed yet), accept answer.
- `/queue` skill in Claude Code with `ask`, `drain`, `status`.
- Deploy to Streamlit Cloud or run locally; share URL with Noa.

**v1:**
- Add handlers: `yidracor_stage_type.py`, `yidracor_castlist_span.py`.
- TK image-crop preview in the card (uses PAGE-XML Coords + TK image URL).
- Bulk-apply / bulk-dismiss for similar items.

**v2 (if it proves out):**
- Add `kimatch_place_match.py` handler — covers Maati's review workflow.
- Per-project landing page (one URL each for YiDraCor RA and Kimatch RA).

## Estimated effort

- v0 MVP: ~1 day of work (queue read/write, one handler, basic Streamlit, deploy).
- v1: another ~1 day (image crops, more handlers, polish).

You've built all the building blocks before (Streamlit apps, surgical TK pushes, JSON-lined data) — this is assembly, not invention.

## Open questions for Sinai

1. **Where does the Streamlit app run?** Streamlit Cloud (easiest, like Kimatch), local-only, or on a VM you control?
2. **TK credentials for the app** — service account, or RA-account-on-behalf-of (proxied)?
3. **Authentication on the UI** — single shared password / per-RA login / open?
4. **Should `queue.jsonl` be in the Dybbuk repo, or in a separate small repo / GitHub Gist** (avoids hundreds of queue commits cluttering Dybbuk history)?
5. **Notification** — does Noa want an email/Slack ping when items are added, or is "she checks the URL when she has time" fine?
