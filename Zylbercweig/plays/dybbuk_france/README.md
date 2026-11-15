# The Dybbuk in France

Play × place dossier: `docs/Visualizations/dybbuk_france.html` (+ `dybbuk_france_entries.json`).
Timeline of lifelines, map of tour routes, agent network, and the quoted record.

    GOOGLE_API_KEY=... python3.11 extract_dybbuk_france.py   # Gemini over candidate_ids.txt -> drafts.jsonl
    python3.11 build_dybbuk_france.py                        # curated.py -> page, entries JSON, dybbuk_france_facts.tsv

`candidate_ids.txt` = the 68 entries mentioning a Dybbuk and a French place/the French language.
`drafts.jsonl` is a finding aid only (≈⅔ of its rows were wrong or off-topic). The page is built
from `curated.py`: hand-verified facts, each anchored to a verbatim quote (`anchors.py` resolves
(start, end) anchors on unpointed text; the build fails if an anchor stops matching).
