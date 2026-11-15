# France dossier

Country-level place dossier (pattern: Istanbul, Danzig). Output:
`docs/Visualizations/france_{dossier,map,timeline}.html` + `france_entries.json`.

    ./run_extract.sh                         # Gemini, 6 shards over france_ids.txt (resumable)
    python3.11 merge_shards.py               # -> france_itinerary_drafts.jsonl
    python3.11 ../itinerary/build_itinerary_data.py --drafts france_itinerary_drafts.jsonl \
        --out-tsv france_stations_resolved.tsv --out-json ../itinerary/france_itineraries.json
    python3.11 resolve_france.py             # France-only spellings (in place; idempotent)
    (cd ../itinerary && python3.11 build_place_pages.py --place france)
    python3.11 kg_slice.py                   # KG France slice -> kg_slice.json
    python3.11 build_france_dossier.py       # hub page + entry-panel JSON

A station is French when Gemini's `country` says France or its QID is in the French set.
5 entries return malformed JSON deterministically and are listed on the page.
Shard drafts / logs are scratch (not committed); the merged drafts file is the record.
