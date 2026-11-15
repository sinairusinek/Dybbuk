# The Dybbuks — play dossier

Every work called "The Dybbuk" in the Leksikon (Ansky, Lateiner, Shomer, parodies, Rocca's opera,
the 1937 film): people by role + geography of creation and production.

    GOOGLE_API_KEY=... python3.11 extract_dybbuk.py     # 146 entries (dybbuk_ids.txt), resumable
    python3.11 flatten_dybbuk.py                        # -> dybbuk_facts.tsv (review) + dybbuk_data.json
    python3.11 build_dybbuk_pages.py                    # -> docs/Visualizations/dybbuk_dossier.html

`page_head.html` / `page_text.py` hold the CSS and prose. Hand reclassifications are `OVERRIDES`
in build_dybbuk_pages.py; name merges (`SAME_PERSON`, surname+initial) are display-only.
