# Unified toponym dataset — visual schema

> Rendered with Mermaid (VSCode markdown preview / GitHub). Prose in
> [unified_toponyms.md](unified_toponyms.md).

## 1. Architecture — attestation spine + derived views + enrich-back

```mermaid
flowchart LR
    subgraph SRC["source lists"]
        UNI["places_unified_corrected.csv<br/>linked person mentions"]:::src
        RAW["raw extraction<br/>(unlinked person spellings)"]:::src
        MA["ZylbercweigPlacesMaaty.tsv<br/>alt person QID (flagged)"]:::src
        KM["kimatch / kima_variants<br/>Kima IDs per QID"]:::src
        OAR["org_alignment_review.tsv<br/>all org place fields / cluster_id"]:::src
        COL["collapse_audit<br/>(cluster_id,variant)→QID"]:::src
        CO["settlement_coords.tsv"]:::src
        PUN["unresolved punchlist<br/>suggested_qid"]:::src
    end

    BUILD{{"build_unified_toponyms.py"}}:::build
    UNI & RAW & MA & KM & OAR & COL & CO & PUN --> BUILD

    SPINE["<b>toponyms_attestations.csv</b><br/>18,790 rows · one per attestation<br/>never deduped · keeps back-pointer"]:::spine
    BUILD --> SPINE

    SPINE -- "group by qid (linked)" --> GAZ["toponyms_gazetteer.csv<br/>883 places"]:::out
    SPINE -- "group by spelling (unlinked)" --> UNL["toponyms_unlinked.csv<br/>4,469 spellings · +attestation_ids"]:::out

    SPINE -. "source_record_id = entry_id" .-> PPL[("people data")]:::back
    SPINE -. "source_record_id = cluster_id<br/>org_db_id → core_db" .-> ORGD[("organization data")]:::back

    classDef src fill:#e3f2fd,stroke:#1565c0,color:#0d47a1;
    classDef spine fill:#ede7f6,stroke:#4527a0,color:#311b92;
    classDef out fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20;
    classDef build fill:#fff3e0,stroke:#e65100,color:#e65100;
    classDef back fill:#fce4ec,stroke:#ad1457,color:#880e4f;
```

## 2. Entity relationship — spine and its derived views

```mermaid
erDiagram
    TOPONYMS_ATTESTATIONS ||--o{ TOPONYMS_GAZETTEER : "linked attestations group by qid"
    TOPONYMS_ATTESTATIONS ||--o{ TOPONYMS_UNLINKED : "unlinked attestations group by spelling"

    TOPONYMS_ATTESTATIONS {
        string attestation_id PK
        string source_corpus "person | org"
        string source_record_id "entry_id | cluster_id — BACK-POINTER"
        string org_db_id "aligned_db_id → core_db (org)"
        string source_field "place/province/country | settlements/venues/…"
        string context "birth/death/burial (person)"
        string source_value "Yiddish, verbatim (attested)"
        string source_value_script
        string link_status "linked | needs_review | unlinked"
        string qid "resolved — empty if unlinked"
        string label_en
        string label_yi
        string place_type
        string category
        string kima_id
        float  lat
        float  lon
        string maaty_qid "person alt-resolution"
        bool   maaty_qid_conflict
        string suggested_qid "candidate for unlinked"
        bool   is_descriptor
        string review_flags
    }

    TOPONYMS_GAZETTEER {
        string qid PK
        string label_en
        string label_yi
        string place_type
        string kima_id
        float  lat
        float  lon
        int    n_attestations
        int    n_person
        int    n_org
        int    n_flagged_mentions
        string fields
        int    n_variants
        string variants
        string corpora
        string external_sources "wikidata[;kima]"
    }

    TOPONYMS_UNLINKED {
        string variant PK "distinct spelling"
        string script
        string corpora "person/org/both"
        int    occurrences
        string fields
        string contexts
        string suggested_qid
        bool   is_descriptor
        string attestation_ids "back-map to spine rows"
    }
```
