"""Run the person-bridge matcher across all undecided clusters and append
candidates into org_alignment_review.tsv.

Adds at most TOP_N person-derived candidates per cluster, skipping any db_id
already in the existing candidate_db_ids list. New candidates tagged with
candidate_methods='person' or 'person_phonetic' so Ruthie can see the source.

Idempotent: re-running won't double-add (tags whose method starts with 'person'
are stripped first).
"""
from __future__ import annotations
import csv
import sys
from pathlib import Path

HERE = Path(__file__).parent
ALIGN = HERE / "org_alignment_review.tsv"

# Import the prototype's internals
sys.path.insert(0, str(HERE))
from person_alignment_candidates import (  # type: ignore
    load_tsv, PEOPLE_DB, CORE_DB,
    extract_person_spans, build_people_index, build_surname_to_orgs,
    resolve_spans_to_persons, person_to_org_candidates, phonetic_encode,
)

TOP_N = 8
PROCESS_DECIDED = False  # only enrich clusters Ruthie hasn't touched


def split_pipes(s: str) -> list[str]:
    return [x.strip() for x in (s or "").split("|") if x.strip()]


def join_pipes(items: list[str]) -> str:
    return " | ".join(items)


def main():
    print("Loading data…")
    people = load_tsv(PEOPLE_DB)
    core = load_tsv(CORE_DB)
    people_lit, people_phon = build_people_index(people)
    surname_lit, surname_phon = build_surname_to_orgs(core)
    print(f"  people={len(people)} core={len(core)}")
    print(f"  indexes: people_lit={len(people_lit)} people_phon={len(people_phon)}"
          f" orgs_lit={len(surname_lit)} orgs_phon={len(surname_phon)}")

    # Read alignment
    with open(ALIGN, encoding="utf-8") as f:
        rd = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        raw = list(rd)
    header = raw[0]
    rows = [dict(zip(header, r + [""] * (len(header) - len(r)))) for r in raw[1:]]
    print(f"  alignment rows: {len(rows)}")

    # Idempotent cleanup: strip all previous 'person*' tags from EVERY row
    # before re-deriving. This ensures clusters dropped by tighter filters
    # actually lose their stale tags.
    n_stripped = 0
    for r in rows:
        methods = split_pipes(r.get("candidate_methods", ""))
        if not any(m.startswith("person") for m in methods): continue
        ids = split_pipes(r.get("candidate_db_ids", ""))
        scores = split_pipes(r.get("candidate_scores", ""))
        keep = [i for i, m in enumerate(methods) if not m.startswith("person")]
        r["candidate_db_ids"] = join_pipes([ids[i] for i in keep if i < len(ids)])
        r["candidate_scores"] = join_pipes([scores[i] for i in keep if i < len(scores)])
        r["candidate_methods"] = join_pipes([methods[i] for i in keep if i < len(methods)])
        n_stripped += 1
    print(f"  stripped prior person-tags from {n_stripped} rows")

    n_processed = n_added_any = total_added = 0
    n_top1_existed = 0  # how often person matcher's #1 was already in the list

    for r in rows:
        if not PROCESS_DECIDED and r.get("decision", "").strip():
            continue
        n_processed += 1
        cluster_name = r.get("canonical_yiddish", "") or ""
        if not cluster_name.strip():
            continue

        spans = extract_person_spans(cluster_name)
        if not spans: continue
        spans_with_persons = resolve_spans_to_persons(spans, people_lit, people_phon)
        if not spans_with_persons: continue
        cands = person_to_org_candidates(spans_with_persons, surname_lit, surname_phon)
        if not cands: continue

        existing_ids = split_pipes(r.get("candidate_db_ids", ""))
        existing_scores = split_pipes(r.get("candidate_scores", ""))
        existing_methods = split_pipes(r.get("candidate_methods", ""))

        # Drop any previous person-tagged candidates so this is idempotent
        keep_idx = [i for i, m in enumerate(existing_methods) if not m.startswith("person")]
        existing_ids = [existing_ids[i] for i in keep_idx if i < len(existing_ids)]
        existing_scores = [existing_scores[i] for i in keep_idx if i < len(existing_scores)]
        existing_methods = [existing_methods[i] for i in keep_idx if i < len(existing_methods)]

        existing_set = set(existing_ids)

        added = 0
        for c in cands[:TOP_N]:
            if c.db_id in existing_set:
                continue
            # Tag: 'person' if any span resolved literally (no phonetic-only fallback),
            # else 'person_phonetic'. Simple proxy: presence of literal match.
            literal_hit = any(c.db_id == o["db_id"]
                              for sp, persons in spans_with_persons
                              for p in persons
                              for surname_field in ((p.get("hebname","") or "").split()[-1:] +
                                                    (p.get("english","") or "").split()[-1:])
                              for o in surname_lit.get(surname_field.lower().strip("'"), [])
                              if o["db_id"] == c.db_id)
            method = "person" if literal_hit else "person_phonetic"
            existing_ids.append(c.db_id)
            existing_scores.append(f"{c.__dict__.get('score', 0.0):.3f}")
            existing_methods.append(method)
            existing_set.add(c.db_id)
            added += 1
            if added == 1 and cands[0].db_id in existing_set - {c.db_id}:
                n_top1_existed += 1

        if added:
            r["candidate_db_ids"] = join_pipes(existing_ids)
            r["candidate_scores"] = join_pipes(existing_scores)
            r["candidate_methods"] = join_pipes(existing_methods)
            n_added_any += 1
            total_added += added

    # Write back
    with open(ALIGN, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_NONE,
                       escapechar="\\", quotechar="")
        w.writerow(header)
        for r in rows:
            w.writerow([str(r.get(k, "") or "").replace("\t", " ").replace("\n", " ").replace("\\", "/")
                        for k in header])

    print(f"\nundecided clusters processed: {n_processed}")
    print(f"  enriched with person cands: {n_added_any}")
    print(f"  total person candidates added: {total_added}")


if __name__ == "__main__":
    main()
