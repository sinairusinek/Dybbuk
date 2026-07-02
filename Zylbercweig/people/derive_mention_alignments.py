"""Materialize the mention→heading→db_id chain (Phase C, C1a).

The three RA mention-validation sheets map mention SURFACE FORMS to subject
headings. Where the heading's entry carries a subject→DB alignment, each
mapping yields a mention→db_id alignment. This script writes the chain to
`derived_mention_alignments.tsv` — one row per (surface, source_sheet).

IMPORTANT semantics: rows from the `surnames` sheet record the DOMINANT
referent of a bare surname corpus-wide (RA judgment), NOT a per-occurrence
truth. The surname resolver (resolve_surname_mentions.py) uses them as a fame
prior; only `full` and `initials` rows are safe to apply as global lexicon
entries.

Run: python3.11 Zylbercweig/people/derive_mention_alignments.py
"""
from __future__ import annotations

import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from people_common import (  # noqa: E402
    build_heading_index,
    build_person_db_map,
    load_extracted,
    read_tsv,
    write_tsv,
)
from people_similarity import normalize_person_name  # noqa: E402

OUT_TSV = HERE / "derived_mention_alignments.tsv"

SHEETS = {
    "full": HERE / "mention_validations_full.tsv",
    "initials": HERE / "mention_validations_initials.tsv",
    "surnames": HERE / "mention_validations_surnames.tsv",
}

FIELDS = [
    "mention_surface", "source_sheet", "occurrences",
    "as_heading", "alternative_heading", "notes",
    "match_method", "matched_person_ids", "n_matched",
    "db_id", "db_status",
]


def main() -> None:
    extracted = load_extracted()
    heading_idx = build_heading_index(extracted)
    norm_idx: dict[str, list[str]] = {}
    for h, pids in heading_idx.items():
        norm_idx.setdefault(normalize_person_name(h), []).extend(pids)
    person_db, db_report = build_person_db_map(extracted)

    out: list[dict] = []
    stats: dict[str, list[int]] = {}
    for sheet, path in SHEETS.items():
        n_total = n_heading = n_matched = n_db = 0
        for r in read_tsv(path):
            n_total += 1
            heading = (r.get("as_heading") or "").strip()
            if not heading:
                continue
            n_heading += 1
            pids = heading_idx.get(heading, [])
            method = "exact"
            if not pids:
                pids = norm_idx.get(normalize_person_name(heading), [])
                method = "normalized" if pids else "none"
            pids = sorted(set(pids))
            if pids:
                n_matched += 1
            db_ids = sorted({person_db[p] for p in pids if p in person_db})
            if len(db_ids) == 1:
                db_id, db_status = db_ids[0], "ok"
                n_db += 1
            elif len(db_ids) > 1:
                db_id, db_status = "", "conflicting_db_ids"
            elif pids:
                db_id, db_status = "", "no_db_alignment"
            else:
                db_id, db_status = "", "no_entry_match"
            out.append({
                "mention_surface": (r.get("mention") or "").strip(),
                "source_sheet": sheet,
                "occurrences": (r.get("occurrences") or "").strip(),
                "as_heading": heading,
                "alternative_heading": (r.get("alternative_heading") or "").strip(),
                "notes": (r.get("notes") or "").strip(),
                "match_method": method,
                "matched_person_ids": "|".join(pids),
                "n_matched": len(pids),
                "db_id": db_id,
                "db_status": db_status,
            })
        stats[sheet] = [n_total, n_heading, n_matched, n_db]

    out.sort(key=lambda r: (r["source_sheet"], r["mention_surface"], r["as_heading"]))
    write_tsv(OUT_TSV, out, FIELDS)

    print(f"wrote {len(out)} rows → {OUT_TSV.name}")
    print(f"{'sheet':<10} {'rows':>6} {'w/heading':>10} {'entry-matched':>14} {'w/db_id':>8}")
    for sheet, (a, b, c, d) in stats.items():
        print(f"{sheet:<10} {a:>6} {b:>10} {c:>14} {d:>8}")
    total_db = sum(v[3] for v in stats.values())
    print(f"total derivable mention→db_id: {total_db} (memory expectation ≈2,950)")
    skipped = {}
    for _, reason in db_report:
        skipped[reason] = skipped.get(reason, 0) + 1
    print(f"person→db map skips: {skipped}")


if __name__ == "__main__":
    main()
