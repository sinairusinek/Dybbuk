"""Emit the vocabulary alignments the extraction schema page reads.

vocab_alignment.tsv is the study behind SCHEMA_DRAFT §6a: every controlled term
we use, mapped to Wikidata, the LoC relators, BnF roles, Getty AAT and SAPA,
with SKOS-style strength (exact / close / broad / none). The page needs it in
two places — on an entity or relation's detail panel, and in a vocabularies
panel of its own — so it is emitted once, as JSON, rather than hand-copied into
the HTML.

Written next to the page as `extraction_vocab.json` and fetched by it. Re-run
after editing vocab_alignment.tsv:

    python3.11 build_vocab_json.py      # from Zylbercweig/extraction_v2/
"""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
TSV = HERE / "vocab_alignment.tsv"
OUT = REPO / "docs" / "Visualizations" / "extraction_vocab.json"

# The external vocabularies, in the order the page shows them, with how to turn
# an id into a link. `None` means the id is not resolvable to a public URL.
SOURCES = [
    ("wikidata",    "Wikidata",      "https://www.wikidata.org/wiki/{}"),
    ("loc_relator", "LoC relator",   "https://id.loc.gov/vocabulary/relators/{}.html"),
    ("bnf_role",    "BnF role",      None),
    ("getty_aat",   "Getty AAT",     "https://vocab.getty.edu/aat/{}"),
    ("sapa",        "SAPA",          None),
]

# Which vocabulary each entity's fields draw on. An entity shows the alignments
# for every vocabulary listed here; a relation shows the `relation` vocabulary.
ENTITY_VOCABS = {
    "person":     ["occupation"],
    "org":        ["org_type", "org_role"],
    "place":      [],
    "work":       ["work_kind", "language"],
    "role":       [],
    "event":      ["event_type", "event_status"],
    "production": ["function", "org_role"],
    "station":    ["verb_class", "reason"],
    "source":     ["source_kind"],
}

# A relation family may also draw on a vocabulary of its own.
RELATION_VOCABS = {
    "pp": ["relation"],
    "pw": ["function"],
    "po": ["relation"],
    "ww": [],
    "src": ["source_kind"],
    "sp": ["verb_class", "reason"],
    "ep": ["function"],
    "eo": ["org_role"],
}


def strength(cell: str) -> tuple[str, str]:
    """Split `exact:Q33999` into ("exact", "Q33999")."""
    cell = (cell or "").strip()
    if not cell or cell.lower() == "none":
        return ("none", "")
    if ":" in cell:
        head, _, rest = cell.partition(":")
        if head in ("exact", "close", "broad"):
            return (head, rest.strip())
    return ("note", cell)


def main() -> int:
    rows = list(csv.DictReader(TSV.open(encoding="utf-8"), delimiter="\t"))

    terms = []
    for r in rows:
        maps = {}
        for key, label, tmpl in SOURCES:
            s, val = strength(r.get(key, ""))
            if s == "none":
                continue
            maps[key] = {
                "strength": s,
                "id": val,
                "url": (tmpl.format(val) if tmpl and val and s != "note"
                        else None),
            }
        terms.append({
            "vocab": r["vocab"],
            "term": r["term"],
            "scope_note": (r.get("scope_note") or "").strip(),
            "note": (r.get("note") or "").strip(),
            "maps": maps,
            # a term with no external match at all is a gap: one of the
            # contributions the schema paper reports
            "gap": not maps,
        })

    by_vocab: dict[str, list] = {}
    for t in terms:
        by_vocab.setdefault(t["vocab"], []).append(t)

    coverage = {}
    for key, label, _ in SOURCES:
        n = sum(1 for t in terms if key in t["maps"])
        coverage[key] = {"label": label, "n": n,
                         "pct": round(100 * n / len(terms))}

    payload = {
        "generated_from": "Zylbercweig/extraction_v2/vocab_alignment.tsv",
        "n_terms": len(terms),
        "n_vocabs": len(by_vocab),
        "n_gaps": sum(1 for t in terms if t["gap"]),
        "sources": [{"key": k, "label": l} for k, l, _ in SOURCES],
        "coverage": coverage,
        "entity_vocabs": ENTITY_VOCABS,
        "relation_vocabs": RELATION_VOCABS,
        "by_vocab": by_vocab,
        "vocab_order": sorted(by_vocab, key=lambda v: -len(by_vocab[v])),
    }

    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1),
                   encoding="utf-8")
    gaps = [t["term"] for t in terms if t["gap"]]
    print(f"wrote {OUT}")
    print(f"  {len(terms)} terms in {len(by_vocab)} vocabularies; "
          f"{len(gaps)} with no external match")
    print("  coverage: " + " · ".join(
        f"{v['label']} {v['n']}" for v in coverage.values()))
    if gaps:
        print(f"  gaps: {', '.join(gaps[:12])}"
              + (" …" if len(gaps) > 12 else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
