#!/usr/bin/env python3
"""Second-pass retrieval: resolve the names the judge READ but could not pick.

When the judge abstains it may still name the person the sentences point to,
in `free_text_name` -- and crucially it supplies the GIVEN name, which the
span usually lacks (`לארגמאנ` -> `ליאַ לאָרגמאַן`). That is the search key the
surname index could never have.

Three outcomes, kept apart because they are different kinds of work:

  recovered   surname AND given name both match one hub -> an eponym proposal
              for the RA, same standing as any other.
  absent      the name does not resolve, or resolves only on the surname ->
              a PEOPLE-side gap: an org named after someone with no usable
              hub. Not an eponym answer; a finding about person_hub.tsv.
  ambiguous   several hubs match even with the given name -> needs a reader.

Why surname-only never counts as recovered: it generates confident nonsense.
`ליאַ לאָרגמאַן` matches `ברין, אַליאַ [גאָלדע זיידלין]` on the given name's
skeleton, and `זיגמונד טורקאָוו` matches `זיגמונד שוואַרץ`. Both are plainly
different people. An earlier draft of this idea used surname-only and would
have shipped those as links.

  python3 build_eponym_recall.py
"""
from __future__ import annotations

import argparse
import collections
import csv
import pathlib

from build_eponym_candidates import HEB, key, load_surname_index
from eponym_dossier import read_tsv

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
ORGS = BASE / "organizations"
VERDICTS = ORGS / "eponym_llm_verdicts.tsv"
CORE_DB = ORGS / "core_db.tsv"
OUT = ORGS / "eponym_recall_candidates.tsv"

FIELDS = [
    "outcome", "db_id", "org_name", "span", "name_read_by_judge",
    "hub_id", "person_heading", "person_db_id", "n_hub_matches",
    "match_basis", "n_candidates_offered", "judge_reason", "judge_quote",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out", default=str(OUT))
    args = ap.parse_args()

    by_surname, by_full = load_surname_index()
    orgs = {r["db_id"]: r for r in read_tsv(CORE_DB)
            if (r.get("db_id") or "").strip()}
    rows = [r for r in read_tsv(VERDICTS)
            if (r.get("free_text_name") or "").strip()]

    out: list[dict] = []
    for r in rows:
        name = r["free_text_name"].strip()
        toks = [t for t in HEB.findall(key(name)) if len(t) > 1]

        hit, basis = None, ""
        # Strict: some token pair must match (surname, given) together.
        for i, a in enumerate(toks):
            for j, b in enumerate(toks):
                if i != j and (a, b) in by_full:
                    hit, basis = by_full[(a, b)], "surname+given"
                    break
            if hit:
                break
        if not hit:
            # Surname alone -- recorded, but NEVER as a recovery.
            for t in toks:
                if t in by_surname:
                    hit, basis = by_surname[t], "surname only"
                    break

        if basis == "surname+given" and len(hit) == 1:
            outcome = "recovered"
        elif basis == "surname+given":
            outcome = "ambiguous"
        else:
            outcome = "absent"

        o = orgs.get(r["db_id"], {})
        h = hit[0] if hit else {}
        out.append({
            "outcome": outcome,
            "db_id": r["db_id"],
            "org_name": (o.get("name_yiddish") or "").strip()
                        or (o.get("name") or "").strip(),
            "span": r["span"],
            "name_read_by_judge": name,
            "hub_id": h.get("hub_id", "") if outcome == "recovered" else "",
            "person_heading": h.get("canonical_heading", "") if outcome == "recovered" else "",
            "person_db_id": (h.get("db_ids", "") or "").split(",")[0].strip()
                            if outcome == "recovered" else "",
            "n_hub_matches": len(hit) if hit else 0,
            "match_basis": basis,
            "n_candidates_offered": r.get("n_candidates", ""),
            "judge_reason": (r.get("reason") or "")[:300],
            "judge_quote": (r.get("quote") or "")[:300],
        })

    order = {"recovered": 0, "ambiguous": 1, "absent": 2}
    out.sort(key=lambda d: (order[d["outcome"]], int(d["db_id"])))

    path = pathlib.Path(args.out)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(out)

    counts = collections.Counter(d["outcome"] for d in out)
    print(f"names the judge read but could not pick: {len(rows)}")
    for k in ("recovered", "ambiguous", "absent"):
        print(f"  {k:10s} {counts.get(k, 0):4d}")
    print(f"\n  'absent' = an organisation named after someone with no usable")
    print(f"  hub. That is a finding about person_hub.tsv, not an eponym answer.")
    print(f"\nwrote {path.relative_to(BASE)}")


if __name__ == "__main__":
    main()
