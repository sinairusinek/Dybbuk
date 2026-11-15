#!/usr/bin/env python3
"""Build the inputs of the Zalmen "People · family cards" view.

For each family directory under people/family_cards/<family>/ this reads
  pilot/profiles/*.json          one profile per member, extracted from the
                                 member's Leksikon entry (quotes verbatim)
  pilot/cases_b3_v2.json         the family's unresolved B3 mentions, target
                                 wrapped in ⟦ ⟧ inside ±650 chars of entry text
  pilot/judge2_out_*.json  + pilot/idmap_v2.json   blind judge reading 1
  pilot/judge3_out_*.json  + pilot/idmap_v3.json   blind judge reading 2
and writes
  <family>/family_sheet.json     the profiles, as the card shows them
  family_proposals.tsv           one row per mention, all families

A proposal is `agree=1` only when both readings named the same hub and
neither reading was low-confidence. The view
pre-selects only those. Precision of that rule on masked full-name mentions
with known answers is recorded in README.md -- re-measure before trusting a new
family, and never write a proposal into the decisions ledger from here.

  python3.11 build_family_cards.py
"""
from __future__ import annotations

import csv
import glob
import json
import pathlib

csv.field_size_limit(10 ** 9)
HERE = pathlib.Path(__file__).resolve().parent
PEOPLE = HERE.parent
OUT = HERE / "family_proposals.tsv"
FIELDS = ["family", "mention_id", "surname", "host_person_id", "host_heading",
          "mention_name", "context", "chips", "resolver_verdict", "resolver_method",
          "resolver_pick", "judge1_answer", "judge1_confidence", "judge1_reason",
          "judge1_quote", "judge2_answer", "judge2_confidence", "judge2_reason",
          "judge2_quote", "agree", "proposal_hub"]


def readings(pilot: pathlib.Path, tag: str) -> dict[str, dict]:
    idmap = pilot / f"idmap_{tag}.json"
    if not idmap.exists():
        return {}
    m = json.loads(idmap.read_text())
    out = {}
    for p in sorted(glob.glob(str(pilot / f"judge{tag[-1]}_out_*.json"))):
        for r in json.loads(pathlib.Path(p).read_text()):
            out[m.get(r["case_id"], r["case_id"])] = r
    return out


def main() -> int:
    res = {r["mention_id"]: r for r in csv.DictReader(
        open(PEOPLE / "mention_surname_resolutions.tsv", encoding="utf-8"), delimiter="\t")}
    rows = []
    for fam_dir in sorted(p for p in HERE.iterdir() if (p / "pilot").is_dir()):
        family, pilot = fam_dir.name, fam_dir / "pilot"
        sheet = [json.loads(pathlib.Path(p).read_text())
                 for p in sorted(glob.glob(str(pilot / "profiles" / "*.json")))]
        (fam_dir / "family_sheet.json").write_text(
            json.dumps(sheet, ensure_ascii=False, indent=1), encoding="utf-8")
        r1, r2 = readings(pilot, "v2"), readings(pilot, "v3")
        for c in json.loads((pilot / "cases_b3_v2.json").read_text()):
            r = res.get(c["mention_id"])
            if not r:
                continue
            ranked = [h for h in r["candidate_hub_ids"].split("|") if h]
            a, b = r1.get(c["case_id"], {}), r2.get(c["case_id"], {})
            # Both readings name the same person and neither is low-confidence:
            # 38/41 = 0.93 on the Adler test set (any-confidence agreement 0.89).
            agree = (a.get("answer", "").startswith("HUB") and a.get("answer") == b.get("answer")
                     and "low" not in (a.get("confidence"), b.get("confidence")))
            rows.append({
                "family": family, "mention_id": c["mention_id"],
                "surname": r["surname_token"], "host_person_id": r["host_person_id"],
                "host_heading": r["host_heading"], "mention_name": r["mention_name"],
                "context": c["context"], "chips": r["chips"],
                "resolver_verdict": r["verdict"], "resolver_method": r["method"],
                "resolver_pick": r["resolved_hub_id"] or (ranked[0] if ranked else ""),
                **{f"judge1_{k}": a.get(k, "") for k in ("answer", "confidence", "reason", "quote")},
                **{f"judge2_{k}": b.get(k, "") for k in ("answer", "confidence", "reason", "quote")},
                "agree": "1" if agree else "0",
                "proposal_hub": a.get("answer", "") if agree else "",
            })
        print(f"{family}: {len(sheet)} members, "
              f"{sum(1 for x in rows if x['family'] == family)} mentions, "
              f"{sum(1 for x in rows if x['family'] == family and x['agree'] == '1')} agreed proposals")
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {OUT.name}: {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
