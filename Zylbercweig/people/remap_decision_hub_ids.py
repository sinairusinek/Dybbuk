"""Repoint B3 decisions at hub ids that were renamed underneath them.

A hub minted from an entry with no DB match gets a provisional id
(HUB-P-3-facs_375_tr_...). When B2 later aligns that entry to a DB record the
hub is reissued as HUB-D####, and any B3 decision naming the old id is silently
orphaned: the person still exists, but nothing downstream can resolve the
decision to them. 99 decisions across two FULLY reviewed surnames (Maurice
Schwartz 52, Horowitz 47) were stranded this way.

The remap is identity-preserving and only applied when it is unambiguous: the
old hub's entry_person_id must now live in exactly one hub. Anything else is
reported and left alone rather than guessed at.

Run: python3.11 Zylbercweig/people/remap_decision_hub_ids.py [--apply]
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[1]
HUB_TSV = HERE / "person_hub.tsv"
DECISIONS_TSV = HERE / "mention_resolution_decisions.tsv"
REL_HUB = "Zylbercweig/people/person_hub.tsv"
FIRST_HUB_REV = "2d0dec45"          # hub file as it stood when B3 review began


def _rows(text: str) -> list[dict]:
    csv.field_size_limit(sys.maxsize)
    return list(csv.DictReader(text.splitlines(), delimiter="\t"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the remap")
    args = ap.parse_args()

    csv.field_size_limit(sys.maxsize)
    now = _rows(HUB_TSV.read_text())
    live = {h["hub_id"] for h in now}
    pid2hubs: dict[str, list[str]] = {}
    for h in now:
        for p in h["entry_person_ids"].split("|"):
            if p:
                pid2hubs.setdefault(p, []).append(h["hub_id"])

    # every hub id that has ever existed, so a vanished one can be traced back
    hist: dict[str, list[str]] = {}
    for rev in (FIRST_HUB_REV, "HEAD"):
        out = subprocess.run(["git", "-C", str(REPO), "show", f"{rev}:{REL_HUB}"],
                             capture_output=True, text=True)
        for h in _rows(out.stdout):
            hist.setdefault(h["hub_id"], [p for p in h["entry_person_ids"].split("|") if p])

    with open(DECISIONS_TSV) as f:
        fields = csv.DictReader(f, delimiter="\t").fieldnames
    decisions = _rows(DECISIONS_TSV.read_text())

    remap: dict[str, str] = {}
    unresolved: set[str] = set()
    for old in {d["resolved_hub_id"] for d in decisions
                if d["resolved_hub_id"] and d["resolved_hub_id"] not in live}:
        targets = {h for p in hist.get(old, []) for h in pid2hubs.get(p, [])}
        if len(targets) == 1:
            remap[old] = next(iter(targets))
        else:
            unresolved.add(old)

    n = sum(1 for d in decisions if d["resolved_hub_id"] in remap)
    head = {h["hub_id"]: h["canonical_heading"] for h in now}
    print(f"decisions naming a dead hub id: "
          f"{sum(1 for d in decisions if d['resolved_hub_id'] and d['resolved_hub_id'] not in live)}")
    for old, new in remap.items():
        k = sum(1 for d in decisions if d["resolved_hub_id"] == old)
        print(f"  {k:3d}  {old}\n       → {new}  {head.get(new, '')}")
    for old in unresolved:
        print(f"  !!   {old} — ambiguous or untraceable, left alone")

    if not args.apply:
        print(f"\ndry run — would rewrite {n} decision(s). Re-run with --apply.")
        return
    for d in decisions:
        new = remap.get(d["resolved_hub_id"])
        if new:
            d["resolved_hub_id"] = new
            d["resolved_heading"] = head.get(new, d.get("resolved_heading", ""))
    with open(DECISIONS_TSV, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fields, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(decisions)
    print(f"\nrewrote {n} decision(s) in {DECISIONS_TSV.name}")


if __name__ == "__main__":
    main()
