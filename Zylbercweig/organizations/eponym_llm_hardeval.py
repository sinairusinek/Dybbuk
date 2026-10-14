#!/usr/bin/env python3
"""Adversarial calibration: can the judge pick the RIGHT person among namesakes?

Why this exists. The plain gold eval (eponym_llm_judge.py --gold) scores 145
cases of which only TWO offer more than one candidate -- because the tiers
Sinai swept in-app were one-candidate-by-construction. So that run measures
"does it confirm an obvious match and resist a wrong one", which is necessary
but is NOT the job the open set demands: 691 of the open rows are ambiguous,
where פֿרידמאַן alone matches 15 hubs.

This builds the missing test out of the same gold. For each case Sinai
CONFIRMED, every OTHER hub sharing that surname skeleton is injected as a
distractor, the known-correct answer is hidden among them, and the judge is
asked the same question. 58 gold cases have at least one namesake available
and 42 have three or more, so the test is real rather than synthetic: the
distractors are actual people in the corpus with the same surname.

A judge that scores well here can be trusted on the open low tier. One that
scores well on --gold but badly here would have produced confident, wrong
proposals for an RA to rubber-stamp -- the exact failure that matters.

  python3 eponym_llm_hardeval.py --dry-run
  python3 eponym_llm_hardeval.py --min-distractors 2
"""
from __future__ import annotations

import argparse
import collections
import csv
import datetime as _dt
import pathlib
import re
import sys

import eponym_dossier as dossier
import eponym_llm_judge as judge
from eponym_dossier import key, read_tsv

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
OUT = BASE / "organizations" / "eponym_llm_hardeval.tsv"
MAX_CANDIDATES = 8   # keep a case readable; the real low tier tops out near 15


def surname_index() -> dict[str, list[str]]:
    """surname skeleton -> hub_ids.

    Uses the generator's split_heading(), NOT a comma split: 660 of 2,897
    headings write `surname given` with no comma, and splitting on the comma
    indexes those under their full name. An earlier version of this file did
    exactly that, so its distractor pools were missing a fifth of the corpus
    -- including the namesakes that make a case hard.
    """
    from build_eponym_candidates import split_heading
    idx: dict[str, list[str]] = collections.defaultdict(list)
    for hid, h in dossier._hubs().items():
        sur, _ = split_heading(h.get("canonical_heading") or "")
        if sur:
            idx[sur].append(hid)
    return idx


def build_hard_cases(min_distractors: int) -> list[dict]:
    idx = surname_index()
    samekh = key("ס")
    cases = []
    for c in judge.gold_cases():
        if not c["gold"].startswith("HUB-"):
            continue
        k = key(c["span"])
        pool = idx.get(k) or (idx.get(k[:-1]) if k.endswith(samekh) else None) or []
        distractors = [h for h in pool if h != c["gold"]]
        if len(distractors) < min_distractors:
            continue
        # Deterministic order that does not put the answer first -- a judge
        # that simply picks candidate #1 must not score well by accident.
        hub_ids = sorted(set(distractors[:MAX_CANDIDATES - 1] + [c["gold"]]))
        cases.append({**c, "hub_ids": hub_ids,
                      "n_distractors": len(hub_ids) - 1})
    return cases


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-distractors", type=int, default=1)
    ap.add_argument("--judge", choices=("gemini", "opus"), default="gemini")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--fresh", action="store_true",
                    help="delete the output file and judge every case again")
    args = ap.parse_args()

    cases = build_hard_cases(args.min_distractors)
    if not cases:
        sys.exit("no gold case has that many same-surname distractors")

    n = collections.Counter(c["n_distractors"] for c in cases)
    print(f"hard cases: {len(cases)}  (gold answer hidden among namesakes)")
    for d in sorted(n):
        print(f"   {d} distractor(s): {n[d]} cases")

    fields = judge.FIELDS + ["n_distractors"]
    if args.fresh and OUT.exists() and not args.dry_run:
        OUT.unlink()
        print(f"--fresh: removed {OUT.name}")
    if OUT.exists() and not args.fresh and not args.dry_run:
        done = {(r["db_id"], r["span"]) for r in read_tsv(OUT)}
        before = len(cases)
        cases = [c for c in cases if (c["db_id"], c["span"]) not in done]
        print(f"resuming: {before - len(cases)} already judged, {len(cases)} to go")

    # Verdicts append as they are produced (out_path), so a kill costs at most
    # the case in flight.
    rows = judge.judge_cases(cases, args.judge, args.limit, args.dry_run,
                             args.sleep,
                             out_path=None if args.dry_run else OUT,
                             fields=fields)
    if args.dry_run:
        tot = sum(r.get("rendered_chars", 0) for r in rows)
        print(f"\n≈{tot // 3:,} input tokens (dossiers) + system prompt per call")
        print("DRY RUN -- no API calls, nothing written.")
        return

    rows = read_tsv(OUT)
    print(f"\n{OUT.relative_to(BASE)}: {len(rows)} rows total")

    answered = [r for r in rows if r["choice"] != "ABSTAIN"]
    right = sum(1 for r in answered if r["correct"] == "yes")
    print()
    print("=" * 64)
    print("ADVERSARIAL EVAL -- pick the right namesake")
    print("=" * 64)
    print(f"  cases                 : {len(rows)}")
    print(f"  answered              : {len(answered)}")
    print(f"  correct               : {right}")
    if answered:
        print(f"  PRECISION on answered : {right / len(answered):.3f}")
    print(f"  abstained             : {len(rows) - len(answered)}")
    gf = sum(1 for r in rows if r["grounding"] == "failed")
    print(f"  grounding failures    : {gf}")

    print("\n  by number of distractors:")
    by = collections.defaultdict(lambda: [0, 0, 0])
    for r in rows:
        b = by[r["n_distractors"]]
        b[0] += 1
        if r["choice"] != "ABSTAIN":
            b[1] += 1
            if r["correct"] == "yes":
                b[2] += 1
    for d, (t, a, c) in sorted(by.items(), key=lambda kv: (kv[0] == "", kv[0])):
        p = f"{c / a:.2f}" if a else " n/a"
        print(f"    {d} distractors: {t:3d} cases  answered {a:3d}  "
              f"correct {c:3d}  precision {p}")

    wrong = [r for r in answered if r["correct"] == "no"]
    if wrong:
        print(f"\n  WRONG NAMESAKE CHOSEN ({len(wrong)}):")
        for r in wrong:
            print(f"    db{r['db_id']:>5s} {r['span']:14s} [{r['confidence']}] "
                  f"chose {r['proposal_heading']}")
            print(f"           correct: {r['gold']}")
            print(f"           why    : {r['reason'][:150]}")


if __name__ == "__main__":
    main()
