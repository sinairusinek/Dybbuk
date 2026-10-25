#!/usr/bin/env python3
"""Report on the eponym judge: is it trustworthy enough to graduate?

Reads eponym_llm_gold_eval.tsv (calibration against Sinai's own 148
decisions), eponym_llm_audit.tsv (Opus second opinion on disagreements) and
eponym_llm_verdicts.tsv (the open-set proposals), and prints what a human
needs in order to decide whether this engine should also draft the Zalmen
org-alignment and minting queues.

The numbers that decide it:
  precision on answered  -- of the cases it did not abstain on, how many match
                            the human answer. The gate is 0.90.
  abstention rate        -- high abstention with high precision is a GOOD
                            result: it means the queue shrinks honestly.
  grounding failures     -- verdicts whose quote was not in the dossier. If
                            this is high the model is confabulating and no
                            precision number can be trusted.
  confidence calibration -- does self-reported "high" actually mean high?

  python3 eponym_llm_report.py
  python3 eponym_llm_report.py --sample 20
"""
from __future__ import annotations

import argparse
import collections
import csv
import pathlib
import random

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
ORGS = BASE / "organizations"
GOLD = ORGS / "eponym_llm_gold_eval.tsv"
AUDIT = ORGS / "eponym_llm_audit.tsv"
VERDICTS = ORGS / "eponym_llm_verdicts.tsv"

GATE_PRECISION = 0.90
GATE_GROUNDING = 0.05


def read_tsv(p: pathlib.Path) -> list[dict]:
    if not p.exists():
        return []
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def rule(title: str) -> None:
    print()
    print("=" * 68)
    print(title)
    print("=" * 68)


def report_gold(rows: list[dict]) -> None:
    rule(f"CALIBRATION vs Sinai's decisions  ({len(rows)} cases)")
    scored = [r for r in rows if r.get("gold")]
    if not scored:
        print("  no scored rows")
        return
    answered = [r for r in scored if r["choice"] != "ABSTAIN"]
    right = sum(1 for r in answered if r["correct"] == "yes")
    prec = right / len(answered) if answered else 0.0
    gf = sum(1 for r in rows if r["grounding"] == "failed")
    gfr = gf / len(rows) if rows else 0.0

    print(f"  answered                : {len(answered)}/{len(scored)}"
          f"  ({len(answered) * 100 // len(scored)}%)")
    print(f"  correct                 : {right}")
    print(f"  PRECISION on answered   : {prec:.3f}   "
          f"{'PASS' if prec >= GATE_PRECISION else 'FAIL'} (gate {GATE_PRECISION})")
    print(f"  grounding failure rate  : {gfr:.3f}   "
          f"{'PASS' if gfr < GATE_GROUNDING else 'FAIL'} (gate <{GATE_GROUNDING})")

    print("\n  by tier (the tier the string matcher assigned):")
    by = collections.defaultdict(lambda: [0, 0, 0])
    for r in scored:
        b = by[r["tier"] or "?"]
        b[0] += 1
        if r["choice"] != "ABSTAIN":
            b[1] += 1
            if r["correct"] == "yes":
                b[2] += 1
    for t, (n, a, c) in sorted(by.items()):
        print(f"    {t:12s} {n:4d} cases   answered {a:4d}   correct {c:4d}   "
              f"precision {(c / a) if a else 0:.2f}")

    print("\n  confidence calibration (does 'high' mean high?):")
    conf = collections.defaultdict(lambda: [0, 0])
    for r in answered:
        b = conf[r["confidence"] or "?"]
        b[0] += 1
        if r["correct"] == "yes":
            b[1] += 1
    for c, (n, ok) in sorted(conf.items()):
        print(f"    {c:8s} {n:4d} answered   correct {ok:4d}   "
              f"precision {(ok / n) if n else 0:.2f}")

    wrong = [r for r in answered if r["correct"] == "no"]
    if wrong:
        print(f"\n  DISAGREEMENTS ({len(wrong)}) — read these before trusting anything:")
        for r in wrong:
            print(f"    db{r['db_id']:>5s} {r['span']:14s} [{r['confidence']}] "
                  f"chose {r['proposal_heading'] or r['choice']}")
            print(f"           gold: {r['gold']}")
            print(f"           why : {r['reason'][:160]}")
            if r.get("quote"):
                print(f"           cite: {r['quote'][:120]}")


def report_audit(rows: list[dict], gold: list[dict]) -> None:
    if not rows:
        return
    rule(f"OPUS AUDIT of disagreements  ({len(rows)} cases)")
    gmap = {(r["db_id"], r["span"]): r for r in gold}
    agree_human = sum(1 for r in rows if r["correct"] == "yes")
    agree_gem = 0
    for r in rows:
        g = gmap.get((r["db_id"], r["span"]))
        if g and g["choice"] == r["choice"]:
            agree_gem += 1
    print(f"  Opus agrees with the human answer : {agree_human}/{len(rows)}")
    print(f"  Opus agrees with Gemini's answer  : {agree_gem}/{len(rows)}")
    print("\n  Where Opus sided with the human, Gemini's prompt is the problem.")
    print("  Where both models disagree with the human, re-read the case —")
    print("  the human answer may itself deserve a second look.")
    for r in rows:
        g = gmap.get((r["db_id"], r["span"]))
        print(f"\n    db{r['db_id']:>5s} {r['span']:14s}")
        print(f"      human : {r['gold']}")
        print(f"      gemini: {g['choice'] if g else '?'}")
        print(f"      opus  : {r['choice']} [{r['confidence']}] — {r['reason'][:120]}")


def report_open(rows: list[dict], sample: int) -> None:
    if not rows:
        return
    rule(f"OPEN-SET PROPOSALS  ({len(rows)} spans judged)")
    ch = collections.Counter(
        "proposal" if r["choice"].startswith("HUB-") else r["choice"]
        for r in rows)
    for k, n in ch.most_common():
        print(f"    {k:12s} {n:5d}  ({n * 100 // len(rows)}%)")
    prop = [r for r in rows if r["choice"].startswith("HUB-")]
    hi = [r for r in prop if r["confidence"] == "high"]
    print(f"\n  named a person        : {len(prop)}")
    print(f"    of those, high conf : {len(hi)}")
    free = [r for r in rows if r.get("free_text_name")]
    print(f"  named someone NOT among the candidates: {len(free)}"
          "   (recall gap — a person the surname index missed)")
    gf = sum(1 for r in rows if r["grounding"] == "failed")
    print(f"  grounding failures    : {gf}")

    if sample and prop:
        print(f"\n  RANDOM SAMPLE of {min(sample, len(prop))} proposals to spot-check:")
        random.seed(0)
        for r in random.sample(prop, min(sample, len(prop))):
            print(f"\n    db{r['db_id']:>5s} {r['span']:14s} [{r['confidence']}]"
                  f"  -> {r['proposal_heading']} [{r['proposal_person_db_id']}]")
            print(f"       why : {r['reason'][:170]}")
            print(f"       cite: {r['quote'][:170]}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=20)
    args = ap.parse_args()

    gold = read_tsv(GOLD)
    if gold:
        report_gold(gold)
    else:
        print("no gold eval yet — run: eponym_llm_judge.py --gold")
    report_audit(read_tsv(AUDIT), gold)
    report_open(read_tsv(VERDICTS), args.sample)
    print()


if __name__ == "__main__":
    main()
