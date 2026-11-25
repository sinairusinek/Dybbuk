#!/usr/bin/env python3
"""Export typed-link-review decisions from the Claude artifact into lp_review_decisions.tsv.

The review runs in a published artifact (reviewable from any device while travelling)
instead of the local Zalmen page; this turns its stored decisions into exactly the TSV
that Zylbercweig/zalmen/views/lp_review.py would have written, so Tomer's pipeline
reads it unchanged.

Every reviewer's answers are kept: the store holds one document per
item+reviewer, so two people deciding the same item do not overwrite each
other. By default this writes ONE row per item -- the primary reviewer's
answer, or, failing that, the earliest -- which is the shape lp_review.py
produces and Tomer's pipeline reads. Pass --all-reviewers to emit every
answer (the agreement set), and --disagreements to see where they differ.

Usage:
    # 1. dump the artifact's `decisions` collection to a directory of JSON files
    #    (via Claude: ArtifactData action=list, collection=decisions, out_dir=DIR)
    # 2. then:
    python3 export_lp_review_from_artifact.py DIR [-o lp_review_decisions.tsv]
    python3 export_lp_review_from_artifact.py DIR --primary Sinai
    python3 export_lp_review_from_artifact.py DIR --all-reviewers -o agreement.tsv
    python3 export_lp_review_from_artifact.py DIR --disagreements
"""
from __future__ import annotations

import argparse
import csv
import json
import pathlib
import sys

FIELDS = ["item", "mention_id", "mention_name", "host_hub_id", "decision",
          "target_hub_id", "kin_relation", "confidence", "reviewer",
          "reviewed_at", "notes"]
VALID = {"same", "kin", "new", "not_person", "unsure"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("src", type=pathlib.Path,
                    help="directory of per-document JSON files, or a single JSON file")
    ap.add_argument("-o", "--out", type=pathlib.Path,
                    default=pathlib.Path(__file__).with_name("lp_review_decisions.tsv"))
    ap.add_argument("--primary", default="",
                    help="reviewer whose answer wins when several decided one item "
                         "(case-insensitive; default: the earliest answer)")
    ap.add_argument("--all-reviewers", action="store_true",
                    help="emit every reviewer's answer, not one row per item")
    ap.add_argument("--disagreements", action="store_true",
                    help="report items where reviewers chose differently, then exit")
    a = ap.parse_args()

    raw = []
    if a.src.is_dir():
        for p in sorted(a.src.rglob("*.json")):
            raw.append(json.loads(p.read_text(encoding="utf-8")))
    else:
        obj = json.loads(a.src.read_text(encoding="utf-8"))
        raw = obj if isinstance(obj, list) else [obj]

    answers, bad = [], []
    for r in raw:
        # accept either the bare document body or the {id, data, version} wrapper
        d = r.get("data") if isinstance(r.get("data"), dict) else r
        item = d.get("item")
        if not item or str(item).startswith("PROBE-"):
            continue
        if d.get("decision") not in VALID:
            bad.append((item, d.get("decision")))
            continue
        if d["decision"] in ("same", "kin") and not d.get("target_hub_id"):
            bad.append((item, d["decision"] + " without target_hub_id"))
            continue
        if d["decision"] == "kin" and not d.get("kin_relation"):
            bad.append((item, "kin without kin_relation"))
            continue
        answers.append({k: str(d.get(k, "") or "").replace("\t", " ").replace("\n", " ")
                        for k in FIELDS})

    # group every reviewer's answers per item
    per_item: dict[str, list[dict]] = {}
    for v in answers:
        per_item.setdefault(v["item"], []).append(v)
    for vs in per_item.values():
        vs.sort(key=lambda v: v["reviewed_at"])

    def verdict(v: dict) -> tuple:
        """What two reviewers must match on to count as agreeing."""
        return (v["decision"], v["target_hub_id"], v["kin_relation"])

    contested = {it: vs for it, vs in per_item.items()
                 if len({verdict(v) for v in vs}) > 1}
    doubled = {it: vs for it, vs in per_item.items() if len(vs) > 1}

    if a.disagreements:
        reviewers = sorted({v["reviewer"] for v in answers})
        print(f"{len(answers)} answers from {len(reviewers)} reviewer(s): "
              f"{', '.join(reviewers) or '(none)'}")
        print(f"{len(per_item)} items decided; {len(doubled)} decided by more than one")
        if doubled:
            agreed = len(doubled) - len(contested)
            print(f"agreement on doubly-decided items: {agreed}/{len(doubled)}"
                  f" ({100*agreed/len(doubled):.0f}%)")
        if not contested:
            print("\nNo disagreements.")
            return 0
        print(f"\n{len(contested)} item(s) where reviewers differ:")
        for it in sorted(contested):
            print(f"\n  {it}  ({contested[it][0]['mention_name']})")
            for v in contested[it]:
                tgt = v["target_hub_id"] or "-"
                kin = f" [{v['kin_relation']}]" if v["kin_relation"] else ""
                print(f"    {v['reviewer']:<14} {v['decision']:<11} {tgt}{kin}"
                      f"  {v['confidence']}  {v['notes']}")
        return 0

    if a.all_reviewers:
        out_rows = sorted(answers, key=lambda v: (v["item"], v["reviewer"]))
    else:
        want = a.primary.strip().lower()
        out_rows = []
        for it in sorted(per_item):
            vs = per_item[it]
            pick = next((v for v in vs if v["reviewer"].lower() == want), vs[0]) if want else vs[0]
            out_rows.append(pick)

    a.out.parent.mkdir(parents=True, exist_ok=True)
    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           extrasaction="ignore", quoting=csv.QUOTE_NONE, escapechar="\\")
        w.writeheader()
        for v in out_rows:
            w.writerow(v)

    print(f"{len(out_rows)} rows -> {a.out}"
          f"  ({len(per_item)} items, {len(answers)} answers in the store)")
    counts: dict[str, int] = {}
    for v in out_rows:
        counts[v["decision"]] = counts.get(v["decision"], 0) + 1
    for k in sorted(counts):
        print(f"  {k:<11} {counts[k]}")
    by_rev: dict[str, int] = {}
    for v in answers:
        by_rev[v["reviewer"]] = by_rev.get(v["reviewer"], 0) + 1
    if len(by_rev) > 1:
        print("  answers per reviewer: "
              + ", ".join(f"{k} {n}" for k, n in sorted(by_rev.items())))
    if doubled and not a.all_reviewers:
        print(f"  {len(doubled)} item(s) decided by more than one reviewer; "
              f"{len(contested)} disagree. Kept one row each"
              + (f" (preferring {a.primary})" if a.primary else " (earliest)")
              + ". Use --disagreements to inspect, --all-reviewers to export all.")
    if bad:
        print(f"\n{len(bad)} rows skipped as malformed:", file=sys.stderr)
        for item, why in bad[:20]:
            print(f"  {item}: {why}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
