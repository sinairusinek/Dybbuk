#!/usr/bin/env python3
"""Export typed-link-review decisions from the Claude artifact into lp_review_decisions.tsv.

The review runs in a published artifact (reviewable from any device while travelling)
instead of the local Zalmen page; this turns its stored decisions into exactly the TSV
that Zylbercweig/zalmen/views/lp_review.py would have written, so Tomer's pipeline
reads it unchanged.

Usage:
    # 1. dump the artifact's `decisions` collection to a directory of JSON files
    #    (via Claude: ArtifactData action=list, collection=decisions, out_dir=DIR)
    # 2. then:
    python3 export_lp_review_from_artifact.py DIR [-o lp_review_decisions.tsv]
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
    a = ap.parse_args()

    raw = []
    if a.src.is_dir():
        for p in sorted(a.src.rglob("*.json")):
            raw.append(json.loads(p.read_text(encoding="utf-8")))
    else:
        obj = json.loads(a.src.read_text(encoding="utf-8"))
        raw = obj if isinstance(obj, list) else [obj]

    rows, bad = {}, []
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
        rows[item] = {k: str(d.get(k, "") or "").replace("\t", " ").replace("\n", " ")
                      for k in FIELDS}

    a.out.parent.mkdir(parents=True, exist_ok=True)
    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           extrasaction="ignore", quoting=csv.QUOTE_NONE, escapechar="\\")
        w.writeheader()
        for item in sorted(rows):
            w.writerow(rows[item])

    print(f"{len(rows)} decisions -> {a.out}")
    counts = {}
    for v in rows.values():
        counts[v["decision"]] = counts.get(v["decision"], 0) + 1
    for k in sorted(counts):
        print(f"  {k:<11} {counts[k]}")
    if bad:
        print(f"\n{len(bad)} rows skipped as malformed:", file=sys.stderr)
        for item, why in bad[:20]:
            print(f"  {item}: {why}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
