"""Integrity checks for the media manifest.

Deliberately reads the TSV as raw text and re-derives everything from the
entity graph and the schema doc, rather than going through common.py — a check
that shares a bug with the writer proves nothing.

Run: python3 DybbukMedia/code/verify_manifest.py
"""
from __future__ import annotations

import collections
import csv
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPO = ROOT.parent
MANIFEST = ROOT / "data" / "media_manifest.tsv"
SCHEMA = ROOT / "docs" / "SCHEMA.md"
SOURCES = ROOT / "data" / "sources.tsv"
GRAPH = REPO / "YiDraCor" / "data" / "entity_graph.json"

LINK_STATUS = {"LINKED", "PROPOSED", "GAP"}
STATUS = {"PENDING", "FETCHED", "CROPPED", "PUBLISHABLE", "REJECTED"}


def main() -> int:
    raw = MANIFEST.read_text(encoding="utf-8")
    lines = raw.splitlines()
    header = lines[0].split("\t")
    rows = list(csv.DictReader(lines, delimiter="\t"))
    fails: list[str] = []

    # The schema doc is the contract; drift either way is a defect.
    documented = set(re.findall(r"^\| `(\w+)` \|", SCHEMA.read_text(encoding="utf-8"),
                                re.M))
    drift = set(header) ^ documented
    if drift:
        fails.append(f"schema drift between header and SCHEMA.md: {sorted(drift)}")

    # A stray tab or newline in a value silently shifts every later column.
    for i, line in enumerate(lines[1:], start=2):
        n = len(line.split("\t"))
        if n != len(header):
            fails.append(f"line {i}: {n} fields, expected {len(header)}")

    dups = [k for k, v in collections.Counter(r["media_id"] for r in rows).items()
            if v > 1]
    if dups:
        fails.append(f"duplicate media_id: {dups[:5]}")
    if any(not r["media_id"] for r in rows):
        fails.append("blank media_id")

    real = {n["id"] for n in json.loads(GRAPH.read_text(encoding="utf-8"))["nodes"]}
    dangling = {e for r in rows for e in r["entity_ids"].split("|")
                if e and e not in real}
    if dangling:
        fails.append(f"dangling entity_ids: {sorted(dangling)[:8]}")

    ids = {r["media_id"] for r in rows}
    for r in rows:
        mid = r["media_id"]
        if r["link_status"] not in LINK_STATUS:
            fails.append(f"{mid}: bad link_status {r['link_status']!r}")
        # The core invariant: an anchor exists iff the row is not a GAP.
        if bool(r["entity_ids"]) == (r["link_status"] == "GAP"):
            fails.append(f"{mid}: anchor/link_status mismatch "
                         f"({r['link_status']}, anchor={bool(r['entity_ids'])})")
        if r["status"] not in STATUS:
            fails.append(f"{mid}: bad status {r['status']!r}")
        if r["crop_of"] and r["crop_of"] not in ids:
            fails.append(f"{mid}: crop_of {r['crop_of']} is not a row")
        # A promotion to LINKED is a decision and must name who made it.
        if r["link_status"] == "LINKED" and not r["reviewer"]:
            fails.append(f"{mid}: LINKED without a reviewer stamp")

    registered = {x["source_id"] for x in
                  csv.DictReader(SOURCES.read_text(encoding="utf-8").splitlines(),
                                 delimiter="\t")}
    unregistered = {r["source"] for r in rows} - registered
    if unregistered:
        fails.append(f"sources absent from sources.tsv: {sorted(unregistered)}")

    anchored = sum(1 for r in rows if r["entity_ids"])
    entities = {e for r in rows for e in r["entity_ids"].split("|") if e}
    print(f"{len(rows)} rows, {len(header)} columns")
    print(f"{anchored} anchored ({100 * anchored // len(rows)}%), "
          f"{len(entities)} entities covered")
    if fails:
        print(f"\nFAIL — {len(fails)} problem(s)")
        for f in fails[:30]:
            print(f"  - {f}")
        return 1
    print("\nok — all checks pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
