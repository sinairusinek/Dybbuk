"""Structural invariants of the work/song/event layers.

Reads the built graph and re-derives every check from the node kinds and edge
relations, independently of the builder that wrote it.

Run: python3 YiDraCor/code/test_work_layer.py
"""
from __future__ import annotations

import collections
import json
import sys
from pathlib import Path

GRAPH = Path(__file__).resolve().parent.parent / "data" / "entity_graph.json"

# A fact about the PLAY. Hanging one of these on a printing is the defect this
# layer exists to fix: it made the graph claim Mishke Mashke was performed in
# 1889 by a book printed in 1911.
WORK_RELS = {"wrote", "composer", "lyrics", "arranger", "choreographer",
             "actor", "actress", "performed_at", "premiered_in",
             "composer (txt fr composer)"}
# A fact about the physical book.
ITEM_RELS = {"published", "printed_in", "holds"}


def main() -> int:
    g = json.loads(GRAPH.read_text(encoding="utf-8"))
    nodes = {n["id"]: n for n in g["nodes"]}
    edges = g["edges"]
    fails: list[str] = []

    for e in edges:
        if e["src"] not in nodes or e["dst"] not in nodes:
            fails.append(f"dangling edge {e['src']} -> {e['dst']}")

    for e in edges:
        kinds = {nodes[e["src"]]["kind"], nodes[e["dst"]]["kind"]}
        if e["rel"] in WORK_RELS and "edition" in kinds:
            fails.append(f"work-level '{e['rel']}' touches an edition: "
                         f"{e['src']} -> {e['dst']}")
        if e["rel"] in ITEM_RELS and "edition" not in kinds:
            fails.append(f"item-level '{e['rel']}' touches no edition: "
                         f"{e['src']} -> {e['dst']}")

    # The WEMI spine: one work per edition, exactly.
    realised = collections.Counter(e["dst"] for e in edges
                                   if e["rel"] == "realised_as")
    for n in (x for x in g["nodes"] if x["kind"] == "edition"):
        if realised.get(n["id"], 0) != 1:
            fails.append(f"edition {n['id']} realises "
                         f"{realised.get(n['id'], 0)} works, expected 1")

    # Every performance event names the work it staged.
    of_work = collections.Counter(e["src"] for e in edges
                                  if e["rel"] == "of_work")
    for n in (x for x in g["nodes"] if x["kind"] == "event"):
        if of_work.get(n["id"], 0) != 1:
            fails.append(f"event {n['id']} has {of_work.get(n['id'], 0)} "
                         f"of_work edges, expected 1")

    # A song is part_of at most one play.
    part_of = collections.Counter(e["src"] for e in edges
                                  if e["rel"] == "part_of")
    for sid, n in part_of.items():
        if n > 1:
            fails.append(f"song {sid} is part_of {n} works")

    # Works are minted from the catalogue, so a work node must carry its id.
    for n in (x for x in g["nodes"] if x["kind"] == "work"):
        if not n.get("expression_id"):
            fails.append(f"work {n['id']} has no expression_id")
        if n.get("attribution") not in {"certain", "uncertain",
                                        "false ascription", "error",
                                        "uncertain/false?", "unknown"}:
            fails.append(f"work {n['id']} has attribution "
                         f"{n.get('attribution')!r}")

    kinds = collections.Counter(n["kind"] for n in g["nodes"])
    print(f"{len(g['nodes'])} nodes, {len(edges)} edges")
    print(f"kinds: {dict(kinds)}")
    if fails:
        print(f"\nFAIL — {len(fails)} problem(s)")
        for f in fails[:25]:
            print(f"  - {f}")
        return 1
    print("\nok — work/song/event invariants hold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
