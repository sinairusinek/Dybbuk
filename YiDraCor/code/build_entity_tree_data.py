"""Regenerate the entity-tree data (window.KG) in the Lateiner & Hurwitz viz.

The tree tab of docs/Visualizations/lateiner_hurwitz_entities.html renders an
author -> edition -> bucket projection of the entity graph, which it carries as an
embedded `window.KG` blob. That blob was hand-pasted once and then went stale: the
page showed 116 nodes / 283 edges long after the graph had grown past 690 / 880.

This script rebuilds it from data/entity_graph.json, so the tree tab and the
schema tab report the same graph. It is the companion of build_schema_tab.py —
run both after every build_entity_graph.py run.

The projection, and why it is a projection:

  the graph is entity-centric      the page is edition-centric
  -------------------------        --------------------------------------
  person --wrote--> work           an author owns the editions of the works
  work --realised_as--> edition      it wrote
  person --composer--> work         a credit on the work shows on that work's
  org --published--> edition         editions
  edition --printed_in--> place

So a credit reaches an edition through its work. Only works with an edition in
this repo appear; the other ~250 catalogued works have no witness here and so no
row to hang off. Entities are deduplicated per edition and per bucket: one person
credited twice on the same edition is one row carrying n=2.

    python3.11 build_entity_tree_data.py        # from YiDraCor/code/
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent            # YiDraCor/
REPO = ROOT.parent                                       # Dybbuk/
GRAPH = ROOT / "data" / "entity_graph.json"
PAGE = REPO / "docs" / "Visualizations" / "lateiner_hurwitz_entities.html"

# The two playwrights the page is about, in the order it lists them.
PLAYWRIGHTS = ["683", "684"]

# Which bucket each relation lands in. The page renders four; anything reaching
# an edition has to be placed in one of them, or it would silently vanish.
BUCKET = {
    # people credited on the work, or holding the witness itself
    "composer": "people", "composer (txt fr composer)": "people",
    "actor": "people", "actress": "people", "lyrics": "people",
    "arranger": "people", "choreographer": "people", "owned": "people",
    # organisations: who staged it, who holds it, who made the book
    "performed_at": "venues", "holds": "venues",
    "published": "publishers", "printed_by": "publishers",
    # places
    "printed_in": "places", "premiered_in": "places",
}


def edge_index(edges: list[dict]) -> dict[str, list[dict]]:
    by_rel: dict[str, list[dict]] = defaultdict(list)
    for e in edges:
        by_rel[e["rel"]].append(e)
    return by_rel


def build_kg(graph: dict) -> dict:
    nodes = {n["id"]: n for n in graph["nodes"]}
    edges = graph["edges"]
    by_rel = edge_index(edges)

    # --- the spine: author -> work -> edition -------------------------------
    work_editions: dict[str, list[str]] = defaultdict(list)
    edition_works: dict[str, list[str]] = defaultdict(list)
    for e in by_rel["realised_as"]:
        work_editions[e["src"]].append(e["dst"])
        edition_works[e["dst"]].append(e["src"])

    authored: dict[str, list[str]] = defaultdict(list)   # person id -> work ids
    for e in by_rel["wrote"]:
        authored[e["src"]].append(e["dst"])

    # --- every edge that reaches an edition, as a bucket row ----------------
    # rows[edition][bucket][dedup key] = accumulating row
    rows: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(dict))

    def place(edition_id: str, rel: str, node_id: str, attrs: dict) -> None:
        bucket = BUCKET.get(rel)
        if bucket is None or node_id not in nodes:
            return                      # a relation we do not surface on this page
        n = nodes[node_id]
        key = (node_id, rel)
        row = rows[edition_id][bucket].get(key)
        if row is None:
            row = {
                "label": n.get("label"),
                "status": n.get("status") or "LINKED",
                "db_id": n.get("db_id"),
                "matched": n.get("matched"),
                "score": n.get("score"),
                "reason": n.get("reason"),
                "candidates": n.get("candidates"),
                "rel": rel,
                "reviewer": n.get("reviewer"),
                "n": 0,
                "years": [],
                "characters": [],
            }
            rows[edition_id][bucket][key] = row
        row["n"] += 1
        yr = attrs.get("year")
        if yr and yr not in row["years"]:
            row["years"].append(yr)
        ch = attrs.get("character")
        if ch and ch not in row["characters"]:
            row["characters"].append(ch)

    def attrs_of(e: dict) -> dict:
        return {k: v for k, v in e.items() if k not in ("src", "dst", "rel")}

    for e in edges:
        rel, src, dst = e["rel"], e["src"], e["dst"]
        if rel == "realised_as" or rel == "wrote":
            continue                    # the spine itself, not a bucket row
        src_kind = nodes.get(src, {}).get("kind")
        dst_kind = nodes.get(dst, {}).get("kind")

        # edges touching an edition directly
        if dst_kind == "edition":
            place(dst, rel, src, attrs_of(e))
        elif src_kind == "edition":
            place(src, rel, dst, attrs_of(e))
        # edges on a work reach every edition of that work
        elif dst_kind == "work" and src_kind == "person":
            for ed in work_editions.get(dst, ()):
                place(ed, rel, src, attrs_of(e))
        elif src_kind == "work" and dst_kind in ("org", "place"):
            for ed in work_editions.get(src, ()):
                place(ed, rel, dst, attrs_of(e))

    # --- assemble the authors -----------------------------------------------
    def sort_key(r: dict) -> tuple:
        # open questions first, then proposals, then by name: the reader is
        # looking for what still needs a decision.
        rank = {"GAP": 0, "PROPOSED": 1}.get(r["status"], 2)
        return (rank, str(r.get("label") or ""))

    by_db = {str(n.get("db_id")): n for n in graph["nodes"]
             if n.get("kind") == "person" and n.get("db_id")}

    authors = []
    for db_id in PLAYWRIGHTS:
        person = by_db.get(db_id)
        if person is None:
            continue
        seen: set[str] = set()
        eds = []
        for work in authored.get(person["id"], ()):
            for ed_id in work_editions.get(work, ()):
                if ed_id in seen:
                    continue
                seen.add(ed_id)
                ed = nodes[ed_id]
                b = rows.get(ed_id, {})
                eds.append({
                    "label": ed.get("label"),
                    "folder": ed.get("folder"),
                    "expression_id": ed.get("expression_id"),
                    "yiddish": ed.get("yiddish_title"),
                    "year": ed.get("year_printed"),
                    "tkb": ed.get("transkribus_doc_id"),
                    "counts": ed.get("counts") or {},
                    "people": sorted(b.get("people", {}).values(), key=sort_key),
                    "venues": sorted(b.get("venues", {}).values(), key=sort_key),
                    "places": sorted(b.get("places", {}).values(), key=sort_key),
                    "publishers": sorted(b.get("publishers", {}).values(), key=sort_key),
                })
        eds.sort(key=lambda x: str(x["label"] or ""))
        authors.append({
            "label": person.get("label"),
            "db_id": int(db_id) if str(db_id).isdigit() else db_id,
            "editions": eds,
        })

    # --- summary: count what the page actually shows ------------------------
    shown = [r for a in authors for e in a["editions"]
             for b in ("people", "venues", "places", "publishers") for r in e[b]]
    by_status = Counter(r["status"] for r in shown)

    # Entities are counted once per kind across the page, not once per row.
    kinds = {"person": set(), "org": set(), "place": set()}
    for a in authors:
        for e in a["editions"]:
            for b, k in (("people", "person"), ("venues", "org"),
                         ("publishers", "org"), ("places", "place")):
                for r in e[b]:
                    kinds[k].add((r["db_id"], r["label"]))

    n_editions = sum(len(a["editions"]) for a in authors)
    summary = {
        "editions": n_editions,
        "by_kind": {
            "edition": n_editions,
            "person": len(kinds["person"]),
            "org": len(kinds["org"]),
            "place": len(kinds["place"]),
        },
        "by_status": dict(by_status),
        "edges": len(shown),
        "gaps": len(graph.get("gaps", [])),
        "gaps_by_reason": dict(
            Counter(g.get("reason", "") for g in graph.get("gaps", []))),
    }

    gaps = [{"kind": g.get("kind"), "label": g.get("label"),
             "reason": g.get("reason"), "candidates": g.get("candidates") or ""}
            for g in graph.get("gaps", [])]

    return {"summary": summary, "gaps": gaps, "authors": authors}


def main() -> int:
    graph = json.loads(GRAPH.read_text(encoding="utf-8"))
    kg = build_kg(graph)

    page = PAGE.read_text(encoding="utf-8")
    # The blob is one long line ending "};" just before its own </script>.
    pat = re.compile(r"(window\.KG\s*=\s*)(\{[^\n]*\})(;?[ \t]*\n</script>)")
    if not pat.search(page):
        raise SystemExit(f"{PAGE.name}: could not find the window.KG assignment")
    blob = json.dumps(kg, ensure_ascii=False, separators=(",", ":"))
    PAGE.write_text(
        pat.sub(lambda m: m.group(1) + blob + m.group(3), page, count=1),
        encoding="utf-8")

    s = kg["summary"]
    print(f"entity tree data rebuilt: {s['editions']} editions, "
          f"{s['edges']} rows, "
          f"{s['by_kind']['person']}p/{s['by_kind']['org']}o/"
          f"{s['by_kind']['place']}pl, "
          f"{len(kg['gaps'])} gaps -> {PAGE}")
    for a in kg["authors"]:
        print(f"  {a['label']}: {len(a['editions'])} editions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
