"""Regenerate the entity-tree data (window.KG) in the Lateiner & Hurwitz viz.

The tree tab of docs/Visualizations/lateiner_hurwitz_entities.html carries its
data as an embedded `window.KG` blob. This script rebuilds it from
data/entity_graph.json, so the tree tab and the schema tab report the same graph.
It is the companion of build_schema_tab.py — run both after every
build_entity_graph.py run.

THE HIERARCHY IS author -> work -> {editions, events, songs, credits, places}.

That follows the graph's own spine, `person --wrote--> work --realised_as-->
edition`. The work is the unit of authorship; an edition is one printed or
manuscript *witness* of a work, and a work may have none, one, or several.

An earlier version of this page collapsed the work out and hung editions
directly off the author. That was wrong twice over:

  * It hid 251 of the 277 catalogued works, because a work with no witness in
    this repo had nothing to hang off. Lateiner showed 19 of 152 works, Hurwitz
    7 of 125 — and 34 of the hidden ones carry real evidence (songs, events).
  * It misattributed work-level facts to a printing. A premiere, a performance
    event, a song and a composer credit belong to the *play*, not to one
    edition of it; only the imprint facts belong to the edition.

The split is clean in the data, and this is the rule the projection follows:

  work-level    wrote, composer, actor, actress, lyrics, arranger,
                choreographer, performed_at, premiered_in, of_work, part_of
  edition-level published, printed_by, printed_in, holds, owned

Entities are deduplicated per work and per bucket: one person credited twice on
the same work is one row carrying n=2.

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

# Credits and venues that belong to the WORK.
WORK_BUCKET = {
    "composer": "people", "composer (txt fr composer)": "people",
    "actor": "people", "actress": "people", "lyrics": "people",
    "arranger": "people", "choreographer": "people",
    "performed_at": "venues",
    "premiered_in": "places",
}

# Imprint facts that belong to a single EDITION.
EDITION_BUCKET = {
    "published": "publishers", "printed_by": "publishers",
    "holds": "venues",
    "owned": "people",
    "printed_in": "places",
}


def _row(node: dict, rel: str) -> dict:
    return {
        "label": node.get("label"),
        "status": node.get("status") or "LINKED",
        "db_id": node.get("db_id"),
        "matched": node.get("matched"),
        "score": node.get("score"),
        "reason": node.get("reason"),
        "candidates": node.get("candidates"),
        "rel": rel,
        "reviewer": node.get("reviewer"),
        "n": 0,
        "years": [],
        "characters": [],
    }


def _accumulate(store: dict, nodes: dict, owner: str, rel: str,
                node_id: str, attrs: dict, bucket_map: dict) -> bool:
    """Place one edge in its bucket. Returns False if the relation is unmapped."""
    bucket = bucket_map.get(rel)
    if bucket is None:
        return False
    if node_id not in nodes:
        return True                     # mapped, but the endpoint is missing
    row = store[owner][bucket].get((node_id, rel))
    if row is None:
        row = _row(nodes[node_id], rel)
        store[owner][bucket][(node_id, rel)] = row
    row["n"] += 1
    yr = attrs.get("year")
    if yr and yr not in row["years"]:
        row["years"].append(yr)
    ch = attrs.get("character")
    if ch and ch not in row["characters"]:
        row["characters"].append(ch)
    return True


def build_kg(graph: dict) -> dict:
    nodes = {n["id"]: n for n in graph["nodes"]}
    edges = graph["edges"]

    authored: dict[str, list[str]] = defaultdict(list)
    work_editions: dict[str, list[str]] = defaultdict(list)
    work_events: dict[str, list[str]] = defaultdict(list)
    work_songs: dict[str, list[str]] = defaultdict(list)
    for e in edges:
        rel = e["rel"]
        if rel == "wrote":
            authored[e["src"]].append(e["dst"])
        elif rel == "realised_as":
            work_editions[e["src"]].append(e["dst"])
        elif rel == "of_work":
            work_events[e["dst"]].append(e["src"])
        elif rel == "part_of":
            work_songs[e["dst"]].append(e["src"])

    # buckets keyed by the thing they describe: a work id, or an edition id
    w_rows: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(dict))
    e_rows: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(dict))

    # The spine itself, and the containment edges rendered as nested nodes.
    STRUCTURAL = {"wrote", "realised_as", "of_work", "part_of"}
    unmapped: Counter = Counter()

    for e in edges:
        rel, src, dst = e["rel"], e["src"], e["dst"]
        if rel in STRUCTURAL:
            continue
        attrs = {k: v for k, v in e.items() if k not in ("src", "dst", "rel")}
        sk = nodes.get(src, {}).get("kind")
        dk = nodes.get(dst, {}).get("kind")

        placed = False
        if dk == "edition":
            placed = _accumulate(e_rows, nodes, dst, rel, src, attrs, EDITION_BUCKET)
        elif sk == "edition":
            placed = _accumulate(e_rows, nodes, src, rel, dst, attrs, EDITION_BUCKET)
        elif dk == "work":
            placed = _accumulate(w_rows, nodes, dst, rel, src, attrs, WORK_BUCKET)
        elif sk == "work":
            placed = _accumulate(w_rows, nodes, src, rel, dst, attrs, WORK_BUCKET)
        elif sk == "event" and dk == "org":
            # an event's venue is evidence about the work it staged
            for w in (nodes.get(src, {}).get("work_id"),):
                if w in nodes:
                    placed = _accumulate(w_rows, nodes, w, rel, dst, attrs,
                                         WORK_BUCKET)
        if not placed:
            unmapped[(rel, sk, dk)] += 1

    if unmapped:
        raise SystemExit(
            "unmapped relations reaching the tree — add them to a bucket:\n  "
            + "\n  ".join(f"{r}: {s} -> {d} ({n})"
                          for (r, s, d), n in unmapped.most_common()))

    def sort_key(r: dict) -> tuple:
        # open questions first: the reader is looking for what needs a decision
        return ({"GAP": 0, "PROPOSED": 1}.get(r["status"], 2),
                str(r.get("label") or ""))

    def buckets_of(store: dict, key: str) -> dict:
        b = store.get(key, {})
        return {name: sorted(b.get(name, {}).values(), key=sort_key)
                for name in ("people", "venues", "places", "publishers")}

    def edition_of(ed_id: str) -> dict:
        ed = nodes[ed_id]
        return {
            "label": ed.get("label"),
            "folder": ed.get("folder"),
            "year": ed.get("year_printed"),
            "tkb": ed.get("transkribus_doc_id"),
            "counts": ed.get("counts") or {},
            **buckets_of(e_rows, ed_id),
        }

    def event_of(ev_id: str) -> dict:
        ev = nodes[ev_id]
        return {"label": ev.get("label"), "date": ev.get("date"),
                "event_type": ev.get("event_type")}

    def song_of(s_id: str) -> dict:
        s = nodes[s_id]
        return {"label": s.get("label"),
                "romanized": s.get("romanized_title"),
                "n_attestations": s.get("n_attestations"),
                "source": s.get("source_publication")}

    by_db = {str(n.get("db_id")): n for n in graph["nodes"]
             if n.get("kind") == "person" and n.get("db_id")}

    authors = []
    for db_id in PLAYWRIGHTS:
        person = by_db.get(db_id)
        if person is None:
            continue
        works = []
        for w_id in dict.fromkeys(authored.get(person["id"], ())):
            w = nodes[w_id]
            evs = sorted(work_events.get(w_id, ()),
                         key=lambda i: str(nodes[i].get("date") or ""))
            sgs = sorted(work_songs.get(w_id, ()),
                         key=lambda i: str(nodes[i].get("label") or ""))
            eds = sorted(work_editions.get(w_id, ()),
                         key=lambda i: str(nodes[i].get("label") or ""))
            works.append({
                "label": w.get("label"),
                "yiddish": w.get("yiddish_title"),
                "expression_id": w.get("expression_id"),
                "genre": w.get("genre") or w.get("tags"),
                "attribution": w.get("attribution"),
                "attribution_note": w.get("attribution_note"),
                "editions": [edition_of(i) for i in eds],
                "events": [event_of(i) for i in evs],
                "songs": [song_of(i) for i in sgs],
                **buckets_of(w_rows, w_id),
            })
        # Works with a witness first, then by title: the ones a reader can open.
        works.sort(key=lambda x: (not x["editions"], str(x["label"] or "")))
        authors.append({
            "label": person.get("label"),
            "db_id": int(db_id) if str(db_id).isdigit() else db_id,
            "works": works,
        })

    # --- summary -------------------------------------------------------------
    all_rows = [r for a in authors for w in a["works"]
                for src in (w, *w["editions"])
                for b in ("people", "venues", "places", "publishers")
                for r in src[b]]
    kinds = {"person": set(), "org": set(), "place": set()}
    for a in authors:
        for w in a["works"]:
            for src in (w, *w["editions"]):
                for b, k in (("people", "person"), ("venues", "org"),
                             ("publishers", "org"), ("places", "place")):
                    for r in src[b]:
                        kinds[k].add((r["db_id"], r["label"]))

    n_works = sum(len(a["works"]) for a in authors)
    n_eds = sum(len(w["editions"]) for a in authors for w in a["works"])
    n_evs = sum(len(w["events"]) for a in authors for w in a["works"])
    n_sgs = sum(len(w["songs"]) for a in authors for w in a["works"])

    summary = {
        "works": n_works,
        "editions": n_eds,
        "events": n_evs,
        "songs": n_sgs,
        "by_kind": {
            "work": n_works, "edition": n_eds,
            "person": len(kinds["person"]),
            "org": len(kinds["org"]),
            "place": len(kinds["place"]),
        },
        "by_status": dict(Counter(r["status"] for r in all_rows)),
        "edges": len(all_rows),
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
    print(f"entity tree data rebuilt: {s['works']} works, {s['editions']} editions, "
          f"{s['events']} events, {s['songs']} songs, {s['edges']} rows, "
          f"{len(kg['gaps'])} gaps -> {PAGE}")
    for a in kg["authors"]:
        wit = sum(1 for w in a["works"] if w["editions"])
        print(f"  {a['label']}: {len(a['works'])} works ({wit} with an edition)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
