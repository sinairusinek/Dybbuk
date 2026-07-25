"""B2 — Assemble the knowledge graph from linked facts.

Inputs: kg_facts_linked.tsv (B1), plays_db.tsv (A1), the curated registries
(labels only), YiDraCor editions.csv/json (play->edition link ONLY — no
catalogue facts are ingested; the catalogue is the held-out evaluation
reference).

Model (Linked Stage Graph / APIS pattern):
  - production-event nodes (kg/events.tsv) for facts anchored to a venue,
    org, place or date; cast attaches to the event
  - plain typed binary edges ("mini-events") for facts with no anchor
  - materialized binary shortcut edges (performed_in, directed, ...) are
    ALSO emitted for event-derived participation, marked derived_from_event
  - nothing is dropped: unmatched surfaces mint unlinked nodes (person:UP-,
    org:UO-, venue:UV-, place:UPL-, play:NEW-)

Adjudication: decisions in kg_link_review.tsv (decision=ALIGN + decided_link)
override the automatic candidate/unmatched resolutions at build time.

Usage:
    python3.11 build_kg.py            # dry-run stats
    python3.11 build_kg.py --execute
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict

import plays_common as pc

LINKED_TSV = pc.HERE / "kg_facts_linked.tsv"
NODES_TSV = pc.KG_DIR / "nodes.tsv"
EDGES_TSV = pc.KG_DIR / "edges.tsv"
EVENTS_TSV = pc.KG_DIR / "events.tsv"

NODE_FIELDS = ["node_id", "node_type", "label_yiddish", "label_english",
               "ext_ref_type", "ext_ref_id", "secondary_ids", "match_status", "notes"]
EDGE_FIELDS = ["edge_id", "source_id", "target_id", "edge_type", "role_detail",
               "character", "date_start", "date_end", "date_precision",
               "event_id", "production_key", "provenance_person_id",
               "provenance_fact_ids", "evidence_sentence", "extraction_model",
               "confidence", "match_status", "review_status"]
EVENT_FIELDS = ["event_id", "event_type", "play_id", "venue_id", "org_id",
                "place_id", "date_start", "date_end", "date_precision",
                "production_key", "provenance_person_id", "provenance_fact_ids",
                "evidence_sentence", "extraction_model", "confidence", "review_status"]

ROLE_EDGE = {"actor": "performed_in", "director": "directed",
             "composer": "composed_music", "prompter": "prompted",
             "translator": "translated_adapted", "adapter": "translated_adapted",
             "producer": "produced", "author": "authored", "other": "involved_in",
             "": "involved_in"}


class Graph:
    def __init__(self) -> None:
        self.nodes: dict[str, dict] = {}
        self.edges: list[dict] = []
        self.events: list[dict] = []
        self._minted: dict[tuple[str, str], str] = {}
        self._mint_seq: Counter = Counter()

    def add_node(self, node_id: str, **kw) -> str:
        n = self.nodes.get(node_id)
        if n:
            for k, v in kw.items():
                if v and not n.get(k):
                    n[k] = v
        else:
            self.nodes[node_id] = {"node_id": node_id, **kw}
        return node_id

    def mint(self, prefix: str, norm_key: str, **kw) -> str:
        key = (prefix, norm_key)
        if key not in self._minted:
            self._mint_seq[prefix] += 1
            self._minted[key] = f"{prefix}-{self._mint_seq[prefix]:04d}"
            self.add_node(self._minted[key], **kw)
        else:
            self.add_node(self._minted[key], **kw)
        return self._minted[key]

    def add_edge(self, **kw) -> None:
        kw.setdefault("review_status", "auto")
        self.edges.append(kw)


def load_registry_labels():
    people = {r["db_id"]: r for r in pc.read_tsv(pc.PEOPLE_DB_TSV) if r.get("db_id")}
    orgs = {r["db_id"]: r for r in pc.read_tsv(pc.ORGS_DIR / "core_db.tsv") if r.get("db_id")}
    clusters = {}
    for r in pc.read_tsv(pc.ORGS_DIR / "org_alignment_review.tsv"):
        if r.get("cluster_id"):
            clusters[r["cluster_id"]] = r
    places = {}
    with open(pc.ZIBN_WORKING / "toponyms_gazetteer.csv", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if r.get("qid"):
                places[r["qid"]] = r
    return people, orgs, clusters, places


def resolve_with_adjudication(row: dict, slot: str, review: dict) -> tuple[str, str]:
    """(link, match_status) after applying kg_link_review decisions."""
    link = row.get(f"{slot}_link", "")
    status = row.get(f"{slot}_link_status", "")
    surf_col = {"person": "person_surface", "org": "org_surface",
                "venue": "venue_surface", "place": "settlement_surface",
                "play": "play_title_surface"}[slot]
    dec = review.get((slot, row.get(surf_col, "")))
    if dec and dec.get("decision") == "ALIGN" and dec.get("decided_link"):
        return dec["decided_link"], "matched"
    if dec and dec.get("decision") == "REJECT":
        return "", "unmatched"
    return link, status


def ensure_endpoint(g: Graph, link: str, status: str, slot: str, surface: str,
                    labels) -> str:
    """Return a node_id for this slot (minting an unlinked node if needed)."""
    people, orgs, clusters, places = labels
    if link and status in ("matched", "candidate") and "|" not in link:
        ns, _, ref = link.partition(":")
        if ns == "person":
            p = people.get(ref, {})
            g.add_node(link, node_type="person", label_yiddish=p.get("hebname", surface),
                       label_english=p.get("english", ""), ext_ref_type="people_db",
                       ext_ref_id=ref, match_status=status)
        elif ns == "person_entry":
            g.add_node(link, node_type="person", label_yiddish=surface,
                       ext_ref_type="entry_person_id", ext_ref_id=ref,
                       match_status=status)
        elif ns == "org":
            o = orgs.get(ref, {})
            g.add_node(link, node_type="org", label_yiddish=o.get("name_yiddish", surface),
                       label_english=o.get("name", ""), ext_ref_type="org_core_db",
                       ext_ref_id=ref, match_status=status)
        elif ns == "org_cluster":
            c = clusters.get(ref, {})
            g.add_node(link, node_type="org", label_yiddish=c.get("canonical_yiddish", surface),
                       ext_ref_type="org_cluster", ext_ref_id=ref, match_status=status)
        elif ns == "place":
            pl = places.get(ref, {})
            sec = json.dumps({k: pl.get(k, "") for k in ("kima_id", "lat", "lon")},
                             ensure_ascii=False) if pl else ""
            g.add_node(link, node_type="place", label_yiddish=pl.get("label_yi", surface),
                       label_english=pl.get("label_en", ""), ext_ref_type="wikidata_qid",
                       ext_ref_id=ref, secondary_ids=sec, match_status=status)
        elif ns == "play":
            return link  # play nodes are added from the registry
        return link
    # unmatched (or multi-candidate, which stays unresolved) -> mint
    if not surface.strip():
        return ""
    prefix = {"person": "person:UP", "org": "org:UO", "venue": "venue:UV",
              "place": "place:UPL", "play": "play:NEW"}[slot]
    node_type = {"person": "person", "org": "org", "venue": "venue",
                 "place": "place", "play": "play"}[slot]
    notes = f"auto_candidates:{link}" if link else ""
    return g.mint(prefix, pc.norm_yiddish(surface) or surface,
                  node_type=node_type, label_yiddish=surface,
                  match_status="unmatched", notes=notes)


def event_key(play: str, venue: str, org: str, place: str, date: str) -> str:
    year = (date or "")[:4]
    raw = "|".join([play, venue or org, place, year])
    return hashlib.sha1(raw.encode()).hexdigest()[:10]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    rows = pc.read_tsv(LINKED_TSV)
    if not rows:
        raise SystemExit("kg_facts_linked.tsv missing — run link_entities.py --execute first")
    review = {(r["slot"], r["surface"]): r for r in pc.read_tsv(pc.LINK_REVIEW_TSV)}
    labels = load_registry_labels()
    people, orgs, clusters, places = labels
    plays = pc.load_plays_db()

    g = Graph()

    # Registry play nodes + authored edges (lexicon-derived created_expressions).
    for p in plays:
        pid = f"play:{p['play_id']}"
        g.add_node(pid, node_type="play", label_yiddish=p["title_yiddish"],
                   ext_ref_type="plays_db", ext_ref_id=p["play_id"],
                   match_status="matched",
                   notes=("disputed_attribution" if p["attribution_status"] == "disputed" else ""))
        author_node = f"person:{p['author_db_id']}"
        pr = people.get(p["author_db_id"], {})
        g.add_node(author_node, node_type="person", label_yiddish=pr.get("hebname", ""),
                   label_english=pr.get("english", ""), ext_ref_type="people_db",
                   ext_ref_id=p["author_db_id"], match_status="matched")
        g.add_edge(source_id=pid, target_id=author_node, edge_type="authored_by",
                   provenance_fact_ids="", evidence_sentence="",
                   extraction_model="registry",
                   provenance_person_id="", confidence="high", match_status="matched",
                   role_detail="", character="", date_start="", date_end="",
                   date_precision="", event_id="", production_key="")

    # Play -> YiDraCor edition links (link only; no catalogue facts ingested).
    seg_index: dict[str, list[dict]] = defaultdict(list)
    for p in plays:
        for seg in (p["title_segments_norm"] or "").split("|"):
            if seg:
                seg_index[seg].append(p)
    seen_docs = set()
    ed_rows = pc.read_tsv(pc.YIDRACOR_DATA / "editions.csv")
    if not ed_rows:
        with open(pc.YIDRACOR_DATA / "editions.csv", encoding="utf-8-sig") as f:
            ed_rows = list(csv.DictReader(f))
    n_ed_links = 0
    for ed in ed_rows:
        doc = (ed.get("transkribus_doc_id") or "").strip().split(".")[0]
        title = (ed.get("title") or "").strip()
        if not doc or doc in seen_docs:
            continue
        seen_docs.add(doc)
        cand_titles = [ed.get("alt_titles", ""), title]
        # editions.json carries the Yiddish names for the print editions
        matches: list[dict] = []
        for t in cand_titles:
            for seg in pc.title_segments(t):
                matches.extend(seg_index.get(seg, []))
        node_ed = f"edition:tkb_{doc}"
        ed_author = (ed.get("author") or "").strip()
        if matches:
            for p in {m["play_id"]: m for m in matches}.values():
                author_ok = (("Lateiner" in ed_author and p["author_db_id"] == "683")
                             or ("Hurwitz" in ed_author and p["author_db_id"] == "684"))
                g.add_node(node_ed, node_type="edition", label_english=title,
                           ext_ref_type="transkribus_doc", ext_ref_id=doc,
                           match_status="matched")
                g.add_edge(source_id=f"play:{p['play_id']}", target_id=node_ed,
                           edge_type="published_as",
                           confidence="high" if author_ok else "low",
                           match_status="matched" if author_ok else "candidate",
                           extraction_model="title_join", provenance_fact_ids="",
                           provenance_person_id="", evidence_sentence="",
                           role_detail="", character="", date_start="", date_end="",
                           date_precision="", event_id="", production_key="")
                n_ed_links += 1

    # Yiddish titles for print editions live in editions.json.
    try:
        ed_json = json.loads((pc.YIDRACOR_DATA / "editions.json").read_text(encoding="utf-8"))
        for e in ed_json.get("editions", []):
            doc = str(e.get("transkribus_doc_id") or "")
            names = [e.get("catalogue_yiddish_name") or "", e.get("title") or ""]
            matches = []
            for t in names:
                for seg in pc.title_segments(t):
                    matches.extend(seg_index.get(seg, []))
            for p in {m["play_id"]: m for m in matches}.values():
                node_ed = f"edition:tkb_{doc}"
                exists = any(ed2["source_id"] == f"play:{p['play_id']}"
                             and ed2["target_id"] == node_ed
                             for ed2 in g.edges if ed2["edge_type"] == "published_as")
                if exists:
                    continue
                g.add_node(node_ed, node_type="edition",
                           label_english=e.get("title", ""),
                           ext_ref_type="transkribus_doc", ext_ref_id=doc,
                           secondary_ids=json.dumps(
                               {"expression_id": e.get("expression_id", "")}),
                           match_status="matched")
                author_ok = p["author_db_id"] == "683"  # all print editions are Lateiner
                g.add_edge(source_id=f"play:{p['play_id']}", target_id=node_ed,
                           edge_type="published_as",
                           confidence="high" if author_ok else "low",
                           match_status="matched" if author_ok else "candidate",
                           extraction_model="title_join", provenance_fact_ids="",
                           provenance_person_id="", evidence_sentence="",
                           role_detail="", character="", date_start="", date_end="",
                           date_precision="", event_id="", production_key="")
                n_ed_links += 1
    except FileNotFoundError:
        pass

    # ---- facts -> events + edges ----
    ev_by_key: dict[str, dict] = {}
    n_binary = n_event_facts = 0
    for r in rows:
        play_link, play_st = resolve_with_adjudication(r, "play", review)
        person_link, person_st = resolve_with_adjudication(r, "person", review)
        org_link, org_st = resolve_with_adjudication(r, "org", review)
        venue_link, venue_st = resolve_with_adjudication(r, "venue", review)
        place_link, place_st = resolve_with_adjudication(r, "place", review)

        play_node = ensure_endpoint(g, play_link, play_st, "play",
                                    r.get("play_title_surface", ""), labels)
        person_node = ensure_endpoint(g, person_link, person_st, "person",
                                      r.get("person_surface", ""), labels) \
            if r.get("person_surface") else ""
        org_node = ensure_endpoint(g, org_link, org_st, "org",
                                   r.get("org_surface", ""), labels) \
            if r.get("org_surface") else ""
        venue_node = ensure_endpoint(g, venue_link, venue_st, "venue",
                                     r.get("venue_surface", ""), labels) \
            if r.get("venue_surface") else ""
        place_node = ensure_endpoint(g, place_link, place_st, "place",
                                     r.get("settlement_surface", ""), labels) \
            if r.get("settlement_surface") else ""

        statuses = [st for st in (play_st, person_st, org_st, venue_st, place_st) if st]
        overall = ("unmatched" if "unmatched" in statuses
                   else "candidate" if "candidate" in statuses else "matched")
        common = dict(
            date_start=r.get("date_start", ""), date_end=r.get("date_end", ""),
            date_precision=r.get("date_precision", ""),
            provenance_person_id=r.get("person_id", ""),
            provenance_fact_ids=r.get("fact_id", ""),
            evidence_sentence=r.get("evidence_quote", ""),
            extraction_model=r.get("model", ""),
            confidence=r.get("confidence", ""), match_status=overall,
        )
        ftype = r.get("fact_type", "")
        role = r.get("person_role", "")
        anchored = ftype in ("production", "premiere") and (
            venue_node or org_node or place_node or r.get("date_start"))

        if anchored and play_node:
            key = event_key(play_node, venue_node, org_node, place_node,
                            r.get("date_start", ""))
            ev = ev_by_key.get(key)
            if not ev:
                ev = {
                    "event_id": f"event:EV-{len(ev_by_key) + 1:04d}",
                    "event_type": "premiere" if ftype == "premiere" else "production",
                    "play_id": play_node, "venue_id": venue_node, "org_id": org_node,
                    "place_id": place_node, "date_start": r.get("date_start", ""),
                    "date_end": r.get("date_end", ""),
                    "date_precision": r.get("date_precision", ""),
                    "production_key": key,
                    "provenance_person_id": r.get("person_id", ""),
                    "provenance_fact_ids": r.get("fact_id", ""),
                    "evidence_sentence": r.get("evidence_quote", ""),
                    "extraction_model": r.get("model", ""),
                    "confidence": r.get("confidence", ""), "review_status": "auto",
                }
                ev_by_key[key] = ev
            else:
                if ftype == "premiere":
                    ev["event_type"] = "premiere"
                if r.get("fact_id") not in ev["provenance_fact_ids"]:
                    ev["provenance_fact_ids"] += f"|{r.get('fact_id')}"
                for col, val in (("date_start", r.get("date_start", "")),
                                 ("date_precision", r.get("date_precision", ""))):
                    if val and not ev[col]:
                        ev[col] = val
            n_event_facts += 1
            if person_node:
                g.add_edge(source_id=person_node, target_id=ev["event_id"],
                           edge_type="cast_in", role_detail=role,
                           character=r.get("character", ""),
                           event_id=ev["event_id"], production_key=key, **common)
                g.add_edge(source_id=person_node, target_id=play_node,
                           edge_type=ROLE_EDGE.get(role, "involved_in"),
                           role_detail="derived_from_event",
                           character=r.get("character", ""),
                           event_id=ev["event_id"], production_key=key, **common)
        else:
            n_binary += 1
            if ftype == "mention_only":
                if person_node and play_node:
                    g.add_edge(source_id=person_node, target_id=play_node,
                               edge_type="mention", role_detail=role,
                               character=r.get("character", ""),
                               event_id="", production_key="", **common)
                continue
            if person_node and play_node:
                etype = ("authored" if ftype == "authorship"
                         else "translated_adapted" if ftype == "translation_adaptation"
                         else "composed_music" if ftype == "music"
                         else ROLE_EDGE.get(role, "involved_in"))
                g.add_edge(source_id=person_node, target_id=play_node,
                           edge_type=etype, role_detail=role,
                           character=r.get("character", ""),
                           event_id="", production_key="", **common)
            elif play_node and (venue_node or place_node or org_node):
                g.add_edge(source_id=play_node,
                           target_id=venue_node or org_node or place_node,
                           edge_type="staged_at" if venue_node or org_node else "performed_in_place",
                           role_detail="", character="",
                           event_id="", production_key="", **common)

    # event structural edges
    for ev in ev_by_key.values():
        g.events.append(ev)
        base = dict(role_detail="", character="", date_start=ev["date_start"],
                    date_end=ev["date_end"], date_precision=ev["date_precision"],
                    event_id=ev["event_id"], production_key=ev["production_key"],
                    provenance_person_id=ev["provenance_person_id"],
                    provenance_fact_ids=ev["provenance_fact_ids"],
                    evidence_sentence=ev["evidence_sentence"],
                    extraction_model=ev["extraction_model"],
                    confidence=ev["confidence"], match_status="matched")
        g.add_edge(source_id=ev["event_id"], target_id=ev["play_id"],
                   edge_type="of_play", **base)
        for col, etype in (("venue_id", "staged_at"), ("org_id", "produced_by"),
                           ("place_id", "held_in")):
            if ev[col]:
                g.add_edge(source_id=ev["event_id"], target_id=ev[col],
                           edge_type=etype, **base)
        g.add_node(ev["event_id"], node_type="production_event",
                   label_english=f"{ev['event_type']} {ev['date_start'][:4]}",
                   match_status="matched")

    for i, e in enumerate(g.edges, 1):
        e["edge_id"] = f"E-{i:05d}"

    nodes = list(g.nodes.values())
    print(f"nodes: {len(nodes)}  by type: {dict(Counter(n['node_type'] for n in nodes))}")
    print(f"edges: {len(g.edges)}  by type: {dict(Counter(e['edge_type'] for e in g.edges))}")
    print(f"events: {len(g.events)}  (facts->events: {n_event_facts}, binary: {n_binary}, "
          f"edition links: {n_ed_links})")

    if not args.execute:
        print("\ndry-run — pass --execute to write kg/")
        return
    for n in nodes:
        for k in NODE_FIELDS:
            n.setdefault(k, "")
    pc.write_tsv(NODES_TSV, nodes, NODE_FIELDS)
    pc.write_tsv(EDGES_TSV, g.edges, EDGE_FIELDS)
    pc.write_tsv(EVENTS_TSV, g.events, EVENT_FIELDS)
    print(f"wrote {NODES_TSV} ({len(nodes)}), {EDGES_TSV} ({len(g.edges)}), "
          f"{EVENTS_TSV} ({len(g.events)})")


if __name__ == "__main__":
    main()
