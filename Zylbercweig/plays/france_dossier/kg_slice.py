#!/usr/bin/env python3.11
"""France KG slice -> france_dossier/kg_slice.json (place nodes, edges, events, orgs)."""
import csv, json
from pathlib import Path
csv.field_size_limit(10**9)
H = Path(__file__).resolve().parent
KG = H.parent / "kg"
N = {r["node_id"]: r for r in csv.DictReader(open(KG / "nodes.tsv"), delimiter="\t")}
FR = {"place:Q90": "Paris", "place:Q142": "France", "place:Q201348": "Drancy",
      "place:Q22690": "Metz", "place:Q40898": "Nancy", "place:Q6602": "Strasbourg",
      "place:Q1724334": "Ay (canton)"}
lab = lambda i: (N.get(i, {}).get("label_english") or N.get(i, {}).get("label_yiddish") or i)
yi = lambda i: N.get(i, {}).get("label_yiddish", "")
edges = []
for e in csv.DictReader(open(KG / "edges.tsv"), delimiter="\t"):
    if e["target_id"] in FR or e["source_id"] in FR:
        edges.append({k: e[k] for k in ("edge_id", "source_id", "target_id", "edge_type",
                      "date_start", "provenance_person_id", "evidence_sentence", "source_layer")}
                     | {"src_en": lab(e["source_id"]), "src_yi": yi(e["source_id"]),
                        "src_type": N.get(e["source_id"], {}).get("node_type", "")})
orgs = {e["source_id"] for e in edges if e["edge_type"] == "located_in"}
org_edges = []
for e in csv.DictReader(open(KG / "edges.tsv"), delimiter="\t"):
    if e["target_id"] in orgs and e["source_id"].startswith("person:"):
        org_edges.append({"edge_id": e["edge_id"], "person": e["source_id"],
                          "person_en": lab(e["source_id"]), "person_yi": yi(e["source_id"]),
                          "org": e["target_id"], "type": e["edge_type"],
                          "role": e["role_detail"], "date": e["date_start"]})
events = []
for v in csv.DictReader(open(KG / "events.tsv"), delimiter="\t"):
    if v["place_id"] in FR:
        events.append(v | {"play_label": yi(v["play_id"]), "org_label": yi(v["org_id"])})
json.dump({"places": {k: {"en": v, "yi": yi(k), "node": N.get(k)} for k, v in FR.items()},
           "edges": edges, "org_edges": org_edges, "events": events,
           "orgs": {o: {"yi": yi(o), "en": lab(o),
                        "type": json.loads(N[o]["attrs"] or "{}").get("org_type", "") if o in N else ""}
                    for o in orgs}},
          open(H / "kg_slice.json", "w"), ensure_ascii=False, indent=1)
print(len(edges), "edges", len(orgs), "orgs", len(org_edges), "person-org edges", len(events), "events")
