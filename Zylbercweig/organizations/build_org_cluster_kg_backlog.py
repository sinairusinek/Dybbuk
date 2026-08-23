import csv, json, sys
from collections import defaultdict, Counter

csv.field_size_limit(10**9)

"""Rank unaligned org clusters (org_cluster: nodes in the plays KG) by the
number of distinct persons the KG attaches to them, with existing
org_alignment_review status. Output feeds the 'KG persons' sort in Zalmen's
Org Alignment view. Re-run after build_kg.py --execute.

    python3.11 Zylbercweig/organizations/build_org_cluster_kg_backlog.py
"""
import pathlib
REPO = str(pathlib.Path(__file__).resolve().parents[2])

nodes_path = f"{REPO}/Zylbercweig/plays/kg/nodes.tsv"
edges_path = f"{REPO}/Zylbercweig/plays/kg/edges.tsv"
clustered_path = f"{REPO}/Zylbercweig/organizations/organizations_clustered.tsv"
review_path = f"{REPO}/Zylbercweig/organizations/org_alignment_review.tsv"
out_path = f"{REPO}/Zylbercweig/organizations/org_cluster_kg_backlog.tsv"

# --- load nodes ---
nodes = {}
org_cluster_nodes = {}  # node_id -> row dict
with open(nodes_path, encoding="utf-8") as f:
    r = csv.DictReader(f, delimiter="\t")
    for row in r:
        nodes[row["node_id"]] = row
        if row.get("ext_ref_type") == "org_cluster":
            org_cluster_nodes[row["node_id"]] = row

print(f"org_cluster nodes: {len(org_cluster_nodes)}", file=sys.stderr)

# --- load edges, filter orgrel edges with target = org_cluster node, source = person ---
# also located_in edges: org -> place
person_edges = defaultdict(list)  # target_org_node_id -> list of (edge_type, source_id)
located_in = defaultdict(list)  # source_org_node_id -> list of target_id (place)

with open(edges_path, encoding="utf-8") as f:
    r = csv.DictReader(f, delimiter="\t")
    for row in r:
        src = row["source_id"]
        tgt = row["target_id"]
        etype = row["edge_type"]
        layer = row.get("source_layer", "")
        if tgt in org_cluster_nodes and layer == "orgrel":
            person_edges[tgt].append((etype, src))
        if src in org_cluster_nodes and etype == "located_in":
            located_in[src].append(tgt)

# --- load organizations_clustered.tsv for cluster_size ---
cluster_size = {}
with open(clustered_path, encoding="utf-8-sig") as f:
    r = csv.DictReader(f, delimiter="\t")
    for row in r:
        cid = row.get("cluster_id")
        if cid:
            cluster_size[cid] = row.get("cluster_size", "")

# --- load org_alignment_review.tsv ---
review = {}
with open(review_path, encoding="utf-8-sig") as f:
    r = csv.DictReader(f, delimiter="\t")
    for row in r:
        cid = row.get("cluster_id")
        if cid:
            review[cid] = {
                "decision": row.get("decision", ""),
                "aligned_db_id": row.get("aligned_db_id", ""),
            }

n_review_nonempty = sum(1 for v in review.values() if v["decision"].strip())

# --- build output rows ---
rows_out = []
count_ge5 = 0
count_ge2 = 0
count_eq1 = 0
count_already_in_queue = 0

for node_id, node in org_cluster_nodes.items():
    cluster_id = node.get("ext_ref_id", "")
    canonical = node.get("label_yiddish", "")
    attrs_raw = node.get("attrs", "")
    org_type = ""
    if attrs_raw:
        try:
            attrs = json.loads(attrs_raw)
            org_type = attrs.get("org_type", "") or ""
        except Exception:
            org_type = ""

    edges = person_edges.get(node_id, [])
    n_person_edges = len(edges)
    persons = set(src for _, src in edges)
    n_distinct_persons = len(persons)

    edge_type_counts = Counter(et for et, _ in edges)
    top_edge_types = "|".join(f"{et}:{c}" for et, c in edge_type_counts.most_common())

    loc_targets = located_in.get(node_id, [])
    n_located_in = len(loc_targets)
    places_en = "|".join(
        (nodes.get(t, {}).get("label_english") or nodes.get(t, {}).get("label_yiddish") or t)
        for t in loc_targets
    )

    # sample persons: up to 3, with label_yiddish(db id)
    sample_list = []
    for pid in list(persons)[:3]:
        pnode = nodes.get(pid, {})
        label = pnode.get("label_yiddish") or pnode.get("label_english") or pid
        dbid = pnode.get("ext_ref_id", "")
        sample_list.append(f"{label}({dbid})" if dbid else label)
    sample_persons = "|".join(sample_list)

    csize = cluster_size.get(cluster_id, "")

    rev = review.get(cluster_id)
    if rev:
        status_bits = []
        if rev["decision"].strip():
            status_bits.append(f"decision={rev['decision']}")
        if rev["aligned_db_id"].strip():
            status_bits.append(f"aligned_db_id={rev['aligned_db_id']}")
        if status_bits:
            existing_review_status = "; ".join(status_bits)
            count_already_in_queue += 1
        else:
            existing_review_status = "in_review_file_no_decision"
    else:
        existing_review_status = ""

    if n_distinct_persons >= 5:
        count_ge5 += 1
    if n_distinct_persons >= 2:
        count_ge2 += 1
    if n_distinct_persons == 1:
        count_eq1 += 1

    rows_out.append({
        "cluster_id": cluster_id,
        "canonical_yiddish": canonical,
        "org_type": org_type,
        "n_person_edges": n_person_edges,
        "n_distinct_persons": n_distinct_persons,
        "top_edge_types": top_edge_types,
        "n_located_in": n_located_in,
        "places_en": places_en,
        "sample_persons": sample_persons,
        "cluster_size": csize,
        "existing_review_status": existing_review_status,
    })

rows_out.sort(key=lambda r: (-r["n_distinct_persons"], -r["n_person_edges"]))

fieldnames = ["cluster_id", "canonical_yiddish", "org_type", "n_person_edges",
              "n_distinct_persons", "top_edge_types", "n_located_in", "places_en",
              "sample_persons", "cluster_size", "existing_review_status"]

with open(out_path, "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
    w.writeheader()
    for row in rows_out:
        w.writerow(row)

print(f"Total org_cluster nodes written: {len(rows_out)}")
print(f">=5 distinct persons: {count_ge5}")
print(f">=2 distinct persons: {count_ge2}")
print(f"=1 distinct person: {count_eq1}")
print(f"already have non-empty decision in org_alignment_review.tsv: {count_already_in_queue}")
print(f"(review file has {n_review_nonempty} total rows with non-empty decision, across all clusters not just org_cluster-node ones)")
